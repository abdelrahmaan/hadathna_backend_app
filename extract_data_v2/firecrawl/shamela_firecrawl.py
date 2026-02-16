import re
import json
import time
import random
import requests
import threading
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed


class SkipReason(Enum):
    API_FAILURE = "api_failure"
    EMPTY_HTML = "empty_html"
    NO_HADITH_BLOCKS = "no_hadith_blocks"
    NO_NARRATORS = "no_narrators"
    SUCCESS = "success"


@dataclass
class ScrapeResult:
    success: bool
    data: Optional[dict] = None
    reason: SkipReason = SkipReason.SUCCESS
    message: str = ""
    raw_html_snippet: str = ""

NARRATOR_RE = re.compile(r"/narrator/(\d+)")
WS_RE = re.compile(r"\s+")

RETRYABLE_FIRECRAWL_HTTP_CODES = {"408", "409", "425", "429", "500", "502", "503", "504"}
RETRYABLE_TARGET_CODES = {
    "408", "409", "423", "425", "429",
    "500", "502", "503", "504",
    "520", "521", "522", "523", "524", "525", "526", "530",
}
CLOUDFLARE_ERROR_MARKERS = [
    "cf-browser-verification",
    "challenge-platform",
    "just a moment",
    "checking your browser",
    "id=\"cf-wrapper\"",
    "id=\"cf-error-details\"",
    "error code 520",
    "error code 522",
    "cloudflare",
]


def _normalize_status_code(value) -> str:
    """Normalize status code to string for consistent comparisons."""
    if value is None:
        return ""
    return str(value).strip()


def _compute_backoff_seconds(attempt: int, base: float = 5.0, cap: float = 45.0) -> float:
    """Linear backoff with small jitter to avoid synchronized retries."""
    linear = base * (attempt + 1)
    jitter = random.uniform(0.0, 1.5)
    return min(cap, linear + jitter)


def _is_cloudflare_error_page(html: str) -> bool:
    """Detect Cloudflare challenge/error pages returned instead of target content."""
    lowered = (html or "").lower()
    return any(marker in lowered for marker in CLOUDFLARE_ERROR_MARKERS)

def norm(text: str) -> str:
    return WS_RE.sub(" ", (text or "")).strip()

def extract_breadcrumb(soup: BeautifulSoup) -> list:
    """
    Extract breadcrumb links: list of {text, href}
    """
    # 1) Find the exact label string on the page
    label_node = soup.find(string=lambda s: isinstance(s, str) and "مسار الصفحة الحالية" in s)
    if label_node:
        container = label_node.parent
        for _ in range(3):
            links = container.find_all("a", href=True)
            if links:
                return [{"text": norm(a.get_text(" ", strip=True)), "href": a["href"]} for a in links]
            container = container.parent

    # 2) Fallback: common breadcrumb classes/ids
    bc = soup.select_one(".breadcrumb, .breadcrumbs, #breadcrumb, .path, .navpath")
    if bc:
        links = bc.find_all("a", href=True)
        return [{"text": norm(a.get_text(" ", strip=True)), "href": a["href"]} for a in links]

    return []

def extract_hadith_and_narrators(soup: BeautifulSoup) -> list:
    """
    Extract hadith blocks from the page.
    Returns list of hadith block dicts.
    """
    blocks = soup.select("div.nass.margin-top-10, div.nass")
    results = []

    for block in blocks:
        # Narrators (IDs + names)
        narrators = []
        for a in block.select("a[href*='/narrator/']"):
            href = a.get("href", "")
            m = NARRATOR_RE.search(href)
            if not m:
                continue
            narrators.append({
                "id": m.group(1),
                "name": norm(a.get_text(" ", strip=True)),
                "url": href
            })

        # Matn often inside span.c2
        matn_el = block.select_one("span.c2")
        matn = norm(matn_el.get_text(" ", strip=True)) if matn_el else ""

        # Clean full text: remove UI garbage (copy button, icons, anchors)
        block_clean = BeautifulSoup(str(block), "html.parser")
        for el in block_clean.select("a.btn_tag, span.fa, span.anchor"):
            el.decompose()

        full_text = norm(block_clean.get_text(" ", strip=True))

        results.append({
            "full_text": full_text,
            "matn": matn,
            "narrators": narrators
        })

    return results

def has_narrator_data(hadith_blocks: list) -> bool:
    """Check if at least one hadith block has narrators."""
    for block in hadith_blocks:
        if block.get("narrators") and len(block["narrators"]) > 0:
            return True
    return False

def scrape_with_firecrawl(url: str, api_key: str, max_retries: int = 3) -> ScrapeResult:
    """
    Scrape a URL using Firecrawl API to bypass Cloudflare protection.
    Returns ScrapeResult with success/failure details for every call.
    """
    firecrawl_url = "https://api.firecrawl.dev/v2/scrape"

    payload = {
        "url": url,
        "onlyMainContent": False,
        "formats": ["html"]
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    html = ""

    for attempt in range(max_retries):
        try:
            print(f"  Firecrawl request: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.post(firecrawl_url, json=payload, headers=headers, timeout=120)

            # Handle HTTP-level errors from Firecrawl API itself
            if response.status_code != 200:
                http_code = response.status_code
                print(f"  Firecrawl HTTP {http_code}")
                http_code_str = _normalize_status_code(http_code)
                if http_code_str in RETRYABLE_FIRECRAWL_HTTP_CODES and attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if http_code_str == "429" else 5.0
                    )
                    print(f"  Retryable Firecrawl HTTP {http_code}, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Firecrawl HTTP {http_code}",
                )

            result = response.json()
            firecrawl_success = result.get("success", False)
            metadata = result.get("data", {}).get("metadata", {})
            target_status = _normalize_status_code(metadata.get("statusCode"))
            html = result.get("data", {}).get("html", "")

            print(f"  Firecrawl response: success={firecrawl_success}, target_statusCode={target_status}")

            # Check if Firecrawl reported failure
            if not firecrawl_success:
                error_msg = result.get("error", "Unknown error")
                print(f"  FAILED: {error_msg}")

                error_str = str(error_msg).lower()
                is_retryable = ("timeout" in error_str or
                                any(code in str(error_msg) for code in RETRYABLE_TARGET_CODES))

                if is_retryable and attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if "429" in str(error_msg) else 5.0
                    )
                    print(f"  Retryable Firecrawl error, waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue

                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Firecrawl failed: {error_msg}",
                )

            # Target responded with retryable upstream status (e.g., 520/522/524)
            if target_status in RETRYABLE_TARGET_CODES:
                if attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(
                        attempt, base=15.0 if target_status == "429" else 6.0
                    )
                    print(f"  Target returned {target_status}; retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message=f"Target status {target_status} after {max_retries} attempts",
                    raw_html_snippet=html[:500],
                )

            if not html or len(html) < 100:
                print(f"  Empty or near-empty HTML ({len(html)} chars)")
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.EMPTY_HTML,
                    message=f"Empty HTML ({len(html)} chars)",
                    raw_html_snippet=html[:500],
                )

            # Check for Cloudflare challenge/error page
            if _is_cloudflare_error_page(html):
                if attempt < max_retries - 1:
                    wait_time = _compute_backoff_seconds(attempt, base=10.0)
                    print(f"  Cloudflare error/challenge page detected, retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    continue
                return ScrapeResult(
                    success=False,
                    reason=SkipReason.API_FAILURE,
                    message="Cloudflare error/challenge page received",
                    raw_html_snippet=html[:500],
                )

            print(f"  HTML retrieved: {len(html)} chars")

            # Parse and check if page content actually loaded
            soup = BeautifulSoup(html, "html.parser")
            hadith_blocks = extract_hadith_and_narrators(soup)

            if not hadith_blocks:
                # Page likely didn't finish loading — retry
                any_nass = soup.select("div.nass")
                any_narrator_links = soup.select("a[href*='/narrator/']")

                # Avoid misclassifying Cloudflare error pages as selector/content failures
                if _is_cloudflare_error_page(html):
                    if attempt < max_retries - 1:
                        wait_time = _compute_backoff_seconds(attempt, base=10.0)
                        print(f"  Cloudflare page (no content), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.API_FAILURE,
                        message=f"Cloudflare page after {max_retries} attempts",
                        raw_html_snippet=html[:500],
                    )

                if len(any_nass) == 0 and len(any_narrator_links) == 0:
                    if attempt < max_retries - 1:
                        wait_time = _compute_backoff_seconds(attempt, base=5.0)
                        print(f"  Page content not loaded (0 div.nass), retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    # All retries exhausted
                    msg = (f"No hadith blocks after {max_retries} attempts. "
                           f"div.nass count: 0, narrator links: 0")
                    print(f"  {msg}")
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.NO_HADITH_BLOCKS,
                        message=msg,
                        raw_html_snippet=html[:500],
                    )
                else:
                    # div.nass exists but no hadith extracted — legit empty or selector issue
                    msg = (f"No hadith blocks. div.nass count: {len(any_nass)}, "
                           f"narrator links in page: {len(any_narrator_links)}")
                    print(f"  {msg}")
                    return ScrapeResult(
                        success=False,
                        reason=SkipReason.NO_HADITH_BLOCKS,
                        message=msg,
                        raw_html_snippet=html[:500],
                    )

            # Got hadith blocks — exit retry loop
            breadcrumb_links = extract_breadcrumb(soup)
            break

        except requests.exceptions.Timeout:
            print(f"  Request timeout")
            if attempt < max_retries - 1:
                wait_time = _compute_backoff_seconds(attempt, base=5.0)
                print(f"  Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            return ScrapeResult(
                success=False,
                reason=SkipReason.API_FAILURE,
                message=f"Request timeout after {max_retries} attempts",
            )

        except requests.exceptions.RequestException as e:
            print(f"  Connection error: {e}")
            if attempt < max_retries - 1:
                wait_time = _compute_backoff_seconds(attempt, base=5.0)
                print(f"  Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            return ScrapeResult(
                success=False,
                reason=SkipReason.API_FAILURE,
                message=f"Connection error after {max_retries} attempts: {e}",
            )

    else:
        return ScrapeResult(
            success=False,
            reason=SkipReason.API_FAILURE,
            message=f"Failed after {max_retries} attempts",
        )

    # Blocks found but no narrators
    if not has_narrator_data(hadith_blocks):
        sample_text = hadith_blocks[0].get("full_text", "")[:100] if hadith_blocks else ""
        msg = f"{len(hadith_blocks)} hadith blocks but no narrator links"
        print(f"  {msg}. Sample: {sample_text}...")
        return ScrapeResult(
            success=False,
            reason=SkipReason.NO_NARRATORS,
            message=msg,
            raw_html_snippet=html[:500],
        )

    # Success
    narrator_count = sum(len(b.get("narrators", [])) for b in hadith_blocks)
    print(f"  OK: {len(hadith_blocks)} hadith blocks, {narrator_count} narrators")

    return ScrapeResult(
        success=True,
        data={
            "url": url,
            "breadcrumb_links": breadcrumb_links,
            "hadith_blocks": hadith_blocks,
        },
    )

def load_scraped_pages(jsonl_path: Path) -> set:
    """
    Load only SUCCESS and NO_NARRATORS page numbers from JSONL file.
    Failed pages (api_failure, empty_html, no_hadith_blocks) will be retried.
    no_narrators is a legit skip (page has content but no narrator links).
    """
    scraped = set()
    if not jsonl_path.exists():
        return scraped
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            status = obj.get("status")
            reason = obj.get("reason", "")
            if status == "success" or reason == "no_narrators":
                scraped.add(obj["page_number"])
    return scraped

def get_failed_pages(jsonl_path: Path) -> List[int]:
    """Get sorted list of failed page numbers that should be retried."""
    if not jsonl_path.exists():
        return []
    failed = set()
    done = set()
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            page = obj.get("page_number")
            status = obj.get("status")
            reason = obj.get("reason", "")
            if status == "success" or reason == "no_narrators":
                done.add(page)
            elif status == "failed":
                failed.add(page)
    return sorted(failed - done)

def remove_failed_entries(jsonl_path: Path, pages_to_retry: set):
    """Remove old failed entries for pages that will be retried."""
    if not jsonl_path.exists() or not pages_to_retry:
        return
    kept_lines = []
    removed = 0
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            page = obj.get("page_number")
            if page in pages_to_retry and obj.get("status") == "failed":
                removed += 1
            else:
                kept_lines.append(line.rstrip('\n'))
    if removed > 0:
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for line in kept_lines:
                f.write(line + '\n')
        print(f"Cleaned {removed} old failed entries for retry")

# Lock for thread-safe file writes and key rotation
_file_lock = threading.Lock()
_key_lock = threading.Lock()

def append_jsonl(obj: dict, jsonl_path: Path):
    """Append a single JSON object as a line to a JSONL file (thread-safe)."""
    with _file_lock:
        with open(jsonl_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

def _scrape_page(book_id: int, page_num: int, api_keys: List[str], key_state: dict,
                 jsonl_output: Path, debug_dir: Optional[Path]) -> Optional[dict]:
    """
    Scrape a single page. Handles 402 key rotation (thread-safe).
    Returns result summary dict or None if all keys exhausted.
    key_state = {"index": int, "key": str} — mutated in place on rotation.
    """
    url = f"https://shamela.ws/book/{book_id}/{page_num}"

    # Get current key (thread-safe read)
    with _key_lock:
        current_key = key_state["key"]

    result = scrape_with_firecrawl(url, current_key)

    # 402 = quota exhausted -> rotate API key and retry same page
    if not result.success and "402" in result.message:
        with _key_lock:
            # Check if another thread already rotated the key
            if key_state["key"] == current_key:
                key_state["index"] += 1
                if key_state["index"] < len(api_keys):
                    key_state["key"] = api_keys[key_state["index"]]
                    print(f"\n  ** API key quota exhausted. Switching to key {key_state['index'] + 1}/{len(api_keys)} **")
                else:
                    print(f"\n  ** All {len(api_keys)} API keys exhausted! Stopping. **")
                    obj = {
                        "status": "failed",
                        "book_id": book_id,
                        "page_number": page_num,
                        "url": url,
                        "reason": "api_failure",
                        "message": "All API keys exhausted (402)",
                    }
                    append_jsonl(obj, jsonl_output)
                    return None  # Signal to stop

            # Retry with new key (could be rotated by this thread or another)
            new_key = key_state["key"]

        if key_state["index"] >= len(api_keys):
            obj = {
                "status": "failed",
                "book_id": book_id,
                "page_number": page_num,
                "url": url,
                "reason": "api_failure",
                "message": "All API keys exhausted (402)",
            }
            append_jsonl(obj, jsonl_output)
            return None

        result = scrape_with_firecrawl(url, new_key)

    if result.success:
        obj = {
            "status": "success",
            "book_id": book_id,
            "page_number": page_num,
            **result.data,
        }
        append_jsonl(obj, jsonl_output)
        return {"success": True, "reason": None}
    else:
        obj = {
            "status": "failed",
            "book_id": book_id,
            "page_number": page_num,
            "url": url,
            "reason": result.reason.value,
            "message": result.message,
        }
        append_jsonl(obj, jsonl_output)

        if debug_dir and result.raw_html_snippet:
            debug_file = debug_dir / f"page_{page_num}_{result.reason.value}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(result.raw_html_snippet)

        return {"success": False, "reason": result.reason}

def _categorize_failure(outcome: dict, api_failures, empty_pages, no_block_pages,
                        no_narrator_pages, page_num: int):
    """Add a failure entry to the appropriate category list."""
    entry = {"page": page_num, "reason": outcome["reason"].value}
    if outcome["reason"] == SkipReason.API_FAILURE:
        api_failures.append(entry)
    elif outcome["reason"] == SkipReason.EMPTY_HTML:
        empty_pages.append(entry)
    elif outcome["reason"] == SkipReason.NO_HADITH_BLOCKS:
        no_block_pages.append(entry)
    elif outcome["reason"] == SkipReason.NO_NARRATORS:
        no_narrator_pages.append(entry)

def _process_batch(batch: List[int], book_id: int, api_keys: List[str], key_state: dict,
                   jsonl_output: Path, debug_dir: Optional[Path], max_workers: int,
                   delay: float, label: str, start_idx: int, total: int,
                   api_failures, empty_pages, no_block_pages, no_narrator_pages, error_pages):
    """
    Process a list of page numbers using concurrent workers.
    Returns (newly_scraped, keys_exhausted).
    """
    newly_scraped = 0
    keys_exhausted = False

    # Process pages in groups of max_workers
    for batch_start in range(0, len(batch), max_workers):
        if keys_exhausted:
            break

        group = batch[batch_start:batch_start + max_workers]
        group_idx = start_idx + batch_start

        for p in group:
            print(f"\n[{label} {group_idx + group.index(p) + 1}/{total}] Page {p}")

        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page_num in group:
                future = executor.submit(
                    _scrape_page, book_id, page_num, api_keys, key_state,
                    jsonl_output, debug_dir
                )
                futures[future] = page_num

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    outcome = future.result()
                    if outcome is None:
                        keys_exhausted = True
                    elif outcome["success"]:
                        newly_scraped += 1
                    else:
                        _categorize_failure(outcome, api_failures, empty_pages,
                                          no_block_pages, no_narrator_pages, page_num)
                except Exception as e:
                    print(f"  UNEXPECTED ERROR on page {page_num}: {e}")
                    obj = {
                        "status": "failed",
                        "book_id": book_id,
                        "page_number": page_num,
                        "url": f"https://shamela.ws/book/{book_id}/{page_num}",
                        "reason": "unexpected_error",
                        "message": str(e),
                    }
                    append_jsonl(obj, jsonl_output)
                    error_pages.append({"page": page_num, "reason": str(e)})

        # Delay between batches (not between individual requests within a batch)
        if not keys_exhausted and batch_start + max_workers < len(batch):
            time.sleep(delay)

    return newly_scraped, keys_exhausted

def scrape_book_pages(book_id: int, start_page: int, end_page: int, api_keys: List[str],
                      jsonl_output: Path, delay: float = 1.0,
                      debug_dir: Optional[Path] = None,
                      max_workers: int = 2) -> int:
    """
    Scrape multiple pages from a Shamela book with concurrent requests.
    1) First retries all previously failed pages (removes old entries, re-scrapes).
    2) Then continues with new pages from where it left off.
    On 402 (quota exhausted), rotates to the next API key.
    max_workers controls concurrency (default 2 for Firecrawl free tier).
    """
    if debug_dir:
        debug_dir.mkdir(parents=True, exist_ok=True)
        print(f"Debug mode: saving problematic HTML to {debug_dir}")

    # API key rotation state (shared across retry + new pages)
    key_state = {"index": 0, "key": api_keys[0]}
    print(f"Using API key {key_state['index'] + 1}/{len(api_keys)}")
    print(f"Concurrent workers: {max_workers}")

    # Categorized failure tracking
    api_failures = []
    empty_pages = []
    no_block_pages = []
    no_narrator_pages = []
    error_pages = []
    newly_scraped = 0
    keys_exhausted = False

    # ── Phase 1: Retry previously failed pages ──
    failed_pages = get_failed_pages(jsonl_output)
    if failed_pages:
        print(f"\n{'='*60}")
        print(f"PHASE 1: Retrying {len(failed_pages)} failed pages")
        print(f"{'='*60}")
        remove_failed_entries(jsonl_output, set(failed_pages))

        phase1_scraped, keys_exhausted = _process_batch(
            failed_pages, book_id, api_keys, key_state, jsonl_output, debug_dir,
            max_workers, delay, "Retry", 0, len(failed_pages),
            api_failures, empty_pages, no_block_pages, no_narrator_pages, error_pages
        )
        newly_scraped += phase1_scraped

        if phase1_scraped > 0:
            print(f"\nRetry results: {phase1_scraped} fixed out of {len(failed_pages)}")

    # ── Phase 2: Continue with new pages ──
    if not keys_exhausted:
        scraped_pages = load_scraped_pages(jsonl_output)
        # Build list of pages still to do
        remaining = [p for p in range(start_page, end_page + 1) if p not in scraped_pages]

        if remaining:
            print(f"\n{'='*60}")
            print(f"PHASE 2: Scraping new pages ({len(remaining)} remaining)")
            print(f"{'='*60}")

            phase2_scraped, keys_exhausted = _process_batch(
                remaining, book_id, api_keys, key_state, jsonl_output, debug_dir,
                max_workers, delay, "", 0, len(remaining),
                api_failures, empty_pages, no_block_pages, no_narrator_pages, error_pages
            )
            newly_scraped += phase2_scraped
        else:
            print("\nAll pages already scraped!")

    # Print detailed summary
    print(f"\n{'='*60}")
    print(f"SCRAPING SUMMARY")
    print(f"{'='*60}")
    total_pages = end_page - start_page + 1
    final_scraped = load_scraped_pages(jsonl_output)
    print(f"Total pages in range:       {total_pages}")
    print(f"Total done (success/skip):  {len(final_scraped)}")
    print(f"Successfully scraped:       {newly_scraped}")
    print(f"{'='*60}")
    print(f"FAILURES:")
    print(f"  API failures:             {len(api_failures)}")
    print(f"  Empty/blocked pages:      {len(empty_pages)}")
    print(f"  No hadith blocks:         {len(no_block_pages)}")
    print(f"  No narrator links:        {len(no_narrator_pages)}")
    print(f"  Unexpected errors:        {len(error_pages)}")
    print(f"{'='*60}")

    if api_failures:
        print(f"\nAPI failures (first 5):")
        for f in api_failures[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if empty_pages:
        print(f"\nEmpty/blocked pages (first 5):")
        for f in empty_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if no_block_pages:
        print(f"\nNo hadith blocks (first 5) -- possible CSS selector issue:")
        for f in no_block_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if no_narrator_pages:
        print(f"\nHadith blocks but no narrators (first 5):")
        for f in no_narrator_pages[:5]:
            print(f"  Page {f['page']}: {f['reason']}")

    if error_pages:
        print(f"\nUnexpected errors:")
        for f in error_pages:
            print(f"  Page {f['page']}: {f['reason']}")

    # Save failure report
    failure_report = {
        "scrape_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "book_id": book_id,
        "page_range": f"{start_page}-{end_page}",
        "api_failures": api_failures,
        "empty_pages": empty_pages,
        "no_block_pages": no_block_pages,
        "no_narrator_pages": no_narrator_pages,
        "error_pages": error_pages,
    }
    report_path = jsonl_output.parent / f"failure_report_{book_id}.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(failure_report, f, ensure_ascii=False, indent=2)
    print(f"\nFailure report saved to: {report_path}")

    return newly_scraped

if __name__ == "__main__":
    # Firecrawl API keys (rotates to next on 402 quota exhausted)
    API_KEYS = [
        "fc-bb3459dabca8414b8c92f647cde7ebf3",
        "fc-68d7c10c71b74bb5a52d3e7534f28730",
        "fc-ff5958295ba0497280bc8cc9ca8f5279",
        "fc-a0e6b09c69d5441293d77c29a403ae85"
        
    ]

    # Configuration
    BOOK_ID = 1681
    START_PAGE = 10
    END_PAGE = 11207

    # Delay between batches (in seconds)
    DELAY_SECONDS = 3.0

    # Concurrent requests (Firecrawl free tier allows 2)
    MAX_WORKERS = 2

    # Debug directory for saving HTML of failed pages (set to None to disable)
    DEBUG_DIR = Path(__file__).parent / f"debug_html_{BOOK_ID}"

    # Output path
    jsonl_output = Path(__file__).parent / f"shamela_book_{BOOK_ID}.jsonl"

    print(f"Starting scrape for book {BOOK_ID}, pages {START_PAGE} to {END_PAGE}")
    print(f"Delay between batches: {DELAY_SECONDS}s")
    print(f"Concurrent workers: {MAX_WORKERS}")
    print(f"API keys available: {len(API_KEYS)}")

    try:
        newly_scraped = scrape_book_pages(
            book_id=BOOK_ID,
            start_page=START_PAGE,
            end_page=END_PAGE,
            api_keys=API_KEYS,
            jsonl_output=jsonl_output,
            delay=DELAY_SECONDS,
            debug_dir=DEBUG_DIR,
            max_workers=MAX_WORKERS,
        )

        print(f"\nData saved to: {jsonl_output}")

    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print("Data has been saved up to the last successful page.")
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
