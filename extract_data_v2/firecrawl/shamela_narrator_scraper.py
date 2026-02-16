import re
import json
import time
import requests
import threading
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

WS_RE = re.compile(r"\s+")

def norm(text: str) -> str:
    return WS_RE.sub(" ", (text or "")).strip()


def scrape_narrator_with_firecrawl(narrator_id: str, api_key: str, max_retries: int = 3) -> dict:
    """
    Scrape narrator profile page using Firecrawl.
    Returns {"success": bool, "data": dict|None, "message": str}
    """
    url = f"https://shamela.ws/narrator/{narrator_id}"
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

    RETRYABLE_CODES = {"522", "429", "500", "502", "503"}

    for attempt in range(max_retries):
        try:
            print(f"  Firecrawl request: {url} (attempt {attempt + 1}/{max_retries})")
            response = requests.post(firecrawl_url, json=payload, headers=headers, timeout=120)

            # HTTP-level errors from Firecrawl API
            if response.status_code != 200:
                http_code = response.status_code
                print(f"  Firecrawl HTTP {http_code}")
                if str(http_code) in RETRYABLE_CODES and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * (15 if http_code == 429 else 5)
                    print(f"  Retryable HTTP {http_code}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return {"success": False, "data": None,
                        "message": f"Firecrawl HTTP {http_code}"}

            result = response.json()
            firecrawl_success = result.get("success", False)
            metadata = result.get("data", {}).get("metadata", {})
            target_status = metadata.get("statusCode")

            print(f"  Firecrawl response: success={firecrawl_success}, target_statusCode={target_status}")

            if not firecrawl_success:
                error_msg = result.get("error", "Unknown error")
                print(f"  FAILED: {error_msg}")
                error_str = str(error_msg).lower()
                is_retryable = ("timeout" in error_str or
                                any(code in str(error_msg) for code in RETRYABLE_CODES))
                if is_retryable and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"  Retryable error, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                return {"success": False, "data": None,
                        "message": f"Firecrawl failed: {error_msg}"}

            html = result.get("data", {}).get("html", "")
            if not html or len(html) < 100:
                print(f"  Empty HTML ({len(html)} chars)")
                return {"success": False, "data": None,
                        "message": f"Empty HTML ({len(html)} chars)"}

            print(f"  HTML retrieved: {len(html)} chars")

            # Parse the HTML
            soup = BeautifulSoup(html, "html.parser")
            narrator_data = extract_narrator_info(soup, narrator_id, url)

            # Validate: reject 404 pages
            name = narrator_data.get("name", "")
            if "404" in name or "Page Not Found" in name:
                print(f"  404 page for narrator {narrator_id}")
                return {"success": False, "data": None, "message": "404 page not found"}

            # Validate: reject empty profiles (no real name AND no kunya)
            # Strip punctuation/whitespace — pages like ID 1 have name=":" which is not real
            clean_name = re.sub(r"^[\s:،,.\-]+", "", name)
            clean_kunya = re.sub(r"^[\s:،,.\-]+", "", narrator_data.get("kunya", ""))
            if not clean_name and not clean_kunya:
                print(f"  Empty profile for narrator {narrator_id} (no name, no kunya)")
                return {"success": False, "data": None, "message": "Empty narrator profile"}

            print(f"  OK: narrator {narrator_id} - {name}")
            return {"success": True, "data": narrator_data, "message": ""}

        except requests.exceptions.Timeout:
            print(f"  Request timeout")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            return {"success": False, "data": None,
                    "message": f"Timeout after {max_retries} attempts"}

        except requests.exceptions.RequestException as e:
            print(f"  Connection error: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            return {"success": False, "data": None,
                    "message": f"Connection error: {e}"}

    return {"success": False, "data": None,
            "message": f"Failed after {max_retries} attempts"}


def _get_field_value(div_tag) -> str:
    """Extract the text after the <b> label in a div. e.g. '<div><b>الاسم:</b> بهز بن أسد</div>' -> 'بهز بن أسد'"""
    b_tag = div_tag.find("b")
    if not b_tag:
        return norm(div_tag.get_text())
    # Get all text after the <b> tag
    b_tag.decompose()
    return norm(div_tag.get_text())


def extract_narrator_info(soup: BeautifulSoup, narrator_id: str, url: str) -> Dict:
    """
    Extract narrator information from the parsed HTML.

    Page structure:
    - section > div.container > div.row > div.col-md-12 > div children
    - Each field is a <div> with <b>label:</b> value
    - Jarh wa ta'dil: <h4> header, then pairs of div.alert-info (scholar) + sibling divs (quotes)
    """
    narrator_data = {
        "narrator_id": narrator_id,
        "url": url,
    }

    # Field label mapping: Arabic label -> JSON key
    FIELD_MAP = {
        "الاسم": "name",
        "الكنية": "kunya",
        "النسب": "nasab",
        "علاقات الراوي": "relations",
        "المذهب العقدي": "aqeeda",
        "تاريخ الوفاة": "death_date",
        "تاريخ الميلاد": "birth_date",
        "طبقة رواة التقريب": "tabaqa",
        "الرتبة عند ابن حجر": "rank_ibn_hajar",
        "الرتبة عند الذهبي": "rank_dhahabi",
    }

    # Find the main content container
    container = soup.select_one("section .col-md-12")
    if not container:
        container = soup  # fallback to whole page

    # Extract labeled fields (divs with <b> labels before the jarh section)
    for div in container.find_all("div", recursive=False):
        b_tag = div.find("b")
        if not b_tag:
            continue
        label_text = norm(b_tag.get_text()).rstrip(":")
        for arabic_label, json_key in FIELD_MAP.items():
            if arabic_label in label_text:
                narrator_data[json_key] = _get_field_value(div)
                break

    # Extract jarh wa ta'dil (scholar criticism/authentication)
    jarh_entries = []
    scholar_divs = container.select("div.alert.alert-info")
    for scholar_div in scholar_divs:
        scholar_name = norm(scholar_div.get_text())
        quotes = []
        # Collect sibling divs until next alert-info or h4
        sibling = scholar_div.find_next_sibling()
        while sibling:
            if sibling.name == "div" and "alert-info" in sibling.get("class", []):
                break
            if sibling.name == "h4":
                break
            if sibling.name == "div":
                text = norm(sibling.get_text())
                if text:
                    quotes.append(text)
            sibling = sibling.find_next_sibling()

        if scholar_name:
            jarh_entries.append({
                "scholar": scholar_name,
                "quotes": quotes,
            })

    narrator_data["jarh_wa_tadil"] = jarh_entries

    return narrator_data


# ── JSONL helpers (thread-safe) ──

_file_lock = threading.Lock()
_key_lock = threading.Lock()

def append_jsonl(obj: dict, jsonl_path: Path):
    """Append a single JSON object as a line to a JSONL file (thread-safe)."""
    with _file_lock:
        with open(jsonl_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')


def load_scraped_narrator_ids(jsonl_path: Path) -> Set[str]:
    """Load narrator IDs that were already successfully scraped from JSONL."""
    scraped = set()
    if not jsonl_path.exists():
        return scraped
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") == "success":
                scraped.add(obj["narrator_id"])
    return scraped


def get_failed_narrator_ids(jsonl_path: Path) -> List[str]:
    """Get sorted list of failed narrator IDs that should be retried."""
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
            nid = obj.get("narrator_id")
            if obj.get("status") == "success":
                done.add(nid)
            elif obj.get("status") == "failed":
                failed.add(nid)
    return sorted(failed - done)


def remove_failed_entries(jsonl_path: Path, ids_to_retry: set):
    """Remove old failed entries for narrator IDs that will be retried."""
    if not jsonl_path.exists() or not ids_to_retry:
        return
    kept_lines = []
    removed = 0
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            if obj.get("narrator_id") in ids_to_retry and obj.get("status") == "failed":
                removed += 1
            else:
                kept_lines.append(line.rstrip('\n'))
    if removed > 0:
        with open(jsonl_path, 'w', encoding='utf-8') as f:
            for line in kept_lines:
                f.write(line + '\n')
        print(f"Cleaned {removed} old failed entries for retry")


def extract_narrator_ids_from_hadith_jsonl(hadith_file: Path) -> Set[str]:
    """
    Extract unique narrator IDs from scraped hadith JSONL data.
    Only reads 'success' lines.
    """
    narrator_ids = set()
    if not hadith_file.exists():
        print(f"Hadith file not found: {hadith_file}")
        return narrator_ids

    with open(hadith_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            for block in obj.get("hadith_blocks", []):
                for narrator in block.get("narrators", []):
                    narrator_ids.add(narrator["id"])

    print(f"Found {len(narrator_ids)} unique narrator IDs from hadith data")
    return narrator_ids


def build_hadith_names_map(hadith_file: Path, output_file: Path):
    """
    Build a map of narrator_id -> list of all name variants from hadith data.
    Reads shamela_book_1681.jsonl, writes narrator_hadith_names.json.
    Can run anytime as hadith data grows — no API calls, just file I/O.
    """
    if not hadith_file.exists():
        print(f"Hadith file not found: {hadith_file}")
        return

    narrators = {}  # id -> set of names
    with open(hadith_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("status") != "success":
                continue
            for block in obj.get("hadith_blocks", []):
                for narrator in block.get("narrators", []):
                    nid = narrator["id"]
                    name = narrator.get("name", "").strip().rstrip("،:,")
                    if nid not in narrators:
                        narrators[nid] = set()
                    if name:
                        narrators[nid].add(name)

    # Convert sets to sorted lists for JSON serialization
    result = {nid: sorted(names) for nid, names in narrators.items()}
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(result)} narrator name maps to {output_file}")


# ── Single narrator scrape with key rotation ──

def _scrape_narrator(narrator_id: str, api_keys: List[str], key_state: dict,
                     jsonl_output: Path) -> Optional[dict]:
    """
    Scrape a single narrator. Handles 402 key rotation (thread-safe).
    Returns {"success": bool} or None if all keys exhausted.
    """
    with _key_lock:
        current_key = key_state["key"]

    result = scrape_narrator_with_firecrawl(narrator_id, current_key)

    # 402 = quota exhausted -> rotate key
    if not result["success"] and "402" in result.get("message", ""):
        with _key_lock:
            if key_state["key"] == current_key:
                key_state["index"] += 1
                if key_state["index"] < len(api_keys):
                    key_state["key"] = api_keys[key_state["index"]]
                    print(f"\n  ** API key quota exhausted. Switching to key {key_state['index'] + 1}/{len(api_keys)} **")
                else:
                    print(f"\n  ** All {len(api_keys)} API keys exhausted! Stopping. **")
                    obj = {
                        "status": "failed",
                        "narrator_id": narrator_id,
                        "url": f"https://shamela.ws/narrator/{narrator_id}",
                        "message": "All API keys exhausted (402)",
                    }
                    append_jsonl(obj, jsonl_output)
                    return None
            new_key = key_state["key"]

        if key_state["index"] >= len(api_keys):
            obj = {
                "status": "failed",
                "narrator_id": narrator_id,
                "url": f"https://shamela.ws/narrator/{narrator_id}",
                "message": "All API keys exhausted (402)",
            }
            append_jsonl(obj, jsonl_output)
            return None

        result = scrape_narrator_with_firecrawl(narrator_id, new_key)

    if result["success"]:
        obj = {"status": "success", **result["data"]}
        append_jsonl(obj, jsonl_output)
        return {"success": True}
    else:
        obj = {
            "status": "failed",
            "narrator_id": narrator_id,
            "url": f"https://shamela.ws/narrator/{narrator_id}",
            "message": result["message"],
        }
        append_jsonl(obj, jsonl_output)
        return {"success": False}


# ── Main orchestrator ──

def scrape_narrators(narrator_ids: Set[str], api_keys: List[str], jsonl_output: Path,
                     delay: float = 1.0, max_workers: int = 2):
    """
    Scrape narrator profiles with concurrent requests and API key rotation.
    Phase 1: Retry failed narrators from previous run.
    Phase 2: Scrape new narrators not yet in the JSONL file.
    """
    key_state = {"index": 0, "key": api_keys[0]}
    print(f"Using API key {key_state['index'] + 1}/{len(api_keys)}")
    print(f"Concurrent workers: {max_workers}")

    successful = 0
    failed = 0
    keys_exhausted = False

    # ── Phase 1: Retry failed narrators ──
    failed_ids = get_failed_narrator_ids(jsonl_output)
    if failed_ids:
        print(f"\n{'='*60}")
        print(f"PHASE 1: Retrying {len(failed_ids)} failed narrators")
        print(f"{'='*60}")
        remove_failed_entries(jsonl_output, set(failed_ids))

        for batch_start in range(0, len(failed_ids), max_workers):
            if keys_exhausted:
                break
            group = failed_ids[batch_start:batch_start + max_workers]
            for nid in group:
                print(f"\n[Retry] Narrator {nid}")

            futures = {}
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for nid in group:
                    future = executor.submit(_scrape_narrator, nid, api_keys, key_state, jsonl_output)
                    futures[future] = nid
                for future in as_completed(futures):
                    nid = futures[future]
                    try:
                        outcome = future.result()
                        if outcome is None:
                            keys_exhausted = True
                        elif outcome["success"]:
                            successful += 1
                        else:
                            failed += 1
                    except Exception as e:
                        print(f"  UNEXPECTED ERROR narrator {nid}: {e}")
                        obj = {"status": "failed", "narrator_id": nid,
                               "url": f"https://shamela.ws/narrator/{nid}", "message": str(e)}
                        append_jsonl(obj, jsonl_output)
                        failed += 1

            if not keys_exhausted and batch_start + max_workers < len(failed_ids):
                time.sleep(delay)

        if successful > 0:
            print(f"\nRetry results: {successful} fixed out of {len(failed_ids)}")

    # ── Phase 2: Scrape new narrators ──
    if not keys_exhausted:
        already_done = load_scraped_narrator_ids(jsonl_output)
        to_scrape = sorted(narrator_ids - already_done)

        if to_scrape:
            print(f"\n{'='*60}")
            print(f"PHASE 2: Scraping {len(to_scrape)} new narrators")
            print(f"{'='*60}")

            for batch_start in range(0, len(to_scrape), max_workers):
                if keys_exhausted:
                    break
                group = to_scrape[batch_start:batch_start + max_workers]
                idx = batch_start + 1
                for nid in group:
                    print(f"\n[{idx + group.index(nid)}/{len(to_scrape)}] Narrator {nid}")

                futures = {}
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    for nid in group:
                        future = executor.submit(_scrape_narrator, nid, api_keys, key_state, jsonl_output)
                        futures[future] = nid
                    for future in as_completed(futures):
                        nid = futures[future]
                        try:
                            outcome = future.result()
                            if outcome is None:
                                keys_exhausted = True
                            elif outcome["success"]:
                                successful += 1
                            else:
                                failed += 1
                        except Exception as e:
                            print(f"  UNEXPECTED ERROR narrator {nid}: {e}")
                            obj = {"status": "failed", "narrator_id": nid,
                                   "url": f"https://shamela.ws/narrator/{nid}", "message": str(e)}
                            append_jsonl(obj, jsonl_output)
                            failed += 1

                if not keys_exhausted and batch_start + max_workers < len(to_scrape):
                    time.sleep(delay)
        else:
            print("\nAll narrators already scraped!")

    # Summary
    total_done = len(load_scraped_narrator_ids(jsonl_output))
    print(f"\n{'='*60}")
    print(f"NARRATOR SCRAPING SUMMARY")
    print(f"{'='*60}")
    print(f"Total unique narrator IDs:  {len(narrator_ids)}")
    print(f"Total done (success):       {total_done}")
    print(f"Newly scraped this run:     {successful}")
    print(f"Failed this run:            {failed}")
    print(f"{'='*60}")

    return successful


if __name__ == "__main__":
    # Firecrawl API keys (rotates to next on 402 quota exhausted)
    API_KEYS = [
        "fc-0b84708fea9e49d285060d75c7b72375",
    ]

    # Hadith data to extract narrator IDs from
    HADITH_FILE = Path(__file__).parent / "shamela_book_1681.jsonl"

    # Output JSONL for narrator profiles
    NARRATOR_OUTPUT = Path(__file__).parent / "shamela_narrators.jsonl"

    # Hadith name variants map
    NAMES_OUTPUT = Path(__file__).parent / "narrator_hadith_names.json"

    # Concurrent requests (Firecrawl tier allows 2)
    MAX_WORKERS = 2

    # Delay between batches (seconds)
    DELAY_SECONDS = 3.0

    print("Shamela Narrator Scraper")
    print("=" * 60)

    # Extract narrator IDs from hadith JSONL
    print(f"\nExtracting narrator IDs from: {HADITH_FILE}")
    narrator_ids = extract_narrator_ids_from_hadith_jsonl(HADITH_FILE)

    if not narrator_ids:
        print("No narrator IDs found. Exiting.")
    else:
        try:
            scrape_narrators(
                narrator_ids=narrator_ids,
                api_keys=API_KEYS,
                jsonl_output=NARRATOR_OUTPUT,
                delay=DELAY_SECONDS,
                max_workers=MAX_WORKERS,
            )
            print(f"\nData saved to: {NARRATOR_OUTPUT}")
        except KeyboardInterrupt:
            print("\n\nScraping interrupted by user!")
            print("Data has been saved up to the last successful narrator.")
        except Exception as e:
            print(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()

    # Always build the hadith names map (fast, no API calls)
    print(f"\nBuilding hadith name variants map...")
    build_hadith_names_map(HADITH_FILE, NAMES_OUTPUT)
