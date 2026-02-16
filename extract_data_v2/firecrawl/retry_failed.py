"""
Retry script for pages that failed with reasons other than 'no_narrators'.
Reads shamela_book_1681.jsonl, collects all pages where status=failed and
reason != 'no_narrators', re-scrapes them, and appends results (success or
new failure) back to the same JSONL file after removing the old failed entries.
"""

import json
from pathlib import Path

from shamela_firecrawl import (
    scrape_book_pages,
    get_failed_pages,
    load_scraped_pages,
    remove_failed_entries,
    append_jsonl,
    _scrape_page,
    _file_lock,
    _key_lock,
)


# ── Configuration ────────────────────────────────────────────────────────────

API_KEYS = [
    "fc-bb3459dabca8414b8c92f647cde7ebf3",
    "fc-68d7c10c71b74bb5a52d3e7534f28730",
    "fc-ff5958295ba0497280bc8cc9ca8f5279",
    "fc-a0e6b09c69d5441293d77c29a403ae85",
    "fc-276067bd3ea54fc9b1a944944f5bdc76",
]

BOOK_ID = 1681
DELAY_SECONDS = 3.0
MAX_WORKERS = 2

JSONL_PATH = Path(__file__).parent / f"shamela_book_{BOOK_ID}.jsonl"
DEBUG_DIR = Path(__file__).parent / f"debug_html_{BOOK_ID}_retry"

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_retryable_pages(jsonl_path: Path) -> list[int]:
    """
    Return sorted list of page numbers whose latest entry is
    status=failed AND reason != 'no_narrators'.
    A page is considered done if any entry has status=success or
    reason=no_narrators.
    """
    if not jsonl_path.exists():
        return []

    latest: dict[int, dict] = {}
    done: set[int] = set()

    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            page = obj.get("page_number")
            if page is None:
                continue
            status = obj.get("status", "")
            reason = obj.get("reason", "")

            if status == "success" or reason == "no_narrators":
                done.add(page)
            else:
                latest[page] = obj  # keep last failure record

    retryable = [p for p in latest if p not in done]
    return sorted(retryable)


def remove_retryable_entries(jsonl_path: Path, pages: set[int]):
    """Remove all failed entries for the given pages from the JSONL file."""
    if not jsonl_path.exists() or not pages:
        return
    kept = []
    removed = 0
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            page = obj.get("page_number")
            if page in pages and obj.get("status") == "failed":
                removed += 1
            else:
                kept.append(stripped)

    if removed:
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for line in kept:
                f.write(line + "\n")
        print(f"Removed {removed} old failed entries for retry.")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    retryable = get_retryable_pages(JSONL_PATH)

    if not retryable:
        print("No retryable pages found. All failures are 'no_narrators' (legitimate skips).")
        raise SystemExit(0)

    print(f"Found {len(retryable)} pages to retry: {retryable}")
    print(f"JSONL output: {JSONL_PATH}")
    print(f"API keys: {len(API_KEYS)}")
    print(f"Concurrent workers: {MAX_WORKERS}")
    print(f"Delay between batches: {DELAY_SECONDS}s")

    # Remove old failed entries so we don't accumulate duplicates
    remove_retryable_entries(JSONL_PATH, set(retryable))

    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Debug HTML dir: {DEBUG_DIR}")

    key_state = {"index": 0, "key": API_KEYS[0]}

    success_count = 0
    still_failed = []
    keys_exhausted = False

    total = len(retryable)

    for batch_start in range(0, total, MAX_WORKERS):
        if keys_exhausted:
            break

        group = retryable[batch_start : batch_start + MAX_WORKERS]

        for p in group:
            print(f"\n[{batch_start + group.index(p) + 1}/{total}] Page {p}")

        futures = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for page_num in group:
                future = executor.submit(
                    _scrape_page,
                    BOOK_ID, page_num, API_KEYS, key_state,
                    JSONL_PATH, DEBUG_DIR,
                )
                futures[future] = page_num

            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    outcome = future.result()
                    if outcome is None:
                        keys_exhausted = True
                        print("All API keys exhausted — stopping retry.")
                    elif outcome["success"]:
                        success_count += 1
                        print(f"  Page {page_num}: SUCCESS")
                    else:
                        still_failed.append({"page": page_num, "reason": outcome["reason"].value})
                        print(f"  Page {page_num}: still failed ({outcome['reason'].value})")
                except Exception as e:
                    print(f"  Page {page_num}: unexpected error — {e}")
                    still_failed.append({"page": page_num, "reason": str(e)})

        if not keys_exhausted and batch_start + MAX_WORKERS < total:
            time.sleep(DELAY_SECONDS)

    print(f"\n{'='*60}")
    print("RETRY SUMMARY")
    print(f"{'='*60}")
    print(f"Pages attempted:   {total}")
    print(f"Newly succeeded:   {success_count}")
    print(f"Still failing:     {len(still_failed)}")
    if still_failed:
        print("\nStill-failing pages:")
        for entry in still_failed:
            print(f"  Page {entry['page']}: {entry['reason']}")
    print(f"\nData appended to: {JSONL_PATH}")
