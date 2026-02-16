#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Tarajm People Crawler

What it does:
1. Reads seed IDs from ids.txt
2. Scrapes each ID and saves to CSV
3. Extracts new people IDs from scraped data (fields_json hrefs)
4. Adds new IDs to queue (if not already scraped)
5. Loops until no new IDs are discovered

State tracking via ids_status.json:
- Tracks which IDs are scraped/pending/failed
- Prevents re-scraping
- Records discovery chain
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Set, List, Dict, Any, Optional, Tuple
from collections import deque

import requests
from bs4 import BeautifulSoup, Tag

# -------------------------
# Config
# -------------------------
BASE_URL = "https://tarajm.com/people/{id}"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
    )
}

OUT_DIR = "out_people_csv"
CSV_PATH = os.path.join(OUT_DIR, "tarajm_people.csv")
STATE_PATH = "ids_status.json"
ERROR_LOG = os.path.join(OUT_DIR, "scrape_errors.log")

CSV_FIELDNAMES = [
    "id",
    "url",
    "http_status",
    "scraped_at_utc",
    "name",
    "summary",
    "translation",
    "fields_json",
    "page_sections_json",
    "all_hrefs",
]

os.makedirs(OUT_DIR, exist_ok=True)

# Regex to extract people IDs from hrefs
PEOPLE_ID_RE = re.compile(r"/people/(\d+)")


# -------------------------
# Utilities
# -------------------------
def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log_error(msg: str):
    ts = utc_now_iso()
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")


def to_compact_json_str(x) -> str:
    try:
        return json.dumps(x, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return ""


def ensure_csv_header(path: str, fieldnames: List[str]):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()


def append_row_to_csv(path: str, fieldnames: List[str], row: Dict[str, Any]):
    ensure_csv_header(path, fieldnames)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow({k: row.get(k, "") for k in fieldnames})


def parse_ids_from_txt(path: str) -> List[int]:
    ids = []
    if not path or not os.path.exists(path):
        return ids
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            m = re.search(r"(\d+)", s)
            if m:
                ids.append(int(m.group(1)))
    return ids


def extract_people_ids_from_text(text: str) -> Set[int]:
    """Extract all people IDs from a text containing hrefs"""
    ids = set()
    for match in PEOPLE_ID_RE.finditer(text):
        try:
            ids.add(int(match.group(1)))
        except ValueError:
            pass
    return ids


def safe_sleep(seconds: float):
    if seconds and seconds > 0:
        time.sleep(seconds)


# -------------------------
# State Management
# -------------------------
def load_state(path: str) -> Dict[int, Dict[str, Any]]:
    """
    Load state from JSON array format.
    Returns dict: {id: status_record}
    """
    if not os.path.exists(path):
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Convert list to dict keyed by id
        if isinstance(data, list):
            return {int(record["id"]): record for record in data}
        return {}
    except Exception as e:
        log_error(f"Failed to load state: {e}")
        return {}


def save_state(path: str, state: Dict[int, Dict[str, Any]]):
    """Save state as JSON array (sorted by id)"""
    # Convert dict to sorted list
    state_list = [state[k] for k in sorted(state.keys())]

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state_list, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def update_state_record(
    state: Dict[int, Dict[str, Any]],
    person_id: int,
    status: str,
    http_status: Optional[int] = None,
    error: Optional[str] = None,
    discovered_from: str = "seed"
):
    """Update or create a state record for a person ID"""
    if person_id not in state:
        state[person_id] = {
            "id": person_id,
            "scraped": False,
            "status": "pending",
            "http_status": None,
            "output_file": None,
            "last_attempt": None,
            "attempts": 0,
            "discovered_from": discovered_from,
        }

    record = state[person_id]
    record["status"] = status
    record["last_attempt"] = utc_now_iso()
    record["attempts"] = record.get("attempts", 0) + 1

    if http_status is not None:
        record["http_status"] = http_status

    if status == "scraped":
        record["scraped"] = True
        record["output_file"] = CSV_PATH
    elif status == "failed":
        record["scraped"] = False
        record["error"] = error or "Unknown error"

    return record


# -------------------------
# Extraction helpers (from tarajm_to_csv.py)
# -------------------------
def clean_key(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("：", ":")
    s = re.sub(r"\s+", " ", s)
    return s[:-1].strip() if s.endswith(":") else s


def text_clean(el: Optional[Tag]) -> str:
    if not el:
        return ""
    txt = el.get_text(" ", strip=True)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def dedup_links(links: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for x in links:
        t = (x.get("text") or "").strip()
        h = (x.get("href") or "").strip()
        if not t or not h:
            continue
        key = (t, h)
        if key in seen:
            continue
        seen.add(key)
        out.append({"text": t, "href": h})
    return out


def dedup_paragraphs(txt: str) -> str:
    lines = [l.strip() for l in txt.split("\n") if l.strip()]
    out = []
    for l in lines:
        if not out or out[-1] != l:
            out.append(l)
    return "\n".join(out)


def abs_url(href: str) -> str:
    if not href:
        return href
    return "https://tarajm.com" + href if href.startswith("/") else href


def extract_links(container: Tag) -> List[Dict[str, str]]:
    links: List[Dict[str, str]] = []
    if not container:
        return links
    for a in container.find_all("a", href=True):
        t = a.get_text(strip=True)
        href = abs_url(a.get("href", ""))
        if t and href:
            links.append({"text": t, "href": href})
    return dedup_links(links)


def extract_value_from_row(row: Tag, label_span: Tag) -> Dict[str, Any]:
    value_container = None

    sib = label_span.find_next_sibling()
    if isinstance(sib, Tag):
        value_container = sib

    if value_container is None:
        value_container = row

    links = extract_links(value_container)
    if links:
        raw_txt = text_clean(value_container)
        raw_txt = re.sub(r"عرض الكل\s*\(\d+\)", "", raw_txt).strip()
        return {"type": "links", "items": links, "text": raw_txt}

    val_txt = text_clean(value_container)
    val_txt = re.sub(r"عرض الكل\s*\(\d+\)", "", val_txt).strip()
    return {"type": "text", "text": val_txt}


def get_main_container(soup: BeautifulSoup) -> Tag:
    main = soup.find("main")
    if main:
        return main
    root = soup.select_one("#__next")
    return root if root else soup


def find_person_summary_section(root: Tag) -> Optional[Tag]:
    sec = root.select_one('section[aria-labelledby="person-summary-heading"]')
    if sec:
        return sec

    h2 = root.find(
        lambda t: isinstance(t, Tag)
        and t.name in ("h2", "h3")
        and text_clean(t) == "ملخص الشخصية"
    )
    if not h2:
        return None
    return h2.find_parent("section") or h2.parent


def find_translation_section(root: Tag) -> Optional[Tag]:
    h2 = root.find(
        lambda t: isinstance(t, Tag) and t.name == "h2" and text_clean(t) == "الترجمة"
    )
    if not h2:
        return None
    return h2.find_parent("section") or h2.parent


def extract_summary_fields(summary_section: Tag) -> Dict[str, Any]:
    if not summary_section:
        return {}

    result: Dict[str, Any] = {}

    h3s = summary_section.find_all("h3")
    if not h3s:
        result.setdefault("general", {})
        for sp in summary_section.find_all("span"):
            label = text_clean(sp)
            if not label.endswith(":"):
                continue
            key = clean_key(label)
            row = sp.find_parent("div") or sp.find_parent("li") or sp.parent
            if not row:
                continue
            val = extract_value_from_row(row, sp)
            result["general"][key] = val
        return {k: v for k, v in result.items() if v}

    for idx, h3 in enumerate(h3s):
        group_name = text_clean(h3) or f"group_{idx+1}"
        bucket: Dict[str, Any] = {}

        nodes: List[Tag] = []
        for sib in h3.next_siblings:
            if isinstance(sib, Tag) and sib.name == "h3":
                break
            if isinstance(sib, Tag):
                nodes.append(sib)

        for node in nodes:
            for sp in node.find_all("span"):
                label = text_clean(sp)
                if not label.endswith(":"):
                    continue
                key = clean_key(label)

                row = sp.find_parent("div") or sp.find_parent("li") or sp.parent
                if not row:
                    continue

                value = extract_value_from_row(row, sp)

                if key in bucket:
                    if not isinstance(bucket[key], list):
                        bucket[key] = [bucket[key]]
                    bucket[key].append(value)
                else:
                    bucket[key] = value

        if not bucket:
            all_links: List[Dict[str, str]] = []
            for node in nodes:
                all_links.extend(extract_links(node))
            all_links = dedup_links(all_links)
            if all_links:
                bucket["items"] = {"type": "links", "items": all_links}

        if bucket:
            result[group_name] = bucket

    return {g: v for g, v in result.items() if v}


def extract_translation(root: Tag) -> str:
    section = find_translation_section(root)
    if not section:
        return ""

    prose_blocks = section.select(".print\\:hidden .prose")
    if not prose_blocks:
        prose_blocks = section.select(".prose")

    texts: List[str] = []
    for pb in prose_blocks:
        t = pb.get_text("\n", strip=True)
        t = dedup_paragraphs(t)
        if t:
            texts.append(t)

    uniq: List[str] = []
    seen = set()
    for t in texts:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)

    return "\n\n".join(uniq).strip()


def extract_page_sections(root: Tag) -> List[str]:
    titles: List[str] = []
    for h in root.find_all(["h1", "h2", "h3"]):
        t = text_clean(h)
        if t:
            titles.append(t)
    seen = set()
    out: List[str] = []
    for t in titles:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def collect_hrefs(obj: Any) -> List[str]:
    """Recursively collect all href values from a nested structure"""
    hrefs: List[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "href" and isinstance(v, str):
                hrefs.append(v)
            else:
                hrefs.extend(collect_hrefs(v))
    elif isinstance(obj, list):
        for x in obj:
            hrefs.extend(collect_hrefs(x))
    return hrefs


# -------------------------
# Scraping
# -------------------------
def fetch_with_retries(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> Tuple[Optional[requests.Response], Optional[Exception]]:
    """Returns (response, error)"""
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            return resp, None
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                safe_sleep(sleep_s)
    return None, last_err


def scrape_one_person(
    session: requests.Session,
    person_id: int,
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[int]]:
    """
    Returns:
      (row_data, error_msg, http_status)
    """
    url = BASE_URL.format(id=person_id)

    resp, err = fetch_with_retries(
        session=session,
        url=url,
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        backoff_base=backoff_base,
    )

    if err is not None:
        return None, f"{type(err).__name__}: {err}", None

    if resp is None:
        return None, "No response", None

    http_status = resp.status_code
    if http_status != 200:
        return None, f"HTTP {http_status}", http_status

    # Parse HTML
    soup = BeautifulSoup(resp.text, "html.parser")
    root = get_main_container(soup)

    h1 = root.find("h1") or soup.find("h1")
    name = text_clean(h1)

    short_summary = ""
    if h1:
        p = h1.find_next("p")
        short_summary = text_clean(p)

    summary_sec = find_person_summary_section(root) or find_person_summary_section(soup)
    fields = extract_summary_fields(summary_sec) if summary_sec else {}

    translation = extract_translation(root) or extract_translation(soup)
    page_sections = extract_page_sections(root)

    # Collect all hrefs
    data = {
        "url": url,
        "name": name,
        "summary": short_summary,
        "fields": fields,
        "translation": translation,
        "page_sections": page_sections,
    }

    hrefs = collect_hrefs(data)
    uniq_hrefs = sorted(set([h.strip() for h in hrefs if isinstance(h, str) and h.strip()]))

    row = {
        "id": str(person_id),
        "url": url,
        "http_status": http_status,
        "scraped_at_utc": utc_now_iso(),
        "name": name,
        "summary": short_summary,
        "translation": translation,
        "fields_json": to_compact_json_str(fields),
        "page_sections_json": to_compact_json_str(page_sections),
        "all_hrefs": "\n".join(uniq_hrefs),
    }

    return row, None, http_status


# -------------------------
# Main Crawler Logic
# -------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Unified Tarajm crawler with recursive ID discovery"
    )
    ap.add_argument(
        "--seed-file",
        default="ids.txt",
        help="Path to seed IDs file (one id per line)",
    )
    ap.add_argument(
        "--state",
        default=STATE_PATH,
        help="State JSON path (ids_status.json)",
    )
    ap.add_argument(
        "--csv",
        default=CSV_PATH,
        help="Output CSV path",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.3,
        help="Sleep seconds between requests",
    )
    ap.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Max retries per request",
    )
    ap.add_argument(
        "--backoff",
        type=float,
        default=0.6,
        help="Backoff base seconds for retries",
    )
    ap.add_argument(
        "--max-ids",
        type=int,
        default=0,
        help="Maximum IDs to scrape (0 = unlimited)",
    )
    args = ap.parse_args()

    # Load state
    print(f"Loading state from {args.state}...")
    state = load_state(args.state)
    print(f"  Loaded {len(state)} existing records")

    # Load seed IDs
    print(f"\nLoading seed IDs from {args.seed_file}...")
    seed_ids = parse_ids_from_txt(args.seed_file)
    print(f"  Found {len(seed_ids)} seed IDs")

    # Initialize queue with seed IDs (only those not already scraped)
    queue = deque()
    seed_added = 0

    for sid in seed_ids:
        if sid not in state:
            state[sid] = {
                "id": sid,
                "scraped": False,
                "status": "pending",
                "http_status": None,
                "output_file": None,
                "last_attempt": None,
                "attempts": 0,
                "discovered_from": "seed_file",
            }
            queue.append(sid)
            seed_added += 1
        elif not state[sid].get("scraped", False):
            queue.append(sid)
            seed_added += 1

    # Add all pending IDs from state (discovered in previous runs)
    pending_added = 0
    for person_id, record in state.items():
        if not record.get("scraped", False) and person_id not in queue:
            queue.append(person_id)
            pending_added += 1

    print(f"  {seed_added} seed IDs queued")
    print(f"  {pending_added} pending IDs from previous runs")
    print(f"  {len(queue)} total IDs queued for scraping")

    # Stats
    total_scraped = 0
    total_failed = 0
    total_discovered = 0
    session = requests.Session()

    ensure_csv_header(args.csv, CSV_FIELDNAMES)

    print(f"\nStarting crawl...\n")

    while queue:
        if args.max_ids > 0 and total_scraped >= args.max_ids:
            print(f"\n⚠️  Reached max IDs limit ({args.max_ids})")
            break

        person_id = queue.popleft()

        # Skip if already scraped
        if state.get(person_id, {}).get("scraped", False):
            continue

        print(f"[Queue: {len(queue)}] Scraping ID={person_id}...", end=" ")

        try:
            row, error, http_status = scrape_one_person(
                session=session,
                person_id=person_id,
                headers=DEFAULT_HEADERS,
                timeout=args.timeout,
                max_retries=args.retries,
                backoff_base=args.backoff,
            )

            if error:
                # Failed
                update_state_record(
                    state,
                    person_id,
                    status="failed",
                    http_status=http_status,
                    error=error,
                )
                save_state(args.state, state)
                log_error(f"ID={person_id} url={BASE_URL.format(id=person_id)} | {error}")
                print(f"❌ {error}")
                total_failed += 1
            else:
                # Success
                update_state_record(
                    state,
                    person_id,
                    status="scraped",
                    http_status=http_status,
                )
                save_state(args.state, state)
                append_row_to_csv(args.csv, CSV_FIELDNAMES, row)

                print(f"✅ {row.get('name', 'Unknown')}")
                total_scraped += 1

                # Extract new people IDs from fields_json and all_hrefs
                fields_json = row.get("fields_json", "")
                all_hrefs = row.get("all_hrefs", "")
                combined_text = fields_json + "\n" + all_hrefs

                new_ids = extract_people_ids_from_text(combined_text)
                added_count = 0

                for new_id in new_ids:
                    if new_id not in state:
                        state[new_id] = {
                            "id": new_id,
                            "scraped": False,
                            "status": "pending",
                            "http_status": None,
                            "output_file": None,
                            "last_attempt": None,
                            "attempts": 0,
                            "discovered_from": f"person_{person_id}",
                        }
                        queue.append(new_id)
                        added_count += 1
                        total_discovered += 1

                if added_count > 0:
                    print(f"   └─ Discovered {added_count} new IDs")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            update_state_record(
                state,
                person_id,
                status="failed",
                error=error_msg,
            )
            save_state(args.state, state)
            log_error(f"ID={person_id} url={BASE_URL.format(id=person_id)} | {error_msg}")
            print(f"❌ Exception: {error_msg}")
            total_failed += 1

        # Rate limit
        safe_sleep(args.sleep)

    # Final summary
    print("\n" + "=" * 60)
    print("Crawl Complete!")
    print("=" * 60)
    print(f"Total scraped:    {total_scraped}")
    print(f"Total failed:     {total_failed}")
    print(f"Total discovered: {total_discovered}")
    print(f"Total in state:   {len(state)}")
    print(f"\nOutput CSV:   {args.csv}")
    print(f"State file:   {args.state}")
    print(f"Error log:    {ERROR_LOG}")


if __name__ == "__main__":
    main()
