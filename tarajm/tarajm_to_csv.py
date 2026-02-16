#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tarajm People Scraper -> CSV directly (no per-person JSON files)

What it does:
- Reads IDs from:
    1) a text file (one id per line) OR
    2) CLI args: --ids 10109 10433 ...
- Deduplicates + sorts IDs (small -> big)
- For each id:
    - Fetch https://tarajm.com/people/{id}
    - Extract: name, short summary, fields (grouped), translation, page_sections
    - Collect all hrefs inside extracted data
    - Append one row to CSV
    - Update state JSON with scraped/not_scraped + error message (if any)
- Robust:
    - retries with backoff
    - try/except per ID (doesn't stop the run)
    - optional rate limiting

Requirements:
pip install requests beautifulsoup4
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

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
STATE_PATH = os.path.join(OUT_DIR, "tarajm_state.json")
ERROR_LOG = os.path.join(OUT_DIR, "scrape_errors.log")

# CSV columns (stable schema)
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
    "all_hrefs",  # newline-separated
]

os.makedirs(OUT_DIR, exist_ok=True)


# -------------------------
# Small utilities
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
        # utf-8-sig helps Excel open Arabic correctly
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()


def append_row_to_csv(path: str, fieldnames: List[str], row: Dict[str, Any]):
    ensure_csv_header(path, fieldnames)
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow({k: row.get(k, "") for k in fieldnames})


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"people": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # If corrupted, keep a backup and start fresh
        try:
            bak = path + ".bak"
            os.replace(path, bak)
            log_error(f"State file corrupted. Backed up to: {bak}")
        except Exception:
            pass
        return {"people": {}}


def save_state(path: str, state: Dict[str, Any]):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def parse_ids_from_txt(path: str) -> List[int]:
    ids = []
    if not path or not os.path.exists(path):
        return ids
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            # allow "10109," or "ID=10109"
            m = re.search(r"(\d+)", s)
            if m:
                ids.append(int(m.group(1)))
    return ids


def normalize_ids(ids: List[int]) -> List[int]:
    uniq = sorted(set([int(x) for x in ids if str(x).isdigit()]))
    return uniq


def collect_hrefs(obj: Any) -> List[str]:
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


def abs_url(href: str) -> str:
    if not href:
        return href
    return "https://tarajm.com" + href if href.startswith("/") else href


def safe_sleep(seconds: float):
    if seconds and seconds > 0:
        time.sleep(seconds)


# -------------------------
# Extraction helpers (from your script)
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


# -------------------------
# Networking with retries
# -------------------------
def fetch_with_retries(
    session: requests.Session,
    url: str,
    headers: Dict[str, str],
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> Tuple[Optional[requests.Response], Optional[Exception]]:
    """
    Returns (response, error). If error is not None -> failed after retries.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            return resp, None
        except Exception as e:
            last_err = e
            sleep_s = backoff_base * (2 ** (attempt - 1))
            safe_sleep(sleep_s)
    return None, last_err


# -------------------------
# Scrape one ID -> CSV row
# -------------------------
def scrape_one_person(session: requests.Session, person_id: int, headers: Dict[str, str],
                      timeout: int, max_retries: int, backoff_base: float) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns:
      row: for CSV
      meta: for state tracking (status, error, etc.)
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
        meta = {
            "status": "not_scraped",
            "http_status": "",
            "error": f"{type(err).__name__}: {err}",
            "updated_at_utc": utc_now_iso(),
        }
        return {}, meta

    http_status = getattr(resp, "status_code", "")
    if resp is None:
        meta = {
            "status": "not_scraped",
            "http_status": "",
            "error": "No response",
            "updated_at_utc": utc_now_iso(),
        }
        return {}, meta

    if http_status != 200:
        meta = {
            "status": "not_scraped",
            "http_status": http_status,
            "error": f"HTTP {http_status}",
            "updated_at_utc": utc_now_iso(),
        }
        return {}, meta

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

    meta = {
        "status": "scraped",
        "http_status": http_status,
        "error": "",
        "updated_at_utc": utc_now_iso(),
        "hrefs_count": len(uniq_hrefs),
    }

    return row, meta


# -------------------------
# CLI / Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ids-file", default="", help="Path to ids.txt (one id per line)")
    ap.add_argument("--ids", nargs="*", default=[], help="IDs passed directly")
    ap.add_argument("--skip-scraped", action="store_true", help="Skip IDs already scraped in state.json")
    ap.add_argument("--sleep", type=float, default=0.3, help="Sleep seconds between requests")
    ap.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    ap.add_argument("--retries", type=int, default=3, help="Max retries per request")
    ap.add_argument("--backoff", type=float, default=0.6, help="Backoff base seconds for retries")
    ap.add_argument("--state", default=STATE_PATH, help="State JSON path")
    ap.add_argument("--csv", default=CSV_PATH, help="Output CSV path")
    args = ap.parse_args()

    # Collect IDs
    ids_from_file = parse_ids_from_txt(args.ids_file)
    ids_from_cli = []
    for x in args.ids:
        try:
            ids_from_cli.append(int(x))
        except Exception:
            pass

    ids = normalize_ids(ids_from_file + ids_from_cli)
    if not ids:
        print("No IDs provided. Use --ids-file or --ids")
        sys.exit(1)

    # Load state
    state = load_state(args.state)
    state.setdefault("people", {})

    # Session
    session = requests.Session()

    total = len(ids)
    ok = 0
    fail = 0
    skipped = 0

    ensure_csv_header(args.csv, CSV_FIELDNAMES)

    for idx, pid in enumerate(ids, start=1):
        pid_str = str(pid)

        # Skip already scraped if requested
        if args.skip_scraped:
            existing = state["people"].get(pid_str, {})
            if existing.get("status") == "scraped":
                skipped += 1
                print(f"[{idx}/{total}] ID={pid} SKIP (already scraped)")
                continue

        try:
            row, meta = scrape_one_person(
                session=session,
                person_id=pid,
                headers=DEFAULT_HEADERS,
                timeout=args.timeout,
                max_retries=args.retries,
                backoff_base=args.backoff,
            )

            # Update state
            state["people"][pid_str] = {
                "id": pid,
                "url": BASE_URL.format(id=pid),
                **meta,
            }
            save_state(args.state, state)

            if meta.get("status") == "scraped":
                append_row_to_csv(args.csv, CSV_FIELDNAMES, row)
                ok += 1
                print(f"[{idx}/{total}] ID={pid} ✅ scraped | name={row.get('name','')}")
            else:
                fail += 1
                msg = meta.get("error", "unknown error")
                log_error(f"ID={pid} url={BASE_URL.format(id=pid)} | {msg}")
                print(f"[{idx}/{total}] ID={pid} ❌ not scraped | {msg}")

        except Exception as e:
            fail += 1
            msg = f"{type(e).__name__}: {e}"
            # Update state even on unexpected crash per-id
            state["people"][pid_str] = {
                "id": pid,
                "url": BASE_URL.format(id=pid),
                "status": "not_scraped",
                "http_status": "",
                "error": msg,
                "updated_at_utc": utc_now_iso(),
            }
            save_state(args.state, state)
            log_error(f"ID={pid} url={BASE_URL.format(id=pid)} | {msg}")
            print(f"[{idx}/{total}] ID={pid} ❌ exception | {msg}")

        # Rate limit
        safe_sleep(args.sleep)

    print("\nDone.")
    print(f"Total: {total} | OK: {ok} | Failed: {fail} | Skipped: {skipped}")
    print(f"CSV: {args.csv}")
    print(f"State: {args.state}")
    print(f"Errors: {ERROR_LOG}")


if __name__ == "__main__":
    main()
