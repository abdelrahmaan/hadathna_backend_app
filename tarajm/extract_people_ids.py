#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import sys

CSV_PATH = "out_people_csv/tarajm_people.csv"
OUT_PATH = "people_ids_extracted.txt"

HREF_COLS = ["href", "url", "link", "all_hrefs"]

# ✅ only people ids
PEOPLE_RE = re.compile(r"/people/(\d+)")


def increase_csv_field_limit():
    # Python's default CSV field size is too small for very large JSON/text cells.
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            break
        except OverflowError:
            limit //= 10


def extract_people_id(href: str):
    if not href:
        return None
    m = PEOPLE_RE.search(href)
    return m.group(1) if m else None


def main():
    increase_csv_field_limit()
    ids = set()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # detect column
        col = None
        for c in HREF_COLS:
            if c in reader.fieldnames:
                col = c
                break

        if not col:
            raise ValueError(f"No href column found. Columns: {reader.fieldnames}")

        for row in reader:
            cell = (row.get(col) or "").strip()
            if not cell:
                continue

            # case: JSON list of hrefs
            if col == "all_hrefs" and cell.startswith("["):
                try:
                    arr = json.loads(cell)
                    for href in arr:
                        pid = extract_people_id(href)
                        if pid:
                            ids.add(pid)
                    continue
                except:
                    pass

            # case: single href
            pid = extract_people_id(cell)
            if pid:
                ids.add(pid)

    # sort numeric
    ids_sorted = sorted(ids, key=int)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for x in ids_sorted:
            f.write(x + "\n")

    print("Done ✅")
    print("count:", len(ids_sorted))
    print("saved to:", OUT_PATH)


if __name__ == "__main__":
    main()
