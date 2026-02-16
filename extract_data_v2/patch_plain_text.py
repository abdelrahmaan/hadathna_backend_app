"""
patch_plain_text.py — Add full_text_plain and matn_plain to existing Hadith nodes.

Run this once against a live graph that was ingested before these properties existed.
Reads hadith text directly from the JSONL source, strips tashkeel, and updates Neo4j.

Usage:
    python extract_data_v2/patch_plain_text.py
    python extract_data_v2/patch_plain_text.py --dry-run
    python extract_data_v2/patch_plain_text.py --batch-size 500
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

from neo4j import GraphDatabase

_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)


def strip_tashkeel(text: str) -> str:
    return _TASHKEEL_RE.sub("", text)


def load_plain_texts(jsonl_path: str) -> dict:
    """Read JSONL and return {hadith_id: {full_text_plain, matn_plain}}."""
    records = {}
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("status") != "success":
                continue
            book_id = int(r["book_id"])
            page_number = int(r["page_number"])
            hadith_id = f"{book_id}_{page_number}"
            for block in r.get("hadith_blocks", []):
                full_text = block.get("full_text") or ""
                matn = block.get("matn") or ""
                records[hadith_id] = {
                    "hadith_id": hadith_id,
                    "full_text_plain": strip_tashkeel(full_text),
                    "matn_plain": strip_tashkeel(matn),
                }
                break  # one hadith per page
    return records


def patch(driver, records: list[dict], batch_size: int) -> int:
    query = """
    UNWIND $batch AS row
    MATCH (h:Hadith {hadith_id: row.hadith_id})
    SET h.full_text_plain = row.full_text_plain,
        h.matn_plain = row.matn_plain
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(records), batch_size):
            chunk = records[i:i + batch_size]
            result = session.run(query, batch=chunk)
            summary = result.consume()
            total += summary.counters.properties_set
            print(f"  Patched {min(i + batch_size, len(records))}/{len(records)} hadiths...", end="\r")
    print()
    return total


def create_fulltext_index(driver) -> None:
    with driver.session() as session:
        session.run(
            "CREATE FULLTEXT INDEX hadith_plain_text_ft IF NOT EXISTS "
            "FOR (h:Hadith) ON EACH [h.matn_plain, h.full_text_plain]"
        )
    print("  Full-text index 'hadith_plain_text_ft' ready.")


def main():
    parser = argparse.ArgumentParser(description="Patch Hadith nodes with plain-text (tashkeel-free) fields.")
    parser.add_argument("--hadith", default="extract_data_v2/firecrawl/shamela_book_1681.jsonl")
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "password"))
    parser.add_argument("--batch-size", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Loading hadith texts from {args.hadith}...")
    records_map = load_plain_texts(args.hadith)
    records = list(records_map.values())
    print(f"  Loaded {len(records)} hadiths.")

    # Quick sanity check
    sample = records[0]
    print(f"\nSample (hadith_id={sample['hadith_id']}):")
    print(f"  matn_plain: {sample['matn_plain'][:120]}")

    if args.dry_run:
        print("\nDry run — no writes.")
        return

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_password))
    try:
        print(f"\nPatching {len(records)} Hadith nodes (batch_size={args.batch_size})...")
        props_set = patch(driver, records, args.batch_size)
        print(f"  Done — {props_set} properties set.")

        print("Creating full-text index...")
        create_fulltext_index(driver)
    finally:
        driver.close()

    print("\nPatch complete.")
    print("Test query in Neo4j Browser:")
    print("  CALL db.index.fulltext.queryNodes('hadith_plain_text_ft', 'النية') YIELD node, score")
    print("  RETURN node.hadith_id, node.matn_plain, score ORDER BY score DESC LIMIT 5")


if __name__ == "__main__":
    main()
