"""
build_graph.py — Ingest Shamela Bukhari data into Neo4j

Reads:
  shamela_book_1681.jsonl     → Hadith, Book, Chapter nodes + chains
  shamela_narrators.jsonl     → Narrator biographical properties
  narrator_hadith_names.json  → Narrator name variants (original_names)

Outputs:
  Neo4j graph (bolt)
  schema_description.md (for chatbot system prompt)

Schema:
  (:Book {book_id, section_id, name})
  (:Chapter {section_id, book_id, name})
  (:Hadith {hadith_id, page_number, book_id, full_text, matn,
            full_text_plain, matn_plain})
  (:Narrator {narrator_id, name, kunya, nasab, tabaqa,
              rank_ibn_hajar, rank_dhahabi, death_date, birth_date,
              aqeeda, original_names, jarh_wa_tadil_json})

  (Hadith)-[:IN_CHAPTER]->(Chapter)
  (Chapter)-[:IN_BOOK]->(Book)
  (Narrator)-[:NARRATED {position, hadith_id}]->(Narrator)
  (Narrator)-[:TRANSMITTED_HADITH {position}]->(Hadith)
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Generator, Optional

# Load .env from project root (two levels up from this file)
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(_env_path)
except ImportError:
    pass  # python-dotenv not installed; rely on env vars being set manually

try:
    from neo4j import GraphDatabase
    _NEO4J_AVAILABLE = True
except ImportError:
    _NEO4J_AVAILABLE = False

# Arabic diacritic (tashkeel) Unicode ranges to strip for plain-text search
_TASHKEEL_RE = re.compile(
    r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7\u06E8\u06EA-\u06ED]"
)


def strip_tashkeel(text: str) -> str:
    """Remove Arabic diacritical marks (tashkeel) from text."""
    return _TASHKEEL_RE.sub("", text)


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def load_bio(path: str) -> dict:
    """Load narrator biographies from shamela_narrators.jsonl.

    Returns dict keyed by narrator_id (str) → bio fields dict.
    """
    bio = {}
    with open(path, encoding="utf-8") as f:
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
            nid = str(r["narrator_id"])
            bio[nid] = {
                "name": (r.get("name") or "").lstrip(": ").strip(),
                "kunya": r.get("kunya") or None,
                "nasab": r.get("nasab") or None,
                "tabaqa": r.get("tabaqa") or None,
                "rank_ibn_hajar": r.get("rank_ibn_hajar") or None,
                "rank_dhahabi": r.get("rank_dhahabi") or None,
                "death_date": r.get("death_date") or None,
                "birth_date": r.get("birth_date") or None,
                "aqeeda": r.get("aqeeda") or None,
                "relations": r.get("relations") or None,
                # Store jarh_wa_tadil as JSON string (not queryable via Cypher directly)
                "jarh_wa_tadil_json": json.dumps(r.get("jarh_wa_tadil") or [], ensure_ascii=False),
            }
    return bio


def load_name_variants(path: str) -> dict:
    """Load narrator name variants from narrator_hadith_names.json.

    Returns dict keyed by narrator_id (str) → list of name strings.
    """
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return {str(k): v for k, v in raw.items()}


def extract_section_id(href: str) -> Optional[int]:
    """Extract the section/page number from a shamela URL.

    e.g. 'https://shamela.ws/book/1681/10961' → 10961
    """
    if not href:
        return None
    m = re.search(r"/book/\d+/(\d+)", href)
    if m:
        return int(m.group(1))
    return None


def parse_hadith_pages(path: str) -> Generator[dict, None, None]:
    """Stream shamela_book_1681.jsonl, yielding one dict per successful hadith.

    Yields:
        hadith_id      str   "{book_id}_{page_number}"
        page_number    int
        book_id        int
        full_text      str
        matn           str
        book_name      str
        book_section_id int | None
        chapter_name   str | None
        chapter_section_id int | None
        narrators      list[{id: str, name: str}]
    """
    with open(path, encoding="utf-8") as f:
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
            breadcrumbs = r.get("breadcrumb_links", [])

            # breadcrumb[0] is always "فهرس الكتاب" (index link) — skip
            # breadcrumb[1] = Book level
            # breadcrumb[2] = Chapter level (may be absent)
            book_name = breadcrumbs[1]["text"] if len(breadcrumbs) > 1 else ""
            book_section_id = extract_section_id(breadcrumbs[1].get("href", "")) if len(breadcrumbs) > 1 else None

            chapter_name = None
            chapter_section_id = None
            if len(breadcrumbs) > 2:
                chapter_name = breadcrumbs[2]["text"]
                chapter_section_id = extract_section_id(breadcrumbs[2].get("href", ""))

            for block in r.get("hadith_blocks", []):
                full_text = block.get("full_text") or ""
                matn = block.get("matn") or ""
                yield {
                    "hadith_id": f"{book_id}_{page_number}",
                    "page_number": page_number,
                    "book_id": book_id,
                    "full_text": full_text,
                    "matn": matn,
                    "full_text_plain": strip_tashkeel(full_text),
                    "matn_plain": strip_tashkeel(matn),
                    "book_name": book_name,
                    "book_section_id": book_section_id,
                    "chapter_name": chapter_name,
                    "chapter_section_id": chapter_section_id,
                    "narrators": block.get("narrators") or [],
                }


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def create_constraints(driver) -> None:
    """Create uniqueness constraints and indexes."""
    queries = [
        "CREATE CONSTRAINT book_section_id_unique IF NOT EXISTS FOR (b:Book) REQUIRE b.section_id IS UNIQUE",
        "CREATE CONSTRAINT chapter_section_id_unique IF NOT EXISTS FOR (c:Chapter) REQUIRE c.section_id IS UNIQUE",
        "CREATE CONSTRAINT hadith_id_unique IF NOT EXISTS FOR (h:Hadith) REQUIRE h.hadith_id IS UNIQUE",
        "CREATE CONSTRAINT narrator_id_unique IF NOT EXISTS FOR (n:Narrator) REQUIRE n.narrator_id IS UNIQUE",
        "CREATE INDEX hadith_book_id_idx IF NOT EXISTS FOR (h:Hadith) ON (h.book_id)",
        "CREATE INDEX narrator_name_idx IF NOT EXISTS FOR (n:Narrator) ON (n.name)",
        "CREATE INDEX narrator_tabaqa_idx IF NOT EXISTS FOR (n:Narrator) ON (n.tabaqa)",
        "CREATE INDEX narrator_rank_ibn_hajar_idx IF NOT EXISTS FOR (n:Narrator) ON (n.rank_ibn_hajar)",
        # Full-text index for tashkeel-free search across hadith text
        "CREATE FULLTEXT INDEX hadith_plain_text_ft IF NOT EXISTS FOR (h:Hadith) ON EACH [h.matn_plain, h.full_text_plain]",
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)
    print("  Constraints and indexes created.")


def _run_batch(session, query: str, batch: list) -> int:
    result = session.run(query, batch=batch)
    summary = result.consume()
    return summary.counters.nodes_created + summary.counters.relationships_created


def ingest_books(driver, records: list[dict], batch_size: int = 200) -> int:
    """MERGE Book nodes."""
    # Deduplicate by section_id
    seen = {}
    for r in records:
        sid = r["book_section_id"]
        if sid is None:
            continue
        if sid not in seen:
            seen[sid] = {"section_id": sid, "book_id": r["book_id"], "name": r["book_name"]}

    unique_books = list(seen.values())
    query = """
    UNWIND $batch AS row
    MERGE (b:Book {section_id: row.section_id})
    SET b.book_id = row.book_id, b.name = row.name
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(unique_books), batch_size):
            chunk = unique_books[i:i + batch_size]
            total += _run_batch(session, query, chunk)
    return total


def ingest_chapters(driver, records: list[dict], batch_size: int = 200) -> int:
    """MERGE Chapter nodes and IN_BOOK relationships."""
    seen = {}
    for r in records:
        sid = r["chapter_section_id"]
        if sid is None:
            continue
        if sid not in seen:
            seen[sid] = {
                "section_id": sid,
                "book_id": r["book_id"],
                "book_section_id": r["book_section_id"],
                "name": r["chapter_name"] or "",
            }

    unique_chapters = list(seen.values())
    query = """
    UNWIND $batch AS row
    MERGE (c:Chapter {section_id: row.section_id})
    SET c.book_id = row.book_id, c.name = row.name
    WITH c, row
    MATCH (b:Book {section_id: row.book_section_id})
    MERGE (c)-[:IN_BOOK]->(b)
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(unique_chapters), batch_size):
            chunk = unique_chapters[i:i + batch_size]
            total += _run_batch(session, query, chunk)
    return total


def ingest_hadiths(driver, records: list[dict], batch_size: int = 200) -> int:
    """MERGE Hadith nodes and IN_CHAPTER relationships."""
    query_with_chapter = """
    UNWIND $batch AS row
    MERGE (h:Hadith {hadith_id: row.hadith_id})
    SET h.page_number = row.page_number,
        h.book_id = row.book_id,
        h.full_text = row.full_text,
        h.matn = row.matn,
        h.full_text_plain = row.full_text_plain,
        h.matn_plain = row.matn_plain
    WITH h, row
    MATCH (c:Chapter {section_id: row.chapter_section_id})
    MERGE (h)-[:IN_CHAPTER]->(c)
    """
    query_no_chapter = """
    UNWIND $batch AS row
    MERGE (h:Hadith {hadith_id: row.hadith_id})
    SET h.page_number = row.page_number,
        h.book_id = row.book_id,
        h.full_text = row.full_text,
        h.matn = row.matn,
        h.full_text_plain = row.full_text_plain,
        h.matn_plain = row.matn_plain
    """
    with_ch = [r for r in records if r["chapter_section_id"] is not None]
    without_ch = [r for r in records if r["chapter_section_id"] is None]

    total = 0
    with driver.session() as session:
        for i in range(0, len(with_ch), batch_size):
            total += _run_batch(session, query_with_chapter, with_ch[i:i + batch_size])
        for i in range(0, len(without_ch), batch_size):
            total += _run_batch(session, query_no_chapter, without_ch[i:i + batch_size])
    return total


def ingest_narrators(
    driver,
    narrator_ids: set[str],
    bio: dict,
    name_variants: dict,
    batch_size: int = 200,
) -> int:
    """MERGE Narrator nodes with biographical properties and name variants."""
    narrator_list = []
    for nid in narrator_ids:
        b = bio.get(nid, {})
        variants = name_variants.get(nid, [])
        narrator_list.append({
            "narrator_id": int(nid),
            "name": b.get("name") or variants[0] if variants else f"narrator_{nid}",
            "kunya": b.get("kunya"),
            "nasab": b.get("nasab"),
            "tabaqa": b.get("tabaqa"),
            "rank_ibn_hajar": b.get("rank_ibn_hajar"),
            "rank_dhahabi": b.get("rank_dhahabi"),
            "death_date": b.get("death_date"),
            "birth_date": b.get("birth_date"),
            "aqeeda": b.get("aqeeda"),
            "relations": b.get("relations"),
            "jarh_wa_tadil_json": b.get("jarh_wa_tadil_json") or "[]",
            "original_names": variants,
        })

    query = """
    UNWIND $batch AS row
    MERGE (n:Narrator {narrator_id: row.narrator_id})
    SET n.name = row.name,
        n.kunya = row.kunya,
        n.nasab = row.nasab,
        n.tabaqa = row.tabaqa,
        n.rank_ibn_hajar = row.rank_ibn_hajar,
        n.rank_dhahabi = row.rank_dhahabi,
        n.death_date = row.death_date,
        n.birth_date = row.birth_date,
        n.aqeeda = row.aqeeda,
        n.relations = row.relations,
        n.jarh_wa_tadil_json = row.jarh_wa_tadil_json,
        n.original_names = row.original_names
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(narrator_list), batch_size):
            total += _run_batch(session, query, narrator_list[i:i + batch_size])
    return total


def ingest_chains(driver, records: list[dict], batch_size: int = 200) -> int:
    """Create NARRATED and TRANSMITTED_HADITH relationships."""
    narrated_rels = []
    transmitted_rels = []

    for r in records:
        narrators = r["narrators"]
        hadith_id = r["hadith_id"]
        n = len(narrators)
        for i in range(n - 1):
            narrated_rels.append({
                "from_id": int(narrators[i]["id"]),
                "to_id": int(narrators[i + 1]["id"]),
                "position": i,
                "hadith_id": hadith_id,
            })
        if n > 0:
            transmitted_rels.append({
                "narrator_id": int(narrators[-1]["id"]),
                "hadith_id": hadith_id,
                "position": n - 1,
            })

    narrated_query = """
    UNWIND $batch AS row
    MATCH (a:Narrator {narrator_id: row.from_id})
    MATCH (b:Narrator {narrator_id: row.to_id})
    CREATE (a)-[:NARRATED {position: row.position, hadith_id: row.hadith_id}]->(b)
    """
    transmitted_query = """
    UNWIND $batch AS row
    MATCH (n:Narrator {narrator_id: row.narrator_id})
    MATCH (h:Hadith {hadith_id: row.hadith_id})
    CREATE (n)-[:TRANSMITTED_HADITH {position: row.position}]->(h)
    """
    total = 0
    with driver.session() as session:
        for i in range(0, len(narrated_rels), batch_size):
            total += _run_batch(session, narrated_query, narrated_rels[i:i + batch_size])
        for i in range(0, len(transmitted_rels), batch_size):
            total += _run_batch(session, transmitted_query, transmitted_rels[i:i + batch_size])
    return total


# ---------------------------------------------------------------------------
# Schema description for chatbot
# ---------------------------------------------------------------------------

SCHEMA_DESCRIPTION = """\
# Hadith Graph — Neo4j Schema Description

This graph contains Hadith data from Sahih Al-Bukhari (shamela.ws, book 1681).

---

## Node Labels

### Book
Represents a major section (كتاب) of the hadith collection.
Properties:
- `book_id` (Integer): Shamela book ID (e.g. 1681)
- `section_id` (Integer): Shamela section/page number uniquely identifying this book
- `name` (String): Arabic name (e.g. "كتاب الإيمان")

### Chapter
Represents a sub-section (باب) within a Book.
Properties:
- `section_id` (Integer): Unique Shamela page number for this chapter
- `book_id` (Integer): Parent book's book_id
- `name` (String): Arabic chapter name (e.g. "باب الإيمان بالقدر")

### Hadith
A single hadith record.
Properties:
- `hadith_id` (String): Unique ID in format "1681_{page_number}" (e.g. "1681_11")
- `page_number` (Integer): Shamela page number
- `book_id` (Integer): Book ID (always 1681 for Bukhari)
- `full_text` (String): Complete Arabic text including sanad (chain) and matn (body)
- `matn` (String): The hadith body text only (without the narrator chain)

### Narrator
A narrator in the transmission chain.
Properties:
- `narrator_id` (Integer): Shamela's unique narrator ID
- `name` (String): Canonical full name in Arabic
- `kunya` (String): Epithet (e.g. "أبو بكر") — may be null
- `nasab` (String): Geographical/tribal attribution (e.g. "البصري") — may be null
- `tabaqa` (String): Generation classification (e.g. "التاسعة", "العاشرة") — may be null
- `rank_ibn_hajar` (String): Reliability grade by Ibn Hajar (e.g. "ثقة", "مقبول", "صدوق") — may be null
- `rank_dhahabi` (String): Reliability grade by Al-Dhahabi (e.g. "ثقة") — may be null
- `death_date` (String): Death date as text — may be null
- `birth_date` (String): Birth date as text — sparse, may be null
- `aqeeda` (String): Theological notes — sparse, may be null
- `relations` (String): Family relationships (e.g. "أخوه: X") — may be null
- `original_names` (List[String]): All name forms this narrator appears as in hadiths
- `jarh_wa_tadil_json` (String): JSON-encoded scholarly assessment quotes — use for text search only

---

## Relationship Types

### (Hadith)-[:IN_CHAPTER]->(Chapter)
Links a hadith to its chapter. Some hadiths may lack this if the page had no chapter breadcrumb.

### (Chapter)-[:IN_BOOK]->(Book)
Links a chapter to its parent book.

### (Narrator)-[:NARRATED {position, hadith_id}]->(Narrator)
A transmission link between two consecutive narrators in a chain.
- `position` (Integer): 0-based position of the FROM narrator in the chain
- `hadith_id` (String): The hadith this chain belongs to

### (Narrator)-[:TRANSMITTED_HADITH {position}]->(Hadith)
Links the final (last) narrator in the chain to the hadith itself.
- `position` (Integer): Position of this narrator in the chain

---

## Key Query Patterns

```cypher
-- Full chain for a hadith
MATCH (n:Narrator)-[:NARRATED*]->(last:Narrator)-[:TRANSMITTED_HADITH]->(h:Hadith {hadith_id:'1681_11'})
RETURN n.name, last.name, h.matn

-- Find hadiths in a specific book
MATCH (h:Hadith)-[:IN_CHAPTER]->(:Chapter)-[:IN_BOOK]->(b:Book)
WHERE b.name CONTAINS 'الإيمان'
RETURN h.hadith_id, h.matn

-- Most frequently appearing narrators
MATCH (n:Narrator)-[:NARRATED|TRANSMITTED_HADITH]->()
RETURN n.name, count(*) AS frequency ORDER BY frequency DESC LIMIT 10

-- Find narrators rated ثقة by Ibn Hajar
MATCH (n:Narrator {rank_ibn_hajar: 'ثقة'})
RETURN n.name, n.tabaqa

-- Find all hadiths a narrator appears in
MATCH (n:Narrator)-[:NARRATED|TRANSMITTED_HADITH]->(target)
WHERE n.name CONTAINS 'البخاري'
WITH n, target
MATCH (h:Hadith) WHERE h.hadith_id = CASE WHEN target:Hadith THEN target.hadith_id ELSE null END
  OR EXISTS((target)-[:NARRATED*]->(:Narrator)-[:TRANSMITTED_HADITH]->(h))
RETURN h.hadith_id, h.matn LIMIT 10

-- Narrator chain path between two narrators
MATCH path = (a:Narrator)-[:NARRATED*..10]->(b:Narrator)
WHERE a.name CONTAINS 'مالك' AND b.name CONTAINS 'نافع'
RETURN path LIMIT 5
```
"""


def write_schema_description(path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(SCHEMA_DESCRIPTION)
    print(f"  Schema description written to {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest Shamela Bukhari data into Neo4j")
    parser.add_argument("--hadith", default="extract_data_v2/firecrawl/shamela_book_1681.jsonl",
                        help="Path to shamela_book_1681.jsonl")
    parser.add_argument("--narrators", default="extract_data_v2/firecrawl/shamela_narrators.jsonl",
                        help="Path to shamela_narrators.jsonl")
    parser.add_argument("--name-variants", default="extract_data_v2/firecrawl/narrator_hadith_names.json",
                        help="Path to narrator_hadith_names.json")
    parser.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD", ""))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true", help="Print stats without writing to Neo4j")
    parser.add_argument("--schema-out", default="extract_data_v2/schema_description.md",
                        help="Path to write schema description for chatbot")
    args = parser.parse_args()

    # --- Load auxiliary data ---
    print("Loading narrator biographies...")
    bio = load_bio(args.narrators)
    print(f"  {len(bio)} narrator bios loaded.")

    print("Loading narrator name variants...")
    name_variants = load_name_variants(args.name_variants)
    print(f"  {len(name_variants)} narrator name variant sets loaded.")

    # --- Stream hadith pages ---
    print("Parsing hadith pages...")
    records = list(parse_hadith_pages(args.hadith))
    print(f"  {len(records)} hadith records parsed.")

    # Collect all unique narrator IDs
    narrator_ids: set[str] = set()
    for r in records:
        for n in r["narrators"]:
            narrator_ids.add(str(n["id"]))
    print(f"  {len(narrator_ids)} unique narrator IDs found in chains.")

    # Stats
    books = {r["book_section_id"] for r in records if r["book_section_id"]}
    chapters = {r["chapter_section_id"] for r in records if r["chapter_section_id"]}
    print(f"  {len(books)} unique books, {len(chapters)} unique chapters.")

    if args.dry_run:
        print("\nDry run complete. No data written to Neo4j.")
        write_schema_description(args.schema_out)
        return

    # --- Ingest ---
    if not _NEO4J_AVAILABLE:
        print("neo4j driver not installed. Run: pip install neo4j")
        sys.exit(1)

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    try:
        print("\nCreating constraints and indexes...")
        create_constraints(driver)

        print("Ingesting Book nodes...")
        n = ingest_books(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting Chapter nodes + IN_BOOK...")
        n = ingest_chapters(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting Hadith nodes + IN_CHAPTER...")
        n = ingest_hadiths(driver, records, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting Narrator nodes...")
        n = ingest_narrators(driver, narrator_ids, bio, name_variants, args.batch_size)
        print(f"  Done ({n} created/merged).")

        print("Ingesting chain relationships (NARRATED + TRANSMITTED_HADITH)...")
        n = ingest_chains(driver, records, args.batch_size)
        print(f"  Done ({n} relationships created).")

        print("\nVerification counts:")
        with driver.session() as session:
            for label in ("Book", "Chapter", "Hadith", "Narrator"):
                count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
                print(f"  {label}: {count}")
            for rel in ("IN_BOOK", "IN_CHAPTER", "NARRATED", "TRANSMITTED_HADITH"):
                count = session.run(f"MATCH ()-[r:{rel}]->() RETURN count(r) AS c").single()["c"]
                print(f"  [{rel}]: {count}")

    finally:
        driver.close()

    write_schema_description(args.schema_out)
    print("\nDone.")


if __name__ == "__main__":
    main()
