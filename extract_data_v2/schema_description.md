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
- `full_text` (String): Complete Arabic text including sanad (chain) and matn (body), with tashkeel
- `matn` (String): The hadith body text only (without the narrator chain), with tashkeel
- `full_text_plain` (String): `full_text` with all tashkeel (diacritics) stripped — use for text search
- `matn_plain` (String): `matn` with all tashkeel stripped — use for text search

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

-- Search hadith text without tashkeel (preferred for user queries)
MATCH (h:Hadith)
WHERE h.matn_plain CONTAINS 'النية'
RETURN h.hadith_id, h.matn
LIMIT 20

-- Full-text index search (faster for large result sets)
CALL db.index.fulltext.queryNodes('hadith_plain_text_ft', 'النية') YIELD node, score
RETURN node.hadith_id, node.matn, score ORDER BY score DESC LIMIT 20

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
