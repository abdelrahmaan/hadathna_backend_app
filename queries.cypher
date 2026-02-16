// ============================================================
// Hadith Narrator Graph — Cypher Query Library (V3 Schema)
// ============================================================
//
// Schema:
//   (:Book       {book_id, section_id, name})
//   (:Chapter    {section_id, book_id, name})
//   (:Hadith     {hadith_id, page_number, book_id, full_text, matn})
//   (:Narrator   {narrator_id, name, kunya, nasab, tabaqa,
//                 rank_ibn_hajar, rank_dhahabi, death_date,
//                 birth_date, aqeeda, relations, original_names[]})
//
//   (Hadith)-[:IN_CHAPTER]->(Chapter)
//   (Chapter)-[:IN_BOOK]->(Book)
//   (Narrator)-[:NARRATED {position, hadith_id}]->(Narrator)
//   (Narrator)-[:TRANSMITTED_HADITH {position}]->(Hadith)
//
// Chain direction: narrators[0] = collector (البخاري)
//                 narrators[-1] = last narrator before the Prophet ﷺ
// ============================================================


// ============================================================
// 1. VERIFICATION / DATABASE STATS
// ============================================================

// Node counts
MATCH (b:Book) RETURN count(b) AS books;
MATCH (c:Chapter) RETURN count(c) AS chapters;
MATCH (h:Hadith) RETURN count(h) AS hadiths;
MATCH (n:Narrator) RETURN count(n) AS narrators;

// Relationship counts
MATCH ()-[r:IN_BOOK]->()        RETURN count(r) AS in_book;
MATCH ()-[r:IN_CHAPTER]->()     RETURN count(r) AS in_chapter;
MATCH ()-[r:NARRATED]->()       RETURN count(r) AS narrated;
MATCH ()-[r:TRANSMITTED_HADITH]->() RETURN count(r) AS transmitted_hadith;

// Full stats in one query
MATCH (b:Book)     WITH count(b) AS books
MATCH (c:Chapter)  WITH books, count(c) AS chapters
MATCH (h:Hadith)   WITH books, chapters, count(h) AS hadiths
MATCH (n:Narrator) WITH books, chapters, hadiths, count(n) AS narrators
RETURN books, chapters, hadiths, narrators;


// ============================================================
// 2. HADITH LOOKUP
// ============================================================

// Get a specific hadith by ID
MATCH (h:Hadith {hadith_id: '1681_11'})
RETURN h.matn AS matn, h.full_text AS full_text;

// Get hadith by page number
MATCH (h:Hadith {page_number: 11})
RETURN h.hadith_id, h.matn;

// Search matn text
MATCH (h:Hadith)
WHERE h.matn CONTAINS 'النية'
RETURN h.hadith_id, h.matn
LIMIT 20;

// Get hadith with its book and chapter
MATCH (h:Hadith {hadith_id: '1681_11'})
OPTIONAL MATCH (h)-[:IN_CHAPTER]->(c:Chapter)-[:IN_BOOK]->(b:Book)
RETURN h.matn, c.name AS chapter, b.name AS book;


// ============================================================
// 3. NARRATOR CHAIN QUERIES
// ============================================================

// Full chain for a specific hadith (ordered by position)
MATCH (h:Hadith {hadith_id: '1681_11'})
MATCH (n:Narrator)-[r:NARRATED|TRANSMITTED_HADITH]->(target)
WHERE (target:Narrator OR target:Hadith)
  AND (r.hadith_id = h.hadith_id OR target = h)
RETURN n.name AS narrator, r.position AS position
ORDER BY position;

// Reconstruct chain as a list
MATCH (h:Hadith {hadith_id: '1681_11'})
MATCH (n:Narrator)-[r:NARRATED {hadith_id: h.hadith_id}]->(next:Narrator)
WITH n, r.position AS pos
ORDER BY pos
WITH collect(n.name) AS chain_narrators
RETURN chain_narrators;

// Full graph path for visual display in Neo4j Browser
MATCH path = (first:Narrator)-[:NARRATED*]->(last:Narrator)-[:TRANSMITTED_HADITH]->(h:Hadith {hadith_id: '1681_11'})
RETURN path;

// ------------------------------------------------------------
// Explain the whole chain graph for ONE hadith (nodes + edges)
// ------------------------------------------------------------
// Parameterized version:
// :param hadithId => '1681_11'
MATCH (h:Hadith {hadith_id: $hadithId})
OPTIONAL MATCH chainPath = (start:Narrator)-[rels:NARRATED*]->(last:Narrator)-[:TRANSMITTED_HADITH]->(h)
WHERE ALL(rel IN rels WHERE rel.hadith_id = h.hadith_id)
  AND NOT (:Narrator)-[:NARRATED {hadith_id: h.hadith_id}]->(start)
WITH h, chainPath
RETURN
  h.hadith_id AS hadith_id,
  [n IN nodes(chainPath) | coalesce(n.name, n.hadith_id)] AS ordered_nodes,
  [rel IN relationships(chainPath) |
    {
      type: type(rel),
      hadith_id: coalesce(rel.hadith_id, h.hadith_id),
      position: rel.position
    }
  ] AS ordered_edges;

// Example (fixed hadith id):
MATCH (h:Hadith {hadith_id: '1681_11'})
OPTIONAL MATCH chainPath = (start:Narrator)-[rels:NARRATED*]->(last:Narrator)-[:TRANSMITTED_HADITH]->(h)
WHERE ALL(rel IN rels WHERE rel.hadith_id = h.hadith_id)
  AND NOT (:Narrator)-[:NARRATED {hadith_id: h.hadith_id}]->(start)
RETURN chainPath;

// Flat edge list (good for explaining/debugging chain links)
// :param hadithId => '1681_11'
MATCH (h:Hadith {hadith_id: $hadithId})
OPTIONAL MATCH (a:Narrator)-[r:NARRATED {hadith_id: h.hadith_id}]->(b:Narrator)
WITH h, a, b, r
ORDER BY r.position
RETURN
  h.hadith_id AS hadith_id,
  collect({
    from_id: a.narrator_id, from_name: a.name,
    to_id: b.narrator_id, to_name: b.name,
    position: r.position
  }) AS narrator_to_narrator_edges;

// Full subgraph (all related nodes + relationships for one hadith)
// :param hadithId => '1681_11'
MATCH (h:Hadith {hadith_id: $hadithId})
OPTIONAL MATCH (n:Narrator)-[r1:NARRATED {hadith_id: h.hadith_id}]->(m:Narrator)
OPTIONAL MATCH (t:Narrator)-[r2:TRANSMITTED_HADITH]->(h)
RETURN h, n, m, t, r1, r2;

// Get all narrators for a hadith with their bios
MATCH (h:Hadith {hadith_id: '1681_11'})
MATCH (n:Narrator)-[r:NARRATED|TRANSMITTED_HADITH]->(target)
WHERE r.hadith_id = h.hadith_id OR target = h
RETURN n.name, n.tabaqa, n.rank_ibn_hajar, n.death_date, r.position AS position
ORDER BY position;


// ============================================================
// 4. NARRATOR QUERIES
// ============================================================

// Find narrator by name
MATCH (n:Narrator)
WHERE n.name CONTAINS 'مالك بن أنس'
RETURN n.narrator_id, n.name, n.kunya, n.nasab, n.tabaqa,
       n.rank_ibn_hajar, n.rank_dhahabi, n.death_date;

// Find narrator by ID
MATCH (n:Narrator {narrator_id: 5361})
RETURN n;

// Narrator's name variants (all forms they appear as in hadiths)
MATCH (n:Narrator {narrator_id: 5361})
RETURN n.name AS canonical_name, n.original_names AS variants;

// All narrators of a specific generation (tabaqa)
MATCH (n:Narrator {tabaqa: 'التاسعة'})
RETURN n.name, n.rank_ibn_hajar, n.death_date
ORDER BY n.name;

// Narrators rated ثقة by Ibn Hajar
MATCH (n:Narrator)
WHERE n.rank_ibn_hajar CONTAINS 'ثقة'
RETURN n.name, n.rank_ibn_hajar, n.rank_dhahabi, n.tabaqa
ORDER BY n.name;

// Narrator biography summary
MATCH (n:Narrator)
WHERE n.name CONTAINS 'الزهري'
RETURN n.name, n.kunya, n.nasab, n.tabaqa,
       n.rank_ibn_hajar, n.rank_dhahabi,
       n.birth_date, n.death_date, n.aqeeda;


// ============================================================
// 5. NARRATOR TRANSMISSION STATISTICS
// ============================================================

// Most frequently appearing narrators (by transmission count)
MATCH (n:Narrator)-[:NARRATED|TRANSMITTED_HADITH]->()
RETURN n.name, count(*) AS transmissions
ORDER BY transmissions DESC
LIMIT 20;

// How many unique hadiths each narrator appears in
MATCH (n:Narrator)-[r:NARRATED]->(x)
WITH n, collect(DISTINCT r.hadith_id) AS hadiths
RETURN n.name, size(hadiths) AS hadith_count
ORDER BY hadith_count DESC
LIMIT 20;

// Narrators who transmitted directly to/from a specific person
MATCH (n:Narrator {narrator_id: 5361})-[:NARRATED]->(student:Narrator)
RETURN student.name AS student, count(*) AS times
ORDER BY times DESC;

MATCH (teacher:Narrator)-[:NARRATED]->(n:Narrator {narrator_id: 5361})
RETURN teacher.name AS teacher, count(*) AS times
ORDER BY times DESC;

// Narrator network: 2-hop connections
MATCH (n:Narrator)-[:NARRATED*1..2]-(connected:Narrator)
WHERE n.name CONTAINS 'مالك'
RETURN DISTINCT connected.name, connected.tabaqa
LIMIT 30;


// ============================================================
// 6. BOOK AND CHAPTER HIERARCHY
// ============================================================

// All books with hadith counts
MATCH (h:Hadith)-[:IN_CHAPTER]->(:Chapter)-[:IN_BOOK]->(b:Book)
RETURN b.name, count(h) AS hadith_count
ORDER BY hadith_count DESC;

// All chapters in a specific book
MATCH (c:Chapter)-[:IN_BOOK]->(b:Book)
WHERE b.name CONTAINS 'الإيمان'
RETURN c.name, c.section_id
ORDER BY c.section_id;

// Hadiths in a specific chapter
MATCH (h:Hadith)-[:IN_CHAPTER]->(c:Chapter)
WHERE c.name CONTAINS 'كيف كان بدء الوحي'
RETURN h.hadith_id, h.matn
LIMIT 10;

// Chapter with most hadiths
MATCH (h:Hadith)-[:IN_CHAPTER]->(c:Chapter)-[:IN_BOOK]->(b:Book)
RETURN b.name AS book, c.name AS chapter, count(h) AS hadiths
ORDER BY hadiths DESC
LIMIT 20;

// Hadiths without a chapter (pages with missing breadcrumb)
MATCH (h:Hadith)
WHERE NOT (h)-[:IN_CHAPTER]->()
RETURN count(h) AS hadiths_without_chapter;


// ============================================================
// 7. CROSS-NARRATOR ANALYSIS
// ============================================================

// Find narrators who appear together in the same chains most often
MATCH (a:Narrator)-[r:NARRATED]->(b:Narrator)
RETURN a.name AS from_narrator, b.name AS to_narrator, count(*) AS shared_transmissions
ORDER BY shared_transmissions DESC
LIMIT 20;

// Find the path between two narrators
MATCH path = (a:Narrator)-[:NARRATED*..8]->(b:Narrator)
WHERE a.name CONTAINS 'البخاري' AND b.name CONTAINS 'نافع'
RETURN [n IN nodes(path) | n.name] AS chain
LIMIT 5;

// Narrators shared across two different hadiths
MATCH (n:Narrator)-[r1:NARRATED {hadith_id: '1681_11'}]->()
MATCH (n)-[r2:NARRATED {hadith_id: '1681_12'}]->()
RETURN n.name AS shared_narrator;

// Top collector narrators (position 0 = first in chain)
MATCH (n:Narrator)-[r:NARRATED {position: 0}]->()
RETURN n.name, count(*) AS times_as_collector
ORDER BY times_as_collector DESC
LIMIT 10;

// Narrators who are chain-terminators (last in chain before hadith)
MATCH (n:Narrator)-[:TRANSMITTED_HADITH]->(h:Hadith)
RETURN n.name, count(h) AS hadiths_transmitted
ORDER BY hadiths_transmitted DESC
LIMIT 10;


// ============================================================
// 8. NARRATOR RELIABILITY ANALYSIS
// ============================================================

// Distribution of Ibn Hajar grades
MATCH (n:Narrator)
WHERE n.rank_ibn_hajar IS NOT NULL
RETURN n.rank_ibn_hajar, count(*) AS count
ORDER BY count DESC;

// Narrators with conflicting grades (ثقة by one, weaker by other)
MATCH (n:Narrator)
WHERE n.rank_ibn_hajar CONTAINS 'ثقة'
  AND n.rank_dhahabi IS NOT NULL
  AND NOT n.rank_dhahabi CONTAINS 'ثقة'
RETURN n.name, n.rank_ibn_hajar, n.rank_dhahabi
LIMIT 20;

// Narrators by generation with their grades
MATCH (n:Narrator)
WHERE n.tabaqa IS NOT NULL AND n.rank_ibn_hajar IS NOT NULL
RETURN n.tabaqa, n.rank_ibn_hajar, count(*) AS count
ORDER BY n.tabaqa, count DESC;


// ============================================================
// 9. SCHEMA INTROSPECTION
// ============================================================

// View schema visualization
CALL db.schema.visualization();

// List all node labels and counts
CALL db.labels() YIELD label
CALL apoc.cypher.run('MATCH (n:' + label + ') RETURN count(n) AS count', {})
YIELD value
RETURN label, value.count AS count;

// List all relationship types
CALL db.relationshipTypes();

// List all property keys
CALL db.propertyKeys();
