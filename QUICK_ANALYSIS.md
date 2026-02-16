# Quick Analysis Guide (V2 Schema)

Quick reference for common hadith graph analysis queries using the **v2 schema** with Chain nodes and TRANSMITTED_TO relationships.

## 🎯 Open Neo4j Browser

```bash
open http://localhost:7474
```

Login: `neo4j` / `password`

---

## 📊 Essential Queries (V2 Schema)

### 1. Display specific hadith chain (CLEAN, no duplicates!) ⭐

```cypher
// Graph visualization - each chain as a clean star pattern
MATCH (h:Hadith {source: 'bukhari', hadith_index: 3})-[:HAS_CHAIN]->(c:Chain)
MATCH (c)-[p:POSITION]->(n:Narrator)
RETURN h, c, p, n;
```

**Click the graph icon** (🔵) - you'll see clean visualization without duplicate arrows!

---

### 2. Reconstruct full chain as a list

```cypher
MATCH (h:Hadith {source: 'bukhari', hadith_index: 3})-[:HAS_CHAIN]->(c:Chain)
MATCH (c)-[p:POSITION]->(n:Narrator)
WITH c.chain_id AS chain_id, p.pos AS pos, n.name AS name
ORDER BY chain_id, pos
WITH chain_id, collect(name) AS narrators
RETURN chain_id,
       narrators,
       size(narrators) AS chain_length;
```

**Shows**: Ordered list of narrators for each chain

---

### 3. Relationship between two narrators (hadith distribution) ⭐

```cypher
// Find how many hadiths Ibn Shihab transmitted from Urwa
MATCH (n1:Narrator)-[r:TRANSMITTED_TO]->(n2:Narrator)
WHERE n1.name CONTAINS 'ابن شهاب' AND n2.name CONTAINS 'عروة'
RETURN n1.name AS from_narrator,
       n2.name AS to_narrator,
       r.count AS hadith_count,
       r.hadith_indices[0..10] AS sample_hadiths;
```

**Shows**: Number of shared hadiths and their indices

---

### 4. Most collaborative narrators (top pairs)

```cypher
MATCH (n1:Narrator)-[r:TRANSMITTED_TO]->(n2:Narrator)
RETURN n1.name AS from_narrator,
       n2.name AS to_narrator,
       r.count AS shared_hadiths
ORDER BY r.count DESC
LIMIT 20;
```

**Shows**: Narrator pairs who appear together most frequently

---

### 5. Narrator's network (who they transmitted to/from)

```cypher
MATCH (n:Narrator)
WHERE n.name CONTAINS 'مالك'
MATCH (n)-[r:TRANSMITTED_TO]-(related:Narrator)
RETURN related.name AS connected_narrator,
       r.count AS shared_hadiths,
       CASE WHEN startNode(r) = n THEN 'transmitted_to' ELSE 'received_from' END AS direction
ORDER BY r.count DESC
LIMIT 30;
```

**Shows**: All narrators connected to a specific person with direction

---

### 6. Get hadith texts shared between two narrators

```cypher
MATCH (n1:Narrator)-[r:TRANSMITTED_TO]->(n2:Narrator)
WHERE n1.name CONTAINS 'مالك' AND n2.name CONTAINS 'نافع'
UNWIND r.hadith_indices AS idx
MATCH (h:Hadith {source: 'bukhari', hadith_index: idx})
RETURN h.hadith_index,
       substring(h.text, 0, 100) + '...' AS text_preview
LIMIT 20;
```

**Shows**: Actual hadith texts for the shared hadiths

---

### 7. Top narrators by unique hadiths

```cypher
MATCH (c:Chain {source: 'bukhari'})-[:POSITION]->(n:Narrator)
WITH n, collect(DISTINCT c.hadith_index) AS hadith_indices
RETURN n.name AS narrator,
       size(hadith_indices) AS hadith_count
ORDER BY hadith_count DESC
LIMIT 30;
```

**Shows**: Narrators appearing in the most different hadiths

---

### 8. Lead narrators (companions) - chain starts

```cypher
MATCH (c:Chain {source: 'bukhari'})-[:POSITION {pos: 0}]->(lead:Narrator)
RETURN lead.name AS companion,
       count(DISTINCT c.hadith_index) AS hadith_count
ORDER BY hadith_count DESC
LIMIT 30;
```

**Shows**: Which companions narrated the most hadiths

---

### 9. Final narrators (sheikhs/recorders)

```cypher
MATCH (c:Chain {source: 'bukhari'})-[p:POSITION]->(n:Narrator)
WHERE p.pos = c.length - 1
RETURN n.name AS sheikh,
       count(DISTINCT c.hadith_index) AS hadith_count
ORDER BY hadith_count DESC
LIMIT 30;
```

**Shows**: Most common final narrators (those who recorded hadiths)

---

### 10. Chain length statistics

```cypher
// Average, min, max chain lengths
MATCH (c:Chain {source: 'bukhari'})
RETURN min(c.length) AS shortest,
       max(c.length) AS longest,
       avg(c.length) AS average,
       count(*) AS total_chains;
```

---

### 11. Chain length distribution

```cypher
MATCH (c:Chain {source: 'bukhari'})
RETURN c.length AS narrators_in_chain,
       count(*) AS frequency
ORDER BY c.length;
```

**Shows**: How many chains have 2, 3, 4, etc. narrators

---

### 12. Database overview (V2)

```cypher
MATCH (n:Narrator {source: 'bukhari'}) WITH count(n) AS narrator_count
MATCH (h:Hadith {source: 'bukhari'}) WITH narrator_count, count(h) AS hadith_count
MATCH (c:Chain {source: 'bukhari'}) WITH narrator_count, hadith_count, count(c) AS chain_count
MATCH ()-[p:POSITION]->() WITH narrator_count, hadith_count, chain_count, count(p) AS position_count
MATCH ()-[t:TRANSMITTED_TO]->()
RETURN narrator_count AS total_narrators,
       hadith_count AS total_hadiths,
       chain_count AS total_chains,
       position_count AS position_edges,
       count(t) AS transmitted_to_edges;
```

**Shows**: Overall statistics

---

## 🔍 Network Analysis Queries

### Narrator centrality (most connections)

```cypher
MATCH (n:Narrator {source: 'bukhari'})
WITH n,
     size((n)-[:TRANSMITTED_TO]->()) AS out_degree,
     size((n)<-[:TRANSMITTED_TO]-()) AS in_degree
RETURN n.name AS narrator,
       out_degree + in_degree AS total_connections,
       out_degree AS transmitted_from_count,
       in_degree AS received_count
ORDER BY total_connections DESC
LIMIT 20;
```

---

### Total hadiths transmitted through narrator

```cypher
MATCH (n:Narrator {source: 'bukhari'})-[r:TRANSMITTED_TO]-()
WITH n, sum(r.count) AS total_transmissions
RETURN n.name AS narrator,
       total_transmissions
ORDER BY total_transmissions DESC
LIMIT 20;
```

---

## 🎨 Visualization Queries

### Clean chain visualization (for one hadith) ⭐

```cypher
MATCH (h:Hadith {source: 'bukhari', hadith_index: 3})-[:HAS_CHAIN]->(c:Chain)
MATCH (c)-[p:POSITION]->(n:Narrator)
RETURN h, c, p, n;
```

Click the graph icon in Neo4j Browser to see clean star patterns from each Chain to its Narrators.

---

### Subgraph around a narrator (using TRANSMITTED_TO)

```cypher
MATCH (n:Narrator {source: 'bukhari'})
WHERE n.name CONTAINS 'ابن شهاب'
MATCH path = (n)-[:TRANSMITTED_TO*1..2]-(related)
RETURN path
LIMIT 100;
```

---

## 💡 Pro Tips

1. **V2 schema advantage**: Clean visualization - each chain connects to its narrators via POSITION, no duplicate arrows
2. **TRANSMITTED_TO advantage**: Aggregate network analysis - one relationship per narrator pair with count
3. **Change hadith_index** in queries to analyze different hadiths
4. **Change narrator names** in `WHERE n.name CONTAINS 'xxx'` to search
5. **Use `LIMIT`** to prevent overwhelming results
6. **Click the graph icon** (🔵) in Neo4j Browser for visualizations

---

## 🔍 Search by Normalized Name

```cypher
// Find all variations of "ابن شهاب"
MATCH (n:Narrator {source: 'bukhari'})
WHERE n.norm CONTAINS 'ابن شهاب'
RETURN n.name AS original_name,
       n.norm AS normalized,
       size((n)-[:TRANSMITTED_TO]-()) + size((n)<-[:TRANSMITTED_TO]-()) AS connections
ORDER BY connections DESC;
```

---

## 📈 V2 Schema Summary

| Entity | Description |
|--------|-------------|
| `(:Narrator)` | Narrator nodes with source, norm, name |
| `(:Hadith)` | Hadith nodes with source, hadith_index, text |
| `(:Chain)` | Chain nodes with source, hadith_index, chain_id, length |
| `[:HAS_CHAIN]` | Hadith → Chain relationship |
| `[:POSITION {pos}]` | Chain → Narrator with position in chain |
| `[:TRANSMITTED_TO {count, hadith_indices}]` | Aggregate narrator → narrator relationship |

---

**Quick access**: Copy these queries into Neo4j Browser ([http://localhost:7474](http://localhost:7474))
