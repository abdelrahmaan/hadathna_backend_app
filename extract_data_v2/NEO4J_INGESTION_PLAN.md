# Neo4j v3 Ingestion Plan

## Overview

This document outlines the plan to ingest normalized hadith data (with unique narrator IDs) into Neo4j using a new v3 schema.

**Status:** 🚧 Phase 3 - In Planning

**Prerequisites:**
- ✅ Phase 1 Complete: Narrator normalization with 67.7% coverage (204 mappings)
- ✅ Input file ready: `Bukhari_Normalized_Ready_For_Graph.json`

---

## v3 Schema Design

### Nodes

#### 1. Narrator Node
```cypher
(:Narrator {
  narrator_id: String,           // PRIMARY KEY - NAR_XXXXXXXXXXXX
  canonical_name: String,         // Normalized full name
  original_names: [String],       // All name variants seen in data
  source: String,                 // Collection (bukhari, muslim, etc.)
  mention_count: Integer,         // How many times this narrator appears
  created_at: DateTime
})
```

**Constraints:**
```cypher
CREATE CONSTRAINT narrator_id_unique IF NOT EXISTS
FOR (n:Narrator) REQUIRE n.narrator_id IS UNIQUE;

CREATE INDEX narrator_source_idx IF NOT EXISTS
FOR (n:Narrator) ON (n.source);

CREATE INDEX narrator_canonical_name_idx IF NOT EXISTS
FOR (n:Narrator) ON (n.canonical_name);
```

#### 2. Hadith Node
```cypher
(:Hadith {
  hadith_id: String,             // PRIMARY KEY - HAD_<source>_<index>
  source: String,                // Collection name
  hadith_index: Integer,         // Original index in collection
  full_text: String,             // Complete hadith text (optional)
  chain_count: Integer,          // Number of chains (primary + follow-ups)
  created_at: DateTime
})
```

**Constraints:**
```cypher
CREATE CONSTRAINT hadith_id_unique IF NOT EXISTS
FOR (h:Hadith) REQUIRE h.hadith_id IS UNIQUE;

CREATE INDEX hadith_source_index_idx IF NOT EXISTS
FOR (h:Hadith) ON (h.source, h.hadith_index);
```

#### 3. Segment Node (Matn Text)
```cypher
(:Segment {
  segment_id: String,            // PRIMARY KEY - SEG_<hadith_id>_<position>
  hadith_id: String,             // Parent hadith
  text: String,                  // Arabic text segment
  position: Integer,             // Order in matn
  embedding: [Float],            // Vector embedding (optional, future Phase 2)
  created_at: DateTime
})
```

**Constraints:**
```cypher
CREATE CONSTRAINT segment_id_unique IF NOT EXISTS
FOR (s:Segment) REQUIRE s.segment_id IS UNIQUE;

CREATE INDEX segment_hadith_idx IF NOT EXISTS
FOR (s:Segment) ON (s.hadith_id);

-- Optional: Vector index for semantic search (Phase 2)
-- CREATE VECTOR INDEX segment_embedding_idx IF NOT EXISTS
-- FOR (s:Segment) ON (s.embedding)
-- OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'cosine'}};
```

---

### Relationships

#### 1. NARRATED (Chain Link)
```cypher
(:Narrator)-[:NARRATED {
  chain_id: String,              // e.g., "chain_1"
  hadith_id: String,             // Parent hadith
  position: Integer,             // Position in chain (0 = top narrator)
  chain_type: String,            // "primary" or "follow_up"
  original_name: String          // Name as it appeared in the original chain
}]->(:Narrator)
```

**Purpose:** Tracks narrator-to-narrator transmission within chains

**Example:**
```
(NAR_A1B2:Narrator {canonical_name: "محمد بن إسماعيل البخاري"})
  -[:NARRATED {chain_id: "chain_1", position: 0, hadith_id: "HAD_bukhari_1"}]->
(NAR_C3D4:Narrator {canonical_name: "الحميدي"})
  -[:NARRATED {chain_id: "chain_1", position: 1, hadith_id: "HAD_bukhari_1"}]->
(NAR_E5F6:Narrator {canonical_name: "سفيان الثوري"})
```

#### 2. NARRATED_HADITH (Top Narrator to Hadith)
```cypher
(:Narrator)-[:NARRATED_HADITH {
  chain_id: String,              // Which chain
  chain_type: String,            // "primary" or "follow_up"
  position: Integer              // Always 0 (top narrator)
}]->(:Hadith)
```

**Purpose:** Quick lookup of hadiths narrated by a specific person

**Example:**
```
(NAR_A1B2:Narrator {canonical_name: "محمد بن إسماعيل البخاري"})
  -[:NARRATED_HADITH {chain_id: "chain_1", chain_type: "primary"}]->
(HAD_bukhari_1:Hadith)
```

#### 3. HAS_SEGMENT (Hadith to Matn Segments)
```cypher
(:Hadith)-[:HAS_SEGMENT {
  position: Integer              // Order in matn
}]->(:Segment)
```

**Purpose:** Links hadith to its text segments for semantic search

**Example:**
```
(HAD_bukhari_1:Hadith)
  -[:HAS_SEGMENT {position: 0}]->
(SEG_bukhari_1_0:Segment {text: "إنما الأعمال بالنيات"})

(HAD_bukhari_1:Hadith)
  -[:HAS_SEGMENT {position: 1}]->
(SEG_bukhari_1_1:Segment {text: "وإنما لكل امرئ ما نوى"})
```

---

## Input Data Format

**File:** `Bukhari_Normalized_Ready_For_Graph.json`

**Structure:**
```json
[
  {
    "hadith_index": 1,
    "source": "bukhari",
    "chains": [
      {
        "chain_id": "chain_1",
        "type": "primary",
        "narrators": [
          {
            "original_name": "الحميدي",
            "name": "عبد الله بن الزبير الحميدي",
            "narrator_id": "NAR_A1B2C3D4E5F6",
            "position": 0
          },
          {
            "original_name": "سفيان",
            "name": "سفيان الثوري",
            "narrator_id": "NAR_B2C3D4E5F6A7",
            "position": 1
          }
        ]
      }
    ],
    "matn_segments": [
      {
        "position": 0,
        "text": "إنما الأعمال بالنيات"
      },
      {
        "position": 1,
        "text": "وإنما لكل امرئ ما نوى"
      }
    ]
  }
]
```

---

## Implementation Steps

### Step 1: Create `ingest_v3.py`

New ingestion script leveraging existing `neo4j_client.py` utilities.

**Key Functions:**

```python
def generate_hadith_id(source: str, hadith_index: int) -> str:
    """Generate unique hadith ID."""
    return f"HAD_{source}_{hadith_index}"

def generate_segment_id(hadith_id: str, position: int) -> str:
    """Generate unique segment ID."""
    return f"SEG_{hadith_id}_{position}"

def create_constraints(client: Neo4jClient) -> None:
    """Create all constraints and indexes for v3 schema."""
    # Narrator constraints
    # Hadith constraints
    # Segment constraints

def batch_create_narrators_v3(
    client: Neo4jClient,
    narrator_data: List[Dict],
    batch_size: int = 100
) -> int:
    """
    Create/merge narrator nodes with unique IDs.

    Uses MERGE on narrator_id to prevent duplicates.
    Accumulates original_names as a set.
    """

def batch_create_hadiths(
    client: Neo4jClient,
    hadith_data: List[Dict],
    batch_size: int = 100
) -> int:
    """Create hadith nodes."""

def batch_create_segments(
    client: Neo4jClient,
    segment_data: List[Dict],
    batch_size: int = 100
) -> int:
    """Create segment nodes and HAS_SEGMENT relationships."""

def batch_create_narrator_chains(
    client: Neo4jClient,
    chain_data: List[Dict],
    batch_size: int = 100
) -> int:
    """
    Create NARRATED relationships between narrators.
    Also creates NARRATED_HADITH for top narrator.
    """
```

**CLI Interface:**

```bash
python extract_data_v2/ingest_v3.py \
  --input "extract_data_v2/Bukhari/Bukhari_Normalized_Ready_For_Graph.json" \
  --source bukhari \
  --clear \
  --batch-size 100 \
  --dry-run
```

---

### Step 2: Data Extraction & Preparation

**Extract Unique Narrators:**
```python
def extract_narrators_from_json(data: List[Dict]) -> List[Dict]:
    """
    Extract all unique narrators from the JSON file.

    Returns:
        List of dicts with: narrator_id, canonical_name, original_names[], source
    """
    narrator_map = {}  # narrator_id -> {canonical_name, original_names_set, source}

    for hadith in data:
        source = hadith["source"]
        for chain in hadith["chains"]:
            for narrator in chain["narrators"]:
                narrator_id = narrator["narrator_id"]
                canonical_name = narrator["name"]
                original_name = narrator["original_name"]

                if narrator_id not in narrator_map:
                    narrator_map[narrator_id] = {
                        "narrator_id": narrator_id,
                        "canonical_name": canonical_name,
                        "original_names": set([original_name]),
                        "source": source,
                        "mention_count": 0
                    }
                else:
                    narrator_map[narrator_id]["original_names"].add(original_name)

                narrator_map[narrator_id]["mention_count"] += 1

    # Convert sets to lists for JSON serialization
    return [
        {**v, "original_names": list(v["original_names"])}
        for v in narrator_map.values()
    ]
```

**Extract Hadiths:**
```python
def extract_hadiths(data: List[Dict]) -> List[Dict]:
    """
    Extract hadith metadata.

    Returns:
        List of dicts with: hadith_id, source, hadith_index, chain_count
    """
    return [
        {
            "hadith_id": generate_hadith_id(h["source"], h["hadith_index"]),
            "source": h["source"],
            "hadith_index": h["hadith_index"],
            "chain_count": len(h["chains"])
        }
        for h in data
    ]
```

**Extract Segments:**
```python
def extract_segments(data: List[Dict]) -> List[Dict]:
    """
    Extract matn segments.

    Returns:
        List of dicts with: segment_id, hadith_id, text, position
    """
    segments = []
    for h in data:
        hadith_id = generate_hadith_id(h["source"], h["hadith_index"])
        for seg in h.get("matn_segments", []):
            segments.append({
                "segment_id": generate_segment_id(hadith_id, seg["position"]),
                "hadith_id": hadith_id,
                "text": seg["text"],
                "position": seg["position"]
            })
    return segments
```

**Extract Chains:**
```python
def extract_chains(data: List[Dict]) -> List[Dict]:
    """
    Extract narrator chains as relationship data.

    Returns:
        List of dicts with: from_id, to_id, chain_id, hadith_id, position,
                           chain_type, original_name
    """
    chains = []
    for h in data:
        hadith_id = generate_hadith_id(h["source"], h["hadith_index"])

        for chain in h["chains"]:
            chain_id = chain["chain_id"]
            chain_type = chain["type"]
            narrators = chain["narrators"]

            # Create NARRATED_HADITH for top narrator
            if narrators:
                chains.append({
                    "type": "NARRATED_HADITH",
                    "from_id": narrators[0]["narrator_id"],
                    "to_hadith_id": hadith_id,
                    "chain_id": chain_id,
                    "chain_type": chain_type,
                    "position": 0
                })

            # Create NARRATED links between narrators
            for i in range(len(narrators) - 1):
                from_narrator = narrators[i]
                to_narrator = narrators[i + 1]

                chains.append({
                    "type": "NARRATED",
                    "from_id": from_narrator["narrator_id"],
                    "to_id": to_narrator["narrator_id"],
                    "chain_id": chain_id,
                    "hadith_id": hadith_id,
                    "position": i,
                    "chain_type": chain_type,
                    "original_name": from_narrator["original_name"]
                })

    return chains
```

---

### Step 3: Batch Ingestion with Progress Tracking

**Main Ingestion Flow:**

```python
def ingest_normalized_data(
    input_file: str,
    source: str,
    clear: bool = False,
    batch_size: int = 100,
    dry_run: bool = False
) -> None:
    """Main ingestion function."""

    # 1. Load JSON
    print(f"📂 Loading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"   ✅ Loaded {len(data)} hadiths")

    # 2. Extract data
    print("\n📊 Extracting data...")
    narrators = extract_narrators_from_json(data)
    hadiths = extract_hadiths(data)
    segments = extract_segments(data)
    chains = extract_chains(data)

    print(f"   - Narrators: {len(narrators)}")
    print(f"   - Hadiths: {len(hadiths)}")
    print(f"   - Segments: {len(segments)}")
    print(f"   - Chain links: {len(chains)}")

    if dry_run:
        print("\n🔍 DRY RUN - No changes will be made to Neo4j")
        return

    # 3. Connect to Neo4j
    print("\n🔌 Connecting to Neo4j...")
    with Neo4jClient() as client:

        # 4. Create constraints
        print("\n🔧 Creating constraints and indexes...")
        create_constraints(client)

        # 5. Optionally clear database
        if clear:
            print("\n⚠️  Clearing database...")
            client.clear_database()

        # 6. Batch create nodes
        print("\n👥 Creating narrator nodes...")
        narrator_count = batch_create_narrators_v3(client, narrators, batch_size)
        print(f"   ✅ Created/merged {narrator_count} narrators")

        print("\n📖 Creating hadith nodes...")
        hadith_count = batch_create_hadiths(client, hadiths, batch_size)
        print(f"   ✅ Created {hadith_count} hadiths")

        print("\n📝 Creating segment nodes...")
        segment_count = batch_create_segments(client, segments, batch_size)
        print(f"   ✅ Created {segment_count} segments")

        # 7. Batch create relationships
        print("\n🔗 Creating chain relationships...")
        chain_count = batch_create_narrator_chains(client, chains, batch_size)
        print(f"   ✅ Created {chain_count} relationships")

        # 8. Verification queries
        print("\n✅ Ingestion complete! Running verification...")
        verify_ingestion(client)
```

**Verification Queries:**

```python
def verify_ingestion(client: Neo4jClient) -> None:
    """Run verification queries after ingestion."""
    with client.session() as session:
        # Count nodes
        result = session.run("""
            MATCH (n:Narrator) WITH count(n) AS narrators
            MATCH (h:Hadith) WITH narrators, count(h) AS hadiths
            MATCH (s:Segment) WITH narrators, hadiths, count(s) AS segments
            RETURN narrators, hadiths, segments
        """)
        row = result.single()
        print(f"   - Narrators: {row['narrators']}")
        print(f"   - Hadiths: {row['hadiths']}")
        print(f"   - Segments: {row['segments']}")

        # Count relationships
        result = session.run("""
            MATCH ()-[r:NARRATED]->() WITH count(r) AS narrated
            MATCH ()-[r2:NARRATED_HADITH]->() WITH narrated, count(r2) AS narrated_hadith
            MATCH ()-[r3:HAS_SEGMENT]->() WITH narrated, narrated_hadith, count(r3) AS has_segment
            RETURN narrated, narrated_hadith, has_segment
        """)
        row = result.single()
        print(f"   - NARRATED: {row['narrated']}")
        print(f"   - NARRATED_HADITH: {row['narrated_hadith']}")
        print(f"   - HAS_SEGMENT: {row['has_segment']}")

        # Top narrators
        result = session.run("""
            MATCH (n:Narrator)
            RETURN n.canonical_name AS name, n.mention_count AS mentions
            ORDER BY mentions DESC LIMIT 10
        """)
        print("\n   Top 10 Narrators:")
        for row in result:
            print(f"      - {row['name']}: {row['mentions']} mentions")
```

---

## Testing Strategy

### Unit Tests

1. **Test ID Generation:**
   - Verify `generate_hadith_id()` produces correct format
   - Verify `generate_segment_id()` produces correct format
   - Verify deterministic narrator IDs from normalization

2. **Test Data Extraction:**
   - Test `extract_narrators_from_json()` with sample data
   - Test `extract_hadiths()` with sample data
   - Test `extract_segments()` with sample data
   - Test `extract_chains()` with sample data

3. **Test Batch Operations:**
   - Test narrator merging (same ID, multiple original names)
   - Test collision detection
   - Test relationship creation

### Integration Tests

1. **Small Dataset Test:**
   - Ingest 10 hadiths
   - Verify node counts
   - Verify relationship counts
   - Query sample chains

2. **Full Dataset Test:**
   - Ingest complete Bukhari collection
   - Verify expected node counts (~2,689 narrators, 7,563 hadiths)
   - Performance benchmarks (time, memory)

3. **Idempotency Test:**
   - Ingest same data twice (without --clear)
   - Verify no duplicate narrators (MERGE on narrator_id)
   - Verify relationships are not duplicated

---

## Performance Considerations

### Batch Size Tuning

- **Small batches (50-100):** Better error isolation, slower overall
- **Large batches (500-1000):** Faster, but harder to debug failures
- **Recommended:** Start with 100, increase to 500 after testing

### Indexing Strategy

1. **Create constraints BEFORE ingestion** (ensures uniqueness)
2. **Create indexes BEFORE ingestion** (faster lookups during MERGE)
3. **Optional: Drop indexes during bulk import, recreate after**

### Memory Management

- Process data in chunks if JSON file is very large (>100MB)
- Use generator functions instead of loading entire dataset into memory
- Monitor Neo4j heap usage during ingestion

---

## Validation & Quality Checks

### Pre-Ingestion Checks

```python
def validate_input_data(data: List[Dict]) -> bool:
    """Validate input JSON before ingestion."""
    errors = []

    for idx, hadith in enumerate(data):
        # Check required fields
        if "hadith_index" not in hadith:
            errors.append(f"Hadith {idx}: Missing hadith_index")
        if "source" not in hadith:
            errors.append(f"Hadith {idx}: Missing source")
        if "chains" not in hadith:
            errors.append(f"Hadith {idx}: Missing chains")

        # Check narrator IDs
        for chain in hadith.get("chains", []):
            for narrator in chain.get("narrators", []):
                if "narrator_id" not in narrator:
                    errors.append(f"Hadith {idx}: Narrator missing narrator_id")
                elif not narrator["narrator_id"].startswith("NAR_"):
                    errors.append(f"Hadith {idx}: Invalid narrator_id format")

    if errors:
        print("❌ Validation errors:")
        for err in errors[:10]:  # Show first 10
            print(f"   - {err}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more")
        return False

    print("✅ Input data validation passed")
    return True
```

### Post-Ingestion Checks

1. **Narrator Deduplication:**
   ```cypher
   // Should return 0 duplicates
   MATCH (n:Narrator)
   WITH n.narrator_id AS id, count(*) AS cnt
   WHERE cnt > 1
   RETURN id, cnt;
   ```

2. **Orphan Segments:**
   ```cypher
   // Segments without parent hadith (should be 0)
   MATCH (s:Segment)
   WHERE NOT EXISTS((s)<-[:HAS_SEGMENT]-(:Hadith))
   RETURN count(s) AS orphan_segments;
   ```

3. **Chain Integrity:**
   ```cypher
   // Verify all chains have valid hadith_id
   MATCH ()-[r:NARRATED]->()
   WHERE NOT EXISTS((:Hadith {hadith_id: r.hadith_id}))
   RETURN count(r) AS invalid_chains;
   ```

---

## Rollback Strategy

### If Ingestion Fails Mid-Process

1. **Option 1: Clear and Retry**
   ```bash
   python ingest_v3.py --input data.json --clear
   ```

2. **Option 2: Manual Cleanup**
   ```cypher
   // Delete all nodes of a specific source
   MATCH (n) WHERE n.source = 'bukhari'
   DETACH DELETE n;
   ```

3. **Option 3: Database Backup/Restore**
   ```bash
   # Before ingestion
   docker exec neo4j-hadith neo4j-admin dump --database=neo4j --to=/backups/pre-ingest.dump

   # After failed ingestion
   docker exec neo4j-hadith neo4j-admin load --from=/backups/pre-ingest.dump --database=neo4j --force
   ```

---

## Example Queries for V3 Schema

### 1. Get all chains for a hadith
```cypher
MATCH (h:Hadith {source: 'bukhari', hadith_index: 1})<-[:NARRATED_HADITH]-(top:Narrator)
MATCH path = (top)-[:NARRATED*]->(n:Narrator)
RETURN path;
```

### 2. Find hadiths narrated by a specific person
```cypher
MATCH (n:Narrator {canonical_name: 'سفيان الثوري'})-[:NARRATED_HADITH]->(h:Hadith)
RETURN h.hadith_index, h.source
ORDER BY h.hadith_index;
```

### 3. Find narrators who appear in both primary and follow-up chains
```cypher
MATCH (n:Narrator)-[r1:NARRATED_HADITH {chain_type: 'primary'}]->(:Hadith)
MATCH (n)-[r2:NARRATED_HADITH {chain_type: 'follow_up'}]->(:Hadith)
RETURN n.canonical_name, count(DISTINCT r1) AS primary_count, count(DISTINCT r2) AS followup_count
ORDER BY primary_count DESC;
```

### 4. Search matn segments
```cypher
MATCH (s:Segment)
WHERE s.text CONTAINS 'النية'
MATCH (h:Hadith)-[:HAS_SEGMENT]->(s)
RETURN h.hadith_index, h.source, s.text;
```

### 5. Narrator name variants
```cypher
MATCH (n:Narrator {narrator_id: 'NAR_A1B2C3D4E5F6'})
RETURN n.canonical_name, n.original_names;
```

---

## Timeline & Milestones

### Week 1: Development
- [ ] Day 1-2: Create `ingest_v3.py` skeleton
- [ ] Day 3: Implement data extraction functions
- [ ] Day 4: Implement batch creation functions
- [ ] Day 5: Add CLI, logging, error handling

### Week 2: Testing & Refinement
- [ ] Day 1-2: Unit tests for all functions
- [ ] Day 3: Integration test with 10 hadiths
- [ ] Day 4: Full dataset test (7,563 hadiths)
- [ ] Day 5: Performance tuning, documentation

### Week 3: Deployment
- [ ] Day 1: Final validation
- [ ] Day 2: Production ingestion
- [ ] Day 3: Verification queries
- [ ] Day 4-5: Buffer for issues

---

## Success Criteria

- ✅ Zero narrator duplicates (verified by `narrator_id` uniqueness)
- ✅ All hadiths ingested (7,563 for Bukhari)
- ✅ All chains tracked (primary + follow-ups)
- ✅ All segments linked to hadiths
- ✅ No orphan nodes or relationships
- ✅ Query performance < 100ms for common queries
- ✅ Complete documentation and runbook

---

## Future Enhancements (Post-MVP)

### Phase 2: Vector Embeddings
- Generate embeddings for `Segment.embedding`
- Create vector index for semantic search
- Implement hybrid search (keyword + semantic)

### Phase 3: Biographical Data
- Add properties to `Narrator`: birth_date, death_date, generation, reliability
- Create `(:Narrator)-[:STUDIED_UNDER]->(:Narrator)` relationships
- Integrate external data sources

### Phase 4: Multi-Collection Support
- Ingest Sahih Muslim collection
- Cross-collection narrator analysis
- Unified search across collections

---

## Notes

- This plan assumes `Bukhari_Normalized_Ready_For_Graph.json` is the single source of truth
- Narrator IDs are deterministic from normalization (same canonical name = same ID)
- Schema supports future vector embeddings without breaking changes
- All timestamps use ISO 8601 format with timezone
