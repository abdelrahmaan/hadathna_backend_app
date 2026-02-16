# Hadith Narrator Graph - Project Tasks

## Completed Tasks

### Frontend (v1 - Mock Data)

- [x] **Project Setup**
  - Next.js 16 with App Router and TypeScript
  - Tailwind CSS for styling
  - Arabic font support (Noto Naskh Arabic)
  - RTL text direction support

- [x] **Search Page** (`/`)
  - Search input with Arabic text support
  - Filter dropdown for source selection (all/bukhari/muslim)
  - Results list showing hadith index, source, snippet, chain count
  - Responsive layout with loading states

- [x] **Hadith Detail Page** (`/hadith/[source]/[index]`)
  - Dynamic routing with source and hadith index
  - Full hadith text display (RTL, large Arabic font)
  - Header with hadith number and source badge
  - Back navigation to search page

- [x] **Chain List View** (Primary View)
  - Each chain displayed as sequential flow
  - RTL arrows between narrators
  - Clickable narrator names
  - Chain numbering and narrator count

- [x] **Graph View** (Secondary View)
  - Pure SVG graph visualization (no external library)
  - Nodes = narrators, edges = narration links
  - Color-coded by chain
  - Shows nodes that appear in multiple chains with badges
  - Clickable nodes
  - Arrow indicators for direction

- [x] **Narrator Side Panel**
  - Slides in from right
  - Shows narrator name (Arabic)
  - Chain count within current hadith
  - Lists immediate neighbors (before/after in chain)
  - Click outside or ESC to close

### Supabase Migration (v2)

- [x] **Database Schema Created**
  - `hadiths` table: id, source, hadith_index, text
  - `narrators` table: id, source, norm, name, full_name
  - `chains` table: id, hadith_id, chain_id, length
  - `chain_narrators` table: id, chain_id, narrator_id, position

- [x] **Frontend Updated for Supabase**
  - Added `@supabase/supabase-js` dependency
  - Created `lib/supabase.ts` singleton client
  - Created `lib/api.ts` with Supabase queries
  - Created `lib/types.ts` with TypeScript types
  - Created `lib/utils.ts` with helper functions
  - Updated `.env.local` with Supabase credentials

- [x] **Build Verification**
  - Project builds successfully with no errors
  - All TypeScript types properly defined

---

## Current Task: Data Import

### Import Neo4j Data to Supabase (Local)

Run these commands from the project root directory.

**Step 1: Install Python dependencies**

```bash
pip install supabase python-dotenv
```

**Step 2: Create the import script**

Create a file called `import_to_supabase.py`:

```python
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from supabase import create_client

load_dotenv()

# Neo4j connection
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Supabase connection
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def import_data():
    with neo4j_driver.session() as session:
        # 1. Get all hadiths
        print("Fetching hadiths from Neo4j...")
        hadiths_result = session.run("""
            MATCH (h:Hadith)
            RETURN h.source as source, h.hadith_index as hadith_index, h.text as text
            ORDER BY h.source, h.hadith_index
        """)

        hadiths = []
        hadith_map = {}  # (source, index) -> uuid

        for record in hadiths_result:
            hadiths.append({
                "source": record["source"],
                "hadith_index": record["hadith_index"],
                "text": record["text"]
            })

        print(f"Inserting {len(hadiths)} hadiths...")
        for hadith in hadiths:
            result = supabase.table("hadiths").upsert(hadith, on_conflict="source,hadith_index").execute()
            if result.data:
                key = (hadith["source"], hadith["hadith_index"])
                hadith_map[key] = result.data[0]["id"]

        # 2. Get all narrators
        print("Fetching narrators from Neo4j...")
        narrators_result = session.run("""
            MATCH (n:Narrator)
            RETURN n.source as source, n.norm as norm, n.name as name, n.full_name as full_name
        """)

        narrators = []
        narrator_map = {}  # (source, norm) -> uuid

        for record in narrators_result:
            narrators.append({
                "source": record["source"],
                "norm": record["norm"],
                "name": record["name"],
                "full_name": record.get("full_name")
            })

        print(f"Inserting {len(narrators)} narrators...")
        for narrator in narrators:
            result = supabase.table("narrators").upsert(narrator, on_conflict="source,norm").execute()
            if result.data:
                key = (narrator["source"], narrator["norm"])
                narrator_map[key] = result.data[0]["id"]

        # 3. Get all chains with their narrators
        print("Fetching chains from Neo4j...")
        chains_result = session.run("""
            MATCH (h:Hadith)-[:HAS_CHAIN]->(c:Chain)
            OPTIONAL MATCH (c)-[r:HAS_NARRATOR]->(n:Narrator)
            RETURN h.source as source, h.hadith_index as hadith_index,
                   c.chain_id as chain_id, c.length as length,
                   n.norm as narrator_norm, r.position as position
            ORDER BY h.source, h.hadith_index, c.chain_id, r.position
        """)

        chains = {}  # (hadith_id, chain_id) -> {chain_data, narrators: []}

        for record in chains_result:
            hadith_key = (record["source"], record["hadith_index"])
            hadith_id = hadith_map.get(hadith_key)
            if not hadith_id:
                continue

            chain_key = (hadith_id, record["chain_id"])

            if chain_key not in chains:
                chains[chain_key] = {
                    "hadith_id": hadith_id,
                    "chain_id": record["chain_id"],
                    "length": record["length"] or 0,
                    "narrators": []
                }

            if record["narrator_norm"]:
                narrator_key = (record["source"], record["narrator_norm"])
                narrator_id = narrator_map.get(narrator_key)
                if narrator_id:
                    chains[chain_key]["narrators"].append({
                        "narrator_id": narrator_id,
                        "position": record["position"]
                    })

        print(f"Inserting {len(chains)} chains...")
        for chain_key, chain_data in chains.items():
            chain_insert = {
                "hadith_id": chain_data["hadith_id"],
                "chain_id": chain_data["chain_id"],
                "length": chain_data["length"]
            }
            result = supabase.table("chains").upsert(chain_insert, on_conflict="hadith_id,chain_id").execute()

            if result.data:
                chain_uuid = result.data[0]["id"]

                # Insert chain_narrators
                for narrator in chain_data["narrators"]:
                    cn_insert = {
                        "chain_id": chain_uuid,
                        "narrator_id": narrator["narrator_id"],
                        "position": narrator["position"]
                    }
                    supabase.table("chain_narrators").upsert(cn_insert, on_conflict="chain_id,position").execute()

        print("Import complete!")

if __name__ == "__main__":
    import_data()
    neo4j_driver.close()
```

**Step 3: Update your `.env` file**

Make sure your `.env` file has all required variables:

```
# Neo4j (your existing local database)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

# Supabase
SUPABASE_URL=https://tklaauroiugionhhflar.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

**Step 4: Run the import**

```bash
python import_to_supabase.py
```

**Step 5: Verify the data**

Check the Supabase dashboard or run:

```sql
SELECT COUNT(*) FROM hadiths;
SELECT COUNT(*) FROM narrators;
SELECT COUNT(*) FROM chains;
SELECT COUNT(*) FROM chain_narrators;
```

---

## Next Steps

### Phase 1: Data Enrichment (Optional)

1. **Add Hadith Metadata**
   - Add hadith categories/topics
   - Add hadith grades (Sahih, Hasan, etc.)

2. **Narrator Information**
   - Add narrator biographical data
   - Birth/death dates
   - Reliability ratings

3. **Enhanced Search**
   - Full-text search in Arabic
   - Search by narrator name
   - Filter by hadith grade

---

### Phase 2: UI/UX Improvements

1. **Search Enhancements**
   - Autocomplete for Arabic text
   - Pagination for large result sets
   - Sort options

2. **Detail Page Enhancements**
   - Print/export hadith and chains
   - Share links
   - Narrator tooltips on hover

3. **Mobile Optimization**
   - Better touch interactions for graph
   - Responsive narrator panel

---

### Phase 3: Deployment

1. **Frontend Deployment**
   - Deploy Next.js to Vercel
   - Configure environment variables

2. **Monitoring**
   - Add error tracking
   - Monitor API performance

---

## Technical Decisions

1. **Supabase Migration**: Replaced Neo4j + Python backend with Supabase for simpler deployment
2. **Direct Client Queries**: Frontend queries Supabase directly (no separate backend needed)
3. **RLS Policies**: Public read access for hadith data (Islamic knowledge is public)
4. **Pure SVG Graph**: No external graph library dependency

---

## Progress Summary

- **Frontend**: 100% complete (Next.js + Supabase)
- **Supabase Schema**: 100% complete
- **Data Import**: 0% complete (script ready, needs execution)
- **Extract Data V2 Pipeline**: 90% complete (75% coverage, 95% achievable)
  - Chain extraction: 100% (7,563 hadiths)
  - Disambiguation engine: 100% (98.3% of ambiguous pairs resolved)
  - Name normalization: 75% coverage (33,560/44,733 mentions)
  - Coverage improvement: pending (auto-add full names for ~95%)
- **Neo4j Ingestion v3**: 0% (planned)
- **Deployment**: 0% complete

**Overall Progress**: ~65% complete

---

## Extract Data V2 Pipeline (Current Focus)

### Chain Extraction (COMPLETE)
- [x] `advanced_extractions_llm_pydantic_with_matn.py` - GPT-4o extraction with Pydantic
- [x] Matn segmentation and chain structure validation
- [x] 7,563 hadiths processed from Bukhari

### Disambiguation Engine (COMPLETE - core)
- [x] `extract_ambiguous_context.py` - Extract 1,555 ambiguous name-student pairs
- [x] `solve_ambiguity.py` - Rule engine (~200 rules from علم الرجال)
  - [x] Unambiguous dictionary (260+ entries)
  - [x] Student-based context rules (14 ambiguous names)
  - [x] Pronoun resolution (117+ father-son pairs)
  - [x] 1,529/1,555 pairs resolved (98.3% coverage)
- [x] Bug fixes applied:
  - [x] Removed البخاري→ابن المبارك fallback (historically impossible - 13 year gap)
  - [x] Fixed هشام+يحيى rule (يحيى القطان narrated from ALL 3 Hishams - marked غامض)
  - [x] Corrected statistics inflation (separated unambiguous from true disambiguation)

### Name Normalization (COMPLETE)
- [x] `narrators_mapping.py` - 3-step resolution with honest categorized counting
- [x] Current coverage: **75.0% (33,560/44,733 mentions)**
  - Unambiguous: 13,894 (31.1%)
  - Context-disambiguated: 2,718 (6.1%)
  - Pronoun-resolved: 781 (1.7%)
  - Static mapping: 16,167 (36.1%)
  - Unmapped: 11,173 (25.0%)
- [x] 186 remaining ambiguous pairs cataloged in `remaining_ambiguous_pairs.json`

### Coverage Improvement (Next)
- [ ] Auto-add ~2,303 full canonical names to `narrator_mappings.json` (→ ~95% coverage)
- [ ] Resolve remaining 186 ambiguous pairs (923 mentions) using teacher-context lookup
- [ ] Fix عبة → شعبة data extraction error (6 pairs, 19 mentions)

---

## Phase 1: Narrator Normalization & Entity Resolution 🧠 **[MOSTLY COMPLETE]**

**Status:** Core normalization is done via `extract_data_v2/`. Remaining work is coverage improvement.

### Completed (via extract_data_v2)
- [x] Extracted all unique narrator names from Bukhari (7,563 hadiths)
- [x] Created disambiguation engine with ~200 rules based on علم الرجال
- [x] Built 3-step resolution pipeline (context → static → identity)
- [x] Generated unique IDs: `NAR_<12-char-SHA256-hash>` (deterministic, collision-free)
- [x] Produced `Bukhari_Normalized_Ready_For_Graph.json` with canonical names and IDs
- [x] Produced `narrators_nodes.csv` for Neo4j LOAD CSV
- [x] Current coverage: **75.0% (33,560/44,733 mentions)**

### Remaining
- [ ] Boost coverage to ~95% by auto-adding ~2,303 full canonical names to dictionary
- [ ] Entity clustering for remaining kunya/nisba variations (الزهري = ابن شهاب = محمد بن مسلم)
- [ ] LLM-assisted resolution for 186 remaining ambiguous pairs

---

## Phase 2: Vector Embeddings for Semantic Search 📉

Enable semantic search on hadith text (not just keyword matching).

### Goal

Convert text in `matn_segments` into vector embeddings for semantic similarity search.

### Implementation Tasks

#### 2.1 Model Selection

- [ ] Research and compare Arabic embedding models:
  - [ ] OpenAI `text-embedding-3-small` (multilingual, good for Arabic)
  - [ ] `intfloat/multilingual-e5-large` (open source)
  - [ ] `sentence-transformers/paraphrase-multilingual-mpnet-base-v2`
  - [ ] Arabic-specific models (CAMeL, AraVec, etc.)
- [ ] Benchmark models on sample hadiths
- [ ] Select model based on quality vs cost/speed

#### 2.2 Embedding Generation

- [ ] Create `generate_embeddings.py`:
  - [ ] Load hadith segments from JSON
  - [ ] Batch process segments through embedding model
  - [ ] Handle rate limits and retries
  - [ ] Progress tracking and resume capability
  - [ ] Store embeddings with metadata (hadith_id, segment_id)

#### 2.3 Embedding Storage

- [ ] **Option A: Store in JSON**
  - [ ] Add `embedding` field to each segment
  - [ ] Compress vectors to reduce file size

- [ ] **Option B: Dedicated Vector Database**
  - [ ] Evaluate Qdrant, Pinecone, Weaviate, or Neo4j Vector Index
  - [ ] Set up vector database
  - [ ] Create ingestion pipeline
  - [ ] Index embeddings with metadata

#### 2.4 Semantic Search Implementation

- [ ] Create `semantic_search.py`:
  - [ ] Implement query embedding generation
  - [ ] Cosine similarity search
  - [ ] Hybrid search (combine keyword + semantic)
  - [ ] Result ranking and filtering
- [ ] Integrate with frontend search API

#### 2.5 Testing & Evaluation

- [ ] Create test queries in Arabic
- [ ] Compare semantic search vs keyword search results
- [ ] Measure search quality (relevance, precision)
- [ ] Performance benchmarks (query latency, throughput)

---

## Phase 3: Graph Construction & Ingestion 🕸️

Convert flat JSON structure into a rich graph database in Neo4j.

### Goal

Create a comprehensive graph with proper nodes, relationships, and metadata.

### Graph Schema Design

#### Nodes

- **`Narrator`** - Individual narrator entity
  - Properties: `unique_id`, `canonical_name`, `name_variants[]`, `source`

- **`Hadith`** - Complete hadith
  - Properties: `id`, `source`, `hadith_index`, `full_text`, `grade?`

- **`Segment`** - Matn segment
  - Properties: `id`, `text`, `position`, `embedding?`

#### Relationships

- **`(:Narrator)-[:NARRATED]->(:Narrator)`** - Chain link
  - Properties: `chain_id`, `hadith_id`, `position`

- **`(:Narrator)-[:NARRATED_HADITH]->(:Hadith)`** - Top narrator to hadith
  - Properties: `chain_id`, `chain_type` (primary/follow_up)

- **`(:Hadith)-[:HAS_SEGMENT]->(:Segment)`** - Hadith to segment
  - Properties: `position`

### Implementation Tasks

#### 3.1 Update Neo4j Schema

- [ ] Update `neo4j_client.py`:
  - [ ] Add constraints for `Narrator.unique_id`
  - [ ] Update `Hadith` node structure
  - [ ] Create `Segment` node support
  - [ ] Add new relationship types

#### 3.2 Enhanced Ingestion Pipeline

- [ ] Create `ingest_v3.py`:
  - [ ] Read JSON with narrator IDs (from Phase 1)
  - [ ] Create/merge `Narrator` nodes with all variants
  - [ ] Create `Hadith` nodes with segments
  - [ ] Build chain relationships using narrator IDs
  - [ ] Add segment nodes and relationships
  - [ ] Batch processing with progress tracking

#### 3.3 Data Validation

- [ ] Verify narrator nodes have no duplicates
- [ ] Check all chains have valid narrator references
- [ ] Validate relationship counts match expectations
- [ ] Test graph queries for correctness

#### 3.4 Migration from Old Data

- [ ] Create migration script if old data exists
- [ ] Clear old data or create new database
- [ ] Import new structured data
- [ ] Verify data integrity

---

## Phase 4: Data Enrichment ✨

Add biographical and scholarly data to narrators.

### Goal

Enrich narrator nodes with additional metadata from Islamic scholarly sources.

### Data to Add

- **Biographical Data**:
  - Birth date (هجري/ميلادي)
  - Death date
  - Place of birth/residence
  - Teachers (شيوخ)
  - Students (تلاميذ)
  - Generation/Tabaqah (طبقة)

- **Scholarly Assessment (الجرح والتعديل)**:
  - Reliability rating (ثقة, صدوق, ضعيف, متروك, etc.)
  - Scholar opinions from different sources
  - Specific strengths/weaknesses

### Implementation Tasks

#### 4.1 Data Source Research

- [ ] Identify available data sources:
  - [ ] Open datasets (if available)
  - [ ] Digitized biography books (تهذيب التهذيب, etc.)
  - [ ] APIs or databases with narrator information
- [ ] Evaluate data quality and coverage
- [ ] Determine licensing/usage permissions

#### 4.2 Data Extraction & Mapping

- [ ] Create `narrator_enrichment.py`:
  - [ ] Parse source data files
  - [ ] Extract relevant fields
  - [ ] Map external narrator names to our `unique_id`
  - [ ] Handle multiple sources with conflicting data

#### 4.3 Graph Integration

- [ ] Update `Narrator` node properties with enriched data
- [ ] Create new relationships:
  - [ ] `(:Narrator)-[:STUDIED_UNDER]->(:Narrator)` - teacher-student
  - [ ] `(:Narrator)-[:CONTEMPORARY_OF]->(:Narrator)` - same generation
- [ ] Add metadata properties for source citation

#### 4.4 Validation & Quality Control

- [ ] Verify enrichment coverage (% of narrators enriched)
- [ ] Cross-reference dates and relationships for consistency
- [ ] Manual review of high-importance narrators
- [ ] Document data provenance

---

## Progress Tracking

### Phase Priorities

1. **Extract Data V2 Pipeline**: ✅ **COMPLETE** (core) - 75% coverage, 95% achievable
2. **Phase 1 (Narrator Normalization)**: 🟡 **MOSTLY COMPLETE** - coverage boost + entity clustering remaining
3. **Phase 3 (Graph Construction)**: 🟢 Next step - Neo4j ingestion with normalized data
4. **Phase 2 (Vector Embeddings)**: 🔵 Future - semantic search
5. **Phase 4 (Data Enrichment)**: 🔵 Future - biographical data

---

## Notes

### Narrator Normalization Status

The core narrator normalization is now implemented in `extract_data_v2/`. The 3-step pipeline resolves 75% of all narrator mentions (33,560/44,733). Key insight: **78% of unmapped names are already full canonical names** just missing from the static dictionary - adding them would push coverage to ~95%.

### Remaining Entity Duplication Risk

Some names still need clustering (e.g., الزهري = ابن شهاب = محمد بن مسلم). The current system resolves short → full names but doesn't yet cluster different aliases of the same person. This is needed before Neo4j ingestion to avoid duplicate nodes.
