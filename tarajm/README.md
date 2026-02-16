# Tarajm People Scraper

Scrapes narrator (rawi) biography pages from tarajm.com and saves structured data to CSV.

## How It Works

The scraper uses an **automated discovery crawler** that runs in a single process:

```
seed IDs (ids.txt)
    |
    v
Initialize queue with unscraped seed IDs
    |
    v
┌───────────────────────────────────┐
│ while queue is not empty:         │
│  1. Pop ID from queue             │
│  2. Check if already scraped      │
│  3. Scrape /people/{id} page      │
│  4. Save to CSV                   │
│  5. Extract new /people/{id} URLs │
│  6. Add new IDs to queue          │
│  7. Update ids_status.json        │
└───────────────────────────────────┘
    |
    v
Complete when queue is exhausted
```

Each person page contains links to other people (teachers, students, etc.), so scraping
one person often reveals IDs we didn't know about. The crawler automatically discovers
and queues new IDs until the entire connected network is scraped.

## Files

### Main Script (Recommended)

| File | Purpose |
|---|---|
| `tarajm_crawler.py` | **🔥 Main unified crawler.** Automatically scrapes, discovers, and queues new people IDs in a single run. Uses `ids.txt` as seed and `ids_status.json` for state tracking. No manual steps needed! |
| `ids.txt` | Seed list of people IDs (one per line). Starting point for the crawler. |
| `ids_status.json` | State tracking file: records which IDs are scraped/pending/failed, discovery lineage, timestamps, and error messages. |
| `out_people_csv/` | Output directory containing the CSV, and error logs. |

### Legacy Scripts (Old Multi-Step Workflow)

| File | Purpose |
|---|---|
| `tarajm_to_csv.py` | Old batch scraper. Reads IDs from file/CLI, scrapes each page, appends to CSV. Does NOT auto-discover new IDs. |
| `extract_people_ids.py` | Post-processing script: reads CSV, extracts `/people/{id}` hrefs, writes to txt file for next round. |
| `people_ids_extracted.txt` | IDs extracted by `extract_people_ids.py`. Fed back manually for next scraping round. |
| `scrape.py` | Discovers initial IDs by paginating through tarajm.com/search. |
| `tarajm.py` | Single-page scraper for testing/debugging extraction logic. Saves output as JSON. |

## Usage

### Quick Start (Recommended)

```bash
# Run the unified crawler with default settings
python tarajm_crawler.py

# That's it! The crawler will:
# 1. Load seed IDs from ids.txt
# 2. Scrape each ID and save to CSV
# 3. Automatically discover new people IDs from scraped data
# 4. Add new IDs to queue and continue
# 5. Stop when no new IDs are found
```

### CLI Options (tarajm_crawler.py)

| Flag | Default | Description |
|---|---|---|
| `--seed-file` | `ids.txt` | Path to seed IDs file (one per line) |
| `--state` | `ids_status.json` | State tracking JSON file |
| `--csv` | `out_people_csv/tarajm_people.csv` | Output CSV path |
| `--sleep` | 0.3 | Seconds to wait between requests |
| `--timeout` | 30 | HTTP timeout in seconds |
| `--retries` | 3 | Max retries per request |
| `--backoff` | 0.6 | Backoff base (exponential) for retries |
| `--max-ids` | 0 | Maximum IDs to scrape (0 = unlimited) |

### Advanced Usage

```bash
# Custom seed file and limit total scrapes
python tarajm_crawler.py --seed-file custom_ids.txt --max-ids 100

# Faster scraping (no sleep, use with caution)
python tarajm_crawler.py --sleep 0

# Resume a previous run (automatically skips already scraped IDs)
python tarajm_crawler.py
```

### Legacy Multi-Step Workflow (Old Method)

<details>
<summary>Click to expand old manual workflow</summary>

**Step 1:** Discover seed IDs (optional)
```bash
python scrape.py
# Manually copy printed IDs into ids.txt
```

**Step 2:** Run batch scraper
```bash
python tarajm_to_csv.py --ids-file ids.txt --skip-scraped --sleep 0.3
```

**Step 3:** Extract newly discovered IDs
```bash
python extract_people_ids.py
# Writes to people_ids_extracted.txt
```

**Step 4:** Re-run with discovered IDs
```bash
python tarajm_to_csv.py --ids-file people_ids_extracted.txt --skip-scraped --sleep 0.3
```

Repeat steps 3-4 until no new IDs appear.

</details>

## Output Schema (CSV)

| Column | Description |
|---|---|
| `id` | Person ID from tarajm.com |
| `url` | Full URL of the scraped page |
| `http_status` | HTTP response code |
| `scraped_at_utc` | Timestamp of when the page was scraped |
| `name` | Person's name (h1) |
| `summary` | Short summary paragraph |
| `translation` | Full biography text (tarjama) |
| `fields_json` | Structured fields (JSON): birth, death, teachers, students, etc. |
| `page_sections_json` | List of section headings on the page (JSON) |
| `all_hrefs` | All hrefs found in the extracted data (newline-separated) |

## Dependencies

```bash
pip install requests beautifulsoup4
```

## How ID Discovery Works

The crawler only extracts and follows **people URLs** (e.g., `https://tarajm.com/people/96459`):

1. **Scrapes a person page** and extracts structured data
2. **Searches `fields_json` and `all_hrefs`** for URLs matching pattern `/people/(\d+)`
3. **Extracts numeric IDs** (e.g., `96459`, `11000`)
4. **Ignores other URLs** like `/hadith/`, `/book/`, etc.
5. **Checks if ID already exists** in `ids_status.json`
6. **Adds new IDs to queue** with `discovered_from` tracking
7. **Continues until queue is empty**

This creates a **breadth-first crawl** of the entire connected network of people (narrators, teachers, students, etc.).

## State Tracking (ids_status.json)

The crawler maintains a comprehensive state file:

```json
[
  {
    "id": 10083,
    "scraped": true,
    "status": "scraped",
    "http_status": 200,
    "last_attempt": "2026-02-11T10:30:00Z",
    "attempts": 1,
    "discovered_from": "seed_file"
  },
  {
    "id": 96459,
    "scraped": true,
    "status": "scraped",
    "http_status": 200,
    "discovered_from": "person_10083",
    "hrefs_count": 23
  },
  {
    "id": 11000,
    "scraped": false,
    "status": "failed",
    "http_status": 404,
    "error": "HTTP 404",
    "attempts": 3,
    "discovered_from": "person_10083"
  }
]
```

**Features:**
- ✅ Prevents re-scraping already completed IDs
- ✅ Tracks discovery lineage (`discovered_from`)
- ✅ Records success/failure status and error messages
- ✅ Counts retry attempts
- ✅ Safe to stop and resume (state saved after each ID)

## Known Issues (Legacy Scripts)

- `tarajm_to_csv.py` uses its own `out_people_csv/tarajm_state.json` instead of `ids_status.json`
- `tarajm.py` and `tarajm_to_csv.py` duplicate ~200 lines of extraction logic
- `scrape.py` prints IDs to stdout but doesn't write them to a file

**Solution:** Use `tarajm_crawler.py` instead, which consolidates everything into a single automated workflow.
