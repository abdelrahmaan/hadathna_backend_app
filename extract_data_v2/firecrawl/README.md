# Firecrawl Data Pipeline

This directory contains scripts and outputs for scraping Shamela hadith pages and narrator profiles.

## Files In This Directory

- `shamela_firecrawl.py`
  - Main scraper for book pages (`/book/1681/<page>`).
  - Extracts hadith blocks, matn, narrators, breadcrumb links.
  - Writes page results to `shamela_book_1681.jsonl`.

- `retry_failed.py`
  - Retries failed pages from `shamela_book_1681.jsonl`.
  - Skips pages where failure reason is `no_narrators`.
  - Removes old failed rows for retryable pages, then appends fresh retry results.

- `shamela_narrator_scraper.py`
  - Scrapes narrator profile pages (`/narrator/<id>`).
  - Extracts fields like name/kunya/nasab/death date/ranks/jarh wa ta'dil.
  - Writes results to `shamela_narrators.jsonl` as `status=success|failed`.
  - Also builds `narrator_hadith_names.json` from hadith data.

- `narrators_info_check.py`
  - Coverage + consistency checker between:
    - `shamela_book_1681.jsonl`
    - `narrator_hadith_names.json`
    - `shamela_narrators.jsonl`
  - Uses `status=success` rows from `shamela_narrators.jsonl` to decide "scraped" IDs.
  - Also checks name-variant completeness per narrator ID (not only ID existence).
  - Can:
    - append missing narrator IDs into `shamela_narrators.jsonl`
    - call `scrape_narrators(...)` from `shamela_narrator_scraper.py` for missing IDs
    - expand missing name variants into `narrator_hadith_names.json`

- `shamela_book_1681.jsonl`
  - Main page-level scrape output (JSONL).

- `shamela_narrators.jsonl`
  - Narrator profile scrape output (JSONL).

- `narrator_hadith_names.json`
  - Map: `narrator_id -> [name variants seen in hadith chains]`.

- `failure_report_1681.json`
  - Failure summary for book page scraping.

- `debug_html_1681/`
  - Saved HTML snapshots for page scraper debugging.

- `debug_html_1681_retry/`
  - Saved HTML snapshots for retry run debugging.

- `__pycache__/`
  - Python bytecode cache.

## Typical Workflow

1. Scrape/refresh hadith book pages.
2. Retry failed pages.
3. Scrape narrator profiles.
4. Run consistency checks and fill gaps.

## Commands

Run from this directory:

```bash
cd extract_data_v2/firecrawl
```

Book pages:

```bash
python3 shamela_firecrawl.py
python3 retry_failed.py
```

Narrator profiles:

```bash
python3 shamela_narrator_scraper.py
```

Coverage and gap handling:

```bash
python3 narrators_info_check.py
python3 narrators_info_check.py --expand-narrator-hadith-names
python3 narrators_info_check.py --scrape-missing --api-key "YOUR_FIRECRAWL_KEY"
python3 narrators_info_check.py --scrape-missing --append-missing-to-shamela --api-key "YOUR_FIRECRAWL_KEY"
```

Use multiple keys by repeating `--api-key` or setting env var:

```bash
export FIRECRAWL_API_KEYS="key1,key2"
python3 narrators_info_check.py --scrape-missing
```

Useful optional flags:

- `--show-limit 50`
  - Change how many IDs are printed in terminal summaries.
- `--output-json report.json`
  - Save full check report to a JSON file.
- `--max-workers 2 --delay 3`
  - Control narrator scrape concurrency and pacing when using `--scrape-missing`.

## Output Conventions

- JSONL files are append-oriented logs.
- For narrator data:
  - `status=success` means profile exists and is scraped.
  - `status=failed` means scrape attempt failed or profile is empty/invalid.
- `narrators_info_check.py` treats only `status=success` as "already scraped".
- `IDs with missing name variants` means:
  - The narrator ID exists in `narrator_hadith_names.json`, but one or more names seen in hadith chains are missing from that ID's name list.
  - Use `--expand-narrator-hadith-names` to merge them.

## Known Behavior

- IDs like `0` and `1` may appear in hadith chains but resolve to empty narrator profile pages on Shamela.
- These IDs can remain `failed` in `shamela_narrators.jsonl` even after retries.
