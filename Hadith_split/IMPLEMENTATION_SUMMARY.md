# Hadith Sanad/Matn Splitting - Implementation Summary

## ✅ What Was Created

### 1. Main Script: `split_hadith_to_matn_and_sanad.py`
A complete Python script that:
- Converts the Jupyter notebook functionality into a standalone script
- Processes hadiths and splits them into sanad (narrator chain) and matn (prophetic statement)
- Outputs JSON format instead of CSV
- Includes resume capability for interrupted runs

**Key Features:**
- ✅ Batch processing (20 hadiths at a time)
- ✅ Error handling with fallback to sequential processing
- ✅ Progress tracking with tqdm
- ✅ Resume support (continues from where it stopped)
- ✅ Flexible input CSV handling (auto-detects format)
- ✅ Output format: `hadith_index`, `hadith_text`, `sanad`, `matn`

### 2. Documentation: `README_split_script.md`
User-friendly guide covering:
- Requirements and setup
- Usage instructions
- Output format examples
- Configuration options

### 3. Test Script: `test_split_script.py`
Helper script to:
- Create sample test data
- Validate the splitting functionality
- Show expected output format

## 📋 Output Format

The script produces `bukhari_hadiths_split.json`:

```json
[
  {
    "hadith_index": 1,
    "hadith_text": "حدثنا الحميدي عبد الله بن الزبير...",
    "sanad": "حدثنا الحميدي عبد الله بن الزبير... عن عمر بن الخطاب",
    "matn": "عن عمر بن الخطاب رضي الله عنه... إنما الأعمال بالنيات..."
  },
  {
    "hadith_index": 2,
    "hadith_text": "...",
    "sanad": "...",
    "matn": "..."
  }
]
```

**Field Descriptions:**
- `hadith_index`: Original hadith ID from source CSV (preserves traceability)
- `hadith_text`: Complete Arabic hadith text
- `sanad`: Chain of narrators (ends at the upper narrator)
- `matn`: Prophetic statement (starts from upper narrator)

## 🔧 How to Use

### Prerequisites

```bash
pip install pandas langchain-openai python-dotenv tqdm pydantic
```

Ensure `.env` file contains:
```
OPENAI_API_KEY=sk-your-key-here
```

### Running the Script

```bash
cd Hadith_split
python3 split_hadith_to_matn_and_sanad.py
```

The script will:
1. Automatically find the input CSV (searches multiple locations)
2. Load existing results if any (resume support)
3. Process remaining hadiths in batches
4. Save to `bukhari_hadiths_split.json`

### Input CSV Options

The script automatically searches for:
1. `bukhari_hadiths_df.csv` (in current directory)
2. `../data/Sahih Bukhari Without_Tashkel.csv`
3. `../extract_data_v2/Bukhari/Bukhari_Without_Tashkel.csv`

**Supported CSV Formats:**
- With `id` or `hadith_id` column + `text_ar` or `hadith_text` column
- Headless CSV (auto-generates sequential IDs)
- Any CSV with hadith text column (auto-detects and adapts)

## 🎯 Key Differences from Notebook

| Aspect | Notebook | Script |
|--------|----------|--------|
| Output Format | CSV (incremental append) | JSON (full file) |
| Output Fields | hadith, sanad, upper_narrator, matn | hadith_index, hadith_text, sanad, matn |
| Index Source | Row position | Original DataFrame ID column |
| Resume Logic | Check CSV row count | Check JSON hadith_index values |
| Input Flexibility | Single hardcoded path | Multiple auto-detected paths |

## ⚙️ Configuration

Edit these constants in the script:

```python
INPUT_CSV_OPTIONS = [...]  # Add custom CSV paths here
OUTPUT_JSON = "bukhari_hadiths_split.json"
BATCH_SIZE = 20
MODEL = "gpt-5-mini"
TEMPERATURE = 0
```

## 📊 Expected Performance

- **Processing Speed**: ~20 hadiths per batch
- **Total Time** (7000 hadiths): ~45-60 minutes
- **Output Size**: ~10-15 MB JSON file
- **API Costs**: Depends on OpenAI pricing for gpt-5-mini

## 🔍 Validation

To validate the output:

1. **Check JSON structure:**
   ```bash
   python3 -c "import json; print(json.load(open('bukhari_hadiths_split.json'))[:2])"
   ```

2. **Count processed hadiths:**
   ```bash
   python3 -c "import json; print(f'{len(json.load(open(\"bukhari_hadiths_split.json\")))} hadiths processed')"
   ```

3. **Compare with notebook:**
   - Process same hadith in both notebook and script
   - Compare sanad/matn outputs character-by-character
   - Should be identical (same model, prompt, temperature=0)

## 🚨 Error Handling

The script handles:
- **Missing input file**: Clear error message with search paths
- **Batch failures**: Automatic fallback to sequential processing
- **Individual hadith errors**: Skips with warning, continues processing
- **Interrupted runs**: Resume from last successful hadith

## 📁 File Locations

```
Hadith_split/
├── split_hadith_to_matn_and_sanad.py   # Main script
├── README_split_script.md               # User documentation
├── test_split_script.py                 # Testing helper
├── IMPLEMENTATION_SUMMARY.md            # This file
└── bukhari_hadiths_split.json          # Output (created when run)
```

## ✨ Next Steps

1. **Install dependencies** if not already installed
2. **Verify .env** has OPENAI_API_KEY
3. **Run the script**: `python3 split_hadith_to_matn_and_sanad.py`
4. **Monitor progress** via tqdm progress bar
5. **Check output** in `bukhari_hadiths_split.json`

## 📝 Notes

- The script uses the same LLM extraction logic as the notebook
- Output is deterministic (temperature=0)
- Resume support allows interrupting and continuing later
- Invalid extractions are skipped (not included in output)
- Output JSON is sorted by hadith_index for consistency

---

**Created**: 2026-02-08
**Based on**: `2_1_split_hadith_to_matn_and_sanad.ipynb`
**Model Used**: gpt-5-mini (OpenAI)
