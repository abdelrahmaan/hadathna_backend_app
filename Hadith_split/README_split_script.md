# Hadith Sanad/Matn Splitting Script

This script processes Arabic hadiths and splits each into:
- **sanad**: The chain of narrators
- **matn**: The prophetic statement/content

## Requirements

```bash
pip install pandas langchain-openai python-dotenv tqdm pydantic
```

## Setup

1. Ensure you have an OpenAI API key in your `.env` file:
   ```
   OPENAI_API_KEY=sk-...
   ```

2. The script will automatically search for input CSV files in these locations:
   - `bukhari_hadiths_df.csv` (current directory)
   - `../data/Sahih Bukhari Without_Tashkel.csv`
   - `../extract_data_v2/Bukhari/Bukhari_Without_Tashkel.csv`

## Usage

```bash
cd Hadith_split
python split_hadith_to_matn_and_sanad.py
```

## Output Format

The script creates `bukhari_hadiths_split.json` with this structure:

```json
[
  {
    "hadith_index": 1,
    "hadith_text": "حدثنا الحميدي عبد الله بن الزبير...",
    "sanad": "حدثنا الحميدي عبد الله بن الزبير... عن عمر بن الخطاب",
    "matn": "عن عمر بن الخطاب رضي الله عنه... إنما الأعمال بالنيات..."
  },
  ...
]
```

## Features

- **Resume Support**: If interrupted, run again to continue from where it stopped
- **Batch Processing**: Processes 20 hadiths at a time for efficiency
- **Error Handling**: Falls back to sequential processing if batch fails
- **Progress Tracking**: Shows progress bar during processing
- **Flexible Input**: Handles different CSV formats automatically

## Configuration

Edit these constants in the script to customize:

```python
BATCH_SIZE = 20          # Number of hadiths per batch
MODEL = "gpt-5-mini"     # OpenAI model to use
TEMPERATURE = 0          # LLM temperature (0 = deterministic)
```

## Notes

- Processing ~7000 hadiths takes approximately 45-60 minutes
- The script uses GPT-5-mini for cost-effective extraction
- Output is sorted by hadith_index
- Invalid extractions are skipped with a warning
