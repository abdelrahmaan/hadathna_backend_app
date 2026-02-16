"""
Hadith Sanad/Matn Splitting Script

This script processes Arabic hadiths from a CSV file and splits each hadith into:
- sanad: The chain of narrators
- matn: The prophetic statement

Output is saved as JSON with structure:
{
    "hadith_index": int,
    "hadith_text": str,
    "sanad": str,
    "matn": str
}
"""

import pandas as pd
import json
from pathlib import Path
from typing import List, Dict
from tqdm import tqdm
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv


# =============================================================================
# Configuration
# =============================================================================

# Try multiple possible input file locations
INPUT_CSV_OPTIONS = [
    "bukhari_hadiths_df.csv",
    "../data/Sahih Bukhari Without_Tashkel.csv",
    "../extract_data_v2/Bukhari/Bukhari_Without_Tashkel.csv"
]
OUTPUT_JSON = "bukhari_hadiths_split.json"
BATCH_SIZE = 20
MODEL = "gpt-5-mini"
TEMPERATURE = 0


# =============================================================================
# Pydantic Model
# =============================================================================

class HadithParts(BaseModel):
    sanad: str
    upper_narrator: str
    matn: str


# =============================================================================
# Few-Shot Examples
# =============================================================================

examples = [
    {
        "hadith": "حدثنا مسلم، قال حدثنا حجاج، عن يونس، عن ابن شهاب، عن عبد الله بن عمر، قال: سمعت رسول الله ﷺ يقول: \"لا ضرر ولا ضرار\"",
        "sanad": "حدثنا مسلم، قال حدثنا حجاج، عن يونس، عن ابن شهاب، عن عبد الله بن عمر",
        "upper_narrator": "عبد الله بن عمر",
        "matn": "عن عبد الله بن عمر، قال: سمعت رسول الله ﷺ يقول: لا ضرر ولا ضرار"
    },
    {
        "hadith": "حدثنا الحميدي عبد الله بن الزبير، قال حدثنا سفيان، قال حدثنا يحيى بن سعيد الأنصاري، قال أخبرني محمد بن إبراهيم التيمي، أنه سمع علقمة بن وقاص الليثي، يقول سمعت عمر بن الخطاب رضى الله عنه على المنبر قال سمعت رسول الله ﷺ يقول: \"إنما الأعمال بالنيات، وإنما لكل امرئ ما نوى...\"",
        "sanad": "حدثنا الحميدي عبد الله بن الزبير، قال حدثنا سفيان، قال حدثنا يحيى بن سعيد الأنصاري، قال أخبرني محمد بن إبراهيم التيمي، أنه سمع علقمة بن وقاص الليثي، يقول سمعت عمر بن الخطاب",
        "upper_narrator": "عمر بن الخطاب",
        "matn": "عن عمر بن الخطاب رضي الله عنه قال: سمعت رسول الله ﷺ يقول: إنما الأعمال بالنيات، وإنما لكل امرئ ما نوى"
    },
    {
        "hadith": "حدثنا سعيد بن يحيى بن سعيد القرشي، قال حدثنا أبي قال، حدثنا أبو بردة بن عبد الله بن أبي بردة، عن أبي بردة، عن أبي موسى رضى الله عنه قال قالوا يا رسول الله أى الإسلام أفضل؟ قال: \"من سلم المسلمون من لسانه ويده\"",
        "sanad": "حدثنا سعيد بن يحيى بن سعيد القرشي، قال حدثنا أبي، قال حدثنا أبو بردة بن عبد الله بن أبي بردة، عن أبي بردة، عن أبي موسى",
        "upper_narrator": "أبو موسى",
        "matn": "عن أبي موسى رضي الله عنه قال: قالوا يا رسول الله أي الإسلام أفضل؟ قال: من سلم المسلمون من لسانه ويده"
    },
    {
        "hadith": "حدثنا عبد الله بن يوسف، قال أخبرنا مالك، عن عبد الله بن دينار، عن عبد الله بن عمر رضي الله عنهما أن رسول الله ﷺ قال: \"المسلم أخو المسلم، لا يظلمه ولا يسلمه\"",
        "sanad": "حدثنا عبد الله بن يوسف، قال أخبرنا مالك، عن عبد الله بن دينار، عن عبد الله بن عمر",
        "upper_narrator": "عبد الله بن عمر",
        "matn": "عن عبد الله بن عمر رضي الله عنهما قال: أن رسول الله ﷺ قال: المسلم أخو المسلم، لا يظلمه ولا يسلمه"
    },
    {
        "hadith": "حدثنا قتيبة بن سعيد، قال حدثنا ليث، عن ابن شهاب، عن أنس بن مالك، قال: قال رسول الله ﷺ: \"لا يؤمن أحدكم حتى يحب لأخيه ما يحب لنفسه\"",
        "sanad": "حدثنا قتيبة بن سعيد، قال حدثنا ليث، عن ابن شهاب، عن أنس بن مالك",
        "upper_narrator": "أنس بن مالك",
        "matn": "عن أنس بن مالك قال: قال رسول الله ﷺ: لا يؤمن أحدكم حتى يحب لأخيه ما يحب لنفسه"
    }
]


# =============================================================================
# Helper Functions
# =============================================================================

def escape_json_for_prompt(d: dict) -> str:
    """
    Converts a dictionary into escaped JSON block suitable for LangChain prompt.
    Turns { → {{ and } → }}
    """
    import json
    raw = json.dumps(d, ensure_ascii=False, indent=2)
    escaped = raw.replace("{", "{{").replace("}", "}}")
    return escaped


def build_few_shots(examples: List[dict]) -> str:
    """Build few-shot examples block for the prompt"""
    text = ""
    for ex in examples:
        escaped_out = escape_json_for_prompt({
            "sanad": ex["sanad"],
            "upper_narrator": ex["upper_narrator"],
            "matn": ex["matn"]
        })

        text += f"""
Example Input:
{ex['hadith']}

Example Output:
{escaped_out}

"""
    return text


# =============================================================================
# LLM Chain Setup
# =============================================================================

few_shots_text = build_few_shots(examples)

llm = ChatOpenAI(model=MODEL, temperature=TEMPERATURE)
structured_llm = llm.with_structured_output(HadithParts)

prompt = ChatPromptTemplate.from_messages([
    ("system", f"""
You are an expert in Hadith sciences. Extract the following:

1. sanad — full narrator chain before the Prophet ﷺ speaks.
2. upper_narrator — the narrator who directly heard the Prophet ﷺ.
3. matn — the prophetic statement starting from the upper narrator.

Guidelines:
- Do NOT rewrite the text.
- Keep sanad and matn exactly as written.
- Upper narrator = last narrator before the Prophet ﷺ quote.

Here are examples to follow:

{few_shots_text}

Return ONLY JSON in this schema:
{{format_instructions}}
"""),
    ("human", "Hadith:\n{text}")
])

chain = prompt | structured_llm


# =============================================================================
# Core Processing Functions
# =============================================================================

def split_hadith_batch(texts: List[str]) -> List[HadithParts]:
    """
    Process a batch of hadith texts with fallback to sequential processing.

    Args:
        texts: List of hadith texts in Arabic

    Returns:
        List of HadithParts objects containing sanad, upper_narrator, and matn
    """
    batch_inputs = [{"text": h} for h in texts]

    try:
        # Try processing the whole batch at once
        return chain.batch(batch_inputs)
    except Exception as e:
        print(f"Batch failed ({str(e)}). Switching to sequential processing...")
        results = []
        for item in batch_inputs:
            try:
                # Process one by one
                res = chain.invoke(item)
                results.append(res)
            except Exception as inner_e:
                print(f"Skipping hadith due to error: {inner_e}")
                # Append error placeholder
                results.append(HadithParts(
                    sanad="ERROR",
                    upper_narrator="ERROR",
                    matn="ERROR"
                ))
        return results


def process_all_hadiths(df: pd.DataFrame, batch_size: int, output_file: Path):
    """
    Main processing loop with resume support.

    Args:
        df: DataFrame containing hadiths with columns: id/hadith_id and text_ar
        batch_size: Number of hadiths to process in each batch
        output_file: Path to output JSON file
    """
    # Load existing results if any
    existing_results = []
    processed_indices = set()

    if output_file.exists():
        print(f"Found existing output file: {output_file}")
        with open(output_file, 'r', encoding='utf-8') as f:
            existing_results = json.load(f)
        processed_indices = {r['hadith_index'] for r in existing_results}
        print(f"✓ Loaded {len(existing_results)} previously processed hadiths")

    # Determine which hadiths need processing
    id_col = 'id' if 'id' in df.columns else 'hadith_id'
    df_to_process = df[~df[id_col].isin(processed_indices)]

    if len(df_to_process) == 0:
        print("✅ All hadiths already processed!")
        return

    print(f"Processing {len(df_to_process)} remaining hadiths...")

    # Process in batches
    new_results = []

    for i in tqdm(range(0, len(df_to_process), batch_size), desc="Processing batches"):
        batch_df = df_to_process.iloc[i:i+batch_size]
        batch_texts = batch_df['text_ar'].tolist()
        batch_indices = batch_df[id_col].tolist()

        # Extract sanad/matn
        batch_parts = split_hadith_batch(batch_texts)

        # Convert to output format
        for idx, text, parts in zip(batch_indices, batch_texts, batch_parts):
            if parts.sanad != "ERROR":  # Skip errors
                new_results.append({
                    "hadith_index": int(idx),
                    "hadith_text": text,
                    "sanad": parts.sanad,
                    "matn": parts.matn
                })
            else:
                print(f"⚠️  Skipping hadith_index {idx} due to extraction error")

    # Combine and save
    all_results = existing_results + new_results
    all_results.sort(key=lambda x: x['hadith_index'])  # Sort by index

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved {len(all_results)} hadiths to {output_file}")
    print(f"   ({len(new_results)} newly processed)")


# =============================================================================
# Main Execution
# =============================================================================

def main():
    """Main entry point for the script"""
    load_dotenv()

    # Try to find CSV file from multiple options
    csv_path = None
    for csv_option in INPUT_CSV_OPTIONS:
        test_path = Path(csv_option)
        if test_path.exists():
            csv_path = test_path
            break

    if csv_path is None:
        print(f"❌ Error: Could not find input CSV file")
        print(f"   Searched for:")
        for csv_option in INPUT_CSV_OPTIONS:
            print(f"   - {Path(csv_option).absolute()}")
        return

    # Load CSV
    print(f"Loading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"✓ Loaded {len(df)} rows")

    # Check and adapt column structure
    # Handle different CSV formats
    if df.columns[0] and 'Bukhari' in str(df.columns[0]):
        # Headless CSV - first row is actual data
        print("Detected headless CSV format")
        df.columns = ['hadith_text']
        # Add index as hadith_id
        df.insert(0, 'id', range(1, len(df) + 1))
        # Rename hadith_text to text_ar
        df.rename(columns={'hadith_text': 'text_ar'}, inplace=True)
    elif 'hadith_text' in df.columns and 'text_ar' not in df.columns:
        # Has hadith_text column but not text_ar
        df.rename(columns={'hadith_text': 'text_ar'}, inplace=True)
        if 'id' not in df.columns and 'hadith_id' not in df.columns:
            df.insert(0, 'id', range(1, len(df) + 1))
    elif 'text_ar' not in df.columns:
        print(f"❌ Error: CSV must contain 'text_ar' or 'hadith_text' column")
        print(f"   Found columns: {', '.join(df.columns)}")
        return

    # Ensure we have an ID column
    if 'id' not in df.columns and 'hadith_id' not in df.columns:
        print("Adding sequential ID column...")
        df.insert(0, 'id', range(1, len(df) + 1))

    print(f"✓ Using columns: {', '.join(df.columns)}")

    # Process
    output_path = Path(OUTPUT_JSON)
    print(f"Output will be saved to: {output_path.absolute()}")
    print(f"Model: {MODEL}, Batch size: {BATCH_SIZE}")
    print("-" * 60)

    process_all_hadiths(
        df=df,
        batch_size=BATCH_SIZE,
        output_file=output_path
    )

    print("-" * 60)
    print("✅ Processing complete!")


if __name__ == "__main__":
    main()
