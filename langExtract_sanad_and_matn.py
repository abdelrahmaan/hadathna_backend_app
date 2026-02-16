import langextract as lx
import textwrap
import csv
import json
from langextract.providers import openai
from dotenv import load_dotenv
import os
load_dotenv()

OPENAI_API_KEY=os.getenv("OPENAI_API_KEY")
# 1. Load hadiths from CSV file
def load_hadiths(csv_path):
    """Load hadiths from CSV file, skipping the header row."""
    hadiths = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        for row in reader:
            if row and row[0].strip():
                hadiths.append(row[0].strip())
    return hadiths

# 2. Define the Instruction (Prompt)
# We tell the model to extract sanad, matn, and individual narrators.
prompt = textwrap.dedent("""
    Extract three components from the hadith text:

    1. Sanad (Chain): The full chain of transmission text
       - Starts with transmission words like "حدثنا" (told us)
       - Ends before the Prophet's saying begins
       - Include all transmission words and narrator names

    2. Matn (Content): The actual saying/action of the Prophet
       - The content after the chain, typically after "قال" (said)
       - The actual hadith text/teaching

    3. Narrators: Individual names from the chain
       - Extract names in transmission order
       - Mark the last one (Companion/Sahabi) with role "lead"
       - Do not include the Prophet
""")

# 3. Define a "Few-Shot" Example
# This teaches the model how to extract sanad, matn, and narrators.
example_text = "حدثنا عبدالله بن يوسف أخبرنا مالك عن نافع عن ابن عمر أن رسول الله صلى الله عليه وسلم قال بني الإسلام على خمس"

examples = [
    lx.data.ExampleData(
        text=example_text,
        extractions=[
            # Sanad (full chain text)
            lx.data.Extraction(
                extraction_class="sanad",
                extraction_text="حدثنا عبدالله بن يوسف أخبرنا مالك عن نافع عن ابن عمر",
                attributes={"type": "chain"}
            ),
            # Matn (hadith content)
            lx.data.Extraction(
                extraction_class="matn",
                extraction_text="بني الإسلام على خمس",
                attributes={"type": "content"}
            ),
            # Individual narrators
            lx.data.Extraction(
                extraction_class="narrator",
                extraction_text="عبدالله بن يوسف",
                attributes={"role": "narrator"}
            ),
            lx.data.Extraction(
                extraction_class="narrator",
                extraction_text="مالك",
                attributes={"role": "narrator"}
            ),
            lx.data.Extraction(
                extraction_class="narrator",
                extraction_text="نافع",
                attributes={"role": "narrator"}
            ),
            lx.data.Extraction(
                extraction_class="narrator",
                extraction_text="ابن عمر",
                attributes={"role": "lead"}
            ),
        ]
    )
]

# 4. Run the Extraction for each hadith
def extract_hadith_parts(hadith_text, hadith_index):
    """Extract sanad, matn, and narrators from a single hadith."""
    try:
        result = lx.extract(
            text_or_documents=hadith_text,
            prompt_description=prompt,
            examples=examples,
            model_id="gpt-4o",  # Automatically selects OpenAI provider
            api_key=OPENAI_API_KEY,
            fence_output=True,
            use_schema_constraints=False,
            temperature=0.0  # Set to 0 for deterministic extraction
        )

        # Filter by extraction class
        sanad = [e for e in result.extractions if e.extraction_class == "sanad"]
        matn = [e for e in result.extractions if e.extraction_class == "matn"]
        narrators = [e for e in result.extractions if e.extraction_class == "narrator"]

        return {
            "sanad": sanad[0] if sanad else None,
            "matn": matn[0] if matn else None,
            "narrators": narrators
        }
    except Exception as e:
        print(f"Error processing hadith {hadith_index}: {e}")
        return {"sanad": None, "matn": None, "narrators": []}

def main():
    csv_path = "Sahih Muslime Without_Tashkel.csv"
    results_file = f"{csv_path.split('.')[0]}_results.json"

    print("Loading hadiths from CSV...")
    # hadiths = load_hadiths(csv_path)[:3]  # Limit to first 10 for testing
    hadiths = load_hadiths(csv_path)
    print(f"Loaded {len(hadiths)} hadiths")

    print("\nRunning LangExtract with Gemini...")

    # Load existing results if file exists
    all_results = []
    if os.path.exists(results_file):
        try:
            with open(results_file, "r", encoding="utf-8") as f:
                all_results = json.load(f)
            print(f"Resuming from {len(all_results)} previously processed hadiths")
        except:
            all_results = []

    # Determine starting point
    start_index = len(all_results)

    for i, hadith_text in enumerate(hadiths, 1):
        # Skip already processed hadiths
        if i <= start_index:
            continue

        print(f"\n--- Hadith {i} ---")
        print(f"Text: {hadith_text}")

        parts = extract_hadith_parts(hadith_text, i)

        # Print extracted parts
        if parts["sanad"]:
            print(f"Sanad: {parts['sanad'].extraction_text}")
        if parts["matn"]:
            print(f"Matn: {parts['matn'].extraction_text}")

        print(f"Extracted Narrators:")
        for j, item in enumerate(parts["narrators"], 1):
            print(f"  {j}. {item.extraction_text} - {item.attributes}")

        result = {
            "hadith_index": i,
            "hadith_text": hadith_text,
            "sanad": parts["sanad"].extraction_text if parts["sanad"] else None,
            "matn": parts["matn"].extraction_text if parts["matn"] else None,
            "narrators": [
                {
                    "name": n.extraction_text,
                    "attributes": n.attributes
                } for n in parts["narrators"]
            ]
        }
        all_results.append(result)

        # Save after each hadith (incremental save)
        with open(results_file, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)

        print(f"Progress: {len(all_results)}/{len(hadiths)} hadiths saved")

    print(f"\n--- Summary ---")
    print(f"Processed {len(all_results)} hadiths")
    print(f"Results saved to {results_file}")

    return all_results

if __name__ == "__main__":
    main()
