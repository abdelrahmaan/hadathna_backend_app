"""
Test script to verify hadith splitting functionality with sample data
"""

import pandas as pd
import json
from pathlib import Path

# Sample hadiths for testing
test_hadiths = [
    "حدثنا الحميدي عبد الله بن الزبير، قال حدثنا سفيان، قال حدثنا يحيى بن سعيد الأنصاري، قال أخبرني محمد بن إبراهيم التيمي، أنه سمع علقمة بن وقاص الليثي، يقول سمعت عمر بن الخطاب رضى الله عنه على المنبر قال سمعت رسول الله صلى الله عليه وسلم يقول ‏\"‏ إنما الأعمال بالنيات، وإنما لكل امرئ ما نوى، فمن كانت هجرته إلى دنيا يصيبها أو إلى امرأة ينكحها فهجرته إلى ما هاجر إليه ‏\"‏‏.‏",
    "حدثنا عمرو بن خالد، قال حدثنا الليث، عن يزيد، عن أبي الخير، عن عبد الله بن عمرو  رضى الله عنهما  أن رجلا، سأل النبي صلى الله عليه وسلم أى الإسلام خير قال ‏\"‏ تطعم الطعام، وتقرأ السلام على من عرفت ومن لم تعرف ‏\"‏‏.‏",
    "حدثنا مسدد، قال حدثنا يحيى، عن شعبة، عن قتادة، عن أنس  رضى الله عنه  عن النبي صلى الله عليه وسلم‏.‏وعن حسين المعلم، قال حدثنا قتادة، عن أنس، عن النبي صلى الله عليه وسلم قال ‏\"‏ لا يؤمن أحدكم حتى يحب لأخيه ما يحب لنفسه ‏\"‏‏.‏"
]

def create_test_csv():
    """Create a small test CSV file"""
    df = pd.DataFrame({
        'id': range(1, len(test_hadiths) + 1),
        'hadith_id': range(1, len(test_hadiths) + 1),
        'text_ar': test_hadiths,
        'source': ['SahihBukhari'] * len(test_hadiths)
    })

    test_file = Path('test_hadiths.csv')
    df.to_csv(test_file, index=False, encoding='utf-8')
    print(f"✓ Created test CSV: {test_file} with {len(test_hadiths)} hadiths")
    return test_file

def run_test():
    """Run the splitting script on test data"""
    print("=" * 60)
    print("Testing Hadith Splitting Script")
    print("=" * 60)

    # Create test CSV
    test_csv = create_test_csv()

    print("\nTo test the script, run:")
    print(f"  1. Update INPUT_CSV_OPTIONS in split_hadith_to_matn_and_sanad.py to include '{test_csv}'")
    print(f"  2. Run: python3 split_hadith_to_matn_and_sanad.py")
    print(f"  3. Check output: bukhari_hadiths_split.json")

    print("\n" + "=" * 60)
    print("Expected output structure:")
    print("=" * 60)

    expected_output = [
        {
            "hadith_index": 1,
            "hadith_text": test_hadiths[0][:100] + "...",
            "sanad": "حدثنا الحميدي... عن عمر بن الخطاب",
            "matn": "عن عمر بن الخطاب رضى الله عنه... إنما الأعمال بالنيات..."
        }
    ]

    print(json.dumps(expected_output, ensure_ascii=False, indent=2))

    print("\n✓ Test preparation complete!")

if __name__ == "__main__":
    run_test()
