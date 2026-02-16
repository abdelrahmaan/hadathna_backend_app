import os
import json
import csv
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY in environment (.env)")

client = OpenAI(api_key=OPENAI_API_KEY)

# =========================
# Config
# =========================
DEFAULT_MODEL = os.getenv("HADITH_MODEL", "gpt-5-mini")   
FALLBACK_MODEL = os.getenv("HADITH_FALLBACK_MODEL", "gpt-5.2")

INPUT_CSV = os.getenv("HADITH_CSV", "Sahih Bukhari Without_Tashkel.csv")
OUTPUT_JSON = os.getenv("HADITH_OUTPUT", "bukhari_chains.json")

# Save incrementally each N hadiths
SAVE_EVERY = int(os.getenv("SAVE_EVERY", "1"))

MAX_RETRIES = 4
RETRY_BACKOFF_SEC = 2

FALLBACK_STATS = {
    "fallback_used_count": 0,        # عدد الأحاديث اللي اضطرينا نستخدم فيها fallback
    "fallback_calls": 0,             # عدد نداءات fallback (محاولات) الفعلية
    "default_failures_before_fb": 0, # إجمالي فشل default قبل ما نتحول للفولباك
    "fallback_success_count": 0,     # عدد نجاحات fallback
    "fallback_failure_count": 0,     # عدد فشل fallback
}

# =========================
# Helpers
# =========================
def load_hadith_rows(csv_path: str) -> List[Dict[str, Any]]:
    """
    Loads hadiths from CSV.
    Supports:
      - header with columns like hadith_index, hadith_text
      - OR no header: first column is hadith_text, index is row number (1-based)
    """
    rows: List[Dict[str, Any]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)

        # Try DictReader first (header)
        has_header = csv.Sniffer().has_header(sample)
        if has_header:
            reader = csv.DictReader(f)
            for i, r in enumerate(reader, start=1):
                text = (r.get("hadith_text") or r.get("text") or r.get("hadith") or "").strip()
                if not text:
                    # fallback: maybe first column name unknown
                    # pick first non-empty field
                    for _, v in r.items():
                        if v and str(v).strip():
                            text = str(v).strip()
                            break
                if not text:
                    continue

                idx_raw = (r.get("hadith_index") or r.get("index") or "").strip()
                idx = int(idx_raw) if idx_raw.isdigit() else i
                rows.append({"hadith_index": idx, "hadith_text": text})
        else:
            reader2 = csv.reader(f)
            for i, r in enumerate(reader2, start=1):
                if not r:
                    continue
                text = (r[0] or "").strip()
                if not text:
                    continue
                rows.append({"hadith_index": i, "hadith_text": text})

    return rows


def load_existing_results(path: str) -> List[Dict[str, Any]]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def save_results(path: str, data: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# Prompt + Few-shots
# =========================

SYSTEM_PROMPT = """
أنت مساعد متخصص في استخراج أسانيد الحديث من نص عربي (صحيح البخاري).
هدفك: إخراج الأسانيد كسلاسل متعددة (chain_id) بدون خلط، وكل سند عبارة عن قائمة رواة بالترتيب (من المصنف نزولًا إلى الصحابي/الراوي الأعلى).
لكن في "output" النهائي نحن نرجّعها بصيغة قائمة الرواة، مع تمييز آخر راوٍ (الأعلى/الصحابي غالبًا) بـ role="lead".
مهم جدًا: لو عندنا "عن فلان وفلان" في نفس الطبقة، اعتبرهم سندين كاملين يشتركان في بقية السند.
"""

# Few-shot covers:
# - basic chain
# - "عن X و Y قالا" => two full chains
# - avoid partial chains like "مروان" alone
FEW_SHOT_1_USER = """
استخرج الأسانيد من هذا النص (صحيح البخاري) وأرجع JSON مطابق للـ schema:

نص:
حدثنا عبد الله بن يوسف أخبرنا مالك عن نافع عن ابن عمر أن رسول الله صلى الله عليه وسلم قال ...
"""

FEW_SHOT_1_ASSISTANT = {
    "hadith_index": 1,
    "chains": {
        "chain_1": [
            {"name": "عبد الله بن يوسف", "attributes": {"role": "narrator"}},
            {"name": "مالك", "attributes": {"role": "narrator"}},
            {"name": "نافع", "attributes": {"role": "narrator"}},
            {"name": "ابن عمر", "attributes": {"role": "lead"}}
        ]
    }
}

FEW_SHOT_2_USER = """
نص فيه "عن فلان وفلان" (لا تعمل سند ناقص):

نص:
حدثني عبد الله بن محمد حدثنا عبد الرزاق أخبرنا معمر عن الزهري عن عروة بن الزبير عن المسور بن مخرمة ومروان قالا خرج رسول الله صلى الله عليه وسلم ...
"""

FEW_SHOT_2_ASSISTANT = {
    "hadith_index": 1,
    "chains": {
        "chain_1": [
            {"name": "عبد الله بن محمد", "attributes": {"role": "narrator"}},
            {"name": "عبد الرزاق", "attributes": {"role": "narrator"}},
            {"name": "معمر", "attributes": {"role": "narrator"}},
            {"name": "الزهري", "attributes": {"role": "narrator"}},
            {"name": "عروة بن الزبير", "attributes": {"role": "narrator"}},
            {"name": "المسور بن مخرمة", "attributes": {"role": "lead"}}
        ],
        "chain_2": [
            {"name": "عبد الله بن محمد", "attributes": {"role": "narrator"}},
            {"name": "عبد الرزاق", "attributes": {"role": "narrator"}},
            {"name": "معمر", "attributes": {"role": "narrator"}},
            {"name": "الزهري", "attributes": {"role": "narrator"}},
            {"name": "عروة بن الزبير", "attributes": {"role": "narrator"}},
            {"name": "مروان", "attributes": {"role": "lead"}}
        ]
    }
}

USER_INSTRUCTIONS = """
المطلوب:
1) استخرج كل الأسانيد الواضحة التي تبدأ بألفاظ الأداء (حدثنا/حدثني/أخبرنا/سمعت/قال حدثنا/عن ...)
2) لا تخرج سند "ناقص" عبارة عن اسم منفرد بدون بقية السلسلة.
3) لو ورد "عن X و Y" في نفس طبقة السند، أخرج سندين كاملين (chain_1/chain_2) يشتركان في بقية الرواة.
4) خرّج الرواة بالترتيب من المصنف/الشيخ الأدنى → حتى الراوي الأعلى.
5) ضع role="lead" فقط على آخر راوٍ في كل سند. والبقية role="narrator".
6) تجاهل الأسماء التي تظهر داخل المتن/القصة وليست ضمن ألفاظ الأداء.
7) أعد chain_* متسلسلة بدون فجوات.

أرجع JSON فقط (بدون شرح).
"""


# JSON Schema for structured output
RESPONSE_SCHEMA = {
    "name": "hadith_chains",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "hadith_index": {"type": "integer"},
            "chains": {
                "type": "object",
                "additionalProperties": False,
                "patternProperties": {
                    "^chain_[1-9][0-9]*$": {
                        "type": "array",
                        "minItems": 2,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "properties": {
                                "name": {"type": "string", "minLength": 1},
                                "attributes": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "role": {"type": "string", "enum": ["narrator", "lead"]}
                                    },
                                    "required": ["role"]
                                }
                            },
                            "required": ["name", "attributes"]
                        }
                    }
                }
            }
        },
        "required": ["hadith_index", "chains"]
    },
    "strict": True
}


def extract_chains_for_hadith(hadith_index: int, hadith_text: str, model: str) -> Dict[str, Any]:
    """
    Calls OpenAI with structured output to extract chains.
    Returns: {"hadith_index": int, "chains": {...}}
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT.strip()},

        # few-shots
        {"role": "user", "content": FEW_SHOT_1_USER.strip()},
        {"role": "assistant", "content": json.dumps(FEW_SHOT_1_ASSISTANT, ensure_ascii=False)},

        {"role": "user", "content": FEW_SHOT_2_USER.strip()},
        {"role": "assistant", "content": json.dumps(FEW_SHOT_2_ASSISTANT, ensure_ascii=False)},

        # real task
        {"role": "user", "content": f"{USER_INSTRUCTIONS.strip()}\n\nhadith_index: {hadith_index}\n\nنص:\n{hadith_text}".strip()},
    ]

    resp = client.responses.create(
        model=model,
        input=messages,
        response_format={"type": "json_schema", "json_schema": RESPONSE_SCHEMA},
        temperature=0
    )

    # responses API returns output text in resp.output_text (JSON string)
    data = json.loads(resp.output_text)
    return data


def robust_extract(hadith_index: int, hadith_text: str) -> Dict[str, Any]:
    last_err: Optional[Exception] = None

    # Try default model first
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return extract_chains_for_hadith(hadith_index, hadith_text, DEFAULT_MODEL)
        except Exception as e:
            last_err = e
            # سجل فشل default
            FALLBACK_STATS["default_failures_before_fb"] += 1
            time.sleep(RETRY_BACKOFF_SEC * attempt)

    # If we got here => fallback will be used for this hadith
    FALLBACK_STATS["fallback_used_count"] += 1

    # Fallback model
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            FALLBACK_STATS["fallback_calls"] += 1
            data = extract_chains_for_hadith(hadith_index, hadith_text, FALLBACK_MODEL)
            FALLBACK_STATS["fallback_success_count"] += 1
            return data
        except Exception as e:
            last_err = e
            FALLBACK_STATS["fallback_failure_count"] += 1
            time.sleep(RETRY_BACKOFF_SEC * attempt)

    raise RuntimeError(f"Failed extraction for hadith {hadith_index}: {last_err}")


def main():
    rows = load_hadith_rows(INPUT_CSV)
    print(f"Loaded {len(rows)} hadith rows from {INPUT_CSV}")

    existing = load_existing_results(OUTPUT_JSON)
    done_ids = {r.get("hadith_index") for r in existing if isinstance(r, dict)}
    print(f"Resuming: {len(existing)} already in {OUTPUT_JSON}")

    results = existing[:]  # append new

    processed = 0
    for row in rows:
        idx = int(row["hadith_index"])
        text = row["hadith_text"]

        if idx in done_ids:
            continue

        print(f"\n--- Hadith {idx} ---")
        try:
            extracted = robust_extract(idx, text)
            results.append({
                "hadith_index": idx,
                "hadith_text": text,
                "chains": extracted["chains"]
            })
            processed += 1

            if processed % SAVE_EVERY == 0:
                save_results(OUTPUT_JSON, results)
                print(f"Saved progress: {len(results)} total")

        except Exception as e:
            print(f"[ERROR] hadith_index={idx}: {e}")
            # Save anyway to not lose progress
            save_results(OUTPUT_JSON, results)

    save_results(OUTPUT_JSON, results)
    print("\nDone.")
    print(f"Saved: {OUTPUT_JSON}")
    print(f"Total results: {len(results)}")


if __name__ == "__main__":
    main()
    print("\n--- Fallback Summary ---")
    print(json.dumps(FALLBACK_STATS, ensure_ascii=False, indent=2))
