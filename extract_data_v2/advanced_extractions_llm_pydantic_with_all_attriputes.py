"""
Advanced Hadith Multi-Chain + Matn Extractor (Books-agnostic)

Built on top of llm_pydantic_extraction.py with:
- Improved prompt for advanced isnad patterns (ح, متابعات, coupling)
- Chain type classification (primary / follow_up)
- Fixed few-shot examples (consistent format)
- Chain completion for partial mutaba'at/tahweel chains

Routing:
  - Routes hadith to Strong model if:
    1) hadith length >= threshold chars, OR
    2) router decides complex=True
  - Otherwise uses Light model first, fallback to Strong.

Output:
  [
    {
      "hadith_index": 1,
      "hadith_text": "...",
      "matn_segments": ["...", "..."],
      "chains": [
        {
          "chain_id": "chain_1",
          "type": "primary",
          "narrators": [
            {"name": "...", "attributes": {"role": "narrator"}},
            ...
            {"name": "...", "attributes": {"role": "lead"}}
          ]
        },
        {
          "chain_id": "chain_2",
          "type": "follow_up",
          "narrators": [...]
        }
      ]
    },
    ...
  ]
"""

import os
import re
import json
import csv
import textwrap
from typing import List, Literal, Dict, Any, Tuple
from dataclasses import dataclass
from dotenv import load_dotenv

from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI

load_dotenv("../.env")

# =========================
# Config (EDIT THESE)
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

STRONG_MODEL = os.getenv("STRONG_MODEL", "gpt-5.2")
LIGHT_MODEL = os.getenv("LIGHT_MODEL", "gpt-4o-mini")
ROUTER_MODEL = os.getenv("ROUTER_MODEL", "gpt-4o-mini")

LEN_STRONG_THRESHOLD = int(os.getenv("LEN_STRONG_THRESHOLD", "500"))

TEMP = float(os.getenv("TEMP", "0.0"))

BOOK = os.getenv("BOOK", "bukhari").strip().lower()

CSV_PATH = os.getenv("CSV_PATH", "Bukhari/Bukhari_Without_Tashkel.csv")
CSV_TEXT_COLUMN = os.getenv("CSV_TEXT_COLUMN", "hadith_text").strip()
OUT_JSON_PATH = os.getenv(
    "OUT_JSON_PATH",
    "Bukhari/Bukhari_Without_Tashkel_results_advanced_with_matn.json",
)
MAX_HADITHS = int(os.getenv("MAX_HADITHS", "5"))  # 0 => all hadiths
RESUME = os.getenv("RESUME", "0").strip().lower() not in ("0", "false", "no")

# Single hadith test mode:
# Put any hadith text here to test just one hadith and skip CSV loading.
# test_hadith = "حدثنا عبد الله بن يوسف أخبرنا مالك عن سمي مولى أبي بكر بن عبد الرحمن عن أبي صالح السمان عن أبي هريرة رضي الله عنه أن رسول الله صلى الله عليه وسلم قال العمرة إلى العمرة كفارة لما بينهما والحج المبرور ليس له جزاء إلا الجنة.  " # bukhari hadith
test_hadith = "حدثنا يحيى بن بكير قال حدثنا الليث عن عقيل عن ابن شهاب عن عروة بن الزبير عن عائشة أم المؤمنين أنها قالت أول ما بدئ به رسول الله صلى الله عليه وسلم من الوحي الرؤيا الصالحة في النوم فكان لا يرى رؤيا إلا جاءت مثل فلق الصبح ثم حبب إليه الخلاء وكان يخلو بغار حراء فيتحنث فيه وهو التعبد الليالي ذوات العدد قبل أن ينزع إلى أهله ويتزود لذلك ثم يرجع إلى خديجة فيتزود لمثلها حتى جاءه الحق وهو في غار حراء فجاءه الملك فقال اقرأ قال ما أنا بقارئ قال فأخذني فغطني حتى بلغ مني الجهد ثم أرسلني فقال اقرأ قلت ما أنا بقارئ فأخذني فغطني الثانية حتى بلغ مني الجهد ثم أرسلني فقال اقرأ فقلت ما أنا بقارئ فأخذني فغطني الثالثة ثم أرسلني فقال { اقرأ باسم ربك الذي خلق خلق الإنسان من علق اقرأ وربك الأكرم } فرجع بها رسول الله صلى الله عليه وسلم يرجف فؤاده فدخل على خديجة بنت خويلد رضي الله عنها فقال زملوني زملوني فزملوه حتى ذهب عنه الروع فقال لخديجة وأخبرها الخبر لقد خشيت على نفسي فقالت خديجة كلا والله ما يخزيك الله أبدا إنك لتصل الرحم وتحمل الكل وتكسب المعدوم وتقري الضيف وتعين على نوائب الحق فانطلقت به خديجة حتى أتت به ورقة بن نوفل بن أسد بن عبد العزى ابن عم خديجة وكان امرأ قد تنصر في الجاهلية وكان يكتب الكتاب العبراني فيكتب من الإنجيل بالعبرانية ما شاء الله أن يكتب وكان شيخا كبيرا قد عمي فقالت له خديجة يا ابن عم اسمع من ابن أخيك فقال له ورقة يا ابن أخي ماذا ترى فأخبره رسول الله صلى الله عليه وسلم خبر ما رأى فقال له ورقة هذا الناموس الذي نزل الله على موسى يا ليتني فيها جذعا ليتني أكون حيا إذ يخرجك قومك فقال رسول الله صلى الله عليه وسلم أومخرجي هم قال نعم لم يأت رجل قط بمثل ما جئت به إلا عودي وإن يدركني يومك أنصرك نصرا مؤزرا ثم لم ينشب ورقة أن توفي وفتر الوحي قال ابن شهاب وأخبرني أبو سلمة بن عبد الرحمن أن جابر بن عبد الله الأنصاري قال وهو يحدث عن فترة الوحي فقال في حديثه بينا أنا أمشي إذ سمعت صوتا من السماء فرفعت بصري فإذا الملك الذي جاءني بحراء جالس على كرسي بين السماء والأرض فرعبت منه فرجعت فقلت زملوني زملوني فأنزل الله تعالى { يا أيها المدثر قم فأنذر إلى قوله والرجز فاهجر } فحمي الوحي وتتابع تابعه عبد الله بن يوسف وأبو صالح وتابعه هلال بن رداد عن الزهري وقال يونس ومعمر بوادره."
TEST_HADITH = os.getenv("TEST_HADITH", "").strip() or test_hadith.strip()

# =========================
# Pydantic Schemas
# =========================
Role = Literal["narrator", "lead"]
ChainType = Literal["primary", "follow_up", "nested"]


class NarratorItem(BaseModel):
    name: str = Field(
        ...,
        description="اسم الراوي فقط بدون ألقاب أو صيغ التبجيل",
    )
    role: Role = Field(..., description="narrator أو lead")


class ChainItem(BaseModel):
    chain_id: str = Field(..., description="صيغة chain_1, chain_2, ...")
    type: ChainType = Field(
        default="primary",
        description='"primary" للسند الرئيسي أو القران، "follow_up" للمتابعات',
    )
    narrators: List[NarratorItem] = Field(..., min_length=1)


class HadithExtraction(BaseModel):
    matn_segments: List[str] = Field(
        ...,
        min_length=1,
        description="قائمة مقاطع المتن (Content Only). افصل القصص/الروايات المدرجة في عناصر مستقلة بدون صيغ الأداء",
    )
    chains: List[ChainItem] = Field(..., min_length=1)


class RouteDecision(BaseModel):
    complex: bool = Field(
        ...,
        description="True إذا كان الحديث متعدد الأسانيد أو فيه تحويل (ح) أو متابعات",
    )


# =========================
# Book hints (SOFT)
# =========================
BOOK_HINTS = {
    "bukhari": {
        "name_ar": "صحيح البخاري",
        "signals": [
            "تابعه",
            "وقال",
            "زعم",
            "نحوه",
            "ورواه",
            "وفي رواية",
            "ح (قد تظهر)",
        ],
    },
    "muslim": {
        "name_ar": "صحيح مسلم",
        "signals": ["ح", "ح وحدثنا", "حدثنا... وحدثنا...", "وفي حديث", "نحوه"],
    },
    "malik": {
        "name_ar": "الموطأ",
        "signals": ["بلاغ", "أنه بلغه", "الأمر عندنا", "قال مالك"],
    },
    "generic": {
        "name_ar": "كتاب حديث",
        "signals": ["تابعه", "ورواه", "وفي رواية", "ح"],
    },
}


def get_book_hints(book: str) -> Dict[str, Any]:
    return BOOK_HINTS.get(book, BOOK_HINTS["generic"])


# =========================
# Helpers
# =========================
def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def clean_narrator_name(name: str) -> str:
    """Remove honorifics, titles, and leaked performance prefixes from narrator names."""
    name = normalize_whitespace(name)

    # Remove leaked performance prefixes (حدثنا/أخبرنا/etc.) in case the LLM includes them
    prefixes = [
        r"^حدثنا\s+",
        r"^أخبرنا\s+",
        r"^حدثني\s+",
        r"^أخبرني\s+",
        r"^سمعت\s+",
        r"^عن\s+",
        r"^قال\s+",
    ]
    for pattern in prefixes:
        name = re.sub(pattern, "", name)

    # Remove honorifics and titles
    honorifics = [
        r"\s+رضي الله عنه.*",
        r"\s+رضي الله عنها.*",
        r"\s+رضي الله عنهم.*",
        r"\s+رضي الله عنهما.*",
        r"\s+صلى الله عليه وسلم.*",
        r"\s+عليه السلام.*",
        r"\s+رحمه الله.*",
        r"\s+رحمها الله.*",
        r"\s+أم المؤمنين.*",
        r"\s+الصحابي.*",
        r"\s+التابعي.*",
    ]

    for pattern in honorifics:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE)

    return normalize_whitespace(name)


def is_arabic_heavy(text: str) -> bool:
    return sum(1 for ch in text if "\u0600" <= ch <= "\u06FF") > 20


def renumber_chain_ids(chains: List[ChainItem]) -> List[ChainItem]:
    """Ensure chain_id contiguous chain_1..chain_n."""
    new = []
    for i, ch in enumerate(chains, 1):
        ch.chain_id = f"chain_{i}"
        new.append(ch)
    return new


def basic_validate_extraction(
    _hadith_text: str, extraction: HadithExtraction
) -> Tuple[bool, str]:
    """
    Lightweight sanity checks:
    - chain_id format ok
    - no empty names
    - last narrator in each chain should be lead
    - avoid Prophet name included as narrator
    - type field is valid
    """
    if not isinstance(extraction.matn_segments, list):
        return False, "matn_segments يجب أن تكون قائمة"
    if not extraction.matn_segments:
        return False, "matn_segments فارغة"
    for seg in extraction.matn_segments:
        if not isinstance(seg, str):
            return False, "كل عنصر في matn_segments يجب أن يكون نصاً"
        if not normalize_whitespace(seg):
            return False, "يوجد عنصر فارغ في matn_segments"

    prophet_markers = [
        "رسول الله",
        "النبي",
        "محمد صلى الله عليه وسلم",
        "رسولِ الله",
    ]
    for ch in extraction.chains:
        if not re.match(r"^chain_\d+$", ch.chain_id):
            return False, f"chain_id غير صحيح: {ch.chain_id}"
        if ch.type not in ("primary", "follow_up", "nested"):
            return False, f"{ch.chain_id} نوع غير صحيح: {ch.type}"
        if not ch.narrators:
            return False, f"{ch.chain_id} بلا رواة"
        for n in ch.narrators:
            name = normalize_whitespace(n.name)
            if not name:
                return False, f"{ch.chain_id} فيه اسم فاضي"
            if any(pm in name for pm in prophet_markers):
                return (
                    False,
                    f"{ch.chain_id} يحتوي النبي ضمن الرواة (غير مسموح)",
                )
            if not n.role:
                return False, f"{ch.chain_id} راوٍ بدون role"
        if ch.narrators[-1].role != "lead":
            return False, f"{ch.chain_id} آخر راوٍ لازم يكون lead"
    return True, "ok"


# =========================
# Prompts
# =========================
def build_router_prompt(book: str) -> str:
    hints = get_book_hints(book)
    name_ar = hints["name_ar"]
    signals = "، ".join(hints["signals"])

    return f"""
أنت Router لتقدير هل نص الحديث "معقد" لاستخراج الأسانيد أم لا.

أرجع JSON مطابق للـ schema:
{{"complex": true/false}}

اعتبر complex=true إذا وُجد أي مما يلي:
- تحويل إسناد: (ح) أو (ح وحدثنا...) أو (حَدَّثَنَا ... ح)
- تعدد طرق/متابعات/تعليقات تشير لطرق رواية إضافية (مثل: تابعه/ورواه/وفي رواية/نحوه/وقال فلان/زعم)
- أكثر من بداية إسناد داخل النص (تكرار: حدثنا/أخبرنا/سمعت...) خارج سياق المتن
- طول شديد + تداخل واضح بين الإسناد والمتن

ملاحظات للكتاب: {name_ar}
إشارات شائعة: {signals}

مهم:
- لا تعتبر أسماء داخل القصة/الشرح طريقًا إلا إذا جاءت في سياق رواية/أداء.
- (ح) قد يظهر حتى في البخاري أحيانًا.

أعطِ قرارًا واحدًا فقط.
""".strip()


def build_advanced_extractor_prompt(book: str) -> str:
    """
    Enhanced prompt for advanced isnad patterns:
    ح separator, متابعات, coupling, chain completion.
    """
    hints = get_book_hints(book)
    name_ar = hints["name_ar"]

    return textwrap.dedent(f"""
    أنت خبير في تحليل أسانيد الحديث العربي (كتاب: {name_ar}).
    مهمتك استخراج جميع الأسانيد/الطرق بدقة عالية وتحويلها إلى بيانات مهيكلة لبناء Knowledge Graph.

    المطلوب: أخرج JSON فيه:
    {{
      "matn_segments": [
        "مقطع المتن الأول (Content Only)",
        "مقطع المتن الثاني إن وُجد سند مدرج/قصة إضافية"
      ],
      "chains": [
        {{
          "chain_id": "chain_1",
          "type": "primary",
          "narrators": [
            {{"name":"الاسم الكامل","role":"narrator"}},
            ...
            {{"name":"الصحابي","role":"lead"}}
          ]
        }},
        ...
      ]
    }}

    === قواعد حاسمة للاستخراج ===

    1) تفكيك التحويل (ح):
       - عند ظهور (ح) أو (ح وحدثنا...) أو (حدثنا... ح): هذا بداية سند جديد مستقل.
       - افصله كـ chain جديد (chain_2, chain_3...) من نوع "primary".
       - هام جداً: إذا ذكر السند الجديد رواةً جزئيين فقط (مثلاً: "ح وحدثنا فلان عن فلان")
         ويتوقف، يجب عليك إكمال بقية السلسلة بناءً على السند الأول الذي يلتقي معه.
         لا تترك السلسلة مقطوعة أبداً.

    2) المتابعات والطرق الإضافية:
       - عبارات: "تابعه فلان"، "ورواه فلان"، "وقال فلان"، "خالفه فلان"، "وفي رواية"
       - هذه تشير لأسانيد إضافية (متابعات/شواهد) → اجعلها chains منفصلة بـ type="follow_up".
       - مهم جداً: أكمل المتابعات بتتبع السند للخلف حتى نقطة الالتقاء مع السند الرئيسي.
       - مثال: "تابعه عبد الله بن يوسف وأبو صالح عن الزهري"
         → chain_2: [عبد الله بن يوسف، الزهري] (follow_up)
         → chain_3: [أبو صالح، الزهري] (follow_up)
       - لا تترك المتابعة كراوٍ وحيد بلا سلسلة.

    3) القران (اجتماع راويين في نفس الطبقة):
       - "عن فلان وفلان"، "حدثنا فلان وفلان" → سندان يشتركان في الشيوخ اللاحقين.
       - افصلهما كـ chains منفصلة (كلاهما type="primary").

    4) الأسانيد المدرجة/الضمنية (Nested Chains) - هام جداً:
       - أحياناً يَرِد في منتصف النص أو بعد المتن جملة مثل:
         "قال ابن شهاب وأخبرني أبو سلمة بن عبد الرحمن أن جابر بن عبد الله قال..."
       - هذا ليس جزءاً من المتن! هذا سند جديد تماماً لقصة/رواية مختلفة.
       - يجب استخراجه كـ chain مستقل بـ type="nested".
       - عادة يبدأ بـ "قال فلان وأخبرني/وحدثني فلان أن فلان..."
       - انتبه: الصحابي (lead) في السند المدرج قد يكون مختلفاً عن الصحابي الأول.
       - كذلك عبارات مثل "وقال فلان وفلان" في آخر الحديث قد تشير لرواة آخرين
         رووا عن نفس الشيخ → افصلهم كـ chains بـ type="follow_up".

    5) استخراج الأسماء:
       - استخدم الاسم الكامل الصريح عند توفره، تجنب الكنى وحدها.
         مثال: "أبو اليمان الحكم بن نافع" → استخدم "الحكم بن نافع"
       - لكن إن كانت الكنية هي الوحيدة (مثل "أبو هريرة" فقط) فاستخدمها.
       - "عن أبيه" → حاول تحديد الأب من السياق إن أمكن، وإلا اتركها "أبيه".
       - لا تُدخل أسماء من المتن/القصة، فقط من سياق الأداء (حدثنا/أخبرنا/عن/قال/سمعت).
       - لا تُدرج النبي صلى الله عليه وسلم ضمن الرواة.

    6) إزالة الألقاب والتبجيل:
       - احذف: رضي الله عنه/عنها/عنهم، أم المؤمنين، صلى الله عليه وسلم، عليه السلام، رحمه الله
       - مثال: "عمر بن الخطاب رضي الله عنه" → "عمر بن الخطاب"
       - مثال: "عائشة أم المؤمنين رضي الله عنها" → "عائشة"

    7) ترتيب الرواة:
       - الترتيب من المصنف/الشيخ الأعلى في النص (index 0) إلى الصحابي (آخر عنصر).
       - آخر راوٍ في كل chain يكون role="lead"، الباقي role="narrator".
       - كل chain لازم يكون له lead واحد فقط (آخر عنصر).

    8) chain_id:
       - استخدم chain_1, chain_2, chain_3... بدون فجوات، متسلسلاً.

    9) chain type:
       - "primary": السند الرئيسي، أو أسانيد القران المتساوية، أو أسانيد التحويل (ح).
       - "follow_up": المتابعات والطرق الإضافية فقط (تابعه، ورواه، وقال، إلخ).
       - "nested": الأسانيد المدرجة في منتصف النص (قال فلان وأخبرني/وحدثني فلان أن فلان).

    10) استخراج المتن (matn_segments):
       - استخرج المتن في الحقل "matn_segments" على هيئة قائمة نصوص.
       - الحالة الافتراضية: عنصر واحد فقط في القائمة.
       - اجعلها أكثر من عنصر عند وجود سند مدرج/قصة إضافية مستقلة دلالياً.
       - المتن = مضمون الرواية فقط، وليس جسر الإسناد أو أسماء الرواة.
       - احذف صيغ الأداء الشائعة في بداية كل مقطع مثل:
         "قال رسول الله"، "أن رسول الله قال"، "عن النبي قال"، "سمعت النبي يقول"، "قال النبي".
       - ابدأ مباشرةً بجوهر المعنى/القول في كل مقطع.
       - لا تجعل "matn_segments" فارغة.
       - أبقِ ألفاظ المتن الأصلية كما هي قدر الإمكان بعد حذف صيغ الأداء فقط.

    أخرج JSON فقط دون أي شرح أو تعليق.
    """).strip()


# =========================
# Few-shots
# =========================
# Covers: simple, coupling, ح separator, mutaba'at
EXAMPLES = [
    # Example 1: Simple single chain
    {
        "hadith_text": (
            "حدثنا عبدالله بن يوسف أخبرنا مالك عن نافع عن ابن عمر "
            "أن رسول الله صلى الله عليه وسلم قال ..."
        ),
        "output": {
            "matn_segments": ["..."],
            "chains": [
                {
                    "chain_id": "chain_1",
                    "type": "primary",
                    "narrators": [
                        {"name": "عبدالله بن يوسف", "role": "narrator"},
                        {"name": "مالك", "role": "narrator"},
                        {"name": "نافع", "role": "narrator"},
                        {"name": "ابن عمر", "role": "lead"},
                    ],
                }
            ]
        },
    },
    # Example 2: Coupling (عن فلان وفلان) → two primary chains
    {
        "hadith_text": (
            "حدثنا عبد الله قال أخبرنا يونس عن الزهري عن عروة "
            "عن المسور بن مخرمة ومروان قالا ..."
        ),
        "output": {
            "matn_segments": ["..."],
            "chains": [
                {
                    "chain_id": "chain_1",
                    "type": "primary",
                    "narrators": [
                        {"name": "عبد الله", "role": "narrator"},
                        {"name": "يونس", "role": "narrator"},
                        {"name": "الزهري", "role": "narrator"},
                        {"name": "عروة", "role": "narrator"},
                        {"name": "المسور بن مخرمة", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_2",
                    "type": "primary",
                    "narrators": [
                        {"name": "عبد الله", "role": "narrator"},
                        {"name": "يونس", "role": "narrator"},
                        {"name": "الزهري", "role": "narrator"},
                        {"name": "عروة", "role": "narrator"},
                        {"name": "مروان", "role": "lead"},
                    ],
                },
            ]
        },
    },
    # Example 3: ح separator → two complete chains
    {
        "hadith_text": (
            "حدثنا آدم قال حدثنا شعبة عن قتادة عن أنس ح وحدثنا يعقوب "
            "قال حدثنا ابن علية عن عبد العزيز بن صهيب عن أنس قال ..."
        ),
        "output": {
            "matn_segments": ["..."],
            "chains": [
                {
                    "chain_id": "chain_1",
                    "type": "primary",
                    "narrators": [
                        {"name": "آدم", "role": "narrator"},
                        {"name": "شعبة", "role": "narrator"},
                        {"name": "قتادة", "role": "narrator"},
                        {"name": "أنس", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_2",
                    "type": "primary",
                    "narrators": [
                        {"name": "يعقوب", "role": "narrator"},
                        {"name": "ابن علية", "role": "narrator"},
                        {"name": "عبد العزيز بن صهيب", "role": "narrator"},
                        {"name": "أنس", "role": "lead"},
                    ],
                },
            ]
        },
    },
    # Example 4: Mutaba'at with chain completion (follow_up)
    {
        "hadith_text": (
            "حدثنا يحيى بن بكير قال حدثنا الليث عن عقيل عن ابن شهاب "
            "عن عروة بن الزبير عن عائشة ... "
            "تابعه عبد الله بن يوسف وأبو صالح عن الزهري"
        ),
        "output": {
            "matn_segments": ["..."],
            "chains": [
                {
                    "chain_id": "chain_1",
                    "type": "primary",
                    "narrators": [
                        {"name": "يحيى بن بكير", "role": "narrator"},
                        {"name": "الليث", "role": "narrator"},
                        {"name": "عقيل", "role": "narrator"},
                        {"name": "ابن شهاب", "role": "narrator"},
                        {"name": "عروة بن الزبير", "role": "narrator"},
                        {"name": "عائشة", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_2",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "عبد الله بن يوسف", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_3",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "أبو صالح", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
            ]
        },
    },
    # Example 5: Nested chain + follow_ups at end (hadith #3 pattern)
    {
        "hadith_text": (
            "حدثنا يحيى بن بكير قال حدثنا الليث عن عقيل عن ابن شهاب "
            "عن عروة بن الزبير عن عائشة أنها قالت ... "
            "قال ابن شهاب وأخبرني أبو سلمة بن عبد الرحمن أن جابر بن عبد الله الأنصاري قال ... "
            "تابعه عبد الله بن يوسف وأبو صالح وتابعه هلال بن رداد عن الزهري "
            "وقال يونس ومعمر بوادره"
        ),
        "output": {
            "matn_segments": [
                "... (المتن الأساسي)",
                "... (متن السند المدرج)"
            ],
            "chains": [
                {
                    "chain_id": "chain_1",
                    "type": "primary",
                    "narrators": [
                        {"name": "يحيى بن بكير", "role": "narrator"},
                        {"name": "الليث", "role": "narrator"},
                        {"name": "عقيل", "role": "narrator"},
                        {"name": "ابن شهاب", "role": "narrator"},
                        {"name": "عروة بن الزبير", "role": "narrator"},
                        {"name": "عائشة", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_2",
                    "type": "nested",
                    "narrators": [
                        {"name": "ابن شهاب", "role": "narrator"},
                        {"name": "أبو سلمة بن عبد الرحمن", "role": "narrator"},
                        {"name": "جابر بن عبد الله الأنصاري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_3",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "عبد الله بن يوسف", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_4",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "أبو صالح", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_5",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "هلال بن رداد", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_6",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "يونس", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
                {
                    "chain_id": "chain_7",
                    "type": "follow_up",
                    "narrators": [
                        {"name": "معمر", "role": "narrator"},
                        {"name": "الزهري", "role": "lead"},
                    ],
                },
            ]
        },
    },
]


def few_shot_block() -> str:
    blocks = []
    for ex in EXAMPLES:
        blocks.append(
            "نص:\n"
            + ex["hadith_text"]
            + "\nJSON:\n"
            + json.dumps(ex["output"], ensure_ascii=False)
        )
    return "\n\n---\n\n".join(blocks)


# =========================
# LLM Calls
# =========================
@dataclass
class RunStats:
    total: int = 0
    routed_to_strong: int = 0
    used_fallback: int = 0
    light_success: int = 0
    strong_success: int = 0
    router_true: int = 0
    len_threshold_true: int = 0


def make_llm(model: str, temperature: float = 0.0) -> ChatOpenAI:
    return ChatOpenAI(model=model, temperature=temperature, api_key=OPENAI_API_KEY)


def route_is_complex(hadith_text: str, book: str, stats: RunStats) -> bool:
    router_llm = make_llm(ROUTER_MODEL, TEMP).with_structured_output(RouteDecision)
    prompt = build_router_prompt(book)
    decision: RouteDecision = router_llm.invoke(
        [
            ("system", "أنت مساعد دقيق وتلتزم بالمخرجات الهيكلية فقط."),
            ("user", prompt + "\n\nنص الحديث:\n" + hadith_text),
        ]
    )
    if decision.complex:
        stats.router_true += 1
    return decision.complex


def extract_with_model(
    hadith_text: str, book: str, model: str
) -> HadithExtraction:
    llm = make_llm(model, TEMP).with_structured_output(HadithExtraction)

    prompt = build_advanced_extractor_prompt(book)
    fewshots = few_shot_block()

    user_msg = (
        prompt
        + "\n\nأمثلة:\n"
        + fewshots
        + "\n\n===\n\nنص الحديث المطلوب:\n"
        + hadith_text
    )

    extraction: HadithExtraction = llm.invoke(
        [
            ("system", "أنت مساعد لا يخرج إلا JSON مطابق للـ schema."),
            ("user", user_msg),
        ]
    )
    extraction.matn_segments = [
        normalize_whitespace(seg) for seg in extraction.matn_segments
    ]
    # Renumber chain_ids defensively
    extraction.chains = renumber_chain_ids(extraction.chains)
    ok, reason = basic_validate_extraction(hadith_text, extraction)
    if not ok:
        raise ValueError(f"Validation failed: {reason}")
    return extraction


def process_one_hadith(
    hadith_text: str,
    hadith_index: int,
    book: str,
    stats: RunStats,
) -> Dict[str, Any]:
    text = normalize_whitespace(hadith_text)
    stats.total += 1

    force_strong = False
    if len(text) >= LEN_STRONG_THRESHOLD:
        force_strong = True
        stats.len_threshold_true += 1

    complex_flag = False
    if not force_strong:
        complex_flag = route_is_complex(text, book, stats)
        if complex_flag:
            force_strong = True

    # Determine routing reason
    if len(text) >= LEN_STRONG_THRESHOLD:
        route_reason = "length_threshold"
    elif complex_flag:
        route_reason = "router_complex"
    else:
        route_reason = "default_light"

    model_used = ""
    if force_strong:
        stats.routed_to_strong += 1
        extraction = extract_with_model(text, book, STRONG_MODEL)
        stats.strong_success += 1
        model_used = STRONG_MODEL
    else:
        try:
            extraction = extract_with_model(text, book, LIGHT_MODEL)
            stats.light_success += 1
            model_used = LIGHT_MODEL
        except Exception:
            stats.used_fallback += 1
            extraction = extract_with_model(text, book, STRONG_MODEL)
            stats.strong_success += 1
            model_used = STRONG_MODEL
            route_reason = "fallback"

    return {
        "hadith_index": hadith_index,
        "hadith_text": hadith_text,
        "model_used": model_used,
        "route_reason": route_reason,
        "matn_segments": [
            normalize_whitespace(seg) for seg in extraction.matn_segments
        ],
        "chains": [
            {
                "chain_id": c.chain_id,
                "type": c.type,
                "narrators": [
                    {
                        "name": clean_narrator_name(n.name),
                        "attributes": {"role": n.role},
                    }
                    for n in c.narrators
                ],
            }
            for c in extraction.chains
        ],
    }


# =========================
# CSV Loading / Resuming
# =========================
def load_hadiths_from_csv(
    csv_path: str, text_column: str = "hadith_text"
) -> List[str]:
    hadiths: List[str] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []

        if text_column not in reader.fieldnames:
            raise ValueError(
                f"Column '{text_column}' not found in {csv_path}. "
                f"Available columns: {reader.fieldnames}"
            )

        for row in reader:
            value = (row.get(text_column) or "").strip()
            if value:
                hadiths.append(value)
    return hadiths


def load_existing_results(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def save_results(path: str, results: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)


def build_test_output_path(path: str) -> str:
    base, ext = os.path.splitext(path)
    if not ext:
        ext = ".json"
    return f"{base}_test_one_hadith{ext}"


# =========================
# Main
# =========================
def main():
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY in environment.")

    if TEST_HADITH:
        print("Single hadith test mode enabled (test_hadith provided).")
        stats = RunStats()
        output_path = build_test_output_path(OUT_JSON_PATH)
        try:
            item = process_one_hadith(TEST_HADITH, 1, BOOK, stats)
            save_results(output_path, [item])
            print(f"Saved test output to: {output_path}")
            print(f"Chains: {len(item['chains'])}")
        except Exception as e:
            error_item = {
                "hadith_index": 1,
                "hadith_text": TEST_HADITH,
                "model_used": "",
                "route_reason": "",
                "matn_segments": [],
                "chains": [],
                "error": str(e),
            }
            save_results(output_path, [error_item])
            print(f"[ERROR] Single hadith test failed: {e}")
            print(f"Saved error output to: {output_path}")
        return

    print("Loading hadiths...")
    hadiths = load_hadiths_from_csv(CSV_PATH, CSV_TEXT_COLUMN)
    if MAX_HADITHS > 0:
        hadiths = hadiths[:MAX_HADITHS]
        print(f"Using first {len(hadiths)} hadith(s) due to MAX_HADITHS={MAX_HADITHS}")
    print(f"Loaded: {len(hadiths)}")

    if RESUME:
        results = load_existing_results(OUT_JSON_PATH)
        start_index = len(results) + 1
        print(f"Resuming from hadith_index={start_index}")
    else:
        results = []
        start_index = 1
        print("Resume disabled (RESUME=0): starting from hadith_index=1")

    stats = RunStats()

    for i, hadith_text in enumerate(hadiths, 1):
        if i < start_index:
            continue

        print(f"\n--- Hadith {i} ---")
        try:
            item = process_one_hadith(hadith_text, i, BOOK, stats)
            results.append(item)
            save_results(OUT_JSON_PATH, results)

            n_chains = len(item["chains"])
            types = [c["type"] for c in item["chains"]]
            print(f"Saved {i}/{len(hadiths)} | Chains: {n_chains} | Types: {types}")

        except Exception as e:
            print(f"[ERROR] Hadith {i}: {e}")
            results.append(
                {
                    "hadith_index": i,
                    "hadith_text": hadith_text,
                    "model_used": "",
                    "route_reason": "",
                    "matn_segments": [],
                    "chains": [],
                    "error": str(e),
                }
            )
            save_results(OUT_JSON_PATH, results)

    print("\n=== DONE ===")
    print(f"Total processed: {stats.total}")
    print(f"Len>=threshold routed to strong: {stats.len_threshold_true}")
    print(f"Router complex=true: {stats.router_true}")
    print(f"Total routed to strong: {stats.routed_to_strong}")
    print(f"Light success: {stats.light_success}")
    print(f"Strong success: {stats.strong_success}")
    print(f"Fallback used (light->strong): {stats.used_fallback}")
    print(f"Output: {OUT_JSON_PATH}")


if __name__ == "__main__":
    main()
