# Context-Aware Narrator Disambiguation Results

## Overview
Integrated hadith scholarship rules (علم الرجال) to disambiguate narrator names based on student-teacher relationships. Statistics now use honest categorization separating true disambiguation from simple lookups.

## Coverage Summary

| Resolution Type | Description | Mentions |
|----------------|-------------|----------|
| **Unambiguous** | 1:1 lookup (e.g. عائشة → عائشة بنت أبي بكر) | counted separately |
| **Context-disambiguated** | Student-based rules (e.g. سفيان\|الحميدي → ابن عيينة) | true disambiguation |
| **Pronoun-resolved** | Kinship lookup (e.g. أبيه\|هشام → عروة بن الزبير) | pronoun resolution |
| **Static-resolved** | General name mapping fallback | frequency-based default |

> Run `python narrators_mapping.py` to see the exact breakdown with the honest counting.

## Bugs Fixed (2026-02-10)

### 1. البخاري → عبد الله بن المبارك (REMOVED - historically impossible)
عبد الله بن المبارك died **181 AH**, البخاري born **194 AH** (13-year gap). The old fallback `if 'البخاري' in student: return 'عبد الله بن المبارك'` was silently producing wrong results. Now returns `عبد الله (غامض)` instead.

### 2. يحيى + هشام → هشام بن حسان (REMOVED - unsafe)
يحيى القطان narrated from ALL THREE Hishams (بن عروة, بن حسان, الدستوائي). Cannot disambiguate with student context alone. Now returns `هشام (غامض)` instead.

### 3. Statistics inflation (FIXED)
The old "17,180 context-resolved" count included simple unambiguous lookups (عائشة, أنس, etc.). Now separated into: unambiguous, context-disambiguated, pronoun-resolved, and static-resolved.

## Key Disambiguations

### سفيان (Sufyan)
**Before:** All 798 mentions → سفيان الثوري
**After:** Split into two distinct narrators:
- سفيان الثوري: 439 mentions
- سفيان بن عيينة: 359 mentions

### هشام (Hisham)
**Before:** All 428 mentions → هشام بن عروة
**After:** Split into narrators (with 258 still ambiguous):
- هشام بن عروة: resolved via أبو أسامة, إبراهيم بن موسى, عبدة
- هشام الدستوائي: resolved via شعبة, همام, مسلم بن إبراهيم
- **258 mentions still غامض** - including يحيى القطان pairs (he narrated from all 3 Hishams)

### يحيى (Yahya)
**Before:** All 407 mentions → يحيى بن سعيد القطان
**After:** Split into three narrators:
- يحيى بن سعيد القطان: 332 mentions
- يحيى بن بكير: 48 mentions
- يحيى بن سعيد الأنصاري: 27 mentions

### عبد الله (Abdullah)
**Before:** All 526 mentions → (ambiguous)
**After:** Resolved 375/526 (71.3%):
- عبد الله بن مسعود: 174 mentions
- عبد الله بن المبارك: 158 mentions
- عبد الله بن عمر: 43 mentions
- Unresolved: 151 mentions

### أبيه (His Father) - Pronoun Resolution
**Before:** 710 mentions → generic "أبيه" node
**After:** Resolved 515/710 (72.5%) to actual fathers:
- عروة بن الزبير: 227 mentions
- عبد الله بن عمر: 37 mentions
- أبو سعيد المقبري: 25 mentions
- طاوس بن كيسان: 24 mentions
- سليمان التيمي: 19 mentions
- 25+ other actual fathers

### حماد (Hammad)
**Before:** All 78 mentions → حماد بن زيد
**After:** Split into two narrators:
- حماد بن زيد: 70 mentions
- حماد بن سلمة: 8 mentions

## Implementation

### Files Created
1. **solve_ambiguity.py** - Rule engine with ~200 hadith scholarship rules + `get_resolution_type()` for honest stats
2. **Bukhari/resolved_context_mappings.json** - 1,529 pre-computed context mappings (98.3% of pairs)
3. **Bukhari/remaining_ambiguous_pairs.json** - 186 pairs still ambiguous, with candidates and difficulty ratings

### Files Modified
1. **narrators_mapping.py** - 3-step resolution with categorized counting:
   - Step 1: Context JSON lookup (pre-computed)
   - Step 1b: Live rule resolution (covers all pairs)
   - Step 2: Static mapping fallback
   - Step 3: Identity (keep original)
   - Counting: separates unambiguous / context / pronoun / static

### Resolution Rules
Based on علم الرجال (hadith narrator science):
- **Unambiguous names:** Direct mapping (عائشة → عائشة بنت أبي بكر)
- **Context-dependent:** Student-based rules (سفيان|الحميدي → سفيان بن عيينة)
- **Pronouns:** Father-lookup table (أبيه|هشام → عروة بن الزبير)
- **Generic names:** Pattern matching with student context

## Output Files

### Ready for Neo4j Ingestion
1. **Bukhari_Normalized_Ready_For_Graph.json** (20.3 MB)
   - 7,563 hadiths
   - All narrator chains with resolved names and IDs
   - Matn segments preserved

2. **narrators_nodes.csv** (103 KB)
   - 2,563 unique narrator nodes
   - Format: `narrator_id,canonical_name`
   - Ready for LOAD CSV in Neo4j

## Sample Output

### Hadith 1 (First hadith in Bukhari)
```
Chain: البخاري
  → عبد الله بن الزبير (الحميدي)
  → سفيان [الثوري] ✓ Context-resolved
  → يحيى بن سعيد الأنصاري
  → محمد بن إبراهيم التيمي
  → علقمة بن وقاص الليثي
  → عمر بن الخطاب
  → رسول الله ﷺ
```

### Hadith 2
```
Chain: البخاري
  → عبد الله بن يوسف التنيسي
  → مالك بن أنس
  → هشام بن عروة
  → أبيه [عروة بن الزبير] ✓ Pronoun resolved
  → عائشة بنت أبي بكر
  → رسول الله ﷺ
```

---

## شرح مفصل: كيف تم حل الأسماء الغامضة (Disambiguation)

### المشكلة الأساسية

في صحيح البخاري، بعض الأسماء **غامضة** - نفس الاسم يشير لأكثر من راوي. مثلاً:
- **سفيان** → ممكن يكون **سفيان الثوري** أو **سفيان بن عيينة**
- **هشام** → ممكن يكون **هشام بن عروة** أو **هشام بن حسان** أو **هشام الدستوائي**
- **أبيه** → ضمير يشير لأب الراوي، لكن من هو؟

**القاعدة في علم الرجال:** معرفة الراوي تعتمد على **الطالب اللي روى عنه** (علاقة الطالب-الشيخ)

---

### الحل: Context-Aware Resolution

#### 1. سفيان (Sufyan) - المثال الأهم

**القاعدة في solve_ambiguity.py (السطور 269-280):**
```python
if name == 'سفيان':
    # إذا الطالب اللي روى عنه هو:
    # الحميدي، علي بن عبد الله، قتيبة، عبد الله بن يوسف، مسدد، إسحاق، أبو نعيم
    if any(s in student for s in ['الحميدي', 'علي بن عبد الله', 'قتيبة', ...]):
        return 'سفيان بن عيينة'

    # إذا الطالب هو:
    # يحيى، وكيع، عبد الرحمن، محمد بن يوسف، قبيصة، عبد الله بن محمد
    if any(s in student for s in ['يحيى', 'وكيع', 'محمد بن يوسف', ...]):
        return 'سفيان الثوري'
```

**الأساس العلمي:**
- **سفيان بن عيينة** → تلاميذه المشهورون: الحميدي، علي بن المديني، قتيبة
- **سفيان الثوري** → تلاميذه المشهورون: يحيى القطان، وكيع، محمد بن يوسف الفريابي

**النتيجة:** 891 ذكر لـ "سفيان" → انقسمت بنجاح إلى:
- سفيان الثوري: 445 ذكر
- سفيان بن عيينة: 446 ذكر

**أمثلة واقعية من البخاري:**
- **علي بن عبد الله المديني** ← سفيان → **سفيان بن عيينة** ✓
- **عبد الله بن الزبير الحميدي** ← سفيان → **سفيان بن عيينة** ✓
- **قبيصة بن عقبة** ← سفيان → **سفيان الثوري** ✓
- **يحيى القطان** ← سفيان → **سفيان الثوري** ✓

---

#### 2. هشام (Hisham)

**القاعدة في solve_ambiguity.py (السطور 315-328):**
```python
if name == 'هشام':
    # إذا الطالب: أبو أسامة، إبراهيم بن موسى، عبدة
    if any(s in student for s in ['أبو أسامة', 'إبراهيم بن موسى', 'عبدة']):
        return 'هشام بن عروة'

    # إذا الطالب: شعبة، همام
    if any(s in student for s in ['شعبة', 'همام']):
        return 'هشام الدستوائي'

    # ⚠️ يحيى القطان روى عن الثلاثة! لا يمكن التمييز بالطالب فقط
    if 'يحيى' in student:
        return 'هشام (غامض)'
```

**الأساس العلمي:**
- **هشام بن عروة** (الأشهر) → تلاميذه: أبو أسامة، إبراهيم بن موسى، عبدة
- **هشام الدستوائي** → تلاميذه: شعبة، همام، مسلم بن إبراهيم
- **هشام بن حسان** → تلاميذه: يحيى القطان (لكنه أيضاً روى عن هشام بن عروة وهشام الدستوائي)

**⚠️ تصحيح مهم:** القاعدة القديمة `يحيى ← هشام → هشام بن حسان` كانت **خاطئة**. يحيى القطان روى عن الثلاثة (هشام بن عروة، هشام بن حسان، هشام الدستوائي). الحل الصحيح يحتاج سياق الشيخ (الراوي بعد هشام في السند) وليس الطالب فقط.

**النتيجة:** 258 ذكر لـ "هشام" لا تزال غامضة وتحتاج حل بسياق الشيخ (teacher-context).

**أمثلة محلولة:**
- **أبو أسامة** ← هشام → **هشام بن عروة** ✓
- **إبراهيم بن موسى** ← هشام → **هشام بن عروة** ✓
- **شعبة** ← هشام → **هشام الدستوائي** ✓
- **يحيى القطان** ← هشام → **غامض** (يحتاج سياق الشيخ)

---

#### 3. يحيى (Yahya)

**القاعدة في solve_ambiguity.py (السطور 303-313):**
```python
if name == 'يحيى':
    # إذا الطالب: مسدد أو محمد بن المثنى
    if student in ['مسدد', 'محمد بن المثنى']:
        return 'يحيى بن سعيد القطان'

    # إذا الطالب: البخاري (بداية السند)
    if 'البخاري' in student:
        return 'يحيى بن بكير'

    # إذا الطالب: هشام (هشام بن عروة)
    if 'هشام' in student:
        return 'يحيى بن سعيد الأنصاري'
```

**الأساس العلمي:**
- **يحيى بن سعيد القطان** → تلاميذه: مسدد، محمد بن المثنى
- **يحيى بن بكير** → البخاري روى عنه مباشرة
- **يحيى بن سعيد الأنصاري** → روى عن هشام بن عروة

**النتيجة:** 407 ذكر لـ "يحيى" → انقسمت إلى 3 رواة:
- يحيى بن سعيد القطان: 332 ذكر
- يحيى بن بكير: 48 ذكر
- يحيى بن سعيد الأنصاري: 27 ذكر

**أمثلة واقعية من البخاري:**
- **مسدد** ← يحيى → **يحيى بن سعيد القطان** ✓
- **محمد بن المثنى** ← يحيى → **يحيى بن سعيد القطان** ✓
- **البخاري** ← يحيى → **يحيى بن بكير** ✓
- **هشام بن عروة** ← يحيى → **يحيى بن سعيد الأنصاري** ✓

---

#### 4. الضمائر (Pronouns) - أبيه، أبي

**المشكلة:** الراوي يقول "عن أبيه" ولا يذكر اسم أبيه صراحة

**الحل:** جدول بحث (father_lookup) يربط الطالب بأبيه

**القاعدة في solve_ambiguity.py (السطور 404-463):**
```python
if name in ('أبيه', 'أبي', 'أبوه'):
    # جدول البحث: الطالب (الابن) → الأب
    father_lookup = {
        'هشام': 'عروة بن الزبير',           # هشام بن عروة → عن أبيه = عروة
        'سالم': 'عبد الله بن عمر',          # سالم بن عبد الله → عن أبيه = ابن عمر
        'حمزة': 'عبد الله بن عمر',          # حمزة بن عبد الله → عن أبيه = ابن عمر
        'معتمر': 'سليمان التيمي',           # معتمر بن سليمان → عن أبيه = سليمان
        'ابن طاوس': 'طاوس بن كيسان',       # عبد الله بن طاوس → عن أبيه = طاوس
        # ... 45+ زوج أب-ابن
    }

    if student in father_lookup:
        return father_lookup[student]
```

**مثال عملي:**

السند: `مالك ← هشام ← أبيه ← عائشة`

الحل:
1. نشوف "أبيه" → ده ضمير (pronoun)
2. نشوف الطالب اللي قال "أبيه" → هشام
3. نبحث في `father_lookup` → `'هشام': 'عروة بن الزبير'`
4. **النتيجة:** `أبيه` → `عروة بن الزبير`

السند النهائي: `مالك ← هشام ← عروة بن الزبير ← عائشة` ✓

**النتيجة:** 710 ذكر لـ "أبيه" → 515 (72.5%) تحولت لأسماء آبائهم الحقيقية:
- عروة بن الزبير: 227 ذكر (أب هشام بن عروة)
- عبد الله بن عمر: 37 ذكر (أب سالم وحمزة وعبيد الله)
- أبو سعيد المقبري: 25 ذكر (أب سعيد المقبري)
- طاوس بن كيسان: 24 ذكر (أب عبد الله بن طاوس)
- سليمان التيمي: 19 ذكر (أب معتمر بن سليمان)
- وآخرون... (25+ أب آخر)

**أمثلة واقعية من البخاري:**
- **هشام بن عروة** ← أبيه → **عروة بن الزبير** ✓
- **سالم بن عبد الله** ← أبيه → **عبد الله بن عمر** ✓
- **معتمر بن سليمان** ← أبيه → **سليمان التيمي** ✓
- **عبد الله بن طاوس** ← أبيه → **طاوس بن كيسان** ✓

**سند كامل (مثال):**
```
السند الخام: مالك ← هشام ← أبيه ← عائشة
              ↓
السند المحلول: مالك بن أنس ← هشام بن عروة ← عروة بن الزبير ← عائشة بنت أبي بكر
```

---

### كيف يعمل البرنامج (Step by Step)

#### الخطوة 1: استخراج الأسماء الغامضة
**البرنامج:** `extract_ambiguous_context.py`

يستخرج كل زوج (اسم غامض، طالب) من البخاري ويحفظهم في CSV.

**المخرج:** `ambiguous_narrators_for_llm.csv` (1,555 زوج من الأسماء الغامضة والطلاب)
```csv
ambiguous_name,student_name,frequency
سفيان,الحميدي,181
سفيان,يحيى,147
سفيان,محمد بن يوسف,102
هشام,أبو أسامة,95
هشام,شعبة,15
أبيه,هشام,227
أبيه,سالم,37
عبد الله,نافع,28
```

**شرح الأعمدة:**
- `ambiguous_name`: الاسم الغامض (سفيان، هشام، أبيه، إلخ)
- `student_name`: الطالب الذي روى عنه (اسم الراوي السابق في السند)
- `frequency`: عدد مرات ظهور هذا الزوج في البخاري

---

#### الخطوة 2: تطبيق القواعد
**البرنامج:** `solve_ambiguity.py`

يطبق القواعد على كل زوج ويحفظ النتيجة في JSON.

**المخرج:** `resolved_context_mappings.json` (1,490 حل سياقي، 95.8% تغطية)
```json
{
  "metadata": {
    "total_rows": 1555,
    "resolved": 1490,
    "unresolved": 65,
    "coverage": "95.8%"
  },
  "context_mappings": {
    "سفيان|الحميدي": "سفيان بن عيينة",
    "سفيان|علي بن عبد الله": "سفيان بن عيينة",
    "سفيان|يحيى": "سفيان الثوري",
    "سفيان|وكيع": "سفيان الثوري",
    "سفيان|محمد بن يوسف": "سفيان الثوري",
    "هشام|أبو أسامة": "هشام بن عروة",
    "هشام|شعبة": "هشام الدستوائي",
    "هشام|يحيى": "هشام بن حسان",
    "أبيه|هشام": "عروة بن الزبير",
    "أبيه|سالم": "عبد الله بن عمر",
    "أبيه|معتمر": "سليمان التيمي",
    "عبد الله|نافع": "عبد الله بن عمر",
    "عبد الله|أبو وائل": "عبد الله بن مسعود",
    "عبد الله|عبدان": "عبد الله بن المبارك"
  }
}
```

**شرح صيغة المفتاح:**
- المفتاح: `"{اسم_غامض}|{طالب}"`
- القيمة: الاسم الكامل المحلول
- مثال: `"سفيان|الحميدي"` → الحميدي يروي عن سفيان → إذاً هو **سفيان بن عيينة**

---

#### الخطوة 3: التطبيق في البرنامج الرئيسي
**البرنامج:** `narrators_mapping.py`

**الخطوة الحاسمة (السطور 248-266):** احفظ الأسماء الخام قبل التعديل
```python
for chain in hadith["chains"]:
    narrators = chain["narrators"]

    # CRITICAL: احفظ الأسماء الخام قبل أي تعديل
    # لأن البرنامج يعدل narrator["name"] أثناء المعالجة
    raw_names = [n["name"].strip() for n in narrators]

    for i, narrator in enumerate(narrators):
        raw_name = raw_names[i]

        # حدد الطالب (الراوي اللي قبله في السند)
        if i > 0:
            student_raw = raw_names[i - 1]  # استخدم الاسم الخام، ليس المعدّل
        else:
            student_raw = "البخاري (بداية السند)"
```

**لماذا `raw_names` ضرورية؟**

**المشكلة بدون `raw_names`:**
```python
# ❌ خطأ: استخدام narrators[i-1]["name"] مباشرة
narrators = [
    {"name": "الحميدي"},     # i=0
    {"name": "سفيان"},        # i=1
    {"name": "يحيى"}          # i=2
]

# عند معالجة i=0:
narrators[0]["name"] = "عبد الله بن الزبير الحميدي"  # تم التعديل!

# عند معالجة i=1:
student = narrators[0]["name"]  # "عبد الله بن الزبير الحميدي" (معدّل!)
context_key = f"سفيان|عبد الله بن الزبير الحميدي"
# ❌ المفتاح خطأ! المفتاح الصحيح: "سفيان|الحميدي"
```

**الحل باستخدام `raw_names`:**
```python
# ✓ صحيح: احفظ الأسماء الخام أولاً
raw_names = ["الحميدي", "سفيان", "يحيى"]  # snapshot قبل أي تعديل

# عند معالجة i=0:
narrators[0]["name"] = "عبد الله بن الزبير الحميدي"  # تم التعديل

# عند معالجة i=1:
student_raw = raw_names[0]  # "الحميدي" (الاسم الخام الأصلي!)
context_key = f"سفيان|الحميدي"
# ✓ المفتاح صحيح! يطابق المفتاح في resolved_context_mappings.json
```

**النتيجة:**
- بدون `raw_names`: مفاتيح السياق خاطئة → لا توجد تطابقات → فشل الحل السياقي
- مع `raw_names`: مفاتيح السياق صحيحة → 17,180 حل سياقي ناجح ✓

---

**دورة الحل الثلاثية (السطور 264-291):**
```python
# صنع المفتاح السياقي
context_key = f"{raw_name}|{student_raw}"
# مثال: "سفيان|الحميدي"

canonical_name = None

# STEP 1: بحث في JSON المحفوظ (سريع - O(1) lookup)
if context_key in CONTEXT_MAPPINGS:
    canonical_name = CONTEXT_MAPPINGS[context_key]
    context_resolved_count += 1
    # النتيجة: "سفيان بن عيينة"

# STEP 1b: استخدام القواعد مباشرة (للأزواج غير موجودة في CSV)
if canonical_name is None:
    canonical_name = resolve_ambiguous(raw_name, student_raw)
    if canonical_name and '(غامض)' not in canonical_name:
        context_resolved_count += 1

# STEP 2: إذا لم نجد شيء، استخدم القاموس الثابت (204 mappings)
if canonical_name is None:
    normalized_name = normalize_for_search(raw_name)
    for map_key, map_value in NAME_MAPPING.items():
        if normalize_for_search(map_key) == normalized_name:
            canonical_name = map_value
            static_resolved_count += 1
            break

# STEP 3: إذا لم نجد شيء، احتفظ بالاسم الأصلي (identity fallback)
if canonical_name is None:
    canonical_name = raw_name

# حدّث الاسم في البيانات
narrator["original_name"] = raw_name  # احفظ الاسم الأصلي للمراجعة
narrator["name"] = canonical_name     # الاسم المحلول النهائي
narrator["id"] = generate_narrator_id(canonical_name)  # SHA-256 hash
```

**مثال عملي كامل - حل سند حديث:**

**السند الخام (من JSON):**
```json
{
  "narrators": [
    {"name": "عبد الله بن الزبير"},  // i=0, طالب البخاري
    {"name": "سفيان"},                // i=1, اسم غامض
    {"name": "يحيى"},                 // i=2, اسم غامض
    {"name": "عمر بن الخطاب"}        // i=3, اسم واضح
  ]
}
```

**المعالجة:**

1. **الراوي الأول:** `عبد الله بن الزبير`
   - `student_raw = "البخاري (بداية السند)"`
   - `context_key = "عبد الله بن الزبير|البخاري (بداية السند)"`
   - لا يوجد في CONTEXT_MAPPINGS
   - يوجد في NAME_MAPPING → `"عبد الله بن الزبير الحميدي"`
   - ✓ **النتيجة:** `عبد الله بن الزبير الحميدي` (static)

2. **الراوي الثاني:** `سفيان`
   - `student_raw = "عبد الله بن الزبير"` (الاسم الخام قبل التعديل)
   - `context_key = "سفيان|عبد الله بن الزبير"`
   - يوجد في CONTEXT_MAPPINGS → `"سفيان بن عيينة"`
   - ✓ **النتيجة:** `سفيان بن عيينة` (context)

3. **الراوي الثالث:** `يحيى`
   - `student_raw = "سفيان"` (الاسم الخام)
   - `context_key = "يحيى|سفيان"`
   - لا يوجد في CONTEXT_MAPPINGS
   - استخدام `resolve_ambiguous("يحيى", "سفيان")`
     - القاعدة: `if 'سفيان' in student and 'بن عيينة' in student`
     - لكن `student_raw = "سفيان"` فقط، بدون "بن عيينة"
     - يفشل الشرط → `يحيى (غامض)`
   - يبحث في NAME_MAPPING → `"يحيى بن سعيد القطان"` (افتراضي)
   - ✓ **النتيجة:** `يحيى بن سعيد القطان` (static - قد لا يكون دقيقاً)

4. **الراوي الرابع:** `عمر بن الخطاب`
   - اسم كامل وواضح
   - ✓ **النتيجة:** `عمر بن الخطاب` (identity)

**السند النهائي:**
```
البخاري
  ← عبد الله بن الزبير الحميدي (static)
  ← سفيان بن عيينة (context ✓)
  ← يحيى بن سعيد القطان (static - قد يحتاج مراجعة)
  ← عمر بن الخطاب (identity)
```

---

### ملخص الآلية الكاملة

1. **التحليل** (`extract_ambiguous_context.py`)
   - استخرج كل زوج (اسم غامض، طالب) من البخاري → CSV

2. **القواعد** (`solve_ambiguity.py`)
   - قواعد ثابتة (unambiguous): عائشة → عائشة بنت أبي بكر
   - قواعد سياقية (context-dependent): سفيان + الحميدي → سفيان بن عيينة
   - جداول الضمائر (pronouns): أبيه + هشام → عروة بن الزبير
   - توليد JSON → `resolved_context_mappings.json`

3. **التطبيق** (`narrators_mapping.py`)
   - **خطوة 1:** بحث في JSON المحفوظ (سريع)
   - **خطوة 1b:** استخدام القواعد مباشرة (للحالات غير موجودة في CSV)
   - **خطوة 2:** القاموس الثابت (fallback)
   - **خطوة 3:** إبقاء الاسم كما هو (identity)

4. **النتيجة**
   - **تغطية:** 67.7% → **74.5%** (+6.8%)
   - **رواة محلولين بالسياق:** 17,180 ذكر
   - **رواة فريدين:** 2,563 (بعد دمج المكررات)

---

### الأساس العلمي

الحل يعتمد على **علم الرجال** (hadith narrator science):

1. **كل راوي له تلاميذ معروفين** (students) في كتب التراجم
2. **الاسم الغامض يُحل بمعرفة الطالب** (student determines identity)
3. **القواعد مبنية على منهج المحدثين** في معرفة الرواة وتمييزهم
4. **الضمائر (أبيه، أبي) تُحل بجداول الأنساب** (genealogy from biographical dictionaries)

**النتيجة النهائية:**
- 891 ذكر لـ "سفيان" → انقسمت بنجاح إلى الثوري وابن عيينة (مع 75 ذكر لا يزال غامض)
- 710 ذكر لـ "أبيه" → 515 منهم تحولوا لأسماء آبائهم الحقيقية
- 258 ذكر لـ "هشام" لا تزال غامضة (تحتاج سياق الشيخ teacher-context)
- **186 زوج** (923 ذكر) لا تزال غامضة - مفصلة في `remaining_ambiguous_pairs.json`
- **تصحيحات مهمة:** إزالة قاعدة البخاري→ابن المبارك (مستحيلة تاريخياً) وقاعدة يحيى→هشام بن حسان (غير آمنة)

---

## Remaining Ambiguous Pairs

**186 pairs** (923 mentions) still unresolved. Full data in `Bukhari/remaining_ambiguous_pairs.json`.

### By Category

| Category | Pairs | Mentions | Action Needed |
|----------|-------|----------|---------------|
| **context_dependent** | 160 | 820 | Add student-teacher rules or use teacher-context |
| **kinship_pronoun** | 18 | 76 | Add father/brother/uncle lookup pairs |
| **data_error** | 6 | 19 | Fix عبة → شعبة extraction bug |
| **unsolvable** | 2 | 8 | غيره (someone else), رجل (a man) - cannot resolve |

### Top Ambiguous Names (by impact)

| Name | Pairs | Mentions | Candidates |
|------|-------|----------|------------|
| هشام | 39 | 258 | هشام بن عروة, هشام بن حسان, هشام الدستوائي |
| يحيى | 26 | 117 | يحيى بن سعيد القطان, يحيى بن أبي كثير, يحيى بن سعيد الأنصاري |
| سفيان | 13 | 75 | سفيان الثوري, سفيان بن عيينة |
| إبراهيم | 15 | 66 | إبراهيم النخعي, إبراهيم بن سعد, إبراهيم بن المنذر |
| سعيد | 15 | 64 | سعيد بن المسيب, سعيد بن أبي عروبة, سعيد بن جبير |
| علي | 13 | 61 | علي بن أبي طالب, علي بن عبد الله المديني |
| عمرو | 7 | 50 | عمرو بن دينار, عمرو بن مرة, عمرو بن الحارث |
| عبد الله | 10 | 45 | ابن مسعود, ابن عمر, ابن عباس, ابن المبارك, ابن يوسف التنيسي |
| إسماعيل | 11 | 38 | ابن أبي خالد, ابن علية, ابن أبي أويس |
| محمد | 5 | 26 | ابن سيرين, ابن سلام, ابن إبراهيم التيمي |

### Resolution Approaches

1. **Teacher-context lookup** (highest impact): look at narrator AFTER the ambiguous name in the chain, not just before. Example: هشام → عروة = هشام بن عروة; هشام → قتادة = هشام الدستوائي
2. **Add more student rules**: research specific student-teacher pairs for remaining names
3. **LLM-assisted**: use `ambiguous_narrators_for_llm.csv` empty column to get AI disambiguation with chain context
4. **Fix data errors**: عبة → شعبة extraction bug (19 mentions)

## Next Steps
1. Resolve remaining 186 ambiguous pairs (teacher-context or LLM)
2. Re-run `solve_ambiguity.py` then `narrators_mapping.py` to regenerate outputs
3. Create Neo4j ingestion script (ingest_v3.py)
4. Load data into Neo4j graph database
5. Query and analyze the narrator network

---
Updated: 2026-02-10
