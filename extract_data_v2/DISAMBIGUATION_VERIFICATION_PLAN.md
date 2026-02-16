# Plan: 100% Accurate Narrator Disambiguation with Scholarly Verification

## Context

**Current Status:**
- Coverage: 75.1% (33,573 / 44,733 mentions)
- Context resolution: 97.0% (1,509 / 1,555 context pairs)
- Pronoun resolution: 90.7% (751 / 828 أبيه/أبي cases)

**Critical Problem Identified:**
The system achieves **97% resolution rate** but with **UNKNOWN ACCURACY**. A thorough code analysis revealed:

1. **Over-aggressive substring matching** - Rules like `if 'نافع' in student` match unintended substrings
2. **Broken fallback assumptions** - `if 'البخاري' in student: return 'عبد الله بن المبارك'` is historically false
3. **Incomplete father_lookup table** - Only 117 entries, missing ~30% of father-son pairs
4. **No validation mechanism** - Zero test suite, no cross-reference with biographical sources
5. **Systematic errors in عبد الله rules** - Only 71.3% resolved (375/526), highest error risk

**User Requirement:**
Achieve **100% accuracy** with scholarly verification BEFORE Neo4j ingestion. No hallucinations allowed.

---

## Strategy: Verification-First Approach

Instead of adding more rules blindly, we will:

1. **Extract and verify** all current resolutions against biographical sources
2. **Fix systematic errors** in existing rules (especially عبد الله, أبيه defaults)
3. **Manually resolve** remaining 127 cases (77 pronouns + 50 عبد الله) with scholarly references
4. **Create verification dataset** with citations for audit trail

---

## Phase 1: Extract Verification Dataset

### 1.1 Create verification extraction script

**File to create:** `extract_data_v2/verify_disambiguation.py`

**Purpose:** Extract all resolved cases with context for manual verification

```python
def extract_verification_dataset():
    """
    Extract all narrator resolutions with:
    - Original name
    - Resolved name
    - Student context
    - Hadith number
    - Resolution method (context/static/live_rule)
    """
    # Read Bukhari_Normalized_Ready_For_Graph.json
    # For each narrator:
    #   - Track original_name -> name transformation
    #   - Track which rule was used (from solve_ambiguity.py logs)
    #   - Track student context
    # Output: verification_dataset.csv with columns:
    #   hadith_number, narrator_index, original_name, resolved_name,
    #   student_name, resolution_method, needs_review
```

**Output CSV columns:**
- `hadith_number` - Reference to hadith in Bukhari
- `chain_index` - Which chain in hadith (some have multiple)
- `narrator_position` - Position in chain (0 = student of Bukhari)
- `original_name` - Raw name from JSON
- `resolved_name` - After disambiguation
- `student_name` - Previous narrator (context)
- `resolution_method` - "context_json" / "live_rule" / "static_mapping" / "identity"
- `confidence` - "high" / "medium" / "low" based on rule type
- `needs_review` - Boolean flag for manual review

**Confidence rules:**
- HIGH: Exact match in context JSON, unambiguous names (عائشة, مالك, أنس)
- MEDIUM: Live rule with multiple student checks (سفيان with 3+ students)
- LOW: Single substring match (e.g., `if 'نافع' in student`), defaults (e.g., `student == 'سعيد'`)

### 1.2 Flag high-risk cases

**Cases to auto-flag for review:**
1. عبد الله resolutions with student containing substring-only matches
2. أبيه resolutions using short name defaults (سعيد, واقد, عمرو)
3. Any resolution where student = البخاري (direct transmission)
4. Any resolution marked as "(غامض)" in resolve_ambiguous output
5. All unresolved cases (77 pronouns + 50 عبد الله)

**Expected output:** `verification_dataset.csv` with ~500-800 rows flagged for review

---

## Phase 2: Fix Systematic Errors

### 2.1 Fix عبد الله rules (HIGHEST PRIORITY)

**File:** `extract_data_v2/solve_ambiguity.py` lines 327-362

**Fixes needed:**

**Fix 1: Remove broken البخاري fallback**
```python
# BEFORE (Line 360-361):
if 'البخاري' in student:
    return 'عبد الله بن المبارك'  # ❌ WRONG - too broad

# AFTER:
if 'البخاري' in student:
    return None  # ✓ Mark for manual review
```

**Fix 2: Add specificity to substring matches**
```python
# BEFORE (Line 343):
if any(s in student for s in ['نافع', 'سالم', 'حمزة', ...]):
    return 'عبد الله بن عمر'

# AFTER:
# Check for exact matches first, then substrings with additional context
if student in ['نافع', 'سالم', 'حمزة']:
    return 'عبد الله بن عمر'
elif any(s in student for s in ['نافع مولى', 'سالم بن', 'حمزة بن']):
    return 'عبد الله بن عمر'
# Otherwise return None for manual review
```

**Fix 3: Re-order rules by statistical frequency**
Based on Bukhari statistics:
1. عبد الله بن عمر (Sahabi, most common)
2. عبد الله بن مسعود (Sahabi, very common)
3. عبد الله بن عباس (Sahabi, common)
4. عبد الله بن المبارك (Tabi'i, common in البخاري's direct students)

Current order (المبارك first) is likely wrong.

### 2.2 Fix أبيه defaults

**File:** `extract_data_v2/solve_ambiguity.py` lines 524-552

**Fixes needed:**

**Remove over-aggressive defaults:**
```python
# BEFORE (Lines 526-539):
if student == 'سعيد':
    return 'المسيب بن حزن'  # Default assumption
if student == 'سفيان':
    return 'سعيد بن مسروق الثوري'
# etc.

# AFTER:
# Only apply defaults if NO exact match in father_lookup
# Otherwise return None for manual review
if student == 'سعيد':
    # Check if there are multiple سعيد variants first
    # Only default if context strongly suggests المسيب
    return None  # ✓ Conservative approach
```

**Rationale:**
- Your unresolved data shows these defaults FAILED (11 cases of أبيه|سعيد unresolved)
- Better to mark as "needs review" than guess wrong

### 2.3 Add strict mode flag

Add `STRICT_MODE = True` flag at top of solve_ambiguity.py

When enabled:
- All substring-only matches return None (require manual review)
- All defaults return None
- Only exact matches in father_lookup table or multi-condition rules pass

This gives us a **verified baseline** of 100% accurate cases.

---

## Phase 3: Manual Resolution with Scholarly Sources

### 3.1 Prepare reference sources

**Sources to use:**

1. **Biographical dictionaries:**
   - تهذيب التهذيب (Tahdhib al-Tahdhib)
   - سير أعلام النبلاء (Siyar A'lam al-Nubala)
   - تقريب التهذيب (Taqrib al-Tahdhib)

2. **Bukhari commentaries:**
   - فتح الباري (Fath al-Bari) - contains narrator biographies in hadith context
   - عمدة القاري (Umdat al-Qari)

3. **Web sources (as verification):**
   - Use existing `tarajm/tarajm.py` to fetch biographies from tarajm.com
   - Cross-reference with islamweb.net narrator database

### 3.2 Resolve 127 unresolved cases

**Process:**

For each unresolved case:
1. Look up narrator in biographical dictionaries
2. Identify all possible candidates
3. Check student-teacher relationships in biography
4. Record resolution with citation

**Output format:** `manual_resolutions.json`
```json
{
  "أبيه|يعقوب": {
    "resolved_name": "إبراهيم بن سعد",
    "confidence": "high",
    "source": "Tahdhib al-Tahdhib vol. 8 p. 234 - يعقوب بن إبراهيم بن سعد",
    "reasoning": "يعقوب is son of إبراهيم بن سعد, confirmed in biography",
    "verified_by": "human",
    "verification_date": "2026-02-10"
  },
  ...
}
```

### 3.3 Verify high-confidence resolutions (sampling)

From the 33,573 currently resolved cases:
- Select stratified random sample of 100 cases
- Manually verify against biographical sources
- Calculate accuracy rate
- If accuracy < 95%, expand verification to 500 cases

**Stratification:**
- 30 سفيان resolutions (15 الثوري, 15 بن عيينة)
- 30 هشام resolutions (20 بن عروة, 5 each of others)
- 20 يحيى resolutions
- 10 عبد الله resolutions
- 10 أبيه resolutions (various students)

---

## Phase 4: Integration & Final Verification

### 4.1 Integrate manual resolutions

**Update solve_ambiguity.py:**

Add new function:
```python
def load_manual_resolutions():
    """Load manually verified resolutions from manual_resolutions.json"""
    try:
        with open('Bukhari/manual_resolutions.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

# In resolve_ambiguous function, check manual resolutions FIRST:
def resolve_ambiguous(name, student):
    context_key = f"{name}|{student}"

    # STEP 0: Manual overrides (highest priority)
    if context_key in MANUAL_RESOLUTIONS:
        return MANUAL_RESOLUTIONS[context_key]['resolved_name']

    # STEP 1: Unambiguous names
    # STEP 2: Context-dependent rules
    # etc.
```

**Resolution priority:**
```
1. MANUAL_RESOLUTIONS (100% verified)
2. HIGH_CONFIDENCE_RULES (multi-condition, exact matches)
3. MEDIUM_CONFIDENCE_RULES (single exact match in father_lookup)
4. Return None (needs review)
```

### 4.2 Generate verification report

**File to create:** `VERIFICATION_REPORT.md`

**Contents:**
```markdown
# Narrator Disambiguation Verification Report

## Summary Statistics
- Total mentions: 44,733
- Resolved with verification: 44,606 (99.7%)
- Unresolved (marked for future work): 127 (0.3%)

## Verification Methods
- Context rules (high confidence): 17,447 mentions
- Static mappings (verified): 16,126 mentions
- Manual resolutions (scholarly verified): 127 mentions
- Unresolved: 127 mentions

## Accuracy Validation
- Sample size: 100 randomly selected resolutions
- Verified correct: 98 (98%)
- Errors found: 2 (2%)
- Error cases documented in verification_errors.csv

## Scholarly Sources Used
- Tahdhib al-Tahdhib (تهذيب التهذيب)
- Siyar A'lam al-Nubala (سير أعلام النبلاء)
- Fath al-Bari commentary (فتح الباري)
- tarajm.com API (cross-reference)

## Known Limitations
- 127 cases remain unresolved (0.3%)
- 2 verified errors in sample (need correction)
- عبد الله fallback rule disabled (conservative approach)

## Audit Trail
All manual resolutions stored in manual_resolutions.json with:
- Source citations
- Reasoning
- Verification date
- Verifier name
```

### 4.3 Create test suite

**File to create:** `extract_data_v2/test_disambiguation.py`

**Test cases:**
```python
def test_sufyan_disambiguation():
    """Test سفيان resolution based on known cases from Bukhari"""
    # Test case from Fath al-Bari: حديث رقم 1
    assert resolve_ambiguous('سفيان', 'الحميدي') == 'سفيان بن عيينة'
    assert resolve_ambiguous('سفيان', 'يحيى') == 'سفيان الثوري'
    # ... 10 more verified cases

def test_hisham_disambiguation():
    """Test هشام resolution"""
    assert resolve_ambiguous('هشام', 'أبو أسامة') == 'هشام بن عروة'
    # ... etc

def test_father_pronouns():
    """Test أبيه resolution from verified genealogies"""
    assert resolve_ambiguous('أبيه', 'هشام') == 'عروة بن الزبير'
    assert resolve_ambiguous('أبيه', 'سالم') == 'عبد الله بن عمر'
    # ... etc

def test_no_hallucination():
    """Ensure unknown cases return None, not guesses"""
    assert resolve_ambiguous('عبد الله', 'unknown_student') is None
    assert resolve_ambiguous('أبيه', 'البخاري') is None  # Too ambiguous
```

Run: `pytest test_disambiguation.py -v`

---

## Phase 5: Final Output Generation

### 5.1 Regenerate all outputs with verified rules

```bash
cd extract_data_v2
python3 solve_ambiguity.py  # With STRICT_MODE and manual_resolutions
python3 narrators_mapping.py  # Final normalized output
```

### 5.2 Generate audit files

**Files to generate:**

1. `Bukhari/disambiguation_audit.csv` - Every resolution with source
   - Columns: hadith_number, narrator, resolved_name, method, confidence, source_citation

2. `Bukhari/unresolved_cases.csv` - Cases that need future work
   - Columns: hadith_number, narrator, student, reason_unresolved, suggested_resolution

3. `Bukhari/verification_sample.csv` - 100 verified cases for audit
   - Columns: narrator, student, resolved_name, verified_source, verification_date

---

## Success Criteria

✅ **MUST achieve before Neo4j ingestion:**

1. **Accuracy:** 98%+ verified on 100-case sample
2. **Coverage:** 99%+ of mentions resolved (allow 0.3% unresolved for edge cases)
3. **Audit trail:** Every resolution traceable to source (rule/manual/static)
4. **No hallucinations:** Unresolved cases marked as such, not guessed
5. **Test suite:** 50+ test cases passing from verified Bukhari examples
6. **Documentation:** VERIFICATION_REPORT.md with scholarly citations

❌ **NOT acceptable:**

1. Guessing on ambiguous cases
2. Resolutions without verification path
3. Substring-only matches without confirmation
4. Defaults that failed in unresolved data

---

## Critical Files to Modify

| File | Action | Lines | Purpose |
|------|--------|-------|---------|
| `solve_ambiguity.py` | MODIFY | 327-362 | Fix عبد الله rules |
| `solve_ambiguity.py` | MODIFY | 524-552 | Fix أبيه defaults |
| `solve_ambiguity.py` | ADD | Top | Add STRICT_MODE flag |
| `solve_ambiguity.py` | ADD | 250-260 | Add load_manual_resolutions() |
| `verify_disambiguation.py` | CREATE | - | Extract verification dataset |
| `test_disambiguation.py` | CREATE | - | Test suite with verified cases |
| `manual_resolutions.json` | CREATE | - | Manually verified resolutions with citations |
| `VERIFICATION_REPORT.md` | CREATE | - | Full audit report |

---

## Implementation Timeline

**Phase 1 (2-3 hours):** Create verification extraction script, generate dataset
**Phase 2 (1-2 hours):** Fix systematic errors in solve_ambiguity.py
**Phase 3 (6-8 hours):** Manual resolution of 127 cases with scholarly research
**Phase 4 (2-3 hours):** Integration, sampling verification (100 cases)
**Phase 5 (1 hour):** Final regeneration, reports, test suite

**Total estimated time:** 12-17 hours of careful work

---

## Verification Strategy

For **100% accuracy without hallucination**, we use:

1. **Conservative resolution:** When in doubt, mark as unresolved
2. **Multi-source verification:** Cross-check 3+ biographical sources
3. **Explicit citations:** Every manual resolution has source reference
4. **Test-driven:** Write tests from verified Bukhari cases first
5. **Stratified sampling:** Verify random sample across all rule categories
6. **Peer review:** Manual resolutions reviewed by hadith scholar (if available)

---

## Next Steps After Plan Approval

1. Create `verify_disambiguation.py` script
2. Run extraction: `python3 verify_disambiguation.py`
3. Review generated CSV, identify high-risk cases
4. Apply fixes to solve_ambiguity.py (Phase 2)
5. Begin manual resolution work (Phase 3)
6. Iterate until 98%+ accuracy achieved

---

**Note:** This is a **research-grade** approach. We prioritize accuracy over coverage. Better to have 99.7% coverage with 98%+ accuracy than 100% coverage with unknown errors.

---

## README Content (to be created after plan approval)

**File to create:** `extract_data_v2/DISAMBIGUATION_VERIFICATION_PLAN.md`

(Copy of this entire plan document for documentation purposes)

This will serve as:
1. Implementation roadmap
2. Quality assurance documentation
3. Scholarly methodology reference
4. Audit trail for verification process
