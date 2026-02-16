# Narrator Disambiguation Verification Plan

## 🎯 Goal

Achieve **98%+ verified accuracy** with **99.7% coverage** for narrator name disambiguation before Neo4j ingestion.

**Principle:** Better to have 99.7% coverage with 98%+ accuracy than 100% coverage with unknown errors.

---

## 📊 Current Status (Baseline)

| Metric | Value | Notes |
|--------|-------|-------|
| **Total narrator mentions** | 44,733 | All narrator instances in Bukhari |
| **Coverage** | 75.1% | 33,573 resolved / 44,733 total |
| **Context resolution** | 97.0% | 1,509 / 1,555 context pairs |
| **Pronoun resolution (أبيه/أبي)** | 90.7% | 751 / 828 cases |
| **Unique narrators** | 2,585 | After normalization |

### ⚠️ Critical Issues Identified

1. **Unknown accuracy** - 97% resolution but no verification against scholarly sources
2. **Over-aggressive substring matching** - `if 'نافع' in student` matches unintended cases
3. **Broken البخاري fallback** - `عبد الله|البخاري → ابن المبارك` is historically incorrect
4. **Incomplete father_lookup** - Missing ~30% of father-son pairs
5. **No validation mechanism** - Zero test suite, no cross-reference

---

## 🔬 Verification Strategy

### Phase 1: Extract Verification Dataset (2-3 hours)

**Create:** `verify_disambiguation.py`

**Purpose:** Extract all resolutions with metadata for manual review

**Output:** `verification_dataset.csv` with columns:
- `hadith_number` - Hadith reference
- `chain_index` - Chain position
- `narrator_position` - Position in chain
- `original_name` - Raw name from JSON
- `resolved_name` - After disambiguation
- `student_name` - Context (previous narrator)
- `resolution_method` - "context_json" / "live_rule" / "static_mapping" / "identity"
- `confidence` - "high" / "medium" / "low"
- `needs_review` - Boolean flag

**Confidence levels:**
- **HIGH:** Exact match in context JSON, unambiguous names (عائشة, مالك, أنس)
- **MEDIUM:** Live rule with multiple conditions (سفيان with 3+ student checks)
- **LOW:** Single substring match, defaults (e.g., `student == 'سعيد'`)

**Auto-flag for review:**
- عبد الله resolutions with substring-only matches
- أبيه resolutions using short name defaults
- Any resolution where student = البخاري
- Unresolved cases (77 pronouns + 50 عبد الله)

---

### Phase 2: Fix Systematic Errors (1-2 hours)

#### 2.1 Fix عبد الله rules (HIGHEST PRIORITY)

**File:** `solve_ambiguity.py` lines 327-362

**Fix 1: Remove broken البخاري fallback**
```python
# BEFORE:
if 'البخاري' in student:
    return 'عبد الله بن المبارك'  # ❌ WRONG

# AFTER:
if 'البخاري' in student:
    return None  # ✓ Mark for manual review
```

**Fix 2: Add specificity to substring matches**
```python
# BEFORE:
if any(s in student for s in ['نافع', 'سالم', 'حمزة', ...]):
    return 'عبد الله بن عمر'

# AFTER:
if student in ['نافع', 'سالم', 'حمزة']:  # Exact match first
    return 'عبد الله بن عمر'
elif any(s in student for s in ['نافع مولى', 'سالم بن', 'حمزة بن']):
    return 'عبد الله بن عمر'
# Otherwise return None
```

**Fix 3: Re-order by statistical frequency**
1. عبد الله بن عمر (Sahabi, most common)
2. عبد الله بن مسعود (Sahabi, very common)
3. عبد الله بن عباس (Sahabi, common)
4. عبد الله بن المبارك (Tabi'i, less common)

#### 2.2 Fix أبيه defaults

**File:** `solve_ambiguity.py` lines 524-552

**Remove over-aggressive defaults:**
```python
# BEFORE:
if student == 'سعيد':
    return 'المسيب بن حزن'  # Default assumption

# AFTER:
if student == 'سعيد':
    return None  # ✓ Conservative approach
```

**Rationale:** Unresolved data shows 11 cases of `أبيه|سعيد` failed → defaults are unreliable

#### 2.3 Add STRICT_MODE flag

```python
# At top of solve_ambiguity.py
STRICT_MODE = True

# When enabled:
# - All substring-only matches return None
# - All defaults return None
# - Only exact matches in father_lookup or multi-condition rules pass
```

---

### Phase 3: Manual Resolution with Scholarly Sources (6-8 hours)

#### 3.1 Reference Sources

**Primary sources:**
1. **تهذيب التهذيب** (Tahdhib al-Tahdhib) - Standard biographical dictionary
2. **سير أعلام النبلاء** (Siyar A'lam al-Nubala) - Biographical encyclopedia
3. **تقريب التهذيب** (Taqrib al-Tahdhib) - Concise biographies
4. **فتح الباري** (Fath al-Bari) - Bukhari commentary with narrator notes
5. **عمدة القاري** (Umdat al-Qari) - Alternative Bukhari commentary

**Web sources (verification only):**
- `tarajm.com` (via existing `tarajm/tarajm.py`)
- `islamweb.net` narrator database

#### 3.2 Resolve 127 unresolved cases

**Process:**
1. Look up narrator in biographical dictionaries
2. Identify all possible candidates
3. Check student-teacher relationships
4. Record resolution with **citation**

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
  }
}
```

#### 3.3 Verify high-confidence resolutions (sampling)

**Sample size:** 100 cases (stratified random)

**Stratification:**
- 30 سفيان resolutions (15 الثوري, 15 بن عيينة)
- 30 هشام resolutions (20 بن عروة, 5 each of others)
- 20 يحيى resolutions
- 10 عبد الله resolutions
- 10 أبيه resolutions

**Success criteria:** ≥95% accuracy → If <95%, expand to 500 cases

---

### Phase 4: Integration & Final Verification (2-3 hours)

#### 4.1 Integrate manual resolutions

**Update solve_ambiguity.py:**

```python
# Load manual overrides
MANUAL_RESOLUTIONS = load_manual_resolutions()

# Resolution priority:
def resolve_ambiguous(name, student):
    context_key = f"{name}|{student}"

    # STEP 0: Manual overrides (100% verified)
    if context_key in MANUAL_RESOLUTIONS:
        return MANUAL_RESOLUTIONS[context_key]['resolved_name']

    # STEP 1: High-confidence rules (multi-condition, exact matches)
    # STEP 2: Medium-confidence rules (single exact match)
    # STEP 3: Return None (needs review)
```

#### 4.2 Generate verification report

**File:** `VERIFICATION_REPORT.md`

**Contents:**
- Summary statistics
- Verification methods breakdown
- Accuracy validation (100-case sample results)
- Scholarly sources used
- Known limitations
- Audit trail with citations

#### 4.3 Create test suite

**File:** `test_disambiguation.py`

```python
def test_sufyan_disambiguation():
    """Test سفيان resolution based on verified Bukhari cases"""
    assert resolve_ambiguous('سفيان', 'الحميدي') == 'سفيان بن عيينة'
    assert resolve_ambiguous('سفيان', 'يحيى') == 'سفيان الثوري'

def test_no_hallucination():
    """Ensure unknown cases return None, not guesses"""
    assert resolve_ambiguous('عبد الله', 'unknown_student') is None
    assert resolve_ambiguous('أبيه', 'البخاري') is None
```

**Run:** `pytest test_disambiguation.py -v`

---

### Phase 5: Final Output Generation (1 hour)

#### 5.1 Regenerate all outputs

```bash
cd extract_data_v2
python3 solve_ambiguity.py  # With STRICT_MODE + manual_resolutions
python3 narrators_mapping.py  # Final normalized output
```

#### 5.2 Generate audit files

1. **`disambiguation_audit.csv`** - Every resolution with source
2. **`unresolved_cases.csv`** - Cases needing future work
3. **`verification_sample.csv`** - 100 verified cases for audit

---

## ✅ Success Criteria (MUST achieve before Neo4j ingestion)

1. **Accuracy:** 98%+ verified on 100-case sample
2. **Coverage:** 99%+ of mentions resolved (allow 0.3% unresolved for edge cases)
3. **Audit trail:** Every resolution traceable to source (rule/manual/static)
4. **No hallucinations:** Unresolved cases marked as such, not guessed
5. **Test suite:** 50+ test cases passing from verified Bukhari examples
6. **Documentation:** VERIFICATION_REPORT.md with scholarly citations

---

## ❌ NOT Acceptable

1. Guessing on ambiguous cases
2. Resolutions without verification path
3. Substring-only matches without confirmation
4. Defaults that failed in unresolved data

---

## 📁 Critical Files to Modify

| File | Action | Lines | Purpose |
|------|--------|-------|---------|
| `solve_ambiguity.py` | MODIFY | 327-362 | Fix عبد الله rules |
| `solve_ambiguity.py` | MODIFY | 524-552 | Fix أبيه defaults |
| `solve_ambiguity.py` | ADD | Top | Add STRICT_MODE flag |
| `solve_ambiguity.py` | ADD | 250-260 | Add load_manual_resolutions() |
| `verify_disambiguation.py` | CREATE | - | Extract verification dataset |
| `test_disambiguation.py` | CREATE | - | Test suite with verified cases |
| `manual_resolutions.json` | CREATE | - | Manually verified resolutions |
| `VERIFICATION_REPORT.md` | CREATE | - | Full audit report |

---

## ⏱️ Implementation Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| **Phase 1** | 2-3 hours | Create verification extraction script, generate dataset |
| **Phase 2** | 1-2 hours | Fix systematic errors in solve_ambiguity.py |
| **Phase 3** | 6-8 hours | Manual resolution of 127 cases with scholarly research |
| **Phase 4** | 2-3 hours | Integration, sampling verification (100 cases) |
| **Phase 5** | 1 hour | Final regeneration, reports, test suite |
| **TOTAL** | **12-17 hours** | Careful, research-grade work |

---

## 🔍 Quality Assurance Methodology

For **100% accuracy without hallucination**, we use:

1. **Conservative resolution:** When in doubt, mark as unresolved
2. **Multi-source verification:** Cross-check 3+ biographical sources
3. **Explicit citations:** Every manual resolution has source reference
4. **Test-driven:** Write tests from verified Bukhari cases first
5. **Stratified sampling:** Verify random sample across all rule categories
6. **Peer review:** Manual resolutions reviewed by hadith scholar (if available)

---

## 📚 Next Steps After Verification

1. Complete all 5 phases above
2. Achieve 98%+ accuracy on verification sample
3. Generate VERIFICATION_REPORT.md
4. **Only then** proceed to Neo4j ingestion
5. Use verified data for graph analysis

---

## 📖 Related Documentation

- [`DISAMBIGUATION_RESULTS.md`](DISAMBIGUATION_RESULTS.md) - Current results with Arabic explanations
- [`solve_ambiguity.py`](solve_ambiguity.py) - Disambiguation rules engine
- [`narrators_mapping.py`](narrators_mapping.py) - Main normalization pipeline
- [`tarajm/tarajm.py`](../tarajm/tarajm.py) - Biography fetching tool

---

**Generated:** 2026-02-10
**Status:** Plan approved, ready for implementation
**Approach:** Research-grade verification prioritizing accuracy over coverage
