"""
TalentScope AI — OPT/CPT/H-1B Work Authorization Classifier

Production-grade, Jobright-style 3-signal approach:
  Signal 1: Job description text analysis (regex keyword matching)
  Signal 2: Employer H-1B filing history (USCIS public data)
  Signal 3: Combined score → final classification

Labels:
  opt_friendly      — strong positive evidence (text OR employer history)
  opt_unclear       — ambiguous signals, needs manual review
  not_opt_friendly  — explicit rejection in job description
  unknown           — no relevant signals found

Design rules:
  - Negative text phrases ALWAYS override everything (explicit rejection = final)
  - Employer history alone can upgrade "unknown" to "opt_friendly" (company sponsors)
  - H-1B tracked separately from OPT (different visa, different rules)
  - A form mentioning OPT/CPT is NOT automatically friendly (could be screening OUT)
  - Deterministic: no LLM, no randomness, same input = same output
  - Employer name matching is fuzzy (handles "Google LLC" vs "Google" vs "GOOGLE INC")

USCIS data source:
  https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub
  Download CSV → place at data/h1b_employers.csv
  Columns used: Employer, Initial Approvals, Initial Denials, Fiscal Year
"""

import os
import re
import csv
from typing import Optional
from functools import lru_cache

from src.utils.logger import get_logger

logger = get_logger("analysis.opt_classifier")


# ============================================================
# Signal 1: Text-based classification
# ============================================================

# NEGATIVE: explicit rejection — overrides everything
NEGATIVE_PATTERNS = [
    r"unable\s+to\s+consider\s+opt[/\s]?cpt",
    r"opt[/\s]?cpt\s+(?:is\s+)?not\s+(?:accepted|eligible|considered)",
    r"does\s+not\s+(?:provide|offer)\s+(?:any\s+)?(?:visa\s+)?sponsorship",
    r"visa\s+sponsorship\s+is\s+not\s+(?:offered|available|provided)",
    r"must\s+be\s+(?:legally\s+)?authorized\s+to\s+work\s+without\s+(?:any\s+)?(?:current\s+or\s+future\s+)?(?:visa\s+)?sponsorship",
    r"without\s+(?:the\s+)?need\s+for\s+(?:current\s+or\s+future\s+)?(?:visa\s+)?sponsorship",
    r"(?:will|does)\s+not\s+(?:provide|offer|consider)\s+(?:visa\s+)?sponsorship",
    r"no\s+(?:visa\s+)?sponsorship\s+(?:is\s+)?(?:available|offered|provided)",
    r"(?:not\s+(?:able|willing)|cannot|unable)\s+to\s+sponsor",
    r"(?:us|u\.s\.)\s+citizen(?:s|ship)?\s+(?:only|required)",
    r"must\s+(?:be|have)\s+(?:a\s+)?(?:us|u\.s\.)\s+citizen",
    r"(?:green\s+card|permanent\s+resident)\s+(?:required|only|holders?\s+only)",
    r"(?:no|not)\s+(?:work\s+)?(?:visa|authorization)\s+(?:assistance|support|sponsorship)",
]

# POSITIVE: clear OPT/CPT acceptance
POSITIVE_PATTERNS = [
    r"(?:accept|welcome|consider|encourage)s?\s+opt[/\s]?cpt\s+(?:candidates?|students?|holders?|applicants?)",
    r"opt\s+(?:and\s+)?(?:cpt\s+)?(?:students?\s+)?(?:are\s+)?(?:welcome|accepted|eligible|encouraged)",
    r"stem\s+opt\s+(?:extension|eligible|accepted|welcome)",
    r"stem\s+opt",
    r"employment\s+authorization\s+document",
    r"\bead\s+(?:card\s+)?holders?\b",
    r"(?:visa|work)\s+sponsorship\s+(?:is\s+)?(?:available|offered|provided)",
    r"(?:we|company|firm)\s+(?:will|can|do)\s+(?:provide|offer)\s+(?:visa\s+)?sponsorship",
    r"(?:open|willing)\s+to\s+sponsor(?:ing)?",
    r"sponsorship\s+(?:is\s+)?(?:available|possible|offered)\s+for",
    r"(?:all|any)\s+work\s+authorizations?\s+(?:accepted|considered|welcome)",
    r"candidates?\s+on\s+opt",
]

# AMBIGUOUS: mentions work auth but intent unclear
AMBIGUOUS_PATTERNS = [
    r"(?:work|employment)\s+(?:authorization|eligibility)\s+(?:status|required|verification|check)",
    r"(?:legally\s+)?authorized\s+to\s+work\s+in\s+the\s+(?:united\s+states|u\.?s\.?)",
    r"proof\s+of\s+(?:work\s+)?(?:authorization|eligibility)",
    r"(?:do|will)\s+you\s+(?:now\s+or\s+in\s+the\s+future\s+)?require\s+(?:visa\s+)?sponsorship",
    r"(?:immigration|visa|sponsorship)\s+(?:status|requirements?)",
    r"\bopt\b(?!\s*(?:in|out|ion|imal|imiz))",  # "opt" but not "option/optimal/optimize/opt-in/opt-out"
    r"\bcpt\b(?!\s*(?:code|u))",  # "cpt" but not "cpt code"
    r"(?:i-?9|e-?verify)\s+(?:verification|required|employer|complian)",
    r"require\s+sponsorship\s+now\s+or\s+in\s+the\s+future",
]

# H-1B specific (tracked SEPARATELY)
H1B_POSITIVE_PATTERNS = [
    r"h-?1b\s+(?:visa\s+)?(?:sponsorship\s+)?(?:available|offered|provided|possible)",
    r"(?:will|can|do)\s+sponsor\s+h-?1b",
    r"(?:open|willing)\s+to\s+(?:sponsor\s+)?h-?1b",
    r"h-?1b\s+(?:transfer|cap-exempt)",
]

H1B_NEGATIVE_PATTERNS = [
    r"(?:no|not|cannot|will\s+not|does\s+not)\s+(?:provide\s+|offer\s+)?h-?1b\s+(?:visa\s+)?sponsorship",
    r"h-?1b\s+(?:visa\s+)?(?:sponsorship\s+)?(?:is\s+)?not\s+(?:available|offered|provided)",
    r"(?:unable|not\s+able)\s+to\s+sponsor\s+h-?1b",
]


def _find_matches(text: str, patterns: list[str]) -> list[str]:
    """Return all matched phrases from patterns in text."""
    matches = []
    for pattern in patterns:
        found = re.search(pattern, text, re.IGNORECASE)
        if found:
            matches.append(found.group(0).strip())
    return matches


def classify_by_text(description: str) -> dict:
    """
    Signal 1: Pure text-based classification.

    Returns:
        {
            "text_status": "positive" | "negative" | "ambiguous" | "none",
            "text_signals": [matched phrases],
            "h1b_text_status": "positive" | "negative" | "none",
        }
    """
    if not description or not description.strip():
        return {"text_status": "none", "text_signals": [], "h1b_text_status": "none"}

    text = description.lower()
    signals = []

    neg = _find_matches(text, NEGATIVE_PATTERNS)
    pos = _find_matches(text, POSITIVE_PATTERNS)
    amb = _find_matches(text, AMBIGUOUS_PATTERNS)
    signals = neg + pos + amb

    # Priority: negative > positive > ambiguous > none
    if neg:
        text_status = "negative"
    elif pos:
        text_status = "positive"
    elif amb:
        text_status = "ambiguous"
    else:
        text_status = "none"

    # H-1B separate
    h1b_pos = _find_matches(text, H1B_POSITIVE_PATTERNS)
    h1b_neg = _find_matches(text, H1B_NEGATIVE_PATTERNS)

    if h1b_neg:
        h1b_text_status = "negative"
    elif h1b_pos:
        h1b_text_status = "positive"
    else:
        h1b_text_status = "none"

    return {
        "text_status": text_status,
        "text_signals": signals,
        "h1b_text_status": h1b_text_status,
    }


# ============================================================
# Signal 2: Employer H-1B filing history (USCIS data)
# ============================================================

H1B_DATA_PATH = os.path.join("data", "h1b_employers.csv")


def _normalize_company_name(name: str) -> str:
    """
    Normalize company names for fuzzy matching.
    'Google LLC' -> 'google'
    'AMAZON.COM INC.' -> 'amazoncom'
    'Meta Platforms, Inc' -> 'meta platforms'
    """
    if not name:
        return ""
    n = name.lower().strip()
    # Remove common suffixes
    for suffix in [
        " inc.", " inc", " llc", " ltd", " corp.", " corp",
        " co.", " co", " l.p.", " lp", " plc",
        " technologies", " technology", " solutions",
        " services", " group", " holdings",
        ",", ".", "'",
    ]:
        n = n.replace(suffix, "")
    return n.strip()


@lru_cache(maxsize=1)
def load_h1b_data() -> dict:
    """
    Load USCIS H-1B employer data from CSV into a lookup dict.
    Returns: {normalized_company_name: {"approvals": int, "denials": int, "years": set}}

    USCIS export quirks handled here:
      - File encoding is UTF-16 LE with BOM (not UTF-8)
      - Delimiter is TAB, not comma (despite .csv extension)
      - Approval types: New, Continuation, Change-Same-Employer,
        New-Concurrent, Change-of-Employer, Amended — we sum ALL of them
        for a true sponsorship volume.

    If file doesn't exist, returns empty dict (graceful degradation).
    """
    if not os.path.exists(H1B_DATA_PATH):
        logger.info(f"H-1B data file not found at {H1B_DATA_PATH} — employer signal disabled")
        return {}

    approval_cols = [
        "New Employment Approval",
        "Continuation Approval",
        "Change with Same Employer Approval",
        "New Concurrent Approval",
        "Change of Employer Approval",
        "Amended Approval",
    ]
    denial_cols = [
        "New Employment Denial",
        "Continuation Denial",
        "Change with Same Employer Denial",
        "New Concurrent Denial",
        "Change of Employer Denial",
        "Amended Denial",
    ]

    employers: dict = {}

    try:
        with open(H1B_DATA_PATH, "r", encoding="utf-16", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")

            row_count = 0
            skipped_no_name = 0
            for row in reader:
                row_count += 1
                name = _normalize_company_name(row.get("Employer (Petitioner) Name", ""))
                if not name:
                    skipped_no_name += 1
                    continue

                try:
                    total_approvals = sum(int(row.get(c, 0) or 0) for c in approval_cols)
                    total_denials = sum(int(row.get(c, 0) or 0) for c in denial_cols)
                    fiscal_year = str(row.get("Fiscal Year", "")).strip()
                except (ValueError, TypeError):
                    continue

                if name not in employers:
                    employers[name] = {"approvals": 0, "denials": 0, "years": set()}

                employers[name]["approvals"] += total_approvals
                employers[name]["denials"] += total_denials
                if fiscal_year:
                    employers[name]["years"].add(fiscal_year)

        logger.info(
            f"Loaded H-1B data: {len(employers):,} unique employers from "
            f"{row_count:,} rows ({skipped_no_name:,} rows skipped — no employer name)"
        )
    except Exception as e:
        logger.warning(f"Failed to load H-1B data: {e}")
        return {}

    return employers


def classify_by_employer(company_name: str, h1b_data: Optional[dict] = None) -> dict:
    """
    Signal 2: Employer-level H-1B sponsorship history.

    Returns:
        {
            "employer_status": "sponsor" | "unknown",
            "h1b_approvals": int,
            "h1b_denials": int,
            "h1b_years_active": int,
            "sponsor_tier": "gold" | "silver" | "none"
        }

    Tier logic (inspired by Jobright):
        gold   = 50+ total approvals across all years (heavy sponsor)
        silver = 1-49 approvals (has sponsored before)
        none   = 0 approvals or company not found
    """
    if h1b_data is None:
        h1b_data = load_h1b_data()

    if not h1b_data or not company_name:
        return {
            "employer_status": "unknown",
            "h1b_approvals": 0,
            "h1b_denials": 0,
            "h1b_years_active": 0,
            "sponsor_tier": "none",
        }

    normalized = _normalize_company_name(company_name)

    # Exact match first (fast path)
    record = h1b_data.get(normalized)

    # Fuzzy match only if exact fails AND name is meaningful (4+ chars)
    # Requires shared significant word — prevents junk substring matches
    # like "co" inside "Amazon Web Services Co"
    if not record and len(normalized) >= 4:
        our_words = {w for w in normalized.split() if len(w) >= 4}

        if our_words:
            best_match = None
            best_score = 0

            for key, val in h1b_data.items():
                # Skip if neither side is long enough
                if len(key) < 4:
                    continue

                key_words = {w for w in key.split() if len(w) >= 4}
                shared = our_words & key_words

                if not shared:
                    continue

                # Score: prefer matches where more of our words are shared
                # AND the key is not wildly longer than our input
                score = len(shared) / max(len(our_words), len(key_words))

                # Require at least 50% word overlap for a match
                if score >= 0.5 and score > best_score:
                    best_score = score
                    best_match = val

            record = best_match

    if not record:
        return {
            "employer_status": "unknown",
            "h1b_approvals": 0,
            "h1b_denials": 0,
            "h1b_years_active": 0,
            "sponsor_tier": "none",
        }

    approvals = record["approvals"]
    denials = record["denials"]
    years = len(record["years"])

    if approvals >= 50:
        tier = "gold"
    elif approvals >= 1:
        tier = "silver"
    else:
        tier = "none"

    return {
        "employer_status": "sponsor" if approvals > 0 else "unknown",
        "h1b_approvals": approvals,
        "h1b_denials": denials,
        "h1b_years_active": years,
        "sponsor_tier": tier,
    }


# ============================================================
# Signal 3: Combined classification
# ============================================================

def classify_opt(
    description: str,
    company_name: str = "",
    h1b_data: Optional[dict] = None,
) -> dict:
    """
    Combine text + employer signals into final OPT classification.

    Priority logic:
        1. Text says NEGATIVE → not_opt_friendly (always wins)
        2. Text says POSITIVE → opt_friendly
        3. Employer is known sponsor + text not negative → opt_friendly
        4. Text is AMBIGUOUS → opt_unclear
        5. Employer is sponsor but text ambiguous → opt_friendly (upgraded)
        6. Nothing found → unknown

    Returns:
        {
            "opt_status": "opt_friendly" | "opt_unclear" | "not_opt_friendly" | "unknown",
            "opt_signals": [matched phrases from text],
            "confidence": "high" | "medium" | "low",
            "h1b_sponsorship": True | False | None,
            "sponsor_tier": "gold" | "silver" | "none",
            "h1b_approvals": int,
            "text_status": str,
            "employer_status": str,
        }
    """
    text_result = classify_by_text(description)
    employer_result = classify_by_employer(company_name, h1b_data)

    text_status = text_result["text_status"]
    employer_status = employer_result["employer_status"]
    sponsor_tier = employer_result["sponsor_tier"]
    signals = text_result["text_signals"]

    # --- H-1B (separate from OPT) ---
    h1b_text = text_result["h1b_text_status"]
    if h1b_text == "negative":
        h1b_sponsorship = False
    elif h1b_text == "positive":
        h1b_sponsorship = True
    elif employer_status == "sponsor":
        h1b_sponsorship = True  # company has filed H-1B before
    else:
        h1b_sponsorship = None

    # --- OPT classification with combined signals ---

    # Rule 1: Explicit rejection in text → always wins
    if text_status == "negative":
        opt_status = "not_opt_friendly"
        confidence = "high"

    # Rule 2: Explicit positive in text
    elif text_status == "positive":
        opt_status = "opt_friendly"
        confidence = "high" if sponsor_tier in ("gold", "silver") else "medium"

    # Rule 3: No text signal but employer is a known heavy sponsor
    elif text_status == "none" and sponsor_tier == "gold":
        opt_status = "opt_friendly"
        confidence = "medium"

    # Rule 4: No text signal but employer has some sponsorship history
    elif text_status == "none" and sponsor_tier == "silver":
        opt_status = "opt_friendly"
        confidence = "low"

    # Rule 5: Ambiguous text + known sponsor → upgrade to friendly
    elif text_status == "ambiguous" and employer_status == "sponsor":
        opt_status = "opt_friendly"
        confidence = "medium"

    # Rule 6: Ambiguous text, unknown employer
    elif text_status == "ambiguous":
        opt_status = "opt_unclear"
        confidence = "low"

    # Rule 7: Nothing at all
    else:
        opt_status = "unknown"
        confidence = "low"

    return {
        "opt_status": opt_status,
        "opt_signals": signals,
        "confidence": confidence,
        "h1b_sponsorship": h1b_sponsorship,
        "sponsor_tier": sponsor_tier,
        "h1b_approvals": employer_result["h1b_approvals"],
        "text_status": text_status,
        "employer_status": employer_status,
    }


def classify_jobs_batch(
    jobs: list[dict],
    h1b_data: Optional[dict] = None,
    description_key: str = "description",
    company_key: str = "company",
) -> list[dict]:
    """
    Classify a batch of job dicts in-place.
    Adds: opt_status, opt_signals, confidence, h1b_sponsorship, sponsor_tier
    """
    if h1b_data is None:
        h1b_data = load_h1b_data()

    for job in jobs:
        result = classify_opt(
            description=job.get(description_key, ""),
            company_name=job.get(company_key, ""),
            h1b_data=h1b_data,
        )
        job["opt_status"] = result["opt_status"]
        job["opt_signals"] = result["opt_signals"]
        job["opt_confidence"] = result["confidence"]
        job["h1b_sponsorship"] = result["h1b_sponsorship"]
        job["sponsor_tier"] = result["sponsor_tier"]
        job["h1b_approvals"] = result["h1b_approvals"]

    return jobs


# ============================================================
# Self-test
# ============================================================

if __name__ == "__main__":
    print("=" * 70)
    print("OPT CLASSIFIER SELF-TEST (Jobright-style 3-signal)")
    print("=" * 70)

    # --- Test Signal 1: Text classification ---
    print("\n--- Signal 1: Text Classification ---\n")

    text_tests = [
        {
            "name": "Explicit OPT acceptance",
            "desc": "We welcome OPT and CPT students to apply. STEM OPT extensions are supported.",
            "expected": "positive",
        },
        {
            "name": "Explicit rejection",
            "desc": "Must be authorized to work without current or future sponsorship. Visa sponsorship is not offered.",
            "expected": "negative",
        },
        {
            "name": "Negative overrides positive",
            "desc": "We welcome OPT candidates. However, visa sponsorship is not offered for this role.",
            "expected": "negative",
        },
        {
            "name": "Ambiguous — screening form",
            "desc": "Do you now or in the future require visa sponsorship? I-9 verification required.",
            "expected": "ambiguous",
        },
        {
            "name": "No signals",
            "desc": "Build ETL pipelines with Python and SQL. 5 years experience required.",
            "expected": "none",
        },
        {
            "name": "US citizen only",
            "desc": "This position requires US citizenship only due to government contract requirements.",
            "expected": "negative",
        },
        {
            "name": "All authorizations welcome",
            "desc": "All work authorizations accepted. We are an equal opportunity employer.",
            "expected": "positive",
        },
        {
            "name": "EAD holders",
            "desc": "EAD card holders are encouraged to apply for this position.",
            "expected": "positive",
        },
        {
            "name": "OPT in context (not option/optimize)",
            "desc": "We accept candidates on OPT status. Great option for recent graduates.",
            "expected": "positive",
        },
        {
            "name": "Green card required",
            "desc": "Permanent resident required. Must hold green card or US citizenship.",
            "expected": "negative",
        },
    ]

    text_passed = 0
    for tc in text_tests:
        result = classify_by_text(tc["desc"])
        ok = result["text_status"] == tc["expected"]
        text_passed += ok
        marker = "OK" if ok else "FAIL"
        print(f"  [{marker}] {tc['name']}")
        if not ok:
            print(f"         got={result['text_status']}  expected={tc['expected']}")
            print(f"         signals={result['text_signals']}")

    print(f"\n  Text tests: {text_passed}/{len(text_tests)} passed")

    # --- Test Signal 2: Employer lookup ---
    print("\n--- Signal 2: Employer Lookup ---\n")

    h1b_data = load_h1b_data()
    if h1b_data:
        # Test with known large sponsors
        for company in ["Google", "Amazon", "Meta", "Apple", "Microsoft", "Airbnb", "Stripe"]:
            result = classify_by_employer(company, h1b_data)
            print(f"  {company:15s} -> tier={result['sponsor_tier']:6s} "
                  f"approvals={result['h1b_approvals']:>6,}  "
                  f"years={result['h1b_years_active']}")
    else:
        print("  SKIPPED — H-1B data file not found at data/h1b_employers.csv")
        print("  Download from: https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub")
        print("  Place CSV at: data/h1b_employers.csv")

    # --- Test Signal 3: Combined classification ---
    print("\n--- Signal 3: Combined Classification ---\n")

    combined_tests = [
        {
            "name": "Text positive + known sponsor",
            "desc": "OPT students welcome. Visa sponsorship available.",
            "company": "Google",
            "expected_status": "opt_friendly",
            "expected_confidence": "high" if h1b_data else "medium",
        },
        {
            "name": "Text negative (always wins)",
            "desc": "Must be authorized to work without sponsorship.",
            "company": "Google",
            "expected_status": "not_opt_friendly",
            "expected_confidence": "high",
        },
        {
            "name": "No text + gold sponsor → friendly",
            "desc": "Build data pipelines with Python.",
            "company": "Google",
            "expected_status": "opt_friendly" if h1b_data else "unknown",
            "expected_confidence": "medium" if h1b_data else "low",
        },
        {
            "name": "Ambiguous text + known sponsor → upgraded",
            "desc": "Work authorization verification required.",
            "company": "Amazon",
            "expected_status": "opt_friendly" if h1b_data else "opt_unclear",
            "expected_confidence": "medium" if h1b_data else "low",
        },
        {
            "name": "No text + truly unknown company",
            "desc": "Join our team of engineers.",
            "company": "Zxqwpmlkjhbvcxzpoiuyt Fictional Co",
            "expected_status": "unknown",
            "expected_confidence": "low",
        },
    ]

    combined_passed = 0
    for tc in combined_tests:
        result = classify_opt(tc["desc"], tc["company"], h1b_data or None)
        status_ok = result["opt_status"] == tc["expected_status"]
        conf_ok = result["confidence"] == tc["expected_confidence"]
        ok = status_ok and conf_ok
        combined_passed += ok
        marker = "OK" if ok else "FAIL"
        print(f"  [{marker}] {tc['name']}")
        print(f"         status={result['opt_status']:20s} (expected: {tc['expected_status']})")
        print(f"         confidence={result['confidence']:10s} (expected: {tc['expected_confidence']})")
        print(f"         tier={result['sponsor_tier']}  h1b_approvals={result['h1b_approvals']}")

    print(f"\n  Combined tests: {combined_passed}/{len(combined_tests)} passed")

    # --- Batch test ---
    print("\n--- Batch Classifier ---\n")

    batch_jobs = [
        {"title": "Data Engineer", "company": "Airbnb",
         "description": "OPT students welcome. STEM OPT supported."},
        {"title": "ML Engineer", "company": "Zxqwpmlkjhbvcxzpoiuyt Fictional Co",
         "description": "Must be authorized to work without sponsorship."},
        {"title": "Analyst", "company": "Google",
         "description": "Build dashboards in Python."},
    ]
    classified = classify_jobs_batch(batch_jobs, h1b_data or None)
    for j in classified:
        print(f"  {j['title']:20s} @ {j['company']:15s} -> "
              f"{j['opt_status']:20s} conf={j['opt_confidence']:6s} "
              f"tier={j['sponsor_tier']:6s} h1b_approvals={j['h1b_approvals']}")

    total = text_passed + combined_passed
    total_tests = len(text_tests) + len(combined_tests)
    print(f"\n{'=' * 70}")
    print(f"TOTAL: {total}/{total_tests} tests passed")
    if total == total_tests:
        print("All assertions passed.")
    else:
        print("SOME TESTS FAILED — review output above.")