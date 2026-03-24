"""
TalentScope AI — Data Normalizer
Standardizes salary, location, and experience from raw job data.
"""

import re
import hashlib
from src.utils.logger import get_logger

logger = get_logger("pipeline.normalize")


def normalize_salary(raw_salary: str) -> dict:
    """
    Convert messy salary strings to annual USD range.
    
    Handles: $80K, $80,000, $38/hr, $80000-$120000, 
             $60.0-$70.0 HOURLY, $134500.0-$219500.0 YEARLY
    
    Returns: {"min": float|None, "max": float|None}
    """
    if not raw_salary or raw_salary.strip() == "":
        return {"min": None, "max": None}

    salary = raw_salary.upper().replace(",", "").replace("$", "").strip()

    # Detect pay period
    is_hourly = any(kw in salary for kw in ["HOUR", "HR", "/HR", "PER HOUR"])
    is_monthly = "MONTH" in salary
    is_yearly = any(kw in salary for kw in ["YEAR", "ANNUAL", "YR", "YEARLY"])

    # Remove period keywords
    for kw in ["HOURLY", "HOUR", "HR", "MONTHLY", "MONTH", "YEARLY", "YEAR", 
               "ANNUAL", "YR", "PER", "/", "USD", "UP TO", "+"]:
        salary = salary.replace(kw, "")
    salary = salary.strip()

    # Extract numbers
    numbers = re.findall(r'[\d.]+', salary)
    numbers = [float(n) for n in numbers if float(n) > 0]

    if not numbers:
        return {"min": None, "max": None}

    # Handle K notation (e.g., 80K = 80000)
    numbers = [n * 1000 if n < 1000 and "K" in raw_salary.upper() else n for n in numbers]

    # Convert to annual
    def to_annual(val):
        if is_hourly or (val < 500):
            return round(val * 2080)  # 40hrs * 52 weeks
        elif is_monthly or (500 <= val < 5000):
            return round(val * 12)
        else:
            return round(val)

    numbers = [to_annual(n) for n in numbers]

    sal_min = min(numbers) if numbers else None
    sal_max = max(numbers) if len(numbers) > 1 else sal_min

    # Sanity check: reject unrealistic salaries
    if sal_min and (sal_min < 15000 or sal_min > 1000000):
        return {"min": None, "max": None}
    if sal_max and (sal_max < 15000 or sal_max > 1000000):
        sal_max = sal_min

    return {"min": sal_min, "max": sal_max}


def normalize_location(raw_location: str) -> dict:
    """
    Parse location into city, state, country, and remote flag.
    
    Returns: {"city": str, "state": str, "country": str, "is_remote": bool}
    """
    result = {"city": None, "state": None, "country": "US", "is_remote": False}

    if not raw_location or raw_location.strip() == "":
        return result

    location = raw_location.strip()

    # Check remote
    remote_keywords = ["remote", "anywhere", "work from home", "wfh"]
    if any(kw in location.lower() for kw in remote_keywords):
        result["is_remote"] = True

    # Clean up common patterns
    location = re.sub(r'\(.*?\)', '', location).strip()

    # US State abbreviations
    STATE_MAP = {
        "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
        "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
        "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
        "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
        "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
        "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
        "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
        "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
        "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
        "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
        "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
        "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
        "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    }
    REVERSE_STATE = {v.lower(): k for k, v in STATE_MAP.items()}

    parts = [p.strip() for p in location.split(",")]

    if len(parts) >= 2:
        result["city"] = parts[0]
        state_part = parts[1].strip()

        # Check if it's an abbreviation
        if state_part.upper() in STATE_MAP:
            result["state"] = state_part.upper()
        elif state_part.lower() in REVERSE_STATE:
            result["state"] = REVERSE_STATE[state_part.lower()]
        else:
            result["state"] = state_part

        if len(parts) >= 3:
            country = parts[-1].strip()
            if country.lower() in ["us", "usa", "united states"]:
                result["country"] = "US"
            else:
                result["country"] = country
    elif len(parts) == 1:
        single = parts[0].strip()
        if single.upper() in STATE_MAP:
            result["state"] = single.upper()
        elif single.lower() in REVERSE_STATE:
            result["state"] = REVERSE_STATE[single.lower()]
        elif single.lower() in ["united states", "us", "usa"]:
            result["country"] = "US"
        else:
            result["city"] = single

    return result


def extract_experience_level(title: str, description: str = "") -> str:
    """
    Extract experience level from job title and description.
    
    Returns: 'junior', 'mid', 'senior', 'lead', or 'unknown'
    """
    text = f"{title} {description}".lower()

    if any(kw in text for kw in ["director", "vp ", "vice president", "head of", "chief"]):
        return "lead"
    if any(kw in text for kw in ["senior", "sr.", "sr ", "staff", "principal"]):
        return "senior"
    if any(kw in text for kw in ["lead", "manager", "team lead"]):
        return "lead"
    if any(kw in text for kw in ["junior", "jr.", "jr ", "entry", "associate", "intern"]):
        return "junior"
    if any(kw in text for kw in ["mid-level", "mid level", "intermediate", " ii", " iii"]):
        return "mid"

    return "mid"  # Default to mid if unclear


def generate_dedup_hash(title: str, company: str, location: str) -> str:
    """Generate SHA-256 hash for deduplication."""
    raw = f"{title}|{company}|{location}".lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()


if __name__ == "__main__":
    # Test salary normalization
    test_salaries = [
        "$80K", "$80,000", "$38/hr", "$80000-$120000",
        "$60.0-$70.0 HOURLY", "$134500.0-$219500.0 YEARLY",
        "$175000.0-$220000.0 YEARLY", "", "Up to $150000",
    ]

    print("--- Salary Normalization Tests ---")
    for s in test_salaries:
        result = normalize_salary(s)
        print(f"  {s:40s} -> min={result['min']}, max={result['max']}")

    # Test location normalization
    test_locations = [
        "New York, NY", "San Francisco, CA, US",
        "Remote", "Austin, Texas", "United States",
        "Columbus, Ohio Metropolitan Area",
    ]

    print("\n--- Location Normalization Tests ---")
    for loc in test_locations:
        result = normalize_location(loc)
        print(f"  {loc:40s} -> {result}")

    # Test experience extraction
    test_titles = [
        "Senior Data Engineer", "Junior Data Analyst",
        "Machine Learning Engineer", "Director of Analytics",
        "Data Scientist II",
    ]

    print("\n--- Experience Level Tests ---")
    for t in test_titles:
        print(f"  {t:40s} -> {extract_experience_level(t)}")