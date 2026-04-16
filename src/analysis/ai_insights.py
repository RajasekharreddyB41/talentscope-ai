"""
TalentScope AI — Groq-powered natural language insights
Reusable functions that turn numbers into one-liner insights for the UI.

Design rules:
- Every function takes REAL DATA (numbers, not just topics) as input
- Every function has a deterministic fallback if Groq is unavailable
- Outputs are 1-2 sentences, glanceable, not essays
- Results are cached in-process to save API calls
"""

import os
import hashlib
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv

from src.utils.logger import get_logger

# Load .env from project root
load_dotenv()

try:
    from groq import Groq
    HAS_GROQ = True
except ImportError:
    HAS_GROQ = False
    logger.warning("groq package not installed — AI insights will use fallbacks")


MODEL = "llama-3.1-8b-instant"  # Fast + free tier friendly
MAX_TOKENS = 120  # Keep outputs short


def _get_client() -> Optional["Groq"]:
    """Return Groq client if available, else None."""
    if not HAS_GROQ:
        return None
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.warning("GROQ_API_KEY not set in environment")
        return None
    try:
        return Groq(api_key=api_key)
    except Exception as e:
        logger.warning(f"Failed to init Groq client: {e}")
        return None


def _cache_key(*args) -> str:
    """Build a stable cache key from function args."""
    return hashlib.md5(str(args).encode()).hexdigest()


@lru_cache(maxsize=256)
def _call_groq(prompt: str, system: str = "You are a concise job market analyst.") -> Optional[str]:
    """
    Low-level Groq call with caching. Returns None on any failure.
    Caller is responsible for fallbacks.
    """
    client = _get_client()
    if client is None:
        return None

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt},
            ],
            max_tokens=MAX_TOKENS,
            temperature=0.3,  # Low temp = consistent, data-grounded outputs
        )
        text = response.choices[0].message.content.strip()
        # Strip surrounding quotes that LLMs sometimes add
        text = text.strip('"\'')
        return text if text else None
    except Exception as e:
        logger.warning(f"Groq call failed: {e}")
        return None


# ============================================================
# Public insight functions
# ============================================================

def generate_market_insight(
    role: str,
    top_skill: str,
    skill_growth_pct: float,
    city: Optional[str] = None,
) -> str:
    """
    One-liner about a skill trend in a role/city.

    Args:
        role: e.g. "Data Engineer"
        top_skill: e.g. "Python"
        skill_growth_pct: e.g. 18.3 means +18.3% week-over-week
        city: optional, e.g. "Boston"

    Returns:
        e.g. "Python demand for Data Engineers is up 18% this week,
              signaling strong hiring momentum in Boston."
    """
    location_phrase = f" in {city}" if city else ""
    direction = "up" if skill_growth_pct >= 0 else "down"
    abs_pct = abs(skill_growth_pct)

    prompt = (
        f"Write ONE sentence (max 25 words) about this job market signal:\n"
        f"- Role: {role}\n"
        f"- Top skill: {top_skill}\n"
        f"- Week-over-week demand: {direction} {abs_pct:.1f}%{location_phrase}\n"
        f"Tone: analyst, plainspoken. No emoji. No quotes. No hedging words like 'might' or 'could'."
    )

    result = _call_groq(prompt)
    if result:
        return result

    # Fallback — deterministic, still useful
    return (
        f"{top_skill} demand for {role} roles is {direction} "
        f"{abs_pct:.1f}% this week{location_phrase}."
    )


def generate_salary_insight(
    role: str,
    predicted_mid: int,
    predicted_min: int,
    predicted_max: int,
    experience: str,
    location: str,
) -> str:
    """
    Narrative wrapper around a salary prediction.

    Returns 1-2 sentences contextualizing the number.
    """
    spread = predicted_max - predicted_min
    prompt = (
        f"Write 1-2 sentences (max 40 words) explaining this salary prediction:\n"
        f"- Role: {role} ({experience} level) in {location}\n"
        f"- Predicted midpoint: ${predicted_mid:,}\n"
        f"- 90% confidence range: ${predicted_min:,} to ${predicted_max:,}\n"
        f"- Range width: ${spread:,}\n"
        f"Focus on what the range width says about salary variance in this market segment. "
        f"Tone: analyst. No emoji. No quotes. Don't restate the numbers verbatim."
    )

    result = _call_groq(prompt)
    if result:
        return result

    # Fallback
    if spread > 80000:
        variance_note = "the wide range reflects high variance across employers"
    elif spread > 40000:
        variance_note = "the range is typical for this role and level"
    else:
        variance_note = "the narrow range suggests consistent market pricing"
    return (
        f"A {experience} {role} in {location} typically earns around "
        f"${predicted_mid:,}; {variance_note}."
    )


def generate_career_tip(
    missing_skills: list,
    user_skill_count: int,
    target_role: str,
) -> str:
    """
    Short, actionable career advice based on skill gap.
    """
    if not missing_skills:
        return f"You cover the core skills for {target_role} roles — focus on depth and portfolio projects."

    top_gaps = ", ".join(missing_skills[:3])
    prompt = (
        f"Write ONE actionable sentence (max 30 words) for a candidate:\n"
        f"- Target role: {target_role}\n"
        f"- They already have: {user_skill_count} skills\n"
        f"- Top missing skills: {top_gaps}\n"
        f"Advise which ONE skill to learn first and why. "
        f"Tone: mentor, direct. No emoji. No quotes. No 'consider' or 'might want to'."
    )

    result = _call_groq(prompt)
    if result:
        return result

    # Fallback
    return (
        f"Start with {missing_skills[0]} — it's the highest-impact gap "
        f"for {target_role} roles based on current market demand."
    )


# ============================================================
# Self-test
# ============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("AI INSIGHTS SELF-TEST")
    print("=" * 60)

    print("\n--- Market insight ---")
    print(generate_market_insight(
        role="Data Engineer",
        top_skill="Python",
        skill_growth_pct=18.3,
        city="Boston",
    ))

    print("\n--- Salary insight ---")
    print(generate_salary_insight(
        role="Data Scientist",
        predicted_mid=205000,
        predicted_min=131000,
        predicted_max=262000,
        experience="senior",
        location="San Francisco",
    ))

    print("\n--- Career tip ---")
    print(generate_career_tip(
        missing_skills=["AWS", "Docker", "Kubernetes"],
        user_skill_count=7,
        target_role="ML Engineer",
    ))

    print("\n" + "=" * 60)
    print("Groq available:" , HAS_GROQ and _get_client() is not None)