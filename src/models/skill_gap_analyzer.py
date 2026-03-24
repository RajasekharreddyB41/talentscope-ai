"""
TalentScope AI — Skill Gap Analyzer
Compares user skills against market demands using NLP + LLM.
"""

import pandas as pd
import numpy as np
from collections import Counter
from sqlalchemy import text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from src.database.connection import get_engine
from src.utils.config import GROQ_API_KEY
from src.utils.logger import get_logger

logger = get_logger("models.skill_gap")


def get_market_skills() -> dict:
    """Get skill demand data from job_features table."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT skill, COUNT(*) as demand
            FROM job_features, UNNEST(skills) AS skill
            GROUP BY skill
            ORDER BY demand DESC
        """))
        skills = {row[0]: row[1] for row in result}

    logger.info(f"Loaded {len(skills)} market skills")
    return skills


def get_skills_by_role(role: str) -> dict:
    """Get skills demanded for a specific role category."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT skill, COUNT(*) as demand
            FROM job_features, UNNEST(skills) AS skill
            WHERE title_category = :role
            GROUP BY skill
            ORDER BY demand DESC
        """), {"role": role})
        skills = {row[0]: row[1] for row in result}

    return skills


def analyze_skill_gap(user_skills: list, target_role: str = None) -> dict:
    """
    Analyze gap between user skills and market demands.

    Args:
        user_skills: List of user's current skills
        target_role: Optional target role to focus analysis

    Returns:
        Dict with matched skills, missing skills, and recommendations
    """
    # Normalize user skills
    user_skills_lower = [s.lower().strip() for s in user_skills]

    # Get market data
    if target_role:
        market_skills = get_skills_by_role(target_role)
    else:
        market_skills = get_market_skills()

    if not market_skills:
        logger.warning("No market skills data found")
        return {"error": "No market data available"}

    total_jobs = sum(market_skills.values())

    # Categorize skills
    matched = {}
    missing = {}

    for skill, demand in market_skills.items():
        pct = round(demand / total_jobs * 100, 1)
        if skill in user_skills_lower:
            matched[skill] = {"demand": demand, "pct": pct}
        else:
            missing[skill] = {"demand": demand, "pct": pct}

    # Sort missing by demand (highest first)
    missing = dict(sorted(missing.items(), key=lambda x: x[1]["demand"], reverse=True))

    # Coverage score
    matched_demand = sum(v["demand"] for v in matched.values())
    total_demand = sum(market_skills.values())
    coverage_score = round(matched_demand / total_demand * 100, 1) if total_demand > 0 else 0

    # Top recommendations (missing skills with highest demand)
    top_recommendations = list(missing.keys())[:10]

    result = {
        "coverage_score": coverage_score,
        "matched_count": len(matched),
        "missing_count": len(missing),
        "total_market_skills": len(market_skills),
        "matched_skills": matched,
        "missing_skills": missing,
        "top_recommendations": top_recommendations,
        "target_role": target_role or "All Tech Roles",
    }

    logger.info(
        f"Skill gap analysis: coverage={coverage_score}% | "
        f"matched={len(matched)} | missing={len(missing)}"
    )

    return result


def tfidf_similarity(user_text: str, n_matches: int = 5) -> list:
    """
    Use TF-IDF + cosine similarity to find most similar jobs
    to a user's resume/skill description.

    Args:
        user_text: User's resume text or skill description
        n_matches: Number of top matches to return

    Returns:
        List of matching jobs with similarity scores
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT c.id, c.title, c.company, c.location_city,
                   c.salary_min, c.salary_max, c.experience_level,
                   c.description
            FROM clean_jobs c
            WHERE c.description IS NOT NULL AND c.description != ''
            LIMIT 500
        """), conn)

    if df.empty:
        return []

    # Combine user text with job descriptions for TF-IDF
    all_texts = [user_text] + df["description"].tolist()

    # Build TF-IDF matrix
    tfidf = TfidfVectorizer(
        max_features=5000,
        stop_words="english",
        ngram_range=(1, 2),
    )
    tfidf_matrix = tfidf.fit_transform(all_texts)

    # Cosine similarity between user (index 0) and all jobs
    similarities = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:]).flatten()

    # Top N matches
    top_indices = similarities.argsort()[-n_matches:][::-1]

    matches = []
    for idx in top_indices:
        job = df.iloc[idx]
        matches.append({
            "title": job["title"],
            "company": job["company"],
            "location": job["location_city"],
            "salary_min": job["salary_min"],
            "salary_max": job["salary_max"],
            "experience": job["experience_level"],
            "similarity": round(float(similarities[idx]) * 100, 1),
        })

    logger.info(f"TF-IDF matching: found {len(matches)} matches (top score: {matches[0]['similarity']}%)")
    return matches


def get_llm_recommendations(skill_gap_result: dict) -> str:
    """
    Use Groq LLM to generate personalized skill recommendations.

    Args:
        skill_gap_result: Output from analyze_skill_gap()

    Returns:
        Natural language recommendations string
    """
    if not GROQ_API_KEY:
        return "LLM recommendations unavailable — add GROQ_API_KEY to .env"

    try:
        from groq import Groq

        client = Groq(api_key=GROQ_API_KEY)

        matched = list(skill_gap_result["matched_skills"].keys())
        missing_top = skill_gap_result["top_recommendations"][:8]
        coverage = skill_gap_result["coverage_score"]
        role = skill_gap_result["target_role"]

        prompt = f"""You are a career advisor for tech professionals.

A user targeting "{role}" roles has these skills: {', '.join(matched)}
Their market coverage score is {coverage}%.

They are missing these high-demand skills: {', '.join(missing_top)}

Give a brief, actionable recommendation (3-4 paragraphs max):
1. What they should learn FIRST (highest impact)
2. A realistic learning path (specific courses, projects, timeline)
3. How their existing skills connect to the missing ones

Be specific and practical. No fluff."""

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=500,
        )

        recommendation = response.choices[0].message.content
        logger.info("LLM recommendations generated successfully")
        return recommendation

    except Exception as e:
        logger.error(f"LLM recommendation failed: {e}")
        return f"LLM recommendation unavailable: {str(e)}"


def get_available_roles() -> list:
    """Get list of role categories available in the database."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT title_category, COUNT(*) as cnt
            FROM job_features
            GROUP BY title_category
            ORDER BY cnt DESC
        """))
        return [row[0] for row in result]


if __name__ == "__main__":
    print("=" * 60)
    print("TALENTSCOPE AI — SKILL GAP ANALYZER")
    print("=" * 60)

    # Test with sample user skills
    sample_skills = ["python", "sql", "pandas", "excel", "git", "tableau"]
    target = "Data Analyst"

    print(f"\nUser skills: {sample_skills}")
    print(f"Target role: {target}")
    print("-" * 40)

    # Skill gap analysis
    result = analyze_skill_gap(sample_skills, target_role=target)

    print(f"\nCoverage Score: {result['coverage_score']}%")
    print(f"Matched: {result['matched_count']} | Missing: {result['missing_count']}")

    print(f"\n--- Matched Skills ---")
    for skill, data in result["matched_skills"].items():
        print(f"  ✓ {skill:20s} (in {data['pct']}% of {target} jobs)")

    print(f"\n--- Top Missing Skills (Learn These) ---")
    for skill in result["top_recommendations"]:
        data = result["missing_skills"][skill]
        print(f"  ✗ {skill:20s} (in {data['pct']}% of {target} jobs)")

    # TF-IDF matching
    print(f"\n--- Top Job Matches (TF-IDF Similarity) ---")
    user_resume = "Experienced data analyst with Python, SQL, Pandas, Excel, Tableau. Built dashboards and reports. Knowledge of statistics and data visualization."
    matches = tfidf_similarity(user_resume, n_matches=5)
    for m in matches:
        sal = f"${m['salary_min']:,.0f}-${m['salary_max']:,.0f}" if m['salary_min'] and not pd.isna(m['salary_min']) else "N/A"
        print(f"  {m['similarity']}% | {m['title'][:40]:40s} | {m['company'][:20]:20s} | {sal}")

    # LLM recommendations
    print(f"\n--- LLM Career Recommendations ---")
    rec = get_llm_recommendations(result)
    print(rec)