"""
TalentScope AI — Feature Engineering
Transforms clean_jobs into ML-ready features in job_features table.
"""

import re
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger

logger = get_logger("models.features")

# Common tech skills to extract from descriptions
SKILL_KEYWORDS = [
    "python", "sql", "java", "javascript", "typescript", "r", "scala", "go", "rust", "c++",
    "aws", "azure", "gcp", "docker", "kubernetes", "terraform", "linux",
    "spark", "hadoop", "kafka", "airflow", "dbt",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "keras",
    "tableau", "power bi", "looker", "excel",
    "postgresql", "mysql", "mongodb", "redis", "snowflake", "bigquery", "redshift",
    "git", "ci/cd", "jenkins", "github actions",
    "react", "angular", "vue", "node.js", "django", "flask", "fastapi",
    "machine learning", "deep learning", "nlp", "computer vision",
    "etl", "data pipeline", "data warehouse", "data lake",
    "agile", "scrum", "jira",
]

# Experience level encoding
EXP_ENCODING = {
    "junior": 0,
    "mid": 1,
    "senior": 2,
    "lead": 3,
}

# Location tiers by cost of living
LOCATION_TIERS = {
    "tier1": ["San Francisco", "New York", "San Jose", "Seattle", "Boston",
              "Los Angeles", "Washington", "Palo Alto", "Mountain View", "Sunnyvale"],
    "tier2": ["Austin", "Denver", "Chicago", "Portland", "San Diego",
              "Atlanta", "Dallas", "Miami", "Minneapolis", "Philadelphia"],
    "tier3": [],  # Everything else
}


def extract_skills(description: str) -> list:
    """Extract tech skills from job description text."""
    if not description:
        return []

    desc_lower = description.lower()
    found_skills = []

    for skill in SKILL_KEYWORDS:
        # Use word boundary matching for short skills
        if len(skill) <= 2:
            pattern = r'\b' + re.escape(skill) + r'\b'
            if re.search(pattern, desc_lower):
                found_skills.append(skill)
        else:
            if skill in desc_lower:
                found_skills.append(skill)

    return sorted(set(found_skills))


def encode_location(city: str) -> str:
    """Encode location into cost-of-living tier."""
    if not city:
        return "unknown"

    for tier, cities in LOCATION_TIERS.items():
        if city in cities:
            return tier

    return "tier3"


def normalize_salary_for_ml(salary_min, salary_max, global_min=30000, global_max=500000):
    """Normalize salary to 0-1 scale for ML."""
    if salary_min is None:
        return None

    mid = (float(salary_min) + float(salary_max or salary_min)) / 2
    normalized = (mid - global_min) / (global_max - global_min)
    return max(0.0, min(1.0, round(normalized, 4)))


def categorize_title(title: str) -> str:
    """Map job title to a standard category."""
    title_lower = title.lower()

    categories = [
        ("data engineer", "Data Engineer"),
        ("data analyst", "Data Analyst"),
        ("data scientist", "Data Scientist"),
        ("machine learning", "ML Engineer"),
        ("ml engineer", "ML Engineer"),
        ("software engineer", "Software Engineer"),
        ("software developer", "Software Engineer"),
        ("python", "Python Developer"),
        ("full stack", "Full Stack"),
        ("fullstack", "Full Stack"),
        ("devops", "DevOps/Cloud"),
        ("cloud", "DevOps/Cloud"),
        ("business intelligence", "BI Analyst"),
        ("analytics", "Analytics"),
        ("backend", "Backend Developer"),
        ("frontend", "Frontend Developer"),
    ]

    for keyword, category in categories:
        if keyword in title_lower:
            return category

    return "Other Tech"


def build_features():
    """
    Build features for all clean_jobs that don't have features yet.
    Inserts into job_features table.
    """
    engine = get_engine()
    tracker = PipelineTracker("feature_engineering", source="all")
    tracker.start()

    try:
        # Get clean jobs without features
        with engine.connect() as conn:
            df = pd.read_sql(text("""
                SELECT c.id, c.title, c.company, c.location_city,
                       c.salary_min, c.salary_max, c.experience_level,
                       c.description
                FROM clean_jobs c
                LEFT JOIN job_features f ON f.clean_job_id = c.id
                WHERE f.id IS NULL
            """), conn)

        logger.info(f"Building features for {len(df)} jobs")

        if df.empty:
            logger.info("No new jobs to process")
            tracker.complete(records_processed=0)
            return

        inserted = 0

        for _, row in df.iterrows():
            try:
                # Extract skills
                skills = extract_skills(row["description"])

                # Encode features
                exp_encoded = EXP_ENCODING.get(row["experience_level"], 1)
                loc_encoded = encode_location(row["location_city"])
                sal_normalized = normalize_salary_for_ml(row["salary_min"], row["salary_max"])
                title_cat = categorize_title(row["title"])

                with engine.connect() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO job_features
                            (clean_job_id, skills, skill_count, experience_encoded,
                             salary_normalized, location_encoded, title_category)
                            VALUES
                            (:clean_job_id, :skills, :skill_count, :exp_encoded,
                             :sal_norm, :loc_encoded, :title_cat)
                        """),
                        {
                            "clean_job_id": int(row["id"]),
                            "skills": skills,
                            "skill_count": len(skills),
                            "exp_encoded": exp_encoded,
                            "sal_norm": sal_normalized,
                            "loc_encoded": loc_encoded,
                            "title_cat": title_cat,
                        }
                    )
                    conn.commit()
                    inserted += 1

            except Exception as e:
                if inserted < 3:
                    logger.error(f"Failed feature row {row['id']}: {e}")

        tracker.complete(records_processed=inserted)
        logger.info(f"Features built for {inserted} jobs")

        # Summary
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM job_features")).fetchone()[0]
            avg_skills = conn.execute(text("SELECT AVG(skill_count) FROM job_features")).fetchone()[0]
            with_salary = conn.execute(text("SELECT COUNT(*) FROM job_features WHERE salary_normalized IS NOT NULL")).fetchone()[0]

            print(f"\n--- Feature Engineering Summary ---")
            print(f"  Total features:    {total}")
            print(f"  Avg skills/job:    {avg_skills:.1f}")
            print(f"  With salary norm:  {with_salary}")

            # Top skills
            result = conn.execute(text("""
                SELECT skill, COUNT(*) as cnt
                FROM job_features, UNNEST(skills) AS skill
                GROUP BY skill
                ORDER BY cnt DESC
                LIMIT 15
            """))
            print(f"\n--- Top 15 Skills ---")
            for row in result:
                print(f"  {row[0]:20s}: {row[1]}")

    except Exception as e:
        tracker.fail(str(e))
        raise


if __name__ == "__main__":
    build_features()