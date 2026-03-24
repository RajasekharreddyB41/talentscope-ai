"""
TalentScope AI — Kaggle Dataset Loader
Loads and filters Kaggle job postings for tech roles.
"""

import pandas as pd
from src.pipeline.etl import load_kaggle_jobs_to_raw
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger

logger = get_logger("ingestion.kaggle")

# Tech-related keywords for filtering
TECH_KEYWORDS = [
    "data analyst", "data engineer", "data scientist",
    "machine learning", "software engineer", "python",
    "sql", "analytics", "business intelligence", "bi developer",
    "cloud engineer", "devops", "mlops", "ai engineer",
    "full stack", "backend", "frontend", "web developer",
]

KAGGLE_FILE = "data/kaggle/postings.csv"


def load_kaggle_tech_jobs(filepath: str = KAGGLE_FILE, sample_size: int = 2000) -> int:
    """
    Load tech-relevant jobs from Kaggle CSV into raw_jobs.

    Args:
        filepath: Path to Kaggle CSV
        sample_size: Max number of records to load

    Returns:
        Number of records loaded
    """
    logger.info(f"Reading Kaggle dataset from {filepath}...")

    # Read only needed columns to save memory
    use_cols = [
        "job_id", "title", "company_name", "description",
        "min_salary", "max_salary", "pay_period", "location",
        "remote_allowed", "formatted_experience_level",
        "formatted_work_type", "skills_desc", "job_posting_url",
        "listed_time", "currency",
    ]

    df = pd.read_csv(
        filepath,
        usecols=use_cols,
        dtype=str,
        on_bad_lines="skip",
    )

    logger.info(f"Total rows in dataset: {len(df):,}")

    # Filter for tech-related jobs
    pattern = "|".join(TECH_KEYWORDS)
    mask = df["title"].str.lower().str.contains(pattern, na=False)
    df_tech = df[mask].copy()

    logger.info(f"Tech-related jobs found: {len(df_tech):,}")

    # Drop rows with no title
    df_tech = df_tech.dropna(subset=["title"])

    # Sample if too many
    if len(df_tech) > sample_size:
        df_tech = df_tech.sample(n=sample_size, random_state=42)
        logger.info(f"Sampled down to {sample_size} records")

    # Rename columns to match our loader expectations
    df_tech = df_tech.rename(columns={
        "company_name": "company",
        "min_salary": "salary_min",
        "max_salary": "salary_max",
    })

    # Build salary string
    df_tech["salary"] = df_tech.apply(
        lambda r: f"${r['salary_min']}-${r['salary_max']} {r.get('pay_period', '')}"
        if pd.notna(r.get("salary_min")) and pd.notna(r.get("salary_max"))
        else "",
        axis=1,
    )

    # Load into raw_jobs
    loaded = load_kaggle_jobs_to_raw(df_tech, source="kaggle")

    return loaded


if __name__ == "__main__":
    tracker = PipelineTracker("ingest_kaggle", source="kaggle")
    tracker.start()

    try:
        count = load_kaggle_tech_jobs()
        tracker.complete(records_processed=count)
    except Exception as e:
        tracker.fail(str(e))
        raise

    # Show totals
    from sqlalchemy import text
    from src.database.connection import get_engine

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT source, COUNT(*) FROM raw_jobs GROUP BY source"
        ))
        print("\n--- raw_jobs by source ---")
        for row in result:
            print(f"  {row[0]}: {row[1]} records")

        total = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        print(f"\n  Total: {total} records")