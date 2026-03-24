"""
TalentScope AI — ETL Pipeline
Loads raw job data from various sources into the raw_jobs table.
"""

import json
from sqlalchemy import text
from src.database.connection import get_engine
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger

logger = get_logger("pipeline.etl")


def load_api_jobs_to_raw(jobs: list, source: str = "api") -> int:
    """
    Load a list of job dictionaries from JSearch API into raw_jobs.

    Args:
        jobs: List of raw job dicts from API
        source: Source identifier

    Returns:
        Number of records inserted
    """
    if not jobs:
        logger.warning("No jobs to load")
        return 0

    engine = get_engine()
    inserted = 0

    with engine.connect() as conn:
        for job in jobs:
            try:
                conn.execute(
                    text("""
                        INSERT INTO raw_jobs 
                        (source, source_job_id, raw_title, raw_company, 
                         raw_location, raw_salary, raw_description, raw_data)
                        VALUES (:source, :source_job_id, :raw_title, :raw_company,
                                :raw_location, :raw_salary, :raw_description, :raw_data)
                    """),
                    {
                        "source": source,
                        "source_job_id": job.get("job_id", ""),
                        "raw_title": job.get("job_title", ""),
                        "raw_company": job.get("employer_name", ""),
                        "raw_location": _build_location(job),
                        "raw_salary": _build_salary(job),
                        "raw_description": job.get("job_description", ""),
                        "raw_data": json.dumps(job),
                    }
                )
                inserted += 1
            except Exception as e:
                logger.error(f"Failed to insert job {job.get('job_id', 'unknown')}: {e}")

        conn.commit()

    logger.info(f"Loaded {inserted}/{len(jobs)} jobs into raw_jobs from source='{source}'")
    return inserted


def load_kaggle_jobs_to_raw(df, source: str = "kaggle") -> int:
    """
    Load a Pandas DataFrame of Kaggle jobs into raw_jobs.

    Args:
        df: Pandas DataFrame with job data
        source: Source identifier

    Returns:
        Number of records inserted
    """
    import math

    if df is None or df.empty:
        logger.warning("No Kaggle data to load")
        return 0

    engine = get_engine()
    inserted = 0
    failed = 0

    def clean_val(val):
        """Convert NaN/None to empty string."""
        if val is None:
            return ""
        if isinstance(val, float) and math.isnan(val):
            return ""
        return str(val)

    def safe_json(row_dict):
        """Convert row dict to valid JSON, replacing NaN with None."""
        cleaned = {}
        for k, v in row_dict.items():
            if isinstance(v, float) and math.isnan(v):
                cleaned[k] = None
            else:
                cleaned[k] = v
        return json.dumps(cleaned, default=str)

    for _, row in df.iterrows():
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw_jobs
                        (source, source_job_id, raw_title, raw_company,
                         raw_location, raw_salary, raw_description, raw_data)
                        VALUES (:source, :source_job_id, :raw_title, :raw_company,
                                :raw_location, :raw_salary, :raw_description, :raw_data)
                    """),
                    {
                        "source": source,
                        "source_job_id": clean_val(row.get("job_id", row.name)),
                        "raw_title": clean_val(row.get("title", row.get("job_title", ""))),
                        "raw_company": clean_val(row.get("company", row.get("company_name", ""))),
                        "raw_location": clean_val(row.get("location", row.get("job_location", ""))),
                        "raw_salary": clean_val(row.get("salary", row.get("salary_range", ""))),
                        "raw_description": clean_val(row.get("description", row.get("job_description", ""))),
                        "raw_data": safe_json(row.to_dict()),
                    }
                )
                conn.commit()
                inserted += 1
        except Exception as e:
            failed += 1
            if failed <= 3:
                logger.error(f"Failed to insert Kaggle row {row.name}: {e}")

    logger.info(f"Loaded {inserted}/{len(df)} jobs into raw_jobs from source='{source}' (failed={failed})")
    return inserted


def load_json_file_to_raw(filepath: str, source: str = "api") -> int:
    """
    Load a cached JSON file into raw_jobs.

    Args:
        filepath: Path to the JSON file
        source: Source identifier

    Returns:
        Number of records inserted
    """
    with open(filepath, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    return load_api_jobs_to_raw(jobs, source)


def _build_location(job: dict) -> str:
    """Build location string from API response fields."""
    parts = []
    if job.get("job_city"):
        parts.append(job["job_city"])
    if job.get("job_state"):
        parts.append(job["job_state"])
    if job.get("job_country"):
        parts.append(job["job_country"])
    return ", ".join(parts) if parts else "Unknown"


def _build_salary(job: dict) -> str:
    """Build salary string from API response fields."""
    sal_min = job.get("job_min_salary")
    sal_max = job.get("job_max_salary")
    sal_type = job.get("job_salary_period", "")

    if sal_min and sal_max:
        return f"${sal_min}-${sal_max} {sal_type}".strip()
    elif sal_min:
        return f"${sal_min}+ {sal_type}".strip()
    elif sal_max:
        return f"Up to ${sal_max} {sal_type}".strip()
    return ""


if __name__ == "__main__":
    import os
    import glob

    # Load all cached JSON files into raw_jobs
    tracker = PipelineTracker("ingest_api", source="api")
    tracker.start()

    json_files = glob.glob("data/raw/jsearch_*.json")
    total = 0

    for filepath in json_files:
        logger.info(f"Loading {filepath}...")
        count = load_json_file_to_raw(filepath, source="api")
        total += count

    tracker.complete(records_processed=total)

    # Show count
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw_jobs"))
        print(f"\nTotal records in raw_jobs: {result.fetchone()[0]}")