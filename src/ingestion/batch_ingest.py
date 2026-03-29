"""
TalentScope AI — Unified Daily Ingestion
Fetches from JSearch + Adzuna, deduplicates, loads into raw_jobs.
"""

import json
from datetime import datetime
from src.ingestion.api_connector import fetch_jobs as jsearch_fetch, save_raw_response as jsearch_save
from src.ingestion.adzuna_connector import (
    fetch_jobs as adzuna_fetch,
    save_raw_response as adzuna_save,
    transform_to_common_format,
)
from src.pipeline.etl import load_api_jobs_to_raw
from src.pipeline.tracker import PipelineTracker
from src.utils.config import RAPIDAPI_KEY, ADZUNA_APP_ID
from src.utils.logger import get_logger
from sqlalchemy import text
from src.database.connection import get_engine

logger = get_logger("ingestion.daily")

# Target roles for both APIs
QUERIES = [
    "data engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "python developer",
]


def ingest_jsearch(queries: list) -> int:
    """Fetch from JSearch API. Returns count of jobs loaded."""
    if not RAPIDAPI_KEY:
        logger.warning("JSearch: No API key configured, skipping")
        return 0

    total = 0
    for query in queries:
        try:
            jobs = jsearch_fetch(query, num_pages=1)
            if jobs:
                jsearch_save(jobs, query)
                count = load_api_jobs_to_raw(jobs, source="jsearch")
                total += count
                logger.info(f"JSearch: Loaded {count} jobs for '{query}'")
        except Exception as e:
            logger.error(f"JSearch failed for '{query}': {e}")

    return total


def ingest_adzuna(queries: list, results_per_page: int = 50) -> int:
    """Fetch from Adzuna API. Returns count of jobs loaded."""
    if not ADZUNA_APP_ID:
        logger.warning("Adzuna: No API credentials configured, skipping")
        return 0

    total = 0
    for query in queries:
        try:
            raw_jobs = adzuna_fetch(query, results_per_page=results_per_page)
            if raw_jobs:
                adzuna_save(raw_jobs, query)

                # Transform to common format for ETL
                transformed = [transform_to_common_format(j) for j in raw_jobs]
                count = load_api_jobs_to_raw(transformed, source="adzuna")
                total += count
                logger.info(f"Adzuna: Loaded {count} jobs for '{query}'")
        except Exception as e:
            logger.error(f"Adzuna failed for '{query}': {e}")

    return total


def run_daily_ingest(queries: list = None):
    """
    Run full daily ingestion from all sources.
    JSearch: 5 queries × 1 page = 5 API calls (~50 jobs)
    Adzuna:  5 queries × 50 results = 5 API calls (~250 jobs)
    Total per day: ~300 fresh jobs
    """
    if queries is None:
        queries = QUERIES

    tracker = PipelineTracker("daily_ingest", source="multi")
    tracker.start()

    print(f"\n{'='*50}")
    print(f"  TalentScope AI — Daily Ingestion")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    total = 0

    try:
        # Source 1: JSearch
        print("📡 Ingesting from JSearch...")
        jsearch_count = ingest_jsearch(queries)
        total += jsearch_count
        print(f"   ✅ JSearch: {jsearch_count} jobs loaded\n")

        # Source 2: Adzuna
        print("📡 Ingesting from Adzuna...")
        adzuna_count = ingest_adzuna(queries, results_per_page=50)
        total += adzuna_count
        print(f"   ✅ Adzuna: {adzuna_count} jobs loaded\n")

        tracker.complete(records_processed=total)

    except Exception as e:
        tracker.fail(str(e))
        logger.error(f"Daily ingest failed: {e}")
        raise

    # Summary
    print(f"{'='*50}")
    print(f"  DAILY INGEST COMPLETE")
    print(f"  Total new jobs loaded: {total}")
    print(f"{'='*50}\n")

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT source, COUNT(*) FROM raw_jobs GROUP BY source ORDER BY source"
        ))
        print("📊 raw_jobs by source:")
        for row in result:
            print(f"   {row[0]}: {row[1]:,} records")

        total_all = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        print(f"\n   TOTAL: {total_all:,} records")

    return total


if __name__ == "__main__":
    run_daily_ingest()