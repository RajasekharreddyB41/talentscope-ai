"""
TalentScope AI — Batch Ingestion
Fetches multiple job queries from API and loads into raw_jobs.
"""

from src.ingestion.api_connector import fetch_jobs, save_raw_response
from src.pipeline.etl import load_api_jobs_to_raw
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger
from sqlalchemy import text
from src.database.connection import get_engine

logger = get_logger("ingestion.batch")

# Queries to fetch — focused on our target roles
QUERIES = [
    "data engineer",
    "data scientist",
    "machine learning engineer",
    "python developer",
]


def run_batch_ingest(queries: list = None):
    """Fetch and load jobs for multiple search queries."""
    if queries is None:
        queries = QUERIES

    tracker = PipelineTracker("batch_api_ingest", source="api")
    tracker.start()

    total_loaded = 0

    try:
        for query in queries:
            logger.info(f"Fetching: '{query}'...")

            # Fetch from API
            jobs = fetch_jobs(query, num_pages=1)

            if jobs:
                # Cache locally
                save_raw_response(jobs, query)

                # Load to database
                count = load_api_jobs_to_raw(jobs, source="api")
                total_loaded += count

        tracker.complete(records_processed=total_loaded)

    except Exception as e:
        tracker.fail(str(e))
        raise

    # Summary
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT source, COUNT(*) FROM raw_jobs GROUP BY source ORDER BY source"
        ))
        print("\n--- raw_jobs by source ---")
        for row in result:
            print(f"  {row[0]}: {row[1]} records")

        total = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        print(f"\n  Total: {total} records")


if __name__ == "__main__":
    run_batch_ingest()