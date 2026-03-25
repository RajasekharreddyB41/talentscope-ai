"""
TalentScope AI — Airflow DAG
Orchestrates the full ETL pipeline: Ingest → Clean → Features → Report
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default args for all tasks
default_args = {
    "owner": "talentscope",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_ingest_api(**kwargs):
    """Fetch jobs from API and load to raw_jobs."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    from src.ingestion.api_connector import fetch_jobs, save_raw_response
    from src.pipeline.etl import load_api_jobs_to_raw
    from src.pipeline.tracker import PipelineTracker

    queries = ["data analyst", "data engineer", "data scientist", "machine learning engineer"]
    tracker = PipelineTracker("airflow_ingest", source="api")
    tracker.start()

    total = 0
    for query in queries:
        jobs = fetch_jobs(query, num_pages=1)
        if jobs:
            save_raw_response(jobs, query)
            count = load_api_jobs_to_raw(jobs, source="api")
            total += count

    tracker.complete(records_processed=total)
    return total


def task_clean(**kwargs):
    """Run normalization and deduplication pipeline."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    from src.pipeline.dedup import run_cleaning_pipeline
    run_cleaning_pipeline()


def task_build_features(**kwargs):
    """Build ML features from clean jobs."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    from src.models.feature_engineering import build_features
    build_features()


def task_report(**kwargs):
    """Log pipeline completion summary."""
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

    from sqlalchemy import text
    from src.database.connection import get_engine
    from src.utils.logger import get_logger

    logger = get_logger("airflow.report")
    engine = get_engine()

    with engine.connect() as conn:
        raw = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        clean = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
        features = conn.execute(text("SELECT COUNT(*) FROM job_features")).fetchone()[0]
        runs = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).fetchone()[0]

    logger.info(
        f"Pipeline complete | raw_jobs={raw} | clean_jobs={clean} | "
        f"job_features={features} | pipeline_runs={runs}"
    )


# Define the DAG
with DAG(
    dag_id="talentscope_etl_pipeline",
    default_args=default_args,
    description="TalentScope AI — Full ETL Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["talentscope", "etl", "production"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_api_jobs",
        python_callable=task_ingest_api,
    )

    clean = PythonOperator(
        task_id="clean_and_dedup",
        python_callable=task_clean,
    )

    features = PythonOperator(
        task_id="build_features",
        python_callable=task_build_features,
    )

    report = PythonOperator(
        task_id="pipeline_report",
        python_callable=task_report,
    )

    # Task dependencies: Ingest → Clean → Features → Report
    ingest >> clean >> features >> report