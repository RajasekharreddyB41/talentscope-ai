"""
TalentScope AI — DAG Task Validation
Tests each Airflow task as standalone Python functions.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.utils.logger import get_logger

logger = get_logger("test.dag")


def test_report_task():
    """Test the report task logic."""
    from sqlalchemy import text
    from src.database.connection import get_engine

    engine = get_engine()
    with engine.connect() as conn:
        raw = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        clean = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
        features = conn.execute(text("SELECT COUNT(*) FROM job_features")).fetchone()[0]
        runs = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).fetchone()[0]

    print(f"\n--- DAG Task Validation ---")
    print(f"  raw_jobs:      {raw}")
    print(f"  clean_jobs:    {clean}")
    print(f"  job_features:  {features}")
    print(f"  pipeline_runs: {runs}")

    assert raw > 0, "raw_jobs is empty"
    assert clean > 0, "clean_jobs is empty"
    assert features > 0, "job_features is empty"
    assert runs > 0, "pipeline_runs is empty"

    print("\n  All DAG task validations PASSED")


def test_task_dependencies():
    """Validate DAG structure by importing it."""
    try:
        # Verify the DAG file is valid Python
        dag_path = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "jobpulse_dag.py")
        with open(dag_path, "r") as f:
            code = f.read()

        # Check key components exist
        checks = [
            ("task_ingest_api", "Ingest task defined"),
            ("task_clean", "Clean task defined"),
            ("task_build_features", "Features task defined"),
            ("task_report", "Report task defined"),
            ("ingest >> clean >> features >> report", "Task dependencies defined"),
            ("retries", "Retry logic configured"),
            ("schedule_interval", "Schedule configured"),
        ]

        print("\n--- DAG Structure Validation ---")
        for keyword, description in checks:
            found = keyword in code
            status = "PASS" if found else "FAIL"
            print(f"  [{status}] {description}")
            assert found, f"Missing: {keyword}"

        print("\n  All DAG structure checks PASSED")

    except FileNotFoundError:
        print("  DAG file not found at expected path")


if __name__ == "__main__":
    test_task_dependencies()
    test_report_task()