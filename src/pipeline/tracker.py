"""
TalentScope AI — Pipeline Run Tracker
Logs every pipeline execution for observability and debugging.
"""

from datetime import datetime, timezone
from sqlalchemy import text
from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("pipeline.tracker")


class PipelineTracker:
    """Tracks pipeline runs in the pipeline_runs table."""

    def __init__(self, pipeline_name: str, source: str = None):
        self.pipeline_name = pipeline_name
        self.source = source
        self.engine = get_engine()
        self.run_id = None
        self.start_time = None

    def start(self):
        """Record pipeline start."""
        self.start_time = datetime.now(timezone.utc)

        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO pipeline_runs (pipeline_name, source, start_time, status)
                    VALUES (:name, :source, :start_time, 'running')
                    RETURNING run_id
                """),
                {
                    "name": self.pipeline_name,
                    "source": self.source,
                    "start_time": self.start_time,
                }
            )
            self.run_id = result.fetchone()[0]
            conn.commit()

        logger.info(f"Pipeline [{self.pipeline_name}] started | run_id={self.run_id}")
        return self.run_id

    def complete(self, records_processed: int = 0, records_failed: int = 0):
        """Record pipeline success."""
        end_time = datetime.now(timezone.utc)

        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE pipeline_runs
                    SET end_time = :end_time,
                        status = 'success',
                        records_processed = :processed,
                        records_failed = :failed
                    WHERE run_id = :run_id
                """),
                {
                    "end_time": end_time,
                    "processed": records_processed,
                    "failed": records_failed,
                    "run_id": self.run_id,
                }
            )
            conn.commit()

        duration = (end_time - self.start_time).total_seconds()
        logger.info(
            f"Pipeline [{self.pipeline_name}] completed | "
            f"run_id={self.run_id} | "
            f"processed={records_processed} | "
            f"failed={records_failed} | "
            f"duration={duration:.1f}s"
        )

    def fail(self, error_message: str):
        """Record pipeline failure."""
        end_time = datetime.now(timezone.utc)

        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE pipeline_runs
                    SET end_time = :end_time,
                        status = 'failed',
                        error_message = :error
                    WHERE run_id = :run_id
                """),
                {
                    "end_time": end_time,
                    "error": error_message,
                    "run_id": self.run_id,
                }
            )
            conn.commit()

        logger.error(
            f"Pipeline [{self.pipeline_name}] FAILED | "
            f"run_id={self.run_id} | "
            f"error={error_message}"
        )


if __name__ == "__main__":
    # Quick test
    tracker = PipelineTracker("test_pipeline", source="test")
    tracker.start()
    tracker.complete(records_processed=10, records_failed=0)

    # Verify in database
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM pipeline_runs ORDER BY run_id DESC LIMIT 1"))
        row = result.fetchone()
        print(f"\nVerification: run_id={row[0]} | pipeline={row[1]} | status={row[4]} | processed={row[6]}")