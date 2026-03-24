"""
TalentScope AI — Raw to Clean Pipeline
Transforms raw_jobs into normalized, deduplicated clean_jobs.
"""

import json
from sqlalchemy import text
from src.database.connection import get_engine
from src.pipeline.normalize import (
    normalize_salary, normalize_location, 
    extract_experience_level, generate_dedup_hash
)
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger

logger = get_logger("pipeline.clean")


def run_cleaning_pipeline():
    """
    Read all unprocessed raw_jobs, normalize, deduplicate, 
    and insert into clean_jobs.
    """
    engine = get_engine()
    tracker = PipelineTracker("raw_to_clean", source="all")
    tracker.start()

    inserted = 0
    skipped_dup = 0
    failed = 0

    try:
        # Get raw jobs that haven't been cleaned yet
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT r.id, r.source, r.raw_title, r.raw_company, 
                       r.raw_location, r.raw_salary, r.raw_description, r.raw_data
                FROM raw_jobs r
                LEFT JOIN clean_jobs c ON c.raw_job_id = r.id
                WHERE c.id IS NULL
                ORDER BY r.id
            """))
            raw_jobs = result.fetchall()

        logger.info(f"Found {len(raw_jobs)} unprocessed raw jobs")

        for row in raw_jobs:
            try:
                raw_id, source, raw_title, raw_company = row[0], row[1], row[2], row[3]
                raw_location, raw_salary, raw_description = row[4], row[5], row[6]
                raw_data_str = row[7]

                # Parse raw_data JSON for extra fields
                extra = {}
                if raw_data_str:
                    try:
                        extra = json.loads(raw_data_str) if isinstance(raw_data_str, str) else raw_data_str
                    except (json.JSONDecodeError, TypeError):
                        extra = {}

                # Skip if no title
                if not raw_title or raw_title.strip() == "":
                    failed += 1
                    continue

                # Normalize salary
                salary = normalize_salary(raw_salary or "")

                # Normalize location
                loc = normalize_location(raw_location or "")

                # Check remote from raw_data too
                if extra.get("job_is_remote") or extra.get("remote_allowed") == "1":
                    loc["is_remote"] = True

                # Extract experience level
                exp_from_data = extra.get("formatted_experience_level", "")
                if exp_from_data and str(exp_from_data).lower() not in ["nan", ""]:
                    exp_map = {
                        "entry level": "junior", "associate": "junior",
                        "mid-senior level": "mid", "internship": "junior",
                        "director": "lead", "executive": "lead",
                    }
                    experience = exp_map.get(str(exp_from_data).lower(), "mid")
                else:
                    experience = extract_experience_level(raw_title, raw_description or "")

                # Employment type
                emp_type = extra.get("job_employment_type", 
                           extra.get("formatted_work_type", "full-time"))
                emp_type = str(emp_type).lower() if emp_type and str(emp_type) != "nan" else "full-time"

                # URL
                url = extra.get("job_apply_link", extra.get("job_posting_url", ""))
                url = str(url) if url and str(url) != "nan" else ""

                # Posted date
                posted = extra.get("job_posted_at_datetime_utc", extra.get("listed_time", ""))
                posted_date = None
                if posted and str(posted) != "nan":
                    posted_str = str(posted)
                    try:
                        if "T" in posted_str:
                            posted_date = posted_str[:10]
                        elif posted_str.replace(".", "").isdigit():
                            # Unix timestamp in milliseconds
                            import datetime
                            ts = float(posted_str) / 1000
                            posted_date = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    except (ValueError, OSError):
                        posted_date = None

                # Skills raw
                skills_raw = extra.get("skills_desc", "")
                skills_raw = str(skills_raw) if skills_raw and str(skills_raw) != "nan" else ""

                # Dedup hash
                dedup_hash = generate_dedup_hash(
                    raw_title or "", raw_company or "", raw_location or ""
                )

                # Insert into clean_jobs
                with engine.connect() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO clean_jobs
                            (raw_job_id, title, company, location_city, location_state,
                             location_country, is_remote, salary_min, salary_max,
                             salary_currency, experience_level, employment_type,
                             description, skills_raw, url, posted_date, source, dedup_hash)
                            VALUES
                            (:raw_job_id, :title, :company, :city, :state,
                             :country, :is_remote, :sal_min, :sal_max,
                             'USD', :experience, :emp_type,
                             :description, :skills_raw, :url, :posted_date, :source, :dedup_hash)
                            ON CONFLICT (dedup_hash) DO NOTHING
                        """),
                        {
                            "raw_job_id": raw_id,
                            "title": raw_title.strip(),
                            "company": (raw_company or "").strip(),
                            "city": loc["city"],
                            "state": loc["state"],
                            "country": loc["country"],
                            "is_remote": loc["is_remote"],
                            "sal_min": salary["min"],
                            "sal_max": salary["max"],
                            "experience": experience,
                            "emp_type": emp_type,
                            "description": (raw_description or "")[:10000],
                            "skills_raw": skills_raw[:5000],
                            "url": url[:500],
                            "posted_date": posted_date,
                            "source": source,
                            "dedup_hash": dedup_hash,
                        }
                    )
                    # Check if it was actually inserted or skipped due to dedup
                    check = conn.execute(
                        text("SELECT id FROM clean_jobs WHERE dedup_hash = :hash"),
                        {"hash": dedup_hash}
                    )
                    if check.fetchone():
                        inserted += 1
                    else:
                        skipped_dup += 1
                    conn.commit()

            except Exception as e:
                failed += 1
                if failed <= 5:
                    logger.error(f"Failed to clean raw_job id={row[0]}: {e}")

        tracker.complete(records_processed=inserted, records_failed=failed)

        logger.info(f"Cleaning complete: inserted={inserted}, duplicates_skipped={skipped_dup}, failed={failed}")

    except Exception as e:
        tracker.fail(str(e))
        raise

    # Summary stats
    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
        clean_count = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
        salary_count = conn.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL")).fetchone()[0]

        print(f"\n--- Cleaning Summary ---")
        print(f"  raw_jobs:    {raw_count}")
        print(f"  clean_jobs:  {clean_count}")
        print(f"  with salary: {salary_count}")
        print(f"  dedup rate:  {((raw_count - clean_count) / max(raw_count, 1) * 100):.1f}%")


if __name__ == "__main__":
    run_cleaning_pipeline()