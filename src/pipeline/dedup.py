"""
TalentScope AI — Raw to Clean Pipeline (Optimized)
Transforms raw_jobs into normalized, deduplicated clean_jobs.
Uses batch inserts for 10-20x speed improvement.
"""

import json
import datetime
from sqlalchemy import text
from src.database.connection import get_engine
from src.pipeline.normalize import (
    normalize_salary, normalize_location,
    extract_experience_level, generate_dedup_hash
)
from src.pipeline.tracker import PipelineTracker
from src.utils.logger import get_logger

logger = get_logger("pipeline.clean")


def _process_raw_row(row) -> dict | None:
    """
    Process a single raw_jobs row into a clean_jobs record.
    Returns dict ready for insert, or None if invalid.
    """
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
        return None

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

    return {
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


def run_cleaning_pipeline():
    """
    Read all unprocessed raw_jobs, normalize, deduplicate,
    and batch insert into clean_jobs.
    """
    engine = get_engine()
    tracker = PipelineTracker("raw_to_clean", source="all")
    tracker.start()

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

        if not raw_jobs:
            tracker.complete(records_processed=0)
            print("\n--- Cleaning Summary ---")
            print("  No new jobs to process")
            return

        # Process all rows in memory first (fast)
        records = []
        seen_hashes = set()
        for row in raw_jobs:
            try:
                record = _process_raw_row(row)
                if record is None:
                    failed += 1
                    continue
                # Skip duplicates within this batch
                if record["dedup_hash"] in seen_hashes:
                    continue
                seen_hashes.add(record["dedup_hash"])
                records.append(record)
            except Exception as e:
                failed += 1
                if failed <= 5:
                    logger.error(f"Failed to process raw_job id={row[0]}: {e}")

        logger.info(f"Processed {len(records)} records, {failed} failed")

        # Batch insert with ON CONFLICT DO NOTHING
        BATCH_SIZE = 50
        inserted = 0

        with engine.connect() as conn:
            for i in range(0, len(records), BATCH_SIZE):
                batch = records[i:i + BATCH_SIZE]
                try:
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
                        batch
                    )
                    inserted += len(batch)
                except Exception as e:
                    # If batch fails, try one by one
                    logger.warning(f"Batch insert failed at {i}, falling back to row-by-row: {e}")
                    for record in batch:
                        try:
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
                                record
                            )
                            inserted += 1
                        except Exception as e2:
                            failed += 1
                            if failed <= 5:
                                logger.error(f"Row insert failed: {e2}")

                if (i + BATCH_SIZE) % 200 == 0 or (i + BATCH_SIZE) >= len(records):
                    logger.info(f"  Progress: {min(i + BATCH_SIZE, len(records))}/{len(records)}")

            conn.commit()

        tracker.complete(records_processed=inserted, records_failed=failed)
        logger.info(f"Cleaning complete: inserted={inserted}, failed={failed}")

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