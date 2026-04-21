"""
TalentScope AI -- Unified Scraper Pipeline

Runs all ATS scrapers, filters to US-only jobs, inserts into raw_jobs,
then triggers the existing cleaning pipeline (raw_jobs -> clean_jobs).

Usage:
    python -m src.pipeline.scraper_pipeline
    python -m src.pipeline.scraper_pipeline --scrapers greenhouse lever
    python -m src.pipeline.scraper_pipeline --dry-run
"""

import json
import time
import argparse
from sqlalchemy import text

from src.database.connection import get_engine
from src.pipeline.tracker import PipelineTracker
from src.pipeline.dedup import run_cleaning_pipeline
from src.utils.logger import get_logger

logger = get_logger("pipeline.scraper")

# All available scrapers and their default company lists
SCRAPER_REGISTRY = {}


def _register_scrapers():
    """
    Lazy-load scrapers so import errors in one scraper
    don't break the entire pipeline.
    """

    try:
        from src.ingestion.greenhouse_scraper import GreenhouseScraper, DEFAULT_COMPANIES as GH_COMPANIES
        SCRAPER_REGISTRY["greenhouse"] = {
            "class": GreenhouseScraper,
            "companies": GH_COMPANIES,
        }
    except ImportError as e:
        logger.warning(f"Could not load greenhouse scraper: {e}")

    try:
        from src.ingestion.lever_scraper import LeverScraper, DEFAULT_COMPANIES as LV_COMPANIES
        SCRAPER_REGISTRY["lever"] = {
            "class": LeverScraper,
            "companies": LV_COMPANIES,
        }
    except ImportError as e:
        logger.warning(f"Could not load lever scraper: {e}")

    try:
        from src.ingestion.smartrecruiters_scraper import SmartRecruitersScraper, DEFAULT_COMPANIES as SR_COMPANIES
        SCRAPER_REGISTRY["smartrecruiters"] = {
            "class": SmartRecruitersScraper,
            "companies": SR_COMPANIES,
        }
    except ImportError as e:
        logger.warning(f"Could not load smartrecruiters scraper: {e}")

    try:
        from src.ingestion.workable_scraper import WorkableScraper, DEFAULT_COMPANIES as WK_COMPANIES
        SCRAPER_REGISTRY["workable"] = {
            "class": WorkableScraper,
            "companies": WK_COMPANIES,
        }
    except ImportError as e:
        logger.warning(f"Could not load workable scraper: {e}")

    try:
        from src.ingestion.ashby_scraper import AshbyScraper, DEFAULT_COMPANIES as AB_COMPANIES
        SCRAPER_REGISTRY["ashby"] = {
            "class": AshbyScraper,
            "companies": AB_COMPANIES,
        }
    except ImportError as e:
        logger.warning(f"Could not load ashby scraper: {e}")


# US country codes and names we accept
US_INDICATORS = {
    "us", "usa", "united states", "united states of america",
}

# US state abbreviations for location-based filtering
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC",
}

US_STATE_NAMES = {
    "alabama", "alaska", "arizona", "arkansas", "california",
    "colorado", "connecticut", "delaware", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas",
    "kentucky", "louisiana", "maine", "maryland", "massachusetts",
    "michigan", "minnesota", "mississippi", "missouri", "montana",
    "nebraska", "nevada", "new hampshire", "new jersey", "new mexico",
    "new york", "north carolina", "north dakota", "ohio", "oklahoma",
    "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia",
}


def is_us_job(job: dict) -> bool:
    """
    Determine if a scraped job is US-based.
    Checks location string for US states, state abbreviations, and country codes.
    Remote jobs with no location are included (could be US remote).
    """
    location = (job.get("location") or "").strip().lower()

    if not location:
        # No location info -- include it (could be remote US)
        return True

    # Check for explicit non-US country names
    non_us_countries = {
        "canada", "uk", "united kingdom", "germany", "france", "india",
        "australia", "japan", "china", "brazil", "mexico", "ireland",
        "netherlands", "singapore", "spain", "italy", "sweden",
        "switzerland", "israel", "south korea", "poland", "portugal",
        "austria", "belgium", "denmark", "finland", "norway",
        "czech republic", "romania", "hungary", "greece", "turkey",
        "argentina", "chile", "colombia", "peru", "philippines",
        "thailand", "vietnam", "indonesia", "malaysia", "taiwan",
        "new zealand", "south africa", "nigeria", "kenya", "egypt",
        "saudi arabia", "uae", "dubai", "qatar",
    }

    # Split location into parts
    parts = [p.strip() for p in location.replace("|", ",").split(",")]

    # If any part is a known non-US country, reject
    for part in parts:
        if part in non_us_countries:
            return False

    # If any part is a US state abbreviation or name, accept
    for part in parts:
        if part.upper() in US_STATES:
            return True
        if part in US_STATE_NAMES:
            return True

    # Check for US country indicators anywhere in location
    for indicator in US_INDICATORS:
        if indicator in location:
            return True

    # Check for common US city names (high confidence)
    us_cities = {
        "new york", "san francisco", "los angeles", "chicago",
        "seattle", "boston", "austin", "denver", "atlanta",
        "dallas", "houston", "miami", "phoenix", "portland",
        "san diego", "san jose", "philadelphia", "washington",
        "minneapolis", "detroit", "charlotte", "nashville",
        "raleigh", "salt lake city", "pittsburgh", "columbus",
    }
    for part in parts:
        if part in us_cities:
            return True

    # If location contains "remote" with no country, include it
    if "remote" in location:
        return True

    # Default: exclude unknown locations (safer for US-only platform)
    return False


def insert_scraped_jobs_to_raw(jobs: list[dict]) -> int:
    """
    Insert scraped jobs into raw_jobs table.
    Maps the normalized scraper schema to the raw_jobs schema.
    Returns number of rows inserted.
    """
    if not jobs:
        return 0

    engine = get_engine()
    inserted = 0

    # Batch insert for speed
    BATCH_SIZE = 50
    with engine.connect() as conn:
        for i in range(0, len(jobs), BATCH_SIZE):
            batch = jobs[i:i + BATCH_SIZE]
            records = []
            for job in batch:
                records.append({
                    "source": job.get("source_platform", "scraper"),
                    "source_job_id": job.get("job_id", ""),
                    "raw_title": job.get("title", ""),
                    "raw_company": job.get("company", ""),
                    "raw_location": job.get("location", ""),
                    "raw_salary": "",  # most ATS pages don't show salary
                    "raw_description": (job.get("description") or "")[:10000],
                    "raw_data": json.dumps({
                        "job_apply_link": job.get("apply_url", ""),
                        "posted_date": job.get("posted_date", ""),
                        "employment_type": job.get("employment_type", ""),
                        "seniority": job.get("seniority", ""),
                        "source_platform": job.get("source_platform", ""),
                        "raw_json": job.get("raw_json", {}),
                    }, default=str),
                })

            try:
                conn.execute(
                    text("""
                        INSERT INTO raw_jobs
                        (source, source_job_id, raw_title, raw_company,
                         raw_location, raw_salary, raw_description, raw_data)
                        VALUES
                        (:source, :source_job_id, :raw_title, :raw_company,
                         :raw_location, :raw_salary, :raw_description, :raw_data)
                    """),
                    records,
                )
                inserted += len(batch)
            except Exception as e:
                logger.warning(f"Batch insert failed at {i}, falling back to row-by-row: {e}")
                for record in records:
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO raw_jobs
                                (source, source_job_id, raw_title, raw_company,
                                 raw_location, raw_salary, raw_description, raw_data)
                                VALUES
                                (:source, :source_job_id, :raw_title, :raw_company,
                                 :raw_location, :raw_salary, :raw_description, :raw_data)
                            """),
                            record,
                        )
                        inserted += 1
                    except Exception as e2:
                        logger.error(f"Row insert failed: {e2}")

        conn.commit()

    return inserted


def run_scraper_pipeline(
    scraper_names: list[str] | None = None,
    dry_run: bool = False,
):
    """
    Main pipeline: scrape -> filter US -> insert raw_jobs -> clean -> done.

    Args:
        scraper_names: list of scraper names to run (None = all)
        dry_run: if True, scrape and filter but don't insert into DB
    """
    _register_scrapers()

    if not SCRAPER_REGISTRY:
        logger.error("No scrapers available. Check import errors above.")
        return

    # Determine which scrapers to run
    if scraper_names:
        selected = {k: v for k, v in SCRAPER_REGISTRY.items() if k in scraper_names}
        unknown = set(scraper_names) - set(SCRAPER_REGISTRY.keys())
        if unknown:
            logger.warning(f"Unknown scrapers ignored: {unknown}")
    else:
        selected = SCRAPER_REGISTRY

    if not selected:
        logger.error("No valid scrapers selected.")
        return

    tracker = PipelineTracker("scraper_pipeline", source="ats_scrapers")
    tracker.start()
    started = time.time()

    all_jobs = []
    scraper_stats = {}

    print("=" * 60)
    print("TALENTSCOPE AI -- SCRAPER PIPELINE")
    print("=" * 60)
    print(f"Scrapers: {', '.join(selected.keys())}")
    print(f"Dry run: {dry_run}")
    print()

    # Step 1: Run each scraper
    for name, config in selected.items():
        print(f"--- {name.upper()} ---")
        try:
            scraper = config["class"]()
            companies = config["companies"]
            jobs = scraper.scrape(companies)

            # Filter to US only
            us_jobs = [j for j in jobs if is_us_job(j)]
            non_us = len(jobs) - len(us_jobs)

            all_jobs.extend(us_jobs)
            scraper_stats[name] = {
                "total": len(jobs),
                "us_only": len(us_jobs),
                "filtered_out": non_us,
                "companies": len(companies),
            }

            print(f"  {len(jobs)} total -> {len(us_jobs)} US jobs ({non_us} filtered out)")
            print()

        except Exception as e:
            logger.error(f"Scraper {name} failed: {e}")
            scraper_stats[name] = {"total": 0, "us_only": 0, "filtered_out": 0, "error": str(e)}
            print(f"  FAILED: {e}")
            print()

    # Step 2: Insert into raw_jobs
    print("=" * 60)
    print(f"Total US jobs scraped: {len(all_jobs)}")

    if dry_run:
        print("\nDRY RUN -- skipping database insert and cleaning")
        tracker.complete(records_processed=0)
    elif all_jobs:
        print(f"\nInserting {len(all_jobs)} jobs into raw_jobs...")
        inserted = insert_scraped_jobs_to_raw(all_jobs)
        print(f"Inserted: {inserted}")

        # Step 3: Run cleaning pipeline (raw_jobs -> clean_jobs with dedup)
        print("\nRunning cleaning pipeline (raw -> clean with dedup)...")
        run_cleaning_pipeline()

        elapsed = time.time() - started
        tracker.complete(records_processed=inserted)
        print(f"\nPipeline complete in {elapsed:.1f}s")
    else:
        print("\nNo US jobs to insert.")
        tracker.complete(records_processed=0)

    # Summary
    print("\n" + "=" * 60)
    print("SCRAPER SUMMARY")
    print("=" * 60)
    for name, stats in scraper_stats.items():
        if "error" in stats:
            print(f"  {name:20s} | ERROR: {stats['error'][:50]}")
        else:
            print(
                f"  {name:20s} | {stats['companies']:3d} companies | "
                f"{stats['total']:5d} total | {stats['us_only']:5d} US | "
                f"{stats['filtered_out']:5d} filtered"
            )
    print(f"\n  TOTAL US JOBS: {len(all_jobs)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TalentScope AI Scraper Pipeline")
    parser.add_argument(
        "--scrapers", nargs="+",
        help="Specific scrapers to run (default: all)",
        choices=["greenhouse", "lever", "smartrecruiters", "workable", "ashby"],
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape and filter but don't insert into database",
    )
    args = parser.parse_args()

    run_scraper_pipeline(
        scraper_names=args.scrapers,
        dry_run=args.dry_run,
    )