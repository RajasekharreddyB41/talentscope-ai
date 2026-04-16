"""
TalentScope AI — Adzuna API Connector
Fetches job postings from Adzuna API (250 requests/day free).
"""

import requests
import json
import os
from datetime import datetime
from src.utils.config import ADZUNA_APP_ID, ADZUNA_APP_KEY
from src.utils.logger import get_logger

logger = get_logger("ingestion.adzuna")

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs/us/search"

# Search queries aligned with JSearch connector
DEFAULT_QUERIES = [
    "data engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "python developer",
]


def fetch_jobs(query: str, page: int = 1, results_per_page: int = 50) -> list:
    """
    Fetch job postings from Adzuna API.

    Args:
        query: Job search query
        page: Page number (1-indexed)
        results_per_page: Results per page (max 50)

    Returns:
        List of raw job dictionaries
    """
    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        logger.error("Adzuna API credentials not configured")
        return []

    url = f"{ADZUNA_BASE_URL}/{page}"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "what": query,
        "results_per_page": results_per_page,
        "content-type": "application/json",
        "sort_by": "date",
        "max_days_old": 30,
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        jobs = data.get("results", [])

        logger.info(f"Fetched {len(jobs)} jobs for query='{query}' | page={page}")
        return jobs

    except requests.exceptions.RequestException as e:
        logger.error(f"Adzuna request failed for query='{query}': {e}")
        return []


def transform_to_common_format(adzuna_job: dict) -> dict:
    """
    Transform Adzuna job format to match JSearch format.
    This ensures the ETL pipeline handles both sources identically.
    """
    # Extract location parts
    location = adzuna_job.get("location", {})
    area = location.get("area", [])
    city = area[-1] if area else ""
    state = area[-2] if len(area) >= 2 else ""
    country = area[0] if area else "US"

    # Extract salary
    salary_min = adzuna_job.get("salary_min")
    salary_max = adzuna_job.get("salary_max")

    return {
        "job_id": adzuna_job.get("id", ""),
        "job_title": adzuna_job.get("title", ""),
        "employer_name": adzuna_job.get("company", {}).get("display_name", ""),
        "job_city": city,
        "job_state": state,
        "job_country": country,
        "job_description": adzuna_job.get("description", ""),
        "job_min_salary": salary_min,
        "job_max_salary": salary_max,
        "job_salary_period": "yearly" if salary_min and salary_min > 1000 else "",
        "job_posted_at_datetime_utc": adzuna_job.get("created", ""),
        "job_apply_link": adzuna_job.get("redirect_url") or adzuna_job.get("url", ""),
        "job_is_remote": (
            "remote" in adzuna_job.get("title", "").lower()
            or "remote" in adzuna_job.get("description", "").lower()
        ),
        "job_employment_type": adzuna_job.get("contract_type", ""),
        "job_source": "adzuna",
        # Keep original for raw_jobs
        "_raw_adzuna": adzuna_job,
    }


def save_raw_response(jobs: list, query: str):
    """Save raw Adzuna API response to data/raw/ for caching."""
    os.makedirs("data/raw", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/adzuna_{query.replace(' ', '_')}_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Saved {len(jobs)} raw Adzuna jobs to {filename}")
    return filename


if __name__ == "__main__":
    # Test with a single query
    query = "data analyst"
    print(f"\nFetching Adzuna jobs for: '{query}'...")

    raw_jobs = fetch_jobs(query, results_per_page=10)

    if raw_jobs:
        save_raw_response(raw_jobs, query)

        # Transform and show sample
        sample = transform_to_common_format(raw_jobs[0])
        print(f"\n--- Sample Job (Adzuna) ---")
        print(f"Title:    {sample['job_title']}")
        print(f"Company:  {sample['employer_name']}")
        print(f"Location: {sample['job_city']}, {sample['job_state']}")
        print(f"Remote:   {sample['job_is_remote']}")
        print(f"Salary:   {sample['job_min_salary']} - {sample['job_max_salary']}")
        print(f"Apply:    {sample['job_apply_link']}")
        print(f"Posted:   {sample['job_posted_at_datetime_utc']}")
        print(f"\nTotal jobs fetched: {len(raw_jobs)}")
    else:
        print("No jobs returned. Check ADZUNA_APP_ID and ADZUNA_APP_KEY in .env")