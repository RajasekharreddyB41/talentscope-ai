"""
TalentScope AI — JSearch API Connector
Fetches job postings from RapidAPI JSearch endpoint.
"""

import requests
import json
import os
from datetime import datetime
from src.utils.config import RAPIDAPI_KEY
from src.utils.logger import get_logger

logger = get_logger("ingestion.api")

JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"

HEADERS = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
}

# Default search queries for tech jobs
DEFAULT_QUERIES = [
    "data engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "python developer",
]


def fetch_jobs(query: str, page: int = 1, num_pages: int = 1) -> list:
    """
    Fetch job postings from JSearch API.

    Args:
        query: Job search query (e.g., "data engineer")
        page: Page number to fetch
        num_pages: Number of pages to fetch

    Returns:
        List of raw job dictionaries
    """
    params = {
        "query": query,
        "page": str(page),
        "num_pages": str(num_pages),
        "date_posted": "month",
    }

    try:
        response = requests.get(JSEARCH_URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        jobs = data.get("data", [])

        logger.info(f"Fetched {len(jobs)} jobs for query='{query}' | page={page}")
        return jobs

    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for query='{query}': {e}")
        return []


def save_raw_response(jobs: list, query: str):
    """Save raw API response to data/raw/ for caching."""
    os.makedirs("data/raw", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/raw/jsearch_{query.replace(' ', '_')}_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(jobs)} raw jobs to {filename}")
    return filename


if __name__ == "__main__":
    # Test with a single query to save API calls
    query = "data analyst"
    print(f"\nFetching jobs for: '{query}'...")

    jobs = fetch_jobs(query, num_pages=1)

    if jobs:
        # Save raw response
        save_raw_response(jobs, query)

        # Show first job as sample
        sample = jobs[0]
        print(f"\n--- Sample Job ---")
        print(f"Title:    {sample.get('job_title', 'N/A')}")
        print(f"Company:  {sample.get('employer_name', 'N/A')}")
        print(f"Location: {sample.get('job_city', 'N/A')}, {sample.get('job_state', 'N/A')}")
        print(f"Remote:   {sample.get('job_is_remote', 'N/A')}")
        print(f"Salary:   {sample.get('job_min_salary', 'N/A')} - {sample.get('job_max_salary', 'N/A')}")
        print(f"Posted:   {sample.get('job_posted_at_datetime_utc', 'N/A')}")
        print(f"\nTotal jobs fetched: {len(jobs)}")
    else:
        print("No jobs returned. Check your API key in .env")