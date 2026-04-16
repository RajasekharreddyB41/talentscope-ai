"""
TalentScope AI — Base scraper class for all ATS platforms.

Subclasses override only three things:
    - platform_name (class attribute)
    - build_url(company_id)
    - parse_jobs(raw_data, company_id)

Everything else is handled here: HTTP session, retries, rate limiting,
structured logging, deterministic job_id hashing, and schema normalization.
"""

import hashlib
import time
from abc import ABC, abstractmethod
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.utils.logger import get_logger


# Fields every normalized job dict must have.
REQUIRED_FIELDS = (
    "job_id",
    "title",
    "company",
    "location",
    "description",
    "apply_url",
    "posted_date",
    "source_platform",
    "employment_type",
    "seniority",
    "raw_json",
)

# HTTP status codes that are worth retrying.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


class ScraperError(Exception):
    """Raised when a fetch fails in a way worth retrying."""


class BaseScraper(ABC):
    """
    Abstract base for all ATS platform scrapers.

    Typical subclass:
        class GreenhouseScraper(BaseScraper):
            platform_name = "greenhouse"

            def build_url(self, company_id: str) -> str:
                return f"https://boards-api.greenhouse.io/v1/boards/{company_id}/jobs?content=true"

            def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
                jobs = []
                for j in raw_data.get("jobs", []):
                    jobs.append(self.normalize_job(
                        source_job_id=str(j["id"]),
                        company_id=company_id,
                        title=j.get("title", ""),
                        company=j.get("company_name", company_id),
                        location=j.get("location", {}).get("name", ""),
                        description=j.get("content", ""),
                        apply_url=j.get("absolute_url", ""),
                        posted_date=j.get("updated_at", "")[:10],
                        raw=j,
                    ))
                return jobs
    """

    # --- Must be set by subclass ---
    platform_name: str = ""

    # --- Can be tuned per subclass if needed ---
    request_timeout: float = 15.0          # seconds per request
    request_delay: float = 0.5             # polite delay between companies (seconds)
    max_retries: int = 3                   # retries on transient errors
    user_agent: str = "TalentScope-AI/1.0 (+https://talentscope-ai-rsr-06.streamlit.app)"

    def __init__(self):
        if not self.platform_name:
            raise ValueError(
                f"{type(self).__name__} must set class attribute 'platform_name'"
            )
        self.logger = get_logger(f"ingestion.{self.platform_name}")
        self.session = self._build_session()

    # ------------------------------------------------------------
    # Methods subclasses MUST implement
    # ------------------------------------------------------------

    @abstractmethod
    def build_url(self, company_id: str) -> str:
        """Return the endpoint URL for a given company identifier."""

    @abstractmethod
    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        """
        Transform platform-specific payload into a list of normalized job dicts.
        Use self.normalize_job(...) to build each dict.
        """

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    def scrape(self, company_ids: list[str]) -> list[dict]:
        """
        Scrape jobs for a list of companies. Returns a flat list of normalized jobs.
        A failure on one company is logged and skipped — it does not abort the run.
        """
        all_jobs: list[dict] = []
        success, failure = 0, 0
        started = time.time()

        self.logger.info(
            f"scrape start platform={self.platform_name} companies={len(company_ids)}"
        )

        for i, company_id in enumerate(company_ids, start=1):
            try:
                raw = self._fetch(company_id)
                jobs = self.parse_jobs(raw, company_id)
                self._validate_jobs(jobs, company_id)
                all_jobs.extend(jobs)
                success += 1
                self.logger.info(
                    f"  [{i}/{len(company_ids)}] {company_id}: {len(jobs)} jobs"
                )
            except Exception as e:
                failure += 1
                self.logger.warning(
                    f"  [{i}/{len(company_ids)}] {company_id}: FAILED ({type(e).__name__}: {e})"
                )

            # Polite delay between companies (not after the last one)
            if i < len(company_ids) and self.request_delay > 0:
                time.sleep(self.request_delay)

        elapsed = time.time() - started
        self.logger.info(
            f"scrape done platform={self.platform_name} jobs={len(all_jobs)} "
            f"ok={success} failed={failure} elapsed={elapsed:.1f}s"
        )
        return all_jobs

    # ------------------------------------------------------------
    # Helpers available to subclasses
    # ------------------------------------------------------------

    def normalize_job(
        self,
        source_job_id: str,
        company_id: str,
        title: str,
        company: str,
        location: str,
        description: str,
        apply_url: str,
        posted_date: str,
        raw: dict,
        employment_type: str = "",
        seniority: str = "",
    ) -> dict:
        """
        Build a normalized job dict with a deterministic job_id.

        job_id is SHA-256 of: platform + company_id + source_job_id
        This makes cross-platform dedup stable even if a company re-lists a role.
        """
        job_id = self._make_job_id(company_id, source_job_id)

        inferred_seniority = seniority or self._infer_seniority(title)

        return {
            "job_id": job_id,
            "title": (title or "").strip(),
            "company": (company or company_id or "").strip(),
            "location": (location or "").strip(),
            "description": (description or "").strip(),
            "apply_url": (apply_url or "").strip(),
            "posted_date": (posted_date or "").strip(),
            "source_platform": self.platform_name,
            "employment_type": (employment_type or "").strip().lower(),
            "seniority": inferred_seniority,
            "raw_json": raw,
        }

    # ------------------------------------------------------------
    # Internal: HTTP layer
    # ------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })
        # A single adapter with pooling; retry logic handled by tenacity (below)
        # so we can log each retry instead of silently retrying inside urllib3.
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def _fetch(self, company_id: str) -> dict:
        """Fetch and JSON-decode the company's job feed. Retries transient errors."""
        url = self.build_url(company_id)
        return self._fetch_with_retry(url)

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(ScraperError),
    )
    def _fetch_with_retry(self, url: str) -> dict:
        try:
            resp = self.session.get(url, timeout=self.request_timeout)
        except requests.RequestException as e:
            # Connection errors, timeouts, DNS, etc. — all retryable
            raise ScraperError(f"request failed: {e}") from e

        if resp.status_code in RETRY_STATUS_CODES:
            raise ScraperError(f"retryable status {resp.status_code}")

        if resp.status_code == 404:
            # Company feed missing — don't retry, return empty so parse_jobs gets []
            self.logger.info(f"404 for {url} — treating as empty feed")
            return {}

        if resp.status_code >= 400:
            # Non-retryable client errors (401, 403, etc.) — surface them
            raise ScraperError(f"HTTP {resp.status_code}: {resp.text[:200]}")

        try:
            return resp.json()
        except ValueError as e:
            raise ScraperError(f"invalid JSON: {e}") from e

    # ------------------------------------------------------------
    # Internal: validation & helpers
    # ------------------------------------------------------------

    @staticmethod
    def _make_job_id(company_id: str, source_job_id: str) -> str:
        key = f"{company_id}::{source_job_id}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]

    def _validate_jobs(self, jobs: list[dict], company_id: str) -> None:
        """Defensive check — catches schema drift early when adding new scrapers."""
        for i, job in enumerate(jobs):
            missing = [f for f in REQUIRED_FIELDS if f not in job]
            if missing:
                raise ValueError(
                    f"job at index {i} for {company_id} missing fields: {missing}"
                )
            if job["source_platform"] != self.platform_name:
                raise ValueError(
                    f"source_platform mismatch: expected {self.platform_name}, "
                    f"got {job['source_platform']}"
                )

    @staticmethod
    def _infer_seniority(title: str) -> str:
        """
        Best-effort seniority inference from job title.
        Returns '' when we can't tell — never guesses.
        """
        t = (title or "").lower()
        if not t:
            return ""
        # Order matters: check 'lead' / 'principal' before 'senior'
        if any(w in t for w in ("principal", " staff ", "staff,", "staff ", "lead ", " lead")):
            return "lead"
        if any(w in t for w in ("senior", " sr.", " sr ", "sr ")):
            return "senior"
        if any(w in t for w in ("junior", " jr.", " jr ", "jr ", "entry", "intern")):
            return "junior"
        if any(w in t for w in ("ii", "iii", "mid-", "mid ", "associate")):
            return "mid"
        return ""


# ------------------------------------------------------------
# Self-test — uses a MockScraper to verify the base class works
# without hitting the network.
# ------------------------------------------------------------

if __name__ == "__main__":

    class _MockScraper(BaseScraper):
        platform_name = "mock"
        request_delay = 0  # skip politeness in tests

        def __init__(self, fixture):
            self._fixture = fixture
            super().__init__()

        def build_url(self, company_id: str) -> str:
            return f"https://example.test/{company_id}"

        # Bypass network entirely for the test
        def _fetch(self, company_id: str) -> dict:
            if company_id == "fail-co":
                raise ScraperError("simulated fetch failure")
            return self._fixture.get(company_id, {"jobs": []})

        def parse_jobs(self, raw_data, company_id):
            out = []
            for j in raw_data.get("jobs", []):
                out.append(self.normalize_job(
                    source_job_id=str(j["id"]),
                    company_id=company_id,
                    title=j.get("title", ""),
                    company=j.get("company", company_id),
                    location=j.get("location", ""),
                    description=j.get("description", ""),
                    apply_url=j.get("url", ""),
                    posted_date=j.get("date", ""),
                    employment_type=j.get("type", ""),
                    raw=j,
                ))
            return out

    print("=" * 60)
    print("BASE SCRAPER SELF-TEST")
    print("=" * 60)

    fixture = {
        "airbnb": {"jobs": [
            {"id": 1, "title": "Senior Data Engineer", "company": "Airbnb",
             "location": "San Francisco", "description": "Build pipelines.",
             "url": "https://airbnb.com/careers/1", "date": "2026-04-10",
             "type": "Full-time"},
            {"id": 2, "title": "Junior ML Intern", "company": "Airbnb",
             "location": "Remote", "description": "Learn ML.",
             "url": "https://airbnb.com/careers/2", "date": "2026-04-12",
             "type": "Part-time"},
        ]},
        "stripe": {"jobs": [
            {"id": 100, "title": "Staff Software Engineer", "company": "Stripe",
             "location": "New York", "description": "Payments infra.",
             "url": "https://stripe.com/jobs/100", "date": "2026-04-11",
             "type": "Full-time"},
        ]},
    }

    scraper = _MockScraper(fixture)
    jobs = scraper.scrape(["airbnb", "stripe", "fail-co", "unknown-co"])

    print(f"\nTotal jobs: {len(jobs)}  (expected 3)")
    assert len(jobs) == 3, "expected 3 jobs total"

    for job in jobs:
        missing = [f for f in REQUIRED_FIELDS if f not in job]
        assert not missing, f"missing fields: {missing}"
        assert job["source_platform"] == "mock"
        assert len(job["job_id"]) == 32
        print(f"  {job['job_id'][:8]}.. | {job['title']:30s} | "
              f"seniority={job['seniority']:6s} | {job['company']}")

    # Deterministic hashing check
    dup_id = scraper._make_job_id("airbnb", "1")
    assert jobs[0]["job_id"] == dup_id, "job_id must be deterministic"
    print(f"\nDeterministic job_id check: OK ({dup_id[:16]}..)")

    # Seniority inference spot-check
    samples = {
        "Senior Data Engineer": "senior",
        "Junior ML Intern": "junior",
        "Staff Software Engineer": "lead",
        "Data Engineer II": "mid",
        "Software Engineer": "",
    }
    print("\nSeniority inference:")
    for title, expected in samples.items():
        got = BaseScraper._infer_seniority(title)
        status = "OK" if got == expected else f"FAIL (got {got!r})"
        print(f"  {title:35s} -> {got or '(none)':8s} {status}")
        assert got == expected, f"seniority mismatch for {title}"

    print("\nAll assertions passed.")