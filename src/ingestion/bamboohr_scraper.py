"""
TalentScope AI -- BambooHR ATS scraper.

Public JSON endpoint, no auth required. Used by thousands of small
to mid-size tech companies.

Endpoint shape:
    https://{company_id}.bamboohr.com/careers/list

Returns all jobs in one response. No pagination.
Apply URLs follow the pattern:
    https://{company_id}.bamboohr.com/careers/{job_id}
"""

from src.ingestion.base_scraper import BaseScraper


# Seed list of known BambooHR-hosted companies.
# company_id is the subdomain from their careers page URL.
DEFAULT_COMPANIES = [
    "asana",
    "ziprecruiter",
    "qualtrics",
    "lucid",
    "weave",
    "podium",
    "pluralsight",
    "domo",
]


class BambooHRScraper(BaseScraper):
    """Scraper for BambooHR-hosted job boards."""

    platform_name = "bamboohr"
    request_delay = 0.5

    def build_url(self, company_id: str) -> str:
        return f"https://{company_id}.bamboohr.com/careers/list"

    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        jobs = []

        # BambooHR returns {"result": [{"id": ..., ...}, ...]}
        result = raw_data.get("result") or []

        for j in result:
            source_job_id = str(j.get("id", "")).strip()
            if not source_job_id:
                continue

            title = (j.get("jobOpeningName") or "").strip()

            # Location fields
            location = self._build_location(j)

            # Company name: BambooHR does not include it in the response,
            # so we title-case the subdomain
            company_name = company_id.replace("-", " ").replace("_", " ").title()

            # No description in the list endpoint
            description = ""

            # Apply URL
            apply_url = f"https://{company_id}.bamboohr.com/careers/{source_job_id}"

            # Employment type
            employment_type = (j.get("employmentStatusLabel") or "").strip().lower()

            # Department can hint at role but not seniority
            # Use title-based inference from base class
            seniority = ""

            jobs.append(self.normalize_job(
                source_job_id=source_job_id,
                company_id=company_id,
                title=title,
                company=company_name,
                location=location,
                description=description,
                apply_url=apply_url,
                posted_date="",
                employment_type=employment_type,
                seniority=seniority,
                raw=j,
            ))

        return jobs

    @staticmethod
    def _build_location(job: dict) -> str:
        """Build location string from BambooHR location fields."""
        city = (job.get("city") or "").strip()
        state = (job.get("state") or "").strip()
        country = (job.get("country") or "").strip()

        # Some jobs have a combined location field instead
        location_str = (job.get("location") or {})
        if isinstance(location_str, dict):
            city = city or (location_str.get("city") or "").strip()
            state = state or (location_str.get("state") or "").strip()
            country = country or (location_str.get("country") or "").strip()

        parts = []
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        if country:
            parts.append(country)
        return ", ".join(parts) if parts else ""


# ------------------------------------------------------------
# Self-test -- hits the real BambooHR API
# ------------------------------------------------------------

if __name__ == "__main__":
    TEST_COMPANIES = ["asana"]

    print("=" * 60)
    print("BAMBOOHR SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = BambooHRScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")

    if len(jobs) == 0:
        print("WARNING: 0 jobs returned. The company slug may have changed.")
        print("Try other slugs like: ziprecruiter, qualtrics, lucid")
    else:
        # Structural checks
        from src.ingestion.base_scraper import REQUIRED_FIELDS
        sample = jobs[0]
        missing = [f for f in REQUIRED_FIELDS if f not in sample]
        assert not missing, f"missing fields: {missing}"
        assert sample["source_platform"] == "bamboohr"
        assert len(sample["job_id"]) == 32

        # Preview first 5 jobs
        print("\nSample jobs:")
        for job in jobs[:5]:
            print(
                f"  {job['company']:15s} | {job['title'][:40]:40s} | "
                f"{job['location'][:25]:25s} | seniority={job['seniority'] or '-':6s}"
            )
            print(f"    apply: {job['apply_url']}")
            print()

        # Seniority distribution
        from collections import Counter
        sen_counts = Counter(j["seniority"] or "(none)" for j in jobs)
        print(f"Seniority breakdown: {dict(sen_counts)}")

        print("\nAll assertions passed.")