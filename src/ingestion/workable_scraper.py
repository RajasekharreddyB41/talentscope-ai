"""
TalentScope AI -- Workable ATS scraper.

Public widget API, no auth required. Used by thousands of mid-size
tech companies worldwide.

Endpoint shape:
    GET https://apply.workable.com/api/v1/widget/accounts/{company_slug}

Returns all jobs in one response. No pagination needed.
Apply URLs follow the pattern:
    https://apply.workable.com/{company_slug}/j/{shortcode}/

Docs: https://help.workable.com/hc/en-us/articles/115012771647
"""

import re
import html as html_mod

from src.ingestion.base_scraper import BaseScraper


_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


# Seed list of known Workable-hosted companies.
# Slug is from the apply.workable.com/{slug} URL.
DEFAULT_COMPANIES = [
    "careers",          # Workable itself
    "tp-link-usa-corp",
    "practicetek",
    "factorial-6",
    "deepl",
    "samsara",
    "wrike",
    "miro-6",
]


def _clean_html(raw: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    if not raw:
        return ""
    unescaped = html_mod.unescape(raw)
    no_tags = _TAG_RE.sub(" ", unescaped)
    return _WHITESPACE_RE.sub(" ", no_tags).strip()


class WorkableScraper(BaseScraper):
    """Scraper for Workable-hosted job boards via the public widget API."""

    platform_name = "workable"
    request_delay = 0.5

    def build_url(self, company_id: str) -> str:
        return (
            f"https://apply.workable.com/api/v1/widget/accounts/"
            f"{company_id}"
        )

    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        jobs = []

        # Widget API returns {"jobs": [...], "name": "Company Name", ...}
        company_name = (
            raw_data.get("name")
            or company_id.replace("-", " ").replace("_", " ").title()
        )

        for j in raw_data.get("jobs", []):
            # shortcode is the unique job identifier in Workable
            shortcode = (j.get("shortcode") or "").strip()
            source_job_id = shortcode or str(j.get("id", "")).strip()
            if not source_job_id:
                continue

            title = (j.get("title") or "").strip()

            # Location
            location = self._build_location(j)

            # Description: widget endpoint may or may not include it
            description = _clean_html(j.get("description") or "")

            # Apply URL
            if shortcode:
                apply_url = f"https://apply.workable.com/{company_id}/j/{shortcode}/"
            else:
                apply_url = (
                    j.get("url")
                    or j.get("shortlink")
                    or f"https://apply.workable.com/{company_id}/"
                )

            # Posted date
            posted_raw = j.get("published_on") or j.get("created_at") or ""
            posted_date = posted_raw[:10] if posted_raw else ""

            # Employment type
            employment_type = (j.get("employment_type") or "").strip().lower()

            # Department (for context, not seniority)
            # Seniority inferred from title by base class
            seniority = ""

            jobs.append(self.normalize_job(
                source_job_id=source_job_id,
                company_id=company_id,
                title=title,
                company=company_name,
                location=location,
                description=description,
                apply_url=apply_url,
                posted_date=posted_date,
                employment_type=employment_type,
                seniority=seniority,
                raw=j,
            ))

        return jobs

    @staticmethod
    def _build_location(job: dict) -> str:
        """Build location from Workable's location fields."""
        # Widget API can return location as a string or as an object
        loc = job.get("location")
        if isinstance(loc, str):
            return loc.strip()
        if isinstance(loc, dict):
            parts = []
            city = (loc.get("city") or "").strip()
            region = (loc.get("region") or "").strip()
            country = (loc.get("country") or "").strip()
            if city:
                parts.append(city)
            if region:
                parts.append(region)
            if country:
                parts.append(country)
            return ", ".join(parts) if parts else ""

        # Fallback: some responses use country_code at top level
        country = (job.get("country") or "").strip()
        city = (job.get("city") or "").strip()
        if city and country:
            return f"{city}, {country}"
        return city or country or ""


# ------------------------------------------------------------
# Self-test -- hits the real Workable widget API
# ------------------------------------------------------------

if __name__ == "__main__":
    TEST_COMPANIES = ["careers"]  # Workable's own job board

    print("=" * 60)
    print("WORKABLE SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = WorkableScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")

    if len(jobs) == 0:
        print("WARNING: 0 jobs returned. The company slug may have changed.")
        print("Try other slugs like: deepl, samsara, practicetek")
    else:
        # Structural checks
        from src.ingestion.base_scraper import REQUIRED_FIELDS
        sample = jobs[0]
        missing = [f for f in REQUIRED_FIELDS if f not in sample]
        assert not missing, f"missing fields: {missing}"
        assert sample["source_platform"] == "workable"
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