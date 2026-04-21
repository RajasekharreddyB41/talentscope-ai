"""
TalentScope AI -- Ashby ATS scraper.

Public posting API, no auth required. Used by Ramp, Notion, Vercel,
Linear, Retool, and hundreds of high-growth startups.

Endpoint shape:
    GET https://api.ashbyhq.com/posting-api/job-board/{company_slug}?includeCompensation=true

Returns all jobs in one response. No pagination needed.
Apply URLs come directly from the response (jobUrl field).

Docs: https://developers.ashbyhq.com/docs/public-job-posting-api
"""

import re
import html as html_mod

from src.ingestion.base_scraper import BaseScraper


_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


# Seed list of known Ashby-hosted companies.
# Slug is from the jobs.ashbyhq.com/{slug} URL.
DEFAULT_COMPANIES = [
    "ramp",
    "notion",
    "vercel",
    "linear",
    "retool",
    "plaid",
    "brex",
    "openai",
]


def _clean_html(raw: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    if not raw:
        return ""
    unescaped = html_mod.unescape(raw)
    no_tags = _TAG_RE.sub(" ", unescaped)
    return _WHITESPACE_RE.sub(" ", no_tags).strip()


class AshbyScraper(BaseScraper):
    """Scraper for Ashby-hosted job boards via the public posting API."""

    platform_name = "ashby"
    request_delay = 0.5

    def build_url(self, company_id: str) -> str:
        return (
            f"https://api.ashbyhq.com/posting-api/job-board/"
            f"{company_id}?includeCompensation=true"
        )

    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        jobs = []

        for j in raw_data.get("jobs", []):
            # Ashby uses a UUID as the job ID
            source_job_id = (j.get("id") or "").strip()
            if not source_job_id:
                continue

            title = (j.get("title") or "").strip()

            # Location
            location = self._build_location(j)

            # Company name: not in the response, use slug
            company_name = company_id.replace("-", " ").replace("_", " ").title()

            # Description: Ashby provides both HTML and plain text
            description = (
                _clean_html(j.get("descriptionHtml") or "")
                or (j.get("descriptionPlain") or "").strip()
            )

            # Apply URL: Ashby provides jobUrl and applyUrl
            apply_url = (
                j.get("jobUrl")
                or j.get("applyUrl")
                or f"https://jobs.ashbyhq.com/{company_id}"
            )

            # Posted date
            posted_raw = j.get("publishedAt") or ""
            posted_date = posted_raw[:10] if posted_raw else ""

            # Employment type: Ashby uses values like "FullTime", "PartTime", "Contract"
            employment_type = (j.get("employmentType") or "").strip()
            # Normalize to lowercase with hyphen
            employment_type = self._normalize_employment_type(employment_type)

            # Seniority: inferred from title by base class
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
        """Build location from Ashby's location and address fields."""
        # Primary location is a simple string
        loc_str = (job.get("location") or "").strip()
        if loc_str:
            return loc_str

        # Fallback: parse the address object
        address = job.get("address") or {}
        postal = address.get("postalAddress") or {}
        parts = []
        city = (postal.get("addressLocality") or "").strip()
        region = (postal.get("addressRegion") or "").strip()
        country = (postal.get("addressCountry") or "").strip()
        if city:
            parts.append(city)
        if region:
            parts.append(region)
        if country:
            parts.append(country)
        return ", ".join(parts) if parts else ""

    @staticmethod
    def _normalize_employment_type(raw: str) -> str:
        """Convert Ashby's PascalCase employment types to lowercase."""
        mapping = {
            "fulltime": "full-time",
            "parttime": "part-time",
            "contract": "contract",
            "intern": "intern",
            "internship": "intern",
            "temporary": "temporary",
        }
        return mapping.get(raw.lower().replace(" ", "").replace("-", ""), raw.lower())


# ------------------------------------------------------------
# Self-test -- hits the real Ashby posting API
# ------------------------------------------------------------

if __name__ == "__main__":
    TEST_COMPANIES = ["ramp"]

    print("=" * 60)
    print("ASHBY SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = AshbyScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")

    if len(jobs) == 0:
        print("WARNING: 0 jobs returned. The company slug may have changed.")
        print("Try other slugs like: notion, vercel, linear, retool")
    else:
        # Structural checks
        from src.ingestion.base_scraper import REQUIRED_FIELDS
        sample = jobs[0]
        missing = [f for f in REQUIRED_FIELDS if f not in sample]
        assert not missing, f"missing fields: {missing}"
        assert sample["source_platform"] == "ashby"
        assert len(sample["job_id"]) == 32

        # Preview first 5 jobs
        print("\nSample jobs:")
        for job in jobs[:5]:
            desc_preview = (
                (job["description"][:80] + "...")
                if len(job["description"]) > 80
                else job["description"]
            )
            print(
                f"  {job['company']:15s} | {job['title'][:40]:40s} | "
                f"{job['location'][:25]:25s} | seniority={job['seniority'] or '-':6s}"
            )
            print(f"    apply: {job['apply_url']}")
            print(f"    desc:  {desc_preview}")
            print()

        # Seniority distribution
        from collections import Counter
        sen_counts = Counter(j["seniority"] or "(none)" for j in jobs)
        print(f"Seniority breakdown: {dict(sen_counts)}")

        print("\nAll assertions passed.")