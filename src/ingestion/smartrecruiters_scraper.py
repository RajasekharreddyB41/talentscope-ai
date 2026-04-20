"""
TalentScope AI -- SmartRecruiters ATS scraper.

Public JSON API, no auth required. Used by Visa, Starbucks, McDonald's,
KPMG, Bosch, Siemens, and many large enterprises.

Endpoint shape:
    https://careers.smartrecruiters.com/api/v1/companies/{company_id}/postings?limit=100&offset=0

Paginated: returns up to 100 per page. Loop until fewer than 100 come back.
"""

import time

from src.ingestion.base_scraper import BaseScraper, ScraperError


# Seed list of known SmartRecruiters-hosted companies.
# company_id is the slug from their careers page URL.
DEFAULT_COMPANIES = [
    "Visa",
    "PepsiCo",
    "Equinix",
    "KPMG",
    "Siemens",
    "BoschGroup",
]

PAGE_SIZE = 100


class SmartRecruitersScraper(BaseScraper):
    """Scraper for SmartRecruiters-hosted job boards."""

    platform_name = "smartrecruiters"
    request_delay = 0.5

    def build_url(self, company_id: str) -> str:
        """Base URL without pagination params. _fetch_all_pages adds offset."""
        return (
            f"https://api.smartrecruiters.com/v1/companies/"
            f"{company_id}/postings"
        )

    def _fetch(self, company_id: str) -> dict:
        """
        Override _fetch to handle pagination.
        Returns a dict with key 'content' holding all jobs across pages.
        """
        all_postings = []
        offset = 0

        while True:
            url = f"{self.build_url(company_id)}?limit={PAGE_SIZE}&offset={offset}"
            data = self._fetch_with_retry(url)

            # SmartRecruiters returns {"content": [...], "totalFound": N, ...}
            page_jobs = data.get("content", [])
            if not page_jobs:
                break

            all_postings.extend(page_jobs)
            self.logger.info(
                f"    page offset={offset} got {len(page_jobs)} jobs "
                f"(total so far: {len(all_postings)})"
            )

            if len(page_jobs) < PAGE_SIZE:
                break  # last page

            offset += PAGE_SIZE
            time.sleep(self.request_delay)  # polite between pages

        return {"content": all_postings}

    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        jobs = []

        for j in raw_data.get("content", []):
            source_job_id = str(j.get("id") or j.get("uuid", "")).strip()
            if not source_job_id:
                continue

            title = (j.get("name") or "").strip()

            # Location: SmartRecruiters nests it under location object
            loc_obj = j.get("location") or {}
            location = self._build_location(loc_obj)

            # Company name from the response
            company_obj = j.get("company") or {}
            company_name = (
                company_obj.get("name")
                or company_id.replace("-", " ").replace("_", " ").title()
            )

            # Description: stored under customField or sometimes not in listing
            # The postings endpoint gives a short description in 'name' and
            # detailed content under 'jobAd' -> 'sections' -> 'companyDescription'
            # but the simpler path is the 'ref' link. We grab what we can.
            description = self._extract_description(j)

            # Apply URL: SmartRecruiters provides a ref link or we build one
            # Apply URL: build the careers page link for direct apply
            apply_url = f"https://careers.smartrecruiters.com/{company_id}/{source_job_id}"

            # Posted date
            posted_raw = j.get("releasedDate") or j.get("createdOn") or ""
            posted_date = posted_raw[:10] if posted_raw else ""

            # Employment type
            type_obj = j.get("typeOfEmployment") or {}
            employment_type = (type_obj.get("label") or "").strip().lower()

            # Seniority from experienceLevel
            exp_obj = j.get("experienceLevel") or {}
            seniority_raw = (exp_obj.get("label") or "").strip().lower()
            seniority = self._map_seniority(seniority_raw)

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
    def _build_location(loc_obj: dict) -> str:
        """Build 'City, State, Country' from SmartRecruiters location object."""
        parts = []
        city = (loc_obj.get("city") or "").strip()
        region = (loc_obj.get("region") or "").strip()
        country = (loc_obj.get("country") or "").strip()
        if city:
            parts.append(city)
        if region:
            parts.append(region)
        if country:
            parts.append(country)
        return ", ".join(parts) if parts else ""

    @staticmethod
    def _extract_description(job: dict) -> str:
        """
        Pull whatever description text is available.
        The postings endpoint has a jobAd section with HTML content.
        We grab the first non-empty section we find.
        """
        job_ad = job.get("jobAd") or {}
        sections = job_ad.get("sections") or {}

        # Try these sections in order of usefulness
        for key in ("jobDescription", "qualifications", "additionalInformation",
                     "companyDescription"):
            section = sections.get(key) or {}
            text = (section.get("text") or "").strip()
            if text:
                # Basic HTML cleanup -- same approach as greenhouse
                import re
                import html as html_mod
                text = html_mod.unescape(text)
                text = re.sub(r"<[^>]+>", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                return text

        return ""

    @staticmethod
    def _map_seniority(raw_level: str) -> str:
        """Map SmartRecruiters experienceLevel labels to our standard values."""
        if not raw_level:
            return ""
        r = raw_level.lower()
        if "intern" in r or "entry" in r or "junior" in r:
            return "junior"
        if "mid" in r or "associate" in r:
            return "mid"
        if "senior" in r or "experienced" in r:
            return "senior"
        if "lead" in r or "principal" in r or "director" in r or "executive" in r:
            return "lead"
        return ""


# ------------------------------------------------------------
# Self-test -- hits the real SmartRecruiters API
# ------------------------------------------------------------

if __name__ == "__main__":
    TEST_COMPANIES = ["Visa"]

    print("=" * 60)
    print("SMARTRECRUITERS SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = SmartRecruitersScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")
    assert len(jobs) > 0, (
        "expected at least one job -- if this fails, SmartRecruiters may be "
        "down or the company slug has changed."
    )

    # Structural checks
    from src.ingestion.base_scraper import REQUIRED_FIELDS
    sample = jobs[0]
    missing = [f for f in REQUIRED_FIELDS if f not in sample]
    assert not missing, f"missing fields: {missing}"
    assert sample["source_platform"] == "smartrecruiters"
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