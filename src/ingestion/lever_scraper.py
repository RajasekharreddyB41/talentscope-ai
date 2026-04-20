"""
TalentScope AI — Lever ATS scraper.

Public JSON API, no auth required. Used by Netflix, Shopify, Reddit,
Notion, Figma, and many high-growth startups.

Endpoint shape:
    https://api.lever.co/v0/postings/{company_id}?mode=json

Returns a flat JSON array of job objects (not wrapped in { "jobs": [...] }).
"""

from datetime import datetime
from typing import Optional

from src.ingestion.base_scraper import BaseScraper


DEFAULT_COMPANIES = [
    "palantir",
]


def _extract_description(lists_block: list) -> str:
    """
    Lever descriptions come as a list of { text, content } blocks.
    Content often contains HTML tags — we strip them to plain text.
    """
    import re
    _tag_re = re.compile(r"<[^>]+>")
    _ws_re = re.compile(r"\s+")

    if not lists_block:
        return ""
    parts = []
    for block in lists_block:
        content = block.get("content") or ""
        if isinstance(content, str):
            parts.append(content.strip())
        elif isinstance(content, list):
            parts.extend(str(c).strip() for c in content if c)
    raw = " ".join(parts)
    cleaned = _tag_re.sub(" ", raw)
    return _ws_re.sub(" ", cleaned).strip()


def _ms_to_date(ms_timestamp) -> str:
    """Convert Lever's millisecond Unix timestamp to ISO date string."""
    if not ms_timestamp:
        return ""
    try:
        ts = int(ms_timestamp) / 1000
        return datetime.fromtimestamp(ts, tz=None).strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return ""


class LeverScraper(BaseScraper):
    """Scraper for Lever-hosted job boards."""

    platform_name = "lever"
    request_delay = 0.5

    def build_url(self, company_id: str) -> str:
        return f"https://api.lever.co/v0/postings/{company_id}?mode=json"

    def parse_jobs(self, raw_data, company_id: str) -> list[dict]:
        """
        Lever returns a flat JSON array, not { "jobs": [...] }.
        If the response is a dict (e.g. error), treat as empty.
        """
        # Handle both list (success) and dict (error/empty) responses
        if isinstance(raw_data, dict):
            # Sometimes Lever wraps in a dict on error
            job_list = raw_data.get("postings", []) or raw_data.get("jobs", [])
            if not job_list:
                return []
        elif isinstance(raw_data, list):
            job_list = raw_data
        else:
            return []

        jobs: list[dict] = []

        for j in job_list:
            source_job_id = str(j.get("id", "")).strip()
            if not source_job_id:
                continue

            title = (j.get("text") or "").strip()

            # Company name — Lever usually includes 'categories.team'
            # but the company display name is more reliable from the top level
            company = (j.get("company") or company_id).strip()
            if company == company_id:
                company = company_id.replace("-", " ").replace("_", " ").title()

            # Location
            categories = j.get("categories") or {}
            location = (categories.get("location") or "").strip()

            # Description — assembled from 'lists' blocks + 'descriptionPlain' fallback
            description = _extract_description(j.get("lists", []))
            if not description:
                description = (j.get("descriptionPlain") or j.get("description") or "").strip()

            # Apply URL — prefer hostedUrl (the direct page), fall back to applyUrl
            apply_url = (j.get("hostedUrl") or j.get("applyUrl") or "").strip()

            # Posted date — createdAt is millisecond Unix timestamp
            posted_date = _ms_to_date(j.get("createdAt"))

            # Employment type — sometimes in categories
            employment_type = (categories.get("commitment") or "").strip()

            # Team/department — useful context, stored in raw_json
            jobs.append(self.normalize_job(
                source_job_id=source_job_id,
                company_id=company_id,
                title=title,
                company=company,
                location=location,
                description=description,
                apply_url=apply_url,
                posted_date=posted_date,
                employment_type=employment_type,
                raw=j,
            ))

        return jobs

    def _fetch(self, company_id: str) -> dict:
        """
        Override _fetch because Lever returns a JSON array, not a dict.
        Our base class _fetch_with_retry calls resp.json() which works fine,
        but the return type hint says dict. We handle both in parse_jobs,
        so just call super and let it through.
        """
        url = self.build_url(company_id)
        return self._fetch_with_retry(url)


# ------------------------------------------------------------
# Self-test — hits the real Lever API.
# ------------------------------------------------------------

if __name__ == "__main__":
    TEST_COMPANIES = ["palantir"]

    print("=" * 60)
    print("LEVER SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = LeverScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")

    if len(jobs) == 0:
        print("WARNING: Zero jobs returned. Possible causes:")
        print("  - These companies may have switched ATS platforms")
        print("  - Lever API may be temporarily down")
        print("  - Company slugs may have changed")
        print("\nTry manually: https://api.lever.co/v0/postings/netflix?mode=json")
    else:
        # --- Structural checks ---
        from src.ingestion.base_scraper import REQUIRED_FIELDS
        sample = jobs[0]
        missing = [f for f in REQUIRED_FIELDS if f not in sample]
        assert not missing, f"missing fields: {missing}"
        assert sample["source_platform"] == "lever"
        assert len(sample["job_id"]) == 32

        # --- Preview first 5 jobs ---
        print("\nSample jobs:")
        for job in jobs[:5]:
            desc_preview = (job["description"][:80] + "...") if len(job["description"]) > 80 else job["description"]
            print(f"  {job['company']:15s} | {job['title'][:40]:40s} | "
                  f"{job['location'][:20]:20s} | seniority={job['seniority'] or '-':6s}")
            print(f"    apply: {job['apply_url']}")
            print(f"    desc:  {desc_preview}")
            print()

        # Seniority distribution
        from collections import Counter
        sen_counts = Counter(j["seniority"] or "(none)" for j in jobs)
        print(f"Seniority breakdown: {dict(sen_counts)}")

        print("\nAll checks passed.")