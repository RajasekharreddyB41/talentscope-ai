"""
TalentScope AI — Greenhouse ATS scraper.

Public JSON API, no auth required. Used by Airbnb, Stripe, Pinterest,
DoorDash, Instacart, Robinhood, Figma, and hundreds of top-tier tech companies.

Endpoint shape:
    https://boards-api.greenhouse.io/v1/boards/{company_id}/jobs?content=true

Docs: https://developers.greenhouse.io/job-board.html
"""

import html
import re
from typing import Optional

from src.ingestion.base_scraper import BaseScraper


# Strip HTML tags but preserve whitespace between block elements.
# Greenhouse returns descriptions as HTML-escaped markup.
_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


# A small curated seed list of well-known Greenhouse-hosted companies.
# Used only by the self-test and as a starting point for Step 15's pipeline.
# Full company list is maintained elsewhere (e.g. config/scraper_companies.yml
# in a future step). Keep this short and safe.
DEFAULT_COMPANIES = [
    "airbnb",
    "stripe",
    "pinterest",
    "doordash",
    "instacart",
    "robinhood",
    "figma",
    "gitlab",
]


def _clean_html(raw: str) -> str:
    """Turn Greenhouse's HTML-escaped description into clean plain text."""
    if not raw:
        return ""
    unescaped = html.unescape(raw)            # &lt;p&gt; -> <p>
    no_tags = _TAG_RE.sub(" ", unescaped)     # <p>hi</p> -> "  hi  "
    return _WHITESPACE_RE.sub(" ", no_tags).strip()


def _first_metadata_value(metadata: list, target_keys: tuple) -> str:
    """
    Greenhouse exposes custom metadata as a list of {name, value} objects.
    Safely pull the first value whose name matches any target key (case-insensitive).
    """
    if not metadata:
        return ""
    targets = {k.lower() for k in target_keys}
    for item in metadata:
        name = (item.get("name") or "").lower()
        if name in targets:
            val = item.get("value")
            if isinstance(val, str):
                return val.strip()
            if isinstance(val, list) and val:
                return str(val[0]).strip()
    return ""


class GreenhouseScraper(BaseScraper):
    """Scraper for Greenhouse-hosted job boards."""

    platform_name = "greenhouse"
    request_delay = 0.5  # polite between companies

    def build_url(self, company_id: str) -> str:
        return f"https://boards-api.greenhouse.io/v1/boards/{company_id}/jobs?content=true"

    def parse_jobs(self, raw_data: dict, company_id: str) -> list[dict]:
        jobs: list[dict] = []
        company_display = self._extract_company_name(raw_data, company_id)

        for j in raw_data.get("jobs", []):
            source_job_id = str(j.get("id", "")).strip()
            if not source_job_id:
                continue  # skip malformed entries

            title = (j.get("title") or "").strip()
            description = _clean_html(j.get("content", ""))
            apply_url = (j.get("absolute_url") or "").strip()

            location = (j.get("location") or {}).get("name", "") or ""

            # posted_date: prefer updated_at, fall back to first_published
            posted_raw = j.get("updated_at") or j.get("first_published") or ""
            posted_date = posted_raw[:10] if posted_raw else ""

            # employment_type sometimes shows up in metadata
            employment_type = _first_metadata_value(
                j.get("metadata"),
                target_keys=("employment type", "type", "employment"),
            )

            jobs.append(self.normalize_job(
                source_job_id=source_job_id,
                company_id=company_id,
                title=title,
                company=company_display,
                location=location,
                description=description,
                apply_url=apply_url,
                posted_date=posted_date,
                employment_type=employment_type,
                raw=j,
            ))

        return jobs

    @staticmethod
    def _extract_company_name(raw_data: dict, company_id: str) -> str:
        """
        Greenhouse doesn't always expose a company display name in the jobs feed.
        Fall back to a title-cased company_id (e.g. 'airbnb' -> 'Airbnb').
        """
        # Some boards include it under meta — check defensively.
        meta = raw_data.get("meta") or {}
        name = meta.get("name") or meta.get("company_name")
        if isinstance(name, str) and name.strip():
            return name.strip()
        return company_id.replace("-", " ").replace("_", " ").title()


# ------------------------------------------------------------
# Self-test — hits the real Greenhouse API with a small company list.
# ------------------------------------------------------------

if __name__ == "__main__":
    # Use a tiny list for the self-test so we're not hammering the API.
    TEST_COMPANIES = ["airbnb", "stripe"]

    print("=" * 60)
    print("GREENHOUSE SCRAPER SELF-TEST")
    print("=" * 60)
    print(f"Testing against: {TEST_COMPANIES}\n")

    scraper = GreenhouseScraper()
    jobs = scraper.scrape(TEST_COMPANIES)

    print(f"\nTotal jobs fetched: {len(jobs)}")
    assert len(jobs) > 0, (
        "expected at least one job — if this fails, Greenhouse may be down "
        "or the test company slugs have changed."
    )

    # --- Structural checks ---
    from src.ingestion.base_scraper import REQUIRED_FIELDS
    sample = jobs[0]
    missing = [f for f in REQUIRED_FIELDS if f not in sample]
    assert not missing, f"missing fields: {missing}"
    assert sample["source_platform"] == "greenhouse"
    assert len(sample["job_id"]) == 32
    assert sample["apply_url"].startswith("http"), (
        f"expected apply_url to start with http, got: {sample['apply_url']}"
    )

    # --- HTML cleanup spot-check ---
    assert "<" not in sample["description"], (
        "description still contains HTML tags — _clean_html broke"
    )
    assert "&lt;" not in sample["description"], (
        "description still contains HTML entities — html.unescape broke"
    )

    # --- Deterministic job_id: re-hash and confirm ---
    recomputed = scraper._make_job_id(
        sample["raw_json"].get("id", "") and
        [c for c in TEST_COMPANIES if True][0],  # placeholder, overwritten below
        str(sample["raw_json"].get("id")),
    )
    # Proper deterministic check — find which company_id this job came from
    for cid in TEST_COMPANIES:
        candidate = scraper._make_job_id(cid, str(sample["raw_json"].get("id")))
        if candidate == sample["job_id"]:
            recomputed = candidate
            break
    assert recomputed == sample["job_id"], "job_id is not deterministic"

    # --- Preview first 5 jobs ---
    print("\nSample jobs:")
    for job in jobs[:5]:
        desc_preview = (job["description"][:80] + "...") if len(job["description"]) > 80 else job["description"]
        print(f"  {job['company']:12s} | {job['title'][:40]:40s} | "
              f"{job['location'][:20]:20s} | seniority={job['seniority'] or '-':6s}")
        print(f"    apply: {job['apply_url']}")
        print(f"    desc:  {desc_preview}")
        print()

    # Seniority distribution
    from collections import Counter
    sen_counts = Counter(j["seniority"] or "(none)" for j in jobs)
    print(f"Seniority breakdown: {dict(sen_counts)}")

    print("\nAll assertions passed.")