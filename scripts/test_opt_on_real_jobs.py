"""One-time test: run OPT classifier on real scraped jobs."""

from src.ingestion.greenhouse_scraper import GreenhouseScraper, DEFAULT_COMPANIES as GH_COMPANIES
from src.ingestion.lever_scraper import LeverScraper, DEFAULT_COMPANIES as LV_COMPANIES
from src.analysis.opt_classifier import classify_jobs_batch, load_h1b_data
from collections import Counter

print("Loading H-1B employer data...")
h1b_data = load_h1b_data()

print("Scraping Greenhouse...")
gh = GreenhouseScraper()
gh_jobs = gh.scrape(GH_COMPANIES)

print("Scraping Lever...")
lv = LeverScraper()
lv_jobs = lv.scrape(LV_COMPANIES)

all_jobs = gh_jobs + lv_jobs
print(f"\nTotal jobs scraped: {len(all_jobs)}")

print("Classifying OPT status...")
classify_jobs_batch(all_jobs, h1b_data)

status_counts = Counter(j["opt_status"] for j in all_jobs)
tier_counts = Counter(j["sponsor_tier"] for j in all_jobs)
conf_counts = Counter(j["opt_confidence"] for j in all_jobs)

print(f"\n{'=' * 60}")
print("OPT STATUS DISTRIBUTION")
print("=" * 60)
for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
    pct = count / len(all_jobs) * 100
    bar = "#" * int(pct)
    print(f"  {status:20s}: {count:5d} ({pct:5.1f}%) {bar}")

print(f"\nSPONSOR TIER DISTRIBUTION")
for tier, count in sorted(tier_counts.items(), key=lambda x: -x[1]):
    pct = count / len(all_jobs) * 100
    print(f"  {tier:20s}: {count:5d} ({pct:5.1f}%)")

print(f"\nCONFIDENCE DISTRIBUTION")
for conf, count in sorted(conf_counts.items(), key=lambda x: -x[1]):
    pct = count / len(all_jobs) * 100
    print(f"  {conf:20s}: {count:5d} ({pct:5.1f}%)")

# Samples
print(f"\nSAMPLE OPT-FRIENDLY JOBS:")
friendly = [j for j in all_jobs if j["opt_status"] == "opt_friendly"][:5]
for j in friendly:
    print(f"  {j['company']:15s} | {j['title'][:40]:40s} | "
          f"tier={j['sponsor_tier']:6s} | approvals={j['h1b_approvals']}")

not_friendly = [j for j in all_jobs if j["opt_status"] == "not_opt_friendly"][:5]
if not_friendly:
    print(f"\nSAMPLE NOT-OPT-FRIENDLY JOBS:")
    for j in not_friendly:
        signals = j["opt_signals"][:2]
        print(f"  {j['company']:15s} | {j['title'][:40]:40s} | signals={signals}")

unclear = [j for j in all_jobs if j["opt_status"] == "opt_unclear"][:5]
if unclear:
    print(f"\nSAMPLE UNCLEAR JOBS:")
    for j in unclear:
        signals = j["opt_signals"][:2]
        print(f"  {j['company']:15s} | {j['title'][:40]:40s} | signals={signals}")

unknown = [j for j in all_jobs if j["opt_status"] == "unknown"][:3]
if unknown:
    print(f"\nSAMPLE UNKNOWN JOBS:")
    for j in unknown:
        print(f"  {j['company']:15s} | {j['title'][:40]:40s} | "
              f"tier={j['sponsor_tier']:6s}")

print(f"\n{'=' * 60}")
print("Done.")