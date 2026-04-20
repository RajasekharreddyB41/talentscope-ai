"""
One-time backfill: classify all existing clean_jobs for OPT status.
Reads description + company, runs the 3-signal classifier, writes back in batch.
"""

import time
from sqlalchemy import text
from src.database.connection import get_engine
from src.analysis.opt_classifier import classify_opt, load_h1b_data
from src.utils.logger import get_logger

logger = get_logger("scripts.backfill_opt")

BATCH_SIZE = 500


def backfill():
    print("=" * 60)
    print("OPT BACKFILL — classifying all clean_jobs")
    print("=" * 60)

    print("\nLoading H-1B employer data...")
    h1b_data = load_h1b_data()

    engine = get_engine()

    # Read all jobs that need classification
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, description, company
            FROM clean_jobs
            ORDER BY id
        """)).fetchall()

    total = len(rows)
    print(f"Jobs to classify: {total}")

    if total == 0:
        print("Nothing to do.")
        return

    # Classify in memory
    updates = []
    started = time.time()

    for i, row in enumerate(rows):
        job_id = row[0]
        description = row[1] or ""
        company = row[2] or ""

        result = classify_opt(description, company, h1b_data)

        updates.append({
            "job_id": job_id,
            "opt_status": result["opt_status"],
            "opt_signals": result["opt_signals"],
            "opt_confidence": result["confidence"],
            "h1b_sponsorship": result["h1b_sponsorship"],
            "sponsor_tier": result["sponsor_tier"],
            "h1b_approvals": result["h1b_approvals"],
        })

        if (i + 1) % 1000 == 0:
            elapsed = time.time() - started
            rate = (i + 1) / elapsed
            print(f"  Classified {i + 1:,}/{total:,} ({rate:.0f} jobs/sec)")

    classify_time = time.time() - started
    print(f"\nClassification done: {total:,} jobs in {classify_time:.1f}s")

    # Write back in batches
    print(f"\nWriting to database (batch size {BATCH_SIZE})...")
    write_start = time.time()
    written = 0

    with engine.connect() as conn:
        for batch_start in range(0, len(updates), BATCH_SIZE):
            batch = updates[batch_start:batch_start + BATCH_SIZE]

            for item in batch:
                conn.execute(
                    text("""
                        UPDATE clean_jobs
                        SET opt_status = :opt_status,
                            opt_signals = :opt_signals,
                            opt_confidence = :opt_confidence,
                            h1b_sponsorship = :h1b_sponsorship,
                            sponsor_tier = :sponsor_tier,
                            h1b_approvals = :h1b_approvals
                        WHERE id = :job_id
                    """),
                    {
                        "job_id": item["job_id"],
                        "opt_status": item["opt_status"],
                        "opt_signals": item["opt_signals"],
                        "opt_confidence": item["opt_confidence"],
                        "h1b_sponsorship": item["h1b_sponsorship"],
                        "sponsor_tier": item["sponsor_tier"],
                        "h1b_approvals": item["h1b_approvals"],
                    },
                )

            conn.commit()
            written += len(batch)
            print(f"  Written {written:,}/{total:,}")

    write_time = time.time() - write_start
    print(f"\nDatabase update done: {written:,} rows in {write_time:.1f}s")

    # Verify with distribution
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT opt_status, COUNT(*) as cnt
            FROM clean_jobs
            GROUP BY opt_status
            ORDER BY cnt DESC
        """))
        print(f"\n{'=' * 60}")
        print("OPT STATUS DISTRIBUTION (from database)")
        print("=" * 60)
        for row in result:
            print(f"  {row[0]:20s}: {row[1]:,}")

        result2 = conn.execute(text("""
            SELECT sponsor_tier, COUNT(*) as cnt
            FROM clean_jobs
            GROUP BY sponsor_tier
            ORDER BY cnt DESC
        """))
        print(f"\nSPONSOR TIER DISTRIBUTION")
        for row in result2:
            print(f"  {row[0]:20s}: {row[1]:,}")

    total_time = time.time() - started
    print(f"\nTotal time: {total_time:.1f}s")
    print("Done.")


if __name__ == "__main__":
    backfill()