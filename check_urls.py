from src.database.connection import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    total = conn.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE url IS NOT NULL AND url != ''")).fetchone()[0]
    no_url = conn.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE url IS NULL OR url = ''")).fetchone()[0]

    print(f"Jobs WITH apply URL: {total}")
    print(f"Jobs WITHOUT apply URL: {no_url}")

    rows = conn.execute(text("SELECT company, url, source FROM clean_jobs WHERE url IS NOT NULL AND url != '' ORDER BY posted_date DESC NULLS LAST LIMIT 20")).fetchall()

    print("\n--- Sample URLs ---\n")
    for r in rows:
        print(f"[{r[2]}] {r[0][:25]:25s} | {r[1][:80]}")