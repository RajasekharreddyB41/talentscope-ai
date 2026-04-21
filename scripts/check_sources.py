from src.database.connection import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT 
            source,
            COUNT(*) as job_count,
            COUNT(CASE WHEN url IS NOT NULL AND url != '' THEN 1 END) as with_url,
            COUNT(DISTINCT company) as companies
        FROM clean_jobs
        GROUP BY source
        ORDER BY job_count DESC
    """))
    
    print("Source breakdown in clean_jobs:")
    print("=" * 70)
    for row in result:
        pct = row[1] / 7411 * 100
        url_pct = row[2] / row[1] * 100 if row[1] > 0 else 0
        print(f"{row[0]:20s} | {row[1]:5d} jobs ({pct:5.1f}%) | {row[2]:5d} URLs ({url_pct:5.1f}%) | {row[3]:4d} companies")