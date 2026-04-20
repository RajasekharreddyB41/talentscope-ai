from src.database.connection import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'clean_jobs'
        ORDER BY ordinal_position
    """))
    print("clean_jobs columns:")
    for row in result:
        print(f"  {row[0]:30s} {row[1]}")

    # Also check how many jobs have apply URLs
    count = conn.execute(text("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN source = 'jsearch' THEN 1 END) as jsearch,
            COUNT(CASE WHEN source = 'adzuna' THEN 1 END) as adzuna,
            COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as has_desc
        FROM clean_jobs
    """)).fetchone()
    print(f"\nJob counts:")
    print(f"  Total:           {count[0]}")
    print(f"  JSearch:         {count[1]}")
    print(f"  Adzuna:          {count[2]}")
    print(f"  Has description: {count[3]}")