"""TalentScope AI — Clean Jobs Quality Audit"""

from sqlalchemy import text
from src.database.connection import get_engine

e = get_engine()
with e.connect() as c:
    print("=== CLEAN JOBS QUALITY AUDIT ===\n")

    # 1. Total counts
    total = c.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
    print(f"Total clean_jobs: {total}")

    # 2. Source breakdown
    print("\n--- By Source ---")
    rows = c.execute(text("SELECT source, COUNT(*) FROM clean_jobs GROUP BY source")).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]}")

    # 3. Salary coverage
    sal = c.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL")).fetchone()[0]
    print(f"\n--- Salary Coverage ---")
    print(f"  With salary: {sal} ({sal/total*100:.1f}%)")
    print(f"  Without:     {total-sal} ({(total-sal)/total*100:.1f}%)")

    # 4. Salary sanity
    print("\n--- Salary Stats ---")
    r = c.execute(text("""
        SELECT MIN(salary_min), AVG(salary_min)::INT, MAX(salary_min),
               MIN(salary_max), AVG(salary_max)::INT, MAX(salary_max)
        FROM clean_jobs WHERE salary_min IS NOT NULL
    """)).fetchone()
    print(f"  Min salary_min: ${r[0]:,.0f}")
    print(f"  Avg salary_min: ${r[1]:,.0f}")
    print(f"  Max salary_min: ${r[2]:,.0f}")
    print(f"  Min salary_max: ${r[3]:,.0f}")
    print(f"  Avg salary_max: ${r[4]:,.0f}")
    print(f"  Max salary_max: ${r[5]:,.0f}")

    # 5. Experience level distribution
    print("\n--- Experience Level ---")
    rows = c.execute(text("""
        SELECT experience_level, COUNT(*) 
        FROM clean_jobs GROUP BY experience_level 
        ORDER BY COUNT(*) DESC
    """)).fetchall()
    for r in rows:
        print(f"  {str(r[0]):10s}: {r[1]} ({r[1]/total*100:.1f}%)")

    # 6. Location coverage
    city = c.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE location_city IS NOT NULL")).fetchone()[0]
    state = c.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE location_state IS NOT NULL")).fetchone()[0]
    remote = c.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE is_remote = TRUE")).fetchone()[0]
    print(f"\n--- Location Coverage ---")
    print(f"  With city:   {city} ({city/total*100:.1f}%)")
    print(f"  With state:  {state} ({state/total*100:.1f}%)")
    print(f"  Remote:      {remote} ({remote/total*100:.1f}%)")

    # 7. Top cities
    print("\n--- Top 10 Cities ---")
    rows = c.execute(text("""
        SELECT location_city, COUNT(*) FROM clean_jobs 
        WHERE location_city IS NOT NULL 
        GROUP BY location_city ORDER BY COUNT(*) DESC LIMIT 10
    """)).fetchall()
    for r in rows:
        print(f"  {str(r[0]):30s}: {r[1]}")

    # 8. Null checks
    print("\n--- Null Checks ---")
    for col in ["title", "company", "description"]:
        n = c.execute(text(
            f"SELECT COUNT(*) FROM clean_jobs WHERE {col} IS NULL OR {col} = ''"
        )).fetchone()[0]
        print(f"  {col} empty: {n}")

    # 9. Description length
    r = c.execute(text("""
        SELECT MIN(LENGTH(description)), AVG(LENGTH(description))::INT, 
               MAX(LENGTH(description))
        FROM clean_jobs WHERE description IS NOT NULL AND description != ''
    """)).fetchone()
    print(f"\n--- Description Length ---")
    print(f"  Min: {r[0]} chars")
    print(f"  Avg: {r[1]} chars")
    print(f"  Max: {r[2]} chars")

    # 10. Posted date coverage
    dated = c.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE posted_date IS NOT NULL")).fetchone()[0]
    print(f"\n--- Posted Date ---")
    print(f"  With date: {dated} ({dated/total*100:.1f}%)")
    if dated > 0:
        r = c.execute(text("SELECT MIN(posted_date), MAX(posted_date) FROM clean_jobs WHERE posted_date IS NOT NULL")).fetchone()
        print(f"  Range: {r[0]} to {r[1]}")