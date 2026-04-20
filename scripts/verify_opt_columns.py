from src.database.connection import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'clean_jobs'
          AND (column_name LIKE 'opt%'
               OR column_name LIKE 'h1b%'
               OR column_name LIKE 'sponsor%')
        ORDER BY column_name
    """))
    print("New OPT columns in clean_jobs:")
    for row in result:
        print(f"  {row[0]:20s} {row[1]}")