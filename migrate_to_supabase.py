import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

SUPA_PASSWORD = quote_plus("Rajasekhar@06")
SUPA_URL = f"postgresql://postgres:{SUPA_PASSWORD}@db.fvaiszwssclfhldaqvfi.supabase.co:5432/postgres"

supa_engine = create_engine(SUPA_URL)

# Fix job_features duplicates
with supa_engine.connect() as conn:
    # Remove duplicate job_features, keep lowest id
    conn.execute(text("""
        DELETE FROM job_features a USING job_features b
        WHERE a.id > b.id AND a.clean_job_id = b.clean_job_id
    """))
    conn.commit()
    print("Duplicates removed")

# Verify final counts
for table in ["clean_jobs", "job_features", "pipeline_runs"]:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", supa_engine).iloc[0]["cnt"]
    print(f"  {table}: {count} rows")