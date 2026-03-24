import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

LOCAL_URL = "postgresql://talentscope:talentscope123@127.0.0.1:5432/talentscope_db"

SUPA_PASSWORD = quote_plus("Rajasekhar06")
SUPA_URL = f"postgresql://postgres:{SUPA_PASSWORD}@db.fvaiszwssclfhldaqvfi.supabase.co:5432/postgres"

local_engine = create_engine(LOCAL_URL)
supa_engine = create_engine(SUPA_URL)

# Clear existing data first
print("Clearing Supabase tables...")
with supa_engine.connect() as conn:
    conn.execute(text("DELETE FROM job_features"))
    conn.execute(text("DELETE FROM clean_jobs"))
    conn.execute(text("DELETE FROM pipeline_runs"))
    conn.commit()
print("Cleared!")

tables = ["clean_jobs", "job_features", "pipeline_runs"]

for table in tables:
    print(f"\nMigrating {table}...")
    df = pd.read_sql(f"SELECT * FROM {table}", local_engine)
    print(f"  Read {len(df)} rows")

    if df.empty:
        continue

    for col in ["id", "run_id"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    if "skills" in df.columns:
        df["skills"] = df["skills"].apply(lambda x: x if isinstance(x, list) else [])

    chunk_size = 25
    total = len(df)
    loaded = 0

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table, supa_engine, if_exists="append", index=False, method="multi")
            loaded += len(chunk)
            if loaded % 250 == 0 or loaded == total:
                print(f"  Progress: {loaded}/{total}")
        except Exception as e:
            print(f"  Error at row {start}: {str(e)[:80]}")

    print(f"  Done: {loaded}/{total}")

print("\nVerifying...")
for table in tables:
    count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", supa_engine).iloc[0]["cnt"]
    print(f"  {table}: {count} rows")