"""
TalentScope AI — Sync local data to Supabase production
Full clear + reload approach. No duplicates possible.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# Local database
LOCAL_URL = "postgresql://talentscope:talentscope123@127.0.0.1:5432/talentscope_db"

# Supabase — replace YOUR_PASSWORD with your actual Supabase password
SUPA_PASSWORD = quote_plus("Rajasekhar06")
SUPA_URL = f"postgresql://postgres.fvaiszwssclfhldaqvfi:{SUPA_PASSWORD}@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

local_engine = create_engine(LOCAL_URL)
supa_engine = create_engine(SUPA_URL)


def get_counts(engine, label):
    print(f"\n  {label}:")
    for table in ["clean_jobs", "job_features", "pipeline_runs"]:
        try:
            count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table}", engine).iloc[0]["cnt"]
            print(f"    {table}: {count:,}")
        except Exception as e:
            print(f"    {table}: error - {e}")


def upload_chunked(df, table_name, engine, chunk_size=25):
    """Upload dataframe in small chunks, skip failed chunks row-by-row."""
    loaded = 0
    failed = 0
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
            loaded += len(chunk)
        except Exception:
            # If chunk fails, try row by row
            for _, row in chunk.iterrows():
                try:
                    row.to_frame().T.to_sql(table_name, engine, if_exists="append", index=False)
                    loaded += 1
                except Exception:
                    failed += 1
    return loaded, failed


def sync():
    print(f"\n{'='*50}")
    print(f"  TalentScope AI — Sync to Supabase")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    get_counts(local_engine, "LOCAL (source)")
    get_counts(supa_engine, "SUPABASE (before sync)")

    # Step 1: FULL CLEAR of Supabase tables (order matters for foreign keys)
    print("\n  Step 1: Clearing ALL Supabase data...")
    with supa_engine.connect() as conn:
        conn.execute(text("TRUNCATE job_features, clean_jobs, pipeline_runs RESTART IDENTITY CASCADE"))
        conn.commit()
    print("  Cleared all tables.")

    # Step 2: Read local clean_jobs and deduplicate
    print("\n  Step 2: Syncing clean_jobs...")
    df_clean = pd.read_sql("SELECT * FROM clean_jobs", local_engine)
    if "id" in df_clean.columns:
        df_clean = df_clean.drop(columns=["id"])

    before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["dedup_hash"], keep="first")
    dupes_removed = before - len(df_clean)
    if dupes_removed > 0:
        print(f"    Removed {dupes_removed} local duplicates")

    loaded, failed = upload_chunked(df_clean, "clean_jobs", supa_engine)
    print(f"    Loaded {loaded:,}/{len(df_clean):,} rows (failed: {failed})")

    # Step 3: Build ID mapping (local -> supabase) via dedup_hash
    print("\n  Step 3: Building ID mapping...")
    local_ids = pd.read_sql("SELECT id, dedup_hash FROM clean_jobs", local_engine)
    supa_ids = pd.read_sql("SELECT id, dedup_hash FROM clean_jobs", supa_engine)
    merged = local_ids.merge(supa_ids, on="dedup_hash", suffixes=("_local", "_supa"))
    id_map = dict(zip(merged["id_local"], merged["id_supa"]))
    print(f"    Mapped {len(id_map):,} IDs")

    # Step 4: Sync job_features with remapped IDs
    print("\n  Step 4: Syncing job_features...")
    df_feat = pd.read_sql("SELECT * FROM job_features", local_engine)
    if "id" in df_feat.columns:
        df_feat = df_feat.drop(columns=["id"])

    # Deduplicate features by clean_job_id (keep first)
    df_feat = df_feat.drop_duplicates(subset=["clean_job_id"], keep="first")

    # Remap clean_job_id from local to supabase
    df_feat["clean_job_id"] = df_feat["clean_job_id"].map(id_map)
    df_feat = df_feat.dropna(subset=["clean_job_id"])
    df_feat["clean_job_id"] = df_feat["clean_job_id"].astype(int)

    loaded, failed = upload_chunked(df_feat, "job_features", supa_engine)
    print(f"    Loaded {loaded:,}/{len(df_feat):,} rows (failed: {failed})")

    # Step 5: Sync pipeline_runs
    print("\n  Step 5: Syncing pipeline_runs...")
    df_runs = pd.read_sql("SELECT * FROM pipeline_runs", local_engine)
    if "run_id" in df_runs.columns:
        df_runs = df_runs.drop(columns=["run_id"])

    loaded, failed = upload_chunked(df_runs, "pipeline_runs", supa_engine)
    print(f"    Loaded {loaded:,}/{len(df_runs):,} rows (failed: {failed})")

    # Final verification
    get_counts(supa_engine, "SUPABASE (after sync)")

    print(f"\n{'='*50}")
    print(f"  SYNC COMPLETE")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    sync()