"""
TalentScope AI — Pipeline Monitor
"""

import streamlit as st
import plotly.express as px
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine
from src.analysis.sql_analytics import pipeline_health

st.set_page_config(page_title="Pipeline Monitor | TalentScope AI", layout="wide")
st.title("⚙️ Pipeline Monitor")
st.markdown("Track data freshness, pipeline health, and ingestion metrics")
st.markdown("---")

engine = get_engine()

# Key metrics
with engine.connect() as conn:
    raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_jobs")).fetchone()[0]
    clean_count = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
    latest_run = conn.execute(text(
        "SELECT MAX(end_time) FROM pipeline_runs WHERE status = 'success'"
    )).fetchone()[0]
    total_runs = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).fetchone()[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Raw Jobs", f"{raw_count:,}")
col2.metric("Clean Jobs", f"{clean_count:,}")
col3.metric("Total Pipeline Runs", total_runs)
col4.metric("Last Successful Run", str(latest_run)[:19] if latest_run else "Never")

st.markdown("---")

# Pipeline health table
st.subheader("📊 Pipeline Run Summary")
df_health = pipeline_health()
st.dataframe(df_health, use_container_width=True, hide_index=True)

# Source breakdown
st.markdown("---")
st.subheader("📦 Data Source Breakdown")

with engine.connect() as conn:
    df_source = pd.read_sql(text(
        "SELECT source, COUNT(*) as count FROM raw_jobs GROUP BY source"
    ), conn)

col1, col2 = st.columns(2)
with col1:
    fig = px.pie(df_source, values="count", names="source",
                 title="Raw Jobs by Source",
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    with engine.connect() as conn:
        df_exp = pd.read_sql(text(
            "SELECT experience_level, COUNT(*) as count FROM clean_jobs GROUP BY experience_level"
        ), conn)
    fig = px.pie(df_exp, values="count", names="experience_level",
                 title="Jobs by Experience Level",
                 color_discrete_sequence=px.colors.qualitative.Set3)
    st.plotly_chart(fig, use_container_width=True)

# Recent runs
st.markdown("---")
st.subheader("🕐 Recent Pipeline Runs")
with engine.connect() as conn:
    df_runs = pd.read_sql(text("""
        SELECT pipeline_name, source, status, records_processed,
               start_time, end_time,
               ROUND(EXTRACT(EPOCH FROM (end_time - start_time))::NUMERIC, 1) AS duration_sec
        FROM pipeline_runs
        ORDER BY start_time DESC
        LIMIT 20
    """), conn)
st.dataframe(df_runs, use_container_width=True, hide_index=True)