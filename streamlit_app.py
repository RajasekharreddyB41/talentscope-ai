"""
TalentScope AI — Streamlit Cloud Entry Point
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import streamlit as st

# TEMP DEBUG
st.sidebar.write("DB URL:", os.getenv("DATABASE_URL", "NOT SET")[:35] if os.getenv("DATABASE_URL") else "NOT SET")

st.set_page_config(
    page_title="TalentScope AI",
    page_icon="🎯",
    layout="wide",
)

from src.database.connection import get_engine
from sqlalchemy import text

st.title("🎯 TalentScope AI")
st.markdown("**Real-Time Job Market Intelligence Platform**")
st.markdown("---")

# Safe database connection
try:
    engine = get_engine()
    with engine.connect() as conn:
        total_jobs = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
        total_companies = conn.execute(text("SELECT COUNT(DISTINCT company) FROM clean_jobs WHERE company != ''")).fetchone()[0]
        with_salary = conn.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL")).fetchone()[0]
        total_cities = conn.execute(text("SELECT COUNT(DISTINCT location_city) FROM clean_jobs WHERE location_city IS NOT NULL")).fetchone()[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Jobs", f"{total_jobs:,}")
    col2.metric("Companies", f"{total_companies:,}")
    col3.metric("With Salary Data", f"{with_salary:,}")
    col4.metric("Cities", f"{total_cities:,}")

except Exception as e:
    st.warning("⚠️ Database not connected yet — app is live but data is loading.")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Jobs", "—")
    col2.metric("Companies", "—")
    col3.metric("With Salary Data", "—")
    col4.metric("Cities", "—")

st.markdown("---")
st.markdown("""
### What TalentScope AI Does
Transforms thousands of fragmented job postings into clear, actionable career insights.

### Navigate the Platform
- 📊 **Market Dashboard** — Skill demand, hiring trends, top companies
- 💰 **Salary Predictor** — ML-powered salary estimates by role & location
- 🧠 **Skill Gap Analyzer** — Compare your resume against market demands
- 🗺️ **Job Clusters** — Discover emerging role categories via NLP
- ⚙️ **Pipeline Monitor** — Data freshness and pipeline health
""")