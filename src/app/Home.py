"""
TalentScope AI — Home Page
"""

import streamlit as st
from sqlalchemy import text
from src.database.connection import get_engine

st.set_page_config(
    page_title="TalentScope AI",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 TalentScope AI")
st.markdown("**Real-Time Job Market Intelligence Platform**")
st.markdown("---")

# Key metrics
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

st.markdown("---")

st.markdown("""
### What TalentScope AI Does

TalentScope AI transforms thousands of fragmented job postings into clear, actionable career insights 
— helping you make smarter decisions about **what to learn**, **where to apply**, and **what to negotiate**.

### Navigate the Platform

- 📊 **Market Dashboard** — Skill demand, hiring trends, top companies
- 💰 **Salary Predictor** — ML-powered salary estimates by role & location
- 🧠 **Skill Gap Analyzer** — Compare your resume against market demands
- 🗺️ **Job Clusters** — Discover emerging role categories via NLP
- ⚙️ **Pipeline Monitor** — Data freshness and pipeline health
""")

# Recent data snapshot
st.markdown("---")
st.subheader("📈 Quick Snapshot")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Top Roles by Job Count**")
    with engine.connect() as conn:
        df = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN LOWER(title) LIKE '%%data engineer%%' THEN 'Data Engineer'
                    WHEN LOWER(title) LIKE '%%data analyst%%' THEN 'Data Analyst'
                    WHEN LOWER(title) LIKE '%%data scientist%%' THEN 'Data Scientist'
                    WHEN LOWER(title) LIKE '%%machine learning%%' THEN 'ML Engineer'
                    WHEN LOWER(title) LIKE '%%software engineer%%' THEN 'Software Engineer'
                    WHEN LOWER(title) LIKE '%%python%%' THEN 'Python Developer'
                    ELSE 'Other'
                END AS role,
                COUNT(*) AS count
            FROM clean_jobs
            GROUP BY role
            ORDER BY count DESC
            LIMIT 6
        """)).fetchall()
        for row in df:
            st.markdown(f"- **{row[0]}**: {row[1]} jobs")

with col2:
    st.markdown("**Avg Salary by Experience**")
    with engine.connect() as conn:
        df = conn.execute(text("""
            SELECT experience_level, 
                   ROUND(AVG((COALESCE(salary_min,0)+COALESCE(salary_max,0))/2)) AS avg_salary
            FROM clean_jobs
            WHERE salary_min IS NOT NULL
            GROUP BY experience_level
            ORDER BY avg_salary DESC
        """)).fetchall()
        for row in df:
            st.markdown(f"- **{row[0].title()}**: ${row[1]:,.0f}")