"""
TalentScope AI — Home Page (with Freshness + Coverage)
"""

import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import text
from src.database.connection import get_engine

st.set_page_config(
    page_title="TalentScope AI",
    page_icon="🎯",
    layout="wide",
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1B4F72;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #5D6D7E;
        margin-top: 0;
    }
    .freshness-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 1rem;
        font-size: 0.85rem;
        font-weight: 500;
        margin-bottom: 0.5rem;
    }
    .fresh { background: #D5F5E3; color: #1E8449; }
    .stale { background: #FADBD8; color: #C0392B; }
    .moderate { background: #FEF9E7; color: #B7950B; }
    .coverage-bar {
        background: #EBF5FB;
        padding: 0.6rem 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #2E86C1;
        font-size: 0.95rem;
        color: #2C3E50;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">🎯 TalentScope AI</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Real-Time Job Market Intelligence Platform</p>', unsafe_allow_html=True)

# Load metrics safely
try:
    engine = get_engine()
    with engine.connect() as conn:
        total_jobs = conn.execute(text("SELECT COUNT(*) FROM clean_jobs")).fetchone()[0]
        total_companies = conn.execute(text("SELECT COUNT(DISTINCT company) FROM clean_jobs WHERE company != ''")).fetchone()[0]
        with_salary = conn.execute(text("SELECT COUNT(*) FROM clean_jobs WHERE salary_min IS NOT NULL")).fetchone()[0]
        total_cities = conn.execute(text("SELECT COUNT(DISTINCT location_city) FROM clean_jobs WHERE location_city IS NOT NULL")).fetchone()[0]
        avg_salary = conn.execute(text("SELECT ROUND(AVG((salary_min + salary_max) / 2)) FROM clean_jobs WHERE salary_min IS NOT NULL")).fetchone()[0]
        top_skill = conn.execute(text("""
            SELECT skill FROM job_features, UNNEST(skills) AS skill
            GROUP BY skill ORDER BY COUNT(*) DESC LIMIT 1
        """)).fetchone()
        top_skill = top_skill[0] if top_skill else "N/A"

        # Freshness: last successful pipeline run
        last_run = conn.execute(text("""
            SELECT end_time FROM pipeline_runs
            WHERE status = 'success' AND end_time IS NOT NULL
            ORDER BY end_time DESC LIMIT 1
        """)).fetchone()

        # Data sources count
        source_count = conn.execute(text("SELECT COUNT(DISTINCT source) FROM clean_jobs")).fetchone()[0]

    db_connected = True
except Exception:
    db_connected = False
    total_jobs = total_companies = with_salary = total_cities = 0
    avg_salary = 0
    top_skill = "N/A"
    last_run = None
    source_count = 0

# Freshness badge
if last_run and last_run[0]:
    last_time = last_run[0]
    if last_time.tzinfo is None:
        last_time = last_time.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    hours_ago = (now - last_time).total_seconds() / 3600

    if hours_ago < 24:
        badge_class = "fresh"
        time_text = f"{int(hours_ago)} hours ago" if hours_ago >= 1 else "just now"
    elif hours_ago < 72:
        badge_class = "moderate"
        time_text = f"{int(hours_ago / 24)} days ago"
    else:
        badge_class = "stale"
        time_text = f"{int(hours_ago / 24)} days ago"

    st.markdown(
        f'<span class="freshness-badge {badge_class}">📡 Data updated {time_text}</span>',
        unsafe_allow_html=True,
    )

# Coverage bar
if db_connected and total_jobs > 0:
    st.markdown(
        f'<div class="coverage-bar">📊 Analyzing <strong>{total_jobs:,}</strong> jobs '
        f'across <strong>{total_cities:,}</strong> cities '
        f'from <strong>{source_count}</strong> data sources</div>',
        unsafe_allow_html=True,
    )

st.markdown("---")

# Key Metrics Row
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Jobs", f"{total_jobs:,}")
col2.metric("Companies", f"{total_companies:,}")
col3.metric("With Salary", f"{with_salary:,}")
col4.metric("Cities", f"{total_cities:,}")
col5.metric("Avg Salary", f"${avg_salary:,}" if avg_salary else "—")
col6.metric("Top Skill", top_skill.upper())

st.markdown("---")

# Two-column layout
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("What TalentScope AI Does")
    st.markdown("""
    TalentScope AI transforms thousands of fragmented job postings into clear,
    actionable career insights — helping you make smarter decisions about
    **what to learn**, **where to apply**, and **what to negotiate**.

    **Navigate the platform using the sidebar:**

    📊 **Market Dashboard** — Skill demand, hiring trends, salary patterns, top companies

    💰 **Salary Predictor** — ML-powered salary estimates with confidence intervals

    🧠 **Skill Gap Analyzer** — Compare your skills against market demands

    🗺️ **Job Clusters** — Discover role groupings via NLP clustering

    ⚙️ **Pipeline Monitor** — Data freshness and pipeline health metrics
    """)

with col2:
    st.subheader("Quick Stats")
    if db_connected:
        # Role distribution mini chart
        try:
            with engine.connect() as conn:
                df_roles = pd.read_sql(text("""
                    SELECT title_category, COUNT(*) as count
                    FROM job_features
                    GROUP BY title_category
                    ORDER BY count DESC
                    LIMIT 6
                """), conn)

            if not df_roles.empty:
                fig = px.pie(
                    df_roles, values="count", names="title_category",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                    hole=0.5,
                )
                fig.update_traces(textposition="inside", textinfo="percent")
                fig.update_layout(
                    height=300, showlegend=True,
                    legend=dict(font=dict(size=10)),
                    margin=dict(t=10, b=10, l=10, r=10),
                )
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.info("Charts loading...")
    else:
        st.warning("Database not connected")

# Bottom section — tech stack
st.markdown("---")
st.subheader("Built With")

tech_cols = st.columns(5)
tech_items = [
    ("🐍 Python", "Core language"),
    ("🐘 PostgreSQL", "Data storage"),
    ("📊 Streamlit", "Dashboard"),
    ("🤖 scikit-learn", "ML models"),
    ("📤 NLP", "Skill analysis"),
]
for col, (tech, desc) in zip(tech_cols, tech_items):
    col.markdown(f"**{tech}**")
    col.caption(desc)