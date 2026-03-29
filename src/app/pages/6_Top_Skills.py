"""
TalentScope AI — Top Skills by Role
Shows the most in-demand skills for each job category.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine

st.set_page_config(page_title="Top Skills | TalentScope AI", layout="wide")
st.title("🏆 Top Skills by Role")
st.markdown("See which skills the market demands most for each role category")
st.markdown("---")

try:
    engine = get_engine()

    # Get available role categories
    with engine.connect() as conn:
        roles_df = pd.read_sql(text("""
            SELECT title_category, COUNT(*) as job_count
            FROM job_features
            WHERE title_category IS NOT NULL AND title_category != ''
            GROUP BY title_category
            ORDER BY job_count DESC
        """), conn)

    if roles_df.empty:
        st.warning("No role data available yet.")
        st.stop()

    # Role selector
    col1, col2 = st.columns([2, 1])
    with col1:
        selected_role = st.selectbox(
            "Select a role category",
            roles_df["title_category"].tolist(),
            index=0,
        )
    with col2:
        job_count = roles_df[roles_df["title_category"] == selected_role]["job_count"].values[0]
        st.metric("Jobs in this category", f"{job_count:,}")

    st.markdown("---")

    # Top skills for selected role
    with engine.connect() as conn:
        skills_df = pd.read_sql(text("""
            SELECT skill, COUNT(*) as demand_count,
                   ROUND(COUNT(*) * 100.0 / :total, 1) as pct
            FROM job_features, UNNEST(skills) AS skill
            WHERE title_category = :role
            GROUP BY skill
            ORDER BY demand_count DESC
            LIMIT 15
        """), conn, params={"role": selected_role, "total": job_count})

    if skills_df.empty:
        st.info(f"No skill data for {selected_role} yet.")
        st.stop()

    # Horizontal bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=skills_df["skill"].iloc[::-1],
        x=skills_df["demand_count"].iloc[::-1],
        orientation="h",
        marker_color=[
            "#1A5276" if i < 3 else "#2E86C1" if i < 7 else "#AED6F1"
            for i in range(len(skills_df))
        ][::-1],
        text=[f"{p}%" for p in skills_df["pct"].iloc[::-1]],
        textposition="outside",
    ))
    fig.update_layout(
        title=f"Top 15 Skills for {selected_role}",
        xaxis_title="Number of Job Postings",
        yaxis_title="",
        height=500,
        margin=dict(l=20, r=40, t=50, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Skill details table
    st.subheader("📋 Skill Breakdown")

    col1, col2 = st.columns([3, 2])
    with col1:
        display_df = skills_df.copy()
        display_df.columns = ["Skill", "Job Postings", "% of Role"]
        display_df.index = range(1, len(display_df) + 1)
        st.dataframe(display_df, use_container_width=True)

    with col2:
        st.markdown("**Key Insights:**")
        top3 = skills_df["skill"].head(3).tolist()
        st.markdown(f"🥇 **{top3[0].upper()}** is the #1 skill for {selected_role}")
        if len(top3) > 1:
            st.markdown(f"🥈 **{top3[1].upper()}** and 🥉 **{top3[2].upper()}** round out the top 3")

        top_pct = skills_df["pct"].iloc[0]
        st.markdown(f"📊 Top skill appears in **{top_pct}%** of {selected_role} postings")

        total_skills = len(skills_df)
        st.markdown(f"🔍 **{total_skills} distinct skills** tracked for this role")

    # Cross-role comparison
    st.markdown("---")
    st.subheader("🔄 Cross-Role Comparison: Top 5 Skills per Category")

    with engine.connect() as conn:
        all_skills_df = pd.read_sql(text("""
            SELECT title_category, skill, COUNT(*) as cnt,
                   ROW_NUMBER() OVER (PARTITION BY title_category ORDER BY COUNT(*) DESC) as rank
            FROM job_features, UNNEST(skills) AS skill
            WHERE title_category IS NOT NULL AND title_category != ''
            GROUP BY title_category, skill
        """), conn)

    # Filter to top 5 per role
    top5 = all_skills_df[all_skills_df["rank"] <= 5].copy()

    if not top5.empty:
        pivot = top5.pivot_table(index="skill", columns="title_category", values="cnt", fill_value=0)

        fig2 = go.Figure()
        colors = ["#1A5276", "#2E86C1", "#5DADE2", "#AED6F1", "#D4E6F1", "#E8DAEF"]
        for i, role in enumerate(pivot.columns):
            fig2.add_trace(go.Bar(
                name=role,
                x=pivot.index,
                y=pivot[role],
                marker_color=colors[i % len(colors)],
            ))

        fig2.update_layout(
            barmode="group",
            title="Skill Demand Across Role Categories",
            xaxis_title="Skill",
            yaxis_title="Job Postings",
            height=450,
            margin=dict(t=50, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig2, use_container_width=True)

except Exception as e:
    st.error(f"Error loading data: {e}")