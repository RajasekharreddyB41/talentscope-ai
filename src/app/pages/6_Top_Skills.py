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
st.markdown("Discover which skills the market demands most — updated daily from real job postings")
st.markdown("---")

try:
    engine = get_engine()

    # Get available role categories (top 8 only)
    with engine.connect() as conn:
        roles_df = pd.read_sql(text("""
            SELECT title_category, COUNT(*) as job_count
            FROM job_features
            WHERE title_category IS NOT NULL AND title_category != ''
            GROUP BY title_category
            ORDER BY job_count DESC
            LIMIT 8
        """), conn)

    if roles_df.empty:
        st.warning("No role data available yet.")
        st.stop()

    # Role selector
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        selected_role = st.selectbox(
            "Select a role category",
            roles_df["title_category"].tolist(),
            index=0,
        )
    with col2:
        job_count = int(roles_df[roles_df["title_category"] == selected_role]["job_count"].values[0])
        st.metric("Jobs in category", f"{job_count:,}")
    with col3:
        total_all = int(roles_df["job_count"].sum())
        pct = round(job_count * 100 / total_all, 1)
        st.metric("Market share", f"{pct}%")

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

    # Two-column layout: bar chart + insights
    col1, col2 = st.columns([3, 1])

    with col1:
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
            title=f"Top 15 Skills — {selected_role}",
            xaxis_title="Job Postings",
            yaxis_title="",
            height=500,
            margin=dict(l=20, r=60, t=50, b=40),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Key Takeaways**")
        top3 = skills_df["skill"].head(3).tolist()
        st.markdown(f"🥇 **{top3[0].upper()}**")
        if len(top3) > 1:
            st.markdown(f"🥈 **{top3[1].upper()}**")
        if len(top3) > 2:
            st.markdown(f"🥉 **{top3[2].upper()}**")

        st.markdown("---")
        top_pct = skills_df["pct"].iloc[0]
        st.markdown(f"Top skill appears in **{top_pct}%** of postings")
        st.markdown(f"**{len(skills_df)}** distinct skills tracked")

    # Cross-role comparison: top 6 roles, top 5 skills each
    st.markdown("---")
    st.subheader("🔄 Skill Comparison Across Top Roles")

    # Let user pick 2-4 roles to compare
    top_roles = roles_df["title_category"].head(6).tolist()
    compare_roles = st.multiselect(
        "Select roles to compare (2-4 recommended)",
        top_roles,
        default=top_roles[:3],
    )

    if len(compare_roles) >= 2:
        with engine.connect() as conn:
            # Get top 8 skills across selected roles
            placeholders = ", ".join([f":r{i}" for i in range(len(compare_roles))])
            params = {f"r{i}": r for i, r in enumerate(compare_roles)}

            compare_df = pd.read_sql(text(f"""
                WITH ranked AS (
                    SELECT title_category, skill, COUNT(*) as cnt
                    FROM job_features, UNNEST(skills) AS skill
                    WHERE title_category IN ({placeholders})
                    GROUP BY title_category, skill
                )
                SELECT * FROM ranked
                WHERE skill IN (
                    SELECT skill FROM ranked
                    GROUP BY skill
                    ORDER BY SUM(cnt) DESC
                    LIMIT 8
                )
            """), conn, params=params)

        if not compare_df.empty:
            pivot = compare_df.pivot_table(
                index="skill", columns="title_category",
                values="cnt", fill_value=0
            )
            # Sort by total demand
            pivot["_total"] = pivot.sum(axis=1)
            pivot = pivot.sort_values("_total", ascending=True).drop(columns=["_total"])

            colors = ["#1A5276", "#E74C3C", "#27AE60", "#F39C12", "#8E44AD", "#2980B9"]
            fig2 = go.Figure()
            for i, role in enumerate(pivot.columns):
                fig2.add_trace(go.Bar(
                    name=role,
                    y=pivot.index,
                    x=pivot[role],
                    orientation="h",
                    marker_color=colors[i % len(colors)],
                ))

            fig2.update_layout(
                barmode="group",
                title="Skill Demand: Head-to-Head",
                xaxis_title="Job Postings",
                yaxis_title="",
                height=400,
                margin=dict(l=20, r=20, t=50, b=40),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                plot_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig2, use_container_width=True)
    elif len(compare_roles) == 1:
        st.info("Select at least 2 roles to compare")

except Exception as e:
    st.error(f"Error loading data: {e}")