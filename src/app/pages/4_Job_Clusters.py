"""
TalentScope AI — Job Clusters Page
"""

import streamlit as st
import plotly.express as px
import pandas as pd
from src.models.job_clustering import get_cluster_data

st.set_page_config(page_title="Job Clusters | TalentScope AI", layout="wide")
st.title("🗺️ Job Clusters")
st.markdown("Discover how job roles naturally group together using NLP clustering")
st.markdown("---")

# Load cluster data
with st.spinner("Building job clusters..."):
    df = get_cluster_data()

if df.empty:
    st.error("Not enough data for clustering")
else:
    # Key metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Jobs Clustered", f"{len(df):,}")
    col2.metric("Clusters Found", df["cluster"].nunique())
    col3.metric("Role Categories", df["title_category"].nunique())

    st.markdown("---")

    # Interactive scatter plot
    st.subheader("Interactive Cluster Map")
    st.markdown("Each dot is a job posting. Color = cluster. Hover for details.")

    # Build hover text
    df["hover_text"] = (
        df["title"].str[:50] + "<br>" +
        df["company"].fillna("Unknown").str[:30] + "<br>" +
        df["cluster_keywords"].fillna("")
    )

    # Salary for size (use skill_count if no salary)
    df["bubble_size"] = df["skill_count"].fillna(5).clip(2, 20)

    fig = px.scatter(
        df,
        x="x", y="y",
        color="cluster_label",
        hover_name="title",
        hover_data={
            "company": True,
            "experience_level": True,
            "title_category": True,
            "x": False, "y": False,
            "cluster_label": False,
            "bubble_size": False,
            "hover_text": False,
        },
        size="bubble_size",
        title="Job Market Landscape — NLP Clusters (TF-IDF + PCA + KMeans)",
        labels={"cluster_label": "Cluster", "x": "", "y": ""},
        color_discrete_sequence=px.colors.qualitative.Set2,
        height=600,
    )
    fig.update_layout(
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Cluster breakdown
    st.subheader("Cluster Breakdown")

    cluster_summary = df.groupby("cluster_label").agg(
        job_count=("id", "count"),
        avg_salary_min=("salary_min", "mean"),
        avg_salary_max=("salary_max", "mean"),
        avg_skills=("skill_count", "mean"),
        top_keywords=("cluster_keywords", "first"),
    ).reset_index()

    cluster_summary["avg_salary_min"] = cluster_summary["avg_salary_min"].round(0)
    cluster_summary["avg_salary_max"] = cluster_summary["avg_salary_max"].round(0)
    cluster_summary["avg_skills"] = cluster_summary["avg_skills"].round(1)
    cluster_summary = cluster_summary.sort_values("job_count", ascending=False)

    st.dataframe(
        cluster_summary.rename(columns={
            "cluster_label": "Cluster",
            "job_count": "Jobs",
            "avg_salary_min": "Avg Min Salary",
            "avg_salary_max": "Avg Max Salary",
            "avg_skills": "Avg Skills",
            "top_keywords": "Top Keywords",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Cluster size bar chart
    st.markdown("---")
    st.subheader("Cluster Size Comparison")

    fig2 = px.bar(
        cluster_summary.sort_values("job_count", ascending=True),
        x="job_count", y="cluster_label",
        orientation="h",
        color="job_count",
        color_continuous_scale="Viridis",
        labels={"job_count": "Number of Jobs", "cluster_label": ""},
        title="Jobs per Cluster",
    )
    fig2.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

    # Filter by cluster
    st.markdown("---")
    st.subheader("Explore a Cluster")

    selected_cluster = st.selectbox(
        "Select a cluster to explore",
        sorted(df["cluster_label"].unique()),
    )

    cluster_df = df[df["cluster_label"] == selected_cluster]

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**Jobs in cluster:** {len(cluster_df)}")
        keywords = cluster_df["cluster_keywords"].iloc[0] if not cluster_df.empty else ""
        st.markdown(f"**Top keywords:** {keywords}")

    with col2:
        sal_min = cluster_df["salary_min"].mean()
        sal_max = cluster_df["salary_max"].mean()
        if pd.notna(sal_min):
            st.markdown(f"**Avg Salary:** ${sal_min:,.0f} - ${sal_max:,.0f}")
        st.markdown(f"**Avg Skills:** {cluster_df['skill_count'].mean():.1f}")

    # Sample jobs from cluster
    st.markdown("**Sample Jobs:**")
    sample = cluster_df[["title", "company", "location_city", "experience_level", "salary_min", "salary_max"]].head(10)
    st.dataframe(sample, use_container_width=True, hide_index=True)