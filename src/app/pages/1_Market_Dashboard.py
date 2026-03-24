"""
TalentScope AI — Market Dashboard
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from src.analysis.sql_analytics import (
    salary_by_experience, top_hiring_companies,
    salary_by_city, title_category_analysis
)

st.set_page_config(page_title="Market Dashboard | TalentScope AI", layout="wide")
st.title("📊 Market Dashboard")
st.markdown("Real-time insights into the tech job market")
st.markdown("---")

# ---- Role Distribution ----
st.subheader("Job Distribution by Role Category")
df_roles = title_category_analysis()

col1, col2 = st.columns(2)

with col1:
    fig = px.pie(
        df_roles, values="job_count", names="title_category",
        title="Role Category Distribution",
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4,
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(showlegend=False, height=450)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        df_roles.sort_values("job_count", ascending=True),
        x="job_count", y="title_category",
        orientation="h",
        title="Jobs by Role Category",
        color="avg_max",
        color_continuous_scale="Viridis",
        labels={"job_count": "Number of Jobs", "title_category": "", "avg_max": "Avg Max Salary"},
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---- Salary by Experience ----
st.subheader("💰 Salary by Experience Level")
df_sal = salary_by_experience()

fig = go.Figure()
fig.add_trace(go.Bar(
    name="Avg Min Salary", x=df_sal["experience_level"], y=df_sal["avg_min_salary"],
    marker_color="#2E86C1",
))
fig.add_trace(go.Bar(
    name="Avg Max Salary", x=df_sal["experience_level"], y=df_sal["avg_max_salary"],
    marker_color="#1B4F72",
))
fig.update_layout(
    barmode="group", title="Salary Range by Experience Level",
    xaxis_title="Experience Level", yaxis_title="Salary (USD)",
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---- Top Companies ----
st.subheader("🏢 Top Hiring Companies")
df_companies = top_hiring_companies()

fig = px.bar(
    df_companies.head(15).sort_values("job_count", ascending=True),
    x="job_count", y="company",
    orientation="h",
    title="Top 15 Companies by Job Count",
    color="job_count",
    color_continuous_scale="Blues",
    labels={"job_count": "Job Count", "company": ""},
)
fig.update_layout(height=500, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---- Salary by City ----
st.subheader("🌎 Salary by Location")
df_city = salary_by_city()

if not df_city.empty:
    fig = px.scatter(
        df_city, x="avg_min", y="avg_max",
        size="job_count", hover_name="location_city",
        color="avg_max",
        color_continuous_scale="Sunset",
        title="City Salary Map (bubble size = job count)",
        labels={"avg_min": "Avg Min Salary", "avg_max": "Avg Max Salary"},
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

# Data table
st.subheader("📋 City Salary Table")
st.dataframe(
    df_city.rename(columns={
        "location_city": "City", "location_state": "State",
        "job_count": "Jobs", "avg_min": "Avg Min", "avg_max": "Avg Max"
    }),
    use_container_width=True,
    hide_index=True,
)