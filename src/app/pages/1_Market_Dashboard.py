"""
TalentScope AI — Market Dashboard (with Trend KPIs)
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.analysis.sql_analytics import (
    salary_by_experience, top_hiring_companies,
    salary_by_city, title_category_analysis,
    skill_trend_momentum, daily_posting_trend
)

st.set_page_config(page_title="Market Dashboard | TalentScope AI", layout="wide")
st.title("📊 Market Dashboard")
st.markdown("Real-time insights into the tech job market — updated daily")
st.markdown("---")

# ============================================
# TREND KPIs (NEW — top of page)
# ============================================
st.subheader("📈 Hiring Velocity & Skill Trends")

col_vel, col_trend = st.columns(2)

with col_vel:
    # Daily posting trend
    df_daily = daily_posting_trend()
    if not df_daily.empty and len(df_daily) > 1:
        # Calculate velocity KPIs
        recent_7 = df_daily.tail(7)["jobs_posted"].sum() if len(df_daily) >= 7 else df_daily["jobs_posted"].sum()
        prev_7 = df_daily.head(7)["jobs_posted"].sum() if len(df_daily) >= 14 else 0

        wow_change = recent_7 - prev_7 if prev_7 > 0 else 0
        wow_pct = round(wow_change / prev_7 * 100, 1) if prev_7 > 0 else 0

        m1, m2 = st.columns(2)
        m1.metric("Jobs This Week", f"{recent_7:,}", delta=f"{wow_change:+,} vs last week")
        m2.metric("WoW Growth", f"{wow_pct:+.1f}%")

        # Daily posting line chart
        fig_daily = go.Figure()
        fig_daily.add_trace(go.Scatter(
            x=df_daily["date"], y=df_daily["jobs_posted"],
            mode="lines+markers",
            line=dict(color="#2E86C1", width=2),
            marker=dict(size=6),
            name="Jobs Posted",
        ))
        if "with_salary" in df_daily.columns:
            fig_daily.add_trace(go.Scatter(
                x=df_daily["date"], y=df_daily["with_salary"],
                mode="lines+markers",
                line=dict(color="#27AE60", width=2, dash="dot"),
                marker=dict(size=4),
                name="With Salary",
            ))
        fig_daily.update_layout(
            title="Daily Job Postings (Last 14 Days)",
            height=320,
            margin=dict(t=50, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_daily, use_container_width=True)
    else:
        st.info("Not enough date data yet for velocity trends")

with col_trend:
    # Skill trend momentum
    df_trend = skill_trend_momentum(top_n=10)
    if not df_trend.empty:
        # Top movers
        gainers = df_trend[df_trend["change"] > 0].head(3)
        if not gainers.empty:
            top_gainer = gainers.iloc[0]
            st.metric(
                "🔥 Hottest Skill",
                top_gainer["skill"].upper(),
                delta=f"+{int(top_gainer['change'])} postings this week",
            )

        # Trend bar chart
        df_trend_sorted = df_trend.sort_values("this_week", ascending=True)

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Bar(
            y=df_trend_sorted["skill"],
            x=df_trend_sorted["last_week"],
            orientation="h",
            name="Last Week",
            marker_color="#AED6F1",
        ))
        fig_trend.add_trace(go.Bar(
            y=df_trend_sorted["skill"],
            x=df_trend_sorted["this_week"],
            orientation="h",
            name="This Week",
            marker_color="#2E86C1",
        ))
        fig_trend.update_layout(
            barmode="group",
            title="Skill Demand: This Week vs Last Week",
            xaxis_title="Job Postings",
            height=320,
            margin=dict(t=50, b=30, l=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("Not enough weekly data yet for skill trends")

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
st.subheader("🌍 Salary by Location")
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

st.subheader("📋 City Salary Table")
st.dataframe(
    df_city.rename(columns={
        "location_city": "City", "location_state": "State",
        "job_count": "Jobs", "avg_min": "Avg Min", "avg_max": "Avg Max"
    }),
    use_container_width=True,
    hide_index=True,
)