"""
TalentScope AI — Salary Predictor Page (Polished)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from src.models.salary_predictor import predict_salary
from src.analysis.sql_analytics import salary_by_experience, title_category_analysis

st.set_page_config(page_title="Salary Predictor | TalentScope AI", layout="wide")
st.title("💰 Salary Predictor")
st.markdown("ML-powered salary estimates based on role, experience, location, and skills")
st.markdown("---")

# Input form
col1, col2 = st.columns(2)

with col1:
    title_category = st.selectbox(
        "🎯 Target Role",
        ["Data Analyst", "Data Engineer", "Data Scientist", "ML Engineer",
         "Software Engineer", "Python Developer", "Full Stack",
         "DevOps/Cloud", "BI Analyst", "Analytics", "Other Tech"],
        index=0,
    )

    experience = st.selectbox(
        "📈 Experience Level",
        ["junior", "mid", "senior", "lead"],
        index=1,
        format_func=lambda x: x.title(),
    )

with col2:
    location_tier = st.selectbox(
        "📍 Location Tier",
        ["tier1", "tier2", "tier3"],
        format_func=lambda x: {
            "tier1": "Tier 1 — SF, NYC, Seattle, Boston (High COL)",
            "tier2": "Tier 2 — Austin, Denver, Chicago, Atlanta (Mid COL)",
            "tier3": "Tier 3 — Other / Remote (Lower COL)",
        }[x],
        index=0,
    )

    skill_count = st.slider("🛠️ Number of Relevant Skills", min_value=1, max_value=20, value=7)

# Predict button
if st.button("Predict Salary", type="primary", use_container_width=True):
    result = predict_salary(
        skill_count=skill_count,
        experience=experience,
        location_tier=location_tier,
        title_category=title_category,
    )

    if result:
        st.markdown("---")

        # Prediction cards
        col1, col2, col3 = st.columns(3)
        col1.metric("Low Estimate", f"${result['predicted_min']:,}")
        col2.metric("Mid Estimate", f"${result['predicted_mid']:,}", delta="Best estimate")
        col3.metric("High Estimate", f"${result['predicted_max']:,}")

        # Confidence interval visualization
        fig = go.Figure()

        # Range bar
        fig.add_trace(go.Bar(
            x=[result["predicted_max"] - result["predicted_min"]],
            y=["Predicted Range"],
            base=[result["predicted_min"]],
            orientation="h",
            marker_color="#AED6F1",
            name="Salary Range",
            hovertemplate="Range: $%{base:,.0f} - $%{x:,.0f}<extra></extra>",
        ))

        # Mid point marker
        fig.add_trace(go.Scatter(
            x=[result["predicted_mid"]],
            y=["Predicted Range"],
            mode="markers+text",
            marker=dict(size=16, color="#1B4F72", symbol="diamond"),
            text=[f"${result['predicted_mid']:,}"],
            textposition="top center",
            name="Mid Estimate",
        ))

        fig.update_layout(
            title=f"Salary Confidence Interval — {title_category} ({experience.title()})",
            xaxis_title="Annual Salary (USD)",
            height=200,
            showlegend=False,
            margin=dict(t=60, b=40, l=20, r=20),
            xaxis=dict(tickformat="$,.0f"),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Context info
        st.info(
            f"This estimate is for a **{experience.title()} {title_category}** with "
            f"**{skill_count} skills** in a **{location_tier.replace('tier', 'Tier ')}** location. "
            f"Based on analysis of {1990:,} job postings."
        )
    else:
        st.error("Model not trained yet. Please run the training pipeline first.")

# Market comparison section
st.markdown("---")
st.subheader("📊 Market Salary Benchmarks")

tab1, tab2 = st.tabs(["By Experience Level", "By Role Category"])

with tab1:
    df_sal = salary_by_experience()
    if not df_sal.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Avg Min", x=df_sal["experience_level"], y=df_sal["avg_min_salary"],
            marker_color="#AED6F1", text=df_sal["avg_min_salary"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else ""),
            textposition="outside",
        ))
        fig.add_trace(go.Bar(
            name="Avg Max", x=df_sal["experience_level"], y=df_sal["avg_max_salary"],
            marker_color="#2E86C1", text=df_sal["avg_max_salary"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else ""),
            textposition="outside",
        ))
        fig.update_layout(barmode="group", height=400, yaxis=dict(tickformat="$,.0f"))
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    df_roles = title_category_analysis()
    if not df_roles.empty:
        df_roles_sorted = df_roles.sort_values("avg_max", ascending=True).dropna(subset=["avg_max"])
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=df_roles_sorted["title_category"], x=df_roles_sorted["avg_min"],
            name="Avg Min", orientation="h", marker_color="#AED6F1",
        ))
        fig.add_trace(go.Bar(
            y=df_roles_sorted["title_category"], x=df_roles_sorted["avg_max"],
            name="Avg Max", orientation="h", marker_color="#2E86C1",
        ))
        fig.update_layout(barmode="group", height=500, xaxis=dict(tickformat="$,.0f"))
        st.plotly_chart(fig, use_container_width=True)