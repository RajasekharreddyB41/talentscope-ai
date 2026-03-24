"""
TalentScope AI — Salary Predictor Page
"""

import streamlit as st
import plotly.graph_objects as go
from src.models.salary_predictor import predict_salary
from src.analysis.sql_analytics import salary_by_experience

st.set_page_config(page_title="Salary Predictor | TalentScope AI", layout="wide")
st.title("💰 Salary Predictor")
st.markdown("ML-powered salary estimates based on role, experience, location, and skills")
st.markdown("---")

# Input form
col1, col2 = st.columns(2)

with col1:
    title_category = st.selectbox(
        "Target Role",
        ["Data Analyst", "Data Engineer", "Data Scientist", "ML Engineer",
         "Software Engineer", "Python Developer", "Full Stack",
         "DevOps/Cloud", "BI Analyst", "Analytics", "Other Tech"],
        index=0,
    )

    experience = st.selectbox(
        "Experience Level",
        ["junior", "mid", "senior", "lead"],
        index=1,
    )

with col2:
    location_tier = st.selectbox(
        "Location Tier",
        ["tier1", "tier2", "tier3"],
        format_func=lambda x: {
            "tier1": "Tier 1 — SF, NYC, Seattle, Boston (High COL)",
            "tier2": "Tier 2 — Austin, Denver, Chicago, Atlanta (Mid COL)",
            "tier3": "Tier 3 — Other / Remote (Lower COL)",
        }[x],
        index=0,
    )

    skill_count = st.slider("Number of Relevant Skills", min_value=1, max_value=20, value=7)

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
        st.subheader("Predicted Salary Range")

        col1, col2, col3 = st.columns(3)
        col1.metric("Low Estimate", f"${result['predicted_min']:,}")
        col2.metric("Mid Estimate", f"${result['predicted_mid']:,}")
        col3.metric("High Estimate", f"${result['predicted_max']:,}")

        # Gauge chart
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=result["predicted_mid"],
            number={"prefix": "$", "valueformat": ","},
            title={"text": f"{title_category} | {experience.title()} | {location_tier}"},
            gauge={
                "axis": {"range": [30000, 350000]},
                "bar": {"color": "#2E86C1"},
                "steps": [
                    {"range": [30000, 80000], "color": "#EBF5FB"},
                    {"range": [80000, 150000], "color": "#D4E6F1"},
                    {"range": [150000, 250000], "color": "#A9CCE3"},
                    {"range": [250000, 350000], "color": "#7FB3D8"},
                ],
                "threshold": {
                    "line": {"color": "#1B4F72", "width": 4},
                    "thickness": 0.75,
                    "value": result["predicted_mid"],
                },
            },
        ))
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

        st.info(
            "This prediction is based on your inputs and data from 1,990 job postings. "
            "Actual salaries vary based on company, industry, and negotiation."
        )
    else:
        st.error("Model not trained yet. Please run the training pipeline first.")

# Market comparison
st.markdown("---")
st.subheader("Market Salary Comparison")

df_sal = salary_by_experience()
if not df_sal.empty:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Avg Min", x=df_sal["experience_level"], y=df_sal["avg_min_salary"],
        marker_color="#AED6F1",
    ))
    fig.add_trace(go.Bar(
        name="Avg Max", x=df_sal["experience_level"], y=df_sal["avg_max_salary"],
        marker_color="#2E86C1",
    ))
    fig.update_layout(barmode="group", height=400, title="Market Average by Experience Level")
    st.plotly_chart(fig, use_container_width=True)