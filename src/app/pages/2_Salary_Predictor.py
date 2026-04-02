"""
TalentScope AI — Salary Predictor Page (v2 with SHAP)
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from src.models.salary_predictor import predict_salary
from src.analysis.sql_analytics import salary_by_experience, title_category_analysis

st.set_page_config(page_title="Salary Predictor | TalentScope AI", layout="wide")
st.title("💰 Salary Predictor")
st.markdown("ML-powered salary estimates with explainability — see *why* the model predicts what it does")
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

    is_remote = st.checkbox("🏠 Remote position", value=False)

# Skills selection
st.markdown("**🛠️ Select your skills:**")
all_skills = ["python", "sql", "aws", "excel", "java", "azure", "git", "scala",
              "docker", "kubernetes", "react", "javascript", "machine learning",
              "tableau", "power bi", "spark", "tensorflow", "pytorch"]

selected_skills = st.multiselect(
    "Choose skills that match the role",
    all_skills,
    default=["python", "sql"],
)
skill_count = max(len(selected_skills), 1)

# Predict button
if st.button("🔮 Predict Salary", type="primary", use_container_width=True):
    result = predict_salary(
        skill_count=skill_count,
        experience=experience,
        location_tier=location_tier,
        title_category=title_category,
        is_remote=is_remote,
        user_skills=selected_skills,
    )

    if result:
        st.markdown("---")

        # Model info badge
        version = result.get("model_version", "v1")
        model_name = result.get("model_name", "Unknown")
        st.caption(f"Model: {model_name} ({version}) | Based on 1,000+ salary records")

        # Prediction cards
        col1, col2, col3 = st.columns(3)
        col1.metric("Low Estimate", f"${result['predicted_min']:,}")
        col2.metric("🎯 Best Estimate", f"${result['predicted_mid']:,}")
        col3.metric("High Estimate", f"${result['predicted_max']:,}")

        # Two-column layout: range chart + SHAP contributions
        col_chart, col_shap = st.columns([3, 2])

        with col_chart:
            # Confidence interval visualization
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=[result["predicted_max"] - result["predicted_min"]],
                y=["Predicted Range"],
                base=[result["predicted_min"]],
                orientation="h",
                marker_color="#AED6F1",
                name="Range",
            ))
            fig.add_trace(go.Scatter(
                x=[result["predicted_mid"]],
                y=["Predicted Range"],
                mode="markers+text",
                marker=dict(size=16, color="#1B4F72", symbol="diamond"),
                text=[f"${result['predicted_mid']:,}"],
                textposition="top center",
                name="Estimate",
            ))
            fig.update_layout(
                title="Salary Range",
                height=180,
                showlegend=False,
                margin=dict(t=50, b=30, l=20, r=20),
                xaxis=dict(tickformat="$,.0f"),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_shap:
            # SHAP feature contributions
            contributions = result.get("contributions", {})
            if contributions:
                st.markdown("**🔍 What drives this prediction:**")
                
                # Sort by impact
                sorted_contribs = sorted(contributions.items(), key=lambda x: x[1], reverse=True)
                
                for feat, impact in sorted_contribs[:6]:
                    display_name = feat.replace("_", " ").title()
                    st.markdown(f"- **{display_name}**: ${impact:,} impact")
            else:
                st.info("Feature contributions available after model retraining with SHAP")

        # SHAP bar chart (full)
        if contributions and len(contributions) > 2:
            st.markdown("---")
            st.subheader("📊 Salary Factor Breakdown (SHAP)")

            sorted_items = sorted(contributions.items(), key=lambda x: x[1])
            feat_names = [item[0].replace("_", " ").title() for item in sorted_items]
            feat_values = [item[1] for item in sorted_items]

            fig_shap = go.Figure()
            fig_shap.add_trace(go.Bar(
                y=feat_names,
                x=feat_values,
                orientation="h",
                marker_color=["#E74C3C" if v < 0 else "#2E86C1" for v in feat_values],
                text=[f"${v:,}" for v in feat_values],
                textposition="outside",
            ))
            fig_shap.update_layout(
                title="How each factor impacts salary prediction",
                xaxis_title="Impact on Salary ($)",
                height=max(300, len(feat_names) * 40),
                margin=dict(l=20, r=60, t=50, b=40),
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(tickformat="$,.0f"),
            )
            st.plotly_chart(fig_shap, use_container_width=True)

        # Context
        remote_text = " (Remote)" if is_remote else ""
        skills_text = ", ".join(selected_skills[:5]) if selected_skills else "general"
        st.info(
            f"Estimate for **{experience.title()} {title_category}{remote_text}** with "
            f"skills in **{skills_text}** in a **{location_tier.replace('tier', 'Tier ')}** market."
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