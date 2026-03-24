"""
TalentScope AI — Skill Gap Analyzer Page
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.models.skill_gap_analyzer import (
    analyze_skill_gap, tfidf_similarity,
    get_llm_recommendations, get_available_roles
)

st.set_page_config(page_title="Skill Gap Analyzer | TalentScope AI", layout="wide")
st.title("🧠 Skill Gap Analyzer")
st.markdown("Compare your skills against real market demands and get personalized recommendations")
st.markdown("---")

# Available roles
roles = get_available_roles()

# Input section
col1, col2 = st.columns([2, 1])

with col1:
    user_skills_input = st.text_area(
        "Enter your skills (comma-separated)",
        value="python, sql, pandas, excel, git",
        height=100,
        help="Type your technical skills separated by commas",
    )

    user_resume = st.text_area(
        "Paste your resume summary (optional — for job matching)",
        height=120,
        placeholder="E.g.: Experienced data analyst with 3 years in Python, SQL, and Tableau. Built dashboards for marketing team...",
    )

with col2:
    target_role = st.selectbox("Target Role", roles, index=0)

    st.markdown("---")
    st.markdown("**How it works:**")
    st.markdown("""
    1. Enter your skills
    2. Pick a target role
    3. See your gap analysis
    4. Get AI recommendations
    """)

# Analyze button
if st.button("Analyze My Skills", type="primary", use_container_width=True):
    # Parse skills
    user_skills = [s.strip().lower() for s in user_skills_input.split(",") if s.strip()]

    if not user_skills:
        st.error("Please enter at least one skill")
    else:
        # Run analysis
        result = analyze_skill_gap(user_skills, target_role=target_role)

        if "error" in result:
            st.error(result["error"])
        else:
            st.markdown("---")

            # Coverage score
            score = result["coverage_score"]
            col1, col2, col3 = st.columns(3)
            col1.metric("Coverage Score", f"{score}%")
            col2.metric("Skills Matched", result["matched_count"])
            col3.metric("Skills Missing", result["missing_count"])

            # Coverage gauge
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=score,
                number={"suffix": "%"},
                title={"text": f"Market Coverage for {target_role}"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#27AE60" if score >= 60 else "#F39C12" if score >= 40 else "#E74C3C"},
                    "steps": [
                        {"range": [0, 40], "color": "#FADBD8"},
                        {"range": [40, 60], "color": "#FEF9E7"},
                        {"range": [60, 100], "color": "#E8F8F5"},
                    ],
                },
            ))
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            # Matched vs Missing
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Matched Skills")
                for skill, data in result["matched_skills"].items():
                    st.markdown(f"✅ **{skill}** — in {data['pct']}% of {target_role} jobs")

            with col2:
                st.subheader("Top Skills to Learn")
                for i, skill in enumerate(result["top_recommendations"][:10]):
                    data = result["missing_skills"][skill]
                    st.markdown(f"🔴 **{skill}** — in {data['pct']}% of {target_role} jobs")

            # Skills demand chart
            st.markdown("---")
            st.subheader("Skill Demand Comparison")

            chart_data = []
            for skill, data in result["matched_skills"].items():
                chart_data.append({"skill": skill, "demand_pct": data["pct"], "status": "You Have"})
            for skill in result["top_recommendations"][:10]:
                data = result["missing_skills"][skill]
                chart_data.append({"skill": skill, "demand_pct": data["pct"], "status": "Missing"})

            df_chart = pd.DataFrame(chart_data).sort_values("demand_pct", ascending=True)

            fig = px.bar(
                df_chart, x="demand_pct", y="skill", color="status",
                orientation="h",
                color_discrete_map={"You Have": "#27AE60", "Missing": "#E74C3C"},
                title=f"Your Skills vs Market Demand ({target_role})",
                labels={"demand_pct": "% of Jobs", "skill": ""},
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

            # TF-IDF Job Matching
            if user_resume:
                st.markdown("---")
                st.subheader("Top Job Matches (AI-Powered)")

                matches = tfidf_similarity(user_resume, n_matches=5)
                if matches:
                    for i, m in enumerate(matches):
                        sal = f"${m['salary_min']:,.0f} - ${m['salary_max']:,.0f}" if m['salary_min'] and not pd.isna(m['salary_min']) else "Not listed"
                        with st.expander(f"#{i+1} — {m['title']} at {m['company']} ({m['similarity']}% match)"):
                            st.markdown(f"**Location:** {m['location'] or 'Not specified'}")
                            st.markdown(f"**Experience:** {m['experience']}")
                            st.markdown(f"**Salary:** {sal}")
                            st.markdown(f"**Match Score:** {m['similarity']}%")

            # LLM Recommendations
            st.markdown("---")
            st.subheader("AI Career Recommendations")

            with st.spinner("Generating personalized recommendations..."):
                rec = get_llm_recommendations(result)
                st.markdown(rec)