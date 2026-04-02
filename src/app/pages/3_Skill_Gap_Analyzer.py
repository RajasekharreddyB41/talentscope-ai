"""
TalentScope AI — Skill Gap Analyzer Page (with Impact Scoring + Cold Start)
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from src.models.skill_gap_analyzer import (
    analyze_skill_gap, tfidf_similarity,
    get_llm_recommendations, get_available_roles,
    compute_skill_impact_scores
)

st.set_page_config(page_title="Skill Gap Analyzer | TalentScope AI", layout="wide")
st.title("🧠 Skill Gap Analyzer")
st.markdown("Compare your skills against real market demands and get a personalized learning roadmap")
st.markdown("---")

# Available roles
roles = get_available_roles()

# Preset skill sets per role (for cold start)
ROLE_PRESETS = {
    "Data Analyst": "python, sql, excel, tableau, power bi",
    "Data Engineer": "python, sql, aws, spark, docker, git",
    "Data Scientist": "python, sql, pandas, scikit-learn, tensorflow, statistics",
    "Software Engineer": "python, java, javascript, git, docker, aws",
    "ML Engineer": "python, tensorflow, pytorch, docker, aws, sql",
    "DevOps/Cloud": "aws, docker, kubernetes, terraform, git, linux",
    "BI Analyst": "sql, excel, tableau, power bi, python",
    "Python Developer": "python, django, flask, sql, git, docker",
    "Analytics": "sql, excel, python, tableau, statistics",
    "Full Stack": "javascript, python, react, sql, docker, git",
    "Backend Developer": "python, java, sql, docker, aws, git",
    "Other Tech": "python, sql, git",
}

# Quick start vs custom input
mode = st.radio(
    "How would you like to start?",
    ["⚡ Quick Start — Pick a role, see instant insights", "✏️ Custom — Enter your own skills"],
    index=0,
    horizontal=True,
)

if mode.startswith("⚡"):
    col1, col2 = st.columns([2, 1])

    with col1:
        target_role = st.selectbox("🎯 Select your target role", roles, index=0)
        default_skills = ROLE_PRESETS.get(target_role, "python, sql, git")

        user_skills_input = st.text_area(
            "Your current skills (edit to match yours)",
            value=default_skills,
            height=80,
        )

    with col2:
        st.markdown("")
        st.markdown("**💡 Quick Start Tips:**")
        st.markdown("""
        1. Pick your target role
        2. Edit skills to match yours
        3. Click Analyze
        4. See your personalized roadmap
        """)

    user_resume = ""

else:
    col1, col2 = st.columns([2, 1])

    with col1:
        user_skills_input = st.text_area(
            "Enter your skills (comma-separated)",
            value="python, sql, pandas, excel, git",
            height=100,
        )

        user_resume = st.text_area(
            "Paste your resume summary (optional — for job matching)",
            height=120,
            placeholder="E.g.: Experienced data analyst with 3 years in Python, SQL, and Tableau...",
        )

    with col2:
        target_role = st.selectbox("Target Role", roles, index=0)

        st.markdown("---")
        st.markdown("**How it works:**")
        st.markdown("""
        1. Enter your skills
        2. Pick a target role
        3. See impact-scored gap analysis
        4. Get AI-powered learning roadmap
        """)

# Analyze button
if st.button("🔍 Analyze My Skills", type="primary", use_container_width=True):
    user_skills = [s.strip().lower() for s in user_skills_input.split(",") if s.strip()]

    if not user_skills:
        st.error("Please enter at least one skill")
    else:
        result = analyze_skill_gap(user_skills, target_role=target_role)

        if "error" in result:
            st.error(result["error"])
        else:
            st.markdown("---")

            # Coverage score row
            score = result["coverage_score"]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Coverage Score", f"{score}%")
            col2.metric("Skills Matched", result["matched_count"])
            col3.metric("Skills to Learn", result["missing_count"])
            col4.metric("Target Role", target_role)

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
            fig.update_layout(height=280, margin=dict(t=60, b=20))
            st.plotly_chart(fig, use_container_width=True)

            # ============================================
            # SKILL IMPACT SCORING TABLE
            # ============================================
            st.markdown("---")
            st.subheader("🎯 Skill Impact Scores — Your Personalized Learning Roadmap")
            st.markdown("Skills ranked by **Demand × Gap = Priority**. Focus on 🔥 Critical skills first.")

            impact_scores = compute_skill_impact_scores(user_skills, target_role)

            if impact_scores:
                missing_scores = [s for s in impact_scores if s["status"] == "missing"]
                covered_scores = [s for s in impact_scores if s["status"] == "have"]

                if missing_scores:
                    st.markdown("**Skills to learn (ranked by impact):**")

                    missing_df = pd.DataFrame(missing_scores)
                    missing_df = missing_df[["priority", "skill", "demand_pct", "impact_score"]]
                    missing_df.columns = ["Priority", "Skill", "Market Demand %", "Impact Score"]
                    missing_df.index = range(1, len(missing_df) + 1)

                    st.dataframe(
                        missing_df,
                        use_container_width=True,
                        column_config={
                            "Priority": st.column_config.TextColumn(width="small"),
                            "Skill": st.column_config.TextColumn(width="medium"),
                            "Market Demand %": st.column_config.ProgressColumn(
                                min_value=0, max_value=100, format="%.0f%%",
                            ),
                            "Impact Score": st.column_config.NumberColumn(format="%.1f"),
                        },
                    )

                if len(missing_scores) > 2:
                    top_missing = missing_scores[:10]
                    fig_impact = go.Figure()

                    colors = []
                    for s in top_missing:
                        if "Critical" in s["priority"]:
                            colors.append("#E74C3C")
                        elif "Important" in s["priority"]:
                            colors.append("#F39C12")
                        elif "Useful" in s["priority"]:
                            colors.append("#3498DB")
                        else:
                            colors.append("#95A5A6")

                    fig_impact.add_trace(go.Bar(
                        y=[s["skill"] for s in top_missing][::-1],
                        x=[s["impact_score"] for s in top_missing][::-1],
                        orientation="h",
                        marker_color=colors[::-1],
                        text=[s["priority"].split(" ")[0] for s in top_missing][::-1],
                        textposition="inside",
                    ))
                    fig_impact.update_layout(
                        title="Skill Impact Score — What to Learn Next",
                        xaxis_title="Impact Score (Demand × Gap)",
                        height=400,
                        margin=dict(l=20, r=20, t=50, b=40),
                        plot_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig_impact, use_container_width=True)

                if covered_scores:
                    st.markdown("**Skills you already have:**")
                    covered_text = " • ".join([f"✅ **{s['skill']}** ({s['demand_pct']}%)" for s in covered_scores])
                    st.markdown(covered_text)

            # ============================================
            # SKILL DEMAND CHART
            # ============================================
            st.markdown("---")
            st.subheader("📊 Your Skills vs Market Demand")

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
                st.subheader("🔗 Top Job Matches (AI-Powered)")

                matches = tfidf_similarity(user_resume, n_matches=5)
                if matches:
                    for i, m in enumerate(matches):
                        sal = f"${m['salary_min']:,.0f} - ${m['salary_max']:,.0f}" if m['salary_min'] and not pd.isna(m['salary_min']) else "Not listed"
                        with st.expander(f"#{i+1} — {m['title']} at {m['company']} ({m['similarity']}% match)"):
                            st.markdown(f"**Location:** {m['location'] or 'Not specified'}")
                            st.markdown(f"**Experience:** {m['experience']}")
                            st.markdown(f"**Salary:** {sal}")

            # LLM Recommendations
            st.markdown("---")
            st.subheader("🤖 AI Career Recommendations")

            with st.spinner("Generating personalized recommendations..."):
                rec = get_llm_recommendations(result)
                st.markdown(rec)