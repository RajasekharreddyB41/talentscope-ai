"""
TalentScope AI — Streamlit Cloud Entry Point
"""

import sys
import os
import re
import streamlit as st

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

# Set page config FIRST
st.set_page_config(
    page_title="TalentScope AI",
    page_icon="🎯",
    layout="wide",
)

# Create navigation
pages = {
    "Home": "src/app/Home.py",
    "📊 Market Dashboard": "src/app/pages/1_Market_Dashboard.py",
    "💰 Salary Predictor": "src/app/pages/2_Salary_Predictor.py",
    "🧠 Skill Gap Analyzer": "src/app/pages/3_Skill_Gap_Analyzer.py",
    "🗺️ Job Clusters": "src/app/pages/4_Job_Clusters.py",
    "💼 Browse Jobs": "src/app/pages/5_Browse_Jobs.py",
    "⚙️ Pipeline Monitor": "src/app/pages/5_Pipeline_Monitor.py",
    "🏆 Top Skills by Role": "src/app/pages/6_Top_Skills.py",
}

# Sidebar navigation
selection = st.sidebar.radio("Navigate", list(pages.keys()))

# Load selected page
page_path = pages[selection]
if os.path.exists(page_path):
    with open(page_path, "r", encoding="utf-8") as f:
        code = f.read()

    # Remove full st.set_page_config(...) block from sub-pages
    code = re.sub(
        r"st\.set_page_config\s*\(.*?\)\s*",
        "",
        code,
        flags=re.DOTALL,
    )

    # Execute cleaned page code
    exec_globals = {
        "__name__": "__main__",
        "__file__": page_path,
    }
    exec(code, exec_globals)
else:
    st.error(f"Page not found: {page_path}")