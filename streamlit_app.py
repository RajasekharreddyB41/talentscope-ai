"""
TalentScope AI — Streamlit Cloud Entry Point
"""

import sys
import os
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
    "⚙️ Pipeline Monitor": "src/app/pages/5_Pipeline_Monitor.py",
}

# Sidebar navigation
selection = st.sidebar.radio("Navigate", list(pages.keys()))

# Load selected page (skip set_page_config in sub-pages)
page_path = pages[selection]
if os.path.exists(page_path):
    with open(page_path, "r") as f:
        code = f.read()
    # Remove set_page_config from sub-pages (already set above)
    code = code.replace("st.set_page_config(", "# st.set_page_config(")
    exec(code)
else:
    st.error(f"Page not found: {page_path}")