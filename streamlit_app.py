"""
TalentScope AI — Streamlit Cloud Entry Point
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

# Run the home page
exec(open("src/app/Home.py").read())