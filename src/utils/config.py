"""
TalentScope AI — Configuration
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Try Streamlit secrets first, then .env
def get_config(key, default=""):
    try:
        import streamlit as st
        return st.secrets.get(key, os.getenv(key, default))
    except:
        return os.getenv(key, default)

DB_CONFIG = {
    "host": get_config("POSTGRES_HOST", "127.0.0.1"),
    "port": int(get_config("POSTGRES_PORT", "5432")),
    "user": get_config("POSTGRES_USER", "talentscope"),
    "password": get_config("POSTGRES_PASSWORD", "talentscope123"),
    "database": get_config("POSTGRES_DB", "talentscope_db"),
}

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

RAPIDAPI_KEY = get_config("RAPIDAPI_KEY", "")
GROQ_API_KEY = get_config("GROQ_API_KEY", "")

APP_ENV = get_config("APP_ENV", "development")
LOG_LEVEL = get_config("LOG_LEVEL", "INFO")