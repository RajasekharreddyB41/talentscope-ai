"""
TalentScope AI — Configuration
"""

import os
from urllib.parse import quote_plus

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def get_config(key, default=""):
    try:
        import streamlit as st
        return st.secrets.get(key, os.getenv(key, default))
    except:
        return os.getenv(key, default)


# Try single DATABASE_URL first, fall back to individual parts
_db_url = get_config("DATABASE_URL", "")

if _db_url:
    if _db_url.startswith("postgresql://"):
        DATABASE_URL = _db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    elif _db_url.startswith("postgres://"):
        DATABASE_URL = _db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    else:
        DATABASE_URL = _db_url
else:
    _host = get_config("POSTGRES_HOST", "127.0.0.1")
    _port = get_config("POSTGRES_PORT", "5432")
    _user = get_config("POSTGRES_USER", "talentscope")
    _pass = get_config("POSTGRES_PASSWORD", "talentscope123")
    _db   = get_config("POSTGRES_DB", "talentscope_db")
    DATABASE_URL = (
        f"postgresql+psycopg2://{quote_plus(_user)}:{quote_plus(_pass)}"
        f"@{_host}:{_port}/{_db}"
    )

RAPIDAPI_KEY = get_config("RAPIDAPI_KEY", "")
GROQ_API_KEY = get_config("GROQ_API_KEY", "")
APP_ENV = get_config("APP_ENV", "development")
LOG_LEVEL = get_config("LOG_LEVEL", "INFO")