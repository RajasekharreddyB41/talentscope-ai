"""
TalentScope AI — Configuration
Loads environment variables for the entire application.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Database
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "talentscope"),
    "password": os.getenv("POSTGRES_PASSWORD", "talentscope123"),
    "database": os.getenv("POSTGRES_DB", "talentscope_db"),
}

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# API Keys
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# App
APP_ENV = os.getenv("APP_ENV", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")