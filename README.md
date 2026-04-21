# 🎯 TalentScope AI

**Real-Time Job Market Intelligence Platform**

TalentScope AI is an end-to-end data and AI platform that ingests real-time job postings, analyzes market trends, and generates actionable career insights using machine learning and NLP.

🔗 **Live Demo:** [https://talentscope-ai-rsr-06.streamlit.app/]

---

## 🚀 Why This Project Matters

Most job platforms stop at listings. TalentScope AI goes further by turning fragmented job data into practical career intelligence.

It helps users:
- identify the skills that are actually in demand
- understand salary patterns across roles and locations
- compare their profile against market requirements
- make smarter decisions about what to learn and where to apply

This bridges the gap between job search and career strategy.

---

## Features

**📊 Market Dashboard** — Interactive visualizations of skill demand, salary patterns, hiring trends, and company activity across the tech job market.

**💰 Salary Predictor** — ML-powered salary estimates based on role, experience, location, and skill count.

**🧠 Skill Gap Analyzer** — Compares a user’s skills against live market demand using NLP and generates personalized recommendations.

**🗺️ Job Clusters** — Discovers natural role groupings through embedding-based clustering and dimensionality reduction.

**⚙️ Pipeline Monitor** — Tracks data freshness, pipeline health, ingestion metrics, and run status.

---

## Architecture

The system follows a layered architecture with a medallion-style data model (Bronze → Silver → Gold):

```text
Data Sources (APIs, Kaggle, Web Scraping, Resume Upload)
        ↓
Data Engineering Layer
  ETL Scripts → Airflow DAGs
  Normalization & Deduplication
  Great Expectations Validation
  PostgreSQL (raw_jobs → clean_jobs → job_features → pipeline_runs)
        ↓
Data Analysis Layer
  SQL Analytics (CTEs, Window Functions)
  Plotly + Streamlit Dashboard
  KPI Tracking (Hiring Velocity, Skill Momentum)
        ↓
Data Science Layer
  Feature Engineering (salary normalization, skill encoding, location tiers)
  Salary Prediction (Linear Regression → Random Forest → XGBoost)
  Skill Gap Analyzer (TF-IDF / Sentence Transformers + Groq LLM)
  Job Clustering (Embeddings + UMAP + KMeans)
  MLflow Experiment Tracking
        ↓
Frontend & Deployment
  Streamlit Cloud | Docker Compose | GitHub Actions CI
```

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Pipeline | Python ETL Scripts → Apache Airflow |
| Database | PostgreSQL / Supabase |
| Validation | Great Expectations |
| Ingestion | Python, Requests, RapidAPI JSearch, Kaggle |
| Analysis | Pandas, SQL (CTEs, Window Functions) |
| Visualization | Plotly, Streamlit |
| ML Models | scikit-learn, XGBoost |
| NLP | TF-IDF, Sentence Transformers, Groq API |
| Experiment Tracking | MLflow |
| Clustering | UMAP, KMeans |
| Containerization | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Deployment | Streamlit Cloud |

---

## Data Pipeline

1. **Ingest** — Fetch job postings from RapidAPI JSearch and Kaggle backfill sources  
2. **Normalize** — Standardize salary formats, clean locations, and extract experience levels  
3. **Deduplicate** — Use SHA-256 hash on `(title + company + location)` to remove duplicates  
4. **Validate** — Run quality checks for nulls, unique hashes, salary sanity, and date validity  
5. **Feature Engineering** — Generate skill vectors, location tiers, and ML-ready fields  
6. **Model Training** — Compare models and track experiments in MLflow  
7. **Serve Insights** — Expose results through the Streamlit app and dashboard  

---

## Database Schema (Medallion Architecture)

| Table | Layer | Purpose |
|-------|-------|---------|
| `raw_jobs` | Bronze | Unprocessed data exactly as received |
| `clean_jobs` | Silver | Normalized, deduplicated records |
| `job_features` | Gold | ML-ready features for analytics and modeling |
| `pipeline_runs` | Observability | Pipeline execution tracking and health monitoring |

---

## Model Performance

Initial model performance is limited by the current feature set and the number of salary-labeled records.

This is expected for an MVP and serves as a baseline for improvement.

### Current Results

| Model | MAE | RMSE | R² |
|-------|-----|------|----|
| Linear Regression | $41,442 | $59,633 | -0.017 |
| Random Forest | $49,411 | $67,402 | -0.299 |
| XGBoost v1 | $57,652 | $79,925 | -0.827 |
| XGBoost v2 (tuned) | $49,383 | $68,328 | -0.335 |

### Planned Improvements

- Add TF-IDF features from job descriptions
- Improve location granularity
- Expand salary-labeled dataset
- Add stronger feature engineering for role and skills

**Focus:** demonstrating end-to-end ML pipeline design, feature engineering, experimentation, and deployment readiness.

---

## Quick Start

### Prerequisites
- Python 3.12+
- PostgreSQL 16+
- Docker (optional)

### Setup

```bash
# Clone the repo
git clone https://github.com/RajasekharreddyB41/talentscope-ai.git
cd talentscope-ai

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials and API keys

# Set up database
psql -U postgres -d postgres -f src/database/schema.sql

# Run ingestion and pipeline
python -m src.ingestion.batch_ingest
python -m src.pipeline.dedup
python -m src.models.feature_engineering

# Train models
python -m src.models.salary_predictor

# Launch dashboard
streamlit run src/app/Home.py
```

---

## API Keys Needed

| Key | Where to Get | Purpose |
|-----|--------------|---------|
| `RAPIDAPI_KEY` | RapidAPI JSearch | Job data ingestion |
| `GROQ_API_KEY` | Groq Console | LLM skill recommendations |

Both are optional if cached or backfill data is available.

---

## Project Structure

```text
talentscope-ai/
├── src/
│   ├── ingestion/        # API connector, scraper, Kaggle loader
│   ├── pipeline/         # ETL, normalize, dedup, validate, tracker
│   ├── database/         # Schema, queries, connection
│   ├── analysis/         # SQL analytics, KPI tracker
│   ├── models/           # Salary predictor, skill gap, clustering, MLflow
│   ├── utils/            # Config, logger
│   └── app/              # Streamlit pages
├── airflow/dags/         # Airflow DAGs
├── tests/                # Unit and pipeline tests
├── data/                 # Raw, processed, models
├── .github/workflows/    # CI pipeline
├── docker-compose.yml
└── requirements.txt
```

---

## Talking Points

**Data Engineer:**  
“I built an automated ETL pipeline that ingests from multiple job APIs, normalizes inconsistent salary formats, deduplicates cross-source postings, and loads them into a layered PostgreSQL data model with pipeline observability tracking.”

**Data Analyst:**  
“I wrote advanced SQL analytics using CTEs and window functions to surface hiring velocity trends, salary distributions by location, and week-over-week skill demand momentum, all visualized in an interactive Streamlit dashboard.”

**Data Scientist / AI Engineer:**  
“I built a salary prediction engine with progressive model comparison tracked in MLflow, plus an NLP-powered skill gap analyzer using embeddings and LLM-assisted recommendations.”

---

## 🚀 Future Roadmap

### Agentic Career Copilot

Planned enhancement using a multi-agent architecture:

- **Ingestion Agent** — Collects and refreshes job data  
- **Skill Analysis Agent** — Extracts demand trends and market insights  
- **Resume Agent** — Evaluates user profile against current market data  
- **Roadmap Agent** — Generates personalized upskilling and job-search plans  
- **Supervisor Agent** — Orchestrates agent workflow and response synthesis  

This evolves TalentScope AI from a dashboard into an intelligent career assistant.

---

## License

MIT

---

Built by [Rajasekhar Reddy.B](https://github.com/RajasekharreddyB41)
