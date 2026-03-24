-- ============================================================
-- TalentScope AI — Database Schema
-- Layered Data Model (Bronze → Silver → Gold)
-- ============================================================

-- ============================================================
-- 1. raw_jobs (Bronze Layer)
-- Stores unprocessed data exactly as received from each source
-- ============================================================
CREATE TABLE IF NOT EXISTS raw_jobs (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    source_job_id   VARCHAR(255),
    raw_title       VARCHAR(500),
    raw_company     VARCHAR(255),
    raw_location    VARCHAR(255),
    raw_salary      VARCHAR(255),
    raw_description TEXT,
    raw_data        JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_jobs_source ON raw_jobs(source);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_ingested ON raw_jobs(ingested_at);

-- ============================================================
-- 2. clean_jobs (Silver Layer)
-- Normalized, deduplicated, validated records
-- ============================================================
CREATE TABLE IF NOT EXISTS clean_jobs (
    id               SERIAL PRIMARY KEY,
    raw_job_id       INT REFERENCES raw_jobs(id),
    title            VARCHAR(255) NOT NULL,
    company          VARCHAR(255),
    location_city    VARCHAR(100),
    location_state   VARCHAR(100),
    location_country VARCHAR(100) DEFAULT 'US',
    is_remote        BOOLEAN DEFAULT FALSE,
    salary_min       NUMERIC,
    salary_max       NUMERIC,
    salary_currency  VARCHAR(10) DEFAULT 'USD',
    experience_level VARCHAR(50),
    employment_type  VARCHAR(50),
    description      TEXT,
    skills_raw       TEXT,
    url              VARCHAR(500),
    posted_date      DATE,
    source           VARCHAR(50),
    cleaned_at       TIMESTAMP DEFAULT NOW(),
    dedup_hash       VARCHAR(64) UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_clean_jobs_title ON clean_jobs(title);
CREATE INDEX IF NOT EXISTS idx_clean_jobs_company ON clean_jobs(company);
CREATE INDEX IF NOT EXISTS idx_clean_jobs_location ON clean_jobs(location_city);
CREATE INDEX IF NOT EXISTS idx_clean_jobs_posted ON clean_jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_clean_jobs_experience ON clean_jobs(experience_level);

-- ============================================================
-- 3. job_features (Gold Layer)
-- ML-ready features extracted from clean records
-- ============================================================
CREATE TABLE IF NOT EXISTS job_features (
    id                  SERIAL PRIMARY KEY,
    clean_job_id        INT REFERENCES clean_jobs(id),
    skills              TEXT[],
    skill_count         INT,
    experience_encoded  INT,
    salary_normalized   NUMERIC,
    location_encoded    VARCHAR(50),
    title_category      VARCHAR(100),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_features_clean_job ON job_features(clean_job_id);
CREATE INDEX IF NOT EXISTS idx_features_title_cat ON job_features(title_category);

-- ============================================================
-- 4. pipeline_runs (Observability)
-- Tracks every pipeline execution
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            SERIAL PRIMARY KEY,
    pipeline_name     VARCHAR(100) NOT NULL,
    source            VARCHAR(50),
    start_time        TIMESTAMP NOT NULL,
    end_time          TIMESTAMP,
    status            VARCHAR(20) DEFAULT 'running',
    records_processed INT DEFAULT 0,
    records_failed    INT DEFAULT 0,
    error_message     TEXT,
    created_at        TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_name ON pipeline_runs(pipeline_name);