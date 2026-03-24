"""
TalentScope AI — SQL Analytics Runner
Executes analytics queries and returns results as DataFrames.
"""

import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine
from src.utils.logger import get_logger

logger = get_logger("analysis.sql")


def run_query(query: str, params: dict = None) -> pd.DataFrame:
    """Execute a SQL query and return as DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        result = pd.read_sql(text(query), conn, params=params)
    return result


def salary_by_experience() -> pd.DataFrame:
    """Q2: Salary distribution by experience level."""
    return run_query("""
        SELECT 
            experience_level,
            COUNT(*) AS job_count,
            ROUND(AVG(salary_min)) AS avg_min_salary,
            ROUND(AVG(salary_max)) AS avg_max_salary,
            ROUND(AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2)) AS avg_mid_salary
        FROM clean_jobs
        WHERE salary_min IS NOT NULL
        GROUP BY experience_level
        ORDER BY avg_mid_salary DESC
    """)


def top_hiring_companies(min_jobs: int = 3) -> pd.DataFrame:
    """Q3: Top hiring companies with salary benchmarks."""
    return run_query("""
        WITH company_stats AS (
            SELECT 
                company,
                COUNT(*) AS job_count,
                ROUND(AVG(salary_min)) AS avg_min,
                ROUND(AVG(salary_max)) AS avg_max,
                COUNT(CASE WHEN is_remote THEN 1 END) AS remote_count
            FROM clean_jobs
            WHERE company IS NOT NULL AND company != ''
            GROUP BY company
            HAVING COUNT(*) >= :min_jobs
        )
        SELECT *,
            RANK() OVER (ORDER BY job_count DESC) AS hiring_rank
        FROM company_stats
        ORDER BY job_count DESC
        LIMIT 20
    """, {"min_jobs": min_jobs})


def salary_by_city(min_jobs: int = 3) -> pd.DataFrame:
    """Q4: Salary by location (top cities)."""
    return run_query("""
        SELECT 
            location_city,
            location_state,
            COUNT(*) AS job_count,
            ROUND(AVG(salary_min)) AS avg_min,
            ROUND(AVG(salary_max)) AS avg_max
        FROM clean_jobs
        WHERE salary_min IS NOT NULL AND location_city IS NOT NULL
        GROUP BY location_city, location_state
        HAVING COUNT(*) >= :min_jobs
        ORDER BY AVG(salary_max) DESC
        LIMIT 15
    """, {"min_jobs": min_jobs})


def hiring_velocity() -> pd.DataFrame:
    """Q6: Jobs posted per week with WoW change."""
    return run_query("""
        WITH weekly_counts AS (
            SELECT 
                DATE_TRUNC('week', posted_date)::DATE AS week_start,
                COUNT(*) AS jobs_posted
            FROM clean_jobs
            WHERE posted_date IS NOT NULL
            GROUP BY DATE_TRUNC('week', posted_date)
        )
        SELECT 
            week_start,
            jobs_posted,
            LAG(jobs_posted) OVER (ORDER BY week_start) AS prev_week,
            jobs_posted - LAG(jobs_posted) OVER (ORDER BY week_start) AS wow_change
        FROM weekly_counts
        ORDER BY week_start DESC
        LIMIT 20
    """)


def title_category_analysis() -> pd.DataFrame:
    """Q8: Job title category breakdown."""
    return run_query("""
        WITH categorized AS (
            SELECT *,
                CASE 
                    WHEN LOWER(title) LIKE '%%data engineer%%' THEN 'Data Engineer'
                    WHEN LOWER(title) LIKE '%%data analyst%%' THEN 'Data Analyst'
                    WHEN LOWER(title) LIKE '%%data scientist%%' THEN 'Data Scientist'
                    WHEN LOWER(title) LIKE '%%machine learning%%' OR LOWER(title) LIKE '%%ml engineer%%' THEN 'ML Engineer'
                    WHEN LOWER(title) LIKE '%%software engineer%%' OR LOWER(title) LIKE '%%software developer%%' THEN 'Software Engineer'
                    WHEN LOWER(title) LIKE '%%python%%' THEN 'Python Developer'
                    WHEN LOWER(title) LIKE '%%full stack%%' OR LOWER(title) LIKE '%%fullstack%%' THEN 'Full Stack'
                    WHEN LOWER(title) LIKE '%%devops%%' OR LOWER(title) LIKE '%%cloud%%' THEN 'DevOps/Cloud'
                    WHEN LOWER(title) LIKE '%%business intelligence%%' THEN 'BI Analyst'
                    WHEN LOWER(title) LIKE '%%analytics%%' THEN 'Analytics'
                    ELSE 'Other Tech'
                END AS title_category
            FROM clean_jobs
        )
        SELECT 
            title_category,
            COUNT(*) AS job_count,
            ROUND(AVG(salary_min)) AS avg_min,
            ROUND(AVG(salary_max)) AS avg_max,
            ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM categorized), 1) AS pct
        FROM categorized
        GROUP BY title_category
        ORDER BY job_count DESC
    """)


def pipeline_health() -> pd.DataFrame:
    """Q10: Pipeline run history."""
    return run_query("""
        SELECT 
            pipeline_name,
            COUNT(*) AS total_runs,
            COUNT(CASE WHEN status = 'success' THEN 1 END) AS successes,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failures,
            SUM(records_processed) AS total_records,
            ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time)))::NUMERIC, 1) AS avg_duration_sec
        FROM pipeline_runs
        GROUP BY pipeline_name
        ORDER BY total_runs DESC
    """)


if __name__ == "__main__":
    print("=== TalentScope AI — Analytics Preview ===\n")

    print("--- Salary by Experience Level ---")
    print(salary_by_experience().to_string(index=False))

    print("\n--- Top Hiring Companies ---")
    print(top_hiring_companies().to_string(index=False))

    print("\n--- Salary by City ---")
    print(salary_by_city().to_string(index=False))

    print("\n--- Title Category Analysis ---")
    print(title_category_analysis().to_string(index=False))

    print("\n--- Pipeline Health ---")
    print(pipeline_health().to_string(index=False))