-- ============================================================
-- TalentScope AI — SQL Analytics & KPIs
-- Production queries: CTEs, Window Functions, Subqueries
-- ============================================================


-- ============================================================
-- Q1: Top 15 In-Demand Skills by Job Count
-- Technique: UNNEST array, GROUP BY, ORDER BY
-- ============================================================
-- (This will be done in Python since skills need extraction)


-- ============================================================
-- Q2: Salary Distribution by Experience Level
-- Technique: GROUP BY with aggregate functions
-- ============================================================
SELECT 
    experience_level,
    COUNT(*) AS job_count,
    ROUND(AVG(salary_min)) AS avg_min_salary,
    ROUND(AVG(salary_max)) AS avg_max_salary,
    ROUND(AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2)) AS avg_mid_salary,
    ROUND(MIN(salary_min)) AS lowest_salary,
    ROUND(MAX(salary_max)) AS highest_salary
FROM clean_jobs
WHERE salary_min IS NOT NULL
GROUP BY experience_level
ORDER BY avg_mid_salary DESC;


-- ============================================================
-- Q3: Top Hiring Companies with Salary Benchmarks
-- Technique: CTE + Window Function (RANK)
-- ============================================================
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
    HAVING COUNT(*) >= 3
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY job_count DESC) AS hiring_rank
    FROM company_stats
)
SELECT 
    hiring_rank,
    company,
    job_count,
    avg_min,
    avg_max,
    remote_count
FROM ranked
WHERE hiring_rank <= 20
ORDER BY hiring_rank;


-- ============================================================
-- Q4: Salary by Location (Top Cities)
-- Technique: CTE + HAVING + ORDER BY
-- ============================================================
WITH city_salaries AS (
    SELECT 
        location_city,
        location_state,
        COUNT(*) AS job_count,
        ROUND(AVG(salary_min)) AS avg_min,
        ROUND(AVG(salary_max)) AS avg_max,
        ROUND(AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2)) AS avg_mid
    FROM clean_jobs
    WHERE salary_min IS NOT NULL 
      AND location_city IS NOT NULL
    GROUP BY location_city, location_state
    HAVING COUNT(*) >= 3
)
SELECT *
FROM city_salaries
ORDER BY avg_mid DESC
LIMIT 15;


-- ============================================================
-- Q5: Experience Level Distribution with Salary Percentiles
-- Technique: Window Function (PERCENT_RANK)
-- ============================================================
WITH salary_percentiles AS (
    SELECT
        experience_level,
        salary_min,
        PERCENT_RANK() OVER (
            PARTITION BY experience_level 
            ORDER BY salary_min
        ) AS percentile
    FROM clean_jobs
    WHERE salary_min IS NOT NULL
)
SELECT 
    experience_level,
    COUNT(*) AS total_jobs,
    ROUND(MIN(salary_min)) AS p0_min,
    ROUND(AVG(CASE WHEN percentile <= 0.25 THEN salary_min END)) AS p25_salary,
    ROUND(AVG(CASE WHEN percentile BETWEEN 0.45 AND 0.55 THEN salary_min END)) AS p50_salary,
    ROUND(AVG(CASE WHEN percentile >= 0.75 THEN salary_min END)) AS p75_salary,
    ROUND(MAX(salary_min)) AS p100_max
FROM salary_percentiles
GROUP BY experience_level
ORDER BY p50_salary DESC;


-- ============================================================
-- Q6: Hiring Velocity — Jobs Posted per Week
-- Technique: DATE_TRUNC + Window Function (LAG for WoW change)
-- ============================================================
WITH weekly_counts AS (
    SELECT 
        DATE_TRUNC('week', posted_date)::DATE AS week_start,
        COUNT(*) AS jobs_posted
    FROM clean_jobs
    WHERE posted_date IS NOT NULL
    GROUP BY DATE_TRUNC('week', posted_date)
    ORDER BY week_start
)
SELECT 
    week_start,
    jobs_posted,
    LAG(jobs_posted) OVER (ORDER BY week_start) AS prev_week,
    jobs_posted - LAG(jobs_posted) OVER (ORDER BY week_start) AS wow_change,
    ROUND(
        (jobs_posted - LAG(jobs_posted) OVER (ORDER BY week_start))::NUMERIC 
        / NULLIF(LAG(jobs_posted) OVER (ORDER BY week_start), 0) * 100, 
    1) AS wow_pct_change
FROM weekly_counts
ORDER BY week_start DESC
LIMIT 20;


-- ============================================================
-- Q7: Remote vs On-Site Analysis
-- Technique: CASE + GROUP BY + Subquery
-- ============================================================
SELECT
    CASE WHEN is_remote THEN 'Remote' ELSE 'On-Site' END AS work_type,
    COUNT(*) AS job_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM clean_jobs), 1) AS pct_of_total,
    ROUND(AVG(salary_min)) AS avg_min_salary,
    ROUND(AVG(salary_max)) AS avg_max_salary
FROM clean_jobs
GROUP BY is_remote
ORDER BY job_count DESC;


-- ============================================================
-- Q8: Job Title Category Analysis
-- Technique: CASE with pattern matching + CTE
-- ============================================================
WITH categorized AS (
    SELECT *,
        CASE 
            WHEN LOWER(title) LIKE '%data engineer%' THEN 'Data Engineer'
            WHEN LOWER(title) LIKE '%data analyst%' THEN 'Data Analyst'
            WHEN LOWER(title) LIKE '%data scientist%' THEN 'Data Scientist'
            WHEN LOWER(title) LIKE '%machine learning%' OR LOWER(title) LIKE '%ml engineer%' THEN 'ML Engineer'
            WHEN LOWER(title) LIKE '%software engineer%' OR LOWER(title) LIKE '%software developer%' THEN 'Software Engineer'
            WHEN LOWER(title) LIKE '%python%' THEN 'Python Developer'
            WHEN LOWER(title) LIKE '%full stack%' OR LOWER(title) LIKE '%fullstack%' THEN 'Full Stack Developer'
            WHEN LOWER(title) LIKE '%devops%' OR LOWER(title) LIKE '%cloud%' THEN 'DevOps/Cloud'
            WHEN LOWER(title) LIKE '%business intelligence%' OR LOWER(title) LIKE '%bi %' THEN 'BI Analyst'
            WHEN LOWER(title) LIKE '%analytics%' THEN 'Analytics'
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
ORDER BY job_count DESC;


-- ============================================================
-- Q9: Company Salary Ranking within Each Experience Level
-- Technique: Window Function (DENSE_RANK, PARTITION BY)
-- ============================================================
WITH company_exp_salary AS (
    SELECT
        company,
        experience_level,
        COUNT(*) AS job_count,
        ROUND(AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2)) AS avg_salary,
        DENSE_RANK() OVER (
            PARTITION BY experience_level 
            ORDER BY AVG((COALESCE(salary_min, 0) + COALESCE(salary_max, 0)) / 2) DESC
        ) AS salary_rank
    FROM clean_jobs
    WHERE salary_min IS NOT NULL 
      AND company IS NOT NULL AND company != ''
    GROUP BY company, experience_level
    HAVING COUNT(*) >= 2
)
SELECT *
FROM company_exp_salary
WHERE salary_rank <= 5
ORDER BY experience_level, salary_rank;


-- ============================================================
-- Q10: Pipeline Health — Run History
-- Technique: Simple analytics on pipeline_runs
-- ============================================================
SELECT 
    pipeline_name,
    COUNT(*) AS total_runs,
    COUNT(CASE WHEN status = 'success' THEN 1 END) AS successes,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failures,
    ROUND(
        100.0 * COUNT(CASE WHEN status = 'success' THEN 1 END) / COUNT(*), 
    1) AS success_rate,
    SUM(records_processed) AS total_records,
    ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time))), 1) AS avg_duration_sec
FROM pipeline_runs
GROUP BY pipeline_name
ORDER BY total_runs DESC;