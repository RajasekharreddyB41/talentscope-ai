import pandas as pd
import streamlit as st
from sqlalchemy import text

from src.database.connection import get_engine

st.set_page_config(page_title="Browse Jobs", page_icon="💼", layout="wide")

engine = get_engine()

st.title("💼 Browse Jobs")
st.caption("Fresh internships, entry-level, and early-career roles from the last 7 days")


def classify_job_level(title: str, experience_level: str) -> str:
    text_value = f"{title or ''} {experience_level or ''}".lower()

    if "intern" in text_value or "internship" in text_value:
        return "internship"
    if any(
        x in text_value
        for x in ["entry level", "junior", "new grad", "graduate", "trainee", "apprentice", "associate"]
    ):
        return "entry_level"
    if "lead" in text_value:
        return "lead"
    if "senior" in text_value:
        return "senior"
    if "mid" in text_value:
        return "mid"
    return "unknown"


@st.cache_data(ttl=3600)
def load_recent_jobs(days: int = 7) -> pd.DataFrame:
    query = text("""
    SELECT
        id,
        title,
        company,
        location_city,
        location_state,
        location_country,
        is_remote,
        salary_min,
        salary_max,
        experience_level,
        employment_type,
        description,
        skills_raw,
        url AS apply_url,
        posted_date,
        source
    FROM clean_jobs
    WHERE posted_date >= CURRENT_DATE - :days
      AND url IS NOT NULL
      AND LENGTH(url) > 5
    ORDER BY posted_date DESC
    LIMIT 500
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"days": days})

    if df.empty:
        return df

    df["job_level_bucket"] = df.apply(
        lambda row: classify_job_level(row.get("title"), row.get("experience_level")),
        axis=1,
    )

    df["location_display"] = (
        df["location_city"].fillna("").astype(str).str.strip()
        + ", "
        + df["location_state"].fillna("").astype(str).str.strip()
    ).str.strip(", ").str.strip()

    df.loc[df["location_display"] == "", "location_display"] = (
        df["location_country"].fillna("Unknown").astype(str)
    )

    return df


st.sidebar.header("Filters")

days_filter = st.sidebar.selectbox("Posted within", [1, 3, 7], index=2)
keyword = st.sidebar.text_input("Keyword search")
selected_sources = st.sidebar.multiselect("Source", ["jsearch", "adzuna"], default=["jsearch", "adzuna"])
remote_only = st.sidebar.checkbox("Remote only")
entry_only = st.sidebar.checkbox("Entry-level only", value=True)
internship_only = st.sidebar.checkbox("Internship only")
selected_state = st.sidebar.text_input("State")
employment_type = st.sidebar.selectbox(
    "Employment type",
    ["All", "full-time", "part-time", "contract", "internship"],
    index=0,
)

df = load_recent_jobs(days_filter)

if df.empty:
    st.warning("No recent jobs found.")
    st.stop()

filtered = df.copy()

if keyword:
    keyword_lower = keyword.lower()
    filtered = filtered[
        filtered["title"].fillna("").str.lower().str.contains(keyword_lower)
        | filtered["company"].fillna("").str.lower().str.contains(keyword_lower)
        | filtered["description"].fillna("").str.lower().str.contains(keyword_lower)
    ]

if selected_sources:
    filtered = filtered[filtered["source"].isin(selected_sources)]

if remote_only:
    filtered = filtered[filtered["is_remote"] == True]

if entry_only:
    filtered = filtered[filtered["job_level_bucket"] == "entry_level"]

if internship_only:
    filtered = filtered[filtered["job_level_bucket"] == "internship"]

if selected_state:
    filtered = filtered[
        filtered["location_state"].fillna("").str.lower() == selected_state.strip().lower()
    ]

if employment_type != "All":
    filtered = filtered[
        filtered["employment_type"].fillna("").str.lower() == employment_type.lower()
    ]

c1, c2, c3 = st.columns(3)
c1.metric("Jobs Found", len(filtered))
c2.metric("Sources", filtered["source"].nunique())
c3.metric("Remote Jobs", int(filtered["is_remote"].fillna(False).sum()))

st.divider()

if filtered.empty:
    st.info("No jobs matched your filters.")
    st.stop()

st.dataframe(filtered[["title", "company", "source", "apply_url"]].head(10))

for _, job in filtered.iterrows():
    with st.container():
        col1, col2 = st.columns([5, 1])

        with col1:
            st.subheader(job["title"] or "Untitled Role")
            st.write(f"**Company:** {job['company'] or 'Unknown'}")

            meta_parts = [
                f"📍 {job['location_display']}",
                f"🗓️ {job['posted_date']}",
                f"🏷️ {job['job_level_bucket']}",
                f"🧩 {job['employment_type'] or 'unknown'}",
                f"🌐 {job['source']}",
            ]

            if bool(job.get("is_remote")):
                meta_parts.append("💻 Remote")

            st.caption(" | ".join(meta_parts))

            if pd.notna(job["salary_min"]) or pd.notna(job["salary_max"]):
                salary_min = f"${int(job['salary_min']):,}" if pd.notna(job["salary_min"]) else "?"
                salary_max = f"${int(job['salary_max']):,}" if pd.notna(job["salary_max"]) else "?"
                st.write(f"**Salary:** {salary_min} - {salary_max}")

            if pd.notna(job["skills_raw"]) and str(job["skills_raw"]).strip():
                st.write(f"**Skills:** {str(job['skills_raw'])[:200]}")

            with st.expander("View details"):
                st.write(job["description"] or "No description available.")

        with col2:
            st.link_button("Apply Now", job["apply_url"], use_container_width=True)

        st.divider()