"""
TalentScope AI - Browse Jobs (with OPT Filter + Direct Apply)
"""
import streamlit as st
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_engine
from src.utils.analytics import track_event, get_session_id

st.set_page_config(page_title="Browse Jobs | TalentScope AI", layout="wide")
st.title("Browse Jobs")
st.markdown("Filter by OPT status, role, location, and apply directly to company career pages")
st.markdown("---")

# Track page view
track_event("browse_jobs", get_session_id(), "page_view")


# ============================================
# Data Loading
# ============================================

@st.cache_data(ttl=300)
def load_jobs():
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text("""
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
                url,
                posted_date,
                source,
                opt_status,
                opt_confidence,
                sponsor_tier,
                h1b_approvals,
                h1b_sponsorship,
                description
            FROM clean_jobs
            ORDER BY posted_date DESC NULLS LAST
        """), conn)
    return df


df_all = load_jobs()

if df_all.empty:
    st.warning("No jobs in the database yet. Run the pipeline first.")
    st.stop()


# ============================================
# Sidebar Filters
# ============================================

st.sidebar.header("Filters")

# OPT Status filter (the main feature)
st.sidebar.subheader("Work Authorization")

opt_options = {
    "OPT Friendly": "opt_friendly",
    "OPT Unclear": "opt_unclear",
    "Not OPT Friendly": "not_opt_friendly",
    "Unknown": "unknown",
}

selected_opt = st.sidebar.multiselect(
    "OPT Status",
    options=list(opt_options.keys()),
    default=["OPT Friendly"],
    help="Filter jobs by OPT/CPT work authorization friendliness",
)

selected_opt_values = [opt_options[o] for o in selected_opt]

# Sponsor tier filter
tier_options = st.sidebar.multiselect(
    "H-1B Sponsor Tier",
    options=["gold", "silver", "none"],
    default=["gold", "silver"],
    help="Gold = 50+ H-1B approvals, Silver = 1-49, None = not found in USCIS data",
)

st.sidebar.markdown("---")
# Freshness filter
freshness_options = {
    "Last 24 hours": 1,
    "Last 3 days": 3,
    "Last 10 days": 10,
    "Last 30 days": 30,
    "All jobs": 9999,
}
selected_freshness = st.sidebar.selectbox(
    "Posted within",
    options=list(freshness_options.keys()),
    index=2,
)
freshness_days = freshness_options[selected_freshness]

st.sidebar.markdown("---")
st.sidebar.subheader("Job Details")

# Role / title search
title_search = st.sidebar.text_input(
    "Search job title",
    placeholder="e.g. Data Engineer, ML, Analyst",
)

# Company search
company_search = st.sidebar.text_input(
    "Search company",
    placeholder="e.g. Google, Airbnb, Stripe",
)

# Location
locations = sorted(df_all["location_state"].dropna().unique())
selected_locations = st.sidebar.multiselect(
    "State / Region",
    options=locations,
)

# Remote filter
remote_filter = st.sidebar.selectbox(
    "Remote",
    options=["All", "Remote Only", "Not Remote"],
)

# Experience level
exp_levels = sorted(df_all["experience_level"].dropna().unique())
selected_exp = st.sidebar.multiselect(
    "Experience Level",
    options=exp_levels,
)

# Source filter
sources = sorted(df_all["source"].dropna().unique())
selected_sources = st.sidebar.multiselect(
    "Data Source",
    options=sources,
)


# ============================================
# Apply Filters
# ============================================

df = df_all.copy()

# Freshness filter
if freshness_days < 9999:
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=freshness_days)
    df = df[df["posted_date"] >= cutoff]

# OPT filter
if selected_opt_values:
    df = df[df["opt_status"].isin(selected_opt_values)]

# Sponsor tier
if tier_options:
    df = df[df["sponsor_tier"].isin(tier_options)]

# Title search
if title_search:
    df = df[df["title"].str.contains(title_search, case=False, na=False)]

# Company search
if company_search:
    df = df[df["company"].str.contains(company_search, case=False, na=False)]

# Location
if selected_locations:
    df = df[df["location_state"].isin(selected_locations)]


# Remote
if remote_filter == "Remote Only":
    df = df[df["is_remote"] == True]
elif remote_filter == "Not Remote":
    df = df[df["is_remote"] != True]

# Experience
if selected_exp:
    df = df[df["experience_level"].isin(selected_exp)]

# Source
if selected_sources:
    df = df[df["source"].isin(selected_sources)]


# ============================================
# Stats Bar
# ============================================

st.subheader("Results")

c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Jobs", f"{len(df):,}")

opt_friendly_count = len(df[df["opt_status"] == "opt_friendly"])
c2.metric("OPT Friendly", f"{opt_friendly_count:,}")

gold_count = len(df[df["sponsor_tier"] == "gold"])
c3.metric("Gold Sponsors", f"{gold_count:,}")

has_salary = len(df[df["salary_min"].notna() & (df["salary_min"] > 0)])
c4.metric("With Salary", f"{has_salary:,}")

st.markdown("---")


# ============================================
# OPT Status Badge Helper
# ============================================

def opt_badge(status, confidence, tier, h1b_approvals):
    """Return a colored badge string for OPT status."""
    if status == "opt_friendly":
        badge = "OPT Friendly"
        color = "#27AE60"
    elif status == "not_opt_friendly":
        badge = "Not OPT Friendly"
        color = "#E74C3C"
    elif status == "opt_unclear":
        badge = "OPT Unclear"
        color = "#F39C12"
    else:
        badge = "Unknown"
        color = "#95A5A6"

    tier_text = ""
    if tier == "gold":
        tier_text = f" | H-1B Gold ({h1b_approvals:,} approvals)"
    elif tier == "silver":
        tier_text = f" | H-1B Silver ({h1b_approvals:,} approvals)"

    return f'<span style="background-color:{color}; color:white; padding:2px 8px; border-radius:4px; font-size:0.85em;">{badge}</span><span style="color:#666; font-size:0.8em;">{tier_text}</span>'


def salary_display(sal_min, sal_max):
    """Format salary range for display."""
    if pd.notna(sal_min) and sal_min > 0:
        if pd.notna(sal_max) and sal_max > 0 and sal_max != sal_min:
            return f"${sal_min:,.0f} - ${sal_max:,.0f}"
        return f"${sal_min:,.0f}"
    return ""


# ============================================
# Job Listings
# ============================================

JOBS_PER_PAGE = 25
total_pages = max(1, (len(df) + JOBS_PER_PAGE - 1) // JOBS_PER_PAGE)

page = st.number_input(
    "Page",
    min_value=1,
    max_value=total_pages,
    value=1,
    step=1,
)

start_idx = (page - 1) * JOBS_PER_PAGE
end_idx = start_idx + JOBS_PER_PAGE
df_page = df.iloc[start_idx:end_idx]

st.caption(f"Showing {start_idx + 1}-{min(end_idx, len(df))} of {len(df):,} jobs  |  Page {page} of {total_pages}")

for _, job in df_page.iterrows():
    with st.container():
        col_info, col_apply = st.columns([4, 1])

        with col_info:
            # Title + company
            title = job["title"] or "Untitled"
            company = job["company"] or "Unknown"
            st.markdown(f"**{title}**  \n{company}", unsafe_allow_html=False)

            # Location + remote
            loc_parts = []
            if pd.notna(job["location_city"]) and job["location_city"]:
                loc_parts.append(job["location_city"])
            if pd.notna(job["location_state"]) and job["location_state"]:
                loc_parts.append(job["location_state"])
            location = ", ".join(loc_parts) if loc_parts else "Location not specified"
            if job.get("is_remote"):
                location += " (Remote)"

            # Salary
            salary = salary_display(job.get("salary_min"), job.get("salary_max"))

            # Experience
            exp = job.get("experience_level", "")
            exp_text = f" | {exp}" if exp else ""

            st.markdown(f"{location}{exp_text}")
            if salary:
                st.markdown(f"**{salary}**")

            # OPT badge
            badge = opt_badge(
                job.get("opt_status", "unknown"),
                job.get("opt_confidence", "low"),
                job.get("sponsor_tier", "none"),
                int(job.get("h1b_approvals", 0) or 0),
            )
            st.markdown(badge, unsafe_allow_html=True)

            # Posted date + source
            posted = job.get("posted_date", "")
            source = job.get("source", "")
            meta_parts = []
            if posted:
                meta_parts.append(f"Posted: {posted}")
            if source:
                meta_parts.append(f"Source: {source}")
            if meta_parts:
                st.caption(" | ".join(meta_parts))

        with col_apply:
            url = job.get("url", "")
            if url and str(url).startswith("http"):
                st.link_button("Apply Now", url, use_container_width=True)
            else:
                st.button("No Link", disabled=True, key=f"no_link_{job['id']}")

        st.markdown("---")


# ============================================
# Footer stats
# ============================================

with st.expander("OPT Distribution in Current Results"):
    if not df.empty:
        opt_dist = df["opt_status"].value_counts()
        tier_dist = df["sponsor_tier"].value_counts()

        fc1, fc2 = st.columns(2)
        with fc1:
            st.markdown("**OPT Status**")
            for status, count in opt_dist.items():
                pct = count / len(df) * 100
                label = status.replace("_", " ").title()
                st.markdown(f"- {label}: {count:,} ({pct:.1f}%)")
        with fc2:
            st.markdown("**Sponsor Tier**")
            for tier, count in tier_dist.items():
                pct = count / len(df) * 100
                label = tier.title()
                st.markdown(f"- {label}: {count:,} ({pct:.1f}%)")