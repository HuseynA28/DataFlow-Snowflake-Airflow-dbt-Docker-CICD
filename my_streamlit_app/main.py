# main.py  –  Open Library Dashboard
import streamlit as st
import pandas as pd

# -----------------------------------------------------------
# 1️⃣  Page‑level configuration
# -----------------------------------------------------------
st.set_page_config(
    page_title="Open Library Dashboard",
    page_icon="📚",
    layout="wide",
)

st.title("📚 Open Library Growth Dashboard")

# -----------------------------------------------------------
# 2️⃣  Snowflake connection (uses .streamlit/secrets.toml)
# -----------------------------------------------------------
conn = st.connection("snowflake")   # looks for [connections.snowflake] in secrets
session = conn.session()            # Snowpark Session

# -----------------------------------------------------------
# 3️⃣  Constants
# -----------------------------------------------------------
DB = "DBT_TARGET_DB"
SC = "DBT_SCHEMA_DBT_SCHEMA_DBT_TARGET_SCHEMA"
TABLES = {
    "LIBRARY_GROWTH_MONTHLY": {"x": "MONTH_START", "y": "TITLES_ADDED"},
    "MIX_OVER_TIME_RATING":   {"x": None, "y": None},          # handled separately
    "TRENDS":                 {"x": "YEAR_MONTH_KEY", "y": "TITLES_ADDED"},
}

# -----------------------------------------------------------
# 4️⃣  Data‑loader helpers (cached for 1 hour)
# -----------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def load_table(table, limit=1_000):
    """Return a pandas DataFrame from Snowflake (limited rows)."""
    return (
        session
        .table(f"{DB}.{SC}.{table}")
        .limit(limit)
        .to_pandas()
    )

# -----------------------------------------------------------
# 5️⃣  Dashboard Tabs
# -----------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Library Growth", "Mix over Time", "Trends"])

# -- Library Growth Monthly ---------------------------------
with tab1:
    st.header("📈 Library Growth per Month")
    try:
        df = load_table("LIBRARY_GROWTH_MONTHLY")
        st.line_chart(df, x="MONTH_START", y="TITLES_ADDED", use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load LIBRARY_GROWTH_MONTHLY: {e}")

# -- Mix Over Time by Rating Group --------------------------
with tab2:
    st.header("📊 Titles Added by Rating Group & Year")
    try:
        df = load_table("MIX_OVER_TIME_RATING")
        # Pivot for multiple lines (one per RATING_GROUP)
        pivot = (
            df.groupby(["YEAR", "RATING_GROUP"])["TITLES_ADDED"]
            .sum()
            .unstack(fill_value=0)
            .sort_index()
        )
        st.line_chart(pivot, use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load MIX_OVER_TIME_RATING: {e}")

# -- Trends -------------------------------------------------
with tab3:
    st.header("📉 Overall Trends")
    try:
        df = load_table("TRENDS")
        st.line_chart(df, x="YEAR_MONTH_KEY", y="TITLES_ADDED", use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Could not load TRENDS: {e}")
