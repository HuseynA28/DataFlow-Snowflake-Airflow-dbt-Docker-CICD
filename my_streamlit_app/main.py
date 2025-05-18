# main.py  –  Dashboard from local CSV snapshots
import streamlit as st
import pandas as pd
from pathlib import Path

# ───────────────────────────────────────────────────────────────
# 1️⃣  Page config
# ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Open Library Dashboard (CSV)", page_icon="📚", layout="wide")
st.title("📚 Open Library Growth Dashboard (CSV snapshots)")

DATA_DIR = Path(__file__).parent / "data"   # points to ./data

# ───────────────────────────────────────────────────────────────
# 2️⃣  CSV loader helper (cached for 1 hour)
# ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_csv(filename, parse_date_cols=None):
    """Load CSV from the data folder into a pandas DataFrame."""
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    df = pd.read_csv(path)
    if parse_date_cols:
        for col in parse_date_cols:
            df[col] = pd.to_datetime(df[col])
    return df

# ───────────────────────────────────────────────────────────────
# 3️⃣  Tabs
# ───────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    ["Library Growth", "Mix over Time", "Trends", "Netflix Titles (extra)"]
)

# -- Library Growth Monthly ------------------------------------
with tab1:
    st.header("📈 Library Growth per Month")
    try:
        df = load_csv("LIBRARY_GROWTH_MONTHLY.csv", parse_date_cols=["MONTH_START"])
        st.line_chart(df, x="MONTH_START", y="TITLES_ADDED", use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading LIBRARY_GROWTH_MONTHLY.csv: {e}")

# -- Mix Over Time by Rating Group -----------------------------
with tab2:
    st.header("📊 Titles Added by Rating Group & Year")
    try:
        df = load_csv("MIX_OVER_TIME_RATING.csv")
        pivot = (
            df.groupby(["YEAR", "RATING_GROUP"])["TITLES_ADDED"]
            .sum().unstack(fill_value=0).sort_index()
        )
        st.line_chart(pivot, use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading MIX_OVER_TIME_RATING.csv: {e}")

# -- Trends -----------------------------------------------------
with tab3:
    st.header("📉 Overall Trends")
    try:
        df = load_csv("TRENDS.csv", parse_date_cols=["YEAR_MONTH_KEY"])
        st.line_chart(df, x="YEAR_MONTH_KEY", y="TITLES_ADDED", use_container_width=True)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading TRENDS.csv: {e}")

# -- Netflix Titles (optional extra) ---------------------------
with tab4:
    st.header("🎬 Netflix Titles (example extra table)")
    try:
        df = load_csv("netflix_titles.csv")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading netflix_titles.csv: {e}")
