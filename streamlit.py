import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.title("Snowflake Table Viewer & Charts")

session = get_active_session()

# Constants
database = "DBT_TARGET_DB"
schema = "DBT_SCHEMA_DBT_SCHEMA_DBT_TARGET_SCHEMA"
tables = ["LIBRARY_GROWTH_MONTHLY", "MIX_OVER_TIME_RATING", "TRENDS"]

# Helper to load a table
def load_table(table_name):
    full_name = f"{database}.{schema}.{table_name}"
    return session.table(full_name).limit(1000).to_pandas()

# LIBRARY_GROWTH_MONTHLY
st.header("📚 Library Growth Monthly")
try:
    df_library = load_table("LIBRARY_GROWTH_MONTHLY")
    st.dataframe(df_library, use_container_width=True)

    st.line_chart(df_library, x="MONTH_START", y="TITLES_ADDED", use_container_width=True)
except Exception as e:
    st.error(f"Could not load LIBRARY_GROWTH_MONTHLY: {e}")

# MIX_OVER_TIME_RATING
st.header("📊 Mix Over Time by Rating Group")
try:
    df_mix = load_table("MIX_OVER_TIME_RATING")
    st.dataframe(df_mix, use_container_width=True)

    st.line_chart(
        df_mix.groupby(["YEAR", "RATING_GROUP"])["TITLES_ADDED"].sum().unstack(),
        use_container_width=True
    )
except Exception as e:
    st.error(f"Could not load MIX_OVER_TIME_RATING: {e}")

# TRENDS
st.header("📈 Trends")
try:
    df_trends = load_table("TRENDS")
    st.dataframe(df_trends, use_container_width=True)

    st.line_chart(df_trends, x="YEAR_MONTH_KEY", y="TITLES_ADDED", use_container_width=True)
except Exception as e:
    st.error(f"Could not load TRENDS: {e}")
