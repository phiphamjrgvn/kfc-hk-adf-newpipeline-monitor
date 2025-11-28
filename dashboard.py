# dashboard.py
import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
from config import SUMMARY_FILE, DIFF_FILE

# -------------------------------------------------------
# STREAMLIT PAGE CONFIG
# -------------------------------------------------------
st.set_page_config(
    page_title="KFC HK Pipeline Validation Dashboard",
    layout="wide"
)

st.title("🍗 KFCHK Pipeline Daily Validation Dashboard")
st.caption("Monitor differences between DAP and New Pipeline (CTI → PosSaleItem)")

# -------------------------------------------------------
# LOAD DATA
# -------------------------------------------------------
summary = pl.read_csv(SUMMARY_FILE)
detail = pl.read_csv(DIFF_FILE)

summary = summary.sort("date")
detail = detail.sort("date")

df_summary = summary.to_pandas()
df_detail = detail.to_pandas()

required_cols = ["dap_total_items", "new_total_items", "diff_item_count", "diff_percent"]
for col in required_cols:
    if col not in df_summary.columns:
        st.error(f"❌ Missing column in summary.csv: {col}")
        st.stop()

# -------------------------------------------------------
# TABS
# -------------------------------------------------------
tab1, tab2 = st.tabs(["📊 Summary", "🔍 Detail by Day"])

# =======================================================
# TAB 1 — SUMMARY (LINE CHARTS)
# =======================================================
with tab1:
    st.header("📊 Summary Overview")

    # -------------------------
    # CHART 1 — DAP (line) + NEW (bar)
    # -------------------------
    st.subheader("📊 Total Final Rows per Day (Pipeline Output)")

    chart1 = (
        alt.Chart(df_summary)
        .mark_bar(color="#ff7f0e", opacity=0.8)
        .encode(
            x=alt.X("date:N", sort=None, title="Date"),
            y=alt.Y("final_df:Q", title="Final Pipeline Rows"),
            tooltip=["date", "final_df"]
        )
        .properties(width="container", height=350)
    )

    st.altair_chart(chart1, use_container_width=True)




    # -------------------------
    # CHART 2 — DAP (line) + DISTINCT ITEM COUNT (bar)
    # -------------------------
    st.subheader("🟧 DAP Rows vs Distinct Items (Line + Bar)")

    base2 = alt.Chart(df_summary).encode(
        x=alt.X("date:N", sort=None, title="Date")
    )

    bar_distinct = base2.mark_bar(color="#ff7f0e", opacity=0.7).encode(
        y=alt.Y("new_total_items:Q", title="Distinct Items (New Pipeline)"),
        tooltip=["date", "new_total_items"]
    )

    line_dap2 = base2.mark_line(color="#1f77b4", strokeWidth=3, point=True).encode(
        y=alt.Y("dap_total_items:Q", title="Row Count (DAP)"),
        tooltip=["date", "dap_total_items"]
    )

    chart2 = alt.layer(bar_distinct, line_dap2).resolve_scale(
        y='independent'
    ).properties(width="container", height=350)

    st.altair_chart(chart2, use_container_width=True)



    # -------------------------
    # TABLE — COLOR DIFF COLUMN
    # -------------------------
    st.subheader("📄 Full Summary Table (with diff coloring)")

    df_sum = df_summary.copy()

    def color_diff_summary(val):
        return (
            "background-color: #b6f2bb" if val == 0 else "background-color: #f2b6b6"
        )

    styled_summary = df_sum.style.applymap(color_diff_summary, subset=["diff_item_count"])

    st.dataframe(styled_summary, use_container_width=True)

# =======================================================
# TAB 2 — DETAIL BY DAY
# =======================================================
with tab2:
    st.header("🔍 Diff Items by Day")

    unique_days = sorted(detail["date"].unique().to_list())
    selected_day = st.selectbox("Select date:", unique_days)

    filtered = detail.filter(pl.col("date") == selected_day)
    df_filtered = filtered.to_pandas()

    st.write(f"🔢 Total products on {selected_day}: **{df_filtered.shape[0]}**")

    def color_diff(val):
        return (
            "background-color: #b6f2bb" if val == 0 else "background-color: #f2b6b6"
        )

    styled = df_filtered.style.applymap(color_diff, subset=["diff"])
    st.dataframe(styled, use_container_width=True)
