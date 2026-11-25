"""
STEP 3 - DASHBOARD (Streamlit)
Reads the folder  payment/output  made by Spark.
Run:  streamlit run 3_dashboard.py     then open  http://localhost:8501
"""
import os
import pandas as pd
import plotly.express as px
import streamlit as st

# ===== REPLACE with the output folder Spark made, e.g. "/home/YOURNAME/payment/output" =====
OUTPUT_DIR = ""     # leave "" to use the "output" folder next to this file
# ====================================================================================
if not OUTPUT_DIR:
    OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

st.set_page_config(page_title="Payment Gateway Dashboard", layout="wide")
st.title("💳 Payment Gateway Dashboard")
st.caption("Data flow: CSV → Hadoop (HDFS) → PySpark → this dashboard")

if not os.path.exists(OUTPUT_DIR):
    st.error("Folder 'output' not found. Run the Spark step first: 2_spark_process.py")
    st.stop()

@st.cache_data
def load(name):
    return pd.read_parquet(os.path.join(OUTPUT_DIR, name))

kpi, summary = load("kpi").iloc[0], load("summary").iloc[0]
daily, gateway, method = load("daily"), load("gateway"), load("method")
hourly, bank, city = load("hourly"), load("bank"), load("city")
category, gw_method = load("category"), load("gateway_method")
failures, status, dq = load("failure_reasons"), load("status"), load("dq_report")

# ---------------- Top numbers
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Transactions", f"{int(kpi.txns):,}")
c2.metric("Success Rate", f"{kpi.success_rate:.2f}%")
c3.metric("Total Volume (INR)", f"₹{kpi.volume / 1e9:,.2f} B")
c4.metric("Gateway Fees", f"₹{kpi.fee / 1e6:,.1f} M")
c5.metric("Avg Processing", f"{kpi.avg_ms:,.0f} ms")

# ---------------- Monthly trend
daily["txn_date"] = pd.to_datetime(daily["txn_date"])
monthly = daily.set_index("txn_date").resample("MS")[["txns", "volume"]].sum().reset_index()
st.plotly_chart(px.line(monthly, x="txn_date", y="volume", markers=True,
                        title="Monthly Successful Volume (INR)"))

# ---------------- Gateway and method
left, right = st.columns(2)
left.plotly_chart(px.bar(gateway.sort_values("success_rate"), x="success_rate", y="gateway",
                         orientation="h", range_x=[80, 100], text="success_rate",
                         title="Success Rate by Gateway (%)"))
right.plotly_chart(px.pie(method, names="payment_method", values="txns", title="Payment Method Share"))

left, right = st.columns(2)
left.plotly_chart(px.bar(gateway, x="gateway", y="avg_ms", title="Average Processing Time (ms)"))
right.plotly_chart(px.density_heatmap(gw_method, x="payment_method", y="gateway", z="success_rate",
                                      histfunc="avg", color_continuous_scale="RdYlGn",
                                      title="Success Rate: Gateway x Payment Method"))

# ---------------- Failures and time
left, right = st.columns(2)
left.plotly_chart(px.bar(failures, x="count", y="failure_reason", orientation="h",
                         title="Top Failure Reasons"))
right.plotly_chart(px.line(hourly, x="hour", y="txns", markers=True, title="Transactions by Hour"))

# ---------------- Bank, city, category
a, b, c = st.columns(3)
a.plotly_chart(px.bar(bank.sort_values("success_rate"), x="success_rate", y="bank", orientation="h",
                      range_x=[80, 100], title="Success Rate by Bank"))
b.plotly_chart(px.bar(city.sort_values("volume", ascending=False), x="city", y="volume",
                      title="Volume by City"))
c.plotly_chart(px.bar(category.sort_values("volume", ascending=False), x="merchant_category", y="volume",
                      title="Volume by Category"))

# ---------------- Data quality
st.subheader("🧹 Data Quality (what Spark cleaned)")
left, right = st.columns(2)
left.dataframe(dq)
right.plotly_chart(px.bar(status, x="status", y="count", title="Transaction Status"))
st.caption(f"Rows read: {int(summary.raw_rows):,} | Duplicates removed: {int(summary.duplicates_removed):,} | "
           f"Valid: {int(summary.valid_rows):,} | Rejected: {int(summary.rejected_rows):,}")
