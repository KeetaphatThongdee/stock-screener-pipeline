"""Stock Pipeline Monitoring Dashboard

Streamlit app สำหรับ monitor pipeline health:
- Pipeline Health Overview (KPI cards + data freshness)
- Row Count Trends (time series chart)
- Data Quality Summary (null ratios, schema status)
- Run History (from metrics JSON files)

Usage: streamlit run dashboard.py
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import streamlit as st

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

_DOCKER_MOUNT = "/data"
DATA_DIR = _DOCKER_MOUNT if os.path.isdir(_DOCKER_MOUNT) else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data"
)

DAILY_CATEGORIES = [
    "dividend", "general", "financials", "analysis", "valuation",
    "etf", "ipo_recent", "trending",
]
INTRADAY_CATEGORIES = [
    "top_gainers", "top_losers", "most_active",
    "premarket_gainers", "premarket_losers",
    "afterhours_gainers", "afterhours_losers",
]

# Key columns for DQ null-ratio display (subset of DQ_KEY_COLUMNS from main.py)
DQ_MONITOR_COLUMNS: dict[str, list[str]] = {
    "dividend":   ["dividendYield", "dps"],
    "general":    ["sector", "exchange"],
    "financials": ["revenue", "eps"],
    "analysis":   ["analystRatings", "priceTarget"],
    "valuation":  ["peForward", "psRatio"],
}

# ---------------------------------------------------------
# Page Config & Theme
# ---------------------------------------------------------

st.set_page_config(
    page_title="Stock Pipeline Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom CSS for dark premium look
st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    /* Global */
    .stApp {
        font-family: 'Inter', sans-serif;
    }

    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        border: 1px solid rgba(255,255,255,0.05);
    }
    .dashboard-header h1 {
        color: #e8e8e8;
        font-size: 1.8rem;
        font-weight: 700;
        margin: 0 0 0.3rem 0;
    }
    .dashboard-header p {
        color: #8892b0;
        font-size: 0.95rem;
        margin: 0;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #1e293b, #0f172a);
        border: 1px solid rgba(99, 102, 241, 0.15);
        border-radius: 14px;
        padding: 1.4rem 1.6rem;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(99, 102, 241, 0.15);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #818cf8, #6366f1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0.3rem 0;
    }
    .kpi-label {
        color: #94a3b8;
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .kpi-sub {
        color: #64748b;
        font-size: 0.78rem;
        margin-top: 0.3rem;
    }

    /* Status badge */
    .status-healthy {
        display: inline-block;
        background: rgba(34, 197, 94, 0.15);
        color: #4ade80;
        padding: 0.3rem 0.9rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        border: 1px solid rgba(34, 197, 94, 0.25);
    }
    .status-stale {
        display: inline-block;
        background: rgba(251, 191, 36, 0.15);
        color: #fbbf24;
        padding: 0.3rem 0.9rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        border: 1px solid rgba(251, 191, 36, 0.25);
    }
    .status-down {
        display: inline-block;
        background: rgba(239, 68, 68, 0.15);
        color: #f87171;
        padding: 0.3rem 0.9rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        border: 1px solid rgba(239, 68, 68, 0.25);
    }

    /* Section headers */
    .section-header {
        color: #e2e8f0;
        font-size: 1.25rem;
        font-weight: 600;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(99, 102, 241, 0.3);
    }

    /* DQ table */
    .dq-pass { color: #4ade80; font-weight: 600; }
    .dq-warn { color: #fbbf24; font-weight: 600; }
    .dq-fail { color: #f87171; font-weight: 600; }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------
# Data Loading Functions
# ---------------------------------------------------------

@st.cache_data(ttl=300)
def scan_all_partitions() -> pd.DataFrame:
    """Scan silver layer เพื่อเก็บ row count per category per date per market.
    ใช้ pyarrow metadata เพื่อไม่ต้องโหลดทั้งไฟล์ (เร็วมาก)
    """
    records = []
    silver_dir = os.path.join(DATA_DIR, "silver")
    if not os.path.isdir(silver_dir):
        return pd.DataFrame()

    for category in os.listdir(silver_dir):
        cat_dir = os.path.join(silver_dir, category)
        if not os.path.isdir(cat_dir):
            continue
        for partition in os.listdir(cat_dir):
            if not partition.startswith("date="):
                continue
            date_str = partition.replace("date=", "")
            part_dir = os.path.join(cat_dir, partition)
            if not os.path.isdir(part_dir):
                continue
            for f in os.listdir(part_dir):
                if not f.endswith(".parquet"):
                    continue
                fpath = os.path.join(part_dir, f)
                market = f.replace(".parquet", "").upper()
                try:
                    meta = pq.read_metadata(fpath)
                    records.append({
                        "category": category,
                        "date": date_str,
                        "market": market,
                        "rows": meta.num_rows,
                        "columns": meta.num_columns,
                        "file_size_kb": round(os.path.getsize(fpath) / 1024, 1),
                        "file_path": fpath,
                    })
                except Exception:
                    pass

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["date", "category", "market"]).reset_index(drop=True)


@st.cache_data(ttl=300)
def load_run_metrics() -> list[dict]:
    """Load run metrics JSON files from data/metrics/runs/"""
    metrics_dir = os.path.join(DATA_DIR, "metrics", "runs")
    if not os.path.isdir(metrics_dir):
        return []

    runs = []
    for f in sorted(os.listdir(metrics_dir), reverse=True):
        if not f.endswith(".json"):
            continue
        try:
            with open(os.path.join(metrics_dir, f), "r", encoding="utf-8") as fh:
                runs.append(json.load(fh))
        except Exception:
            pass
    return runs


@st.cache_data(ttl=300)
def load_change_data() -> list[dict]:
    """Load change detection results from data/metrics/changes/"""
    changes_dir = os.path.join(DATA_DIR, "metrics", "changes")
    if not os.path.isdir(changes_dir):
        return []

    results = []
    for f in sorted(os.listdir(changes_dir), reverse=True):
        if not f.endswith(".json"):
            continue
        try:
            with open(os.path.join(changes_dir, f), "r", encoding="utf-8") as fh:
                results.append(json.load(fh))
        except Exception:
            pass
    return results


@st.cache_data(ttl=600)
def compute_dq_null_ratios(partition_df: pd.DataFrame) -> pd.DataFrame:
    """คำนวณ null ratio ของ key columns จาก silver parquet ล่าสุด"""
    if partition_df.empty:
        return pd.DataFrame()

    latest_date = partition_df["date"].max()
    latest = partition_df[partition_df["date"] == latest_date]

    records = []
    for _, row in latest.iterrows():
        cat = row["category"]
        monitor_cols = DQ_MONITOR_COLUMNS.get(cat)
        if not monitor_cols:
            continue
        try:
            df = pd.read_parquet(row["file_path"])
            for col in monitor_cols:
                if col in df.columns:
                    null_pct = round(df[col].isna().mean() * 100, 1)
                    records.append({
                        "category": cat,
                        "market": row["market"],
                        "column": col,
                        "null_pct": null_pct,
                        "total_rows": len(df),
                    })
        except Exception:
            pass

    return pd.DataFrame(records) if records else pd.DataFrame()


def get_data_freshness() -> tuple[float, str]:
    """Return (age_hours, formatted_string) ของ silver data ล่าสุด"""
    silver_dir = os.path.join(DATA_DIR, "silver")
    if not os.path.isdir(silver_dir):
        return 999, "No data"

    freshest = 0
    for root, _, files in os.walk(silver_dir):
        for f in files:
            if f.endswith(".parquet"):
                mtime = os.path.getmtime(os.path.join(root, f))
                freshest = max(freshest, mtime)

    if freshest == 0:
        return 999, "No data"

    age_hours = (time.time() - freshest) / 3600
    if age_hours < 1:
        return age_hours, f"{age_hours * 60:.0f} minutes ago"
    elif age_hours < 24:
        return age_hours, f"{age_hours:.1f} hours ago"
    else:
        return age_hours, f"{age_hours / 24:.1f} days ago"


# ---------------------------------------------------------
# Dashboard Sections
# ---------------------------------------------------------

def render_header(age_hours: float, freshness_str: str):
    """Dashboard header with status badge"""
    if age_hours < 25:
        badge = '<span class="status-healthy">● Healthy</span>'
    elif age_hours < 48:
        badge = '<span class="status-stale">● Stale</span>'
    else:
        badge = '<span class="status-down">● Down</span>'

    st.markdown(f"""
    <div class="dashboard-header">
        <h1>📊 Stock Pipeline Monitor</h1>
        <p>Real-time monitoring for the Stock Screener Data Pipeline &nbsp;&nbsp; {badge}
        &nbsp;&nbsp; Last data: {freshness_str}</p>
    </div>
    """, unsafe_allow_html=True)


def render_kpi_cards(partition_df: pd.DataFrame, run_metrics: list[dict]):
    """KPI summary cards"""
    if partition_df.empty:
        total_rows = 0
        categories = 0
        partitions = 0
    else:
        total_rows = partition_df["rows"].sum()
        categories = partition_df["category"].nunique()
        partitions = partition_df["date"].nunique()

    dq_warnings = 0
    last_duration = "—"
    if run_metrics:
        latest = run_metrics[0]
        dq_warnings = len(latest.get("dq_warnings", []))
        last_duration = f'{latest.get("duration_seconds", 0):.0f}s'

    # Count changes from latest change detection
    change_data = load_change_data()
    changes_count = change_data[0].get("total_changes", 0) if change_data else 0

    cols = st.columns(6)
    kpis = [
        ("Total Rows", f"{total_rows:,}", "Across all categories"),
        ("Categories", str(categories), f"{len(DAILY_CATEGORIES)} daily + {len(INTRADAY_CATEGORIES)} intraday"),
        ("Partitions", str(partitions), "Unique date snapshots"),
        ("Changes", str(changes_count), "From latest detection"),
        ("DQ Warnings", str(dq_warnings), "From latest run"),
        ("Last Duration", last_duration, "Pipeline execution time"),
    ]
    for col, (label, value, sub) in zip(cols, kpis):
        col.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)


def render_row_count_trends(partition_df: pd.DataFrame):
    """Row count time series chart"""
    st.markdown('<div class="section-header">📈 Row Count Trends</div>', unsafe_allow_html=True)

    if partition_df.empty:
        st.info("No partition data available.")
        return

    # Aggregate rows per category per date
    agg = partition_df.groupby(["date", "category"])["rows"].sum().reset_index()

    # Category filter
    all_cats = sorted(agg["category"].unique())
    col1, col2 = st.columns([3, 1])
    with col2:
        selected_cats = st.multiselect(
            "Filter categories",
            all_cats,
            default=all_cats[:5] if len(all_cats) > 5 else all_cats,
            key="cat_filter",
        )

    if not selected_cats:
        selected_cats = all_cats

    filtered = agg[agg["category"].isin(selected_cats)]

    fig = px.line(
        filtered,
        x="date",
        y="rows",
        color="category",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", size=12),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="right", x=1,
            font=dict(size=11),
        ),
        xaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title="Row Count", gridcolor="rgba(255,255,255,0.05)"),
        margin=dict(l=20, r=20, t=40, b=20),
        height=400,
    )
    with col1:
        st.plotly_chart(fig, use_container_width=True)

    # Market breakdown bar chart
    market_agg = partition_df.groupby(["date", "market"])["rows"].sum().reset_index()
    fig2 = px.bar(
        market_agg,
        x="date",
        y="rows",
        color="market",
        barmode="group",
        color_discrete_map={"US": "#6366f1", "TH": "#22d3ee"},
    )
    fig2.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(title="", gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(title="Row Count", gridcolor="rgba(255,255,255,0.05)"),
        margin=dict(l=20, r=20, t=30, b=20),
        height=300,
        title=dict(text="Rows by Market (US vs TH)", font=dict(size=14, color="#94a3b8")),
    )
    st.plotly_chart(fig2, use_container_width=True)


def render_dq_summary(dq_df: pd.DataFrame, partition_df: pd.DataFrame):
    """Data Quality Summary section"""
    st.markdown('<div class="section-header">🔍 Data Quality Summary</div>', unsafe_allow_html=True)

    if partition_df.empty:
        st.info("No data available for DQ analysis.")
        return

    col1, col2 = st.columns(2)

    # --- Null ratio table ---
    with col1:
        st.markdown("**Null Ratio — Key Columns (Latest Partition)**")
        if dq_df.empty:
            st.info("No DQ data computed.")
        else:
            display = dq_df.copy()

            def _style_null(val):
                if val < 10:
                    return "🟢 Pass"
                elif val < 50:
                    return "🟡 Warning"
                else:
                    return "🔴 Fail"

            display["status"] = display["null_pct"].apply(_style_null)
            display["null_pct"] = display["null_pct"].apply(lambda x: f"{x:.1f}%")
            display = display.rename(columns={
                "category": "Category",
                "market": "Market",
                "column": "Column",
                "null_pct": "Null %",
                "total_rows": "Rows",
                "status": "Status",
            })
            st.dataframe(
                display[["Category", "Market", "Column", "Null %", "Rows", "Status"]],
                use_container_width=True,
                hide_index=True,
            )

    # --- Category coverage heatmap ---
    with col2:
        st.markdown("**Category Coverage — Latest Partition**")
        latest = partition_df[partition_df["date"] == partition_df["date"].max()]
        if latest.empty:
            st.info("No latest partition.")
        else:
            pivot = latest.pivot_table(
                index="category", columns="market",
                values="rows", fill_value=0, aggfunc="sum",
            )
            fig = px.imshow(
                pivot,
                text_auto=True,
                color_continuous_scale=["#1e1b4b", "#4338ca", "#818cf8"],
                aspect="auto",
            )
            fig.update_layout(
                template="plotly_dark",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", size=11),
                margin=dict(l=10, r=10, t=10, b=10),
                height=max(250, len(pivot) * 35),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)


def render_run_history(run_metrics: list[dict]):
    """Run History section from metrics JSON files"""
    st.markdown('<div class="section-header">🕐 Run History</div>', unsafe_allow_html=True)

    if not run_metrics:
        st.info(
            "No run metrics found. Metrics will appear after the pipeline runs "
            "with the updated code that saves `data/metrics/runs/*.json`."
        )
        return

    # Build table
    rows = []
    for m in run_metrics[:20]:  # แสดง 20 runs ล่าสุด
        rows.append({
            "Date": m.get("run_date", ""),
            "Mode": m.get("run_mode", ""),
            "Duration": f'{m.get("duration_seconds", 0):.0f}s',
            "APIs": f'{m.get("success_count", 0)}/{m.get("api_count", 0)}',
            "Rows": f'{m.get("total_rows", 0):,}',
            "DQ Warnings": len(m.get("dq_warnings", [])),
            "Session": m.get("session_type", ""),
            "Failed": ", ".join(m.get("failed", [])) or "—",
        })

    df = pd.DataFrame(rows)

    col1, col2 = st.columns([3, 2])

    with col1:
        st.dataframe(df, use_container_width=True, hide_index=True)

    # Duration trend
    with col2:
        if len(run_metrics) >= 2:
            dur_data = []
            for m in reversed(run_metrics[:20]):
                dur_data.append({
                    "run": f'{m.get("run_date", "")}\n{m.get("run_mode", "")}',
                    "seconds": m.get("duration_seconds", 0),
                    "mode": m.get("run_mode", ""),
                })
            dur_df = pd.DataFrame(dur_data)
            fig = px.bar(
                dur_df, x="run", y="seconds", color="mode",
                color_discrete_map={"DAILY": "#6366f1", "INTRADAY": "#22d3ee"},
            )
            fig.update_layout(
                template="plotly_dark",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter", size=11),
                xaxis=dict(title="", tickangle=-45, gridcolor="rgba(255,255,255,0.05)"),
                yaxis=dict(title="Duration (s)", gridcolor="rgba(255,255,255,0.05)"),
                margin=dict(l=10, r=10, t=30, b=10),
                height=350,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                title=dict(text="Run Duration", font=dict(size=14, color="#94a3b8")),
            )
            st.plotly_chart(fig, use_container_width=True)

    # DQ Warnings detail (expandable)
    if run_metrics and run_metrics[0].get("dq_warnings"):
        with st.expander(f"⚠️ DQ Warnings from latest run ({len(run_metrics[0]['dq_warnings'])} issues)"):
            for w in run_metrics[0]["dq_warnings"]:
                st.markdown(f"- {w}")


def render_changes():
    """Change Detection / Anomaly Detection tab"""
    st.markdown('<div class="section-header">🔔 Change Detection</div>', unsafe_allow_html=True)

    change_data = load_change_data()
    if not change_data:
        st.info(
            "No change detection data found. Changes will appear after the pipeline "
            "runs in DAILY mode with the updated code."
        )
        return

    # Date selector
    dates = [d.get("run_date", "") for d in change_data]
    selected_date = st.selectbox("Select date", dates, index=0, key="cd_date")

    # Find selected data
    selected = next((d for d in change_data if d.get("run_date") == selected_date), None)
    if not selected or not selected.get("changes"):
        st.info(f"No significant changes detected on {selected_date}.")
        return

    changes = selected["changes"]

    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    numeric_count = sum(1 for c in changes if c["type"] == "numeric_change")
    cat_count = sum(1 for c in changes if c["type"] == "categorical_change")
    new_count = sum(1 for c in changes if c["type"] == "new_ticker")
    c1.metric("Total Changes", len(changes))
    c2.metric("Numeric Changes", numeric_count)
    c3.metric("Rating Changes", cat_count)
    c4.metric("New Tickers", new_count)

    st.markdown("---")

    # Filter by type
    type_filter = st.multiselect(
        "Filter by type",
        ["numeric_change", "categorical_change", "new_ticker"],
        default=["numeric_change", "categorical_change", "new_ticker"],
        format_func=lambda x: {
            "numeric_change": "📊 Numeric Change",
            "categorical_change": "🏷️ Rating Change",
            "new_ticker": "🆕 New Ticker",
        }.get(x, x),
        key="cd_type_filter",
    )

    filtered = [c for c in changes if c["type"] in type_filter]

    if not filtered:
        st.info("No changes match the filter.")
        return

    # Build display table
    rows = []
    for c in filtered:
        type_icon = {
            "numeric_change": "📊",
            "categorical_change": "🏷️",
            "new_ticker": "🆕",
        }.get(c["type"], "")

        pct_str = f'{c["pct_change"]:+.1f}%' if c.get("pct_change") is not None else "—"
        old_str = str(c["old_value"]) if c["old_value"] is not None else "—"
        new_str = str(c["new_value"]) if c["new_value"] is not None else "—"

        rows.append({
            "Type": type_icon,
            "Ticker": c["ticker"],
            "Category": c["category"],
            "Market": c["market"].upper(),
            "Field": c["field"],
            "Old": old_str,
            "New": new_str,
            "Change %": pct_str,
        })

    df = pd.DataFrame(rows)

    # Category filter
    categories = sorted(df["Category"].unique())
    cat_filter = st.multiselect(
        "Filter by category", categories, default=categories, key="cd_cat_filter",
    )
    df = df[df["Category"].isin(cat_filter)]

    st.dataframe(df, use_container_width=True, hide_index=True, height=500)

    st.caption(f"Showing {len(df)} changes for {selected_date}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    # Load data
    partition_df = scan_all_partitions()
    run_metrics = load_run_metrics()
    age_hours, freshness_str = get_data_freshness()

    # Render sections
    render_header(age_hours, freshness_str)
    render_kpi_cards(partition_df, run_metrics)

    st.markdown("<br>", unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Row Trends", "🔍 Data Quality", "🔔 Changes", "🕐 Run History"])

    with tab1:
        render_row_count_trends(partition_df)

    with tab2:
        dq_df = compute_dq_null_ratios(partition_df)
        render_dq_summary(dq_df, partition_df)

    with tab3:
        render_changes()

    with tab4:
        render_run_history(run_metrics)

    # Footer
    st.markdown("---")
    st.markdown(
        '<p style="text-align:center; color:#475569; font-size:0.8rem;">'
        'Stock Pipeline Monitor • Auto-refresh every 5 minutes • '
        f'Last loaded: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}'
        '</p>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
