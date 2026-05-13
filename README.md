# Stock Screener Data Pipeline

An automated data engineering pipeline that scrapes financial data from [stockanalysis.com](https://stockanalysis.com) on a scheduled basis, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), and stores it locally as partitioned Parquet files.

Built as part of a Data Engineering internship.

---

## 🛠 Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Orchestration | Prefect |
| Browser Automation | Playwright (Chromium) |
| HTTP Client | Requests |
| Data Processing | Pandas, PyArrow |
| Monitoring | Streamlit, Plotly |
| Containerization | Docker & Docker Compose |
| Testing | pytest |

---

## 🏗 Architecture

> For detailed architecture diagrams and data flow, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

```
stockanalysis.com API
        │
        ▼
┌───────────────┐
│  Bronze Layer │  Raw JSON — partitioned by date
│  data/bronze/ │  e.g. bronze/dividend/date=2026-03-31/us.json
└───────┬───────┘
        │  extract + transform
        ▼
┌───────────────┐
│  Silver Layer │  Cleaned Parquet — split by market (US/TH)
│  data/silver/ │  e.g. silver/dividend/date=2026-03-31/us.parquet
└───────┬───────┘
        │  merge US+TH + computed columns
        ▼
┌───────────────┐
│   Gold Layer  │  Merged Parquet — all markets + derived metrics
│  data/gold/   │  e.g. gold/dividend/date=2026-03-31/all.parquet
└───────────────┘
```

### Gold Layer — Computed Columns

| Category | Column | Formula |
|---|---|---|
| `financials` | `profit_margin` | `netIncome / revenue` |
| `financials` | `operating_margin` | `operatingIncome / revenue` |
| `financials` | `fcf_margin` | `fcf / revenue` |
| `dividend` | `high_yield` | `dividendYield >= 4%` |
| `valuation` | `value_score` | average percentile rank of PE + PS + PB (lower = cheaper) |

### Session Management

Playwright intercepts a real browser request to `stockanalysis.com/stocks/screener/` and captures the actual HTTP headers and cookies. These are cached to disk (`data/session/session_cookies.json`) and reused for subsequent `requests` calls. Sessions older than 23 hours are automatically refreshed.

### Data Quality Checks

The pipeline runs automated DQ checks at every layer:

| Check | Layer | Description |
|---|---|---|
| Schema Enforcement | Silver | Missing columns → added as NaN + warning; extra columns → kept + logged |
| Row Count Drop | Silver | If row count drops >10% vs previous partition → warning |
| Null Ratio | Silver | If key columns have >50% null → warning |
| Row Count Validation | Gold | Merged rows must ≥ sum of silver inputs |
| Market Completeness | Gold | `market` column must have expected values |
| Computed Column Check | Gold | Computed columns must not be 100% NaN |

---

## 📦 Data Collected

### DAILY (08:00 BKK) — Fundamental data, updated once per day

| Dataset | Description | Markets |
|---|---|---|
| `dividend` | Dividend yield, DPS, payout ratio, growth | US, TH |
| `general` | Sector, exchange, country, fiscal year | US, TH |
| `financials` | Revenue, operating income, net income, FCF, EPS | US, TH |
| `analysis` | Analyst ratings, price targets, upside % | US, TH |
| `valuation` | PE, PS, PB, P/FCF, enterprise value | US, TH |
| `etf` | ETF list with asset class (top 500) | US |
| `ipo_recent` | Recent IPOs — last 200 listings | US |
| `trending` | Top 20 most-viewed stocks today | US |

### INTRADAY (04:00 and 22:00 BKK) — Real-time market data

| Dataset | Description |
|---|---|
| `top_gainers` | Top 20 stocks by % gain today |
| `top_losers` | Top 20 stocks by % loss today |
| `most_active` | Top 20 stocks by volume today |
| `premarket_gainers` | Top 20 premarket gainers |
| `premarket_losers` | Top 20 premarket losers |
| `afterhours_gainers` | Top 20 after-hours gainers |
| `afterhours_losers` | Top 20 after-hours losers |

---

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose installed

### 1. Clone the repo
```bash
git clone <repo-url>
cd stock-scraper
```

### 2. Configure environment variables
Copy the included example env file and adjust values as needed:
```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `TZ` | `Asia/Bangkok` | Container timezone |
| `RUN_HOUR` | `8` | Hour for DAILY run (local time) |
| `RUN_MINUTE` | `0` | Minute for DAILY run |
| `INTRADAY_HOUR_1` | `4` | First INTRADAY run hour |
| `INTRADAY_HOUR_2` | `22` | Second INTRADAY run hour |
| `RETENTION_DAYS` | `90` | Days of data to keep before auto-cleanup |
| `MAX_CONCURRENT` | `5` | Max concurrent API fetches per wave |
| `PREFECT_HOME` | `/data/prefect` | Prefect metadata storage (persists in data volume) |
| `ALERT_WEBHOOK_URL` | _(empty)_ | Discord/Slack webhook URL for failure alerts (optional) |
| `ALERT_ON_SUCCESS` | `false` | Send heartbeat notification on successful runs |
| `SKIP_WEEKEND_CHECK` | `false` | Override weekend skip for INTRADAY mode |

### 3. Start the containers
```bash
# Start pipeline + dashboard
docker compose up -d

# Start pipeline only
docker compose up -d stock-scraper

# Start dashboard only
docker compose up -d dashboard
```

The pipeline runs both DAILY and INTRADAY immediately on startup, then follows the cron schedule.

### 4. View logs
```bash
docker logs -f stock-scraper
```

### 5. Open Monitoring Dashboard
```
http://localhost:8501
```

---

## 📊 Monitoring Dashboard

A Streamlit-based monitoring dashboard is included for real-time pipeline health monitoring.

| Section | Description |
|---|---|
| **Health Overview** | Status badge (Healthy/Stale/Down), data freshness, KPI cards |
| **Row Count Trends** | Interactive time-series charts per category, US vs TH breakdown |
| **Data Quality** | Null ratio table for key columns, category coverage heatmap |
| **Run History** | Recent pipeline runs with duration, success/fail counts, DQ warnings |

```bash
# Run locally
streamlit run dashboard.py

# Run via Docker
docker compose up -d dashboard
# Open http://localhost:8501
```

---

## 🧪 Testing

Unit tests cover core pipeline logic:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest test_extract.py
pytest test_tasks.py
```

| Test File | Covers |
|---|---|
| `test_extract.py` | `extract_items`, `_dict_to_records`, `_enforce_schema`, `_run_quality_checks`, `_safe_divide`, `GOLD_COMPUTED_COLUMNS` lambdas |
| `test_tasks.py` | `fetch_url` (retry/403/429/timeout), `save_bronze_layer`, `save_silver_layer` (dedup), `save_gold_layer` (merge + computed), `cleanup_old_partitions`, `_run_gold_quality_checks` |

---

## 📁 Data Directory Structure

```
data/
├── session/
│   └── session_cookies.json       # Cached browser session
├── bronze/
│   ├── dividend/
│   │   ├── date=2026-03-31/
│   │   │   ├── us.json
│   │   │   └── th.json
│   │   └── date=2026-04-01/
│   │       └── ...
│   ├── financials/
│   ├── valuation/
│   └── ...
├── silver/
│   ├── dividend/
│   │   ├── date=2026-03-31/
│   │   │   ├── us.parquet
│   │   │   └── th.parquet
│   │   └── ...
│   └── ...
├── gold/
│   ├── dividend/
│   │   ├── date=2026-03-31/
│   │   │   └── all.parquet        # US+TH merged + computed columns
│   │   └── ...
│   ├── financials/
│   │   └── ...                    # + profit_margin, operating_margin, fcf_margin
│   └── ...
└── metrics/
    └── runs/                      # Pipeline run metrics (JSON)
        ├── 2026-03-31_DAILY_20260331T010000Z.json
        └── ...
```

Partitions older than `RETENTION_DAYS` are deleted automatically after each run.

---

## ⚙️ Schedule

| Flow | Cron (BKK) | `RUN_MODE` | Data |
|---|---|---|---|
| Daily Flow | `0 8 * * *` | `DAILY` | Fundamental |
| Intraday Flow (morning) | `0 4 * * *` | `INTRADAY` | Market movers |
| Intraday Flow (evening) | `0 22 * * *` | `INTRADAY` | Market movers |

Scheduled via `prefect.serve()` running 3 deployments in a single process.

INTRADAY mode automatically skips weekends (US market closed) unless `SKIP_WEEKEND_CHECK=true`.

---

## 🔔 Alerting

The pipeline supports webhook notifications via Discord or Slack:

- **Failure alerts**: Sent when APIs fail, get skipped (403), or DQ warnings are detected
- **Heartbeat**: Optional success notification after every successful run (`ALERT_ON_SUCCESS=true`)

Set `ALERT_WEBHOOK_URL` in `.env` to enable.

---

## 📚 Additional Documentation

| Document | Description |
|---|---|
| [Architecture](docs/ARCHITECTURE.md) | Detailed architecture diagrams, data flow, component descriptions |
| [Runbook](docs/RUNBOOK.md) | Troubleshooting guide, common issues, operational procedures |