# Stock Screener Data Pipeline

An automated data engineering pipeline that scrapes financial data from [stockanalysis.com](https://stockanalysis.com) on a scheduled basis, processes it through a Medallion Architecture (Bronze → Silver → Gold), and stores it locally as partitioned Parquet files.

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
| Containerization | Docker & Docker Compose |

---

## 🏗 Architecture

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
| `PREFECT_HOME` | `/data/prefect` | Prefect metadata storage (persists in data volume) |
| `ALERT_WEBHOOK_URL` | _(empty)_ | Discord/Slack webhook URL for failure alerts (optional) |

### 3. Start the container
```bash
docker compose up -d
```

The pipeline runs both DAILY and INTRADAY immediately on startup, then follows the cron schedule.

### 4. View logs
```bash
docker logs -f stock-scraper
```

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
└── gold/
    ├── dividend/
    │   ├── date=2026-03-31/
    │   │   └── all.parquet          # US+TH merged + computed columns
    │   └── ...
    ├── financials/
    │   └── ...                      # + profit_margin, operating_margin, fcf_margin
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