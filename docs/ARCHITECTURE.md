# Architecture

Detailed architecture documentation for the Stock Screener Data Pipeline.

---

## System Overview

```mermaid
graph TB
    subgraph External
        SA["stockanalysis.com"]
    end

    subgraph Docker["Docker Container: stock-scraper"]
        PW["Playwright<br/>(Chromium)"]
        REQ["Requests<br/>(HTTP Client)"]
        PREFECT["Prefect<br/>(Orchestration)"]
        MAIN["main.py<br/>(Pipeline Logic)"]
    end

    subgraph Storage["Docker Volume: ./data"]
        SESSION["session/<br/>session_cookies.json"]
        BRONZE["bronze/<br/>Raw JSON"]
        SILVER["silver/<br/>Cleaned Parquet"]
        GOLD["gold/<br/>Merged Parquet"]
        METRICS["metrics/runs/<br/>Run Results JSON"]
    end

    subgraph Monitoring["Docker Container: stock-dashboard"]
        DASH["dashboard.py<br/>(Streamlit)"]
    end

    SA -->|"Intercept headers"| PW
    PW -->|"Save session"| SESSION
    SESSION -->|"Load session"| REQ
    REQ -->|"Fetch API data"| SA
    PREFECT -->|"Schedule & orchestrate"| MAIN
    MAIN -->|"Save raw JSON"| BRONZE
    MAIN -->|"Transform & save"| SILVER
    MAIN -->|"Merge & compute"| GOLD
    MAIN -->|"Save run results"| METRICS
    SILVER -->|"Read Parquet"| DASH
    GOLD -->|"Read Parquet"| DASH
    METRICS -->|"Read JSON"| DASH
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Fetch
        A1["Init Session<br/>(Playwright)"] --> A2["Fetch APIs<br/>(Concurrent)"]
        A2 -->|"403?"| A3["Refresh Session"]
        A3 --> A2
    end

    subgraph Bronze
        A2 -->|"Raw JSON"| B1["save_bronze_layer<br/>data/bronze/{category}/date={date}/{market}_{ts}.json"]
    end

    subgraph Silver
        A2 -->|"extract_items()"| C1["save_silver_layer"]
        C1 --> C2["Schema Enforcement"]
        C2 --> C3["DQ Checks<br/>(null ratio, row drop)"]
        C3 --> C4["Deduplication"]
        C4 --> C5["data/silver/{category}/date={date}/{market}.parquet"]
    end

    subgraph Gold
        C5 --> D1["save_gold_layer"]
        D1 --> D2["Merge US + TH"]
        D2 --> D3["Computed Columns"]
        D3 --> D4["Gold DQ Checks"]
        D4 --> D5["data/gold/{category}/date={date}/all.parquet"]
    end
```

---

## Session Management Flow

```mermaid
flowchart TD
    START["Pipeline Start"] --> CHECK{"session_cookies.json<br/>exists?"}
    CHECK -->|No| PLAYWRIGHT["Launch Playwright<br/>(headless Chromium)"]
    CHECK -->|Yes| AGE{"Age > 23 hours?"}
    AGE -->|Yes| PLAYWRIGHT
    AGE -->|No| LOAD["Load from file"]
    LOAD --> VALID{"Has headers<br/>+ cookies?"}
    VALID -->|No| PLAYWRIGHT
    VALID -->|Yes| USE["Use cached session"]

    PLAYWRIGHT --> INTERCEPT["Navigate to stockanalysis.com<br/>Intercept screener API request"]
    INTERCEPT --> CAPTURE["Capture headers + cookies"]
    CAPTURE --> SAVE["Save to session_cookies.json"]
    SAVE --> USE

    USE --> FETCH["Fetch APIs with requests"]
    FETCH -->|"403 Forbidden"| REFRESH["Refresh via Playwright"]
    REFRESH --> RETRY["Re-fetch failed APIs"]
```

---

## Concurrent Fetch Strategy

```mermaid
sequenceDiagram
    participant Flow as main_flow
    participant W1 as Wave 1 (0-4)
    participant W2 as Wave 2 (5-9)
    participant W3 as Wave 3 (10-14)
    participant API as stockanalysis.com

    Flow->>W1: Submit tasks (delay=0s)
    Flow->>W2: Submit tasks (delay=3s)
    Flow->>W3: Submit tasks (delay=6s)

    W1->>API: us_dividend, th_dividend, us_general, th_general, us_financials
    Note over W1,API: Random 1.5-3.5s between each

    W2->>API: th_financials, us_analysis, th_analysis, us_valuation, th_valuation
    W3->>API: us_etf, us_ipo_recent, us_trending

    API-->>W1: 200 OK (or 429 → retry with backoff)
    API-->>W2: 200 OK
    API-->>W3: 200 OK

    Note over Flow: All waves run concurrently<br/>429 in one wave doesn't block others
```

---

## Docker Architecture

```mermaid
graph LR
    subgraph docker-compose
        SC["stock-scraper<br/>(Pipeline)"]
        SD["stock-dashboard<br/>(Streamlit)"]
    end

    subgraph volumes
        DATA["./data<br/>(shared volume)"]
    end

    subgraph network
        NET["stock-pipeline-network<br/>(bridge)"]
    end

    SC -->|"read/write"| DATA
    SD -->|"read only"| DATA
    SC --- NET
    SD --- NET
    SD -->|"port 8501"| USER["User Browser"]
```

| Container | Purpose | Resources | Port |
|---|---|---|---|
| `stock-scraper` | Pipeline + Prefect scheduler | 2GB RAM, 1.5 CPU | — |
| `stock-dashboard` | Streamlit monitoring UI | 512MB RAM, 0.5 CPU | 8501 |

---

## Module Structure

```
main.py (1,200 lines)
├── Configuration           (L1-57)      Constants, env vars, timeouts
├── API Definitions         (L59-110)    DAILY_APIS + INTRADAY_APIS
├── DQ Configuration        (L112-132)   Thresholds, key columns
├── Gold Computed Columns   (L135-161)   Lambda formulas for Gold layer
├── Schema Enforcement      (L163-218)   Expected columns per category
├── Data Quality Checks     (L220-278)   Null ratio + row count drop
├── Session Management      (L280-404)   Playwright + requests session
├── Data Extraction         (L406-444)   extract_items, _dict_to_records
├── Prefect Tasks           (L446-678)   init_session, fetch_url, save_bronze/silver, cleanup
├── Gold Layer              (L680-819)   save_gold_layer + gold DQ checks
├── Alerting                (L821-893)   Webhook notifications
├── Trading Day Check       (L895-913)   Weekend detection
├── Run Metrics             (L916-957)   Persist run results for dashboard
├── Main Flow               (L960-1081)  Pipeline orchestration
└── Scheduler               (L1083-end)  Prefect serve() with cron schedules

dashboard.py (575 lines)
├── Config + Styling        (L1-167)     Data paths, CSS, theme
├── Data Loading            (L170-289)   Scan partitions, load metrics, compute DQ
├── UI Sections             (L294-540)   Header, KPIs, charts, tables
└── Main                    (L546-575)   Tab layout + assembly
```

---

## Error Handling Strategy

| Error | Behavior | Retry? |
|---|---|---|
| **403 Forbidden** | Signal flow to refresh session → re-fetch | Yes (1x with new session) |
| **429 Too Many Requests** | Exponential backoff (5s, 10s, 20s) | Yes (3x) |
| **Timeout / ConnectionError** | Exponential backoff | Yes (3x) |
| **500+ Server Error** | Log and skip | No |
| **Invalid JSON** | Log and skip | No |
| **Playwright timeout** | Fall back to default headers | No |
| **Session file corrupted** | Refresh via Playwright | Automatic |
