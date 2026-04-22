# Standard library
import json
import logging
import os
import random
import shutil
import time
from datetime import datetime, timedelta, timezone

# Third-party
import pandas as pd
import pyarrow.parquet as pq
import requests
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from prefect import task, flow, get_run_logger, serve
from prefect.runtime import flow_run

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# กำหนด DATA_DIR กลาง: ถ้าเจอ /data (Docker) ให้ใช้เลย ถ้าไม่เจอให้ใช้ BASE_DIR/data (Local)
_DOCKER_MOUNT = "/data"
DATA_DIR = _DOCKER_MOUNT if os.path.isdir(_DOCKER_MOUNT) else os.path.join(BASE_DIR, "data")

# ให้ Session และข้อมูลทั้งหมดอ้างอิงจาก DATA_DIR
SESSION_DIR = os.path.join(DATA_DIR, "session")
os.makedirs(SESSION_DIR, exist_ok=True)
COOKIE_FILE = os.path.join(SESSION_DIR, "session_cookies.json")

# Data retention: เก็บข้อมูลย้อนหลังกี่วัน ลบ partition เก่าอัตโนมัติ
# ปรับผ่าน env var RETENTION_DAYS ได้
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 90))

# Fetch settings
CONNECT_TIMEOUT = 10   # ถ้า server ไม่รับ connection = server down 
READ_TIMEOUT    = 30   # รอ response body นานกว่าได้ เพราะ API อาจช้าแต่ยังทำงานอยู่
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# Alert webhook: ส่ง notification เมื่อมี DQ warning หรือ API fail
# รองรับ Discord, Slack, หรือ webhook ทั่วไป — ถ้าไม่ตั้งค่าจะ skip
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")


# Logger สำหรับ helper functions (ทำงานนอก Prefect context)
# Prefect tasks/flow ใช้ get_run_logger() แทน เพื่อให้ log ขึ้น Prefect UI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAILY_APIS: fundamental data — stockanalysis.com อัพเดตวันละครั้ง
# RUN_MODE=DAILY → run ตอน 08:00 BKK
# ---------------------------------------------------------------------------
DAILY_APIS = [
    # --- Dividends ---
    {"name": "us_dividend",  "url": "https://stockanalysis.com/api/screener/s/bd/dps+dividendYield+payoutRatio+dividendGrowth+payoutFrequency.json",                              "description": "Dividend Data (US)"},
    {"name": "th_dividend",  "url": "https://stockanalysis.com/api/screener/s/bd/dps+dividendYield+payoutRatio+dividendGrowth.json?c=TH",                                         "description": "Dividend Data (TH)"},
    # --- General ---
    {"name": "us_general",   "url": "https://stockanalysis.com/api/screener/s/bd/isin+exchange+sector+country+founded+fiscalYearEnd+isPrimaryListing+payoutFrequency.json",        "description": "Company Profile (US)"},
    {"name": "th_general",   "url": "https://stockanalysis.com/api/screener/s/bd/isin+exchange+sector+country+founded+fiscalYearEnd+isPrimaryListing+payoutFrequency.json?c=TH",   "description": "Company Profile (TH)"},
    # --- Financials ---
    {"name": "us_financials","url": "https://stockanalysis.com/api/screener/s/bd/exchange+revenue+operatingIncome+netIncome+fcf+eps.json",                                        "description": "Financials (US)"},
    {"name": "th_financials","url": "https://stockanalysis.com/api/screener/s/bd/exchange+revenue+operatingIncome+netIncome+fcf+eps.json?c=TH",                                   "description": "Financials (TH)"},
    # --- Analysis ---
    {"name": "us_analysis",  "url": "https://stockanalysis.com/api/screener/s/bd/exchange+analystRatings+analystCount+priceTarget+priceTargetChange.json",                        "description": "Analyst Ratings (US)"},
    {"name": "th_analysis",  "url": "https://stockanalysis.com/api/screener/s/bd/exchange+analystRatings+analystCount+priceTarget+priceTargetChange.json?c=TH",                   "description": "Analyst Ratings (TH)"},
    # --- Valuation ---
    {"name": "us_valuation", "url": "https://stockanalysis.com/api/screener/s/bd/enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio.json",                                       "description": "Valuation Ratios (US)"},
    {"name": "th_valuation", "url": "https://stockanalysis.com/api/screener/s/bd/enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio.json?c=TH",                                  "description": "Valuation Ratios (TH)"},
    # --- ETF / IPO / Trending ---
    {"name": "us_etf",       "url": "https://stockanalysis.com/api/screener/e/f?m=s&s=asc&c=s,n,assetClass&cn=500&p=1&i=etf",                                                    "description": "ETF Overall (US)"},
    {"name": "us_ipo_recent","url": "https://stockanalysis.com/api/screener/s/f?m=ipoDate&s=desc&c=s,exchange,ipoPrice,sharesOffered,ds,ipoDate&cn=200&i=histip-recent",           "description": "Recent IPOs - Last 200 (US)"},
    {"name": "us_trending",  "url": "https://stockanalysis.com/api/screener/s/f?m=views&s=desc&c=no,s,views,tr1m,tr6m,trYTD,tr1y,tr5y,tr10y&cn=20&f=views-over-1&p=1&i=stocks",  "description": "Trending Today - Top 20 (US)"},
]

# ---------------------------------------------------------------------------
# INTRADAY_APIS: real-time market data — เปลี่ยนตามตลาด US
# ตลาด US: 09:30–16:00 ET = 21:30–04:00 BKK
# Premarket:   04:00–09:30 ET = 16:00–21:30 BKK
# After-hours: 16:00–20:00 ET = 04:00–08:00 BKK
# RUN_MODE=INTRADAY → run ตอน 04:00 BKK และ 22:00 BKK
# ---------------------------------------------------------------------------
INTRADAY_APIS = [
    # --- Market Movers ---
    {"name": "us_top_gainers",        "url": "https://stockanalysis.com/api/screener/s/f?m=change&s=desc&c=no,s,n,change,price,volume,marketCap&cn=20&f=change-over-0,priceDate-isLastTradingDay&p=1&i=stocks",                                                             "description": "Top Gainers Today (US)"},
    {"name": "us_top_losers",         "url": "https://stockanalysis.com/api/screener/s/f?m=change&s=asc&c=no,s,n,change,price,volume,marketCap&cn=20&f=change-under-0,priceDate-isLastTradingDay&p=1&i=stocks",                                                            "description": "Top Losers Today (US)"},
    {"name": "us_most_active",        "url": "https://stockanalysis.com/api/screener/s/f?m=volume&s=desc&c=no,s,volume,price,ma50,ma200,beta,rsi&cn=20&f=volume-over-0,priceDate-isLastTradingDay&p=1&i=stocks",                                                           "description": "Most Active Today (US)"},
    # --- Pre/After Market ---
    {"name": "us_premarket_gainers",  "url": "https://stockanalysis.com/api/screener/s/f?m=premarketChangePercent&s=desc&c=no,s,n,premarketChangePercent,premarketPrice,premarketVolume,marketCap&cn=20&f=premarketChangePercent-over-0,premarketDate-isLastTradingDay&p=1&i=stocks",   "description": "Premarket Gainers (US)"},
    {"name": "us_premarket_losers",   "url": "https://stockanalysis.com/api/screener/s/f?m=premarketChangePercent&s=asc&c=no,s,n,premarketChangePercent,premarketPrice,premarketVolume,marketCap&cn=20&f=premarketChangePercent-under-0,premarketDate-isLastTradingDay&p=1&i=stocks",    "description": "Premarket Losers (US)"},
    {"name": "us_afterhours_gainers", "url": "https://stockanalysis.com/api/screener/s/f?m=postmarketChangePercent&s=desc&c=no,s,n,postmarketChangePercent,postmarketPrice,postClose,marketCap&cn=20&f=postmarketChangePercent-over-0,postmarketDate-isLastTradingDay&p=1&i=stocks",    "description": "After Hours Gainers (US)"},
    {"name": "us_afterhours_losers",  "url": "https://stockanalysis.com/api/screener/s/f?m=postmarketChangePercent&s=asc&c=no,s,n,postmarketChangePercent,postmarketPrice,postClose,marketCap&cn=20&f=postmarketChangePercent-under-0,postmarketDate-isLastTradingDay&p=1&i=stocks",     "description": "After Hours Losers (US)"},
]

# RUN_MODE → เลือก API list ที่จะ fetch ใน run นี้
# DAILY    → fundamental data
# INTRADAY → market movers, premarket, after-hours
_MODE_MAP = {
    "DAILY":    DAILY_APIS,
    "INTRADAY": INTRADAY_APIS,
}

# ---------------------------------------------------------
# Data Quality Configuration
# ---------------------------------------------------------

# ถ้า row count วันนี้ลดลงจากวันก่อนเกิน ROW_DROP_THRESHOLD → warning
# ถ้า null ratio ของ key columns เกิน NULL_THRESHOLD → warning
ROW_DROP_THRESHOLD = 0.10   # 10%
NULL_THRESHOLD     = 0.50   # 50%

# key columns ที่ monitor null ratio แยกตาม category และ market
# structure: {category: {market: [columns]}}
# ใช้ None แทน list ว่าง เพื่อ skip check นั้นทั้งหมด (known data gap)
# - th_analysis: stockanalysis.com ไม่มี analyst coverage สำหรับหุ้นไทย
# - th_valuation: PE data ของหุ้นไทยส่วนใหญ่ไม่มีใน stockanalysis.com
DQ_KEY_COLUMNS: dict[str, dict[str, list[str] | None]] = {
    "dividend":   {"us": ["dividendYield", "dps"],         "th": ["dividendYield", "dps"]},
    "general":    {"us": ["sector", "exchange"],           "th": ["sector", "exchange"]},
    "financials": {"us": ["revenue", "eps"],               "th": ["revenue", "eps"]},
    "analysis":   {"us": ["analystRatings", "priceTarget"],"th": None},   # no TH analyst data
    "valuation":  {"us": ["peForward"],                    "th": None},   # no TH PE data
}


# ---------------------------------------------------------------------------
# Gold Layer: computed columns สำหรับแต่ละ category
# key = category name, value = list of (new_col, formula_description, lambda)
# lambda รับ df แล้ว return Series — ถ้า column ที่ต้องใช้ไม่มีจะ skip อัตโนมัติ
# ---------------------------------------------------------------------------
import numpy as np

def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Division ที่แปลง inf/-inf เป็น NaN — ป้องกัน division by zero ใน Pandas
    (Pandas ไม่ raise ZeroDivisionError แต่ return inf แทน)
    """
    return (numerator / denominator).replace([np.inf, -np.inf], pd.NA)

GOLD_COMPUTED_COLUMNS: dict[str, list[tuple[str, str, callable]]] = {
    "financials": [
        ("profit_margin",    "netIncome / revenue",        lambda df: _safe_divide(df["netIncome"], df["revenue"])),
        ("operating_margin", "operatingIncome / revenue",   lambda df: _safe_divide(df["operatingIncome"], df["revenue"])),
        ("fcf_margin",       "fcf / revenue",               lambda df: _safe_divide(df["fcf"], df["revenue"])),
    ],
    "dividend": [
        ("high_yield",       "dividendYield >= 4%",         lambda df: df["dividendYield"].ge(4)),
    ],
    "valuation": [
        ("value_score",      "average rank of PE+PS+PB (lower = cheaper)",
         lambda df: df[["peForward", "psRatio", "pbRatio"]].rank(pct=True).mean(axis=1)),
    ],
}


# ---------------------------------------------------------------------------
# Schema Enforcement: expected columns สำหรับแต่ละ category
# ถ้า API เปลี่ยน response format แล้ว column หายไป → เติมให้เป็น NaN + warning
# ถ้ามี column ใหม่ที่ไม่รู้จัก → เก็บไว้แต่ log ให้รู้
# "s" คือ ticker symbol — เป็น key column ที่ทุก category ต้องมี
# ---------------------------------------------------------------------------
EXPECTED_SCHEMA: dict[str, list[str]] = {
    "dividend":       ["s", "dps", "dividendYield", "payoutRatio", "dividendGrowth"],
    "general":        ["s", "isin", "exchange", "sector", "country", "founded", "fiscalYearEnd"],
    "financials":     ["s", "exchange", "revenue", "operatingIncome", "netIncome", "fcf", "eps"],
    "analysis":       ["s", "exchange", "analystRatings", "analystCount", "priceTarget", "priceTargetChange"],
    "valuation":      ["s", "enterpriseValue", "peForward", "psRatio", "pbRatio", "pFcfRatio"],
    "etf":            ["s", "n", "assetClass"],
    "ipo_recent":     ["s", "exchange", "ipoPrice", "sharesOffered", "ipoDate"],
    "trending":       ["s", "views"],
    "top_gainers":    ["s", "n", "change", "price", "volume", "marketCap"],
    "top_losers":     ["s", "n", "change", "price", "volume", "marketCap"],
    "most_active":    ["s", "volume", "price"],
    "premarket_gainers":  ["s", "n", "premarketChangePercent", "premarketPrice"],
    "premarket_losers":   ["s", "n", "premarketChangePercent", "premarketPrice"],
    "afterhours_gainers": ["s", "n", "postmarketChangePercent", "postmarketPrice"],
    "afterhours_losers":  ["s", "n", "postmarketChangePercent", "postmarketPrice"],
}


def _enforce_schema(df: pd.DataFrame, category: str, filename: str, log) -> list[str]:
    """ตรวจสอบ columns กับ EXPECTED_SCHEMA
    - column หายไป → เติมเป็น NaN + เตือน
    - column ใหม่ที่ไม่อยู่ใน schema → เก็บไว้ + log info
    คืน list ของ warning strings
    """
    warnings = []
    expected = EXPECTED_SCHEMA.get(category)
    if expected is None:
        return warnings

    actual = set(df.columns)
    expected_set = set(expected)

    # columns ที่ควรมีแต่หายไป
    missing = expected_set - actual
    if missing:
        msg = f"{filename}: missing columns {sorted(missing)} — added as NaN"
        log.warning("SCHEMA WARNING: %s", msg)
        warnings.append(msg)
        for col in missing:
            df[col] = pd.NA

    # columns ใหม่ที่ไม่อยู่ใน schema (ไม่นับ metadata columns ที่เราเติมเอง)
    pipeline_cols = {"fetched_at", "flow_run_id", "source_api"}
    extra = actual - expected_set - pipeline_cols
    if extra:
        log.info("SCHEMA INFO: %s has extra columns %s (kept)", filename, sorted(extra))

    return warnings


def _run_quality_checks(df: pd.DataFrame, filename: str, category: str, country: str, log) -> list[str]:
    """
    checks ที่ทำ:
    1. Row count drop — เทียบกับ partition วันก่อนหน้าล่าสุด
    2. Null ratio   — ของ key columns ที่กำหนดใน DQ_KEY_COLUMNS
    """
    warnings = []

    # --- Check 1: Row count drop ---
    silver_category_dir = os.path.join(DATA_DIR, "silver", category)
    if os.path.isdir(silver_category_dir):
        # หา partition วันก่อนหน้าล่าสุด (ไม่นับวันนี้)
        today_partition = f"date={df['fetched_at'].iloc[0].strftime('%Y-%m-%d')}"
        prev_partitions = sorted([
            d for d in os.listdir(silver_category_dir)
            if d.startswith("date=") and d != today_partition
        ], reverse=True)

        if prev_partitions:
            prev_path = os.path.join(silver_category_dir, prev_partitions[0], f"{country}.parquet")
            if os.path.exists(prev_path):
                try:
                    prev_rows = pq.read_metadata(prev_path).num_rows
                    curr_rows = len(df)
                    if prev_rows > 0:
                        drop_pct = (prev_rows - curr_rows) / prev_rows
                        if drop_pct > ROW_DROP_THRESHOLD:
                            msg = (
                                f"{filename}: row count dropped {drop_pct:.1%} "
                                f"({prev_rows} -> {curr_rows}) vs {prev_partitions[0]}"
                            )
                            log.warning("DQ WARNING: %s", msg)
                            warnings.append(msg)
                        else:
                            log.info("DQ OK -- %s row count: %d -> %d (%.1f%%)",
                                     filename, prev_rows, curr_rows,
                                     (curr_rows - prev_rows) / prev_rows * 100)
                except Exception as e:
                    log.warning("%s: could not read previous partition for DQ check: %s", filename, e)

    # --- Check 2: Null ratio ---
    # lookup per-market columns — None หมายความว่า skip check นี้ (known data gap)
    market_cols = DQ_KEY_COLUMNS.get(category, {}).get(country)
    if market_cols is None:
        log.info("DQ SKIP -- %s null check skipped (known data gap for %s)", filename, country.upper())
    else:
        for col in market_cols:
            if col not in df.columns:
                continue
            null_ratio = df[col].isna().mean()
            if null_ratio > NULL_THRESHOLD:
                msg = f"{filename}: high null ratio in '{col}' ({null_ratio:.1%} of {len(df)} rows)"
                log.warning("DQ WARNING: %s", msg)
                warnings.append(msg)
            else:
                log.info("DQ OK -- %s '%s' null: %.1f%%", filename, col, null_ratio * 100)

    return warnings



def refresh_session_with_playwright() -> dict:
    logger.info("Starting Playwright to capture real API headers...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        page = context.new_page()

        try:
            with page.expect_response(
                lambda r: "stockanalysis.com/api/screener" in r.url,
                timeout=30000,
            ) as response_info:
                page.goto(
                    "https://stockanalysis.com/stocks/screener/",
                    wait_until="domcontentloaded",
                    timeout=60000,
                )

            # ดึง headers จาก request ที่ trigger response นั้น
            intercepted_request = response_info.value.request
            logger.info("Captured API request headers from: %s", intercepted_request.url)

            cookies = context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}

            HEADERS_BLOCKLIST = {"host", "content-length", "cookie"}
            raw_headers = dict(intercepted_request.headers)
            api_headers = {
                k: v for k, v in raw_headers.items()
                if not k.startswith(":")
                and k.lower() not in HEADERS_BLOCKLIST
            }
            logger.info(
                "Using %d filtered headers (from %d intercepted)",
                len(api_headers), len(raw_headers)
            )

            session_data = {
                "headers": api_headers,
                "cookies": cookie_dict,
            }

            with open(COOKIE_FILE, "w") as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            logger.info("Session saved to %s", COOKIE_FILE)

            return session_data

        except PlaywrightTimeoutError:
            # expect_response หมดเวลา — page โหลดสำเร็จแต่ไม่มี screener API request
            # cookies น่าจะได้แล้ว ใช้ fallback headers แทน intercepted headers
            logger.warning(
                "Timed out waiting for screener API request. "
                "Proceeding with fallback headers."
            )
            cookies = context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}
            # ดึง UA จริงจาก browser แทน hardcode
            real_ua = page.evaluate("navigator.userAgent")
            api_headers = {
                "User-Agent": real_ua,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://stockanalysis.com/stocks/screener/",
                "Origin": "https://stockanalysis.com",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            session_data = {"headers": api_headers, "cookies": cookie_dict}
            with open(COOKIE_FILE, "w") as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            return session_data

        except Exception as e:
            logger.error("Playwright failed: %s", e)
            return {}
        finally:
            browser.close()


def build_requests_session(session_data: dict) -> requests.Session:
    """สร้าง requests.Session จาก session_data dict"""
    session = requests.Session()
    session.headers.update(session_data.get("headers", {}))
    session.cookies.update(session_data.get("cookies", {}))
    return session


SESSION_MAX_AGE_HOURS = 23  # session ที่เก่ากว่านี้ให้ refresh ทันที

def load_or_refresh_session() -> dict:
    if os.path.exists(COOKIE_FILE):
        # เช็คอายุไฟล์ก่อน — session เก่าเกินไปให้ refresh ทันทีโดยไม่ต้องโหลด
        age_hours = (time.time() - os.path.getmtime(COOKIE_FILE)) / 3600
        if age_hours > SESSION_MAX_AGE_HOURS:
            logger.info(
                "Session file is %.1f hours old (max %dh), refreshing...",
                age_hours, SESSION_MAX_AGE_HOURS,
            )
            return refresh_session_with_playwright()

        try:
            with open(COOKIE_FILE, "r") as f:
                data = json.load(f)
            if data.get("headers") and data.get("cookies"):
                logger.info("Loaded existing session from file (age: %.1fh).", age_hours)
                return data
        except Exception:
            logger.warning("Session file corrupted, will refresh.")

    return refresh_session_with_playwright()


def _dict_to_records(d: dict) -> list:
    return [
        {"s": ticker, **fields} if isinstance(fields, dict) else {"s": ticker, "value": fields}
        for ticker, fields in d.items()
    ]


def extract_items(json_data) -> list:
    if isinstance(json_data, list):
        return json_data

    if not isinstance(json_data, dict):
        return []

    NESTED_KEYS = ["data", "results", "items", "records", "stocks"]

    # เจาะเข้าไปใน nested dict (เช่น {"data": {"data": {...}}})
    candidate = json_data
    for key in NESTED_KEYS:
        if key in candidate and isinstance(candidate[key], dict):
            candidate = candidate[key]
            break

    # หา list หรือ dict-of-dicts ข้างใน
    for key in NESTED_KEYS:
        if key not in candidate:
            continue
        val = candidate[key]
        if isinstance(val, dict):
            return _dict_to_records(val)
        if isinstance(val, list):
            return val

    # กรณี candidate ตัวเองเป็น dict-of-dicts โดยตรง
    sample = list(candidate.values())[:5]
    if sample and all(isinstance(v, dict) for v in sample):
        return _dict_to_records(candidate)

    return []


@task(name="Init Session")
def init_session() -> dict:
    log = get_run_logger()
    log.info("Initializing session...")
    return load_or_refresh_session()


MAX_RETRIES = 3  # ใช้สำหรับทุก transient error (429, Timeout, ConnectionError)

@task(name="Fetch Raw Data")
def fetch_url(config_item: dict, session_data: dict) -> tuple[dict | None, bool]:
    log = get_run_logger()
    name = config_item["name"]
    url = config_item["url"]

    time.sleep(random.uniform(1.5, 3.5))
    log.info("Fetching: %s", name)

    session = build_requests_session(session_data)

    last_status = None  # track สาเหตุล่าสุดเพื่อ log ตอนหมด retry ได้ถูกต้อง

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            last_status = f"HTTP {response.status_code}"

            # 403 → session expired, ส่งสัญญาณให้ flow refresh ทันที (ไม่ retry)
            if response.status_code == 403:
                log.warning("%s: 403 session expired, signaling flow to refresh...", name)
                return None, True

            # 200 → success
            if response.status_code == 200:
                try:
                    json_data = response.json()
                except requests.exceptions.JSONDecodeError:
                    log.error("%s: Response is not valid JSON (got: %s)", name, response.text[:100])
                    return None, False
                items = extract_items(json_data)
                log.info("%s: SUCCESS - %d items", name, len(items))
                return json_data, False

            # 429 → rate limited, retry ด้วย backoff
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = min(5 * (2 ** attempt), 60)  # exponential backoff สูงสุด 60s
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        pass  # server ส่งมาเป็น date string ใช้ default แทน
                log.warning("%s: 429 Too Many Requests (attempt %d/%d), waiting %ds...",
                            name, attempt + 1, MAX_RETRIES, wait)
                time.sleep(wait)
                continue

            # อื่นๆ (4xx/5xx) → ไม่ retry เพราะไม่ใช่ transient error
            log.error("%s: HTTP %d | %s", name, response.status_code, response.text[:200])
            return None, False

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Transient network error → retry ด้วย backoff
            last_status = type(e).__name__
            wait = 5 * (2 ** attempt)  # exponential backoff: 5s, 10s, 20s
            log.warning("%s: %s (attempt %d/%d), waiting %ds...",
                        name, last_status, attempt + 1, MAX_RETRIES, wait)
            if attempt < MAX_RETRIES - 1:
                time.sleep(wait)
                continue
            # attempt สุดท้าย → fall through ไป log ข้างล่าง

        except Exception as e:
            # error ที่ไม่รู้จัก → log แล้ว skip ทันที ไม่ retry
            log.error("%s: Unexpected error - %s", name, e)
            return None, False

    # loop หมด — retry ครบแล้ว (429, Timeout, หรือ ConnectionError)
    log.error("%s: Giving up after %d retries (last: %s).", name, MAX_RETRIES, last_status)
    return None, False


@task(name="Save Bronze (JSON)")
def save_bronze_layer(json_data: dict, filename: str, run_date: str):
    """Save raw JSON to data/bronze/"""
    log = get_run_logger()
    if not json_data:
        log.warning("BRONZE skipped: %s (no data)", filename)
        return None

    try:
        country, category = filename.split("_", 1)
    except ValueError:
        country, category = "unknown", filename

    output_dir = os.path.join(DATA_DIR, "bronze", category, f"date={run_date}")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"{country}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False)
    log.info("BRONZE saved: %s", file_path)


@task(name="Save Silver (Parquet)")
def save_silver_layer(json_data: dict, filename: str, run_date: str) -> list[str]:
    """Extract table, convert to DataFrame, run DQ checks, save as Parquet in data/silver/
    คืนค่า list of DQ warning strings (เพื่อให้ main_flow รวม summary ท้าย run)
    """
    log = get_run_logger()
    if not json_data:
        return []

    items = extract_items(json_data)
    if not items:
        log.warning("No tabular data for %s. Skipping Silver.", filename)
        return []

    try:
        country, category = filename.split("_", 1)
    except ValueError:
        country, category = "unknown", filename

    output_dir = os.path.join(DATA_DIR, "silver", category, f"date={run_date}")
    os.makedirs(output_dir, exist_ok=True)

    df = pd.DataFrame(items)
    df["fetched_at"] = datetime.now(timezone.utc)
    try:
        df["flow_run_id"] = flow_run.id or "local_run"
    except Exception:
        df["flow_run_id"] = "local_run"
    df["source_api"] = filename

    # --- Schema enforcement ก่อน DQ checks ---
    schema_warnings = _enforce_schema(df, category, filename, log)
    dq_warnings = schema_warnings

    # --- Data quality checks ก่อน save ---
    dq_warnings += _run_quality_checks(df, filename, category, country, log)

    file_path = os.path.join(output_dir, f"{country}.parquet")
    df.to_parquet(file_path, index=False)
    log.info("SILVER saved: %s (%d rows)", file_path, len(df))

    return dq_warnings


@task(name="Cleanup Old Partitions")
def cleanup_old_partitions():
    log = get_run_logger()
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    deleted_count = 0

    for layer in ["bronze", "silver", "gold"]:
        layer_dir = os.path.join(DATA_DIR, layer)
        if not os.path.isdir(layer_dir):
            continue

        # loop ทุก category (dividend, general, financials, ...)
        for category in os.listdir(layer_dir):
            category_dir = os.path.join(layer_dir, category)
            if not os.path.isdir(category_dir):
                continue

            # loop ทุก partition (date=2026-01-01, date=2026-01-02, ...)
            for partition in os.listdir(category_dir):
                if not partition.startswith("date="):
                    continue

                # parse วันที่จาก folder name
                date_str = partition.replace("date=", "")
                try:
                    partition_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    log.warning("Skipping unrecognized partition: %s", partition)
                    continue

                if partition_date < cutoff:
                    partition_path = os.path.join(category_dir, partition)
                    try:
                        shutil.rmtree(partition_path)
                        log.info("Deleted old partition: %s", partition_path)
                        deleted_count += 1
                    except OSError as e:
                        log.warning("Could not delete %s: %s", partition_path, e)

    if deleted_count == 0:
        log.info("Cleanup: no partitions older than %d days found.", RETENTION_DAYS)
    else:
        log.info("Cleanup: deleted %d partition(s) older than %d days.",
                 deleted_count, RETENTION_DAYS)



@task(name="Save Gold (Merged Parquet)")
def save_gold_layer(run_date: str) -> int:
    """Merge US + TH silver parquet per category, add computed columns,
    save to data/gold/{category}/date={run_date}/all.parquet
    คืนค่าจำนวน categories ที่สร้าง Gold สำเร็จ
    """
    log = get_run_logger()
    silver_base = os.path.join(DATA_DIR, "silver")
    gold_count = 0

    if not os.path.isdir(silver_base):
        log.warning("GOLD skipped: silver directory not found.")
        return 0

    # วน loop ทุก category ใน silver (dividend, general, financials, ...)
    today_partition = f"date={run_date}"
    for category in os.listdir(silver_base):
        partition_dir = os.path.join(silver_base, category, today_partition)
        if not os.path.isdir(partition_dir):
            continue

        # อ่านทุก parquet ใน partition (us.parquet, th.parquet, ...)
        frames = []
        for parquet_file in os.listdir(partition_dir):
            if not parquet_file.endswith(".parquet"):
                continue
            market = parquet_file.replace(".parquet", "").upper()  # "us" -> "US"
            file_path = os.path.join(partition_dir, parquet_file)
            try:
                df = pd.read_parquet(file_path)
                df["market"] = market
                frames.append(df)
                log.info("GOLD read: %s/%s/%s (%d rows)",
                         category, today_partition, parquet_file, len(df))
            except Exception as e:
                log.warning("GOLD could not read %s: %s", file_path, e)

        if not frames:
            continue

        merged = pd.concat(frames, ignore_index=True)

        # --- Computed columns ---
        computed_specs = GOLD_COMPUTED_COLUMNS.get(category, [])
        for col_name, description, func in computed_specs:
            try:
                merged[col_name] = func(merged)
                # Safety net: แปลง inf ที่หลุดรอดมาเป็น NaN (กัน lambda ใหม่ที่ลืมใช้ _safe_divide)
                if merged[col_name].dtype.kind == "f":  # float columns only
                    inf_count = np.isinf(merged[col_name]).sum()
                    if inf_count > 0:
                        log.warning("GOLD computed '%s.%s': replaced %d inf values with NaN",
                                    category, col_name, inf_count)
                        merged[col_name] = merged[col_name].replace([np.inf, -np.inf], pd.NA)
                log.info("GOLD computed: %s.%s (%s)", category, col_name, description)
            except (KeyError, TypeError, ZeroDivisionError) as e:
                log.warning("GOLD skip computed '%s.%s': %s", category, col_name, e)

        # --- Save ---
        gold_dir = os.path.join(DATA_DIR, "gold", category, today_partition)
        os.makedirs(gold_dir, exist_ok=True)
        gold_path = os.path.join(gold_dir, "all.parquet")
        merged.to_parquet(gold_path, index=False)
        log.info("GOLD saved: %s (%d rows, %d columns)",
                 gold_path, len(merged), len(merged.columns))
        gold_count += 1

    if gold_count == 0:
        log.warning("GOLD: no categories processed for %s.", run_date)
    else:
        log.info("GOLD: %d category(ies) merged for %s.", gold_count, run_date)

    return gold_count



# ---------------------------------------------------------
# Alert Notification
# ---------------------------------------------------------


def _send_alert_if_needed(
    meta: dict, run_mode: str, duration: float,
    total_rows: int, total_apis: int, log,
) -> None:
    """ส่ง webhook alert ถ้ามี issue (failed, skipped, DQ warnings)
    ถ้า ALERT_WEBHOOK_URL ไม่ได้ตั้งค่า จะ skip ทั้งหมด
    รองรับ Discord webhook (content) และ Slack webhook (text)
    """
    if not ALERT_WEBHOOK_URL:
        return

    has_issues = meta["failed"] or meta["skipped"] or meta["dq_warnings"]
    if not has_issues:
        log.info("Alert: no issues detected, skipping notification.")
        return

    # --- สร้าง message ---
    lines = []
    status = "⚠️ WARNING" if not meta["failed"] else "🔴 ALERT"
    lines.append(f"**{status} — Stock Pipeline ({run_mode})**")
    lines.append(f"APIs: {len(meta['success'])}/{total_apis} success | Duration: {duration:.0f}s | Rows: {total_rows}")

    if meta["failed"]:
        lines.append(f"\n**❌ Failed ({len(meta['failed'])}):** {', '.join(meta['failed'])}")
    if meta["skipped"]:
        lines.append(f"**⏭️ Skipped ({len(meta['skipped'])}):** {', '.join(meta['skipped'])}")
    if meta["dq_warnings"]:
        lines.append(f"\n**📊 DQ Warnings ({len(meta['dq_warnings'])}):**")
        for w in meta["dq_warnings"][:5]:  # แสดงสูงสุด 5 รายการ กัน message ยาวเกิน
            lines.append(f"  • {w}")
        if len(meta["dq_warnings"]) > 5:
            lines.append(f"  ... and {len(meta['dq_warnings']) - 5} more")

    message = "\n".join(lines)

    # --- ส่ง webhook (รองรับทั้ง Discord และ Slack) ---
    try:
        payload = {"content": message, "text": message}
        resp = requests.post(
            ALERT_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )
        if resp.ok:
            log.info("Alert sent successfully (HTTP %d).", resp.status_code)
        else:
            log.warning("Alert webhook returned HTTP %d: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        log.warning("Alert webhook failed: %s", e)


# ---------------------------------------------------------
# Main Flow
# ---------------------------------------------------------


@flow(name="Stock Scraper (Medallion Architecture)")
def main_flow(run_mode: str = "DAILY") -> dict:
    log = get_run_logger()
    run_mode = run_mode.upper()
    api_list = _MODE_MAP.get(run_mode)
    if api_list is None:
        log.critical("Unknown run_mode='%s'. Valid values: DAILY, INTRADAY. Aborting.", run_mode)
        return {"error": f"Unknown run_mode='{run_mode}'"}

    log.info("Starting Stock Data Pipeline | Mode: %s | Run time: %s",
             run_mode, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

    session_data = init_session()

    if not session_data:
        log.critical("Could not initialize session. Aborting.")
        return {"error": "session_init_failed"}

    session_refreshed_this_run = False

    # run metadata สำหรับ summary ท้าย run
    meta = {
        "success":      [],
        "failed":       [],
        "skipped":      [],  # API ที่ถูก skip (403 ซ้ำ, refresh fail)
        "rows":         {},  # {api_name: row_count}
        "session_type": "file",  # file | intercepted | fallback | refreshed
        "dq_warnings":  [],  # data quality warnings จาก save_silver_layer
    }

    run_start = datetime.now(timezone.utc)
    run_date  = run_start.strftime("%Y-%m-%d")  # UTC date — ตรงกับ fetched_at ใน Silver layer

    # --- Cleanup ก่อน fetch: เคลียร์ partition เก่าเพื่อเปิดพื้นที่ disk ---
    cleanup_old_partitions()

    for item in api_list:
        json_data, need_refresh = fetch_url(item, session_data)

        if need_refresh:
            if session_refreshed_this_run:
                log.error("%s: Still 403 after refresh, skipping.", item["name"])
                meta["skipped"].append(item["name"])
                continue

            log.warning("Refreshing session at flow level (once per run)...")
            new_session_data = refresh_session_with_playwright()
            session_refreshed_this_run = True
            meta["session_type"] = "refreshed"

            if not new_session_data:
                log.error("%s: Refresh failed, skipping.", item["name"])
                meta["skipped"].append(item["name"])
                continue

            if new_session_data.get("headers") and new_session_data.get("cookies"):
                session_data = new_session_data
            else:
                log.warning("Incomplete session data, keeping old session.")
                meta["session_type"] = "fallback"

            json_data, need_refresh = fetch_url(item, session_data)
            if need_refresh or json_data is None:
                log.error("%s: Failed after refresh, skipping.", item["name"])
                meta["skipped"].append(item["name"])
                continue

        if json_data:
            items = extract_items(json_data)
            meta["success"].append(item["name"])
            meta["rows"][item["name"]] = len(items)
            save_bronze_layer(json_data, item["name"], run_date)
            warnings = save_silver_layer(json_data, item["name"], run_date)
            if warnings:
                meta["dq_warnings"].extend(warnings)
        else:
            log.warning("%s: No data fetched, skipping.", item["name"])
            meta["failed"].append(item["name"])

    # --- Run Summary ---
    duration  = (datetime.now(timezone.utc) - run_start).total_seconds()
    total_rows = sum(meta["rows"].values())
    us_rows   = sum(v for k, v in meta["rows"].items() if k.startswith("us_"))
    th_rows   = sum(v for k, v in meta["rows"].items() if k.startswith("th_"))

    log.info("=" * 60)
    log.info("Run Summary")
    log.info("=" * 60)
    log.info("Start time : %s", run_start.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("Duration   : %.1fs", duration)
    log.info("Session    : %s", meta["session_type"])
    log.info("-" * 60)
    log.info("APIs       : Total=%d | Success=%d | Failed=%d | Skipped=%d",
             len(api_list), len(meta["success"]), len(meta["failed"]), len(meta["skipped"]))
    log.info("Total rows : %d (US=%d, TH=%d)", total_rows, us_rows, th_rows)
    if meta["failed"]:
        log.warning("Failed     : %s", ", ".join(meta["failed"]))
    if meta["skipped"]:
        log.warning("Skipped    : %s", ", ".join(meta["skipped"]))
    if meta["dq_warnings"]:
        log.warning("DQ issues  : %d warning(s)", len(meta["dq_warnings"]))
        for w in meta["dq_warnings"]:
            log.warning("  [!] %s", w)
    log.info("=" * 60)

    # --- Alert: ส่ง notification ถ้ามี issue ---
    _send_alert_if_needed(meta, run_mode, duration, total_rows, len(api_list), log)

    # --- Gold Layer: merge US+TH + computed columns ---
    if meta["success"]:
        gold_count = save_gold_layer(run_date)
        log.info("Gold layer: %d category(ies) created.", gold_count)

    return meta


if __name__ == "__main__":
    # Daily schedule: fundamental data — 08:00 BKK ทุกวัน
    RUN_HOUR   = int(os.getenv("RUN_HOUR",   8))
    RUN_MINUTE = int(os.getenv("RUN_MINUTE", 0))

    # Intraday schedule: market movers — 04:00 และ 22:00 BKK ทุกวัน
    INTRADAY_HOUR_1 = int(os.getenv("INTRADAY_HOUR_1",  4))
    INTRADAY_HOUR_2 = int(os.getenv("INTRADAY_HOUR_2", 22))

    def _make_daily_flow():
        return main_flow(run_mode="DAILY")

    def _make_intraday_flow():
        return main_flow(run_mode="INTRADAY")

    daily_serve    = flow(name="Daily Flow - Fundamentals")(_make_daily_flow)
    intraday_serve = flow(name="Intraday Flow - Market Movers")(_make_intraday_flow)

    # รันทันทีตอน startup
    logger.info("Running all modes immediately on startup...")
    main_flow(run_mode="DAILY")
    main_flow(run_mode="INTRADAY")

    logger.info(
        "Starting Prefect schedulers:\n"
        "  DAILY    : %02d:%02d BKK (cron: %d %d * * *)\n"
        "  INTRADAY : %02d:00 BKK and %02d:00 BKK",
        RUN_HOUR, RUN_MINUTE, RUN_MINUTE, RUN_HOUR,
        INTRADAY_HOUR_1, INTRADAY_HOUR_2,
    )

    SCHEDULE_TZ = os.getenv("SCHEDULE_TZ", "Asia/Bangkok")

    from prefect.client.schemas.schedules import CronSchedule

    serve(
        daily_serve.to_deployment(
            name="daily-fundamentals",
            schedules=[CronSchedule(cron=f"{RUN_MINUTE} {RUN_HOUR} * * *", timezone=SCHEDULE_TZ)],
        ),
        intraday_serve.to_deployment(
            name="intraday-movers-morning",
            schedules=[CronSchedule(cron=f"0 {INTRADAY_HOUR_1} * * *", timezone=SCHEDULE_TZ)],
        ),
        intraday_serve.to_deployment(
            name="intraday-movers-evening",
            schedules=[CronSchedule(cron=f"0 {INTRADAY_HOUR_2} * * *", timezone=SCHEDULE_TZ)],
        ),
    )