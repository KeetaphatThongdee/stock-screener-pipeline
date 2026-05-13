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

# Security: จำกัดสิทธิ์ session directory เฉพาะ owner (0o700)
# ป้องกันไม่ให้ user อื่นใน container อ่าน cookies/headers ได้
try:
    os.chmod(SESSION_DIR, 0o700)
except OSError:
    pass  # Windows ไม่รองรับ chmod — skip

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

# Heartbeat: ส่ง success notification ทุกครั้งที่ pipeline รันสำเร็จ
# เพื่อให้รู้ว่า pipeline ยัง alive อยู่ (default: false — แจ้งแค่ตอน fail)
ALERT_ON_SUCCESS = os.getenv("ALERT_ON_SUCCESS", "false").lower() in ("true", "1", "yes")


# Logger สำหรับ helper functions (ทำงานนอก Prefect context)
# Prefect tasks/flow ใช้ get_run_logger() แทน เพื่อให้ log ขึ้น Prefect UI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _mask_sensitive(value: str, visible: int = 6) -> str:
    """Mask ค่า sensitive สำหรับ logging — แสดงแค่ตัวอักษรแรก + ****
    ป้องกัน webhook URL, API keys หลุดไปใน log files
    """
    if not value or len(value) <= visible:
        return "****"
    return value[:visible] + "****"

# ---------------------------------------------------------------------------
# DAILY_APIS: fundamental data — stockanalysis.com อัพเดตวันละครั้ง
# RUN_MODE=DAILY → run ตอน 08:00 BKK
# ---------------------------------------------------------------------------
DAILY_APIS = [
    # --- Dividends ---
    {"name": "us_dividend",  "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=dps+dividendYield+payoutRatio+dividendGrowth+payoutFrequency",                              "description": "Dividend Data (US)"},
    {"name": "th_dividend",  "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=dps+dividendYield+payoutRatio+dividendGrowth+payoutFrequency&c=TH",                                         "description": "Dividend Data (TH)"},
    # --- General ---
    {"name": "us_general",   "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=isin+exchange+sector+country+founded+fiscalYearEnd+isPrimaryListing",        "description": "Company Profile (US)"},
    {"name": "th_general",   "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=isin+sector+country+founded+fiscalYearEnd+isPrimaryListing&c=TH",   "description": "Company Profile (TH)"},
    # --- Financials ---
    {"name": "us_financials","url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=revenue+operatingIncome+netIncome+fcf+eps",                                        "description": "Financials (US)"},
    {"name": "th_financials","url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=revenue+operatingIncome+netIncome+fcf+eps&c=TH",                                   "description": "Financials (TH)"},
    # --- Analysis ---
    {"name": "us_analysis",  "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=analystRatings+analystCount+priceTarget+priceTargetChange+exchange",                        "description": "Analyst Ratings (US)"},
    {"name": "th_analysis",  "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=analystRatings+analystCount+priceTarget+priceTargetChange+exchange&c=TH",                   "description": "Analyst Ratings (TH)"},
    # --- Valuation ---
    {"name": "us_valuation", "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio",                                       "description": "Valuation Ratios (US)"},
    {"name": "th_valuation", "url": "https://stockanalysis.com/_api/endpoints/screener/data-points?type=s&ids=enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio&c=TH",                                  "description": "Valuation Ratios (TH)"},
    # --- ETF / IPO / Trending ---
    {"name": "us_etf",       "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=e&m=s&s=asc&c=s,price,change,volume,low52,low52ch,high52,high52ch&cn=500&p=1&i=etf",                                                    "description": "ETF Overall (US)"},
    {"name": "us_ipo_recent","url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=ipoDate&s=desc&c=ipoDate,s,n,ipoPrice,ippc,ipr&cn=200&i=histip-recent",           "description": "Recent IPOs - Last 200 (US)"},
    {"name": "us_trending",  "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=views&s=desc&c=no,s,views,tr1m,tr6m,trYTD,tr1y,tr5y,tr10y&cn=20&f=views-over-1&p=1&i=stocks",  "description": "Trending Today - Top 20 (US)"},
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
    {"name": "us_top_gainers",        "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=change&s=desc&c=no,s,n,change,price,volume,marketCap&cn=20&f=change-over-0,priceDate-isLastTradingDay&p=1&i=stock-movers",                                                             "description": "Top Gainers Today (US)"},
    {"name": "us_top_losers",         "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=change&s=asc&c=no,s,n,change,price,volume,marketCap&cn=20&f=change-under-0,priceDate-isLastTradingDay&p=1&i=stock-movers",                                                            "description": "Top Losers Today (US)"},
    {"name": "us_most_active",        "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=volume&s=desc&c=no,s,volume,price,ma50,ma200,beta,rsi&cn=20&f=volume-over-0,priceDate-isLastTradingDay&p=1&i=stock-movers",                                                           "description": "Most Active Today (US)"},
    # --- Pre/After Market ---
    {"name": "us_premarket_gainers",  "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=premarketChangePercent&s=desc&c=no,s,n,premarketChangePercent,premarketPrice,premarketVolume,marketCap&cn=20&f=premarketChangePercent-over-0,premarketDate-isLastTradingDay&p=1&i=stock-movers",   "description": "Premarket Gainers (US)"},
    {"name": "us_premarket_losers",   "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=premarketChangePercent&s=asc&c=no,s,n,premarketChangePercent,premarketPrice,premarketVolume,marketCap&cn=20&f=premarketChangePercent-under-0,premarketDate-isLastTradingDay&p=1&i=stock-movers",    "description": "Premarket Losers (US)"},
    {"name": "us_afterhours_gainers", "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=postmarketChangePercent&s=desc&c=no,s,n,postmarketChangePercent,postmarketPrice,postClose,marketCap&cn=20&f=postmarketChangePercent-over-0,postmarketDate-isLastTradingDay&p=1&i=stock-movers",    "description": "After Hours Gainers (US)"},
    {"name": "us_afterhours_losers",  "url": "https://stockanalysis.com/_api/endpoints/screener/table?type=s&m=postmarketChangePercent&s=asc&c=no,s,n,postmarketChangePercent,postmarketPrice,postClose,marketCap&cn=20&f=postmarketChangePercent-under-0,postmarketDate-isLastTradingDay&p=1&i=stock-movers",     "description": "After Hours Losers (US)"},
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
    "general":        ["s", "isin", "exchange", "sector", "country", "founded", "fiscalYearEnd", "isPrimaryListing", "payoutFrequency"],
    "financials":     ["s", "exchange", "revenue", "operatingIncome", "netIncome", "fcf", "eps"],
    "analysis":       ["s", "exchange", "analystRatings", "analystCount", "priceTarget", "priceTargetChange"],
    "valuation":      ["s", "enterpriseValue", "peForward", "psRatio", "pbRatio", "pFcfRatio"],
    "etf":            ["s", "n", "assetClass"],
    "ipo_recent":     ["s", "exchange", "ipoPrice", "sharesOffered", "ds", "ipoDate"],
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
        today_partition = f"date={pd.Timestamp(df['fetched_at'].iloc[0]).strftime('%Y-%m-%d')}"
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


def _is_valid_session(data: dict) -> bool:
    """ตรวจว่า session data มี headers + cookies ครบ"""
    return bool(data and data.get("headers") and data.get("cookies"))


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
            if _is_valid_session(data):
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


@task(name="Refresh Session (403)", retries=1, retry_delay_seconds=10)
def refresh_session_task() -> dict:
    """Prefect task wrapper สำหรับ refresh session หลังเจอ 403
    แยกเป็น task เพื่อให้ Prefect track retry/logging ได้
    """
    log = get_run_logger()
    log.warning("Refreshing session via Playwright (triggered by 403)...")
    try:
        return refresh_session_with_playwright()
    except Exception as e:
        log.error("Session refresh failed: %s", e)
        return {}


MAX_RETRIES = 3  # ใช้สำหรับทุก transient error (429, Timeout, ConnectionError)

@task(name="Fetch Raw Data")
def fetch_url(config_item: dict, session_data: dict, wave_delay: float = 0) -> tuple[dict | None, bool]:
    log = get_run_logger()
    name = config_item["name"]
    url = config_item["url"]

    # wave_delay: stagger requests across waves เพื่อ rate-limit
    # โดยไม่ต้อง block batch ก่อนหน้า (ถ้า task หนึ่งโดน 429 จะไม่บล็อกตัวอื่น)
    if wave_delay > 0:
        time.sleep(wave_delay)
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
def save_bronze_layer(json_data: dict, filename: str, run_date: str) -> str | None:
    """Save raw JSON to data/bronze/
    ใช้ timestamp ใน filename เพื่อเก็บทุก snapshot (re-run ไม่ overwrite)
    คืน file path ที่บันทึกสำเร็จ หรือ None ถ้า skip
    """
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

    # timestamp ใน filename เพื่อเก็บทุก snapshot
    # เช่น us_20260429T043000Z.json — ถ้า re-run จะไม่ overwrite ตัวก่อนหน้า
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = os.path.join(output_dir, f"{country}_{ts}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False)
    log.info("BRONZE saved: %s", file_path)
    return file_path


@task(name="Save Silver (Parquet)")
def save_silver_layer(items: list, filename: str, run_date: str) -> list[str]:
    """Convert items to DataFrame, run DQ checks, save as Parquet in data/silver/
    รับ items ที่ extract แล้วจาก main_flow (ไม่ต้อง extract_items ซ้ำ)
    คืนค่า list of DQ warning strings (เพื่อให้ main_flow รวม summary ท้าย run)
    """
    log = get_run_logger()
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

    # --- Deduplication: ถ้า re-run ในวันเดียวกัน ให้ merge กับข้อมูลเดิม ---
    # เก็บ row ล่าสุดตาม ticker 's' (ถ้ามี column 's')
    file_path = os.path.join(output_dir, f"{country}.parquet")
    if os.path.exists(file_path) and "s" in df.columns:
        try:
            existing = pd.read_parquet(file_path)
            before_count = len(df)
            combined = pd.concat([existing, df], ignore_index=True)
            # เก็บ row ล่าสุด (fetched_at มากสุด) ต่อ ticker
            combined = combined.sort_values("fetched_at", ascending=False)
            combined = combined.drop_duplicates(subset=["s"], keep="first")
            df = combined.sort_index().reset_index(drop=True)
            log.info("SILVER dedup: %s merged %d existing + %d new -> %d unique tickers",
                     filename, len(existing), before_count, len(df))
        except Exception as e:
            log.warning("SILVER dedup failed for %s, saving new data only: %s", filename, e)
    elif os.path.exists(file_path):
        log.info("SILVER overwriting existing (no 's' column for dedup): %s", file_path)

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



def _run_gold_quality_checks(
    merged: pd.DataFrame,
    category: str,
    silver_row_counts: list[int],
    log,
) -> list[str]:
    """DQ checks สำหรับ Gold layer หลัง merge + computed columns
    checks ที่ทำ:
    1. Row count — merged ต้อง >= sum ของ silver inputs (กัน row หาย)
    2. Market column — ต้องมี unique values ตามที่คาดหวัง
    3. Computed columns — ถ้ามี computed column ที่เป็น NaN 100% → warning
    """
    warnings = []
    expected_rows = sum(silver_row_counts)
    actual_rows = len(merged)

    # --- Check 1: Row count after merge ---
    if actual_rows < expected_rows:
        msg = (
            f"GOLD {category}: row count after merge ({actual_rows}) "
            f"< sum of silver inputs ({expected_rows}) — possible data loss"
        )
        log.warning("GOLD DQ WARNING: %s", msg)
        warnings.append(msg)
    else:
        log.info("GOLD DQ OK -- %s row count: %d (expected %d)",
                 category, actual_rows, expected_rows)

    # --- Check 2: Market column completeness ---
    if "market" in merged.columns:
        markets = sorted(merged["market"].dropna().unique())
        if not markets:
            msg = f"GOLD {category}: 'market' column is empty after merge"
            log.warning("GOLD DQ WARNING: %s", msg)
            warnings.append(msg)
        else:
            log.info("GOLD DQ OK -- %s markets: %s", category, markets)

    # --- Check 3: Computed columns ไม่ควรเป็น NaN 100% ---
    computed_specs = GOLD_COMPUTED_COLUMNS.get(category, [])
    for col_name, description, _ in computed_specs:
        if col_name not in merged.columns:
            continue
        null_ratio = merged[col_name].isna().mean()
        if null_ratio == 1.0:
            msg = (
                f"GOLD {category}: computed column '{col_name}' is 100% NaN "
                f"({description})"
            )
            log.warning("GOLD DQ WARNING: %s", msg)
            warnings.append(msg)
        else:
            log.info("GOLD DQ OK -- %s.%s null: %.1f%%",
                     category, col_name, null_ratio * 100)

    return warnings


@task(name="Save Gold (Merged Parquet)")
def save_gold_layer(run_date: str) -> tuple[int, list[str]]:
    """Merge US + TH silver parquet per category, add computed columns,
    save to data/gold/{category}/date={run_date}/all.parquet
    คืนค่า (จำนวน categories ที่สร้าง Gold สำเร็จ, list of DQ warnings)
    """
    log = get_run_logger()
    silver_base = os.path.join(DATA_DIR, "silver")
    gold_count = 0
    all_warnings: list[str] = []

    if not os.path.isdir(silver_base):
        log.warning("GOLD skipped: silver directory not found.")
        return 0, []

    # วน loop ทุก category ใน silver (dividend, general, financials, ...)
    today_partition = f"date={run_date}"
    for category in os.listdir(silver_base):
        partition_dir = os.path.join(silver_base, category, today_partition)
        if not os.path.isdir(partition_dir):
            continue

        # อ่านทุก parquet ใน partition (us.parquet, th.parquet, ...)
        frames = []
        silver_row_counts = []
        for parquet_file in os.listdir(partition_dir):
            if not parquet_file.endswith(".parquet"):
                continue
            market = parquet_file.replace(".parquet", "").upper()  # "us" -> "US"
            file_path = os.path.join(partition_dir, parquet_file)
            try:
                df = pd.read_parquet(file_path)
                df["market"] = market
                silver_row_counts.append(len(df))
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

        # --- DQ checks หลัง merge + computed ---
        gold_warnings = _run_gold_quality_checks(merged, category, silver_row_counts, log)
        all_warnings.extend(gold_warnings)

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

    return gold_count, all_warnings



# ---------------------------------------------------------
# Alert Notification
# ---------------------------------------------------------


def _send_alert_if_needed(
    meta: dict, run_mode: str, duration: float,
    total_rows: int, total_apis: int, log,
) -> None:
    """Webhook notification — ส่งทั้งตอน fail และ success (heartbeat)
    - มี issue (failed, skipped, DQ warnings) → ส่งเสมอ
    - ไม่มี issue + ALERT_ON_SUCCESS=true → ส่ง heartbeat
    - ไม่มี issue + ALERT_ON_SUCCESS=false → skip
    ถ้า ALERT_WEBHOOK_URL ไม่ได้ตั้งค่า จะ skip ทั้งหมด
    รองรับ Discord webhook (content) และ Slack webhook (text)
    """
    if not ALERT_WEBHOOK_URL:
        return

    has_issues = meta["failed"] or meta["skipped"] or meta["dq_warnings"]

    # --- Success heartbeat ---
    if not has_issues:
        if not ALERT_ON_SUCCESS:
            log.info("Alert: no issues detected, skipping notification.")
            return
        # ส่ง heartbeat ให้รู้ว่า pipeline ยัง alive
        message = (
            f"✅ **SUCCESS — Stock Pipeline ({run_mode})**\n"
            f"APIs: {len(meta['success'])}/{total_apis} success | "
            f"Duration: {duration:.0f}s | Rows: {total_rows}"
        )
        _send_webhook(message, log)
        return

    # --- มี issue: สร้าง alert message ---
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
    _send_webhook(message, log)


def _send_webhook(message: str, log) -> None:
    """ส่ง message ไปยัง webhook (รองรับทั้ง Discord และ Slack)"""
    masked_url = _mask_sensitive(ALERT_WEBHOOK_URL)
    try:
        payload = {"content": message, "text": message}
        resp = requests.post(
            ALERT_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )
        if resp.ok:
            log.info("Alert sent to %s (HTTP %d).", masked_url, resp.status_code)
        else:
            log.warning("Alert webhook %s returned HTTP %d: %s", masked_url, resp.status_code, resp.text[:200])
    except Exception as e:
        log.warning("Alert webhook %s failed: %s", masked_url, e)


# ---------------------------------------------------------
# Trading Day Check
# ---------------------------------------------------------

# ข้าม weekend check สำหรับ testing/backfill (เช่น อยากรัน intraday วันเสาร์)
SKIP_WEEKEND_CHECK = os.getenv("SKIP_WEEKEND_CHECK", "false").lower() in ("true", "1", "yes")


def _is_us_trading_day() -> bool:
    """เช็คว่าวันนี้เป็น US trading day (จันทร์-ศุกร์) หรือไม่
    ใช้ US Eastern Time (UTC-5/UTC-4) เป็น reference
    ตรวจเฉพาะ weekday เท่านั้น (ไม่รวม US holidays เช่น Independence Day, Thanksgiving)
    """
    # ใช้ UTC-5 เป็น EST แบบปลอดภัย (ต่างจาก EDT ไป 1 ชั่วโมง แต่ไม่กระทบ weekday check)
    us_eastern = timezone(timedelta(hours=-5))
    us_now = datetime.now(us_eastern)
    # weekday(): 0=Mon, 1=Tue, ..., 5=Sat, 6=Sun
    return us_now.weekday() < 5


# ---------------------------------------------------------
# Run Metrics Persistence (สำหรับ Dashboard)
# ---------------------------------------------------------

METRICS_DIR = os.path.join(DATA_DIR, "metrics", "runs")
os.makedirs(METRICS_DIR, exist_ok=True)


def _save_run_metrics(
    meta: dict, run_mode: str, run_date: str,
    run_start: datetime, duration: float, total_rows: int, log,
) -> None:
    """บันทึก run metadata เป็น JSON สำหรับ Dashboard อ่าน
    ไฟล์: data/metrics/runs/{run_date}_{run_mode}_{timestamp}.json
    """
    ts = run_start.strftime("%Y%m%dT%H%M%SZ")
    metrics = {
        "run_mode": run_mode,
        "run_date": run_date,
        "start_time": run_start.isoformat(),
        "duration_seconds": round(duration, 1),
        "total_rows": total_rows,
        "api_count": len(meta.get("success", [])) + len(meta.get("failed", [])) + len(meta.get("skipped", [])),
        "success_count": len(meta.get("success", [])),
        "failed_count": len(meta.get("failed", [])),
        "skipped_count": len(meta.get("skipped", [])),
        "success": meta.get("success", []),
        "failed": meta.get("failed", []),
        "skipped": meta.get("skipped", []),
        "rows": meta.get("rows", {}),
        "dq_warnings": meta.get("dq_warnings", []),
        "session_type": meta.get("session_type", "unknown"),
    }
    filename = f"{run_date}_{run_mode}_{ts}.json"
    filepath = os.path.join(METRICS_DIR, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)
        log.info("Run metrics saved: %s", filepath)
    except Exception as e:
        log.warning("Could not save run metrics: %s", e)


# ---------------------------------------------------------
# Change Detection / Anomaly Detection
# ---------------------------------------------------------

CHANGES_DIR = os.path.join(DATA_DIR, "metrics", "changes")
os.makedirs(CHANGES_DIR, exist_ok=True)

# Thresholds สำหรับ detect การเปลี่ยนแปลงที่สำคัญ (percent change)
CHANGE_THRESHOLDS: dict[str, list[tuple[str, float]]] = {
    "dividend":   [("dividendYield", 0.20)],        # yield เปลี่ยน >20%
    "analysis":   [("priceTarget", 0.15)],           # price target เปลี่ยน >15%
    "financials": [("eps", 0.25), ("revenue", 0.20)], # EPS >25%, revenue >20%
    "valuation":  [("peForward", 0.25)],             # PE เปลี่ยน >25%
}

# Categorical fields — detect เมื่อค่าเปลี่ยน (ไม่ใช่ numeric)
CATEGORICAL_CHANGES: dict[str, list[str]] = {
    "analysis": ["analystRatings"],  # เช่น Hold → Buy
}


def _find_prev_partition(category: str, today_date: str) -> str | None:
    """หา partition วันก่อนหน้าล่าสุดของ category (ไม่นับวันนี้)"""
    cat_dir = os.path.join(DATA_DIR, "silver", category)
    if not os.path.isdir(cat_dir):
        return None
    today_part = f"date={today_date}"
    partitions = sorted([
        d for d in os.listdir(cat_dir)
        if d.startswith("date=") and d != today_part
    ], reverse=True)
    return partitions[0].replace("date=", "") if partitions else None


def _detect_numeric_changes(
    today_df: pd.DataFrame, prev_df: pd.DataFrame,
    category: str, market: str, fields: list[tuple[str, float]],
) -> list[dict]:
    """เปรียบเทียบ numeric fields ระหว่าง 2 partitions"""
    changes = []
    if "s" not in today_df.columns or "s" not in prev_df.columns:
        return changes

    merged = today_df.merge(prev_df, on="s", suffixes=("_new", "_old"), how="inner")

    for col, threshold in fields:
        col_new = f"{col}_new"
        col_old = f"{col}_old"
        if col_new not in merged.columns or col_old not in merged.columns:
            continue

        # คำนวณ percent change (ข้ามแถวที่ old = 0 หรือ NaN)
        mask = merged[col_old].notna() & merged[col_new].notna() & (merged[col_old] != 0)
        subset = merged[mask].copy()
        if subset.empty:
            continue

        subset["_pct_change"] = ((subset[col_new] - subset[col_old]) / subset[col_old].abs())

        # filter เฉพาะที่เปลี่ยนเกิน threshold
        significant = subset[subset["_pct_change"].abs() > threshold]

        for _, row in significant.iterrows():
            changes.append({
                "ticker": row["s"],
                "category": category,
                "market": market,
                "field": col,
                "old_value": round(float(row[col_old]), 4) if pd.notna(row[col_old]) else None,
                "new_value": round(float(row[col_new]), 4) if pd.notna(row[col_new]) else None,
                "pct_change": round(float(row["_pct_change"]) * 100, 2),
                "type": "numeric_change",
            })
    return changes


def _detect_categorical_changes(
    today_df: pd.DataFrame, prev_df: pd.DataFrame,
    category: str, market: str, fields: list[str],
) -> list[dict]:
    """เปรียบเทียบ categorical fields (เช่น analystRatings)"""
    changes = []
    if "s" not in today_df.columns or "s" not in prev_df.columns:
        return changes

    merged = today_df.merge(prev_df, on="s", suffixes=("_new", "_old"), how="inner")

    for col in fields:
        col_new = f"{col}_new"
        col_old = f"{col}_old"
        if col_new not in merged.columns or col_old not in merged.columns:
            continue

        # filter: ทั้งสองค่าต้องไม่ใช่ None/NaN และต้องต่างกัน
        mask = merged[col_old].notna() & merged[col_new].notna()
        subset = merged[mask]
        changed = subset[subset[col_old].astype(str) != subset[col_new].astype(str)]

        for _, row in changed.iterrows():
            changes.append({
                "ticker": row["s"],
                "category": category,
                "market": market,
                "field": col,
                "old_value": str(row[col_old]),
                "new_value": str(row[col_new]),
                "pct_change": None,
                "type": "categorical_change",
            })
    return changes


def _detect_new_tickers(
    today_df: pd.DataFrame, prev_df: pd.DataFrame,
    category: str, market: str,
) -> list[dict]:
    """ตรวจจับ ticker ที่ปรากฏในวันนี้แต่ไม่มีวันก่อนหน้า"""
    if "s" not in today_df.columns or "s" not in prev_df.columns:
        return []

    new_tickers = set(today_df["s"]) - set(prev_df["s"])
    return [
        {
            "ticker": t,
            "category": category,
            "market": market,
            "field": "new_ticker",
            "old_value": None,
            "new_value": "NEW",
            "pct_change": None,
            "type": "new_ticker",
        }
        for t in sorted(new_tickers)[:50]  # จำกัด 50 เพื่อกัน noise
    ]


@task(name="Change Detection")
def run_change_detection(run_date: str) -> list[dict]:
    """เปรียบเทียบ silver data วันนี้ vs วันก่อนหน้า เพื่อตรวจจับ anomalies
    Save ผลลัพธ์ลง data/metrics/changes/{date}.json
    """
    log = get_run_logger()
    all_changes: list[dict] = []
    categories_checked = 0

    for category in CHANGE_THRESHOLDS.keys() | CATEGORICAL_CHANGES.keys():
        prev_date = _find_prev_partition(category, run_date)
        if not prev_date:
            continue

        today_dir = os.path.join(DATA_DIR, "silver", category, f"date={run_date}")
        prev_dir = os.path.join(DATA_DIR, "silver", category, f"date={prev_date}")
        if not os.path.isdir(today_dir):
            continue

        categories_checked += 1

        # เปรียบเทียบแต่ละ market (us.parquet, th.parquet)
        for parquet_file in os.listdir(today_dir):
            if not parquet_file.endswith(".parquet"):
                continue
            market = parquet_file.replace(".parquet", "")
            prev_file = os.path.join(prev_dir, parquet_file)
            if not os.path.exists(prev_file):
                continue

            try:
                today_df = pd.read_parquet(os.path.join(today_dir, parquet_file))
                prev_df = pd.read_parquet(prev_file)
            except Exception as e:
                log.warning("Change detection: could not read %s/%s: %s", category, market, e)
                continue

            # Numeric changes
            if category in CHANGE_THRESHOLDS:
                changes = _detect_numeric_changes(
                    today_df, prev_df, category, market, CHANGE_THRESHOLDS[category],
                )
                all_changes.extend(changes)

            # Categorical changes
            if category in CATEGORICAL_CHANGES:
                changes = _detect_categorical_changes(
                    today_df, prev_df, category, market, CATEGORICAL_CHANGES[category],
                )
                all_changes.extend(changes)

            # New tickers
            new = _detect_new_tickers(today_df, prev_df, category, market)
            all_changes.extend(new)

    # --- Save results ---
    result = {
        "run_date": run_date,
        "categories_checked": categories_checked,
        "total_changes": len(all_changes),
        "changes": all_changes,
    }
    filepath = os.path.join(CHANGES_DIR, f"{run_date}.json")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        log.info("Change detection: %d changes found across %d categories → %s",
                 len(all_changes), categories_checked, filepath)
    except Exception as e:
        log.warning("Could not save change detection results: %s", e)

    # Log summary
    if all_changes:
        by_type = {}
        for c in all_changes:
            by_type[c["type"]] = by_type.get(c["type"], 0) + 1
        log.info("Change detection summary: %s", by_type)
    else:
        log.info("Change detection: no significant changes detected.")

    return all_changes

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

    # --- Weekend check: INTRADAY skip วันเสาร์-อาทิตย์ (US market ปิด) ---
    if run_mode == "INTRADAY" and not SKIP_WEEKEND_CHECK and not _is_us_trading_day():
        log.info(
            "INTRADAY skipped: US market is closed (weekend). "
            "Set SKIP_WEEKEND_CHECK=true to override."
        )
        return {"skipped_reason": "weekend", "success": [], "failed": [], "skipped": [],
                "rows": {}, "dq_warnings": []}

    log.info("Starting Stock Data Pipeline | Mode: %s | Run time: %s",
             run_mode, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

    session_data = init_session()

    if not session_data:
        log.critical("Could not initialize session. Aborting.")
        return {"error": "session_init_failed"}


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
    _ = cleanup_old_partitions()

    # --- Phase 1: Concurrent fetch — submit ทุก task พร้อมกัน ด้วย staggered delay ---
    MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", 5))
    WAVE_DELAY = 3.0  # seconds between each wave (rate-limit ต่อ server)
    log.info("Fetching %d APIs (max %d per wave, %.0fs between waves)...",
             len(api_list), MAX_CONCURRENT, WAVE_DELAY)

    # submit ทุก task ทีเดียว — แต่ละ task มี wave_delay ตาม position
    # wave 0 (task 0-4): delay=0, wave 1 (task 5-9): delay=3s, ...
    # ถ้า task ใน wave 0 โดน 429 retry → task ใน wave 1+ ไม่ถูก block
    futures = {}  # {api_name: (item, future)}
    for idx, item in enumerate(api_list):
        wave = idx // MAX_CONCURRENT
        delay = wave * WAVE_DELAY
        future = fetch_url.submit(item, session_data, delay)
        futures[item["name"]] = (item, future)

    # เก็บผลทั้งหมด (truly concurrent — ไม่ block ข้าม wave)
    results = {}
    for name, (item, future) in futures.items():
        json_data, need_refresh = future.result()
        results[name] = (item, json_data, need_refresh)

    # --- Phase 2: Handle 403 — refresh session แล้ว re-fetch เฉพาะตัวที่ fail ---
    needs_refresh = [name for name, (_, _, nr) in results.items() if nr]
    if needs_refresh:
        log.warning("%d API(s) returned 403, refreshing session...", len(needs_refresh))
        new_session_data = refresh_session_task()
        meta["session_type"] = "refreshed"

        if _is_valid_session(new_session_data):
            session_data = new_session_data

            # re-fetch เฉพาะตัวที่ 403
            retry_futures = {}
            for name in needs_refresh:
                item = results[name][0]
                future = fetch_url.submit(item, session_data)
                retry_futures[name] = (item, future)

            for name, (item, future) in retry_futures.items():
                json_data, need_refresh = future.result()
                if need_refresh or json_data is None:
                    log.error("%s: Still failed after refresh, skipping.", name)
                    results[name] = (item, None, False)  # mark as failed
                    meta["skipped"].append(name)
                else:
                    results[name] = (item, json_data, False)
        else:
            log.error("Session refresh failed, skipping all 403 APIs.")
            meta["session_type"] = "fallback"
            for name in needs_refresh:
                meta["skipped"].append(name)

    # --- Phase 3: Save Bronze + Silver สำหรับ API ที่สำเร็จ ---
    for name, (item, json_data, need_refresh) in results.items():
        if need_refresh:
            # ยังไม่ถูก handle ใน Phase 2 (ไม่ควรเกิด แต่ safe guard)
            meta["skipped"].append(name)
            continue

        if json_data:
            items = extract_items(json_data)
            if not items:
                log.warning("%s: No tabular data extracted, skipping.", name)
                meta["failed"].append(name)
                continue
            meta["success"].append(name)
            meta["rows"][name] = len(items)
            save_bronze_layer(json_data, name, run_date)
            warnings = save_silver_layer(items, name, run_date)
            if warnings:
                meta["dq_warnings"].extend(warnings)
        else:
            if name not in meta["skipped"]:
                log.warning("%s: No data fetched, skipping.", name)
                meta["failed"].append(name)

    # --- Run Summary (ส่วนที่ไม่เกี่ยวกับ DQ) ---
    duration  = (datetime.now(timezone.utc) - run_start).total_seconds()
    total_rows = sum(meta["rows"].values())
    us_rows   = sum(v for k, v in meta["rows"].items() if k.startswith("us_"))
    th_rows   = sum(v for k, v in meta["rows"].items() if k.startswith("th_"))

    # --- Gold Layer: merge US+TH + computed columns ---
    if meta["success"]:
        gold_count, gold_warnings = save_gold_layer(run_date)
        if gold_warnings:
            meta["dq_warnings"].extend(gold_warnings)
        log.info("Gold layer: %d category(ies) created.", gold_count)

    # --- Change Detection: เปรียบเทียบกับวันก่อนหน้า (เฉพาะ DAILY) ---
    if run_mode == "DAILY" and meta["success"]:
        cd_changes = run_change_detection(run_date)
        if cd_changes:
            meta["changes_detected"] = len(cd_changes)

    # --- Run Summary (log หลัง Gold เพื่อให้ DQ warnings ครบ) ---
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

    # --- Alert: ส่ง notification หลัง Gold layer เพื่อให้ DQ warnings ครบทุก layer ---
    _send_alert_if_needed(meta, run_mode, duration, total_rows, len(api_list), log)

    # --- Persist run metrics สำหรับ Dashboard ---
    _save_run_metrics(meta, run_mode, run_date, run_start, duration, total_rows, log)

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

    # รันทันทีตอน startup — เก็บ meta เพื่อ print combined summary
    logger.info("Running all modes immediately on startup...")
    startup_start = datetime.now(timezone.utc)
    daily_meta    = main_flow(run_mode="DAILY")
    intraday_meta = main_flow(run_mode="INTRADAY")
    startup_secs  = (datetime.now(timezone.utc) - startup_start).total_seconds()

    # --- Combined Startup Summary ---
    logger.info("=" * 60)
    logger.info("STARTUP SUMMARY")
    logger.info("=" * 60)
    for mode, meta in [("DAILY", daily_meta), ("INTRADAY", intraday_meta)]:
        if isinstance(meta, dict) and "success" in meta:
            total = len(meta.get("success", [])) + len(meta.get("failed", [])) + len(meta.get("skipped", []))
            rows  = sum(meta.get("rows", {}).values())
            logger.info("  %-10s: %d/%d APIs OK | %d rows | %d warnings",
                        mode, len(meta["success"]), total, rows, len(meta.get("dq_warnings", [])))
        else:
            logger.warning("  %-10s: ERROR — %s", mode, meta)
    logger.info("-" * 60)
    logger.info("  Total time: %.1fs", startup_secs)
    # DQ warnings รวม
    all_warnings = []
    for meta in [daily_meta, intraday_meta]:
        if isinstance(meta, dict):
            all_warnings.extend(meta.get("dq_warnings", []))
    if all_warnings:
        logger.warning("  DQ issues : %d warning(s)", len(all_warnings))
        for w in all_warnings:
            logger.warning("    [!] %s", w)
    logger.info("=" * 60)

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