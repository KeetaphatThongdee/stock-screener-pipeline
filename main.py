import requests
import pandas as pd
import os
import json
import time
import random
import logging
from prefect import task, flow, get_run_logger
from datetime import datetime
from playwright.sync_api import sync_playwright
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

# Fetch settings
CONNECT_TIMEOUT = 10   # ถ้า server ไม่รับ connection = server down 
READ_TIMEOUT    = 30   # รอ response body นานกว่าได้ เพราะ API อาจช้าแต่ยังทำงานอยู่
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)
MAX_429_RETRIES = 3    # จำนวนครั้ง retry เมื่อเจอ 429

# Logger สำหรับ helper functions (ทำงานนอก Prefect context)
# Prefect tasks/flow ใช้ get_run_logger() แทน เพื่อให้ log ขึ้น Prefect UI
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# stockanalysis.com อัพเดตข้อมูลทุกวัน
API_LIST = [
    # --- Dividends ---
    {
        "name": "us_dividend",
        "url": "https://stockanalysis.com/api/screener/s/bd/dps+dividendYield+payoutRatio+dividendGrowth+payoutFrequency.json",
        "description": "Dividend Data (US)"
    },
    {
        "name": "th_dividend",
        "url": "https://stockanalysis.com/api/screener/s/bd/dps+dividendYield+payoutRatio+dividendGrowth.json?c=TH",
        "description": "Dividend Data (TH)"
    },
    # --- General ---
    {
        "name": "us_general",
        "url": "https://stockanalysis.com/api/screener/s/bd/isin+exchange+sector+country+founded+fiscalYearEnd+isPrimaryListing+payoutFrequency.json",
        "description": "Company Profile (US)"
    },
    {
        "name": "th_general",
        "url": "https://stockanalysis.com/api/screener/s/bd/isin+exchange+sector+country+founded+fiscalYearEnd+isPrimaryListing+payoutFrequency.json?c=TH",
        "description": "Company Profile (TH)"
    },
    # --- Financials ---
    {
        "name": "us_financials",
        "url": "https://stockanalysis.com/api/screener/s/bd/exchange+revenue+operatingIncome+netIncome+fcf+eps.json",
        "description": "Financials (US)"
    },
    {
        "name": "th_financials",
        "url": "https://stockanalysis.com/api/screener/s/bd/exchange+revenue+operatingIncome+netIncome+fcf+eps.json?c=TH",
        "description": "Financials (TH)"
    },
    # --- Analysis ---
    {
        "name": "us_analysis",
        "url": "https://stockanalysis.com/api/screener/s/bd/exchange+analystRatings+analystCount+priceTarget+priceTargetChange.json",
        "description": "Analyst Ratings (US)"
    },
    {
        "name": "th_analysis",
        "url": "https://stockanalysis.com/api/screener/s/bd/exchange+analystRatings+analystCount+priceTarget+priceTargetChange.json?c=TH",
        "description": "Analyst Ratings (TH)"
    },
    # --- Valuation ---
    {
        "name": "us_valuation",
        "url": "https://stockanalysis.com/api/screener/s/bd/enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio.json",
        "description": "Valuation Ratios (US)"
    },
    {
        "name": "th_valuation",
        "url": "https://stockanalysis.com/api/screener/s/bd/enterpriseValue+peForward+psRatio+pbRatio+pFcfRatio.json?c=TH",
        "description": "Valuation Ratios (TH)"
    },
    # --- ETF Overall ---
    {
        "name": "us_etf",
        "url": "https://stockanalysis.com/api/screener/e/f?m=s&s=asc&c=s,n,assetClass&cn=500&p=1&i=etf",
        "description": "ETF Overall (US)"
    },
    # --- Recent IPOs (Last 200) ---
    {
        "name": "us_ipo_recent",
        "url": "https://stockanalysis.com/api/screener/s/f?m=ipoDate&s=desc&c=s,exchange,ipoPrice,sharesOffered,ds,ipoDate&cn=200&i=histip-recent",
        "description": "Recent IPOs - Last 200 (US)"
    }
]

def refresh_session_with_playwright() -> dict:
    logger.info("Starting Playwright to capture real API headers...")

    captured_session = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        page = context.new_page()

        def on_request(request):
            if "stockanalysis.com/api/screener" in request.url:
                if not captured_session:
                    captured_session["headers"] = dict(request.headers)
                    logger.info("Captured API request headers from: %s", request.url)

        page.on("request", on_request)

        try:
            page.goto("https://stockanalysis.com/stocks/screener/",
                      wait_until="domcontentloaded", timeout=60000)

            # รอให้ JS โหลดและยิง XHR request ก่อน intercept
            page.wait_for_timeout(5000)

            if not captured_session:
                page.evaluate("window.scrollTo(0, 300)")
                page.wait_for_timeout(3000)

            cookies = context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}

            if captured_session.get("headers"):

                HEADERS_BLOCKLIST = {"host", "content-length", "cookie"}
                raw_headers = captured_session["headers"]
                api_headers = {
                    k: v for k, v in raw_headers.items()
                    if not k.startswith(":")
                    and k.lower() not in HEADERS_BLOCKLIST
                }
                logger.info(
                    "Using %d filtered headers (from %d intercepted)",
                    len(api_headers), len(raw_headers)
                )
            else:
                logger.warning("Could not intercept API request. Using fallback headers.")
                api_headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://stockanalysis.com/stocks/screener/",
                    "Origin": "https://stockanalysis.com",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                }

            session_data = {
                "headers": api_headers,
                "cookies": cookie_dict,
            }

            with open(COOKIE_FILE, "w") as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            logger.info("Session saved to %s", COOKIE_FILE)

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


def load_or_refresh_session() -> dict:
    """โหลด session จากไฟล์ ถ้าไม่มีหรือเสียให้ refresh ด้วย Playwright"""
    if os.path.exists(COOKIE_FILE):
        try:
            with open(COOKIE_FILE, "r") as f:
                data = json.load(f)
            if data.get("headers") and data.get("cookies"):
                logger.info("Loaded existing session from file.")
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
    """
    Return dict แทน Session object
    เพราะ requests.Session() ไม่ serializable -> Prefect จะ error
    """
    log = get_run_logger()
    log.info("Initializing session...")
    return load_or_refresh_session()


@task(name="Fetch Raw Data", retries=3, retry_delay_seconds=5)
def fetch_url(config_item: dict, session_data: dict):
    log = get_run_logger()
    name = config_item["name"]
    url = config_item["url"]

    time.sleep(random.uniform(1.5, 3.5))
    log.info("Fetching: %s", name)

    session = build_requests_session(session_data)

    for attempt in range(MAX_429_RETRIES):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)

            if response.status_code == 403:
                log.warning("%s: 403 session expired, signaling flow to refresh...", name)
                return None, True

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait = min(5 * (2 ** attempt), 60)  # default backoff
                if retry_after:
                    try:
                        wait = int(retry_after)
                    except ValueError:
                        pass  # server ส่งมาเป็น date string ใช้ default แทน
                log.warning("%s: 429 Too Many Requests (attempt %d/%d), waiting %ds...",
                            name, attempt + 1, MAX_429_RETRIES, wait)
                time.sleep(wait)
                continue  # retry loop

            if response.status_code == 200:
                json_data = response.json()
                items = extract_items(json_data)
                log.info("%s: SUCCESS - %d items", name, len(items))
                return json_data, False

            log.error("%s: HTTP %d | %s", name, response.status_code, response.text[:200])
            return None, False

        except requests.exceptions.Timeout:
            # Timeout เป็น transient error -> raise ให้ Prefect retry (retries=3)
            log.warning("%s: Request timed out (attempt %d/%d), retrying...",
                        name, attempt + 1, MAX_429_RETRIES)
            raise

        except requests.exceptions.ConnectionError:
            # ConnectionError เช่น DNS fail, connection refused -> raise ให้ Prefect retry
            log.warning("%s: Connection error (attempt %d/%d), retrying...",
                        name, attempt + 1, MAX_429_RETRIES)
            raise

        except Exception as e:
            # error อื่นๆ ที่ไม่รู้จัก -> log แล้ว skip ไม่ retry
            log.error("%s: Unexpected error - %s", name, e)
            return None, False

    # ถ้า retry 429 ครบแล้วยังไม่ได้ -> log แล้ว skip
    log.error("%s: Giving up after %d retries on 429.", name, MAX_429_RETRIES)
    return None, False


@task(name="Save Bronze (JSON)")
def save_bronze_layer(json_data: dict, filename: str):
    """Save raw JSON to data/bronze/"""
    log = get_run_logger()
    if not json_data:
        return

    try:
        country, category = filename.split("_", 1)
    except ValueError:
        country, category = "unknown", filename

    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # ใชั DATA_DIR แทน BASE_DIR
    output_dir = os.path.join(DATA_DIR, "bronze", category, f"date={today_str}")
    os.makedirs(output_dir, exist_ok=True)

    file_path = os.path.join(output_dir, f"{country}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False)
    log.info("BRONZE saved: %s", file_path)


@task(name="Save Silver (Parquet)")
def save_silver_layer(json_data: dict, filename: str):
    """Extract table, convert to DataFrame, save as Parquet in data/silver/"""
    log = get_run_logger()
    if not json_data:
        return

    items = extract_items(json_data)
    if not items:
        log.warning("No tabular data for %s. Skipping Silver.", filename)
        return

    try:
        country, category = filename.split("_", 1)
    except ValueError:
        country, category = "unknown", filename

    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # ใชั DATA_DIR แทน BASE_DIR
    output_dir = os.path.join(DATA_DIR, "silver", category, f"date={today_str}")
    os.makedirs(output_dir, exist_ok=True)

    df = pd.DataFrame(items)
    df["fetched_at"] = datetime.now()
    
    df["flow_run_id"] = flow_run.id or "local_run"
    df["source_api"] = filename

    file_path = os.path.join(output_dir, f"{country}.parquet")
    df.to_parquet(file_path, index=False)
    log.info("SILVER saved: %s (%d rows)", file_path, len(df))

# ---------------------------------------------------------
# Main Flow
# ---------------------------------------------------------

@flow(name="Stock Scraper (Medallion Architecture)")
def main_flow():
    log = get_run_logger()
    log.info("Starting Stock Data Pipeline | Run time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    session_data = init_session()

    if not session_data:
        log.critical("Could not initialize session. Aborting.")
        return

    session_refreshed_this_run = False

    for item in API_LIST:
        json_data, need_refresh = fetch_url(item, session_data)

        if need_refresh:
            if session_refreshed_this_run:
                log.error("%s: Still 403 after refresh, skipping.", item["name"])
                continue

            log.warning("Refreshing session at flow level (once per run)...")
            new_session_data = refresh_session_with_playwright()
            session_refreshed_this_run = True

            if not new_session_data:
                log.error("%s: Refresh failed, skipping.", item["name"])
                continue

            if new_session_data.get("headers") and new_session_data.get("cookies"):
                session_data = new_session_data
            else:
                log.warning("Incomplete session data, keeping old session.")

            json_data, need_refresh = fetch_url(item, session_data)
            if need_refresh or json_data is None:
                log.error("%s: Failed after refresh, skipping.", item["name"])
                continue

        if json_data:
            save_bronze_layer(json_data, item["name"])
            save_silver_layer(json_data, item["name"])
        else:
            log.warning("%s: No data fetched, skipping.", item["name"])

    log.info("Pipeline Completed.")


if __name__ == "__main__":
    import signal
    from datetime import timedelta

    running = True
    def _handle_signal(sig, frame):
        global running
        logger.info("Shutdown signal received. Stopping after current run...")
        running = False
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    RUN_HOUR   = int(os.getenv("RUN_HOUR", 8))
    RUN_MINUTE = int(os.getenv("RUN_MINUTE", 0))

    logger.info("Scheduler started. Will run daily at %02d:%02d (Asia/Bangkok)",
                RUN_HOUR, RUN_MINUTE)

    logger.info("Running immediately on startup...")
    main_flow()

    while running:
        now = datetime.now()

        # คำนวณเวลารันครั้งถัดไป
        next_run = now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
        if now >= next_run:
            next_run = next_run + timedelta(days=1)

        wait_seconds = (next_run - now).total_seconds()
        logger.info("Next run scheduled at %s (in %.1f hours)",
                    next_run.strftime("%Y-%m-%d %H:%M:%S"),
                    wait_seconds / 3600)

        # sleep รอเป็นช่วงๆ ละ 60 วินาที เพื่อให้ SIGTERM หยุดได้ทัน
        while running and (datetime.now() < next_run):
            time.sleep(60)

        if running:
            main_flow()