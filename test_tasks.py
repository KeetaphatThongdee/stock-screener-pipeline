"""Unit tests for Prefect tasks and pipeline functions.

Covers:
- fetch_url              (retry logic, 403/429 handling, timeout)
- save_bronze_layer      (file I/O, timestamp filenames)
- save_silver_layer      (parquet I/O, deduplication)
- save_gold_layer        (merge logic, computed columns)
- cleanup_old_partitions (partition deletion)
- _run_gold_quality_checks (Gold DQ validation)
"""

import json
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import main
from main import (
    _run_gold_quality_checks,
    GOLD_COMPUTED_COLUMNS,
)


# ---------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------

@pytest.fixture
def mock_log():
    return logging.getLogger("test_tasks")


@pytest.fixture
def data_dir(tmp_path):
    """Temporarily override DATA_DIR to use tmp_path."""
    original = main.DATA_DIR
    main.DATA_DIR = str(tmp_path)
    yield tmp_path
    main.DATA_DIR = original


# =========================================================
# fetch_url — ใช้ .fn() เพื่อเรียก task function โดยตรง
# =========================================================

class TestFetchUrl:
    """Test fetch_url logic by mocking requests.Session and Prefect logger."""

    @pytest.fixture(autouse=True)
    def _patch_sleep_and_logger(self):
        with patch("main.time.sleep"), \
             patch("main.get_run_logger", return_value=logging.getLogger("test")), \
             patch("main.random.uniform", return_value=0):
            yield

    def _make_config(self, name="us_test", url="https://example.com/api"):
        return {"name": name, "url": url}

    def _make_session_data(self):
        return {"headers": {"User-Agent": "test"}, "cookies": {}}

    @patch("main.build_requests_session")
    def test_success_200(self, mock_build):
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"data": {"data": {"AAPL": {"eps": 6.0}}}}
        mock_build.return_value.get.return_value = mock_resp

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is not None
        assert need_refresh is False

    @patch("main.build_requests_session")
    def test_403_signals_refresh(self, mock_build):
        mock_resp = MagicMock(status_code=403, text="Forbidden")
        mock_build.return_value.get.return_value = mock_resp

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is None
        assert need_refresh is True

    @patch("main.build_requests_session")
    def test_429_retries_then_fails(self, mock_build):
        mock_resp = MagicMock(status_code=429, text="Too Many Requests")
        mock_resp.headers = {}
        mock_build.return_value.get.return_value = mock_resp

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is None
        assert need_refresh is False
        # ต้องเรียก .get() 3 ครั้ง (MAX_RETRIES)
        assert mock_build.return_value.get.call_count == main.MAX_RETRIES

    @patch("main.build_requests_session")
    def test_500_no_retry(self, mock_build):
        mock_resp = MagicMock(status_code=500, text="Server Error")
        mock_build.return_value.get.return_value = mock_resp

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is None
        assert need_refresh is False
        assert mock_build.return_value.get.call_count == 1

    @patch("main.build_requests_session")
    def test_timeout_retries(self, mock_build):
        import requests as req
        mock_build.return_value.get.side_effect = req.exceptions.Timeout("timeout")

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is None
        assert mock_build.return_value.get.call_count == main.MAX_RETRIES

    @patch("main.build_requests_session")
    def test_invalid_json_response(self, mock_build):
        import requests as req
        mock_resp = MagicMock(status_code=200, text="not json")
        mock_resp.json.side_effect = req.exceptions.JSONDecodeError("err", "doc", 0)
        mock_build.return_value.get.return_value = mock_resp

        result, need_refresh = main.fetch_url.fn(
            self._make_config(), self._make_session_data()
        )
        assert result is None
        assert need_refresh is False


# =========================================================
# save_bronze_layer
# =========================================================

class TestSaveBronzeLayer:

    @pytest.fixture(autouse=True)
    def _patch_logger(self):
        with patch("main.get_run_logger", return_value=logging.getLogger("test")):
            yield

    def test_saves_json_with_timestamp(self, data_dir):
        json_data = {"data": {"AAPL": {"eps": 6.0}}}
        result = main.save_bronze_layer.fn(json_data, "us_dividend", "2026-04-29")

        assert result is not None
        assert os.path.exists(result)
        assert result.endswith(".json")
        # filename ต้องมี timestamp pattern
        basename = os.path.basename(result)
        assert basename.startswith("us_")
        assert "T" in basename  # timestamp format: us_20260429T...Z.json

    def test_skip_empty_data(self, data_dir):
        result = main.save_bronze_layer.fn({}, "us_dividend", "2026-04-29")
        assert result is None

    def test_skip_none_data(self, data_dir):
        result = main.save_bronze_layer.fn(None, "us_dividend", "2026-04-29")
        assert result is None

    def test_multiple_runs_no_overwrite(self, data_dir):
        """Re-run ต้องสร้างไฟล์ใหม่ ไม่ overwrite"""
        json_data = {"data": {"AAPL": {"eps": 6.0}}}

        # mock timestamp ให้ต่างกันเพื่อจำลอง 2 runs คนละเวลา
        with patch("main.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 4, 29, 10, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            path1 = main.save_bronze_layer.fn(json_data, "us_dividend", "2026-04-29")

            mock_dt.now.return_value = datetime(2026, 4, 29, 10, 0, 1, tzinfo=timezone.utc)
            path2 = main.save_bronze_layer.fn(json_data, "us_dividend", "2026-04-29")

        assert path1 != path2
        assert os.path.exists(path1)
        assert os.path.exists(path2)

    def test_correct_directory_structure(self, data_dir):
        json_data = {"data": {"AAPL": {"eps": 6.0}}}
        result = main.save_bronze_layer.fn(json_data, "us_dividend", "2026-04-29")
        # ต้องอยู่ใน bronze/dividend/date=2026-04-29/
        assert "bronze" in result
        assert "dividend" in result
        assert "date=2026-04-29" in result


# =========================================================
# save_silver_layer
# =========================================================

class TestSaveSilverLayer:

    @pytest.fixture(autouse=True)
    def _patch_logger(self):
        with patch("main.get_run_logger", return_value=logging.getLogger("test")):
            yield

    def test_saves_parquet(self, data_dir):
        items = [{"s": "AAPL", "dps": 1.0}, {"s": "MSFT", "dps": 2.0}]
        warnings = main.save_silver_layer.fn(items, "us_dividend", "2026-04-29")

        parquet_path = data_dir / "silver" / "dividend" / "date=2026-04-29" / "us.parquet"
        assert parquet_path.exists()
        df = pd.read_parquet(parquet_path)
        assert len(df) == 2
        assert "fetched_at" in df.columns
        assert "flow_run_id" in df.columns

    def test_empty_items_returns_empty(self, data_dir):
        warnings = main.save_silver_layer.fn([], "us_dividend", "2026-04-29")
        assert warnings == []

    def test_dedup_on_rerun(self, data_dir):
        """Re-run ต้อง dedup บน ticker 's' เก็บ row ล่าสุด"""
        items1 = [{"s": "AAPL", "dps": 1.0}, {"s": "MSFT", "dps": 2.0}]
        main.save_silver_layer.fn(items1, "us_dividend", "2026-04-29")

        items2 = [{"s": "AAPL", "dps": 1.5}, {"s": "GOOG", "dps": 3.0}]
        main.save_silver_layer.fn(items2, "us_dividend", "2026-04-29")

        parquet_path = data_dir / "silver" / "dividend" / "date=2026-04-29" / "us.parquet"
        df = pd.read_parquet(parquet_path)
        # AAPL, MSFT, GOOG — 3 unique tickers
        assert len(df) == 3
        # AAPL ต้องเป็นค่าใหม่ (1.5)
        aapl_row = df[df["s"] == "AAPL"].iloc[0]
        assert aapl_row["dps"] == 1.5


# =========================================================
# cleanup_old_partitions
# =========================================================

class TestCleanupOldPartitions:

    @pytest.fixture(autouse=True)
    def _patch_logger(self):
        with patch("main.get_run_logger", return_value=logging.getLogger("test")):
            yield

    def test_deletes_old_partitions(self, data_dir):
        """Partition เก่ากว่า RETENTION_DAYS ต้องถูกลบ"""
        original_retention = main.RETENTION_DAYS
        main.RETENTION_DAYS = 7

        # สร้าง partition เก่า (10 วันก่อน) + ใหม่ (วันนี้)
        old_date = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
        new_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        old_dir = data_dir / "silver" / "dividend" / f"date={old_date}"
        new_dir = data_dir / "silver" / "dividend" / f"date={new_date}"
        old_dir.mkdir(parents=True)
        new_dir.mkdir(parents=True)
        (old_dir / "us.parquet").write_text("old")
        (new_dir / "us.parquet").write_text("new")

        main.cleanup_old_partitions.fn()

        assert not old_dir.exists()
        assert new_dir.exists()

        main.RETENTION_DAYS = original_retention

    def test_keeps_recent_partitions(self, data_dir):
        """Partition ใหม่กว่า RETENTION_DAYS ต้องไม่ถูกลบ"""
        original_retention = main.RETENTION_DAYS
        main.RETENTION_DAYS = 90

        recent_date = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")
        recent_dir = data_dir / "bronze" / "dividend" / f"date={recent_date}"
        recent_dir.mkdir(parents=True)
        (recent_dir / "us.json").write_text("{}")

        main.cleanup_old_partitions.fn()

        assert recent_dir.exists()

        main.RETENTION_DAYS = original_retention

    def test_skips_invalid_partition_names(self, data_dir):
        """Partition ที่ชื่อไม่ใช่ date= format ต้อง skip ไม่ crash"""
        invalid_dir = data_dir / "silver" / "dividend" / "date=invalid"
        invalid_dir.mkdir(parents=True)
        (invalid_dir / "us.parquet").write_text("data")

        # ไม่ควร crash
        main.cleanup_old_partitions.fn()
        assert invalid_dir.exists()


# =========================================================
# _run_gold_quality_checks
# =========================================================

class TestRunGoldQualityChecks:

    def test_row_count_ok(self, mock_log):
        merged = pd.DataFrame({"s": ["AAPL", "MSFT", "PTT"], "market": ["US", "US", "TH"]})
        warnings = _run_gold_quality_checks(merged, "dividend", [2, 1], mock_log)
        assert warnings == []

    def test_row_count_drop_warning(self, mock_log):
        """Row count หลัง merge < sum of silver inputs → warning"""
        merged = pd.DataFrame({"s": ["AAPL"], "market": ["US"]})
        warnings = _run_gold_quality_checks(merged, "dividend", [5, 3], mock_log)
        assert len(warnings) == 1
        assert "possible data loss" in warnings[0]

    def test_empty_market_warning(self, mock_log):
        merged = pd.DataFrame({"s": ["AAPL"], "market": [None]})
        warnings = _run_gold_quality_checks(merged, "dividend", [1], mock_log)
        assert any("empty" in w for w in warnings)

    def test_computed_column_all_nan_warning(self, mock_log):
        """Computed column ที่เป็น NaN 100% → warning"""
        merged = pd.DataFrame({
            "s": ["AAPL", "MSFT"],
            "market": ["US", "US"],
            "netIncome": [100.0, 200.0],
            "revenue": [1000.0, 2000.0],
            "operatingIncome": [50.0, 100.0],
            "fcf": [30.0, 60.0],
            "profit_margin": [np.nan, np.nan],
            "operating_margin": [0.05, 0.05],
            "fcf_margin": [0.03, 0.03],
        })
        warnings = _run_gold_quality_checks(merged, "financials", [2], mock_log)
        assert any("profit_margin" in w and "100% NaN" in w for w in warnings)

    def test_computed_column_partial_nan_ok(self, mock_log):
        """Computed column มีค่าบ้าง → ไม่ warning"""
        merged = pd.DataFrame({
            "s": ["AAPL", "MSFT"],
            "market": ["US", "US"],
            "profit_margin": [0.1, np.nan],
            "operating_margin": [0.05, 0.05],
            "fcf_margin": [0.03, 0.03],
            "netIncome": [100.0, 200.0],
            "revenue": [1000.0, 2000.0],
            "operatingIncome": [50.0, 100.0],
            "fcf": [30.0, 60.0],
        })
        warnings = _run_gold_quality_checks(merged, "financials", [2], mock_log)
        assert not any("profit_margin" in w for w in warnings)


# =========================================================
# save_gold_layer — integration test with tmp_path
# =========================================================

class TestSaveGoldLayer:

    @pytest.fixture(autouse=True)
    def _patch_logger(self):
        with patch("main.get_run_logger", return_value=logging.getLogger("test")):
            yield

    def _create_silver(self, data_dir, category, run_date, market, df):
        """Helper: สร้าง silver parquet สำหรับ test"""
        silver_dir = data_dir / "silver" / category / f"date={run_date}"
        silver_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(silver_dir / f"{market}.parquet", index=False)

    def test_merge_us_th(self, data_dir):
        """Merge US + TH silver → Gold all.parquet"""
        run_date = "2026-04-29"
        us_df = pd.DataFrame({"s": ["AAPL"], "revenue": [1000.0], "fetched_at": datetime.now(timezone.utc)})
        th_df = pd.DataFrame({"s": ["PTT"], "revenue": [500.0], "fetched_at": datetime.now(timezone.utc)})

        self._create_silver(data_dir, "financials", run_date, "us", us_df)
        self._create_silver(data_dir, "financials", run_date, "th", th_df)

        gold_count, warnings = main.save_gold_layer.fn(run_date)

        assert gold_count >= 1
        gold_path = data_dir / "gold" / "financials" / f"date={run_date}" / "all.parquet"
        assert gold_path.exists()
        merged = pd.read_parquet(gold_path)
        assert len(merged) == 2
        assert set(merged["market"].unique()) == {"US", "TH"}

    def test_no_silver_returns_zero(self, data_dir):
        gold_count, warnings = main.save_gold_layer.fn("2026-04-29")
        assert gold_count == 0

    def test_computed_columns_added(self, data_dir):
        """Gold ต้องมี computed columns (profit_margin ฯลฯ)"""
        run_date = "2026-04-29"
        df = pd.DataFrame({
            "s": ["AAPL", "MSFT"],
            "exchange": ["NASDAQ", "NASDAQ"],
            "revenue": [1000.0, 2000.0],
            "operatingIncome": [200.0, 400.0],
            "netIncome": [100.0, 200.0],
            "fcf": [80.0, 160.0],
            "eps": [6.0, 11.0],
            "fetched_at": datetime.now(timezone.utc),
        })
        self._create_silver(data_dir, "financials", run_date, "us", df)

        gold_count, warnings = main.save_gold_layer.fn(run_date)
        gold_path = data_dir / "gold" / "financials" / f"date={run_date}" / "all.parquet"
        merged = pd.read_parquet(gold_path)

        assert "profit_margin" in merged.columns
        assert "operating_margin" in merged.columns
        assert "fcf_margin" in merged.columns
        assert merged["profit_margin"].iloc[0] == pytest.approx(0.1)
