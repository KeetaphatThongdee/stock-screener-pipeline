"""Unit tests for the stock scraper pipeline.

Covers:
- extract_items / _dict_to_records  (data extraction)
- _enforce_schema                   (schema validation)
- _run_quality_checks               (data quality: null ratio)
- _safe_divide                      (division by zero protection)
- GOLD_COMPUTED_COLUMNS lambdas     (computed column formulas)
"""

import logging
import os

import numpy as np
import pandas as pd
import pytest

from main import (
    extract_items,
    _dict_to_records,
    _enforce_schema,
    _run_quality_checks,
    _safe_divide,
    GOLD_COMPUTED_COLUMNS,
    DQ_KEY_COLUMNS,
    NULL_THRESHOLD,
)


# ---------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------

@pytest.fixture
def mock_log():
    """Mock logger ที่มี .warning() .info() .error() เหมือน Prefect run logger"""
    return logging.getLogger("test")


# =========================================================
# _dict_to_records
# =========================================================

class TestDictToRecords:
    def test_dict_of_dicts(self):
        """ปกติ: ticker -> dict of fields"""
        data = {"AAPL": {"eps": 6.57, "revenue": 100}, "MSFT": {"eps": 11.86, "revenue": 200}}
        result = _dict_to_records(data)
        assert len(result) == 2
        assert {"s": "AAPL", "eps": 6.57, "revenue": 100} in result
        assert {"s": "MSFT", "eps": 11.86, "revenue": 200} in result

    def test_dict_of_scalars(self):
        """กรณี value ไม่ใช่ dict → ใส่ key 'value' แทน"""
        data = {"AAPL": 150.0, "MSFT": 300.0}
        result = _dict_to_records(data)
        assert result == [
            {"s": "AAPL", "value": 150.0},
            {"s": "MSFT", "value": 300.0},
        ]

    def test_empty_dict(self):
        assert _dict_to_records({}) == []


# =========================================================
# extract_items — input เป็น list โดยตรง
# =========================================================

class TestExtractItemsList:
    def test_plain_list(self):
        """ถ้า input เป็น list อยู่แล้ว → return ตรงๆ"""
        data = [{"s": "AAPL"}, {"s": "MSFT"}]
        assert extract_items(data) == data

    def test_empty_list(self):
        assert extract_items([]) == []


# =========================================================
# extract_items — input ไม่ใช่ dict/list
# =========================================================

class TestExtractItemsInvalid:
    def test_none(self):
        assert extract_items(None) == []

    def test_string(self):
        assert extract_items("not a dict") == []

    def test_int(self):
        assert extract_items(42) == []


# =========================================================
# extract_items — nested dict with known keys
# =========================================================

class TestExtractItemsNested:
    def test_data_key_with_list(self):
        """API response: {"data": [...]}"""
        items = [{"s": "AAPL", "price": 150}]
        assert extract_items({"data": items}) == items

    def test_results_key_with_list(self):
        """API response: {"results": [...]}"""
        items = [{"s": "TSLA"}]
        assert extract_items({"results": items}) == items

    def test_data_key_with_dict_of_dicts(self):
        """API response: {"data": {"AAPL": {...}, "MSFT": {...}}}"""
        raw = {"data": {"AAPL": {"eps": 6.57}, "MSFT": {"eps": 11.86}}}
        result = extract_items(raw)
        assert len(result) == 2
        assert result[0]["s"] in ("AAPL", "MSFT")

    def test_double_nested_data(self):
        """API response: {"data": {"data": {"AAPL": {...}}}}
        ขั้นแรก drill เข้า data (dict) → candidate = {"data": {"AAPL": ...}}
        ขั้นสอง หา key "data" → เจอ dict-of-dicts → convert
        """
        raw = {"data": {"data": {"AAPL": {"eps": 6.57}}}}
        result = extract_items(raw)
        assert len(result) == 1
        assert result[0]["s"] == "AAPL"
        assert result[0]["eps"] == 6.57

    def test_data_wrapping_items_list(self):
        """API response: {"data": {"items": [...]}}"""
        items = [{"s": "GOOG"}]
        raw = {"data": {"items": items}}
        result = extract_items(raw)
        assert result == items


# =========================================================
# extract_items — dict-of-dicts โดยตรง (no nested key)
# =========================================================

class TestExtractItemsFlatDictOfDicts:
    def test_direct_dict_of_dicts(self):
        """Input เป็น {ticker: {fields}} โดยตรง ไม่มี data/results wrapper"""
        raw = {
            "AAPL": {"eps": 6.57, "pe": 25},
            "MSFT": {"eps": 11.86, "pe": 30},
            "GOOG": {"eps": 5.0, "pe": 22},
            "AMZN": {"eps": 3.0, "pe": 50},
            "META": {"eps": 8.0, "pe": 20},
        }
        result = extract_items(raw)
        assert len(result) == 5
        tickers = {r["s"] for r in result}
        assert tickers == {"AAPL", "MSFT", "GOOG", "AMZN", "META"}

    def test_dict_without_nested_keys_non_dict_values(self):
        """dict ที่ values ไม่ใช่ dict → return []"""
        raw = {"key1": "value1", "key2": "value2"}
        assert extract_items(raw) == []


# =========================================================
# extract_items — real-world API response shapes
# =========================================================

class TestExtractItemsRealWorld:
    def test_screener_bd_response(self):
        """จำลอง response จาก /api/screener/s/bd/ (bulk data)"""
        raw = {
            "data": {
                "data": {
                    "AAPL": {"dividendYield": 0.55, "dps": 0.96},
                    "MSFT": {"dividendYield": 0.72, "dps": 3.0},
                }
            }
        }
        result = extract_items(raw)
        assert len(result) == 2

    def test_screener_f_response(self):
        """จำลอง response จาก /api/screener/s/f (filtered) — returns list"""
        raw = {
            "data": {
                "data": [
                    {"no": 1, "s": "NVDA", "change": 5.2},
                    {"no": 2, "s": "TSLA", "change": 4.1},
                ]
            }
        }
        result = extract_items(raw)
        assert len(result) == 2
        assert result[0]["s"] == "NVDA"


# =========================================================
# _enforce_schema
# =========================================================

class TestEnforceSchema:
    def test_all_columns_present(self, mock_log):
        """ถ้ามีทุก column → ไม่มี warning"""
        df = pd.DataFrame({"s": ["AAPL"], "dividendYield": [0.5], "dps": [1.0],
                           "payoutRatio": [0.3], "dividendGrowth": [0.1]})
        warnings = _enforce_schema(df, "dividend", "us_dividend", mock_log)
        assert warnings == []

    def test_missing_columns_added_as_nan(self, mock_log):
        """ถ้า column หายไป → เติม NaN + return warning"""
        df = pd.DataFrame({"s": ["AAPL"], "dividendYield": [0.5]})
        warnings = _enforce_schema(df, "dividend", "us_dividend", mock_log)
        assert len(warnings) == 1
        assert "missing columns" in warnings[0]
        # ต้องมี column ที่เติมมาใน df
        assert "dps" in df.columns
        assert "payoutRatio" in df.columns
        assert "dividendGrowth" in df.columns
        assert df["dps"].isna().all()

    def test_extra_columns_kept(self, mock_log):
        """ถ้ามี column ใหม่ → เก็บไว้ ไม่ warning"""
        df = pd.DataFrame({"s": ["AAPL"], "dividendYield": [0.5], "dps": [1.0],
                           "payoutRatio": [0.3], "dividendGrowth": [0.1],
                           "newColumn": [42]})
        warnings = _enforce_schema(df, "dividend", "us_dividend", mock_log)
        assert warnings == []
        assert "newColumn" in df.columns

    def test_unknown_category_skipped(self, mock_log):
        """ถ้า category ไม่อยู่ใน EXPECTED_SCHEMA → skip ไม่ error"""
        df = pd.DataFrame({"s": ["AAPL"], "foo": [1]})
        warnings = _enforce_schema(df, "unknown_category", "test", mock_log)
        assert warnings == []

    def test_pipeline_columns_not_flagged_as_extra(self, mock_log):
        """metadata columns (fetched_at, flow_run_id, source_api) ไม่ควรถูก flag"""
        df = pd.DataFrame({"s": ["AAPL"], "dividendYield": [0.5], "dps": [1.0],
                           "payoutRatio": [0.3], "dividendGrowth": [0.1],
                           "fetched_at": ["2026-01-01"], "flow_run_id": ["abc"],
                           "source_api": ["us_dividend"]})
        warnings = _enforce_schema(df, "dividend", "us_dividend", mock_log)
        assert warnings == []


# =========================================================
# _run_quality_checks — null ratio only (row count drop ต้องใช้ disk)
# =========================================================

class TestRunQualityChecksNullRatio:
    def test_low_null_no_warning(self, mock_log, tmp_path):
        """null ratio ต่ำกว่า threshold → ไม่มี warning"""
        df = pd.DataFrame({
            "s": ["AAPL", "MSFT", "GOOG"],
            "dividendYield": [0.5, 0.7, 0.3],
            "dps": [1.0, 2.0, 3.0],
            "fetched_at": pd.Timestamp("2026-01-01", tz="UTC"),
        })
        # ใช้ tmp_path เป็น DATA_DIR เพื่อไม่ให้หา prev partition
        import main
        original_data_dir = main.DATA_DIR
        main.DATA_DIR = str(tmp_path)
        try:
            warnings = _run_quality_checks(df, "us_dividend", "dividend", "us", mock_log)
            assert warnings == []
        finally:
            main.DATA_DIR = original_data_dir

    def test_high_null_triggers_warning(self, mock_log, tmp_path):
        """null ratio สูงกว่า threshold → ได้ warning"""
        # สร้าง df ที่ dps เป็น null > 50%
        df = pd.DataFrame({
            "s": ["A", "B", "C", "D"],
            "dividendYield": [0.5, 0.7, None, None],
            "dps": [None, None, None, 1.0],  # 75% null
            "fetched_at": pd.Timestamp("2026-01-01", tz="UTC"),
        })
        import main
        original_data_dir = main.DATA_DIR
        main.DATA_DIR = str(tmp_path)
        try:
            warnings = _run_quality_checks(df, "us_dividend", "dividend", "us", mock_log)
            assert len(warnings) >= 1
            assert any("dps" in w for w in warnings)
        finally:
            main.DATA_DIR = original_data_dir

    def test_known_data_gap_skipped(self, mock_log, tmp_path):
        """th_analysis ไม่มี analyst data → skip null check"""
        df = pd.DataFrame({
            "s": ["PTT", "SCB"],
            "analystRatings": [None, None],  # 100% null
            "fetched_at": pd.Timestamp("2026-01-01", tz="UTC"),
        })
        import main
        original_data_dir = main.DATA_DIR
        main.DATA_DIR = str(tmp_path)
        try:
            warnings = _run_quality_checks(df, "th_analysis", "analysis", "th", mock_log)
            # ควรเป็น [] เพราะ DQ_KEY_COLUMNS["analysis"]["th"] = None
            assert warnings == []
        finally:
            main.DATA_DIR = original_data_dir


# =========================================================
# _safe_divide
# =========================================================

class TestSafeDivide:
    def test_normal_division(self):
        """ปกติ: 10/5 = 2.0"""
        num = pd.Series([10.0, 20.0, 30.0])
        den = pd.Series([5.0, 4.0, 10.0])
        result = _safe_divide(num, den)
        expected = pd.Series([2.0, 5.0, 3.0])
        pd.testing.assert_series_equal(result, expected)

    def test_division_by_zero_returns_nan(self):
        """division by zero → NaN ไม่ใช่ inf"""
        num = pd.Series([10.0, 20.0, 0.0])
        den = pd.Series([0.0, 0.0, 0.0])
        result = _safe_divide(num, den)
        assert result.isna().all()
        # ตรวจว่าไม่มี inf หลุดรอด (convert เป็น float ก่อนเช็ค)
        numeric = pd.to_numeric(result, errors="coerce")
        assert not np.isinf(numeric.fillna(0)).any()

    def test_mixed_zero_and_normal(self):
        """บาง row เป็น 0 บาง row ปกติ"""
        num = pd.Series([10.0, 20.0, 30.0])
        den = pd.Series([5.0, 0.0, 10.0])
        result = _safe_divide(num, den)
        assert result.iloc[0] == 2.0
        assert pd.isna(result.iloc[1])
        assert result.iloc[2] == 3.0

    def test_negative_inf_also_replaced(self):
        """negative / zero → -inf → ต้องเป็น NaN"""
        num = pd.Series([-10.0])
        den = pd.Series([0.0])
        result = _safe_divide(num, den)
        assert pd.isna(result.iloc[0])

    def test_nan_input_stays_nan(self):
        """NaN input → NaN output"""
        num = pd.Series([np.nan, 10.0])
        den = pd.Series([5.0, np.nan])
        result = _safe_divide(num, den)
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])


# =========================================================
# GOLD_COMPUTED_COLUMNS — ทดสอบ lambda formulas
# =========================================================

class TestGoldComputedColumns:
    def test_financials_profit_margin(self):
        """profit_margin = netIncome / revenue"""
        df = pd.DataFrame({"netIncome": [100.0, 50.0], "revenue": [500.0, 200.0],
                           "operatingIncome": [200.0, 100.0], "fcf": [80.0, 40.0]})
        specs = GOLD_COMPUTED_COLUMNS["financials"]
        # profit_margin
        result = specs[0][2](df)
        assert result.iloc[0] == pytest.approx(0.2)
        assert result.iloc[1] == pytest.approx(0.25)

    def test_financials_zero_revenue(self):
        """revenue = 0 → profit_margin = NaN (ไม่ใช่ inf)"""
        df = pd.DataFrame({"netIncome": [100.0], "revenue": [0.0],
                           "operatingIncome": [50.0], "fcf": [30.0]})
        specs = GOLD_COMPUTED_COLUMNS["financials"]
        result = specs[0][2](df)
        assert pd.isna(result.iloc[0])

    def test_financials_all_margins(self):
        """ทดสอบทุก margin ใน financials"""
        df = pd.DataFrame({"netIncome": [100.0], "revenue": [1000.0],
                           "operatingIncome": [200.0], "fcf": [150.0]})
        specs = GOLD_COMPUTED_COLUMNS["financials"]
        profit = specs[0][2](df).iloc[0]
        operating = specs[1][2](df).iloc[0]
        fcf = specs[2][2](df).iloc[0]
        assert profit == pytest.approx(0.1)
        assert operating == pytest.approx(0.2)
        assert fcf == pytest.approx(0.15)

    def test_dividend_high_yield(self):
        """high_yield = dividendYield >= 4%"""
        df = pd.DataFrame({"dividendYield": [2.0, 4.0, 6.0, None]})
        spec = GOLD_COMPUTED_COLUMNS["dividend"][0]
        result = spec[2](df)
        assert result.iloc[0] == False
        assert result.iloc[1] == True
        assert result.iloc[2] == True

    def test_valuation_value_score(self):
        """value_score = average percentile rank of PE+PS+PB"""
        df = pd.DataFrame({
            "peForward": [10.0, 20.0, 30.0],
            "psRatio": [1.0, 2.0, 3.0],
            "pbRatio": [0.5, 1.0, 1.5],
        })
        spec = GOLD_COMPUTED_COLUMNS["valuation"][0]
        result = spec[2](df)
        # ตัวแรกมีค่าต่ำสุดทุก metric → score ต่ำสุด (cheapest)
        assert result.iloc[0] < result.iloc[1] < result.iloc[2]

    def test_valuation_with_nan(self):
        """value_score handles NaN gracefully"""
        df = pd.DataFrame({
            "peForward": [10.0, None, 30.0],
            "psRatio": [1.0, 2.0, None],
            "pbRatio": [0.5, 1.0, 1.5],
        })
        spec = GOLD_COMPUTED_COLUMNS["valuation"][0]
        result = spec[2](df)
        # ไม่ควร crash และควรได้ผลลัพธ์ (NaN values ถูก skip ใน rank)
        assert len(result) == 3


# =========================================================
# DQ_KEY_COLUMNS — ตรวจสอบ config ไม่ผิดพลาด
# =========================================================

class TestDQKeyColumnsConfig:
    def test_known_data_gaps_are_none(self):
        """th_analysis และ th_valuation ควรเป็น None (known data gap)"""
        assert DQ_KEY_COLUMNS["analysis"]["th"] is None
        assert DQ_KEY_COLUMNS["valuation"]["th"] is None

    def test_us_categories_have_columns(self):
        """US categories ทุกตัวควรมี list ของ columns"""
        for cat in ["dividend", "general", "financials", "analysis", "valuation"]:
            cols = DQ_KEY_COLUMNS[cat]["us"]
            assert isinstance(cols, list), f"{cat}/us should be a list"
            assert len(cols) > 0, f"{cat}/us should have at least 1 column"
