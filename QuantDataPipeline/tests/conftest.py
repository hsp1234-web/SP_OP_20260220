"""
pytest conftest.py — QuantDataPipeline 測試基礎設施
提供 fixtures: tmp_db, mock_session, sample DataFrames
"""
import sys
import json
import pytest
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

import polars as pl
import numpy as np

# 確保專案根目錄在 path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ─────────────────────────────────────────────
# Fixture: 臨時 DB (繞過 Singleton)
# ─────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    """
    建立臨時 SQLite 資料庫，繞過 DBManager Singleton。
    每個測試函數有獨立的 DB 實例。
    """
    from core.db_metadata_manager import DBManager

    db_path = tmp_path / "test_status.db"

    # 強制重置 Singleton
    old_instance = DBManager._instance
    DBManager._instance = None

    db = DBManager(db_path)

    yield db

    # 恢復 Singleton
    DBManager._instance = old_instance
    DBManager._instance = None  # 清理避免污染


@pytest.fixture
def tmp_data_dir(tmp_path):
    """臨時資料目錄，用於 Parquet 讀寫測試"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


# ─────────────────────────────────────────────
# Fixture: Mock HTTP Session
# ─────────────────────────────────────────────

@pytest.fixture
def mock_session():
    """模擬 HTTP session，攔截所有 API 呼叫"""
    session = MagicMock()
    session.get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"msg": "success", "data": []}
    )
    return session


@pytest.fixture
def mock_finmind_response():
    """模擬 FinMind API 的標準回應格式"""
    def _make_response(data, msg="success", status=200):
        resp = MagicMock()
        resp.status_code = status
        resp.json.return_value = {"msg": msg, "data": data}
        resp.raise_for_status.return_value = None
        return resp
    return _make_response


# ─────────────────────────────────────────────
# Fixture: 預製 DataFrames
# ─────────────────────────────────────────────

@pytest.fixture
def sample_tick_df():
    """
    預製選擇權 tick DataFrame，包含 FinMind 格式的欄位。
    用於 compute_greeks_pipeline 測試。
    """
    np.random.seed(42)
    n = 200
    base_time = datetime(2024, 5, 2, 9, 0, 0)
    times = [base_time + timedelta(seconds=i * 3) for i in range(n)]

    return pl.DataFrame({
        "date": times,
        "option_id": ["TXO" + str(i % 10).zfill(5) for i in range(n)],
        "contract_date": ["202405" for _ in range(n)],
        "PutCall": ["C" if i % 2 == 0 else "P" for i in range(n)],
        "ExercisePrice": [20000.0 + (i % 10) * 100 for i in range(n)],
        "price": np.random.uniform(50, 500, n).tolist(),
        "volume": np.random.randint(1, 50, n).tolist(),
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


@pytest.fixture
def sample_futures_tick_df():
    """預製期貨 tick DataFrame"""
    np.random.seed(42)
    n = 300
    base_time = datetime(2024, 5, 2, 8, 45, 0)
    times = [base_time + timedelta(seconds=i * 2) for i in range(n)]

    return pl.DataFrame({
        "date": times,
        "price": np.random.uniform(19800, 20200, n).tolist(),
        "volume": np.random.randint(1, 100, n).tolist(),
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )


@pytest.fixture
def sample_greeks_df():
    """
    預製含 Greeks 特徵的 DataFrame (已經過 Asof Join + Greeks 計算)。
    用於 timeframe_aggregator 和 market_microstructure 測試。
    """
    np.random.seed(42)
    n = 500
    base_time = datetime(2024, 5, 2, 9, 0, 0)
    times = [base_time + timedelta(seconds=i * 2) for i in range(n)]

    underlying_prices = 20000.0 + np.cumsum(np.random.randn(n) * 5)

    df = pl.DataFrame({
        "date": times,
        "option_id": ["TXO" + str(i % 20).zfill(5) for i in range(n)],
        "contract_date": ["202405" for _ in range(n)],
        "PutCall": ["C" if i % 2 == 0 else "P" for i in range(n)],
        "ExercisePrice": [20000.0 + (i % 20 - 10) * 100 for i in range(n)],
        "price": np.random.uniform(30, 600, n).tolist(),
        "volume": np.random.randint(1, 100, n).tolist(),
        "Underlying_S": underlying_prices.tolist(),
        "Years_to_Maturity": [0.05 for _ in range(n)],
        "IV": np.random.uniform(0.1, 0.5, n).tolist(),
        "Delta": np.random.uniform(-1, 1, n).tolist(),
        "Gamma": np.random.uniform(0, 0.01, n).tolist(),
        "Vega": np.random.uniform(0, 50, n).tolist(),
        "Theta": np.random.uniform(-10, 0, n).tolist(),
        "Vanna": np.random.uniform(-5, 5, n).tolist(),
        "Charm": np.random.uniform(-5, 5, n).tolist(),
    }).with_columns(
        pl.col("date").cast(pl.Datetime("ns"))
    )

    return df
