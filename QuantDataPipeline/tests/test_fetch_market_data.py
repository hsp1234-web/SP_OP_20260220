"""
V4.2 Phase 1 下載管線測試 (test_fetch_market_data.py)
使用 Mock API 回應，不呼叫任何真實 FinMind API。
"""
import gzip
import json
import pytest
import sys
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.sync_tracker import SyncTracker
import fetch_market_data as fm


# ========== Fixtures ==========

@pytest.fixture(autouse=True)
def reset_all():
    """每個測試前重置全域狀態"""
    SyncTracker._reset_instance()
    fm.STOP_FLAG.clear()
    fm._last_request_time = 0.0
    yield
    SyncTracker._reset_instance()
    fm.STOP_FLAG.clear()


@pytest.fixture
def tmp_tracker(tmp_path):
    """臨時 tracker"""
    return SyncTracker(db_path=str(tmp_path / "test.db"))


@pytest.fixture
def tmp_data_dir(tmp_path, monkeypatch):
    """臨時資料目錄"""
    temp_raw = tmp_path / "data_v4" / "temp_raw_data"
    temp_raw.mkdir(parents=True)
    monkeypatch.setattr(fm, "TEMP_RAW_DIR", temp_raw)
    monkeypatch.setattr(fm, "DATA_V4_DIR", tmp_path / "data_v4")
    return temp_raw


@pytest.fixture
def mock_trading_days():
    """模擬交易日曆 (5 個交易日)"""
    return {
        "2024-02-19",
        "2024-02-20",
        "2024-02-21",
        "2024-02-22",
        "2024-02-23",
    }


def make_mock_api_response(dataset, date_str, records=5):
    """產生模擬的 FinMind API JSON 回應"""
    data = [
        {"date": date_str, "stock_id": f"{2330+i}", "close": 100.0 + i}
        for i in range(records)
    ]
    return {"msg": "success", "data": data, "status": 200}


# ========== 測試類別 ==========

class TestSaveJsonGz:
    """JSON 壓縮存檔測試"""

    def test_creates_gz_file(self, tmp_data_dir):
        """應建立 .json.gz 檔案"""
        data = {"msg": "success", "data": [{"price": 100}]}
        path = fm.save_json_gz(data, "TestDS", "2024-02-26")
        assert path.exists()
        assert path.suffix == ".gz"

    def test_correct_filename(self, tmp_data_dir):
        """檔名應為 {Dataset}_{YYYYMMDD}.json.gz"""
        data = {"msg": "success", "data": [{"price": 100}]}
        path = fm.save_json_gz(data, "TaiwanStockPriceAdj", "2024-02-26")
        assert path.name == "TaiwanStockPriceAdj_20240226.json.gz"

    def test_creates_subdirectory(self, tmp_data_dir):
        """應自動建立以資料集名稱命名的子目錄"""
        data = {"msg": "success", "data": [{"price": 100}]}
        fm.save_json_gz(data, "MyDataset", "2024-01-01")
        assert (tmp_data_dir / "MyDataset").is_dir()

    def test_gz_readable(self, tmp_data_dir):
        """壓縮檔應可正確解壓讀取"""
        original = {"msg": "success", "data": [{"a": 1}, {"b": 2}]}
        path = fm.save_json_gz(original, "ReadTest", "2024-03-01")
        
        with gzip.open(path, "rt", encoding="utf-8") as f:
            loaded = json.load(f)
        
        assert loaded["msg"] == "success"
        assert len(loaded["data"]) == 2


class TestScanDownloadedDates:
    """掃描已下載日期測試"""

    def test_empty_directory(self, tmp_data_dir):
        """空目錄應回傳空集合"""
        result = fm.scan_downloaded_dates("EmptyDS")
        assert result == set()

    def test_detects_existing_files(self, tmp_data_dir):
        """應正確偵測已下載的日期"""
        ds_dir = tmp_data_dir / "TestDS"
        ds_dir.mkdir()
        (ds_dir / "TestDS_20240226.json.gz").touch()
        (ds_dir / "TestDS_20240227.json.gz").touch()
        
        result = fm.scan_downloaded_dates("TestDS")
        assert result == {"2024-02-26", "2024-02-27"}

    def test_ignores_non_matching_files(self, tmp_data_dir):
        """應忽略不符合命名規則的檔案"""
        ds_dir = tmp_data_dir / "TestDS"
        ds_dir.mkdir()
        (ds_dir / "random_file.txt").touch()
        (ds_dir / "TestDS_20240226.json.gz").touch()

        result = fm.scan_downloaded_dates("TestDS")
        assert result == {"2024-02-26"}


class TestFetchFromApi:
    """API 呼叫測試 (全部 Mock)"""

    @patch("fetch_market_data.requests.Session")
    def test_success_response(self, MockSession, tmp_data_dir):
        """成功回應應回傳 dict"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = make_mock_api_response("TestDS", "2024-02-26")

        session = MagicMock()
        session.get.return_value = mock_resp

        result = fm.fetch_from_api("TestDS", "2024-02-26", session=session)
        assert result is not None
        assert result["msg"] == "success"
        assert len(result["data"]) == 5

    @patch("fetch_market_data.requests.Session")
    def test_rate_limit_sets_stop_flag(self, MockSession, tmp_data_dir):
        """收到 rate limit 應設定 STOP_FLAG"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"msg": "rate limit exceeded", "data": []}

        session = MagicMock()
        session.get.return_value = mock_resp

        result = fm.fetch_from_api("TestDS", "2024-02-26", session=session)
        assert result is None
        assert fm.STOP_FLAG.is_set()

    @patch("fetch_market_data.requests.Session")
    def test_fatal_error_sets_stop_flag(self, MockSession, tmp_data_dir):
        """永久性錯誤應設定 STOP_FLAG"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"msg": "user level is not enough", "data": []}

        session = MagicMock()
        session.get.return_value = mock_resp

        result = fm.fetch_from_api("TestDS", "2024-02-26", session=session)
        assert result is None
        assert fm.STOP_FLAG.is_set()

    def test_stop_flag_short_circuits(self, tmp_data_dir):
        """STOP_FLAG 已設定時應直接回傳 None"""
        fm.STOP_FLAG.set()
        result = fm.fetch_from_api("TestDS", "2024-02-26")
        assert result is None


class TestRunFetchIntegration:
    """整合測試：模擬完整下載流程"""

    @patch("fetch_market_data.fetch_from_api")
    def test_full_flow_with_mock(
        self, mock_api, tmp_data_dir, tmp_tracker, mock_trading_days
    ):
        """完整流程：5 個交易日的模擬下載"""
        # Mock API 回傳成功
        def side_effect(dataset, date_str, data_id="", session=None):
            return make_mock_api_response(dataset, date_str)

        mock_api.side_effect = side_effect

        # 只測 1 個資料集
        total = fm.run_fetch(
            mode="update",
            batch_size=5,
            target_datasets=["TaiwanStockPriceAdj"],
            trading_calendar_override=mock_trading_days,
            tracker_override=tmp_tracker,
        )

        assert total == 5  # 5 個交易日全部成功

    @patch("fetch_market_data.fetch_from_api")
    def test_stop_flag_halts_processing(
        self, mock_api, tmp_data_dir, tmp_tracker, mock_trading_days
    ):
        """STOP_FLAG 觸發後應停止處理"""
        call_count = 0

        def side_effect(dataset, date_str, data_id="", session=None):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                fm.STOP_FLAG.set()
                return None
            return make_mock_api_response(dataset, date_str)

        mock_api.side_effect = side_effect

        total = fm.run_fetch(
            mode="backfill",
            batch_size=5,
            target_datasets=["TaiwanStockPriceAdj"],
            trading_calendar_override=mock_trading_days,
            tracker_override=tmp_tracker,
        )

        assert total < 5  # 不應跑完全部

    @patch("fetch_market_data.fetch_from_api")
    def test_skips_already_downloaded(
        self, mock_api, tmp_data_dir, tmp_tracker, mock_trading_days
    ):
        """應跳過已下載的日期"""
        # 預先建立 3 天的假檔案
        ds_dir = tmp_data_dir / "TaiwanStockPriceAdj"
        ds_dir.mkdir(parents=True)
        for d in ["20240219", "20240220", "20240221"]:
            (ds_dir / f"TaiwanStockPriceAdj_{d}.json.gz").touch()

        mock_api.side_effect = lambda ds, dt, **kw: make_mock_api_response(ds, dt)

        total = fm.run_fetch(
            mode="update",
            batch_size=10,
            target_datasets=["TaiwanStockPriceAdj"],
            trading_calendar_override=mock_trading_days,
            tracker_override=tmp_tracker,
        )

        # 只應下載 2 天 (22, 23)
        assert total == 2


class TestCheckApiQuota:
    """API 額度查詢測試 (Mock)"""

    def test_quota_success(self):
        """成功回傳額度資訊"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "user_count": 500,
            "api_request_limit": 1600,
        }
        session = MagicMock()
        session.get.return_value = mock_resp

        # 暫時設定 Token
        original_token = fm.FINMIND_API_TOKEN
        fm.FINMIND_API_TOKEN = "test_token"
        try:
            result = fm.check_api_quota(session=session)
        finally:
            fm.FINMIND_API_TOKEN = original_token

        assert result is not None
        assert result["used"] == 500
        assert result["limit"] == 1600
        assert result["remaining"] == 1100

    def test_quota_no_token(self):
        """無 Token 時應回傳 None"""
        original_token = fm.FINMIND_API_TOKEN
        fm.FINMIND_API_TOKEN = ""
        try:
            result = fm.check_api_quota()
        finally:
            fm.FINMIND_API_TOKEN = original_token

        assert result is None

    def test_quota_network_error(self):
        """網路錯誤時應回傳 None"""
        session = MagicMock()
        session.get.side_effect = Exception("Connection timeout")

        original_token = fm.FINMIND_API_TOKEN
        fm.FINMIND_API_TOKEN = "test_token"
        try:
            result = fm.check_api_quota(session=session)
        finally:
            fm.FINMIND_API_TOKEN = original_token

        assert result is None

    @patch("fetch_market_data.check_api_quota")
    @patch("fetch_market_data.fetch_from_api")
    def test_run_fetch_stops_when_quota_zero(
        self, mock_api, mock_quota, tmp_data_dir, tmp_tracker, mock_trading_days
    ):
        """額度為 0 時 run_fetch 應直接回傳 0"""
        mock_quota.return_value = {"used": 1600, "limit": 1600, "remaining": 0}

        total = fm.run_fetch(
            mode="update",
            batch_size=5,
            target_datasets=["TaiwanStockPriceAdj"],
            trading_calendar_override=mock_trading_days,
            tracker_override=tmp_tracker,
        )

        assert total == 0
        mock_api.assert_not_called()  # 不應發出任何 API 請求
