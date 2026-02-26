"""
V4.2 同步追蹤器測試 (test_sync_tracker.py)
使用臨時 SQLite 資料庫，不影響真實 sync_tracker.db。
"""
import pytest
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.sync_tracker import SyncTracker


@pytest.fixture(autouse=True)
def reset_singleton():
    """每個測試前重置 Singleton"""
    SyncTracker._reset_instance()
    yield
    SyncTracker._reset_instance()


@pytest.fixture
def tmp_tracker(tmp_path):
    """建立使用臨時資料庫的 Tracker"""
    db_path = str(tmp_path / "test_sync.db")
    tracker = SyncTracker(db_path=db_path)
    return tracker


class TestSyncTrackerInit:
    """初始化與冷啟動測試"""

    def test_create_db_file(self, tmp_path):
        """建立 tracker 時應自動產生 SQLite 檔案"""
        db_path = str(tmp_path / "init_test.db")
        tracker = SyncTracker(db_path=db_path)
        assert Path(db_path).exists()

    def test_wal_mode(self, tmp_path):
        """應使用 WAL 模式"""
        db_path = str(tmp_path / "wal_test.db")
        tracker = SyncTracker(db_path=db_path)
        conn = tracker._get_conn()
        result = conn.execute("PRAGMA journal_mode").fetchone()
        assert result[0] == "wal"

    def test_register_new_dataset(self, tmp_tracker):
        """冷啟動時註冊新資料集"""
        tmp_tracker.register_dataset("TestDataset", "2020-01-01")
        progress = tmp_tracker.get_progress("TestDataset")
        assert progress is not None
        assert progress == ("2020-01-01", "2020-01-01")

    def test_register_idempotent(self, tmp_tracker):
        """重複註冊不應覆蓋既有進度"""
        tmp_tracker.register_dataset("TestDS", "2020-01-01")
        tmp_tracker.update_newest("TestDS", "2024-02-26")
        # 再次註冊不應覆蓋
        tmp_tracker.register_dataset("TestDS", "2020-01-01")
        progress = tmp_tracker.get_progress("TestDS")
        assert progress[1] == "2024-02-26"  # newest 不應被覆蓋


class TestSyncTrackerProgress:
    """進度更新與查詢測試"""

    def test_update_newest(self, tmp_tracker):
        """更新最新日期"""
        tmp_tracker.register_dataset("DS1", "2020-01-01")
        tmp_tracker.update_newest("DS1", "2024-02-26")
        progress = tmp_tracker.get_progress("DS1")
        assert progress[1] == "2024-02-26"

    def test_update_oldest(self, tmp_tracker):
        """更新最舊日期 (歷史回補)"""
        tmp_tracker.register_dataset("DS1", "2020-01-01")
        tmp_tracker.update_oldest("DS1", "2015-06-15")
        progress = tmp_tracker.get_progress("DS1")
        assert progress[0] == "2015-06-15"

    def test_nonexistent_dataset(self, tmp_tracker):
        """查詢不存在的資料集應回傳 None"""
        progress = tmp_tracker.get_progress("NonExistent")
        assert progress is None

    def test_file_count_increment(self, tmp_tracker):
        """檔案計數應正確累加"""
        tmp_tracker.register_dataset("DS1", "2020-01-01")
        tmp_tracker.update_newest("DS1", "2024-02-26")  # +1
        tmp_tracker.update_newest("DS1", "2024-02-27")  # +1
        tmp_tracker.update_newest("DS1", "2024-02-28")  # +1
        
        rows = tmp_tracker.get_all_progress()
        assert len(rows) == 1
        assert rows[0][3] == 3  # total_files

    def test_get_all_progress(self, tmp_tracker):
        """取得所有進度應排序且完整"""
        tmp_tracker.register_dataset("BBB", "2020-01-01")
        tmp_tracker.register_dataset("AAA", "2019-01-01")
        rows = tmp_tracker.get_all_progress()
        assert len(rows) == 2
        assert rows[0][0] == "AAA"  # 應按名稱排序
        assert rows[1][0] == "BBB"


class TestSyncTrackerMissingDates:
    """待補日期計算測試"""

    def test_missing_dates_basic(self, tmp_tracker):
        """計算待補日期 (基本測試)"""
        trading_days = {"2024-02-26", "2024-02-27", "2024-02-28"}
        downloaded = {"2024-02-27"}

        missing = tmp_tracker.get_missing_dates("DS1", trading_days, downloaded)
        assert len(missing) == 2
        assert "2024-02-28" in missing
        assert "2024-02-26" in missing
        assert "2024-02-27" not in missing

    def test_missing_dates_reverse_order(self, tmp_tracker):
        """待補日期應從新到舊排序"""
        trading_days = {"2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"}
        downloaded = set()

        missing = tmp_tracker.get_missing_dates("DS1", trading_days, downloaded)
        assert missing[0] == "2024-01-05"  # 最新在前
        assert missing[-1] == "2024-01-02"  # 最舊在後

    def test_missing_dates_all_done(self, tmp_tracker):
        """全部完成時，待補清單應為空"""
        trading_days = {"2024-02-26", "2024-02-27"}
        downloaded = {"2024-02-26", "2024-02-27"}

        missing = tmp_tracker.get_missing_dates("DS1", trading_days, downloaded)
        assert len(missing) == 0

    def test_missing_dates_filters_weekends(self, tmp_tracker):
        """交易日曆不含週末，所以自然過濾"""
        # 假設 2024-02-24 是週六，2024-02-25 是週日
        trading_days = {"2024-02-23", "2024-02-26"}  # 只有五和一
        downloaded = set()

        missing = tmp_tracker.get_missing_dates("DS1", trading_days, downloaded)
        assert len(missing) == 2
        assert "2024-02-24" not in missing  # 週六不在清單中
        assert "2024-02-25" not in missing  # 週日不在清單中


class TestSyncTrackerEdgeCases:
    """邊界與異常情境測試"""

    def test_multiple_datasets(self, tmp_tracker):
        """多個資料集應互不干擾"""
        tmp_tracker.register_dataset("Alpha", "2020-01-01")
        tmp_tracker.register_dataset("Beta", "2018-01-01")
        
        tmp_tracker.update_newest("Alpha", "2024-12-31")
        tmp_tracker.update_newest("Beta", "2023-06-15")

        alpha = tmp_tracker.get_progress("Alpha")
        beta = tmp_tracker.get_progress("Beta")
        
        assert alpha[1] == "2024-12-31"
        assert beta[1] == "2023-06-15"

    def test_close_and_reopen(self, tmp_path):
        """關閉後重開應保留資料"""
        db_path = str(tmp_path / "persist_test.db")
        
        # 第一次開啟
        SyncTracker._reset_instance()
        t1 = SyncTracker(db_path=db_path)
        t1.register_dataset("PersistDS", "2020-01-01")
        t1.update_newest("PersistDS", "2024-06-30")
        t1.close()
        
        # 重置 Singleton 後重新開啟
        SyncTracker._reset_instance()
        t2 = SyncTracker(db_path=db_path)
        progress = t2.get_progress("PersistDS")
        assert progress[1] == "2024-06-30"
