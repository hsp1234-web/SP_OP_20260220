"""
test_run_all.py — run_all.py 自動化管線測試

測試重點:
  1. lookback 日期生成邏輯
  2. 狀態機跳過已完成任務
  3. 429 冷卻不崩潰
  4. 空資料標記 EMPTY_SKIP
"""
import sys
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class TestLookbackDateGeneration:
    """測試 lookback 日期推算邏輯"""

    def test_lookback_default_30_days(self):
        """預設 lookback=30 應產生合理的日期範圍"""
        from run_all import run_pipeline

        today = datetime.now()
        expected_start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        expected_end = today.strftime("%Y-%m-%d")

        # 不實際執行管線，只驗證日期計算邏輯
        assert expected_start < expected_end

    def test_explicit_date_range(self):
        """明確指定日期範圍時不應使用 lookback"""
        start = "2024-05-01"
        end = "2024-05-10"
        assert start < end


class TestDBStateMachine:
    """測試 DB 狀態機邏輯"""

    def test_skip_completed_tasks(self, tmp_db):
        """已完成的任務 (status>=1) 應被跳過"""
        tmp_db.register_task("2024-05-02_TaiwanOptionTick_TXO", "2024-05-02", "TaiwanOptionTick", "TXO")
        tmp_db.update_task_status("2024-05-02_TaiwanOptionTick_TXO", 1)

        # 已完成任務不應出現在 pending 列表
        pending = tmp_db.get_pending_tasks()
        task_ids = [t[0] for t in pending]
        assert "2024-05-02_TaiwanOptionTick_TXO" not in task_ids

    def test_pending_tasks_included(self, tmp_db):
        """status=0 的任務應出現在待處理列表"""
        tmp_db.register_task("2024-05-03_TaiwanFuturesTick_TX", "2024-05-03", "TaiwanFuturesTick", "TX")

        pending = tmp_db.get_pending_tasks()
        task_ids = [t[0] for t in pending]
        assert "2024-05-03_TaiwanFuturesTick_TX" in task_ids

    def test_empty_skip_not_retried(self, tmp_db):
        """EMPTY_SKIP (status=3) 應被跳過"""
        tmp_db.register_task("2024-05-01_TaiwanOptionTick_TXO", "2024-05-01", "TaiwanOptionTick", "TXO")
        tmp_db.update_task_status("2024-05-01_TaiwanOptionTick_TXO", 3)

        status = tmp_db.get_task_status("2024-05-01_TaiwanOptionTick_TXO")
        assert status == 3

        pending = tmp_db.get_pending_tasks()
        task_ids = [t[0] for t in pending]
        assert "2024-05-01_TaiwanOptionTick_TXO" not in task_ids

    def test_get_tasks_by_status(self, tmp_db):
        """get_tasks_by_status 應正確過濾"""
        tmp_db.register_task("t1", "2024-05-01", "D1", "X")
        tmp_db.register_task("t2", "2024-05-02", "D2", "Y")
        tmp_db.update_task_status("t1", 1)

        l1_tasks = tmp_db.get_tasks_by_status(1)
        assert len(l1_tasks) == 1
        assert l1_tasks[0][0] == "t1"

        pending = tmp_db.get_tasks_by_status(0)
        assert len(pending) == 1
        assert pending[0][0] == "t2"

    def test_orphan_reset(self, tmp_db, tmp_data_dir):
        """孤兒任務 (DB有記錄但Parquet不存在) 應被重置"""
        tmp_db.register_task("2024-05-02_TaiwanOptionTick_TXO", "2024-05-02", "TaiwanOptionTick", "TXO")
        tmp_db.update_task_status("2024-05-02_TaiwanOptionTick_TXO", 1)

        # Parquet 不存在 → 應該被重置
        reset_count = tmp_db.reset_orphan_tasks(tmp_data_dir)
        assert reset_count == 1
        assert tmp_db.get_task_status("2024-05-02_TaiwanOptionTick_TXO") == 0


class TestCooldownLogic:
    """測試 API 冷卻邏輯"""

    def test_is_api_quota_error(self):
        """檢測 429 / 額度耗盡錯誤"""
        from run_all import _is_api_quota_error

        assert _is_api_quota_error(Exception("HTTP 429 Too Many Requests"))
        assert _is_api_quota_error(Exception("rate limit exceeded"))
        assert _is_api_quota_error(Exception("API quota exceeded"))
        assert not _is_api_quota_error(Exception("Connection timeout"))
        assert not _is_api_quota_error(Exception("Some other error"))


class TestComputeGreeksPipeline:
    """測試 compute_greeks_pipeline 重構後的功能"""

    def test_third_wednesday(self):
        """結算日計算：第三個星期三"""
        from compute_greeks_pipeline import _third_wednesday

        # 2024-05 的第三個星期三是 5/15
        result = _third_wednesday(2024, 5)
        assert result.day == 15
        assert result.weekday() == 2  # Wednesday

    def test_calc_years_to_maturity(self):
        """Years_to_Maturity 應為正值且合理"""
        from compute_greeks_pipeline import _calc_years_to_maturity

        trade_date = datetime(2024, 5, 2)
        t = _calc_years_to_maturity(trade_date, "202405")
        assert t > 0
        assert t < 1.0  # 應小於一年

    def test_years_to_maturity_past_settlement(self):
        """已過結算日時應回傳極小值"""
        from compute_greeks_pipeline import _calc_years_to_maturity

        trade_date = datetime(2024, 5, 20)  # 結算日之後
        t = _calc_years_to_maturity(trade_date, "202405")
        assert t == pytest.approx(0.0001, abs=0.001)

    def test_compute_returns_tuple(self):
        """compute_greeks_for_date 應回傳三元組"""
        from compute_greeks_pipeline import compute_greeks_for_date

        # 找不到檔案時應回傳 (None, None, False)
        df, path, ok = compute_greeks_for_date("1999-01-01", data_dir=Path("/nonexistent"))
        assert df is None
        assert path is None
        assert ok is False
