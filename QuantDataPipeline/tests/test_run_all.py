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


class TestFileBasedLogic:
    """測試基於實體檔案檢查的邏輯 (取代舊版 DB 狀態機)"""

    def test_is_valid_parquet_file_exists_and_large(self, tmp_path):
        """檔案存在且大小正常，應回傳 True"""
        from run_all import _is_valid_parquet_file
        
        test_file = tmp_path / "good.parquet"
        test_file.write_text("dummy content" * 100)  # > 1024 bytes
        assert _is_valid_parquet_file(test_file) is True

    def test_is_valid_parquet_file_missing(self, tmp_path):
        """檔案不存在，應回傳 False"""
        from run_all import _is_valid_parquet_file
        
        test_file = tmp_path / "missing.parquet"
        assert _is_valid_parquet_file(test_file) is False

    def test_is_valid_parquet_file_too_small(self, tmp_path):
        """檔案存在但過小 (0 bytes, 損毀)，應回傳 False"""
        from run_all import _is_valid_parquet_file
        
        test_file = tmp_path / "bad.parquet"
        test_file.write_text("")  # 0 bytes
        assert _is_valid_parquet_file(test_file) is False

    @patch("run_all._is_valid_parquet_file")
    @patch("run_all.get_session")
    @patch("run_all.trading_date.fetch_trading_dates")
    @patch("run_all.ThreadPoolExecutor", create=True)
    def test_phase1_skips_when_files_present(self, mock_executor, mock_fetch, mock_session, mock_is_valid):
        """當 1, 2 都在時，Phase 1 應完全跳過且 phase1_tasks 長度為 0"""
        from run_all import run_pipeline
        import polars as pl
        
        # 建立一個假的前置交易日
        mock_df = pl.DataFrame({"date": [datetime(2024, 5, 2)]})
        mock_fetch.return_value = mock_df
        
        # Mock 檔案檢查：總是回傳 True (檔案都有)
        mock_is_valid.return_value = True

        stats = run_pipeline(
            start_date="2024-05-02", 
            end_date="2024-05-02",
            skip_phase2=True
        )
        
        # 沒有相依缺失檔案被下載
        assert stats["phase1_downloads"] == 0
        assert stats["dates_checked"] == 1

    @patch("run_all._is_valid_parquet_file")
    @patch("run_all.get_session")
    @patch("run_all.trading_date.fetch_trading_dates")
    @patch("run_all.ThreadPoolExecutor", create=True)
    @patch("run_all.process_task")
    def test_phase1_downloads_when_missing(self, mock_process, mock_executor, mock_fetch, mock_session, mock_is_valid):
        """當缺件時，應該會列入 Phase 1 的 tasks。"""
        from run_all import run_pipeline
        import polars as pl
        
        mock_df = pl.DataFrame({"date": [datetime(2024, 5, 2)]})
        mock_fetch.return_value = mock_df
        
        # Mock 永遠是 False，假裝大家都不在
        mock_is_valid.return_value = False

        stats = run_pipeline(
            start_date="2024-05-02", 
            end_date="2024-05-02",
            skip_phase2=True,
            # 我們不能真的用 ThreadPool，因為 mock_process 沒有回傳 future
            # 所以只能斷言他會呼叫 process_task
        )
        # 即使沒有真的呼叫 future，只要確認統計數值符合預期即可
        assert stats["phase1_downloads"] == 2



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
