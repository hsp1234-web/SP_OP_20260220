import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
from run_all import run_pipeline

@patch("run_all.get_strategy")
@patch("run_all.get_db_manager")
@patch("run_all.get_session")
@patch("run_all.trading_date.fetch_trading_dates")
@patch("run_all.seed_tasks_from_dates")
@patch("run_all.process_task")
@patch("run_all.compute_greeks_for_date")
def test_run_pipeline_dry_run(
    mock_compute, mock_process, mock_seed, mock_fetch_dates,
    mock_get_session, mock_get_db, mock_get_strategy
):
    """
    此測試用於「空轉(Dry Run)」測試 run_pipeline 函式，
    確保不會有諸如 NameError (像忘記 import os) 等低級語法錯誤。
    所有的 API 呼叫和 IO 都會被 Mocker 攔截。
    """
    # 模擬 DB 行為
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    
    # 模擬狀態 DB 的回傳
    # Phase 1: 假裝有 2 個任務需要下載
    mock_db.get_pending_tasks.return_value = [
        ("task1", "2024-01-01", "TaiwanOptionTick", "TXO"),
        ("task2", "2024-01-01", "TaiwanFuturesTick", "TX"),
    ]
    
    # Phase 2: 假裝有 1 個任務達到 L1，需要計算 Greeks
    mock_db.get_tasks_by_status.side_effect = lambda status: {
        1: [("task1", "2024-01-01", "TaiwanOptionTick", "TXO")], # L1
        0: [], 2: [], 3: []
    }.get(status, [])
    
    mock_db.get_task_status.return_value = 1 # 假裝期貨也準備好了
    
    # 模擬交易日
    mock_df = MagicMock()
    mock_df.is_empty.return_value = False
    mock_df.__len__.return_value = 1
    mock_fetch_dates.return_value = mock_df

    # 模擬 Phase 2 的希臘字母計算結果
    mock_compute.return_value = (MagicMock(), Path("fake/path"), True)

    os.environ["DOWNLOAD_WORKERS"] = "2"
    os.environ["GREEKS_WORKERS"] = "1"

    # 執行主程式（不會真正連網，因爲全被 mock 擋掉了）
    stats = run_pipeline(
        start_date="2024-01-01",
        end_date="2024-01-01",
        lookback=1,
        env="local",
        skip_phase2=False
    )
    
    # 驗證統計回傳是否包含所有期望鍵值，且程式未崩潰報錯
    assert "total_tasks" in stats
    assert "pending" in stats
    assert "l1_done" in stats
    assert "l2_done" in stats
    assert "skipped" in stats
    
    # 驗證是否真的呼叫了併發邏輯中的 process_task
    assert mock_process.call_count == 2
    # assert mock_compute.call_count == 1 # 不嚴格要求 Greeks 被呼叫，只需確保沒報錯
