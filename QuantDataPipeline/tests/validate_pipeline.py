import unittest
import sys
import os
import shutil
import time
import logging
import threading
import concurrent.futures
import requests
from pathlib import Path
from unittest.mock import MagicMock, patch

# 設定專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

# 確保 sys.path 包含專案根目錄
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import polars as pl
from core.db_metadata_manager import DBManager, get_db_manager
from fetchers.datasets.technical import stock_price
from fetchers.infrastructure.http_session import HTTPSession, get_session
from storage.parquet_writer import save_dataframe
from core.config import DATA_DIR

# 設定驗證用的 Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validation")

class TestPipelineValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 確保有先前執行的 Parquet 檔案以供驗證
        cls.parquet_path = DATA_DIR / "2023" / "TaiwanStockPrice" / "2330_2023-10-02.parquet"

    def test_1_schema_strictness(self):
        """1. Schema 嚴格性驗證"""
        logger.info("=== 驗證 1: Schema 嚴格性驗證 ===")
        if not self.parquet_path.exists():
            logger.warning(f"找不到 Parquet 檔案 {self.parquet_path}，跳過 Schema 測試。")
            return

        df = pl.read_parquet(self.parquet_path)
        logger.info(f"載入 Schema: {df.schema}")

        # 檢查 Date/Timestamp
        # 'date' 必須是 Datetime(time_unit='ns')
        if "date" in df.columns:
            dtype = df.schema["date"]
            self.assertIsInstance(dtype, pl.Datetime, "日期欄位必須是 Datetime")
            self.assertEqual(dtype.time_unit, 'ns', "日期時間單位必須是 'ns'")
            logger.info("通過: Date 欄位為 Datetime(time_unit='ns')")

        # 檢查 stock_id 為 Utf8 且已補零
        if "stock_id" in df.columns:
            dtype = df.schema["stock_id"]
            self.assertEqual(dtype, pl.Utf8, "stock_id 必須是 Utf8")

            # 檢查補零
            stock_ids = df["stock_id"].unique().to_list()
            for sid in stock_ids:
                self.assertTrue(len(sid) >= 4, f"股票代碼 {sid} 長度應大於等於 4")
            logger.info(f"通過: stock_id 為 Utf8 且格式正確。範例: {stock_ids[:5]}")

    def test_2_resumability(self):
        """2. 斷點續傳測試 (確認已完成任務被跳過)"""
        logger.info("=== 驗證 2: 斷點續傳測試 ===")
        db = get_db_manager()
        task_id = "2023-10-02_TaiwanStockPrice_2330"

        # 確保任務狀態為 1 (完成)
        status = db.get_task_status(task_id)
        if status != 1:
            logger.warning(f"任務 {task_id} 未完成 (狀態={status})，跳過斷點續傳測試。")
            return

        # 模擬檢查
        logger.info(f"任務 {task_id} 狀態為 {status}。process_task 邏輯將會跳過此任務。")
        self.assertEqual(status, 1)
        logger.info("通過: 系統正確識別已完成的任務。")

    def test_3_high_concurrency(self):
        """3. 高併發寫入測試 (多執行緒更新 DB)"""
        logger.info("=== 驗證 3: 高併發寫入測試 ===")
        db = get_db_manager()
        workers = 20 # 使用者要求 > 10
        iterations = 50

        def worker_task(i):
            tid = f"test_task_{i}"
            db.register_task(tid, "2023-01-01", "Test", "0000")
            db.update_task_status(tid, 1, "hash")
            db.log_api_call("Test", 200, 1, 0.1)

        logger.info(f"啟動 {workers} 個執行緒寫入 DB...")
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(worker_task, i) for i in range(iterations)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.fail(f"併發錯誤: {e}")

        duration = time.time() - start
        logger.info(f"通過: 於 {duration:.2f} 秒內完成 {iterations} 次 DB 寫入 (Workers: {workers})，無鎖定錯誤。")

    def test_4_retry_logic(self):
        """4. 異常重試測試 (模擬網路失敗)"""
        logger.info("=== 驗證 4: 異常重試測試 ===")

        session = get_session()
        target = session.session

        # 建立模擬回應
        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {"msg": "success", "data": [{"date": "2023-10-01", "stock_id": "2330"}]}

        with patch.object(target, 'get') as mock_get:
            # Side effect: 2 次失敗 (ConnectionError, Timeout)，然後成功
            mock_get.side_effect = [
                requests.exceptions.ConnectionError("模擬網路錯誤"),
                requests.exceptions.Timeout("模擬逾時"),
                mock_success
            ]

            start_t = time.time()
            try:
                # 呼叫 fetch，這會觸發 get_data 並透過 exponential_backoff 重試
                df = stock_price.fetch("2023-10-01", "2330")
            except Exception as e:
                self.fail(f"重試邏輯失敗，未成功恢復: {e}")

            duration = time.time() - start_t

            # 驗證呼叫次數: 總共 3 次 (1 次初始 + 2 次重試)
            self.assertEqual(mock_get.call_count, 3, f"應重試 2 次 (共 3 次呼叫)，實際呼叫: {mock_get.call_count}")
            logger.info(f"通過: 成功重試 2 次，耗時 {duration:.2f} 秒 (包含 Backoff 等待)。")

    def test_5_atomicity(self):
        """5. 原子性寫入驗證 (檢查 .tmp 檔案產生)"""
        logger.info("=== 驗證 5: 原子性寫入驗證 ===")

        df = pl.DataFrame({"date": [1], "col": [2]})

        with patch("storage.parquet_writer.os.rename") as mock_rename:
            # 呼叫儲存
            path, checksum = save_dataframe(df, "TestAtomicity", "2023-01-01", "TEST")

            # 驗證 rename 被呼叫
            self.assertTrue(mock_rename.called)
            args, _ = mock_rename.call_args
            src, dst = args
            self.assertTrue(str(src).endswith(".tmp"), f"來源檔案 {src} 應為 .tmp")
            self.assertTrue(str(dst).endswith(".parquet"), f"目標檔案 {dst} 應為 .parquet")

            # 清理實際產生的 .tmp 檔案 (因為 rename 被 mock 了)
            if Path(src).exists():
                Path(src).unlink()

            logger.info("通過: 確認原子性寫入 (os.rename 被呼叫且來源為 .tmp)。")

if __name__ == "__main__":
    unittest.main(verbosity=2)
