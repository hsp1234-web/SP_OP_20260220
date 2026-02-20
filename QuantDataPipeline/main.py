import concurrent.futures
import logging
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple

# 確保專案根目錄在 sys.path 中
sys.path.append(str(Path(__file__).parent))

from core.db_metadata_manager import get_db_manager
from core.pipeline_logger import setup_logger
from core.config import LOG_FILE

# 匯入核心調度與爬取器
from fetchers.datasets.technical import trading_date
from fetchers.infrastructure.http_session import get_session
from core.fetch_orchestrator import process_task, seed_tasks_from_dates

logger = setup_logger("pipeline.main", LOG_FILE)

def main():
    parser = argparse.ArgumentParser(description="量化數據中台主程式")
    parser.add_argument("--start_date", type=str, help="開始日期 (YYYY-MM-DD)", default="")
    parser.add_argument("--end_date", type=str, help="結束日期 (YYYY-MM-DD)", default="")
    parser.add_argument("--workers", type=int, help="並行工作數", default=2)
    args = parser.parse_args()

    db = get_db_manager()
    session = get_session()

    # 決定日期範圍
    # 若未指定，預設為最近 3 天
    start_date = args.start_date
    end_date = args.end_date

    if not start_date or not end_date:
        today = datetime.now()
        if not end_date:
            end_date = today.strftime("%Y-%m-%d")
        if not start_date:
            start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
        logger.info(f"未指定日期範圍，使用預設值: {start_date} 至 {end_date}")

    # 1. 抓取交易日 (Fetch Trading Dates)
    logger.info(f"正在抓取交易日範圍: {start_date} 至 {end_date}")
    trading_dates_df = trading_date.fetch_trading_dates(session, start_date, end_date)

    if trading_dates_df.is_empty():
        logger.warning("在此範圍內未找到交易日。程式結束。")
        return

    logger.info(f"共找到 {len(trading_dates_df)} 個交易日。")

    # 定義要抓取的目標資料集 (改為期貨選項核心運算需求)
    target_datasets = [
        ("TaiwanOptionTick", "TXO"),
        ("TaiwanFuturesTick", "TX"),
    ]

    seed_tasks_from_dates(db, trading_dates_df, target_datasets)

    # 3. 取得待處理任務
    pending_tasks = db.get_pending_tasks()
    logger.info(f"發現 {len(pending_tasks)} 個待處理任務。")

    if not pending_tasks:
        logger.info("無待處理任務。")
        return

    # 4. 並行執行 (Execute Concurrently)
    workers = args.workers
    logger.info(f"使用 {workers} 個工作執行緒開始處理...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for task_id, trade_date_str, dataset_name, data_id in pending_tasks:
            future = executor.submit(process_task, task_id, trade_date_str, dataset_name, data_id)
            futures.append(future)

        # 等待完成
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"任務執行發生錯誤: {e}")

if __name__ == "__main__":
    main()
