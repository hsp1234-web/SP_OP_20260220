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

# 匯入資料爬取器
from fetchers.datasets.technical import stock_price_tick, stock_price, trading_date
from fetchers.datasets.derivative import option_tick
from fetchers.datasets.chip import large_traders
from fetchers.infrastructure.http_session import get_session
from storage.parquet_writer import save_dataframe

logger = setup_logger("pipeline.main", LOG_FILE)

def process_task(task_id: str, date: str, dataset_name: str, data_id: str):
    """
    執行單一任務：爬取 -> 儲存 -> 更新狀態
    """
    # 取得 DBManager 單例 (內部使用 ThreadLocal 管理連線)
    db = get_db_manager()

    # 再次確認狀態以避免競態條件
    status = db.get_task_status(task_id)
    if status is not None and status >= 1:
        logger.info(f"任務 {task_id} 已完成，跳過。")
        return

    logger.info(f"開始執行任務 {task_id} ({dataset_name} {date} {data_id})")

    try:
        # 1. 資料爬取 (Fetch)
        # 註：爬取器內部使用 get_session() 取得單例 Session
        df = None
        if dataset_name == "TaiwanStockPriceTick":
            df = stock_price_tick.fetch(date, data_id)
        elif dataset_name == "TaiwanStockPrice":
            df = stock_price.fetch(date, data_id)
        elif dataset_name == "TaiwanOptionTick":
            df = option_tick.fetch(date, data_id)
        elif dataset_name == "TaiwanOptionOpenInterestLargeTraders":
            df = large_traders.fetch(date, data_id)
        else:
            logger.error(f"未知的資料集名稱: {dataset_name} (任務 {task_id})")
            return

        # 2. 資料儲存 (Store L4)
        if df is not None and not df.is_empty():
            file_path, checksum = save_dataframe(df, dataset_name, date, data_id)

            # 3. 更新狀態 (Update Status)
            if checksum:
                db.update_task_status(task_id, 1, checksum) # 1 = L1 完成
                logger.info(f"任務 {task_id} 執行成功。已儲存至 {file_path}")
            else:
                logger.warning(f"任務 {task_id} 未產生檔案 (可能為空資料)。標記為 EMPTY_SKIP (3)。")
                db.update_task_status(task_id, 3)
        else:
             logger.warning(f"任務 {task_id} 抓取到空資料。標記為 EMPTY_SKIP (3)。")
             db.update_task_status(task_id, 3)

    except Exception as e:
        logger.error(f"任務 {task_id} 失敗: {e}", exc_info=True)
        # 保持狀態為 0 (Pending) 以便後續重試

def seed_tasks_from_dates(db, dates_df, target_datasets: List[Tuple[str, str]]):
    """
    根據日期與目標資料集註冊任務。
    """
    if dates_df.is_empty():
        logger.warning("未提供日期以建立任務。")
        return

    # dates_df 的 'date' 欄位為 Datetime
    # 需轉換為字串 'YYYY-MM-DD'
    dates = dates_df["date"].dt.strftime("%Y-%m-%d").to_list()

    count = 0
    for date_str in dates:
        for dset, did in target_datasets:
            task_id = f"{date_str}_{dset}_{did}"
            db.register_task(task_id, date_str, dset, did)
            count += 1

    logger.info(f"已針對 {len(dates)} 個日期建立了 {count} 個任務。")

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

    # 2. 建立任務 (Seed Tasks) - 冪等操作
    # 定義要抓取的目標資料集
    # 範例 (POC):
    # - 台積電 (2330) 日股價
    # - 台指期 (TXO) 大額交易人
    target_datasets = [
        ("TaiwanStockPrice", "2330"),
        ("TaiwanOptionOpenInterestLargeTraders", "TXO"),
        # 可在此擴充
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
