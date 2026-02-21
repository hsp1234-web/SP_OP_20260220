import logging
from typing import List, Tuple
from core.db_metadata_manager import get_db_manager
from storage.parquet_writer import save_dataframe

from fetchers.datasets.technical import stock_price_tick, stock_price
from fetchers.datasets.derivative import option_tick, futures_tick
from fetchers.datasets.chip import large_traders

logger = logging.getLogger("pipeline.orchestrator")

# 利用字典模式，讓不同 API 的爬取函式可以直接呼叫，不需要寫過多的 if-elif
FETCHERS = {
    "TaiwanStockPriceTick": stock_price_tick.fetch,
    "TaiwanStockPrice": stock_price.fetch,
    "TaiwanOptionTick": option_tick.fetch,
    "TaiwanFuturesTick": futures_tick.fetch,
    "TaiwanOptionOpenInterestLargeTraders": large_traders.fetch,
}

def process_task(task_id: str, date: str, dataset_name: str, data_id: str):
    """
    執行單一資料抓取任務：爬取 -> 儲存 -> 更新狀態
    """
    db = get_db_manager()

    # 再次確認狀態以避免競態條件
    status = db.get_task_status(task_id)
    if status is not None and status >= 1:
        logger.info(f"任務 {task_id} 已完成，跳過。")
        return

    logger.info(f"開始執行任務 {task_id} ({dataset_name} {date} {data_id})")

    try:
        # 0.5 本地實體檔案檢查 (Self-Healing)
        # 用來防禦 status.db 遺失但實體檔案還在的情況，避免浪費 API 額度
        from core.config import DATA_DIR
        year = date.split('-')[0]
        filename = f"{data_id}_{date}.parquet" if data_id else f"{date}.parquet"
        target_path = DATA_DIR / year / dataset_name / filename
        
        file_exists = False
        if target_path.exists() and target_path.stat().st_size > 0:
            file_exists = True
        else:
            import os
            from pathlib import Path
            if os.environ.get("SYNC_TO_DRIVE", "False").lower() == "true":
                drive_base = os.environ.get("DRIVE_PATH", "/content/drive/MyDrive/QuantData")
                drive_path = Path(drive_base) / "data" / year / dataset_name / filename
                if drive_path.exists() and drive_path.stat().st_size > 0:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    import shutil
                    shutil.copy2(drive_path, target_path)
                    file_exists = True

        if file_exists:
            logger.debug(f"實體檔案已存在，跳過 API 爬取直接標記完成 (自癒DB): {filename}")
            from storage.integrity_validator import compute_md5
            checksum = compute_md5(target_path)
            db.update_task_status(task_id, 1, checksum)
            return

        # 1. 資料爬取 (Fetch)
        fetch_func = FETCHERS.get(dataset_name)
        if not fetch_func:
            logger.error(f"未知的資料集名稱: {dataset_name} (任務 {task_id})")
            return
            
        df = fetch_func(date, data_id)

        # 2. 資料儲存 (Store L4 - SSD)
        if df is not None and not df.is_empty():
            file_path, checksum = save_dataframe(df, dataset_name, date, data_id)

            # 3. 雲端同步與狀態核實 (Cloud Customs Verification)
            if checksum:
                # 只有 Colab 環境的 strategy 會有這段邏輯，local 的會直接 return True
                import os
                if os.environ.get("SYNC_TO_DRIVE", "False").lower() == "true":
                    from run_all import ColabStrategy
                    from core.config import DB_PATH
                    import pathlib
                    
                    # 臨時產生個 Strategy 來借用 sync 功能
                    strategy = ColabStrategy(pathlib.Path("/content/local_data"), DB_PATH)
                    sync_success = strategy.sync_file_to_remote(pathlib.Path(file_path))
                    
                    if not sync_success:
                        logger.error(f"任務 {task_id} 雲端同步失敗，拒絕核發 DB 許可 (保持為 0)")
                        return
                    else:
                        # 成功同步到 Drive，核實通過！
                        db.update_task_status(task_id, 1, checksum)
                        logger.debug(f"任務 {task_id} 執行成功並同步至 Drive。已儲存至 {file_path}")
                else:
                    # 無開啟 Drive 同步，直接結案
                    db.update_task_status(task_id, 1, checksum) # 1 = L1 完成
                    logger.debug(f"任務 {task_id} 執行成功。已儲存至 {file_path}")
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

    dates = dates_df["date"].dt.strftime("%Y-%m-%d").to_list()

    count = 0
    for date_str in dates:
        for dset, did in target_datasets:
            task_id = f"{date_str}_{dset}_{did}"
            db.register_task(task_id, date_str, dset, did)
            count += 1

    logger.info(f"已針對 {len(dates)} 個交易日建立了 {count} 個任務。")
