import concurrent.futures
import logging
import sys
from pathlib import Path
from typing import List, Tuple

# Ensure project root is in sys.path
sys.path.append(str(Path(__file__).parent))

from core.db_metadata_manager import DBMetadataManager
from core.pipeline_logger import setup_logger
from core.config import LOG_FILE

# Import fetchers
from fetchers.datasets.technical import stock_price_tick, stock_price
from fetchers.datasets.derivative import option_tick
from fetchers.datasets.chip import large_traders
from storage.parquet_writer import save_dataframe

logger = setup_logger("pipeline.main", LOG_FILE)

def process_task(task_id: str, date: str, dataset_name: str, data_id: str):
    """
    Execute a single task: Fetch -> Store -> Update DB
    """
    db = DBMetadataManager()

    # Check status again to handle potential race conditions
    status = db.get_task_status(task_id)
    if status is not None and status >= 1:
        logger.info(f"Task {task_id} already completed. Skipping.")
        return

    logger.info(f"Starting task {task_id} ({dataset_name} {date} {data_id})")

    try:
        # 1. Fetch
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
            logger.error(f"Unknown dataset: {dataset_name} for task {task_id}")
            return

        # 2. Store (L4)
        if df is not None and not df.is_empty():
            file_path, checksum = save_dataframe(df, dataset_name, date, data_id)

            # 3. Update Status
            if checksum:
                db.update_task_status(task_id, 1, checksum) # 1 = L1_Ready
                logger.info(f"Task {task_id} completed successfully. Saved to {file_path}")
            else:
                logger.warning(f"Task {task_id} produced no file (empty df?). Marking as EMPTY_SKIP (3).")
                db.update_task_status(task_id, 3)
        else:
             logger.warning(f"Task {task_id} fetched empty data. Marking as EMPTY_SKIP (3).")
             db.update_task_status(task_id, 3) # Empty

    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}", exc_info=True)
        # We leave status as 0 (Pending) to allow retries in future runs.

def seed_initial_tasks(db: DBMetadataManager):
    """
    Seed some initial tasks for testing/POC.
    """
    # Example tasks
    tasks = [
        # TaiwanStockPrice (Daily) for TSMC (2330)
        ("2023-10-02", "TaiwanStockPrice", "2330"),

        # TaiwanStockPriceTick (Tick) for TSMC - heavier payload
        # ("2023-10-02", "TaiwanStockPriceTick", "2330"),

        # Chip data
        ("2023-10-02", "TaiwanOptionOpenInterestLargeTraders", "TXO"),
    ]

    for date, dset, did in tasks:
        task_id = f"{date}_{dset}_{did}"
        db.register_task(task_id, date, dset, did)

def main():
    db = DBMetadataManager()

    # 1. Seed tasks (idempotent)
    seed_initial_tasks(db)

    # 2. Get pending tasks
    pending_tasks = db.get_pending_tasks()
    logger.info(f"Found {len(pending_tasks)} pending tasks.")

    if not pending_tasks:
        logger.info("No pending tasks found.")
        return

    # 3. Execute concurrently
    # Using 2 workers to respect rate limits (anonymous: 300/hr = 1 req/12s)
    # With 2 threads, we might hit limits faster, but RateLimiter is a global singleton lock,
    # so threads will just wait for each other.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for task_id, trade_date, dataset_name, data_id in pending_tasks:
            future = executor.submit(process_task, task_id, trade_date, dataset_name, data_id)
            futures.append(future)

        # Wait for completion
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Task execution error: {e}")

if __name__ == "__main__":
    main()
