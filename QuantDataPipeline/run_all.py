"""
run_all.py — 自動化管線 (P0 核心)

使用方式:
    # 本地環境 — 倒推 30 天
    python run_all.py --lookback 30 --env local

    # Colab 環境 — 倒推 60 天
    python run_all.py --lookback 60 --env colab

    # 指定日期範圍
    python run_all.py --start 2024-01-01 --end 2024-01-31

架構:
    [啟動] → [環境初始化] → [取得交易日]
          → FOR 每個交易日:
              ├─ status=0 → Phase 1 (API 下載) → status=1
              ├─ status=1 → Phase 2 (Greeks 計算) → status=2
              ├─ status=2/3 → 跳過
              └─ API 429 → sleep(300) → 繼續
          → [月度結算檢查] → [完成]
"""
import argparse
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from core.config import DATA_DIR, DB_PATH, LOG_FILE
from core.pipeline_logger import setup_logger
from core.db_metadata_manager import get_db_manager
from core.fetch_orchestrator import process_task, seed_tasks_from_dates
from fetchers.datasets.technical import trading_date
from fetchers.infrastructure.http_session import get_session
from compute_greeks_pipeline import compute_greeks_for_date

logger = setup_logger("pipeline.run_all", LOG_FILE)

# ─────────────────────────────────────────────
# 常數
# ─────────────────────────────────────────────
COOLDOWN_SECONDS = 300  # API 額度耗盡冷卻 5 分鐘
TARGET_DATASETS = [
    ("TaiwanOptionTick", "TXO"),
    ("TaiwanFuturesTick", "TX"),
]


# ─────────────────────────────────────────────
# 環境策略
# ─────────────────────────────────────────────

class EnvironmentStrategy:
    """環境策略基類"""

    def __init__(self, data_dir: Path, db_path: Path):
        self.data_dir = data_dir
        self.db_path = db_path

    def setup(self):
        """啟動前初始化"""
        pass

    def teardown(self):
        """結束後清理/同步"""
        pass

    def sync_file_to_remote(self, local_path: Path) -> bool:
        """將檔案同步到遠端 (本地模式無操作)"""
        return True


class LocalStrategy(EnvironmentStrategy):
    """本地開發環境 — 直接操作本地磁碟"""
    pass


class ColabStrategy(EnvironmentStrategy):
    """
    Google Colab 環境策略:
    - 啟動時: 複製 Drive 上的 status.db 到 /content/local_data
    - 執行中: 全部在本地 SSD 操作
    - 完成時: 同步回 Drive
    """

    def __init__(self, data_dir: Path, db_path: Path,
                 drive_data_dir: Path = None, drive_db_path: Path = None):
        super().__init__(data_dir, db_path)
        self.drive_data_dir = drive_data_dir or Path("/content/drive/MyDrive/QuantData")
        self.drive_db_path = drive_db_path or self.drive_data_dir / "status.db"
        self.local_data_dir = Path("/content/local_data")
        self.local_db_path = self.local_data_dir / "status.db"

    def setup(self):
        """將 Drive 上的 DB 複製到本地"""
        self.local_data_dir.mkdir(parents=True, exist_ok=True)

        if self.drive_db_path.exists():
            shutil.copy2(self.drive_db_path, self.local_db_path)
            logger.info(f"已將 {self.drive_db_path} 複製到本地")
        else:
            logger.info("Drive 上無 status.db，將建立新的")

        # 覆蓋全域變數
        self.data_dir = self.local_data_dir
        self.db_path = self.local_db_path

        import core.config
        core.config.DATA_DIR = self.local_data_dir

    def teardown(self):
        """同步 DB 回 Drive"""
        self.drive_data_dir.mkdir(parents=True, exist_ok=True)
        if self.local_db_path.exists():
            shutil.copy2(self.local_db_path, self.drive_db_path)
            logger.info(f"已將 status.db 同步回 {self.drive_db_path}")

    def sync_file_to_remote(self, local_path: Path) -> bool:
        """將 Parquet 複製到 Drive"""
        try:
            # 雲端海關檢查：阻擋副檔名不對、檔案大小為 0 的破損檔
            if local_path.suffix == ".tmp":
                logger.warning(f"拒絕同步暫存檔至 Drive: {local_path.name}")
                return False
            
            if not local_path.exists() or local_path.stat().st_size == 0:
                logger.error(f"拒絕同步空檔案 (0 byte) 至 Drive: {local_path}")
                return False
                
            relative = local_path.relative_to(self.local_data_dir)
            dest = self.drive_data_dir / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
            # 驗證
            if dest.exists() and dest.stat().st_size > 0:
                logger.info(f"已同步 {local_path.name} → Drive")
                
                # 同步成功後，清除本地端 SSD 的暫存副本，避免幾百 GB 將 Colab 灌爆
                # (預設會清，除非指定 CLEANUP_AFTER_SYNC=False)
                if os.environ.get("CLEANUP_AFTER_SYNC", "True").lower() == "true":
                    try:
                        local_path.unlink()
                    except Exception as e:
                        logger.warning(f"清除本地暫存失敗: {local_path.name} ({e})")
                        
                return True
            else:
                logger.error(f"Drive 同步驗證失敗: {dest}")
                return False
        except Exception as e:
            logger.error(f"Drive 同步失敗: {e}")
            return False


def get_strategy(env: str) -> EnvironmentStrategy:
    """工廠函數: 根據環境名稱回傳策略實例"""
    if env == "colab":
        return ColabStrategy(DATA_DIR, DB_PATH)
    return LocalStrategy(DATA_DIR, DB_PATH)


# ─────────────────────────────────────────────
# 核心管線邏輯
# ─────────────────────────────────────────────

def _is_api_quota_error(e: Exception) -> bool:
    """檢測是否為 API 額度耗盡或速率限制錯誤"""
    err_msg = str(e).lower()
    return any(kw in err_msg for kw in [
        "429", "rate limit", "quota", "額度", "too many requests",
        "request limit", "usage limit"
    ])


def run_pipeline(
    start_date: str = None,
    end_date: str = None,
    lookback: int = 30,
    env: str = "local",
    skip_phase1: bool = False,
    skip_phase2: bool = False,
):
    """
    主管線入口。

    Args:
        start_date: 起始日 (YYYY-MM-DD)，若未指定則用 lookback
        end_date: 結束日 (YYYY-MM-DD)，預設今天
        lookback: 從今天往回推的天數
        env: 環境模式 ('local' | 'colab')
        skip_phase2: 是否跳過 Phase 2 (Greeks 計算)
    """
    strategy = get_strategy(env)
    strategy.setup()

    db = get_db_manager()
    session = get_session()

    # ── 決定日期範圍 ──
    today = datetime.now()
    if not end_date:
        end_date = today.strftime("%Y-%m-%d")
    if not start_date:
        start_dt = today - timedelta(days=lookback)
        start_date = start_dt.strftime("%Y-%m-%d")

    logger.info(f"{'='*60}")
    logger.info(f"  QuantDataPipeline 自動化管線啟動")
    logger.info(f"  日期範圍: {start_date} → {end_date}")
    logger.info(f"  環境: {env.upper()}")
    logger.info(f"{'='*60}")

    # ── 0. 清理孤兒狀態 ──
    orphan_count = db.reset_orphan_tasks(strategy.data_dir)
    if orphan_count:
        logger.info(f"已清理 {orphan_count} 個孤兒任務")

    # ── 1. 取得交易日 ──
    logger.info(f"正在取得 {start_date} 至 {end_date} 的交易日曆...")
    try:
        trading_dates_df = trading_date.fetch_trading_dates(session, start_date, end_date)
    except Exception as e:
        logger.error(f"取得交易日曆失敗: {e}")
        strategy.teardown()
        return

    if trading_dates_df is None or trading_dates_df.is_empty():
        logger.info("在此範圍內未找到交易日。程式結束。")
        strategy.teardown()
        return

    n_dates = len(trading_dates_df)
    logger.info(f"共找到 {n_dates} 個交易日")

    # ── 2. 註冊任務到 DB ──
    seed_tasks_from_dates(db, trading_dates_df, TARGET_DATASETS)

    # ── 3. Phase 1: API 下載迴圈 ──
    if not skip_phase1:
        logger.info(f"\n{'─'*40}")
        logger.info(f"  Phase 1: API 資料下載")
        logger.info(f"{'─'*40}")
    
        pending = db.get_pending_tasks()
        total_pending = len(pending)
        logger.info(f"待處理任務: {total_pending} 個")
    
        completed_p1 = 0
        download_workers = int(os.environ.get("DOWNLOAD_WORKERS", "30"))
        logger.info(f"使用 {download_workers} 個執行緒進行「無速限」併發下載")
    
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import time
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=download_workers) as executor:
            future_to_task = {
                executor.submit(process_task, task_id, trade_date_str, dataset_name, data_id): task_id
                for task_id, trade_date_str, dataset_name, data_id in pending
            }
            
            for i, future in enumerate(as_completed(future_to_task), 1):
                task_id = future_to_task[future]
                try:
                    future.result()
                    completed_p1 += 1
                    
                    if i % 10 == 0 or i == total_pending:
                        elapsed = time.time() - start_time
                        avg_time = elapsed / i
                        eta_sec = avg_time * (total_pending - i)
                        eta_str = f"{int(eta_sec//60)}m {int(eta_sec%60)}s" if eta_sec < 3600 else f"{int(eta_sec//3600)}h {int((eta_sec%3600)//60)}m"
                        
                        pct = (i / total_pending)
                        blocks = int(pct * 20)
                        bar = "🟩" * blocks + "⬛" * (20 - blocks)
                        
                        logger.info(f"[P1 下載進度] {bar} {i}/{total_pending} ({pct:.1%}) | 預估剩餘: {eta_str}")
                except Exception as e:
                    if _is_api_quota_error(e):
                        logger.error(f"🚫 API 額度已耗盡或觸發限制 (429)，將在此停止！")
                        executor.shutdown(wait=False, cancel_futures=True)
                        sys.exit(1)
                    else:
                        logger.error(f"任務 {task_id} 失敗: {e}")
    
        logger.info(f"Phase 1 完成: {completed_p1}/{total_pending} 個任務處理完畢 (包含重試/跳過)")

    # ── 4. Phase 2: Greeks 計算 ──
    if not skip_phase2:
        logger.info(f"\n{'─'*40}")
        logger.info(f"  Phase 2: Greeks 特徵計算")
        logger.info(f"{'─'*40}")

        l1_tasks = db.get_tasks_by_status(1)
        # 過濾出需要計算 Greeks 的選擇權任務
        option_dates = set()
        for task_id, trade_date_str, dataset_name, data_id in l1_tasks:
            if dataset_name == "TaiwanOptionTick":
                # 檢查是否已有 Greeks 輸出
                year = trade_date_str.split("-")[0]
                greeks_path = strategy.data_dir / year / "GreeksFeatures" / f"TXO_Greeks_{trade_date_str}.parquet"
                if not greeks_path.exists():
                    option_dates.add(trade_date_str)

        # 需要期貨也標記為 L1 才能 Asof Join
        computable_dates = []
        for d in sorted(option_dates):
            fut_task_id = f"{d}_TaiwanFuturesTick_TX"
            fut_status = db.get_task_status(fut_task_id)
            if fut_status is not None and fut_status >= 1:
                computable_dates.append(d)
            else:
                logger.warning(f"跳過 {d}: 期貨資料未就緒 (status={fut_status})")

        total_compute = len(computable_dates)
        logger.info(f"待計算日期: {total_compute} 個")

        completed_p2 = 0
        greeks_workers = int(os.environ.get("GREEKS_WORKERS", "2"))
        logger.info(f"使用 {greeks_workers} 個執行緒進行 Greeks 計算")

        with ThreadPoolExecutor(max_workers=max(1, greeks_workers)) as executor:
            future_to_date = {
                executor.submit(compute_greeks_for_date, date_str, data_dir=strategy.data_dir): date_str
                for date_str in computable_dates
            }

            for i, future in enumerate(as_completed(future_to_date), 1):
                date_str = future_to_date[future]
                try:
                    df_greeks, output_path, success = future.result()
                    if success:
                        opt_task_id = f"{date_str}_TaiwanOptionTick_TXO"
                        db.update_task_status(opt_task_id, 2)
                        completed_p2 += 1
                        logger.info(f"  ✅ [P2 {i}/{total_compute}] {date_str} Greeks 計算完成 ({len(df_greeks)} 筆)")
                    else:
                        logger.warning(f"  ⚠️ [P2 {i}/{total_compute}] {date_str} Greeks 計算無有效資料")
                except Exception as e:
                    logger.error(f"  ❌ [P2 {i}/{total_compute}] {date_str} Greeks 計算失敗: {e}")

        logger.info(f"Phase 2 完成: {completed_p2}/{total_compute} 個日期處理完畢")

    # ── 5. 收尾 ──
    strategy.teardown()

    # 統計
    stats = {
        "total_tasks": len(db.get_tasks_by_status(0)) + len(db.get_tasks_by_status(1))
                       + len(db.get_tasks_by_status(2)) + len(db.get_tasks_by_status(3)),
        "pending": len(db.get_tasks_by_status(0)),
        "l1_done": len(db.get_tasks_by_status(1)),
        "l2_done": len(db.get_tasks_by_status(2)),
        "skipped": len(db.get_tasks_by_status(3)),
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"  管線執行完畢")
    logger.info(f"  任務總數: {stats['total_tasks']}")
    logger.info(f"  待處理: {stats['pending']} | L1完成: {stats['l1_done']} | "
                f"L2完成: {stats['l2_done']} | 跳過: {stats['skipped']}")
    logger.info(f"{'='*60}")

    return stats


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QuantDataPipeline 自動化管線")
    parser.add_argument("--start", type=str, default=None,
                        help="起始日期 (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None,
                        help="結束日期 (YYYY-MM-DD)")
    parser.add_argument("--lookback", type=int, default=30,
                        help="從今天往回推的天數 (預設 30)")
    parser.add_argument("--env", type=str, default="local",
                        choices=["local", "colab"],
                        help="執行環境 (預設 local)")
    parser.add_argument("--skip-phase1", action="store_true",
                        help="跳過 Phase 1 API 下載")
    parser.add_argument("--skip-phase2", action="store_true",
                        help="跳過 Phase 2 Greeks 計算")

    args = parser.parse_args()

    run_pipeline(
        start_date=args.start,
        end_date=args.end,
        lookback=args.lookback,
        env=args.env,
        skip_phase1=args.skip_phase1,
        skip_phase2=args.skip_phase2,
    )
