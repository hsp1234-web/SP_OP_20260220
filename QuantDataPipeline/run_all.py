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
# 常數與輔助函數
# ─────────────────────────────────────────────
COOLDOWN_SECONDS = 300  # API 額度耗盡冷卻 5 分鐘

def _is_valid_parquet_file(file_path: Path, min_size_bytes: int = 1024) -> bool:
    """檢查 Parquet 檔案是否存在且有效 (大小大於閾值)"""
    if not file_path.exists():
        return False
    # 一個最基礎的空 parquet 表頭大約有數百 byte，如果小於 1024 通常是有問題或壞掉的
    if file_path.stat().st_size < min_size_bytes:
        logger.warning(f"偵測到雲端存在損毀/過小檔案: {file_path}，將視為遺失。")
        return False
    return True


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

    def get_scan_dir(self) -> Path:
        """取得進行檔案盤點的目錄基準"""
        return self.data_dir


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
        self.drive_data_dir = drive_data_dir or Path("/content/drive/MyDrive/QuantData/data")
        self.drive_db_path = drive_db_path or Path("/content/drive/MyDrive/QuantData/status.db")
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
            import sqlite3
            try:
                with sqlite3.connect(self.local_db_path) as src, sqlite3.connect(self.drive_db_path) as dst:
                    src.backup(dst)
                logger.info(f"已將 status.db 備份回 {self.drive_db_path}")
            except Exception as e:
                logger.error(f"備份 status.db 失敗: {e}")

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
            if dest.exists() and dest.stat().st_size > 0:
                logger.debug(f"已同步 {local_path.name} → Drive")
                
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

    def get_scan_dir(self) -> Path:
        """雲端模式下，盤點應以真正的 Drive 為基準"""
        return self.drive_data_dir if self.drive_data_dir else self.data_dir


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

    # ── 0. 清理孤兒狀態 (保留供相容性與日誌，但不再強制依賴) ──
    try:
        orphan_count = db.reset_orphan_tasks(strategy.data_dir)
        if orphan_count:
            logger.info(f"已清理 {orphan_count} 個 DB 孤兒紀錄")
    except Exception:
        pass

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

    # 不再強依賴 db seed，直接進入實體檔案比對
    # seed_tasks_from_dates(db, trading_dates_df, TARGET_DATASETS)

    all_trade_dates = sorted(trading_dates_df["date"].cast(str).to_list(), reverse=True)
    all_trade_dates = [d[:10] for d in all_trade_dates]  # YYYY-MM-DD

    # 動態指派的任務清單
    phase1_tasks = []  # [(task_id, date, dataset, data_id), ...]
    phase2_dates = []  # [date, ...]

    # ── 檔案狀態盤點 ──
    scan_dir = strategy.get_scan_dir()
    logger.info("正在根據 Google Drive / 實體儲存空間進行檔案盤點...")
    for trade_date in all_trade_dates:
        year = trade_date.split("-")[0]
        opt_path = scan_dir / year / "TaiwanOptionTick" / f"TXO_{trade_date}.parquet"
        fut_path = scan_dir / year / "TaiwanFuturesTick" / f"TX_{trade_date}.parquet"
        greeks_path = scan_dir / year / "GreeksFeatures" / f"TXO_Greeks_{trade_date}.parquet"

        has_opt = _is_valid_parquet_file(opt_path)
        has_fut = _is_valid_parquet_file(fut_path)
        has_greeks = _is_valid_parquet_file(greeks_path)

        # Phase 1 判斷: 缺材料就補
        if not has_opt:
            phase1_tasks.append((f"{trade_date}_TaiwanOptionTick_TXO", trade_date, "TaiwanOptionTick", "TXO"))
        if not has_fut:
            phase1_tasks.append((f"{trade_date}_TaiwanFuturesTick_TX", trade_date, "TaiwanFuturesTick", "TX"))

        # Phase 2 判斷: 有材料沒成品就做 (如果有材料且也要做 Phase 1 的話，Phase 1 做完就會自然觸發)
        # 注意：如果 --skip-phase1 模式下，只有材料都有才能做 Phase 2
        # 若正常模式，只要這天沒有 greeks，且最後(經過 P1 後)會有材料，就排入 P2
        if not skip_phase2 and not has_greeks:
            phase2_dates.append(trade_date)


    # ── 3. Phase 1: API 下載迴圈 ──
    if not skip_phase1:
        logger.info(f"\n{'─'*40}")
        logger.info(f"  Phase 1: API 資料下載 (實體檔案盤點模式)")
        logger.info(f"{'─'*40}")
    
        total_pending = len(phase1_tasks)
        logger.info(f"待處理下載任務 (缺件): {total_pending} 個")
    
        completed_p1 = 0
        if total_pending > 0:
            download_workers = int(os.environ.get("DOWNLOAD_WORKERS", "30"))
            logger.info(f"使用 {download_workers} 個執行緒進行併發下載")
        
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time
            start_time = time.time()
            
            month_totals = {}
            month_completed = {}
            for task_id, trade_date_str, dataset_name, data_id in phase1_tasks:
                m = trade_date_str[:7]
                month_totals[m] = month_totals.get(m, 0) + 1
                month_completed[m] = 0

            with ThreadPoolExecutor(max_workers=download_workers) as executor:
                future_to_task = {
                    executor.submit(process_task, task_id, trade_date_str, dataset_name, data_id): (task_id, trade_date_str)
                    for task_id, trade_date_str, dataset_name, data_id in phase1_tasks
                }
                
                for i, future in enumerate(as_completed(future_to_task), 1):
                    task_id, trade_date_str = future_to_task[future]
                    try:
                        future.result()
                        completed_p1 += 1
                        
                        month = trade_date_str[:7]
                        month_completed[month] += 1
                        
                        if i % 10 == 0 or i == total_pending:
                            elapsed = time.time() - start_time
                            avg_time = elapsed / i
                            eta_sec = avg_time * (total_pending - i)
                            eta_str = f"{int(eta_sec//60)}m {int(eta_sec%60)}s" if eta_sec < 3600 else f"{int(eta_sec//3600)}h {int((eta_sec%3600)//60)}m"
                            
                            pct = (i / total_pending)
                            blocks = int(pct * 20)
                            bar = "🟩" * blocks + "⬛" * (20 - blocks)
                            
                            m_comp = month_completed[month]
                            m_tot = month_totals[month]
                            m_pct = m_comp / m_tot if m_tot > 0 else 0
                            m_blocks = int(m_pct * 10)
                            m_bar = "🟦" * m_blocks + "⬛" * (10 - m_blocks)
                            
                            logger.info(f"[P1 下載進度] 總覽 <br>📅 <b>目標月份 ({month})</b>: {m_bar} {m_comp}/{m_tot} ({m_pct:.1%}) <br>🚀 <b>整體管線進度</b>: {bar} {i}/{total_pending} ({pct:.1%}) | ETA: {eta_str}")
                    except Exception as e:
                        if _is_api_quota_error(e):
                            logger.error(f"🚫 API 額度已耗盡或觸發限制 (429)，將在此停止！")
                            executor.shutdown(wait=False, cancel_futures=True)
                            sys.exit(1)
                        else:
                            logger.error(f"任務 {task_id} 失敗: {e}")
        
            logger.info(f"Phase 1 完成: 成功獲取 {completed_p1}/{total_pending} 份缺失資料")
        else:
            logger.info("Phase 1 全部檔案齊全，無需下載。")

    # ── 4. Phase 2: Greeks 計算 ──
    if not skip_phase2:
        logger.info(f"\n{'─'*40}")
        logger.info(f"  Phase 2: Greeks 特徵計算 (缺件補齊模式)")
        logger.info(f"{'─'*40}")

        # 在 P1 下載後，重新盤點可計算的日期 (以防 P1 有失敗導致還是缺件)
        computable_dates = []
        for d in sorted(phase2_dates, reverse=True):
            year = d.split("-")[0]
            opt_path = scan_dir / year / "TaiwanOptionTick" / f"TXO_{d}.parquet"
            fut_path = scan_dir / year / "TaiwanFuturesTick" / f"TX_{d}.parquet"
            
            if _is_valid_parquet_file(opt_path) and _is_valid_parquet_file(fut_path):
                # 確保進 Phase 2 計算之前，本地高速 SSD 有材料檔案
                local_opt = strategy.data_dir / year / "TaiwanOptionTick" / f"TXO_{d}.parquet"
                local_fut = strategy.data_dir / year / "TaiwanFuturesTick" / f"TX_{d}.parquet"
                if opt_path != local_opt:
                    if not _is_valid_parquet_file(local_opt):
                        local_opt.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(opt_path, local_opt)
                    if not _is_valid_parquet_file(local_fut):
                        local_fut.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(fut_path, local_fut)

                computable_dates.append(d)
            else:
                logger.warning(f"跳過 {d}: 材料不齊全 (雲端檔案不存在或毀損)")

        total_compute = len(computable_dates)
        logger.info(f"待計算 Greeks 日期數: {total_compute} 天")

        completed_p2 = 0
        if total_compute > 0:
            greeks_workers = int(os.environ.get("GREEKS_WORKERS", "2"))
            logger.info(f"使用 {greeks_workers} 個執行緒進行 Greeks 計算")

            from concurrent.futures import ThreadPoolExecutor, as_completed
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
                            completed_p2 += 1
                            logger.info(f"  ✅ [P2 {i}/{total_compute}] {date_str} Greeks 計算完成 ({len(df_greeks)} 筆)")
                            
                            # 算完後，呼叫策略的 sync 將結果傳回 Drive
                            strategy.sync_file_to_remote(output_path)
                            # 但這裡直接依賴 compute_greeks 和後續流程，或者讓外部 Colab Launcher 控制
                        else:
                            logger.warning(f"  ⚠️ [P2 {i}/{total_compute}] {date_str} Greeks 計算無有效資料")
                    except Exception as e:
                        logger.error(f"  ❌ [P2 {i}/{total_compute}] {date_str} Greeks 計算失敗: {e}")

            logger.info(f"Phase 2 完成: {completed_p2}/{total_compute} 個日期處理完畢")
        else:
            logger.info("Phase 2 無需計算 (Greeks 皆已存在或材料不足)。")

    # ── 5. 收尾 ──
    strategy.teardown()

    # 統計
    stats = {
        "dates_checked": len(all_trade_dates),
        "phase1_downloads": completed_p1 if not skip_phase1 else 0,
        "phase2_computed": completed_p2 if not skip_phase2 else 0,
    }

    logger.info(f"\n{'='*60}")
    logger.info(f"  管線執行完畢 (實體檔案模式)")
    logger.info(f"  交易日查核總數: {stats['dates_checked']}")
    logger.info(f"  補齊缺少 P1 檔案: {stats['phase1_downloads']} 份")
    logger.info(f"  補齊缺少 P2 Greeks: {stats['phase2_computed']} 份")
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
