#!/usr/bin/env python3
"""
V4.2 Phase 1: 全市場倒序抓取腳本 (fetch_market_data.py)

僅負責從 FinMind API 下載 JSON 並落地存檔，不做任何轉換處理。
支援「每日更新 (update)」與「歷史回補 (backfill)」兩種模式。

用法:
  python fetch_market_data.py --mode update
  python fetch_market_data.py --mode backfill --batch-size 60
  python fetch_market_data.py --mode backfill --datasets TaiwanStockPriceAdj
"""

import argparse
import gzip
import json
import logging
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Set, Optional, Dict

import requests

# 確保專案根目錄在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.datasets_registry import (
    ALL_DATASETS,
    TRADING_CALENDAR,
    DATASETS_BY_NAME,
    get_dataset_names,
    validate_dataset_name,
)
from core.sync_tracker import SyncTracker

# 載入 .env
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

# ========== 日誌 ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_market_data")

# ========== 全域設定 ==========
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")

# 環境偵測
IS_COLAB = "google.colab" in sys.modules

# 資源配額
DOWNLOAD_WORKERS = int(os.getenv("V4_DOWNLOAD_WORKERS", "10"))
_quota = os.getenv("FINMIND_QUOTA_PER_HOUR")
if _quota:
    # 加強衝刺能力：為了配合 10 併發，將延遲縮短。
    # 即使設很短，若觸發 429 也會進入冷卻模式。
    API_DELAY = 0.5  # 基礎間隔，允許多個執行緒交錯
else:
    API_DELAY = 1.0  # token 預設間隔

# 資料目錄
DATA_V4_DIR = PROJECT_ROOT / "data_v4"
TEMP_RAW_DIR = DATA_V4_DIR / "temp_raw_data"
PROCESSED_DIR = DATA_V4_DIR / "processed_parquet"

# ========== 狀態與控制 ==========
STOP_FLAG = threading.Event()
PAUSE_FLAG = threading.Event()  # 用於冷卻中暫停所有執行緒
WAIT_SEQUENCE = [600, 900, 1200, 1500, 1800]  # 10m, 15m, 20m, 25m, 30m (秒)
retry_level = 0

# ========== 速率限制器 ==========
_rate_lock = threading.Lock()
_last_request_time = 0.0


def _rate_limit_wait():
    """全域速率限制：確保兩次請求之間有足夠間隔"""
    global _last_request_time
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < API_DELAY:
            time.sleep(API_DELAY - elapsed)
        _last_request_time = time.time()


# ========== API 額度查詢 ==========
USER_INFO_URL = "https://api.web.finmindtrade.com/v2/user_info"


def check_api_quota(
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, int]]:
    """
    查詢 FinMind API 額度使用狀況。
    
    Returns:
        成功: {"used": int, "limit": int, "remaining": int}
        失敗: None
    """
    if not FINMIND_API_TOKEN:
        logger.warning("⚠️ 無 Token，無法查詢額度")
        return None

    sess = session or requests.Session()
    try:
        headers = {"Authorization": f"Bearer {FINMIND_API_TOKEN}"}
        resp = sess.get(USER_INFO_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        used = int(data.get("user_count", 0))
        limit = int(data.get("api_request_limit", 0))
        remaining = limit - used

        return {"used": used, "limit": limit, "remaining": remaining}

    except Exception as e:
        logger.warning(f"⚠️ 額度查詢失敗: {e}")
        return None


def display_quota_status(
    quota: Dict[str, int],
    planned_calls: int = 0,
):
    """顯示額度使用狀況"""
    used = quota["used"]
    limit = quota["limit"]
    remaining = quota["remaining"]
    pct_used = (used / limit * 100) if limit > 0 else 0

    logger.info(f"📊 API 額度: {used:,} / {limit:,} (已用 {pct_used:.1f}%)")
    logger.info(f"📊 剩餘可用: {remaining:,} 次")

    if planned_calls > 0:
        pct_of_remaining = (planned_calls / remaining * 100) if remaining > 0 else 999
        logger.info(f"📊 本次預計消耗: {planned_calls:,} 次 (佔剩餘 {pct_of_remaining:.1f}%)")
        if planned_calls > remaining:
            logger.warning(
                f"⚠️ 額度不足！需要 {planned_calls:,} 次，但只剩 {remaining:,} 次。"
                f"將盡量下載至額度用盡後自動停止。"
            )


# ========== API 呼叫 ==========
def fetch_from_api(
    dataset: str,
    date_str: str,
    data_id: str = "",
    session: Optional[requests.Session] = None,
) -> Optional[dict]:
    """
    呼叫 FinMind API 取得單日資料。
    
    Returns:
        成功: dict (API 原始回應)
        失敗: None
    """
    if STOP_FLAG.is_set():
        return None

    _rate_limit_wait()

    params = {
        "dataset": dataset,
        "start_date": date_str,
        "end_date": date_str,
        "token": FINMIND_API_TOKEN,
    }
    if data_id:
        params["data_id"] = data_id

    sess = session or requests.Session()
    try:
        # 如果正在冷卻中，執行緒先行等待
        while PAUSE_FLAG.is_set() and not STOP_FLAG.is_set():
            time.sleep(1)

        resp = sess.get(FINMIND_API_URL, params=params, timeout=30)
        
        # 處理 HTTP 狀態碼
        if resp.status_code in [402, 429]:
            if not PAUSE_FLAG.is_set():
                reason = "權限/額度限制 (402)" if resp.status_code == 402 else "速率限制 (429)"
                logger.warning(f"⚠️ 觸發 {reason}，即將進入冷卻模式...")
                PAUSE_FLAG.set()
            return None
        elif resp.status_code == 403:
            logger.error(f"❌ 禁止存取 (403): Token 無效或無此資料集權限")
            STOP_FLAG.set()
            return None
            
        resp.raise_for_status()
        result = resp.json()

        # 檢查 API 錯誤訊息 (JSON 內容)
        msg = result.get("msg", "")
        if "rate limit" in msg.lower():
            if not PAUSE_FLAG.is_set():
                logger.warning(f"⚠️ API 回報額度限制: {msg}")
                PAUSE_FLAG.set()
            return None

        # 檢查永久性錯誤 (帳號等級不足)
        fatal_keywords = ["user level", "sponsor", "permission", "unauthorized"]
        if any(kw in msg.lower() for kw in fatal_keywords):
            logger.error(f"❌ 永久性錯誤: {msg}")
            STOP_FLAG.set()
            return None

        if msg != "success":
            logger.warning(f"⚠️ API 非成功回應: {msg}")
            return None

        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ 網路錯誤 [{dataset} {date_str}]: {e}")
        return None


# ========== 檔案落地 ==========
def save_json_gz(data: dict, dataset: str, date_str: str) -> Path:
    """
    將 API 回傳的 JSON 壓縮存檔 (原子性寫入)。
    路徑: data_v4/temp_raw_data/{DatasetName}/{DatasetName}_{YYYYMMDD}.json.gz
    """
    date_compact = date_str.replace("-", "")
    dest_dir = TEMP_RAW_DIR / dataset
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{dataset}_{date_compact}.json.gz"
    filepath = dest_dir / filename

    # 先寫到暫存檔
    tmp_path = filepath.with_suffix(".tmp")
    with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    
    # 寫完後原子性更名，防止轉檔機讀到一半的檔案
    os.rename(tmp_path, filepath)
    return filepath


# ========== 交易日曆 ==========
def fetch_trading_calendar(
    start_date: str = "2010-01-01",
    session: Optional[requests.Session] = None,
) -> Set[str]:
    """
    取得完整交易日曆 (Phase 0)。
    回傳所有交易日的 Set (YYYY-MM-DD 格式)。
    """
    logger.info(f"📅 取得交易日曆 ({start_date} ~ 今天)...")
    today = datetime.now().strftime("%Y-%m-%d")

    params = {
        "dataset": TRADING_CALENDAR.name,
        "start_date": start_date,
        "end_date": today,
        "token": FINMIND_API_TOKEN,
    }

    sess = session or requests.Session()
    
    while not STOP_FLAG.is_set():
        try:
            resp = sess.get(FINMIND_API_URL, params=params, timeout=60)
            
            if resp.status_code in [402, 429]:
                wait_time = 900
                logger.warning(f"⚠️ 取得交易日曆遭遇額度限制 ({resp.status_code})，全系統冷卻 {wait_time//60} 分鐘...")
                for _ in range(wait_time):
                    if STOP_FLAG.is_set(): break
                    time.sleep(1)
                continue
            
            resp.raise_for_status()
            result = resp.json()

            if result.get("msg") != "success":
                raise RuntimeError(f"交易日曆 API 失敗: {result.get('msg')}")

            dates = set()
            for row in result.get("data", []):
                d = row.get("date", "")
                if d:
                    dates.add(d[:10])  # 取前 10 字元 YYYY-MM-DD

            logger.info(f"📅 取得 {len(dates)} 個交易日")
            return dates

        except Exception as e:
            if STOP_FLAG.is_set(): break
            logger.error(f"❌ 交易日曆取得失敗: {e}，10 秒後重試...")
            time.sleep(10)
            
    return set()


# ========== 已下載檔案掃描 ==========
def scan_downloaded_dates(dataset: str) -> Set[str]:
    """掃描已下載的 JSON 檔案，回傳已完成的日期集合"""
    dest_dir = TEMP_RAW_DIR / dataset
    if not dest_dir.exists():
        return set()

    dates = set()
    for f in dest_dir.iterdir():
        if f.suffix == ".gz" and f.name.startswith(dataset):
            # 從檔名解析日期: TaiwanStockPriceAdj_20240226.json.gz
            parts = f.stem.replace(".json", "").split("_")
            if len(parts) >= 2:
                date_compact = parts[-1]
                if len(date_compact) == 8 and date_compact.isdigit():
                    formatted = f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:8]}"
                    dates.add(formatted)
    return dates


# ========== 也掃描已轉檔的 Parquet 日期 ==========
def scan_processed_dates(dataset: str) -> Set[str]:
    """掃描已轉為 Parquet 的日期 (避免重複下載已處理的資料)"""
    processed_dir = PROCESSED_DIR / dataset
    if not processed_dir.exists():
        return set()
    
    all_dates = set()
    try:
        import polars as pl
        for p in processed_dir.glob("*.parquet"):
            try:
                # 僅讀取 date 欄位，極速掃描
                df = pl.read_parquet(p, columns=["date"])
                # 轉換為 YYYY-MM-DD 格式
                unique_dates = df["date"].dt.strftime("%Y-%m-%d").unique().to_list()
                all_dates.update(unique_dates)
            except Exception:
                continue
    except ImportError:
        # 若未安裝 polars，則回傳空，由 DB 紀錄作為主防線
        pass
        
    return all_dates


# ========== 單一任務處理 ==========
def process_single_date(
    dataset: str,
    date_str: str,
    session: requests.Session,
    tracker: SyncTracker,
    mode: str,
) -> bool:
    """
    下載單一日期的資料並存檔。
    Returns: True=成功, False=失敗或被中斷
    """
    if STOP_FLAG.is_set():
        return False

    config = DATASETS_BY_NAME[dataset]
    data_id = "" if config.bulk_mode else ""

    result = fetch_from_api(dataset, date_str, data_id=data_id, session=session)
    if result is None:
        return False

    # 檢查是否有實際資料
    records = result.get("data", [])
    if not records:
        logger.info(f"  ⏭️ {dataset} {date_str} 無資料 (空回傳)")
        return True  # 不算失敗，只是該日無交易

    # 存檔
    filepath = save_json_gz(result, dataset, date_str)
    size_kb = filepath.stat().st_size / 1024
    logger.info(f"  ✅ {dataset} {date_str} | {len(records)} 筆 | {size_kb:.1f} KB")

    return True


# ========== 主流程 ==========
def run_fetch(
    mode: str = "update",
    batch_size: int = 30,
    target_datasets: Optional[List[str]] = None,
    trading_calendar_override: Optional[Set[str]] = None,
    tracker_override: Optional[SyncTracker] = None,
    until_done: bool = False,
):
    """
    執行全市場資料下載。
    
    Args:
        mode: "update" (抓最新) 或 "backfill" (歷史回補)
        batch_size: 每批次處理的交易日數量
        target_datasets: 指定資料集 (None=全部)
        trading_calendar_override: 測試用交易日曆注入
        tracker_override: 測試用 tracker 注入
        until_done: 是否持續迴圈直到全部補全
    """
    logger.info("=" * 60)
    logger.info(f"🚀 V4.2 全市場資料下載 | 模式: {mode} | 批次: {batch_size}")
    logger.info("=" * 60)
    # 決定目標資料集
    if target_datasets:
        datasets = [validate_dataset_name(n) for n in target_datasets]
    else:
        datasets = ALL_DATASETS

    # 初始化追蹤器
    tracker = tracker_override or SyncTracker()

    # 註冊所有資料集並進行「初次磁碟同步」
    logger.info("🔍 正在同步磁碟狀態至資料庫 (確保不重複下載)...")
    for ds in datasets:
        tracker.register_dataset(ds.name, ds.default_start)
        
        # 將現有檔案（JSON + Parquet）的日期同步到 DB 的 sync_dates 表中
        # 這樣即使 JSON 被刪除，DB 也能記住已完成
        existing_json = scan_downloaded_dates(ds.name)
        existing_parquet = scan_processed_dates(ds.name)
        for d in (existing_json | existing_parquet):
            tracker.mark_date_done(ds.name, d)

    session = requests.Session()
    total_success = 0
    total_skip = 0
    global retry_level

    while True:
        # 🔍 第一步：檢查額度 (如果是 0 就地睡覺)
        quota = check_api_quota(session=session)
        if quota and quota["remaining"] <= 0:
            wait_time = WAIT_SEQUENCE[min(retry_level, len(WAIT_SEQUENCE)-1)]
            logger.info(f"😴 [啟動前] 額度已用盡，自動冷卻 {wait_time//60} 分鐘 (層級 {retry_level+1})")
            for _ in range(wait_time):
                if STOP_FLAG.is_set(): break
                time.sleep(1)
            retry_level += 1
            continue # 睡完覺再次檢查額度

        # 🔍 第二步：取得交易日曆 (如果剛才還沒拿過的話)
        if trading_calendar_override:
            trading_days = trading_calendar_override
        else:
            try:
                trading_days = fetch_trading_calendar()
                if not trading_days:
                    if STOP_FLAG.is_set(): break
                    logger.warning("📅 無法取得交易日曆，重試中...")
                    time.sleep(10)
                    continue
            except Exception as e:
                logger.error(f"❌ 嚴重錯誤：無法建立交易日曆 ({e})")
                time.sleep(10)
                continue
        # 🔍 第三步：計算並顯示預期配額消耗
        loop_work_done = False
        remaining_tasks = 0
        total_planned = 0
        for ds in datasets:
            downloaded = scan_downloaded_dates(ds.name)
            processed = scan_processed_dates(ds.name)
            all_done = downloaded | processed
            missing = tracker.get_missing_dates(ds.name, trading_days, all_done)
            total_planned += min(len(missing), batch_size)
            if missing:
                remaining_tasks += len(missing)

        if quota:
            display_quota_status(quota, planned_calls=total_planned)

        if remaining_tasks == 0:
            logger.info("🎉 [補全達成] 所有資料集均已無缺失日期！")
            break

        for ds in datasets:
            if STOP_FLAG.is_set():
                break

            # 掃描資料進度
            downloaded = scan_downloaded_dates(ds.name)
            processed = scan_processed_dates(ds.name)
            all_done = downloaded | processed
            missing = tracker.get_missing_dates(ds.name, trading_days, all_done)

            if not missing:
                continue

            loop_work_done = True
            logger.info(f"\n📦 處理資料集: {ds.label_zh} ({ds.name}) | 剩餘待補: {len(missing)} 天")
            
            total_success_before_batch = total_success

            # 切分批次
            full_batch = missing[:batch_size]
            logger.info(f"  🚀 併發衝刺: {len(full_batch)} 天 ({full_batch[0]} ~ {full_batch[-1]})")

            # 使用執行緒池併發處理
            with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
                future_to_date = {
                    executor.submit(process_single_date, ds.name, date_str, session, tracker, mode): date_str
                    for date_str in full_batch
                }

                for future in as_completed(future_to_date):
                    date_str = future_to_date[future]
                    try:
                        success = future.result()
                        if success:
                            total_success += 1
                            if mode == "update":
                                tracker.update_newest(ds.name, date_str)
                            else:
                                tracker.update_oldest(ds.name, date_str)
                        else:
                            if PAUSE_FLAG.is_set() and not STOP_FLAG.is_set():
                                wait_time = WAIT_SEQUENCE[min(retry_level, len(WAIT_SEQUENCE)-1)]
                                logger.info(f"😴 額度用盡，進入冷卻模式... 等待 {wait_time//60} 分鐘")
                                
                                for _ in range(wait_time):
                                    if STOP_FLAG.is_set(): break
                                    time.sleep(1)
                                
                                if not STOP_FLAG.is_set():
                                    retry_level += 1
                                    logger.info("✨ 冷卻結束，準備下一輪衝刺...")
                                    PAUSE_FLAG.clear()
                            total_skip += 1
                            if STOP_FLAG.is_set(): break
                    except Exception as exc:
                        logger.error(f"❌ 執行緒例外 [{date_str}]: {exc}")

            # 💡 成功重置機制：如果本輪有成功下載到資料，將退避層級歸零
            if total_success > total_success_before_batch:
                if retry_level > 0:
                    logger.info(f"✨ 本輪成功下載 {total_success - total_success_before_batch} 天資料，重置冷卻層級。")
                retry_level = 0

        if not until_done or not loop_work_done or STOP_FLAG.is_set():
            if until_done and not loop_work_done:
                logger.info("🎉 [補全達成] 所有資料集均已無缺失日期！")
            break
        
        logger.info(f"\n--- 批次完成，繼續補全剩餘任務 (約剩 {remaining_tasks} 天) ---")

    # 結束統計：再查一次額度，顯示本次實際消耗
    logger.info("\n" + "=" * 60)
    quota_after = check_api_quota(session=session)
    if quota and quota_after:
        actual_used = quota_after["used"] - quota["used"]
        logger.info(f"📊 本次實際消耗 API: {actual_used} 次")
        logger.info(f"📊 剩餘額度: {quota_after['remaining']:,} / {quota_after['limit']:,}")

    if STOP_FLAG.is_set():
        logger.info(f"🛑 因額度限制提前結束 | 成功: {total_success} | 跳過: {total_skip}")
        logger.info("💾 進度已儲存至 sync_tracker.db，下次執行將自動接續")
    else:
        logger.info(f"🎉 全部完成 | 成功: {total_success} | 跳過: {total_skip}")
    logger.info("=" * 60)

    session.close()
    return total_success


# ========== CLI ==========
def main():
    parser = argparse.ArgumentParser(description="V4.2 全市場資料下載")
    parser.add_argument(
        "--mode", choices=["update", "backfill"], default="update",
        help="update=抓最新, backfill=歷史回補"
    )
    parser.add_argument("--batch-size", type=int, default=30, help="每批次天數")
    parser.add_argument(
        "--datasets", type=str, default=None,
        help="指定資料集 (逗號分隔), 例如: TaiwanStockPriceAdj,TaiwanFuturesInstitutionalInvestors"
    )
    parser.add_argument(
        "--until-done", action="store_true",
        help="持續執行直到補全所有缺失日期"
    )
    args = parser.parse_args()

    target = args.datasets.split(",") if args.datasets else None
    run_fetch(mode=args.mode, batch_size=args.batch_size, target_datasets=target, until_done=args.until_done)


if __name__ == "__main__":
    main()
