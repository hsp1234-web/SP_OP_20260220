import os
import sys
import time
import psutil
from datetime import datetime, timedelta
from pathlib import Path
import traceback

sys.path.append(str(Path(__file__).parent))
os.environ["FINMIND_API_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0yMCAxNjoxODowNSIsInVzZXJfaWQiOiJmaW5taW5kaHNwIiwiaXAiOiIxMTQuNDcuMTk0LjEzIiwiZXhwIjoxNzcyMTgwMjg1fQ.A_YwaoTK_e3GEO4aLyMpKxR7xC7CIIjUbVVxhwZkTEk"
os.environ["POLARS_MAX_THREADS"] = "1"

import polars as pl
from fetchers.infrastructure.http_session import get_session
from core.db_metadata_manager import get_db_manager
from fetchers.infrastructure.backoff_retry import exponential_backoff

EXPIRATION_MAP = {
    "202404W1": "2024-04-03 13:30:00",
    "202404W2": "2024-04-10 13:30:00",
    "202404":   "2024-04-17 13:30:00",
    "202404W4": "2024-04-24 13:30:00",
    "202405W1": "2024-05-01 13:30:00",
    "202405W2": "2024-05-08 13:30:00",
    "202405":   "2024-05-15 13:30:00",
    "202406":   "2024-06-19 13:30:00",
    "202407":   "2024-07-17 13:30:00",
    "202409":   "2024-09-18 13:30:00",
    "202412":   "2024-12-18 13:30:00",
}

def map_contract_date(date_str: str) -> str:
    return EXPIRATION_MAP.get(date_str, "2024-12-31 13:30:00")

@exponential_backoff(retries=5, base_delay=2.0)
def fetch_raw_data_with_retry(dataset: str, data_id: str, date: str) -> pl.DataFrame:
    session = get_session()
    res_json = session.get_data(dataset=dataset, data_id=data_id, start_date=date, end_date=date)
    msg = res_json.get("msg", "")
    if msg != "success":
        # Raise exception to trigger backoff
        raise ValueError(f"API Error: {msg}")
        
    data_list = res_json.get("data", [])
    if data_list:
        return pl.from_dicts(data_list)
    return pl.DataFrame()

def process_single_day(date_str: str) -> dict:
    print(f"\n[{date_str}] 開始處理 (L1 下載 + L2 對齊)...")
    day_start_time = time.perf_counter()
    
    # L1 下載階段
    print(f"  -> 抓取 {date_str} 的 MTX 與 TXO...")
    df_mtx_raw = fetch_raw_data_with_retry("TaiwanFuturesTick", "MTX", date_str)
    df_txo_raw = fetch_raw_data_with_retry("TaiwanOptionTick", "TXO", date_str)
    
    if df_mtx_raw.is_empty() or df_txo_raw.is_empty():
        print(f"  -> {date_str} 抓取到空資料，跳過。")
        return {"status": "skipped", "time": time.perf_counter() - day_start_time, "rows": 0}

    print(f"  -> L1 完成：小台 {len(df_mtx_raw)} 筆，選擇權 {len(df_txo_raw)} 筆。開始 L2 運算...")
    
    # L2 對齊階段
    try:
        df_mtx = df_mtx_raw.with_columns(
            pl.col("date").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S.%f", strict=False)
        ).sort("date")
        
        df_txo = df_txo_raw.with_columns(
            pl.col("date").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S.%f", strict=False)
        ).sort("date")

        df_txo = df_txo.with_columns(
            pl.col("contract_date").map_elements(map_contract_date, return_dtype=pl.Utf8).alias("expire_str")
        ).with_columns(
            pl.col("expire_str").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )
        
        df_txo = df_txo.with_columns(
            Days_to_Maturity=((pl.col("expire_str") - pl.col("date")).dt.total_milliseconds() / (1000 * 60 * 60 * 24)),
        ).with_columns(
            Years_to_Maturity=(pl.col("Days_to_Maturity") / 365.0)
        )

        # 簡單過濾出主力合約 (假設以年月為準，此為簡易寫法，實務會動態找最大量)
        main_contract = date_str[:4] + date_str[5:7] # ex: 202405
        df_mtx_main = df_mtx.filter(pl.col("contract_date") == main_contract).select([
            pl.col("date"),
            pl.col("price").alias("Underlying_S")
        ])

        if df_mtx_main.is_empty():
             df_mtx_main = df_mtx.select([pl.col("date"), pl.col("price").alias("Underlying_S")]) # fallback
             
        df_aligned = df_txo.join_asof(
            df_mtx_main,
            on="date",
            strategy="backward"
        )
        
        rows = len(df_aligned)
        print(f"  -> L2 完成！共產出 {rows} 筆超級特徵。")
        
    except Exception as e:
        print(f"  -> 處理錯誤: {e}")
        return {"status": "error", "time": time.perf_counter() - day_start_time, "rows": 0}

    day_time = time.perf_counter() - day_start_time
    return {"status": "success", "time": day_time, "rows": rows}

def main():
    print("=== 量化數據管線自動化與防禦機制測試 (倒序排程 10 天) ===")
    
    # 1. 取得交易日並倒序 (模擬從今天往回算)
    # 我們假設從 2024-05-02 往回算 10 個交易日
    end_date = "2024-05-02"
    start_date = "2024-04-10"
    
    session = get_session()
    print("正在向交易所查詢交易日歷...")
    res = session.get_data("TaiwanStockTradingDate", start_date=start_date, end_date=end_date)
    trading_dates = [d["date"] for d in res.get("data", [])]
    
    # **關鍵：倒序排序，從最新往舊的處理**
    trading_dates.sort(reverse=True)
    target_dates = trading_dates[:10] # 取最近 10 天
    
    print(f"計畫處理的交易日 (新到舊): {target_dates}")
    
    total_start_time = time.perf_counter()
    process = psutil.Process(os.getpid())
    start_cpu_times = process.cpu_times()
    
    total_rows = 0
    results = {}
    
    # 依序處理這 10 天
    for d in target_dates:
        res_info = process_single_day(d)
        results[d] = res_info
        total_rows += res_info["rows"]
        
    # 結算時間
    total_end_time = time.perf_counter()
    end_cpu_times = process.cpu_times()
    wall_time = total_end_time - total_start_time
    cpu_time = (end_cpu_times.user - start_cpu_times.user) + (end_cpu_times.system - start_cpu_times.system)
    
    print("\n=== 10 天批次處理完整報告 ===")
    print(f"總處理天數: {len(target_dates)} 天")
    print(f"總產出超級特徵筆數: {total_rows} 筆")
    print(f"整體花費時間 (含 API Rate Limit): {wall_time:.2f} 秒 ({wall_time/60:.2f} 分鐘)")
    print(f"實際 CPU 運算時間: {cpu_time:.2f} 秒")
    print("-------------------------")
    for d, info in results.items():
        print(f"日期: {d} | 狀態: {info['status']} | 耗時: {info['time']:.2f}秒 | 產出筆數: {info['rows']}")

if __name__ == "__main__":
    main()
