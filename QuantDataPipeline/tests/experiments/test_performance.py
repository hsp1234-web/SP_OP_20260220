import os
import sys
import time
import psutil
from pathlib import Path
import traceback

sys.path.append(str(Path(__file__).parent))
os.environ["FINMIND_API_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0yMCAxNjoxODowNSIsInVzZXJfaWQiOiJmaW5taW5kaHNwIiwiaXAiOiIxMTQuNDcuMTk0LjEzIiwiZXhwIjoxNzcyMTgwMjg1fQ.A_YwaoTK_e3GEO4aLyMpKxR7xC7CIIjUbVVxhwZkTEk"

import polars as pl
from fetchers.infrastructure.http_session import get_session

def fetch_raw_data(dataset: str, data_id: str, date: str) -> pl.DataFrame:
    session = get_session()
    res_json = session.get_data(dataset=dataset, data_id=data_id, start_date=date, end_date=date)
    data_list = res_json.get("data", [])
    if data_list:
        return pl.from_dicts(data_list)
    return pl.DataFrame()

# 結算日對照表 (為了測試效能，我們先寫死 2024-05-02 盤面上會遇到的合約的絕對結算時間)
EXPIRATION_MAP = {
    "202405W1": "2024-05-01 13:30:00", # 已結算 (理論上不該有，但有些過期報價可能殘留)
    "202405W2": "2024-05-08 13:30:00",
    "202405":   "2024-05-15 13:30:00",
    "202406":   "2024-06-19 13:30:00",
    "202407":   "2024-07-17 13:30:00",
    "202409":   "2024-09-18 13:30:00",
    "202412":   "2024-12-18 13:30:00"
}

def map_contract_date(date_str: str) -> str:
    return EXPIRATION_MAP.get(date_str, "2024-12-31 13:30:00")

def main():
    date = "2024-05-02"
    print("--- [階段 1] 網路下載原始資料 (不計入運算時間) ---")
    df_mtx_raw = fetch_raw_data("TaiwanFuturesTick", "MTX", date)
    df_txo_raw = fetch_raw_data("TaiwanOptionTick", "TXO", date)
    print(f"小台原始筆數: {len(df_mtx_raw)}, 選擇權原始筆數: {len(df_txo_raw)}")
    
    if df_mtx_raw.is_empty() or df_txo_raw.is_empty():
        print("資料下載失敗！")
        return

    print("\n--- [階段 2] 核心 CPU 運算與對齊開始 ---")
    
    # 限制 Polars 只能使用單核心運算 (模擬極端受限環境)
    os.environ["POLARS_MAX_THREADS"] = "1"
    
    # 紀錄初始資源
    process = psutil.Process(os.getpid())
    start_cpu_times = process.cpu_times()
    start_mem = process.memory_info().rss / (1024 * 1024) # MB
    start_time = time.perf_counter()

    try:
        # 1. 時間格式轉換 (字串轉奈秒 Timestamp)
        df_mtx = df_mtx_raw.with_columns(
            pl.col("date").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S.%f", strict=False)
        ).sort("date")
        
        df_txo = df_txo_raw.with_columns(
            pl.col("date").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S.%f", strict=False)
        ).sort("date")

        # 2. 計算剩餘時間 (Days & Years 兩個欄位並存，如您所建議！)
        # 把到期日 map 上去
        df_txo = df_txo.with_columns(
            pl.col("contract_date").map_elements(map_contract_date, return_dtype=pl.Utf8).alias("expire_str")
        ).with_columns(
            pl.col("expire_str").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )
        
        df_txo = df_txo.with_columns(
            # 計算毫秒差，再換算成天數
            Days_to_Maturity=((pl.col("expire_str") - pl.col("date")).dt.total_milliseconds() / (1000 * 60 * 60 * 24)),
        ).with_columns(
            # 同時保留年化時間供數學引擎使用
            Years_to_Maturity=(pl.col("Days_to_Maturity") / 365.0)
        )

        # 3. 處理 Asof Join 對齊 (周選與月選都對齊當月小台主力)
        # 為簡化邏輯，這天的主力合約是 202405，我們過濾期貨只留 202405
        df_mtx_main = df_mtx.filter(pl.col("contract_date") == "202405").select([
            pl.col("date"),
            pl.col("price").alias("Underlying_S"),
            pl.col("volume").alias("MTX_Volume")
        ])

        # 執行 Asof Join 向後對齊
        # 這會幫每一筆 TXO 找到時間點小於等於它的最新一筆 MTX 價格
        df_aligned = df_txo.join_asof(
            df_mtx_main,
            on="date",
            strategy="backward"
        )

        # 確保運算完成
        aligned_count = len(df_aligned)
        sample_result = df_aligned.select(["date", "contract_date", "ExercisePrice", "price", "Underlying_S", "Days_to_Maturity", "Years_to_Maturity"]).head(5)

    except Exception as e:
        print(f"運算發生錯誤: {e}")
        traceback.print_exc()
        return

    # 計算資源消耗
    end_time = time.perf_counter()
    end_cpu_times = process.cpu_times()
    end_mem = process.memory_info().rss / (1024 * 1024) # MB

    wall_time = end_time - start_time
    # User CPU time 是實際 CPU 運算花費的總秒數
    cpu_time = (end_cpu_times.user - start_cpu_times.user) + (end_cpu_times.system - start_cpu_times.system)
    mem_used = end_mem - start_mem

    print("\n--- [階段 3] 資源消耗與運算結果 ---")
    print(f"最終超級特徵表筆數: {aligned_count}")
    print(f"真實經過時間 (Wall Time): {wall_time:.4f} 秒")
    print(f"總 CPU 運算時間 (單核極限): {cpu_time:.4f} 秒")
    print(f"峰值記憶體增加: {mem_used:.2f} MB")
    print("\n[資料範例 - 對齊標的價與雙時間特徵]")
    print(sample_result)

if __name__ == "__main__":
    main()
