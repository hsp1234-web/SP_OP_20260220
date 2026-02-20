import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent))

os.environ["FINMIND_API_TOKEN"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0yMCAxNjoxODowNSIsInVzZXJfaWQiOiJmaW5taW5kaHNwIiwiaXAiOiIxMTQuNDcuMTk0LjEzIiwiZXhwIjoxNzcyMTgwMjg1fQ.A_YwaoTK_e3GEO4aLyMpKxR7xC7CIIjUbVVxhwZkTEk"

import polars as pl
from fetchers.infrastructure.http_session import get_session
from fetchers.parsers.finmind_extractor import extract_and_cast

def fetch_data(dataset: str, data_id: str, date: str) -> pl.DataFrame:
    session = get_session()
    print(f"Fetching {dataset} ({data_id}) for {date}...")
    try:
        res_json = session.get_data(
            dataset=dataset,
            data_id=data_id,
            start_date=date,
            end_date=date
        )
        msg = res_json.get("msg", "")
        if msg != "success":
            print(f"API Error for {dataset}: {msg}")
            
        df = extract_and_cast(res_json, dataset)
        print(f"Loaded {len(df)} rows for {dataset}.")
        return df
    except Exception as e:
        print(f"Failed to fetch {dataset}: {e}")
        return pl.DataFrame()

def main():
    date = "2024-05-02"  # 挑選一個最近的交易日
    
    print("--- 開始抓取資料 ---")
    # 抓取大台 (TX)
    df_tx = fetch_data("TaiwanFuturesTick", "TX", date)
    # 抓取小台 (MTX)
    df_mtx = fetch_data("TaiwanFuturesTick", "MTX", date)
    # 抓取選擇權 (TXO)
    df_options = fetch_data("TaiwanOptionTick", "TXO", date)
    
    print("\n--- 資料分析與合約代碼觀察 ---")
    if not df_tx.is_empty():
        print(f"\n[大台 TX] 總筆數: {len(df_tx)}")
        if "futures_id" in df_tx.columns:
            print(f"包含的合約代碼 (futures_id): {df_tx['futures_id'].unique().to_list()}")
        print("資料範例:")
        print(df_tx.head(3))
        
    if not df_mtx.is_empty():
        print(f"\n[小台 MTX] 總筆數: {len(df_mtx)}")
        if "futures_id" in df_mtx.columns:
            print(f"包含的合約代碼 (futures_id): {df_mtx['futures_id'].unique().to_list()}")
        print("資料範例:")
        print(df_mtx.head(3))
        
        print(f"\n[選擇權 TXO] 總筆數: {len(df_options)}")
        if "option_id" in df_options.columns:
            options_list = df_options['option_id'].unique().to_list()
            print(f"包含的合約代碼 (option_id) [前20個]: {options_list[:20]}")
        if "contract_date" in df_options.columns:
            contract_dates = df_options['contract_date'].unique().to_list()
            print(f"包含的到期月份 (contract_date): {contract_dates}")
        print("資料範例:")
        print(df_options.head(3))

if __name__ == "__main__":
    main()
