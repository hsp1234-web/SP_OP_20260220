import polars as pl
import numpy as np
import time
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.append(str(Path(__file__).parent))

from processors.greeks_engine import calculate_greeks

def test_greeks():
    print("--- 準備假資料 (模擬已對齊的 L2 DataFrame) ---")
    data = {
        "date": [pl.Datetime("us"), pl.Datetime("us"), pl.Datetime("us"), pl.Datetime("us")],
        "PutCall": ["C", "P", "C", "P"],
        "ExercisePrice": [18000.0, 18000.0, 20000.0, 20000.0],
        "Underlying_S": [18500.0, 18500.0, 19500.0, 19500.0],
        "Years_to_Maturity": [0.05, 0.05, 0.1, 0.1],
        "price": [600.0, 50.0, 200.0, 600.0]
    }
    df = pl.DataFrame(data)
    print("輸入欄位如下:")
    print(df)
    
    print("\n--- 驅動 Numba 加速運算引擎 ---")
    start = time.perf_counter()
    # 第一次執行 Numba 會進行 JIT 編譯 (Just-In-Time Compilation)
    df_out = calculate_greeks(df, r=0.015)
    end = time.perf_counter()
    print(f"✅ 計算成功！花費時間: {end - start:.4f} 秒")
    
    print("\n--- 輸出含 Greeks 的超級特徵表 ---")
    print(df_out.select([
        "PutCall", "ExercisePrice", "Underlying_S", "price", 
        "IV", "Delta", "Gamma", "Vega", "Vanna", "Charm"
    ]))

if __name__ == "__main__":
    test_greeks()
