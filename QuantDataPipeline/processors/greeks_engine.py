import numpy as np
from numba import vectorize, float64, jit
import math
import polars as pl
import logging

logger = logging.getLogger("pipeline.greeks")

# -------------------------------------------------------------
# NUMBA NATIVE MATH FUNCTIONS FOR MAX SPEED
# -------------------------------------------------------------

@vectorize([float64(float64)])
def _numba_norm_cdf(x):
    """標準常態分配 CDF 的 Numba 高速版"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

@vectorize([float64(float64)])
def _numba_norm_pdf(x):
    """標準常態分配 PDF 的 Numba 高速版"""
    return math.exp(-0.5 * x**2) / math.sqrt(2.0 * math.pi)

# -------------------------------------------------------------
# CORE BSM PRICER AND GREEKS CALCULATOR (VECTORIZED)
# -------------------------------------------------------------

@jit(nopython=True, cache=True)
def _bsm_iv_and_greeks_kernel(S, K, T, r, option_price, is_call):
    """
    核心演算 Kernel：使用牛頓法/二分逼近反推 IV，並給出所有一階、二階 Greeks。
    防呆機制：若時間 T<=0，則回傳全部 0，防止除以零。
    """
    n = len(S)
    
    # 初始化輸出陣列
    out_iv = np.zeros(n, dtype=np.float64)
    out_delta = np.zeros(n, dtype=np.float64)
    out_gamma = np.zeros(n, dtype=np.float64)
    out_vega = np.zeros(n, dtype=np.float64)
    out_theta = np.zeros(n, dtype=np.float64)
    out_vanna = np.zeros(n, dtype=np.float64)
    out_charm = np.zeros(n, dtype=np.float64)
    
    for i in range(n):
        s = S[i]
        k = K[i]
        t = T[i]
        rate = r[i]
        p = option_price[i]
        call_flag = is_call[i]
        
        # 1. 時間或報價防呆
        if t <= 0.00001 or s <= 0 or k <= 0 or p <= 0:
            continue
            
        # 2. 逼近法計算 IV (簡單的二分逼近，範圍 1% ~ 300%)
        # 實務上牛頓法在這裡不穩定 (特別是深度價外)，二分法反而更保證收斂
        sigma_low = 0.0001
        sigma_high = 3.0
        sigma = 0.2  # 預設起點
        
        for _ in range(40): # 最大跌代次數
            sigma = (sigma_low + sigma_high) / 2.0
            
            d1 = (math.log(s / k) + (rate + 0.5 * sigma**2) * t) / (sigma * math.sqrt(t))
            d2 = d1 - sigma * math.sqrt(t)
            
            if call_flag:
                # Call 定價
                price_est = s * _numba_norm_cdf(d1) - k * math.exp(-rate * t) * _numba_norm_cdf(d2)
            else:
                # Put 定價
                price_est = k * math.exp(-rate * t) * _numba_norm_cdf(-d2) - s * _numba_norm_cdf(-d1)
                
            diff = price_est - p
            
            if abs(diff) < 1e-4:
                break
            elif diff > 0:
                sigma_high = sigma
            else:
                sigma_low = sigma
                
        # 儲存逼近出的 IV
        out_iv[i] = sigma
        
        # 3. 再用求出來的 IV 進行 Greeks 嚴格計算
        d1 = (math.log(s / k) + (rate + 0.5 * sigma**2) * t) / (sigma * math.sqrt(t))
        d2 = d1 - sigma * math.sqrt(t)
        
        pdf_d1 = _numba_norm_pdf(d1)
        cdf_d1 = _numba_norm_cdf(d1)
        cdf_n_d1 = _numba_norm_cdf(-d1)
        cdf_d2 = _numba_norm_cdf(d2)
        cdf_n_d2 = _numba_norm_cdf(-d2)
        
        # Gamma & Vega 買賣權都一樣
        gamma = pdf_d1 / (s * sigma * math.sqrt(t))
        vega = s * pdf_d1 * math.sqrt(t) / 100.0 # 換算為 1% 的變動影響
        
        if call_flag:
            delta = cdf_d1
            theta = (- (s * pdf_d1 * sigma) / (2 * math.sqrt(t)) - rate * k * math.exp(-rate * t) * cdf_d2) / 365.0
            # Vanna = d(Delta)/d(Vol) = -Vega/S * d1/sigma (近似公式)
            vanna = -pdf_d1 * d2 / sigma
            # Charm = -d(Delta)/d(T)
            charm = pdf_d1 * (rate / (sigma * math.sqrt(t)) - d2 / (2 * t))
        else:
            delta = cdf_d1 - 1.0
            theta = (- (s * pdf_d1 * sigma) / (2 * math.sqrt(t)) + rate * k * math.exp(-rate * t) * cdf_n_d2) / 365.0
            vanna = -pdf_d1 * d2 / sigma
            charm = pdf_d1 * (rate / (sigma * math.sqrt(t)) - d2 / (2 * t))
            
        out_delta[i] = delta
        out_gamma[i] = gamma
        out_vega[i] = vega
        out_theta[i] = theta
        out_vanna[i] = vanna
        out_charm[i] = charm
        
    return out_iv, out_delta, out_gamma, out_vega, out_theta, out_vanna, out_charm

# -------------------------------------------------------------
# POLARS DATAFRAME INTEGRATION
# -------------------------------------------------------------

def calculate_greeks(df: pl.DataFrame, r: float = 0.015) -> pl.DataFrame:
    """
    接收對齊後的 DataFrame (L2)，無狀態地產生包含 IV 與 Greeks 的超級 DataFrame。
    必須存在的欄位: Underlying_S, ExercisePrice, Years_to_Maturity, price, PutCall
    """
    if df.is_empty():
         return df

    logger.info(f"開始無狀態計算 {len(df)} 筆選擇權的 IV 與 Greeks...")

    # 將 Polars Series 轉為 NumPy 供 Numba 加速計算
    S = df["Underlying_S"].fill_null(0.0).cast(pl.Float64).to_numpy()
    K = df["ExercisePrice"].fill_null(0.0).cast(pl.Float64).to_numpy()
    T = df["Years_to_Maturity"].fill_null(0.0).cast(pl.Float64).to_numpy()
    P = df["price"].fill_null(0.0).cast(pl.Float64).to_numpy()
    
    # 判斷是 Call 還是 Put
    is_call = (df["PutCall"] == "C").to_numpy()
    
    # 準備無風險利率陣列 (可以改成動態，這邊先傳入常數)
    R = np.full_like(S, r)

    # 進入 Numba 加速 Kernel
    iv, delta, gamma, vega, theta, vanna, charm = _bsm_iv_and_greeks_kernel(S, K, T, R, P, is_call)

    # 將結果掛載回原本的 DataFrame
    df_result = df.with_columns([
        pl.Series("IV", iv),
        pl.Series("Delta", delta),
        pl.Series("Gamma", gamma),
        pl.Series("Vega", vega),
        pl.Series("Theta", theta),
        pl.Series("Vanna", vanna),
        pl.Series("Charm", charm)
    ])
    
    logger.info("Greeks 計算完成！")
    return df_result
