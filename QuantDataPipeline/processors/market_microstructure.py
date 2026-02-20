"""
market_microstructure.py — 全市場微觀摘要特徵 (表B Alpha 核心)

負責計算每個時間窗口的市場級摘要特徵：
- RV (Realized Volatility)
- IV_Skew / IV_Slope_25D / IV_Curvature
- Net_GEX (全市場淨 Gamma 曝險)
- PCR_Volume (看跌看漲比)
- VRP_Daily (僅日級)
"""
import polars as pl
import numpy as np
import logging

logger = logging.getLogger("pipeline.microstructure")


def calculate_pcr_volume(df: pl.DataFrame) -> float:
    """
    計算 Put/Call 成交量比率。
    PCR > 1 表示看空情緒, PCR < 1 表示看多情緒。
    """
    if df.is_empty() or "PutCall" not in df.columns:
        return float("nan")

    vol_col = "volume" if "volume" in df.columns else None
    if vol_col:
        put_vol = df.filter(pl.col("PutCall") == "P")[vol_col].sum()
        call_vol = df.filter(pl.col("PutCall") == "C")[vol_col].sum()
    else:
        # 以筆數代替
        put_vol = df.filter(pl.col("PutCall") == "P").height
        call_vol = df.filter(pl.col("PutCall") == "C").height

    if call_vol == 0 or call_vol is None:
        return float("nan")
    return put_vol / call_vol


def calculate_net_gex(df: pl.DataFrame) -> float:
    """
    計算全市場淨 Gamma 曝險 (Net GEX)。
    GEX = Σ(Gamma * OI_or_Volume * S² * 0.01)
    Call GEX 為正, Put GEX 為負, 取淨值。
    """
    required = {"Gamma", "Underlying_S", "PutCall"}
    if df.is_empty() or not required.issubset(set(df.columns)):
        return float("nan")

    vol_col = "volume" if "volume" in df.columns else None

    # 計算 GEX per tick
    if vol_col:
        gex = (
            df.with_columns([
                (pl.col("Gamma") * pl.col(vol_col) * pl.col("Underlying_S").pow(2) * 0.01)
                .alias("raw_gex")
            ])
        )
    else:
        gex = (
            df.with_columns([
                (pl.col("Gamma") * pl.col("Underlying_S").pow(2) * 0.01)
                .alias("raw_gex")
            ])
        )

    # Call 為正, Put 為負
    gex = gex.with_columns([
        pl.when(pl.col("PutCall") == "C")
        .then(pl.col("raw_gex"))
        .otherwise(-pl.col("raw_gex"))
        .alias("signed_gex")
    ])

    return gex["signed_gex"].sum()


def _find_atm_iv(df: pl.DataFrame) -> dict:
    """
    找出 ATM (最接近標的價格) 的 IV。
    回傳 {"atm_call_iv": float, "atm_put_iv": float, "atm_strike": float}
    """
    required = {"ExercisePrice", "Underlying_S", "IV", "PutCall"}
    if df.is_empty() or not required.issubset(set(df.columns)):
        return {"atm_call_iv": float("nan"), "atm_put_iv": float("nan"), "atm_strike": float("nan")}

    # 取最後一筆標的價格為 ATM 基準
    last_s = df["Underlying_S"][-1]

    # ATM = 距離 S 最近的 strike
    df_with_dist = df.with_columns([
        (pl.col("ExercisePrice") - last_s).abs().alias("atm_dist")
    ])

    atm_strike = df_with_dist.sort("atm_dist")["ExercisePrice"][0]

    # 取 ATM strike 的 Call/Put IV (取最近一筆)
    atm_calls = df_with_dist.filter(
        (pl.col("ExercisePrice") == atm_strike) & (pl.col("PutCall") == "C")
    )
    atm_puts = df_with_dist.filter(
        (pl.col("ExercisePrice") == atm_strike) & (pl.col("PutCall") == "P")
    )

    atm_call_iv = atm_calls["IV"][-1] if not atm_calls.is_empty() else float("nan")
    atm_put_iv = atm_puts["IV"][-1] if not atm_puts.is_empty() else float("nan")

    return {"atm_call_iv": atm_call_iv, "atm_put_iv": atm_put_iv, "atm_strike": atm_strike}


def _find_25d_iv(df: pl.DataFrame) -> dict:
    """
    找出 25 Delta 的 Put/Call IV。
    25D Put: Delta 最接近 -0.25 的 OTM Put
    25D Call: Delta 最接近 0.25 的 OTM Call
    """
    required = {"Delta", "IV", "PutCall"}
    if df.is_empty() or not required.issubset(set(df.columns)):
        return {"iv_25d_put": float("nan"), "iv_25d_call": float("nan")}

    # 25D Put (Delta ~ -0.25)
    puts = df.filter(pl.col("PutCall") == "P")
    if not puts.is_empty():
        puts_with_dist = puts.with_columns([
            (pl.col("Delta") + 0.25).abs().alias("d25_dist")
        ])
        iv_25d_put = puts_with_dist.sort("d25_dist")["IV"][0]
    else:
        iv_25d_put = float("nan")

    # 25D Call (Delta ~ 0.25)
    calls = df.filter(pl.col("PutCall") == "C")
    if not calls.is_empty():
        calls_with_dist = calls.with_columns([
            (pl.col("Delta") - 0.25).abs().alias("d25_dist")
        ])
        iv_25d_call = calls_with_dist.sort("d25_dist")["IV"][0]
    else:
        iv_25d_call = float("nan")

    return {"iv_25d_put": iv_25d_put, "iv_25d_call": iv_25d_call}


def calculate_iv_skew(df: pl.DataFrame) -> float:
    """IV_Skew = ATM_Put_IV - ATM_Call_IV"""
    atm = _find_atm_iv(df)
    return atm["atm_put_iv"] - atm["atm_call_iv"]


def calculate_iv_slope_25d(df: pl.DataFrame) -> float:
    """IV_Slope_25D = IV(25D Put) - IV(ATM)"""
    atm = _find_atm_iv(df)
    d25 = _find_25d_iv(df)
    atm_iv = (atm["atm_call_iv"] + atm["atm_put_iv"]) / 2.0
    return d25["iv_25d_put"] - atm_iv


def calculate_iv_curvature(df: pl.DataFrame) -> float:
    """IV_Curvature = IV(25D Put) + IV(25D Call) - 2*ATM_IV"""
    atm = _find_atm_iv(df)
    d25 = _find_25d_iv(df)
    atm_iv = (atm["atm_call_iv"] + atm["atm_put_iv"]) / 2.0
    return d25["iv_25d_put"] + d25["iv_25d_call"] - 2.0 * atm_iv


def calculate_rv(df: pl.DataFrame) -> float:
    """
    Realized Volatility = sqrt(sum(log_return²))
    基於期間內標的價格 (Underlying_S) 的對數報酬率。
    """
    if df.is_empty() or "Underlying_S" not in df.columns:
        return float("nan")

    prices = df["Underlying_S"].drop_nulls()
    if len(prices) < 2:
        return float("nan")

    prices_np = prices.to_numpy().astype(np.float64)
    log_returns = np.diff(np.log(prices_np))
    return float(np.sqrt(np.sum(log_returns ** 2)))


def calculate_vrp_daily(df: pl.DataFrame) -> float:
    """
    VRP (Volatility Risk Premium) = ATM_IV² - RV_Daily
    僅在日級表中使用。正值表示市場存在波動率溢酬。
    """
    atm = _find_atm_iv(df)
    atm_iv = (atm["atm_call_iv"] + atm["atm_put_iv"]) / 2.0
    rv = calculate_rv(df)

    if np.isnan(atm_iv) or np.isnan(rv):
        return float("nan")

    return atm_iv ** 2 - rv


def compute_market_summary(df: pl.DataFrame, timeframe: str = "1m") -> dict:
    """
    計算完整的市場微觀摘要特徵。

    Args:
        df: 該時間窗口內的完整 Greeks DataFrame。
        timeframe: 當前週期 (用於判斷是否計算 VRP)。

    Returns:
        dict 包含所有特徵值
    """
    result = {
        "RV": calculate_rv(df),
        "IV_Skew": calculate_iv_skew(df),
        "IV_Slope_25D": calculate_iv_slope_25d(df),
        "IV_Curvature": calculate_iv_curvature(df),
        "Net_GEX": calculate_net_gex(df),
        "PCR_Volume": calculate_pcr_volume(df),
    }

    # VRP 僅日級表
    if timeframe == "1d":
        result["VRP_Daily"] = calculate_vrp_daily(df)

    return result
