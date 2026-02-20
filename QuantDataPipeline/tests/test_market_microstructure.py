"""
test_market_microstructure.py — 市場微觀特徵測試

測試重點:
  1. PCR_Volume 計算
  2. Net_GEX 計算
  3. IV_Skew / IV_Slope_25D / IV_Curvature
  4. RV (Realized Volatility) 計算
  5. VRP_Daily
"""
import sys
import math
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import numpy as np
from processors.market_microstructure import (
    calculate_pcr_volume,
    calculate_net_gex,
    calculate_iv_skew,
    calculate_iv_slope_25d,
    calculate_iv_curvature,
    calculate_rv,
    calculate_vrp_daily,
    compute_market_summary,
    _find_atm_iv,
    _find_25d_iv,
)


# ─────────────────────────────────────────────
# 輔助工具
# ─────────────────────────────────────────────

@pytest.fixture
def simple_option_df():
    """
    簡單的期權 DataFrame 用於單元測試。
    ATM strike=20000 同時有 Call 和 Put。
    已知 Call 50 筆 (30@ATM + 20@20100), Put 30 筆 (15@ATM + 15@19900)。
    """
    return pl.DataFrame({
        "PutCall": ["C"] * 30 + ["C"] * 20 + ["P"] * 15 + ["P"] * 15,
        "volume": [10] * 50 + [15] * 30,
        "ExercisePrice": [20000.0] * 30 + [20100.0] * 20 + [20000.0] * 15 + [19900.0] * 15,
        "Underlying_S": [20050.0] * 80,
        "price": [100.0] * 80,
        "IV": [0.20] * 30 + [0.22] * 20 + [0.23] * 15 + [0.25] * 15,
        "Delta": [0.50] * 30 + [0.25] * 20 + [-0.50] * 15 + [-0.25] * 15,
        "Gamma": [0.001] * 80,
    })


# ─────────────────────────────────────────────
# PCR 測試
# ─────────────────────────────────────────────

class TestPCRVolume:
    def test_pcr_calculation(self, simple_option_df):
        """PCR = Put_Volume / Call_Volume"""
        pcr = calculate_pcr_volume(simple_option_df)
        expected = (30 * 15) / (50 * 10)  # 450 / 500 = 0.9
        assert pcr == pytest.approx(expected, rel=0.01)

    def test_pcr_no_calls_returns_nan(self):
        """全為 Put 時 PCR 應為 nan"""
        df = pl.DataFrame({"PutCall": ["P"] * 10, "volume": [5] * 10})
        pcr = calculate_pcr_volume(df)
        assert math.isnan(pcr)

    def test_pcr_empty_df(self):
        """空 DataFrame 應回傳 nan"""
        pcr = calculate_pcr_volume(pl.DataFrame())
        assert math.isnan(pcr)

    def test_pcr_balanced(self):
        """Put/Call 等量時 PCR=1"""
        df = pl.DataFrame({
            "PutCall": ["C"] * 10 + ["P"] * 10,
            "volume": [100] * 20,
        })
        pcr = calculate_pcr_volume(df)
        assert pcr == pytest.approx(1.0)


# ─────────────────────────────────────────────
# GEX 測試
# ─────────────────────────────────────────────

class TestNetGEX:
    def test_gex_calculation(self, sample_greeks_df):
        """Net_GEX 應回傳數值 (不是 nan)"""
        gex = calculate_net_gex(sample_greeks_df)
        assert not math.isnan(gex)

    def test_gex_calls_positive_puts_negative(self):
        """Call GEX 為正, Put GEX 為負"""
        df_call = pl.DataFrame({
            "PutCall": ["C"],
            "Gamma": [0.01],
            "Underlying_S": [20000.0],
            "volume": [100],
        })
        df_put = pl.DataFrame({
            "PutCall": ["P"],
            "Gamma": [0.01],
            "Underlying_S": [20000.0],
            "volume": [100],
        })
        gex_call = calculate_net_gex(df_call)
        gex_put = calculate_net_gex(df_put)
        assert gex_call > 0
        assert gex_put < 0

    def test_gex_empty_df(self):
        """空 DataFrame 回傳 nan"""
        gex = calculate_net_gex(pl.DataFrame())
        assert math.isnan(gex)


# ─────────────────────────────────────────────
# IV 特徵測試
# ─────────────────────────────────────────────

class TestIVFeatures:
    def test_atm_iv_detection(self, simple_option_df):
        """ATM IV 應找到最接近標的價格的 strike"""
        atm = _find_atm_iv(simple_option_df)
        # Underlying_S = 20050, 最近 strike = 20000
        assert atm["atm_strike"] == 20000.0

    def test_iv_skew(self, simple_option_df):
        """IV_Skew = ATM_Put_IV - ATM_Call_IV"""
        skew = calculate_iv_skew(simple_option_df)
        # 不應為 nan
        assert not math.isnan(skew)

    def test_iv_slope_25d(self, simple_option_df):
        """IV_Slope_25D 應回傳數值"""
        slope = calculate_iv_slope_25d(simple_option_df)
        assert not math.isnan(slope)

    def test_iv_curvature(self, simple_option_df):
        """IV_Curvature 應回傳數值"""
        curv = calculate_iv_curvature(simple_option_df)
        assert not math.isnan(curv)

    def test_25d_detection(self, simple_option_df):
        """25D IV 應找到 Delta 最接近 ±0.25 的合約"""
        d25 = _find_25d_iv(simple_option_df)
        assert not math.isnan(d25["iv_25d_put"])
        assert not math.isnan(d25["iv_25d_call"])


# ─────────────────────────────────────────────
# RV 測試
# ─────────────────────────────────────────────

class TestRealizedVolatility:
    def test_rv_positive(self, sample_greeks_df):
        """RV 應為非負值"""
        rv = calculate_rv(sample_greeks_df)
        assert rv >= 0

    def test_rv_constant_price_is_zero(self):
        """若價格不變，RV 應為 0"""
        df = pl.DataFrame({"Underlying_S": [100.0] * 10})
        rv = calculate_rv(df)
        assert rv == pytest.approx(0.0)

    def test_rv_empty_df(self):
        """空 DataFrame 回傳 nan"""
        rv = calculate_rv(pl.DataFrame())
        assert math.isnan(rv)

    def test_rv_single_price(self):
        """只有一筆價格，RV 為 nan"""
        df = pl.DataFrame({"Underlying_S": [100.0]})
        rv = calculate_rv(df)
        assert math.isnan(rv)


# ─────────────────────────────────────────────
# VRP 測試
# ─────────────────────────────────────────────

class TestVRP:
    def test_vrp_daily(self, simple_option_df):
        """VRP_Daily 應回傳數值 (可為正或負)"""
        vrp = calculate_vrp_daily(simple_option_df)
        assert not math.isnan(vrp)


# ─────────────────────────────────────────────
# 整合測試
# ─────────────────────────────────────────────

class TestComputeMarketSummary:
    def test_summary_contains_all_keys(self, sample_greeks_df):
        """compute_market_summary 應回傳所有特徵 key"""
        result = compute_market_summary(sample_greeks_df, "1m")
        expected_keys = {"RV", "IV_Skew", "IV_Slope_25D", "IV_Curvature", "Net_GEX", "PCR_Volume"}
        assert expected_keys.issubset(set(result.keys()))

    def test_summary_1d_has_vrp(self, sample_greeks_df):
        """1d 模式應額外包含 VRP_Daily"""
        result = compute_market_summary(sample_greeks_df, "1d")
        assert "VRP_Daily" in result

    def test_summary_1m_no_vrp(self, sample_greeks_df):
        """1m 模式不應包含 VRP_Daily"""
        result = compute_market_summary(sample_greeks_df, "1m")
        assert "VRP_Daily" not in result
