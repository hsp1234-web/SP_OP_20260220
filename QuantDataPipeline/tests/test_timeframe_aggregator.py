"""
test_timeframe_aggregator.py — 多週期降採樣測試

測試重點:
  1. 1m 聚合正確 OHLCV
  2. 多週期產出
  3. 雙表結構驗證
  4. 空 DataFrame 防呆
"""
import sys
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from processors.timeframe_aggregator import (
    aggregate_contract_bars,
    aggregate_market_summary,
    process_all_timeframes,
    TIMEFRAME_MAP,
)


class TestContractBars:
    """表A: 合約級 K 線表測試"""

    def test_1m_aggregation_produces_ohlcv(self, sample_greeks_df):
        """1m 聚合應產出 open/high/low/close 欄位"""
        result = aggregate_contract_bars(sample_greeks_df, "1m")

        assert not result.is_empty()
        for col in ["open", "high", "low", "close"]:
            assert col in result.columns, f"缺少 {col} 欄位"

    def test_1m_has_trade_count(self, sample_greeks_df):
        """聚合後應有 trade_count"""
        result = aggregate_contract_bars(sample_greeks_df, "1m")
        assert "trade_count" in result.columns

    def test_greeks_last_values(self, sample_greeks_df):
        """Greeks 應取 last 值"""
        result = aggregate_contract_bars(sample_greeks_df, "1m")
        for g in ["IV", "Delta", "Gamma", "Vega"]:
            assert g in result.columns, f"缺少 Greeks 欄位 {g}"

    def test_high_gte_low(self, sample_greeks_df):
        """high 應 >= low"""
        result = aggregate_contract_bars(sample_greeks_df, "1m")
        if "high" in result.columns and "low" in result.columns:
            highs = result["high"].to_list()
            lows = result["low"].to_list()
            for h, l in zip(highs, lows):
                if h is not None and l is not None:
                    assert h >= l

    def test_1h_aggregation(self, sample_greeks_df):
        """1h 聚合應正常運作"""
        result = aggregate_contract_bars(sample_greeks_df, "1h")
        assert not result.is_empty()
        # 1h 的 bar 應比 1m 少
        result_1m = aggregate_contract_bars(sample_greeks_df, "1m")
        assert len(result) <= len(result_1m)

    def test_4h_and_1d(self, sample_greeks_df):
        """4h 和 1d 聚合應正常運作"""
        r4h = aggregate_contract_bars(sample_greeks_df, "4h")
        r1d = aggregate_contract_bars(sample_greeks_df, "1d")
        assert not r4h.is_empty()
        assert not r1d.is_empty()
        assert len(r1d) <= len(r4h)

    def test_empty_df_handling(self):
        """空 DataFrame 應安全處理"""
        empty_df = pl.DataFrame({"date": [], "price": []}).with_columns(
            pl.col("date").cast(pl.Datetime("ns"))
        )
        result = aggregate_contract_bars(empty_df, "1m")
        assert result.is_empty()

    def test_invalid_timeframe_raises(self, sample_greeks_df):
        """不支援的時間週期應拋出 ValueError"""
        with pytest.raises(ValueError, match="不支援"):
            aggregate_contract_bars(sample_greeks_df, "12h")


class TestMarketSummary:
    """表B: 全市場微觀摘要表測試"""

    def test_1m_summary_has_features(self, sample_greeks_df):
        """市場摘要應包含核心特徵"""
        result = aggregate_market_summary(sample_greeks_df, "1m")
        assert not result.is_empty()

        for col in ["RV", "IV_Skew", "Net_GEX", "PCR_Volume"]:
            assert col in result.columns, f"缺少特徵 {col}"

    def test_summary_has_tick_count(self, sample_greeks_df):
        """摘要應有 tick_count"""
        result = aggregate_market_summary(sample_greeks_df, "1m")
        assert "tick_count" in result.columns

    def test_1d_has_vrp(self, sample_greeks_df):
        """日級摘要應包含 VRP_Daily"""
        result = aggregate_market_summary(sample_greeks_df, "1d")
        assert "VRP_Daily" in result.columns

    def test_1m_no_vrp(self, sample_greeks_df):
        """1m 摘要不應包含 VRP_Daily"""
        result = aggregate_market_summary(sample_greeks_df, "1m")
        assert "VRP_Daily" not in result.columns

    def test_empty_df_returns_empty(self):
        """空 DataFrame 應回傳空結果"""
        empty_df = pl.DataFrame()
        result = aggregate_market_summary(empty_df, "1m")
        assert result.is_empty()


class TestProcessAllTimeframes:
    """全週期聚合測試"""

    def test_outputs_all_four_timeframes(self, sample_greeks_df):
        """應產出 4 個週期的結果"""
        results = process_all_timeframes(sample_greeks_df, "2024-05-02")
        assert len(results) == 4
        for tf in ["1m", "1h", "4h", "1d"]:
            assert tf in results

    def test_each_result_is_tuple_of_two_dfs(self, sample_greeks_df):
        """每個週期應回傳 (contract_bars, market_summary) 二元組"""
        results = process_all_timeframes(sample_greeks_df, "2024-05-02")
        for tf, (bars, summary) in results.items():
            assert isinstance(bars, pl.DataFrame), f"{tf} bars 非 DataFrame"
            assert isinstance(summary, pl.DataFrame), f"{tf} summary 非 DataFrame"

    def test_empty_df_returns_empty_dict(self):
        """空 DataFrame 應回傳空 dict"""
        result = process_all_timeframes(pl.DataFrame())
        assert result == {}
