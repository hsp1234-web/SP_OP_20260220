"""
timeframe_aggregator.py — 多週期降採樣與雙表聚合

將 Tick 級 Greeks DataFrame 聚合為多時間週期的雙表結構：
  表 A：合約級 K 線表 (Contract-Level Bars)
  表 B：全市場微觀摘要表 (Market-Level Summary)

支援的時間週期：1m, 1h, 4h, 1d
使用 Polars group_by_dynamic 實現高效聚合。
"""
import polars as pl
import logging
from typing import Dict, Tuple

from processors.market_microstructure import compute_market_summary

logger = logging.getLogger("pipeline.aggregator")

# 週期對應的 Polars duration 字串
TIMEFRAME_MAP = {
    "1m": "1m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}


def aggregate_contract_bars(df: pl.DataFrame, timeframe: str = "1m") -> pl.DataFrame:
    """
    表 A：合約級 K 線表

    以 [option_id, contract_date, PutCall, ExercisePrice] 為單位，
    對每個時間窗口聚合 OHLCV + Greeks 末值。

    Args:
        df: 含 Greeks 的完整 tick DataFrame (需含 date 欄位)
        timeframe: 目標時間週期 ("1m", "1h", "4h", "1d")

    Returns:
        聚合後的合約級 K 線 DataFrame
    """
    if df.is_empty():
        logger.warning("空 DataFrame，跳過合約級聚合")
        return df

    every = TIMEFRAME_MAP.get(timeframe)
    if every is None:
        raise ValueError(f"不支援的時間週期: {timeframe}。可用: {list(TIMEFRAME_MAP.keys())}")

    # 確保 date 是排序的
    df = df.sort("date")

    # 定義聚合群組鍵
    group_cols = []
    for col in ["option_id", "contract_date", "PutCall", "ExercisePrice"]:
        if col in df.columns:
            group_cols.append(col)

    if not group_cols:
        logger.warning("DataFrame 缺少分群欄位 (option_id/contract_date/PutCall/ExercisePrice)")
        return df

    # 聚合表達式
    agg_exprs = []

    # OHLCV (期權價格)
    if "price" in df.columns:
        agg_exprs.extend([
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
        ])

    # 標的價格 OHLC
    if "Underlying_S" in df.columns:
        agg_exprs.extend([
            pl.col("Underlying_S").first().alias("underlying_open"),
            pl.col("Underlying_S").max().alias("underlying_high"),
            pl.col("Underlying_S").min().alias("underlying_low"),
            pl.col("Underlying_S").last().alias("underlying_close"),
        ])

    # Volume
    if "volume" in df.columns:
        agg_exprs.append(pl.col("volume").sum().alias("volume"))

    # Trade count
    agg_exprs.append(pl.len().alias("trade_count"))

    # Greeks 末值 (last)
    greeks_cols = ["IV", "Delta", "Gamma", "Vega", "Theta", "Vanna", "Charm"]
    for g in greeks_cols:
        if g in df.columns:
            agg_exprs.append(pl.col(g).last().alias(g))

    # group_by_dynamic 聚合
    result = (
        df.group_by_dynamic(
            "date",
            every=every,
            group_by=group_cols,
        )
        .agg(agg_exprs)
        .sort("date")
    )

    logger.info(f"合約級聚合完成 [{timeframe}]: {len(df)} ticks → {len(result)} bars")
    return result


def aggregate_market_summary(df: pl.DataFrame, timeframe: str = "1m") -> pl.DataFrame:
    """
    表 B：全市場微觀摘要表

    每個時間窗口僅產出 1 筆摘要紀錄，包含 RV, IV_Skew, Net_GEX, PCR 等。

    Args:
        df: 含 Greeks 的完整 tick DataFrame
        timeframe: 目標時間週期

    Returns:
        市場摘要 DataFrame (每 row = 一個時間窗口)
    """
    if df.is_empty():
        logger.warning("空 DataFrame，跳過市場摘要聚合")
        return pl.DataFrame()

    every = TIMEFRAME_MAP.get(timeframe)
    if every is None:
        raise ValueError(f"不支援的時間週期: {timeframe}")

    df = df.sort("date")

    # 先用 group_by_dynamic 按時間窗口分割
    # 取得每個窗口的起始時間
    windowed = (
        df.group_by_dynamic("date", every=every)
        .agg([
            pl.col("date").first().alias("window_start"),
            pl.col("date").last().alias("window_end"),
            pl.len().alias("tick_count"),
        ])
        .sort("date")
    )

    # 用唯一的窗口起始時間切割原始 df，對每個窗口呼叫 compute_market_summary
    window_starts = windowed["date"].to_list()

    records = []
    for i, ws in enumerate(window_starts):
        # 取得窗口內資料
        if i + 1 < len(window_starts):
            window_df = df.filter(
                (pl.col("date") >= ws) & (pl.col("date") < window_starts[i + 1])
            )
        else:
            window_df = df.filter(pl.col("date") >= ws)

        if window_df.is_empty():
            continue

        # 計算市場摘要
        summary = compute_market_summary(window_df, timeframe)
        summary["date"] = ws
        summary["tick_count"] = len(window_df)
        records.append(summary)

    if not records:
        return pl.DataFrame()

    result = pl.DataFrame(records).sort("date")
    logger.info(f"市場摘要聚合完成 [{timeframe}]: {len(window_starts)} windows → {len(result)} summaries")
    return result


def process_all_timeframes(
    df: pl.DataFrame,
    date_str: str = None,
) -> Dict[str, Tuple[pl.DataFrame, pl.DataFrame]]:
    """
    一次產出所有週期的雙表。

    Args:
        df: 含 Greeks 的完整 tick DataFrame
        date_str: 交易日 (資訊用，不影響計算)

    Returns:
        {
            "1m": (contract_bars_df, market_summary_df),
            "1h": (contract_bars_df, market_summary_df),
            "4h": (contract_bars_df, market_summary_df),
            "1d": (contract_bars_df, market_summary_df),
        }
    """
    if df.is_empty():
        logger.warning("空 DataFrame，跳過全週期聚合")
        return {}

    logger.info(f"開始全週期聚合 {f'({date_str})' if date_str else ''}: {len(df)} 筆 tick 資料")

    results = {}
    for tf in TIMEFRAME_MAP:
        try:
            contract_bars = aggregate_contract_bars(df, tf)
            market_summary = aggregate_market_summary(df, tf)
            results[tf] = (contract_bars, market_summary)
        except Exception as e:
            logger.error(f"[{tf}] 聚合失敗: {e}", exc_info=True)
            results[tf] = (pl.DataFrame(), pl.DataFrame())

    return results
