import polars as pl
import logging
from typing import Dict, Any

logger = logging.getLogger("pipeline.schema_enforcer")

def enforce_schema(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """
    Applies strict schema enforcement and type casting for FinMind datasets.
    Ensures all dates are ns-precision timestamps and IDs are strings.
    """
    if df.is_empty():
        return df

    try:
        # Common transformations
        # Most FinMind datasets have 'date' or 'date' + 'Time'
        if "date" in df.columns:
            # If there's a 'Time' column, we might need to combine them,
            # but usually FinMind 'date' is YYYY-MM-DD and 'Time' is HH:mm:ss.
            # However, for daily data, 'date' is sufficient.
            # For tick data, 'Time' is present.

            # Basic date casting first (handle YYYY-MM-DD)
            df = df.with_columns(
                pl.col("date").cast(pl.Utf8).str.strptime(pl.Datetime(time_unit="ns"), "%Y-%m-%d", strict=False)
            )

        # Dataset-specific logic
        if dataset_name == "TaiwanStockPrice":
            df = df.with_columns([
                pl.col("stock_id").cast(pl.Utf8),
                pl.col("Trading_Volume").cast(pl.Int64),
                pl.col("Trading_money").cast(pl.Int64),
                pl.col("open").cast(pl.Float64),
                pl.col("max").cast(pl.Float64),
                pl.col("min").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("spread").cast(pl.Float64),
                pl.col("Trading_turnover").cast(pl.Int64),
            ])

        elif dataset_name == "TaiwanStockPriceTick":
             # Tick data usually has 'Time' like '13:30:00.123456'
             # We need to combine 'date' and 'Time' into a single timestamp if possible,
             # or at least cast them correctly.
             # For now, let's just cast types.
             df = df.with_columns([
                pl.col("stock_id").cast(pl.Utf8),
                pl.col("deal_price").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
                # Time is usually string, keep it as is or parse later in processors?
                # The prompt says "Time: Force convert to Datetime".
                # If we have date and Time, we should combine.
                # But let's assume raw schema enforcement here.
                pl.col("Time").cast(pl.Utf8)
             ])
             # Optimization: Combine date + Time -> timestamp?
             # Let's do it if both exist
             if "date" in df.columns and "Time" in df.columns:
                 df = df.with_columns(
                     (pl.col("date").dt.strftime("%Y-%m-%d") + " " + pl.col("Time")).str.strptime(pl.Datetime(time_unit="ns"), "%Y-%m-%d %H:%M:%S%.f")
                     .alias("timestamp")
                 )

        elif dataset_name == "TaiwanOptionTick":
             df = df.with_columns([
                pl.col("contract_date").cast(pl.Utf8), # Delivery month
                pl.col("strike_price").cast(pl.Float64),
                pl.col("call_put").cast(pl.Utf8),
                pl.col("deal_price").cast(pl.Float64),
                pl.col("volume").cast(pl.Int64),
             ])
             if "date" in df.columns and "Time" in df.columns:
                 df = df.with_columns(
                     (pl.col("date").dt.strftime("%Y-%m-%d") + " " + pl.col("Time")).str.strptime(pl.Datetime(time_unit="ns"), "%Y-%m-%d %H:%M:%S%.f")
                     .alias("timestamp")
                 )

        elif dataset_name == "TaiwanOptionOpenInterestLargeTraders":
             # As per prompt example
             df = df.with_columns([
                # date is already handled in common block if it's YYYY-MM-DD
                pl.col("contract_id").cast(pl.Utf8),
                pl.col("buy_volume").cast(pl.Int64),
                pl.col("sell_volume").cast(pl.Int64),
                pl.col("buy_oi").cast(pl.Int64),
                pl.col("sell_oi").cast(pl.Int64),
             ])

        return df

    except Exception as e:
        logger.error(f"Schema enforcement failed for {dataset_name}: {e}")
        raise e
