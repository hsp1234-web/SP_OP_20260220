import polars as pl
import logging

logger = logging.getLogger("pipeline.schema")

class SchemaEnforcer:
    @staticmethod
    def sanitize_stock_id(df: pl.DataFrame, col_name="stock_id") -> pl.DataFrame:
        """強制補零邏輯：確保股票代碼為 4 或 6 位字串"""
        if col_name in df.columns:
            # 確保轉為字串並補零
            return df.with_columns(
                pl.col(col_name).cast(pl.Utf8).str.pad_start(4, "0")
            )
        return df

    @staticmethod
    def apply_standard_types(df: pl.DataFrame, dataset: str) -> pl.DataFrame:
        """根據資料集套用標準型別"""
        if df.is_empty():
            return df

        # 統一處理日期
        if "date" in df.columns:
            # Check if date is already Datetime
            if df.schema["date"] == pl.Utf8:
                 df = df.with_columns(
                    pl.col("date").str.strptime(pl.Datetime(time_unit="ns"), "%Y-%m-%d", strict=False)
                )
            elif df.schema["date"] == pl.Date:
                 df = df.with_columns(pl.col("date").cast(pl.Datetime(time_unit="ns")))

        # 針對特定資料集加強
        if dataset == "TaiwanStockPrice":
            df = SchemaEnforcer.sanitize_stock_id(df)
            # 加回數值型別轉換以確保資料品質
            cols_to_cast = {
                "Trading_Volume": pl.Int64,
                "Trading_money": pl.Int64,
                "open": pl.Float64,
                "max": pl.Float64,
                "min": pl.Float64,
                "close": pl.Float64,
                "spread": pl.Float64,
                "Trading_turnover": pl.Int64,
            }
            exprs = [pl.col(c).cast(t) for c, t in cols_to_cast.items() if c in df.columns]
            if exprs:
                df = df.with_columns(exprs)

        elif dataset == "TaiwanStockPriceTick":
            df = SchemaEnforcer.sanitize_stock_id(df)
            cols_to_cast = {
                "deal_price": pl.Float64,
                "volume": pl.Int64,
            }
            exprs = [pl.col(c).cast(t) for c, t in cols_to_cast.items() if c in df.columns]
            if exprs:
                df = df.with_columns(exprs)

            # Tick Time handling?
            if "Time" in df.columns and "date" in df.columns:
                 # Combine date + Time -> timestamp?
                 # FinMind Time is usually HH:mm:ss.SSSSSS
                 pass

        elif dataset == "TaiwanStockTradingDate":
            df = SchemaEnforcer.sanitize_stock_id(df)

        return df

def enforce_schema(df: pl.DataFrame, dataset_name: str) -> pl.DataFrame:
    """Wrapper for backward compatibility."""
    return SchemaEnforcer.apply_standard_types(df, dataset_name)
