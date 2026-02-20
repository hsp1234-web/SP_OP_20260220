import polars as pl
import logging
from typing import Optional

from fetchers.infrastructure.http_session import HTTPSession
from fetchers.parsers.schema_enforcer import enforce_schema

logger = logging.getLogger("pipeline.trading_date")

def fetch_trading_dates(session: HTTPSession, start_date: str = "", end_date: str = "") -> pl.DataFrame:
    """
    抓取台灣股市交易日。

    參數:
        session: HTTPSession 實體。
        start_date: 開始日期 (YYYY-MM-DD)。
        end_date: 結束日期 (YYYY-MM-DD)。

    回傳:
        包含 'date' 欄位的 Polars DataFrame。
    """
    logger.info(f"正在抓取交易日: {start_date} 至 {end_date}...")

    try:
        res = session.get_data(
            dataset="TaiwanStockTradingDate",
            start_date=start_date,
            end_date=end_date
        )

        data = res.get("data", [])
        if not data:
            logger.warning("未找到任何交易日資料。")
            return pl.DataFrame({"date": []})

        df = pl.from_dicts(data)

        # Schema 強制轉換 (轉換日期為 Datetime ns)
        df = enforce_schema(df, "TaiwanStockTradingDate")

        # 使用者提供的邏輯範例：若有 stock_id 則可能需過濾 (目前 API 通常只回傳日期)
        if "stock_id" in df.columns:
             logger.info(f"發現欄位: {df.columns}")

        return df.select("date").unique().sort("date")

    except Exception as e:
        logger.error(f"抓取交易日失敗: {e}")
        return pl.DataFrame({"date": []})
