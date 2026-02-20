"""
monthly_roller.py — 月度打包 / 日檔混合儲存

解決 Colab 斷線 + Google Drive I/O 限流的折衷方案:
  - 最新月份 → 保留日檔 (daily/)
  - 過往月份 → 合併為月檔 (monthly/)
"""
import polars as pl
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger("pipeline.roller")


def list_daily_files(
    data_dir: Path,
    table_type: str,
    timeframe: str,
    year_month: str = None
) -> List[Path]:
    """
    列出 daily/ 目錄下指定月份的散檔。

    Args:
        data_dir: 資料根目錄
        table_type: 表類型 (e.g., "ContractBars", "MarketSummary")
        timeframe: 時間週期 (e.g., "1m")
        year_month: 月份過濾 (e.g., "202405")，None 則列出全部

    Returns:
        符合條件的 Parquet 路徑列表
    """
    daily_dir = data_dir / "Features" / "daily"
    if not daily_dir.exists():
        return []

    pattern = f"TXO_{table_type}_{timeframe}_*.parquet"
    files = sorted(daily_dir.glob(pattern))

    if year_month:
        files = [f for f in files if year_month in f.stem]

    return files


def rollover_month(
    data_dir: Path,
    table_type: str,
    timeframe: str,
    year_month: str,
    delete_daily: bool = True,
) -> Optional[Path]:
    """
    將指定月份的日檔合併為月檔。

    Args:
        data_dir: 資料根目錄
        table_type: 表類型
        timeframe: 時間週期
        year_month: 目標月份 (e.g., "202405")
        delete_daily: 合併成功後是否刪除散檔

    Returns:
        月檔路徑 (成功時) 或 None (失敗時)
    """
    daily_files = list_daily_files(data_dir, table_type, timeframe, year_month)

    if not daily_files:
        logger.info(f"[{table_type}/{timeframe}] 月份 {year_month} 無日檔可合併")
        return None

    logger.info(f"[{table_type}/{timeframe}] 合併 {len(daily_files)} 個日檔 → {year_month} 月檔")

    try:
        # 讀取並合併所有日檔
        dfs = [pl.read_parquet(f) for f in daily_files]
        merged = pl.concat(dfs).sort("date") if dfs else pl.DataFrame()

        if merged.is_empty():
            logger.warning(f"合併後 DataFrame 為空")
            return None

        # 寫入月檔
        monthly_dir = data_dir / "Features" / "monthly"
        monthly_dir.mkdir(parents=True, exist_ok=True)
        monthly_path = monthly_dir / f"TXO_{table_type}_{timeframe}_{year_month}.parquet"

        merged.write_parquet(monthly_path, compression="zstd", compression_level=3)
        logger.info(f"月檔已寫入: {monthly_path} ({len(merged)} 筆)")

        # 刪除散檔
        if delete_daily:
            for f in daily_files:
                f.unlink()
                logger.debug(f"已刪除日檔: {f.name}")
            logger.info(f"已清理 {len(daily_files)} 個日檔")

        return monthly_path

    except Exception as e:
        logger.error(f"月度合併失敗: {e}", exc_info=True)
        return None


def check_and_rollover(data_dir: Path):
    """
    自動檢查並觸發月度結算。

    邏輯：若 daily/ 中存在「不是當月」的檔案，觸發該月份的 rollover。
    """
    daily_dir = data_dir / "Features" / "daily"
    if not daily_dir.exists():
        return

    current_month = datetime.now().strftime("%Y%m")

    # 收集所有日檔的月份
    months_found = set()
    for f in daily_dir.glob("TXO_*.parquet"):
        # 檔名格式: TXO_{table}_{tf}_{date}.parquet
        parts = f.stem.split("_")
        if len(parts) >= 4:
            date_str = parts[-1]  # YYYY-MM-DD
            try:
                file_month = date_str[:4] + date_str[5:7]
                months_found.add((file_month, "_".join(parts[1:-1])))  # (month, table_tf)
            except (IndexError, ValueError):
                continue

    # 對每個非當月的月份觸發 rollover
    for month, table_tf in months_found:
        if month == current_month:
            continue  # 當月保留日檔

        # 解析 table_type 和 timeframe
        # table_tf 格式: "ContractBars_1m" 或 "MarketSummary_1h"
        parts = table_tf.rsplit("_", 1)
        if len(parts) == 2:
            table_type, timeframe = parts
            logger.info(f"觸發月度結算: {month} / {table_type} / {timeframe}")
            rollover_month(data_dir, table_type, timeframe, month)


def save_feature_daily(
    df: pl.DataFrame,
    data_dir: Path,
    table_type: str,
    timeframe: str,
    date_str: str,
) -> Optional[Path]:
    """
    存入日檔 (daily/)。

    Args:
        df: 特徵 DataFrame
        data_dir: 資料根目錄
        table_type: "ContractBars" 或 "MarketSummary"
        timeframe: "1m", "1h", "4h", "1d"
        date_str: 交易日 (YYYY-MM-DD)

    Returns:
        儲存路徑
    """
    if df.is_empty():
        return None

    daily_dir = data_dir / "Features" / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    path = daily_dir / f"TXO_{table_type}_{timeframe}_{date_str}.parquet"
    df.write_parquet(path, compression="zstd", compression_level=3)
    logger.info(f"儲存日檔: {path.name} ({len(df)} 筆)")
    return path
