"""
compute_greeks_pipeline.py — 純粹的計算管線

職責：
1. 讀取本地 Parquet（Lazy Loading）
2. Asof Join 對齊期權與期貨時間
3. 計算結算日真實 Years_to_Maturity
4. 透過 Numba 加速計算 Greeks
5. 存回 GreeksFeatures/ Parquet

可透過 CLI 或程式化呼叫 compute_greeks_for_date()。
"""
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

import polars as pl

sys.path.append(str(Path(__file__).parent))

from core.config import DATA_DIR
from processors.greeks_engine import calculate_greeks

logger = logging.getLogger("compute.greeks")


# ─────────────────────────────────────────────
# 結算日曆工具
# ─────────────────────────────────────────────

def _third_wednesday(year: int, month: int) -> datetime:
    """計算指定年月的第三個星期三 (台灣期交所結算日)"""
    import calendar
    cal = calendar.monthcalendar(year, month)
    # monthcalendar 回傳 [[Mon, Tue, Wed, ...], ...]
    # 找第三個 Wed (index=2)
    wed_count = 0
    for week in cal:
        if week[2] != 0:  # Wednesday 有值
            wed_count += 1
            if wed_count == 3:
                return datetime(year, month, week[2])
    # fallback
    return datetime(year, month, 15)


def _calc_years_to_maturity(trade_date: datetime, contract_date_str: str) -> float:
    """
    計算 Years_to_Maturity。
    contract_date_str 格式: 'YYYYMM' 或 'YYYYMMWN'
    trade_date: 該筆 tick 的交易日期

    回傳距結算日的年化天數 (days / 365)。
    若已過結算 → 回傳極小值 0.0001 避免除零。
    """
    try:
        year = int(contract_date_str[:4])
        month = int(contract_date_str[4:6])
        settlement = _third_wednesday(year, month)

        delta = (settlement - trade_date).total_seconds() / (365.25 * 24 * 3600)
        return max(delta, 0.0001)
    except (ValueError, IndexError):
        return 0.05  # fallback 常數


# ─────────────────────────────────────────────
# 核心計算函數 (可程式化匯入)
# ─────────────────────────────────────────────

def compute_greeks_for_date(date_str: str, data_dir: Path = None) -> tuple:
    """
    計算指定日期的 Greeks 特徵。

    Args:
        date_str: 交易日 (YYYY-MM-DD)
        data_dir: 資料根目錄 (預設使用 config.DATA_DIR)

    Returns:
        (df_greeks, output_path, success) — 成功時 success=True
        (None, None, False) — 失敗時
    """
    if data_dir is None:
        data_dir = DATA_DIR

    year = date_str.split('-')[0]
    opt_path = data_dir / year / "TaiwanOptionTick" / f"TXO_{date_str}.parquet"
    fut_path = data_dir / year / "TaiwanFuturesTick" / f"TX_{date_str}.parquet"

    # 前置檢查
    if not opt_path.exists():
        logger.warning(f"找不到選擇權 Parquet: {opt_path}")
        return None, None, False
    if not fut_path.exists():
        logger.warning(f"找不到期貨 Parquet: {fut_path}")
        return None, None, False

    logger.info(f"啟動 {date_str} 資料合併與計算引擎 (Lazy Loading)")

    try:
        # 1. Lazy Load
        lf_opt = pl.scan_parquet(opt_path)
        lf_fut = pl.scan_parquet(fut_path)

        # 2. 欄位提取與清理
        lf_fut_clean = lf_fut.select([
            pl.col("date"),
            pl.col("price").alias("Underlying_S")
        ]).sort("date")

        lf_opt_clean = lf_opt.select([
            pl.col("date"),
            pl.col("option_id"),
            pl.col("contract_date"),
            pl.col("PutCall"),
            pl.col("ExercisePrice"),
            pl.col("price"),
        ]).sort("date")

        # 如果原始資料有 volume 欄位就帶上
        try:
            lf_opt_with_vol = lf_opt.select([
                pl.col("date"),
                pl.col("option_id"),
                pl.col("contract_date"),
                pl.col("PutCall"),
                pl.col("ExercisePrice"),
                pl.col("price"),
                pl.col("volume"),
            ]).sort("date")
            lf_opt_clean = lf_opt_with_vol
        except Exception:
            pass  # volume 不存在就忽略

        # 3. Asof Join
        lf_joined = lf_opt_clean.join_asof(
            lf_fut_clean,
            on="date",
            strategy="backward"
        ).drop_nulls("Underlying_S")

        # 4. Collect & 計算真實 Years_to_Maturity
        df_joined = lf_joined.collect()

        if df_joined.is_empty():
            logger.warning(f"{date_str} Asof Join 後無有效資料")
            return None, None, False

        # 使用結算日日曆計算 T
        trade_date_dt = datetime.strptime(date_str, "%Y-%m-%d")
        contract_dates = df_joined["contract_date"].to_list()
        t_values = [_calc_years_to_maturity(trade_date_dt, cd) for cd in contract_dates]

        df_joined = df_joined.with_columns([
            pl.Series("Years_to_Maturity", t_values)
        ])

        logger.info(f"對齊完成 ({date_str} 共 {len(df_joined)} 筆對齊紀錄)")

        # 5. Numba Greeks 計算
        logger.info("進入 Numba 加速 Greeks 運算引擎")
        df_greeks = calculate_greeks(df_joined, r=0.015)

        # 6. 存回 Parquet
        output_dir = data_dir / year / "GreeksFeatures"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"TXO_Greeks_{date_str}.parquet"

        df_greeks.write_parquet(output_path, compression="zstd", compression_level=3)
        logger.info(f"成功儲存 {output_path} ({len(df_greeks)} 筆)")

        return df_greeks, output_path, True

    except Exception as e:
        logger.error(f"{date_str} Greeks 計算失敗: {e}", exc_info=True)
        return None, None, False


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(description="Greeks 特徵計算管線")
    parser.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    df, path, ok = compute_greeks_for_date(args.date)
    if ok:
        print(f"\n✅ 成功產出 {len(df)} 筆 Greeks 紀錄 → {path}")
        display_cols = [c for c in ["date", "contract_date", "PutCall", "ExercisePrice",
                                     "price", "Underlying_S", "IV", "Delta", "Gamma",
                                     "Vega", "Vanna", "Charm"] if c in df.columns]
        print(df.select(display_cols).head(15))
    else:
        print(f"\n❌ {args.date} 計算失敗")
        sys.exit(1)
