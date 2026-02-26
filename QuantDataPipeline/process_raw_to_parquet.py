#!/usr/bin/env python3
"""
V4.2 Phase 2: JSON → Parquet 轉檔腳本 (process_raw_to_parquet.py)

掃描 data_v4/temp_raw_data/ 下的 JSON 檔，轉為 Polars DataFrame 並存成 Parquet。
此腳本完全不需要網路連線。

用法:
  python process_raw_to_parquet.py
  python process_raw_to_parquet.py --datasets TaiwanStockPriceAdj
  python process_raw_to_parquet.py --workers 2
"""

import argparse
import gzip
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List, Optional, Dict

# 🔥 強制 Polars 只使用 1 個 CPU 核心，極低負載運行
os.environ["POLARS_MAX_THREADS"] = "1"

try:
    import polars as pl
except ImportError:
    pl = None
    print("⚠️ Polars 未安裝，請執行: pip install polars")

# 確保專案根目錄在 sys.path 中
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.datasets_registry import ALL_DATASETS, DATASETS_BY_NAME, get_dataset_names

# ========== 日誌 ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("process_raw")

# ========== 路徑 ==========
DATA_V4_DIR = PROJECT_ROOT / "data_v4"
TEMP_RAW_DIR = DATA_V4_DIR / "temp_raw_data"
PROCESSED_DIR = DATA_V4_DIR / "processed_parquet"

# ========== 環境偵測與資源控制 ==========
IS_COLAB = "google.colab" in sys.modules
DEFAULT_WORKERS = os.cpu_count() if IS_COLAB else int(os.getenv("V4_PROCESS_WORKERS", "2"))


# ========== Schema 定義 ==========
# 定義每個資料集的欄位型別強制轉換規則
SCHEMA_OVERRIDES: Dict[str, Dict[str, type]] = {
    "TaiwanStockPriceAdj": {
        "date": pl.Utf8 if pl else str,
        "stock_id": pl.Utf8 if pl else str,
        "open": pl.Float64 if pl else float,
        "max": pl.Float64 if pl else float,
        "min": pl.Float64 if pl else float,
        "close": pl.Float64 if pl else float,
        "Trading_Volume": pl.Float64 if pl else float,
        "Trading_money": pl.Float64 if pl else float,
    },
}


def read_json_gz(filepath: Path) -> Optional[list]:
    """讀取 gzip 壓縮的 JSON 檔案，回傳 data 欄位的列表"""
    try:
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            content = json.load(f)
        
        data = content.get("data", [])
        if not data:
            return None
        return data
    except Exception as e:
        logger.error(f"❌ 讀取失敗 {filepath.name}: {e}")
        return None


def parse_date_from_filename(filename: str, dataset: str) -> Optional[str]:
    """從檔名解析日期 (YYYY-MM-DD 格式)"""
    # 支援 .json.gz 和可能的 .tmp 副檔名
    name_no_ext = filename.replace(".json.gz", "").replace(".tmp", "")
    parts = name_no_ext.split("_")
    if len(parts) >= 2:
        date_compact = parts[-1]
        if len(date_compact) == 8 and date_compact.isdigit():
            return f"{date_compact[:4]}-{date_compact[4:6]}-{date_compact[6:8]}"
    return None


def group_files_by_month(files: List[Path], dataset: str) -> Dict[str, List[Path]]:
    """將檔案按月份分組 (key: YYYYMM)"""
    groups = defaultdict(list)
    for f in files:
        date_str = parse_date_from_filename(f.name, dataset)
        if date_str:
            month_key = date_str[:7].replace("-", "")  # YYYYMM
            groups[month_key].append(f)
    return dict(groups)


def convert_and_merge(
    dataset: str,
    files: List[Path],
    month_key: str,
) -> Optional[Path]:
    """
    將多個 JSON 檔合併為一個月度 Parquet 檔。
    
    Returns:
        成功: Parquet 檔路徑
        失敗: None
    """
    if pl is None:
        logger.error("❌ Polars 未安裝")
        return None

    all_records = []
    for f in sorted(files):
        records = read_json_gz(f)
        if records:
            all_records.extend(records)

    if not all_records:
        logger.warning(f"  ⏭️ {dataset} {month_key} 無有效資料，跳過")
        return None

    # 轉為 Polars DataFrame
    try:
        new_df = pl.DataFrame(all_records)
    except Exception as e:
        logger.error(f"❌ DataFrame 建立失敗 [{dataset} {month_key}]: {e}")
        return None

    # Schema 強制轉型
    if dataset in SCHEMA_OVERRIDES:
        for col, dtype in SCHEMA_OVERRIDES[dataset].items():
            if col in new_df.columns:
                try:
                    new_df = new_df.with_columns(pl.col(col).cast(dtype))
                except Exception:
                    pass

                    
    dest_dir = PROCESSED_DIR / dataset
    dest_dir.mkdir(parents=True, exist_ok=True)
    parquet_name = f"{dataset}_{month_key}.parquet"
    parquet_path = dest_dir / parquet_name

    # 🔗 如果已經存在舊的 Parquet（例如之前下載的五月資料），則需合併
    if parquet_path.exists():
        try:
            old_df = pl.read_parquet(parquet_path)
            # 使用 vstack 串接，然後用 unique 確保不要把重複的資料疊上去
            # 不同資料集的 Unique Key 可能不一樣，這裡使用 date 和 stock_id 作為基本防線，或者直接 unique()
            combined_df = pl.concat([old_df, new_df]).unique()
            df = combined_df
            logger.info(f"  🔗 發現舊檔，成功與新資料合併 ({len(old_df)} -> {len(df)})")
        except Exception as e:
            logger.error(f"❌ 舊檔合併失敗，請檢查 {parquet_path}: {e}")
            return None
    else:
        df = new_df

    # 原子性寫入: 先寫 .tmp 再 rename
    tmp_path = parquet_path.with_suffix(".tmp")
    df.write_parquet(tmp_path, compression="zstd", compression_level=3)

    # 驗證
    try:
        check = pl.read_parquet(tmp_path).head(1)
        if len(check) == 0:
            logger.error(f"❌ Parquet 驗證失敗 (空檔): {parquet_name}")
            tmp_path.unlink(missing_ok=True)
            return None
    except Exception as e:
        logger.error(f"❌ Parquet 驗證失敗: {e}")
        tmp_path.unlink(missing_ok=True)
        return None

    # 原子替換
    os.rename(tmp_path, parquet_path)

    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    logger.info(f"  ✅ {parquet_name} | {len(df)} 筆 | {size_mb:.2f} MB")

    return parquet_path


def process_dataset(dataset: str, delete_after: bool = True) -> bool:
    """處理單一資料集的所有未轉檔 JSON。回傳是否處理了任何檔案。"""
    src_dir = TEMP_RAW_DIR / dataset
    if not src_dir.exists():
        return False

    files = sorted(src_dir.glob("*.json.gz"))
    if not files:
        return False

    logger.info(f"\n📦 處理: {dataset} | {len(files)} 個 JSON 檔")

    # 依月份分組
    monthly_groups = group_files_by_month(files, dataset)
    logger.info(f"  📊 涵蓋 {len(monthly_groups)} 個月份")
    
    work_done = False
    for month_key in sorted(monthly_groups.keys()):
        month_files = monthly_groups[month_key]
        result = convert_and_merge(dataset, month_files, month_key)

        if result and delete_after:
            for f in month_files:
                f.unlink(missing_ok=True)
            logger.info(f"  🗑️ 已刪除 {len(month_files)} 個 JSON 原始檔")
            work_done = True
            
    return work_done


def run_process(
    target_datasets: Optional[List[str]] = None,
    delete_after: bool = True,
    watch_mode: bool = False,
):
    """執行轉檔主流程"""
    logger.info("=" * 60)
    logger.info(f"🔄 V4.2 JSON → Parquet 轉檔 | 單核潛水艇模式")
    logger.info("=" * 60)

    while True:
        if target_datasets:
            datasets = target_datasets
        else:
            # 自動偵測有哪些資料集需要處理
            if TEMP_RAW_DIR.exists():
                datasets = [d.name for d in TEMP_RAW_DIR.iterdir() if d.is_dir()]
            else:
                datasets = []

        if not datasets:
            work_done = False
        else:
            work_done = False
            for ds_name in sorted(datasets):
                # process_dataset 如果沒有處理任何東西，會直接 return
                res = process_dataset(ds_name, delete_after=delete_after)
                if res:  # process_dataset 被修改後，若有處理檔案則回傳 True
                    work_done = True

        if not watch_mode:
            break
            
        if not work_done:
            logger.info("👀 暫無待處理檔案，休眠 60 秒...")
            time.sleep(60)
        else:
            logger.info("🔥 剛完成一批任務，休眠 5 秒後繼續...")
            time.sleep(5)

    if not watch_mode:
        logger.info("\n" + "=" * 60)
        logger.info("🎉 單次轉檔掃描完成")
        logger.info("=" * 60)



# ========== CLI ==========
def main():
    parser = argparse.ArgumentParser(description="V4.2 JSON → Parquet 轉檔 (預設單核運行)")
    parser.add_argument(
        "--datasets", type=str, default=None,
        help="指定資料集 (逗號分隔)"
    )
    parser.add_argument(
        "--watch", action="store_true", default=False,
        help="常駐守望模式：持續掃描新下載的檔案進行轉檔"
    )
    parser.add_argument(
        "--keep-json", action="store_true", default=False,
        help="轉檔後不刪除 JSON 原始檔"
    )
    args = parser.parse_args()

    target = args.datasets.split(",") if args.datasets else None
    run_process(
        target_datasets=target,
        delete_after=not args.keep_json,
        watch_mode=args.watch
    )


if __name__ == "__main__":
    main()
