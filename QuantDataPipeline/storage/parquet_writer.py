import os
import logging
from pathlib import Path
import polars as pl

from core.config import DATA_DIR, COMPRESSION_LEVEL
from .integrity_validator import compute_md5

logger = logging.getLogger("pipeline.storage")

def save_dataframe(
    df: pl.DataFrame,
    dataset_name: str,
    date: str,
    data_id: str = ""
) -> tuple[str, str]:
    """
    Save DataFrame to Parquet with atomic write and MD5 checksum.
    Path: data/{year}/{dataset_name}/{data_id}_{date}.parquet (or {date}.parquet if no data_id)

    Returns:
        (file_path, md5_checksum)
    """
    if df.is_empty():
        logger.warning(f"{dataset_name} {date} {data_id} 資料為空。跳過寫入。")
        return "", ""

    # Filename strategy:
    # If data_id is present: {data_id}_{date}.parquet
    # If not: {date}.parquet
    # 動態取得最新的 DATA_DIR (為了支援 Colab動態改變路徑)
    import core.config
    current_data_dir = core.config.DATA_DIR
    
    year = date.split("-")[0]
    target_dir = current_data_dir / year / dataset_name
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{data_id}_{date}.parquet" if data_id else f"{date}.parquet"
    target_path = target_dir / filename
    tmp_path = target_path.with_suffix(".tmp")

    try:
        # Write to tmp using Polars native writer
        df.write_parquet(
            tmp_path,
            compression="zstd",
            compression_level=COMPRESSION_LEVEL
        )

        # Verify Integrity
        try:
            pl.read_parquet(tmp_path).head(1)
            file_size = tmp_path.stat().st_size
            if file_size == 0:
                raise ValueError("Parquet 檔案大小為 0 byte")
        except Exception as e:
            logger.error(f"Parquet 基因檢測失敗 (檔案損毀或為空殼): {e}")
            raise ValueError(f"Parquet 格式無效或損毀: {e}")

        checksum = compute_md5(tmp_path)

        # Atomic Rename
        if target_path.exists():
            logger.debug(f"覆蓋既有檔案: {target_path}")

        os.rename(tmp_path, target_path)
        logger.info(f"成功儲存 {target_path} (MD5: {checksum})")

        return str(target_path), checksum

    except Exception as e:
        logger.error(f"儲存 parquet 檔案 {target_path} 失敗: {e}")
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise e
