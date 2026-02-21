import os
import pytest
import polars as pl
from pathlib import Path

from storage.parquet_writer import save_dataframe

def test_parquet_writer_validation(tmp_data_dir):
    # 建立一個測試用的空殼 df
    df = pl.DataFrame({"A": [1, 2, 3]})
    
    # 預期正常寫入
    saved_path, checksum = save_dataframe(df, "TestDataset", "2024-01-01", "TEST_ID")
    assert saved_path.endswith("TEST_ID_2024-01-01.parquet")
    assert Path(saved_path).exists()
    
    # 模擬寫入過程 tmp 被掉包為 0 byte 檔案
    # 這是為了覆蓋例外處理，因為 Polars 寫入本質上不會 0 byte
    # 這裡我們手動覆蓋 os.path.getsize 來觸發 ValueError 測試
    
    def mock_write_parquet(self, file_path, *args, **kwargs):
        # 寫一個假檔 (0 byte file)
        Path(file_path).write_bytes(b"")
        
    original_write = pl.DataFrame.write_parquet
    
    try:
        pl.DataFrame.write_parquet = mock_write_parquet
        with pytest.raises(ValueError, match="Parquet 格式無效或損毀"):
            save_dataframe(df, "TestDataset", "2024-01-02", "TEST_ID")
    finally:
        pl.DataFrame.write_parquet = original_write
