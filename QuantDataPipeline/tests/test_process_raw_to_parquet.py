"""
V4.2 Phase 2 轉檔測試 (test_process_raw_to_parquet.py)
測試 JSON → Parquet 轉換邏輯，不需要網路。
"""
import gzip
import json
import pytest
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

import process_raw_to_parquet as proc


# ========== Fixtures ==========

@pytest.fixture
def tmp_dirs(tmp_path, monkeypatch):
    """設定臨時的資料目錄"""
    temp_raw = tmp_path / "data_v4" / "temp_raw_data"
    processed = tmp_path / "data_v4" / "processed_parquet"
    temp_raw.mkdir(parents=True)
    processed.mkdir(parents=True)
    
    monkeypatch.setattr(proc, "TEMP_RAW_DIR", temp_raw)
    monkeypatch.setattr(proc, "PROCESSED_DIR", processed)
    monkeypatch.setattr(proc, "DATA_V4_DIR", tmp_path / "data_v4")
    
    return temp_raw, processed


def create_mock_json_gz(dest_dir: Path, dataset: str, date_str: str, records: int = 10):
    """建立模擬的 JSON.gz 檔案"""
    date_compact = date_str.replace("-", "")
    ds_dir = dest_dir / dataset
    ds_dir.mkdir(parents=True, exist_ok=True)

    data = {
        "msg": "success",
        "data": [
            {
                "date": date_str,
                "stock_id": f"{2330 + i}",
                "close": float(100 + i),
                "open": float(99 + i),
                "max": float(101 + i),
                "min": float(98 + i),
                "Trading_Volume": float(1000 * (i + 1)),
                "Trading_money": float(100000 * (i + 1)),
            }
            for i in range(records)
        ],
    }

    filepath = ds_dir / f"{dataset}_{date_compact}.json.gz"
    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return filepath


# ========== 測試類別 ==========

class TestReadJsonGz:
    """JSON 讀取測試"""

    def test_read_valid_file(self, tmp_dirs):
        """應正確讀取有效的 JSON.gz"""
        temp_raw, _ = tmp_dirs
        path = create_mock_json_gz(temp_raw, "TestDS", "2024-02-26", records=5)
        result = proc.read_json_gz(path)
        assert result is not None
        assert len(result) == 5

    def test_read_empty_data(self, tmp_dirs):
        """空 data 欄位應回傳 None"""
        temp_raw, _ = tmp_dirs
        ds_dir = temp_raw / "EmptyDS"
        ds_dir.mkdir()
        path = ds_dir / "EmptyDS_20240226.json.gz"
        with gzip.open(path, "wt") as f:
            json.dump({"msg": "success", "data": []}, f)

        result = proc.read_json_gz(path)
        assert result is None


class TestParseDateFromFilename:
    """檔名日期解析測試"""

    def test_standard_format(self):
        """標準格式: Dataset_YYYYMMDD.json.gz"""
        result = proc.parse_date_from_filename(
            "TaiwanStockPriceAdj_20240226.json.gz", "TaiwanStockPriceAdj"
        )
        assert result == "2024-02-26"

    def test_invalid_format(self):
        """無效格式應回傳 None"""
        result = proc.parse_date_from_filename("random.json.gz", "TestDS")
        assert result is None


class TestGroupFilesByMonth:
    """檔案按月分組測試"""

    def test_groups_correctly(self, tmp_dirs):
        """同月份檔案應分在同一組"""
        temp_raw, _ = tmp_dirs
        files = []
        for d in ["2024-02-01", "2024-02-15", "2024-02-28", "2024-03-01"]:
            f = create_mock_json_gz(temp_raw, "TestDS", d)
            files.append(f)

        groups = proc.group_files_by_month(files, "TestDS")
        assert "202402" in groups
        assert "202403" in groups
        assert len(groups["202402"]) == 3
        assert len(groups["202403"]) == 1


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
class TestConvertAndMerge:
    """JSON → Parquet 轉換測試"""

    def test_basic_conversion(self, tmp_dirs):
        """基本轉換: 3 天 → 1 個月檔"""
        temp_raw, processed = tmp_dirs
        files = []
        for d in ["2024-02-01", "2024-02-15", "2024-02-28"]:
            f = create_mock_json_gz(temp_raw, "TestDS", d, records=5)
            files.append(f)

        result = proc.convert_and_merge("TestDS", files, "202402")
        assert result is not None
        assert result.exists()
        assert result.suffix == ".parquet"

        # 驗證 Parquet 內容
        df = pl.read_parquet(result)
        assert len(df) == 15  # 3天 × 5筆
        assert "date" in df.columns

    def test_output_filename(self, tmp_dirs):
        """輸出檔名應為 {Dataset}_{YYYYMM}.parquet"""
        temp_raw, processed = tmp_dirs
        f = create_mock_json_gz(temp_raw, "MyDS", "2024-03-15")
        result = proc.convert_and_merge("MyDS", [f], "202403")
        assert result.name == "MyDS_202403.parquet"

    def test_atomic_write(self, tmp_dirs):
        """轉檔完成後不應殘留 .tmp 檔"""
        temp_raw, processed = tmp_dirs
        f = create_mock_json_gz(temp_raw, "AtomicDS", "2024-01-01")
        proc.convert_and_merge("AtomicDS", [f], "202401")

        tmp_files = list((processed / "AtomicDS").glob("*.tmp"))
        assert len(tmp_files) == 0

    def test_zstd_compression(self, tmp_dirs):
        """Parquet 應使用 Zstandard 壓縮"""
        temp_raw, processed = tmp_dirs
        f = create_mock_json_gz(temp_raw, "CompDS", "2024-01-01", records=100)
        result = proc.convert_and_merge("CompDS", [f], "202401")
        
        # 壓縮檔應比原始 JSON 小很多
        json_size = f.stat().st_size
        parquet_size = result.stat().st_size
        # Parquet 由於列式儲存+壓縮，應該更有效率
        assert parquet_size > 0


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
class TestProcessDataset:
    """完整資料集處理測試"""

    def test_process_single_dataset(self, tmp_dirs):
        """處理單一資料集的完整流程"""
        temp_raw, processed = tmp_dirs
        
        # 建立 5 天的模擬資料 (跨兩個月)
        for d in ["2024-01-15", "2024-01-16", "2024-01-17", "2024-02-01", "2024-02-02"]:
            create_mock_json_gz(temp_raw, "FullDS", d, records=3)

        proc.process_dataset("FullDS", delete_after=False)

        # 應產出 2 個月檔
        parquets = list((processed / "FullDS").glob("*.parquet"))
        assert len(parquets) == 2

    def test_delete_json_after_convert(self, tmp_dirs):
        """轉檔後應刪除原始 JSON"""
        temp_raw, processed = tmp_dirs
        create_mock_json_gz(temp_raw, "DelDS", "2024-01-01")

        proc.process_dataset("DelDS", delete_after=True)

        remaining_json = list((temp_raw / "DelDS").glob("*.json.gz"))
        assert len(remaining_json) == 0

    def test_keep_json_option(self, tmp_dirs):
        """delete_after=False 時應保留原始 JSON"""
        temp_raw, processed = tmp_dirs
        create_mock_json_gz(temp_raw, "KeepDS", "2024-01-01")

        proc.process_dataset("KeepDS", delete_after=False)

        remaining_json = list((temp_raw / "KeepDS").glob("*.json.gz"))
        assert len(remaining_json) == 1

    def test_empty_directory_no_error(self, tmp_dirs):
        """空目錄不應報錯"""
        proc.process_dataset("NonExistentDS", delete_after=True)
        # 不應拋出任何異常
