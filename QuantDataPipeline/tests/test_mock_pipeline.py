import unittest
import json
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock
from datetime import datetime

# Ensure project root is in path
sys.path.append(str(Path(__file__).parent))

import polars as pl
from fetchers.parsers.finmind_extractor import extract_and_cast
from storage.parquet_writer import save_dataframe

logging.basicConfig(level=logging.INFO)

class TestPipeline(unittest.TestCase):
    def setUp(self):
        # Load mock data
        self.mock_file = Path(__file__).parent / "test_mock.json"
        if not self.mock_file.exists():
            self.skipTest("Mock data file not found")

        with open(self.mock_file, "r", encoding="utf-8") as f:
            self.mock_json = json.load(f)

    def test_extract_and_cast_schema(self):
        """Test L1 Extraction and Schema Enforcement using mock data."""
        import requests
        mock_response = MagicMock(spec=requests.Response)
        mock_response.json.return_value = self.mock_json
        mock_response.raise_for_status.return_value = None

        # Test extraction (assuming mock data is TaiwanStockPrice based on previous step)
        dataset_name = "TaiwanStockPrice"
        try:
            df = extract_and_cast(mock_response, dataset_name)
        except Exception as e:
            self.fail(f"extract_and_cast raised exception: {e}")

        self.assertFalse(df.is_empty(), "DataFrame should not be empty")

        # Verify schema enforcement
        # TaiwanStockPrice should have Trading_Volume as Int64
        if "Trading_Volume" in df.columns:
            self.assertEqual(df.schema["Trading_Volume"], pl.Int64, "Trading_Volume should be Int64")

        # Verify date casting (ns precision)
        if "date" in df.columns:
            # Polars Datetime(time_unit='ns')
            self.assertEqual(df.schema["date"].time_unit, 'ns', "Date should have ns time unit")

    def test_parquet_writer(self):
        """Test L4 Storage logic."""
        # Create a dummy DataFrame with concrete types
        df = pl.DataFrame({
            "date": [datetime(2023, 10, 2)],
            "col1": [123]
        }).with_columns(
            pl.col("date").cast(pl.Datetime("ns"))
        )

        # Save
        dataset_name = "TestDataset"
        date = "2023-10-02"
        data_id = "TEST"

        try:
            path, checksum = save_dataframe(df, dataset_name, date, data_id)

            self.assertTrue(Path(path).exists(), "Parquet file should exist")
            self.assertTrue(Path(path).stat().st_size > 0, "File should not be empty")
            self.assertIsNotNone(checksum, "Checksum should be returned")

            # Cleanup
            p = Path(path)
            if p.exists():
                p.unlink()
            if p.parent.exists():
                try:
                    p.parent.rmdir() # Might fail if not empty
                except OSError:
                    pass
        except Exception as e:
             self.fail(f"save_dataframe failed: {e}")

if __name__ == "__main__":
    unittest.main()
