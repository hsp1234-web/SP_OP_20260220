"""
V4.2 資料集白名單測試 (test_datasets_registry.py)
"""
import pytest
import sys
from pathlib import Path

# 確保專案根目錄在 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.datasets_registry import (
    ALL_DATASETS,
    DATASETS_BY_NAME,
    TRADING_CALENDAR,
    get_dataset_names,
    get_datasets_by_tier,
    get_datasets_by_category,
    is_valid_dataset,
    validate_dataset_name,
    DatasetConfig,
)


class TestDatasetsRegistry:
    """資料集白名單測試"""

    def test_total_count(self):
        """白名單應包含 7 個目標資料集"""
        assert len(ALL_DATASETS) == 7

    def test_all_names_unique(self):
        """所有資料集名稱應唯一"""
        names = get_dataset_names()
        assert len(names) == len(set(names))

    def test_trading_calendar_exists(self):
        """交易日曆 (Phase 0) 應獨立於主清單"""
        assert TRADING_CALENDAR.name == "TaiwanStockTradingDate"
        assert TRADING_CALENDAR not in ALL_DATASETS

    def test_s_tier_count(self):
        """S 級資料集應有 5 個"""
        s_tier = get_datasets_by_tier("S")
        assert len(s_tier) == 5

    def test_a_tier_count(self):
        """A 級資料集應有 2 個"""
        a_tier = get_datasets_by_tier("A")
        assert len(a_tier) == 2

    def test_all_bulk_mode_true(self):
        """所有目標資料集的 bulk_mode 應為 True (全市場下載)"""
        for ds in ALL_DATASETS:
            assert ds.bulk_mode is True, f"{ds.name} bulk_mode 應為 True"

    def test_chip_category(self):
        """籌碼面資料集應有 5 個"""
        chips = get_datasets_by_category("chip")
        assert len(chips) == 5

    def test_valid_dataset_check(self):
        """合法資料集名稱檢查"""
        assert is_valid_dataset("TaiwanStockPriceAdj") is True
        assert is_valid_dataset("FakeDataset") is False

    def test_validate_raises_on_invalid(self):
        """無效名稱應拋出 ValueError"""
        with pytest.raises(ValueError, match="無效的資料集名稱"):
            validate_dataset_name("NotExist")

    def test_validate_returns_config(self):
        """合法名稱應回傳 DatasetConfig"""
        config = validate_dataset_name("TaiwanStockPriceAdj")
        assert isinstance(config, DatasetConfig)
        assert config.label_zh == "台灣還原股價"
        assert config.tier == "S"

    def test_datasets_by_name_dict(self):
        """DATASETS_BY_NAME 字典應包含所有資料集"""
        assert len(DATASETS_BY_NAME) == 7
        assert "TaiwanFuturesInstitutionalInvestors" in DATASETS_BY_NAME

    def test_all_have_default_start(self):
        """所有資料集應有預設起始日期"""
        for ds in ALL_DATASETS:
            assert ds.default_start, f"{ds.name} 缺少 default_start"
            # 格式檢查 YYYY-MM-DD
            parts = ds.default_start.split("-")
            assert len(parts) == 3, f"{ds.name} default_start 格式錯誤"

    def test_frozen_dataclass(self):
        """DatasetConfig 應為不可變 (frozen)"""
        ds = ALL_DATASETS[0]
        with pytest.raises(AttributeError):
            ds.name = "Hacked"

    def test_expected_dataset_names(self):
        """確認所有 7 個預期的資料集名稱都存在"""
        expected = {
            "TaiwanStockPriceAdj",
            "TaiwanFuturesInstitutionalInvestors",
            "TaiwanOptionInstitutionalInvestors",
            "TaiwanFuturesOpenInterestLargeTraders",
            "TaiwanOptionOpenInterestLargeTraders",
            "TaiwanTotalExchangeMarginMaintenance",
            "TaiwanStockInstitutionalInvestorsBuySell",
        }
        actual = set(get_dataset_names())
        assert actual == expected
