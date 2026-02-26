"""
V4.2 資料集白名單 (Datasets Registry)
定義所有目標資料集的靜態清單，作為下載管線的唯一合法來源。
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass(frozen=True)
class DatasetConfig:
    """單一資料集的設定"""
    name: str                    # FinMind API 的 dataset 參數值
    label_zh: str               # 中文標籤
    category: str               # 分類: technical / chip / sentiment / derivative
    tier: str                   # 等級: S / A / B
    bulk_mode: bool = True      # True = 留空 data_id, 全市場一次下載
    default_start: str = "2010-01-01"  # 冷啟動時的預設起始日期
    notes: str = ""             # 備註


# ========== 交易日曆 (Phase 0 基礎設施) ==========
TRADING_CALENDAR = DatasetConfig(
    name="TaiwanStockTradingDate",
    label_zh="交易日曆",
    category="infrastructure",
    tier="S",
    bulk_mode=True,
    default_start="2015-01-01",
    notes="Phase 0 獨立處理，啟動時一次性取得完整日曆",
)

# ========== S 級核心資料集 ==========
_S_TIER: List[DatasetConfig] = [
    DatasetConfig(
        name="TaiwanStockPriceAdj",
        label_zh="台灣還原股價",
        category="technical",
        tier="S",
        bulk_mode=True,
        default_start="2015-01-01",
        notes="全市場橫斷面，一次請求取得所有個股",
    ),
    DatasetConfig(
        name="TaiwanFuturesInstitutionalInvestors",
        label_zh="期貨三大法人",
        category="chip",
        tier="S",
        bulk_mode=True,
        default_start="2018-06-05",
    ),
    DatasetConfig(
        name="TaiwanOptionInstitutionalInvestors",
        label_zh="選擇權三大法人",
        category="chip",
        tier="S",
        bulk_mode=True,
        default_start="2018-06-05",
    ),
    DatasetConfig(
        name="TaiwanFuturesOpenInterestLargeTraders",
        label_zh="期貨大額交易人",
        category="chip",
        tier="S",
        bulk_mode=True,
        default_start="1998-07-01",
    ),
    DatasetConfig(
        name="TaiwanOptionOpenInterestLargeTraders",
        label_zh="選擇權大額交易人",
        category="chip",
        tier="S",
        bulk_mode=True,
        default_start="1998-07-01",
    ),
]

# ========== A 級輔助資料集 ==========
_A_TIER: List[DatasetConfig] = [
    DatasetConfig(
        name="TaiwanTotalExchangeMarginMaintenance",
        label_zh="大盤融資維持率",
        category="sentiment",
        tier="A",
        bulk_mode=True,
        default_start="2015-01-01",
        notes="散戶斷頭預警指標",
    ),
    DatasetConfig(
        name="TaiwanStockInstitutionalInvestorsBuySell",
        label_zh="個股三大法人買賣超",
        category="chip",
        tier="A",
        bulk_mode=True,
        default_start="2015-01-01",
        notes="全市場橫斷面，一次請求取得所有個股",
    ),
]

# ========== 合併所有目標資料集 ==========
ALL_DATASETS: List[DatasetConfig] = _S_TIER + _A_TIER

# 建立以 name 為 key 的快速查詢字典
DATASETS_BY_NAME: Dict[str, DatasetConfig] = {
    ds.name: ds for ds in ALL_DATASETS
}


def get_dataset_names() -> List[str]:
    """取得所有目標資料集的名稱清單"""
    return [ds.name for ds in ALL_DATASETS]


def get_datasets_by_tier(tier: str) -> List[DatasetConfig]:
    """依等級篩選資料集 (S / A / B)"""
    return [ds for ds in ALL_DATASETS if ds.tier == tier]


def get_datasets_by_category(category: str) -> List[DatasetConfig]:
    """依分類篩選資料集 (technical / chip / sentiment)"""
    return [ds for ds in ALL_DATASETS if ds.category == category]


def is_valid_dataset(name: str) -> bool:
    """檢查資料集名稱是否在白名單中"""
    return name in DATASETS_BY_NAME


def validate_dataset_name(name: str) -> DatasetConfig:
    """驗證資料集名稱並回傳設定，無效時拋出 ValueError"""
    if name not in DATASETS_BY_NAME:
        valid_names = ", ".join(get_dataset_names())
        raise ValueError(
            f"無效的資料集名稱: '{name}'. "
            f"合法名稱: [{valid_names}]"
        )
    return DATASETS_BY_NAME[name]
