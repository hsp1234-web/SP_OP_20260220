# 量化數據中台 (QuantDataPipeline)

本專案為高效能金融數據中台，旨在將原始金融數據 (L1) 經過標準化處理轉換為分析就緒的 Parquet 格式 (L4)，並支援高併發、斷點續傳與嚴格的資料驗證。

## 系統架構

本系統採用 ETL (Extract, Transform, Load) 架構，資料流向如下：

```mermaid
graph LR
    A[FinMind API] -->|HTTPSession (Retries/RateLimit)| B(Fetchers)
    B -->|SchemaEnforcer (型別校驗/補零)| C(Parsers)
    C -->|Polars DataFrame| D(Processors)
    D -->|Atomic Write (.tmp -> .parquet)| E[Storage (L4 Data)]
    E -->|Status Update| F[SQLite DB (WAL Mode)]
```

*   **L1 (Raw Data)**: 從 FinMind API 獲取原始 JSON 數據。
*   **L2 (Normalized)**: 經過 Schema 強制轉型 (Date: Datetime[ns], StockID: Utf8)。
*   **L4 (Ready-to-Use)**: 儲存為高效能 Parquet 檔案，供回測或分析使用。

## 環境安裝

本專案建議使用 Python 3.10 以上版本。

1.  **複製專案**
    ```bash
    git clone https://github.com/your-repo/QuantDataPipeline.git
    cd QuantDataPipeline
    ```

2.  **安裝依賴套件**
    ```bash
    pip install -r requirements.txt
    ```

3.  **環境變數設定 (非必要)**
    若擁有 FinMind 付費帳號，可設定環境變數以提升 API 限額：
    ```bash
    export FINMIND_API_TOKEN="your_token_here"
    ```

## 快速上手

執行 `main.py` 即可啟動數據管線。系統會自動抓取交易日並產生任務。

### 範例 1：抓取特定日期範圍
```bash
# 抓取 2023-10-02 到 2023-10-04 的數據
python3 QuantDataPipeline/main.py --start_date 2023-10-02 --end_date 2023-10-04 --workers 4
```

### 範例 2：預設執行 (最近 3 天)
```bash
python3 QuantDataPipeline/main.py
```

### 驗證執行結果
執行驗證腳本以確保資料完整性與系統穩定性：
```bash
python3 QuantDataPipeline/tests/validate_pipeline.py
```

## 資料規格 (Schema)

所有資料皆儲存為 Parquet 格式，並遵循以下嚴格型別定義：

### 核心欄位規範
*   **date / timestamp**: `Datetime(time_unit='ns')` (奈秒精度時間戳記)
*   **stock_id**: `Utf8` (字串格式，保留前綴零，如 `0050`, `2330`)

### 主要資料集範例

#### 1. TaiwanStockPrice (個股日成交資訊)
| 欄位名稱 | 型別 | 說明 |
| :--- | :--- | :--- |
| date | Datetime[ns] | 交易日期 |
| stock_id | Utf8 | 股票代碼 (如 2330) |
| open | Float64 | 開盤價 |
| max | Float64 | 最高價 |
| min | Float64 | 最低價 |
| close | Float64 | 收盤價 |
| Trading_Volume | Int64 | 成交量 |
| Trading_money | Int64 | 成交金額 |

#### 2. TaiwanOptionOpenInterestLargeTraders (期貨大額交易人)
| 欄位名稱 | 型別 | 說明 |
| :--- | :--- | :--- |
| date | Datetime[ns] | 交易日期 |
| contract_id | Utf8 | 契約代碼 (如 TXO) |
| buy_volume | Int64 | 買方口數 |
| sell_volume | Int64 | 賣方口數 |

## 目錄結構說明

```
QuantDataPipeline/
├── core/                   # 核心組件 (Config, DB Manager, Logger)
│   ├── db_metadata_manager.py # SQLite WAL 連線管理與任務狀態
│   └── ...
├── fetchers/               # 資料爬取層 (L1 -> L2)
│   ├── datasets/           # 各類資料集實作 (Technical, Chip, Derivative)
│   ├── infrastructure/     # 網路基礎設施 (HTTPSession, Retry, RateLimit)
│   └── parsers/            # 資料解析與 Schema 驗證 (SchemaEnforcer)
├── processors/             # 資料處理層 (L2 -> L3/L4) (如指標計算)
├── storage/                # 儲存層 (Parquet Writer, Integrity Validator)
├── tests/                  # 測試與驗證腳本
├── data/                   # 產出的 Parquet 資料檔案 (依年份分資料夾)
├── status.db               # 任務狀態與 API 統計資料庫
├── main.py                 # 程式進入點
└── requirements.txt        # 專案依賴列表
```

## 在地化規範

*   本專案之 GitHub 說明、Commit Messages、程式碼註解及日誌輸出皆採用 **繁體中文**。
*   日期格式統一使用 `YYYY-MM-DD`。
*   時間戳記統一使用 Polars `ns` 精度。
