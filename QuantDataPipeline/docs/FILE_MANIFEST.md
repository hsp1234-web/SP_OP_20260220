# 專案檔案清單與功能說明 (File Manifest)

本文件詳細列出 `QuantDataPipeline` 專案中各個檔案的功能、職責與使用的技術棧，供後續開發者快速理解系統架構。

## 目錄結構

```
QuantDataPipeline/
├── core/                   # 核心組件 (Config, DB Manager, Logger)
├── fetchers/               # 資料爬取層 (L1 -> L2)
│   ├── datasets/           # 各類資料集實作
│   ├── infrastructure/     # 網路基礎設施
│   └── parsers/            # 資料解析與 Schema 驗證
├── processors/             # 資料處理層 (L2 -> L3/L4)
├── storage/                # 儲存層
├── tests/                  # 測試與驗證腳本
├── data/                   # 資料產出目錄
├── docs/                   # 文件目錄
├── status.db               # 任務狀態資料庫
├── main.py                 # 程式進入點
└── requirements.txt        # 依賴清單
```

## 詳細檔案說明

### 根目錄

#### `main.py`
*   **功能**: 程式的主要進入點。負責解析命令列參數、初始化資料庫與 Session、抓取交易日並產生任務、以及使用執行緒池 (ThreadPoolExecutor) 並行執行資料爬取任務。
*   **技術棧**: `argparse`, `concurrent.futures`, `logging`
*   **職責**: 任務排程與協調 (Orchestrator)。

#### `requirements.txt`
*   **功能**: 列出專案執行所需的所有 Python 套件。
*   **內容包含**: `polars`, `requests`, `duckdb`, `zstandard`, `fastapi`, `uvicorn`, `python-dotenv` 等。

---

### `core/` (核心組件)

#### `core/config.py`
*   **功能**: 集中管理全域設定，如資料目錄路徑、資料庫路徑、API Token、重試參數、速率限制等。
*   **技術棧**: `pathlib`, `os`

#### `core/db_metadata_manager.py`
*   **功能**: 封裝 SQLite 資料庫操作。實作 `DBManager` 類別 (Singleton)，管理任務狀態 (`task_registry`) 與 API 呼叫統計 (`api_call_stats`)。
*   **技術棧**: `sqlite3` (WAL Mode), `threading.local` (Thread-safety)
*   **關鍵機制**: 使用 Write-Ahead Logging (WAL) 模式以支援高併發寫入。

#### `core/pipeline_logger.py`
*   **功能**: 設定 logging 的格式與輸出目標 (檔案與終端機)。

---

### `fetchers/` (資料爬取層)

#### `fetchers/infrastructure/http_session.py`
*   **功能**: 封裝 `requests.Session`。實作 `HTTPSession` 類別 (Singleton)，負責執行 HTTP 請求並自動記錄 API 呼叫統計 (Latency, Status Code)。
*   **技術棧**: `requests`, `threading.Lock`

#### `fetchers/infrastructure/rate_limiter.py`
*   **功能**: 實作全域速率限制器 (Singleton)，確保不超過 FinMind API 的請求限制。
*   **技術棧**: `time`, `threading.Lock`

#### `fetchers/infrastructure/backoff_retry.py`
*   **功能**: 提供 `exponential_backoff` 裝飾器，針對特定異常 (如網路錯誤) 執行指數退避重試。
*   **技術棧**: `functools`, `time`, `random`

#### `fetchers/parsers/finmind_extractor.py`
*   **功能**: 負責將 API 回傳的 JSON 資料轉換為 Polars DataFrame。處理錯誤檢查與空值判斷。
*   **技術棧**: `polars`

#### `fetchers/parsers/schema_enforcer.py`
*   **功能**: 負責強制執行資料型別轉換 (Schema Enforcement)。確保日期為 `Datetime[ns]`，股票代碼為補零後的 `Utf8` 字串。
*   **技術棧**: `polars`

#### `fetchers/parsers/payload_builder.py`
*   **功能**: 建構 API 請求參數。

#### `fetchers/datasets/technical/`
*   **`stock_price.py`**: 抓取個股日成交資訊 (TaiwanStockPrice)。
*   **`stock_price_tick.py`**: 抓取個股逐筆成交資訊 (TaiwanStockPriceTick)。
*   **`trading_date.py`**: 抓取交易日列表 (TaiwanStockTradingDate)。

#### `fetchers/datasets/derivative/`
*   **`option_tick.py`**: 抓取期貨逐筆成交資訊 (TaiwanOptionTick)。

#### `fetchers/datasets/chip/`
*   **`large_traders.py`**: 抓取期貨大額交易人資訊 (TaiwanOptionOpenInterestLargeTraders)。

---

### `storage/` (儲存層)

#### `storage/parquet_writer.py`
*   **功能**: 將 Polars DataFrame 寫入 Parquet 檔案。實作原子性寫入機制 (先寫入 `.tmp` 再 `rename`) 與 MD5 校驗。
*   **技術棧**: `polars`, `os`, `hashlib`

#### `storage/integrity_validator.py`
*   **功能**: 計算檔案 MD5 雜湊值，用於完整性驗證。

---

### `tests/` (測試與驗證)

#### `tests/validate_pipeline.py`
*   **功能**: 自動化驗證腳本。測試 Schema 嚴格性、斷點續傳機制、高併發寫入穩定性、異常重試邏輯與原子性寫入。
*   **技術棧**: `unittest`, `unittest.mock`

#### `tests/test_mock_pipeline.py`
*   **功能**: 使用 Mock 資料進行單元測試。
