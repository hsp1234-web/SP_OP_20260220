# 專案檔案清單與功能說明 (File Manifest)

本文件詳細列出 `QuantDataPipeline` 專案中**每一個檔案**的功能、職責、核心邏輯與使用的技術棧，供後續工程團隊快速理解系統架構。

> **最後更新**: 2026-02-20  
> **Python 版本**: 3.10+  
> **核心依賴**: Polars, Numba, SQLite(WAL), Requests

---

## 目錄總覽

```
QuantDataPipeline/
├── core/                           # L0 核心層：設定、DB管理、日誌
│   ├── config.py
│   ├── db_metadata_manager.py
│   ├── fetch_orchestrator.py
│   └── pipeline_logger.py
├── fetchers/                       # L1 資料爬取層
│   ├── datasets/                   # 各類 FinMind API 資料集
│   │   ├── technical/
│   │   │   ├── stock_price.py
│   │   │   ├── stock_price_tick.py
│   │   │   └── trading_date.py
│   │   ├── derivative/
│   │   │   ├── option_tick.py
│   │   │   └── futures_tick.py
│   │   └── chip/
│   │       └── large_traders.py
│   ├── infrastructure/             # 網路基礎設施
│   │   ├── http_session.py
│   │   ├── rate_limiter.py
│   │   └── backoff_retry.py
│   └── parsers/                    # 資料解析與型別強制
│       ├── finmind_extractor.py
│       ├── schema_enforcer.py
│       └── payload_builder.py
├── processors/                     # L2 計算與特徵工程層
│   ├── greeks_engine.py
│   ├── market_microstructure.py
│   ├── timeframe_aggregator.py
│   ├── session_aligner.py
│   └── options_greeks.py
├── storage/                        # L3 儲存與完整性驗證層
│   ├── parquet_writer.py
│   ├── integrity_validator.py
│   └── monthly_roller.py
├── tests/                          # 測試與驗證
│   ├── conftest.py
│   ├── test_run_all.py
│   ├── test_timeframe_aggregator.py
│   ├── test_market_microstructure.py
│   ├── test_mock_pipeline.py
│   ├── validate_pipeline.py
│   ├── test_mock.json
│   └── experiments/
│       ├── generate_mock_data.py
│       ├── test_10days_pipeline.py
│       ├── test_asof_join.py
│       ├── test_greeks.py
│       └── test_performance.py
├── data/                           # 資料產出目錄 (自動建立，已 gitignore)
│   └── {year}/
│       ├── TaiwanOptionTick/       # TXO_{date}.parquet
│       ├── TaiwanFuturesTick/      # TX_{date}.parquet
│       ├── GreeksFeatures/         # TXO_Greeks_{date}.parquet
│       └── Features/
│           ├── daily/              # 日檔散檔
│           └── monthly/            # 月度合併檔
├── docs/                           # 文件
│   ├── FILE_MANIFEST.md            # 本檔案
│   └── HANDOVER_SPEC_V2.md        # 開發規格書
├── colab_launcher.ipynb            # ⭐ Colab 一鍵啟動器 (表單控制面板)
├── main.py                         # L1 下載管線入口
├── run_all.py                      # 全自動化管線入口 (下載 + 計算)
├── compute_greeks_pipeline.py      # L2 Greeks 計算管線
├── requirements.txt                # Python 依賴清單
├── status.db                       # SQLite 任務狀態資料庫
├── pipeline.log                    # 滾動式日誌檔
└── .env                            # 環境變數 (Token + 額度)
```

---

## 一、根目錄檔案

### `main.py` — L1 下載管線入口
- **職責**: 純粹的 API 資料下載入口點。解析命令列參數 (`--start_date`, `--end_date`, `--workers`)，透過交易日曆取得有效交易日，向 `status.db` 註冊任務後，以 `ThreadPoolExecutor` 並行執行下載。
- **核心流程**: 交易日取得 → 任務註冊 → 並行下載 → DB 狀態更新
- **技術棧**: `argparse`, `concurrent.futures.ThreadPoolExecutor`, `logging`
- **用法**: `python main.py --start_date 2024-05-01 --end_date 2024-05-31 --workers 4`
- **注意**: 此入口**僅執行下載** (Phase 1)。若需同時計算 Greeks 請使用 `run_all.py`。

### `run_all.py` — 全自動化管線入口 (P0 核心)
- **職責**: 整合下載 (Phase 1) 與計算 (Phase 2) 的自動化狀態機管線。支援 `--lookback` 倒推模式（從今天往回 N 天），自動跳過已完成任務。
- **核心特性**:
  - 🔄 **狀態機驅動**: 根據 `status.db` 中的 `status` 欄位 (0→1→2) 自動判斷每個任務的下一步。
  - 🧊 **5 分鐘冷卻**: 遇到 API 429 / 額度耗盡時自動 `sleep(300)` 再繼續，絕不崩潰中斷。
  - 🏠 **環境策略**: `--env local` 直接操作本地磁碟；`--env colab` 啟用 Google Drive 同步邏輯。
  - 🔍 **孤兒清理**: 啟動時自動檢查 DB 標記完成但 Parquet 不存在的任務，重置為待處理。
- **技術棧**: `argparse`, `time`, `shutil`, `pathlib`
- **用法**:
  ```bash
  python run_all.py --lookback 30 --env local          # 倒推 30 天
  python run_all.py --start 2024-01-01 --end 2024-01-31  # 指定範圍
  python run_all.py --lookback 60 --env colab            # Colab 模式
  python run_all.py --lookback 30 --skip-phase2          # 只下載不計算
  ```

### `compute_greeks_pipeline.py` — L2 Greeks 計算管線
- **職責**: 純粹的計算管線（無網路）。讀取本地 Parquet (Lazy Loading) → Asof Join 對齊期權與期貨 → 計算結算日真實 `Years_to_Maturity` → Numba 加速 Black-Scholes Greeks → 存回 `GreeksFeatures/` Parquet。
- **核心函數**:
  - `compute_greeks_for_date(date_str, data_dir)` → `(df, path, success)` 三元組
  - `_third_wednesday(year, month)` → 台灣期交所第三個星期三結算日
  - `_calc_years_to_maturity(trade_date, contract_date_str)` → 年化剩餘天數
- **技術棧**: `polars` (Lazy Loading, Asof Join), `numba`, `calendar`
- **API**: 可透過 CLI (`--date 2024-05-02`) 或程式化匯入使用。
- **改進**: 相較舊版硬編碼 `T=0.05`，現已改用結算日日曆計算真實到期時間。

### `colab_launcher.ipynb` — ⭐ Colab 一鍵啟動器
- **職責**: 提供 Google Colab 中的**單儲存格全自動化管線**，透過圖形化表單控制面板讓使用者無需接觸程式碼。
- **表單參數**:
  | 參數 | 類型 | 說明 |
  |------|------|------|
  | `FINMIND_API_TOKEN` | 文字 | FinMind API 金鑰 (需 backer/sponsor 等級) |
  | `API_QUOTA_PER_HOUR` | 整數 | 每小時 API 額度，系統自動計算最佳請求速率 |
  | `BRANCH` | 文字 | GitHub 分支號碼 |
  | `LOOKBACK_DAYS` | 整數 | 回溯天數 (`0` = 全量 2011-01-03 至今) |
  | `SKIP_GREEKS` | 布林 | 是否跳過 Greeks 計算 |
  | `SYNC_TO_DRIVE` | 布林 | 是否同步到 Google Drive |
  | `DRIVE_PATH` | 文字 | Drive 儲存路徑 |
- **核心特性**:
  - 🖥️ **固定高度輸出** (420px) + 右側原生捲軸，畫面不閃爍不洗版
  - 📊 **API 用量預估**: 啟動時顯示預估呼叫數、佔額度百分比、預估耗時
  - 📈 **即時進度**: 每筆任務一行 `✅ 2026-02-02 TXO | 下載成功 [5/43]`
  - ⛔ **永久性錯誤偵測**: 帳號等級不足時第一次就立即中止，不浪費重試
  - 🔄 **模組快取清除**: 每次執行自動清除 `sys.modules` 快取，確保載入最新代碼
  - 🧊 **429 冷卻**: API 限速時自動冷卻 5 分鐘後繼續
  - 📊 **結束統計**: 顯示實際 API 呼叫數、實際速率 vs 額度、總耗時
- **執行流程**: Phase 0 (環境準備) → Phase 1 (資料下載) → Phase 2 (Greeks 計算) → Phase 3 (Drive 同步) → 統計
- **技術棧**: `IPython.display`, `subprocess`, `os.environ`, `sys.modules` 操作

### `requirements.txt` — Python 依賴清單
- **內容**: `polars`, `numba`, `scipy`, `numpy`, `duckdb`, `zstandard`, `requests`, `fastapi`, `uvicorn`, `python-dotenv`, `pytest`, `pytest-cov`
- **安裝**: `pip install -r requirements.txt`

### `status.db` — SQLite 任務狀態資料庫
- **模式**: WAL (Write-Ahead Logging) 確保高併發安全。
- **核心表 `task_registry`**: `task_id` (PK), `trade_date`, `dataset_name`, `data_id`, `status` (0=待處理, 1=L1成功, 2=L2成功, 3=EMPTY_SKIP), `file_hash`。
- **統計表 `api_call_stats`**: 記錄每次 API 呼叫的 `dataset`, `status_code`, `data_count`, `latency_ms`。

### `pipeline.log` — 滾動式日誌檔
- **大小限制**: 10MB × 5 份備份 (`RotatingFileHandler`)
- **格式**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`

### `.env` — 環境變數
- **內容**:
  | 變數 | 說明 |
  |------|------|
  | `FINMIND_API_TOKEN` | FinMind API Token (需 backer/sponsor 等級才能存取逐筆資料) |
  | `FINMIND_QUOTA_PER_HOUR` | 每小時 API 額度 (如 1600)，系統自動計算最佳速率 |
  | `RATE_LIMIT_DELAY` | (選用) 直接指定請求間隔秒數，覆蓋自動計算 |

---

## 二、`core/` — L0 核心層

### `core/config.py` — 全域設定中心
- **職責**: 集中管理所有可調整的全域常數與路徑。
- **核心設定**:
  | 設定 | 預設值 | 說明 |
  |------|--------|------|
  | `PROJECT_ROOT` | 自動偵測 | 專案根目錄 |
  | `DATA_DIR` | `{ROOT}/data/` | Parquet 資料存放處 |
  | `DB_PATH` | `{ROOT}/status.db` | SQLite 路徑 |
  | `FINMIND_API_TOKEN` | 從 `.env` / `os.environ` 讀取 | API 金鑰 |
  | `RATE_LIMIT_DELAY` | 自動計算 | API 速率限制 (秒/請求) |
  | `MAX_RETRIES` | 5 | 重試上限 |
  | `COMPRESSION_LEVEL` | 3 | Zstandard 壓縮等級 |
- **速率分級**:
  | 優先級 | 來源 | 說明 |
  |--------|------|------|
  | 1 | `RATE_LIMIT_DELAY` 環境變數 | 直接指定秒數 |
  | 2 | `FINMIND_QUOTA_PER_HOUR` 環境變數 | 自動計算 (含 10% 安全邊際) |
  | 3 | 有 Token | 6s/req |
  | 4 | 無 Token (匿名) | 12s/req |
- **技術棧**: `pathlib`, `python-dotenv`, `os.getenv`

### `core/db_metadata_manager.py` — 資料庫管理器
- **職責**: 封裝所有 SQLite 操作的 Singleton 類別。提供任務註冊、狀態更新、查詢等介面。
- **核心類別**: `DBManager` (Singleton, Thread-safe)
- **關鍵方法**:
  | 方法 | 說明 |
  |------|------|
  | `register_task(task_id, ...)` | 註冊新任務 (INSERT OR IGNORE) |
  | `update_task_status(task_id, status, file_hash)` | 更新任務狀態 |
  | `get_pending_tasks()` | 取得所有 status=0 任務 |
  | `get_tasks_by_status(status)` | 依狀態過濾任務 |
  | `get_task_status(task_id)` | 查詢單一任務狀態 |
  | `reset_orphan_tasks(data_dir)` | 清理 DB 有記錄但 Parquet 不存在的幽靈任務 |
  | `log_api_call(...)` | 記錄 API 呼叫統計 |
  | `_reset_instance()` | 重置 Singleton (僅供測試) |
- **技術棧**: `sqlite3` (WAL Mode), `threading.local`, `threading.Lock`

### `core/fetch_orchestrator.py` — 任務調度器
- **職責**: 負責執行單一下載任務的完整流程。根據 `dataset_name` 分派到對應的 fetcher 函數, 下載後交給 `parquet_writer` 存檔, 再更新 DB 狀態。
- **核心邏輯**: `process_task(task_id, date, dataset, data_id)` — 下載 → 儲存 → 狀態更新
- **Fetcher 字典**: 透過 `FETCHERS` dict 將 API 名稱映射到對應的 `fetch` 函數，消除長 if-elif 結構。
- **空資料處理**: API 回傳空資料時自動標記為 `EMPTY_SKIP (3)`。
- **技術棧**: 工廠模式 (Factory Pattern)

### `core/pipeline_logger.py` — 日誌設定
- **職責**: 提供統一的 `setup_logger()` 函數，配置 `RotatingFileHandler` (10MB × 5) + `StreamHandler` (Console)。
- **用法**: `logger = setup_logger("pipeline.main", LOG_FILE)`

---

## 三、`fetchers/` — L1 資料爬取層

### `fetchers/infrastructure/http_session.py` — HTTP 連線管理器
- **職責**: 封裝 `requests.Session` 的 Singleton 類別。負責向 FinMind API 發送請求，自動附加 Token，並將每次 API 呼叫的延遲、狀態碼、資料筆數記錄到 `api_call_stats` 表。
- **核心方法**: `get_data(dataset, data_id, start_date, end_date)` → JSON dict
- **Base URL**: `https://api.finmindtrade.com/api/v4/data`
- **Timeout**: 30 秒
- **技術棧**: `requests.Session` (連線池), Singleton

### `fetchers/infrastructure/rate_limiter.py` — 全域速率限制器
- **職責**: Singleton 速率限制器，確保 API 呼叫頻率不超過 FinMind 限制。每次呼叫 `wait()` 會阻塞直到距上次請求已過 `RATE_LIMIT_DELAY` 秒。
- **限速邏輯**: 匿名 12s/req，註冊帳號 6s/req (由 `config.py` 控制)
- **技術棧**: `time.time()`, `threading.Lock`

### `fetchers/infrastructure/backoff_retry.py` — 指數退避重試裝飾器
- **職責**: 提供 `@exponential_backoff` 裝飾器。當被裝飾的函數拋出指定異常時，自動以指數退避策略重試。**永久性錯誤 (帳號等級/Token/權限) 會立即拋出，不浪費重試次數。**
- **退避公式**: `delay = min(base * 2^attempt, max_delay) ± 10% jitter`
- **預設**: 最多 5 次重試, base=1s, max=60s
- **永久性錯誤關鍵字**: `user level`, `sponsor`, `permission denied`, `invalid token`, `unauthorized`
- **核心函數**: `_is_fatal_error(e)` — 檢查錯誤訊息是否包含永久性關鍵字
- **技術棧**: `functools.wraps`, `random.uniform` (Jitter)

### `fetchers/parsers/finmind_extractor.py` — API 回應解析器
- **職責**: 將 FinMind API 的 JSON 回應或 `requests.Response` 物件轉換為嚴格型別的 Polars DataFrame。處理 API 錯誤訊息 (`msg != "success"`)、空資料防護，並委派 `schema_enforcer` 進行型別強制。
- **核心函數**: `extract_and_cast(data_source, dataset_name)` → `pl.DataFrame`
- **防護**: HTTP 錯誤 → `raise_for_status()`, JSON 解析失敗 → `ValueError`, 空資料 → 空 DataFrame

### `fetchers/parsers/schema_enforcer.py` — Schema 強制轉型器
- **職責**: 確保所有輸出 DataFrame 的型別一致性。核心規則:
  - `date` → `Datetime(time_unit='ns')` (奈秒精度)
  - `stock_id` → `Utf8` (補零至 4 位，如 `0050`)
  - 針對不同 dataset 有專屬的欄位型別映射
- **核心類別**: `SchemaEnforcer` (靜態方法)
  - `sanitize_stock_id()` — 補零邏輯
  - `apply_standard_types()` — 依 dataset 做型別轉換
- **支援 Dataset**: `TaiwanStockPrice`, `TaiwanStockPriceTick`, `TaiwanStockTradingDate`

### `fetchers/parsers/payload_builder.py` — API 請求參數建構器
- **職責**: 建構發送到 FinMind API 的請求參數字典。

### `fetchers/datasets/technical/stock_price.py` — 個股日成交行情
- **職責**: 抓取 `TaiwanStockPrice` — 台灣個股每日收盤行情 (OHLCV)。
- **函數**: `fetch(date, data_id)` → `pl.DataFrame`

### `fetchers/datasets/technical/stock_price_tick.py` — 個股逐筆成交
- **職責**: 抓取 `TaiwanStockPriceTick` — 台灣個股逐筆成交明細。
- **函數**: `fetch(date, data_id)` → `pl.DataFrame`

### `fetchers/datasets/technical/trading_date.py` — 交易日曆
- **職責**: 抓取 `TaiwanStockTradingDate` — 台灣證交所/期交所的官方交易日清單。`run_all.py` 依賴此模組判斷哪些日期是有效交易日。
- **函數**: `fetch_trading_dates(session, start_date, end_date)` → `pl.DataFrame`

### `fetchers/datasets/derivative/option_tick.py` — 選擇權逐筆成交
- **職責**: 抓取 `TaiwanOptionTick` — TXO 選擇權逐筆成交明細。包含 `ExercisePrice`, `PutCall`, `contract_date` 等期權專屬欄位。
- **函數**: `fetch(date, data_id)` → `pl.DataFrame`

### `fetchers/datasets/derivative/futures_tick.py` — 期貨逐筆成交
- **職責**: 抓取 `TaiwanFuturesTick` — TX 期貨逐筆成交明細。其 `price` 欄位作為選擇權的標的價格 (`Underlying_S`)。
- **函數**: `fetch(date, data_id)` → `pl.DataFrame`

### `fetchers/datasets/chip/large_traders.py` — 大額交易人
- **職責**: 抓取 `TaiwanOptionOpenInterestLargeTraders` — 期貨大額交易人未平倉資訊。
- **函數**: `fetch(date, data_id)` → `pl.DataFrame`

---

## 四、`processors/` — L2 計算與特徵工程層

### `processors/greeks_engine.py` — Numba 加速 BSM Greeks 引擎
- **職責**: 系統最核心的計算引擎。使用 Numba JIT 編譯的 Black-Scholes Model，從選擇權價格反推 Implied Volatility (IV) 並計算一階/二階 Greeks。
- **核心函數**:
  - `calculate_greeks(df, r)` → `pl.DataFrame` (掛載 IV, Delta, Gamma, Vega, Theta, Vanna, Charm)
  - `_bsm_iv_and_greeks_kernel(S, K, T, r, price, is_call)` → 7 個 numpy 陣列
  - `_numba_norm_cdf(x)` / `_numba_norm_pdf(x)` — Numba 加速的常態分配函數
- **IV 逼近**: 二分法 (40 次迭代, 精度 1e-4), 範圍 0.01% ~ 300%
- **防呆**: T≤0、S≤0、K≤0、P≤0 時回傳全 0
- **性能**: 1 秒內計算數十萬筆 Greeks
- **技術棧**: `numba.vectorize`, `numba.jit(nopython=True, cache=True)`, `numpy`

### `processors/market_microstructure.py` — 全市場微觀摘要特徵
- **職責**: 計算每個時間窗口的市場級摘要特徵（表 B 的核心）。這些特徵是量化 AI 模型的 Alpha 訓練資料來源。
- **核心函數與公式**:
  | 函數 | 輸出特徵 | 公式/邏輯 |
  |------|---------|----------|
  | `calculate_pcr_volume(df)` | PCR_Volume | Put 成交量 / Call 成交量 |
  | `calculate_net_gex(df)` | Net_GEX | Σ(Gamma × volume × S² × 0.01), Call正Put負 |
  | `calculate_rv(df)` | RV | √Σ(log_return²), 基於 Underlying_S |
  | `calculate_iv_skew(df)` | IV_Skew | ATM_Put_IV - ATM_Call_IV |
  | `calculate_iv_slope_25d(df)` | IV_Slope_25D | IV(25D Put) - ATM_IV |
  | `calculate_iv_curvature(df)` | IV_Curvature | IV(25D Put) + IV(25D Call) - 2×ATM_IV |
  | `calculate_vrp_daily(df)` | VRP_Daily | ATM_IV² - RV_Daily (僅日級) |
  | `compute_market_summary(df, tf)` | 完整摘要 dict | 整合以上所有特徵 |
- **ATM 判定**: `abs(ExercisePrice - Underlying_S)` 最小
- **25D 判定**: `abs(Delta - 0.25)` 最小的 OTM 合約
- **技術棧**: `polars`, `numpy`

### `processors/timeframe_aggregator.py` — 多週期降採樣聚合器
- **職責**: 將 Tick 級 Greeks DataFrame 降採樣為多時間週期的「雙表結構」。
- **支援週期**: `1m` (1分鐘), `1h` (1小時), `4h` (4小時), `1d` (1天)
- **核心函數**:
  | 函數 | 產出 | 說明 |
  |------|------|------|
  | `aggregate_contract_bars(df, tf)` | 表 A | 每合約每週期的 OHLCV + Greeks last 值 |
  | `aggregate_market_summary(df, tf)` | 表 B | 每週期 1 筆市場摘要 (委派 `market_microstructure`) |
  | `process_all_timeframes(df, date)` | 4 組雙表 | `{"1m": (bars, summary), "1h": ..., ...}` |
- **表 A 分群鍵**: `[option_id, contract_date, PutCall, ExercisePrice]`
- **技術棧**: `polars.group_by_dynamic`

### `processors/session_aligner.py` — 交易時段對齊器
- **職責**: 識別 TAIFEX 日盤 / 盤後交易時段。為每筆 tick 添加 `session` 欄位 (`"Regular"` / `"AfterHours"`)。
- **時段切分**: 15:00 為分界線
- **目前狀態**: 基礎實作完成，需搭配結算日日曆做更精確的 trade_date 判定。

### `processors/options_greeks.py` — Greeks 計算空殼 (已被 `greeks_engine.py` 取代)
- **狀態**: ⚠️ **空殼未實作**。`greeks_engine.py` 才是實際使用的 Greeks 計算引擎。此檔案保留為歷史參考。

---

## 五、`storage/` — L3 儲存層

### `storage/parquet_writer.py` — Parquet 原子性寫入器
- **職責**: 將 Polars DataFrame 安全地寫入 Parquet 檔案。採用**原子性寫入**機制防止斷電/崩潰導致檔案損壞。
- **寫入流程**: DataFrame → `.tmp` 暫存 → MD5 校驗 → `os.rename()` → `.parquet` 最終檔
- **路徑策略**: `data/{year}/{dataset_name}/{data_id}_{date}.parquet`
- **壓縮**: Zstandard (level 3)
- **技術棧**: `polars.write_parquet`, `os.rename` (原子操作)

### `storage/integrity_validator.py` — 檔案完整性校驗
- **職責**: 提供 `compute_md5(file_path)` 函數，以 4KB chunk 方式計算檔案的 MD5 雜湊值。用於寫入後驗證。
- **技術棧**: `hashlib.md5`

### `storage/monthly_roller.py` — 月度打包與混合儲存管理
- **職責**: 實作「未滿月日檔 + 過往月份月檔」的混合儲存架構。解決 Google Drive I/O 壓力與資料即時性的平衡。
- **核心函數**:
  | 函數 | 說明 |
  |------|------|
  | `save_feature_daily(df, data_dir, table_type, tf, date)` | 存入 `Features/daily/` 目錄 |
  | `rollover_month(data_dir, table_type, tf, year_month)` | 將散檔合併為月檔 |
  | `check_and_rollover(data_dir)` | 自動檢查並觸發跨月結算 |
  | `list_daily_files(data_dir, table_type, tf, year_month)` | 列出指定月份散檔 |
- **結算邏輯**: 啟動時掃描 `daily/`，若發現非當月檔案則自動合併為 `monthly/{year_month}.parquet` 並刪除散檔。

---

## 六、`tests/` — 測試與驗證

### `tests/conftest.py` — pytest 核心 Fixtures
- **職責**: 提供所有測試共用的 fixture，避免重複初始化。
- **核心 Fixtures**:
  | Fixture | Scope | 說明 |
  |---------|-------|------|
  | `tmp_db` | function | 臨時 SQLite DB（繞過 Singleton，每測試獨立） |
  | `tmp_data_dir` | function | 臨時資料目錄 |
  | `mock_session` | function | Mock HTTP session |
  | `mock_finmind_response` | function | Mock FinMind API 回應工廠 |
  | `sample_tick_df` | function | 200 筆選擇權 tick DataFrame |
  | `sample_futures_tick_df` | function | 300 筆期貨 tick DataFrame |
  | `sample_greeks_df` | function | 500 筆含 Greeks 的 DataFrame |

### `tests/test_run_all.py` — 自動化管線測試 (12 cases)
- **測試範圍**: lookback 日期生成、DB 狀態機 (跳過/重試)、429 冷卻偵測、孤兒清理、結算日計算、Years_to_Maturity、compute_greeks 回傳格式

### `tests/test_timeframe_aggregator.py` — 多週期聚合測試 (16 cases)
- **測試範圍**: OHLCV 產出、trade_count、high≥low 不變量、多週期 bar 數量遞減、Greeks last 值、VRP 存在性、空 DataFrame 防呆、不支援週期拋錯、雙表結構

### `tests/test_market_microstructure.py` — 市場特徵測試 (20 cases)
- **測試範圍**: PCR 數值正確性 (已知值驗證)、GEX 正負號 (Call正Put負)、ATM/25D IV 偵測、IV_Skew/Slope/Curvature 可計算性、RV 非負性/常數價格為零、VRP 可計算性、compute_market_summary 完整性

### `tests/test_mock_pipeline.py` — Mock 資料單元測試 (2 cases)
- **測試範圍**: `extract_and_cast` 的 Schema 正確性、`parquet_writer` 讀寫循環
- **框架**: `unittest.TestCase` (可被 pytest 直接執行)

### `tests/validate_pipeline.py` — 整合驗證腳本 (5 cases)
- **測試範圍**: Schema 嚴格性 (Datetime[ns])、斷點續傳、20 線程高併發 DB 寫入、異常重試 (2 次失敗 + 1 次成功)、原子性寫入 (.tmp → .parquet)
- **框架**: `unittest.TestCase` (可被 pytest 直接執行)
- **注意**: 部分測試需要 `data/` 中有真實 Parquet 資料才能通過。

### `tests/test_mock.json` — Mock 測試資料
- **內容**: FinMind API 的範例 JSON 回應，供 `test_mock_pipeline.py` 使用。

### `tests/experiments/` — 實驗性腳本
| 檔案 | 說明 |
|------|------|
| `generate_mock_data.py` | 從 FinMind API 下載真實資料生成 mock JSON |
| `test_10days_pipeline.py` | 10 天資料的端對端壓測腳本 |
| `test_asof_join.py` | Asof Join 對齊邏輯的實驗驗證 |
| `test_greeks.py` | Greeks 計算引擎的獨立測試 |
| `test_performance.py` | 效能基準測試 (throughput, latency) |

> ⚠️ 這些是裸腳本 (無 pytest/unittest 框架)，需手動執行。

---

## 七、`docs/` — 文件

### `docs/HANDOVER_SPEC_V2.md` — 開發規格書
- **內容**: 完整的系統架構、已完成進度、下階段目標 (run_all.py, 多週期聚合, Colab 策略)、雙表 Schema 定義、Google Drive 同步策略。**本次開發的所有模組均依此規格書實作。**

### `docs/FILE_MANIFEST.md` — 本檔案
- **內容**: 專案中每個檔案的詳細功能說明。

---

## 八、`data/` — 資料產出目錄

此目錄由管線自動建立，**已加入 `.gitignore`**。

```
data/
└── {year}/                          # 按年份分目錄
    ├── TaiwanOptionTick/            # TXO 選擇權逐筆原始資料
    │   └── TXO_{YYYY-MM-DD}.parquet
    ├── TaiwanFuturesTick/           # TX 期貨逐筆原始資料
    │   └── TX_{YYYY-MM-DD}.parquet
    ├── GreeksFeatures/              # Greeks 計算結果
    │   └── TXO_Greeks_{YYYY-MM-DD}.parquet
    └── Features/                    # 多週期聚合特徵 (由 timeframe_aggregator 產出)
        ├── daily/                   # 當月日檔散檔
        │   ├── TXO_ContractBars_1m_{date}.parquet
        │   └── TXO_MarketSummary_1m_{date}.parquet
        └── monthly/                 # 歷史月度合併檔
            ├── TXO_ContractBars_1m_{YYYYMM}.parquet
            └── TXO_MarketSummary_1m_{YYYYMM}.parquet
```
