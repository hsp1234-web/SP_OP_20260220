# 📈 QuantDataPipeline - Data Architecture Context
這裡是量化選擇權資料中台的結構說明。請以此資料結構作為後續開發、回測、機器學習建模的唯一基準。

## 📁 1. 目錄與儲存架構 (Google Drive / 本地 SSD)
根目錄 (DATA_DIR): `/content/drive/MyDrive/QuantData/data/` 或 `/content/local_data/`

資料採用 `Year / Category / File` 結構，皆為 `Parquet` 高壓縮格式，具備欄位型態安全。
```text
data/
 ├── 2024/ ... (原有 Tick 與 Greeks 目錄)
 └── data_v4/                    # 【V4.2】新增：全市場籌碼與報價區
      ├── temp_raw_data/         # Phase 1: 暫存 JSON (按日)
      └── processed_parquet/     # Phase 2: 最終 Parquet (按月合併)
           ├── TaiwanStockPriceAdj/
           ├── TaiwanFuturesInstitutionalInvestors/
           └── ... (其餘 5 種)
```

## 📊 2. 資料表結構 (Data Schema)

### A. 期貨資料表 (TaiwanFuturesTick/TX_*.parquet)
- **date** `[datetime]`: 逐筆交易時間 (Tick 時間戳記)
- **price** `[float]`: 結算/成交價格
- **volume** `[float/int]`: (Optional) 交易量

### B. 選擇權資料表 (TaiwanOptionTick/TXO_*.parquet)
- **date** `[datetime]`: 交易時間
- **option_id** `[string]`: 選擇權合約代碼 (例如：TXO202001C11000)
- **contract_date** `[string]`: 月選 (YYYYMM) 或 周選 (YYYYMMWN)
- **PutCall** `[string]`: 'Call' 或 'Put' (部份資料為 'C'/'P')
- **ExercisePrice** `[float]`: 履約價
- **price** `[float]`: 選擇權逐筆交易價

### C. 機器學習特徵表 (GreeksFeatures/TXO_Greeks_*.parquet) ★ 核心表
此表由期權與期貨資料經過 `Polars Asof Join (backward 逼近)`，並透過 `Numba` C級加速牛頓逼近法，無狀態計算出完整金融特徵，為回測與建模的核心。
包括：
- **基礎對齊欄位**: `date`, `contract_date`, `PutCall`, `ExercisePrice`, `price`
- **Underlying_S** `[float]`: 利用 Asof Join 抓取的「當下期貨價格」做為底層標的定價
- **Years_to_Maturity** `[float]`: 年化剩餘到期時間 (自動判斷台指期第三個星期三的結算日)
- **IV (Implied Volatility)** `[float]`: 隱含波動率
- **Delta** `[float]`: 標的物價格變動1單位的選擇權價格變動
- **Gamma** `[float]`: 標的物價格變動1單位的 Delta 變動
- **Vega** `[float]`: 波動率變動 1% 的選擇權價格變動
- **Theta** `[float]`: 時間流逝1天的價值衰減
- **Vanna** `[float]`: 二階特徵 (d(Delta)/d(Vol))
- **Charm** `[float]`: 二階特徵 (-d(Delta)/d(T))

### D. 全市場籌碼與報價表 (data_v4/processed_parquet/*.parquet) ★ V4.2 新增
包含 7 種核心資料集，採用全市場橫斷面下載，每資料集每月合併為一檔 Parquet。
起算年份根據 FinMind API 的來源極限與系統設定：
1. **TaiwanStockPriceAdj** (台灣還原股價): `2015-01-01` 起算 (`date`, `stock_id`, `open` 等)
2. **TaiwanFuturesInstitutionalInvestors** (期貨三大法人): `2018-06-05` 起算
3. **TaiwanOptionInstitutionalInvestors** (選擇權三大法人): `2018-06-05` 起算
4. **TaiwanFuturesOpenInterestLargeTraders** (期貨大額交易人): `1998-07-01` 起算
5. **TaiwanOptionOpenInterestLargeTraders** (選擇權大額交易人): `1998-07-01` 起算
6. **TaiwanTotalExchangeMarginMaintenance** (大盤融資維持率): `2015-01-01` 起算
7. **TaiwanStockInstitutionalInvestorsBuySell** (個股三大法人買賣超): `2015-01-01` 起算

## ⚙️ 3. 開發守則與注意事項
1. **讀取方式**: 檔案規模達千億 Tick 等級，務必使用 `polars.scan_parquet().collect()` 進行 Lazy Loading，嚴禁直接使用 Pandas 全量載入。
2. **V4.2 下載策略**: Phase 1 JSON 僅為過渡，建模時應優先使用 `data_v4/processed_parquet/` 下的月度合併檔。
3. **無風險利率 (r)**: 程式內部定價模組預設常數使用 `r = 0.015` (1.5%)。
4. **配額意識**: 下載腳本支援 `/v2/user_info` 即時查詢，開發者呼叫 API 時應避開尖峰或配合冷卻機制。
