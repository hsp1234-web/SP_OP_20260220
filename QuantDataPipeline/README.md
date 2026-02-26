# 📈 QuantDataPipeline - 台灣選擇權與全市場量化資料管線

高效、穩定、具備自我修復與記憶能力的大型量化資料下載與特徵工程中台。

## 🌟 核心特色 (V4.2 版本)

本專案經過多次迭代，最新 V4.2 版本引入了強大的「**生產者-消費者 (Producer-Consumer)**」雙軌異步架構，專門針對 FinMind API 的頻寬極限與額度規則進行深度優化：

1. **極限併發與原子寫入**：20 執行緒火力全開下載，透過寫入 `.tmp` 後瞬間更名為 `.json.gz` 確保資料原子性。
2. **SQLite 斷點記憶與 Deep Scan**：不依賴脆弱的硬碟檔案存在與否，下載啟動時會深入掃描 Parquet 內部資料，並將成功記錄寫入 `sync_tracker.db`，確保即使生肉被刪除也**絕不重複下載**浪費額度。
3. **智慧階梯式冷卻 (Smart Reset)**：碰到 API 限流 (402/429)，自動進入 10 / 15 / 20 / 25 / 30 分鐘的階梯式冷卻；一旦抓取成功，立即「消氣」將等待時間歸零。
4. **單核潛水艇轉檔模式**：將龐大的 JSON 轉檔壓制在單顆 CPU 核心背景運行 (`--watch`)，不僅能與舊 Parquet 自動合併去重，更在轉換完畢後自動清理 JSON 以維持高達 GB 級的空間彈性。
5. **時空邊界探索**：歷史資料追溯力最強可達 1998 年（大額交易人）與 2018 年（法人屬性），全由 `datasets_registry.py` 精準管控。

---

## 🚀 雙終端自動化流水線 (How to run)

請在兩個獨立的終端機分別啟動以下腳本，讓「下載生產者」與「轉檔消費者」並肩作戰：

### 終端機 1：下載尖兵 (生產者)
負責往前線衝刺，獲取 JSON 資料，遇到限流自動睡覺。
```bash
python3 fetch_market_data.py --mode backfill --batch-size 100 --until-done
```
*(啟動時會花幾秒鐘進行「Deep Scan」與資料庫同步)*

### 終端機 2：單核轉檔機 (消費者)
負責在後方收拾，將新增的月度資料壓製成極高壓縮率的 Parquet，並自動刪除佔空間的原始 JSON。
```bash
python3 process_raw_to_parquet.py --watch
```
*(若您希望保留原始 JSON，可以在後面加上 `--keep-json`)*

---

## 📂 專案核心目錄

| 目錄/檔案 | 說明 |
| --- | --- |
| `core/` | L0 核心：設定、白名單 (`datasets_registry.py`)、追蹤器 (`sync_tracker.py`) |
| `fetchers/` | L1 爬取層：FinMind 各類目標 API 封裝、HTTP 連線池、重試邏輯 |
| `processors/` | L2 計算層：包含採用 Numba 加速的 Black-Scholes Greeks 計算引擎 |
| `storage/` | L3 儲存層：Parquet 的高安全寫入校驗與月度打包合併邏輯 |
| `data_v4/` | 【V4.2】新增的主戰場，存放 `temp_raw_data/` 暫存，與最終的 `processed_parquet/` |
| `docs/` | 完整的系統架構與每一份檔案的功能清單 |
| `colab_launcher.ipynb` | Google Colab 一鍵啟動器 (圖形化表單控制面板) |

---

## 🔐 隱私與安全防護

本專案特別客製了嚴苛的 `.gitignore` 規則：
- 絕對封鎖 `data/` 與 `data_v4/` 等實體資料夾。
- 絕對封鎖所有 SQLite WAL 兄弟檔 (`*.db`, `*.db-wal`) 避免記憶資料庫外流。
- 絕對封鎖 `.env` 金鑰配置與幾十 MB 的 `pipeline.log`。
您可安心執行 `git commit`，不用擔心把硬碟資料庫上傳到遠端。

---

## 📖 進階文件
若您是開發者，或要將此管線交接給下一個 AI 助手，請務必先詳細閱讀：
1. `docs/FILE_MANIFEST.md` : 每個 `.py` 的具體功能與原理。
2. `docs/AI_Context_Data_Schema.md` : 最終 Parquet 資料庫的 Schema 與定義。
