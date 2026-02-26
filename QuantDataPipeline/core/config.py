import os
import sys
from pathlib import Path

# 專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 資料目錄 (Parquet 檔案存放處)
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 資料庫路徑
DB_PATH = PROJECT_ROOT / "status.db"

# V4.2 全市場管線設定
DATA_V4_DIR = PROJECT_ROOT / "data_v4"
SYNC_DB_PATH = PROJECT_ROOT / "sync_tracker.db"
TEMP_RAW_DIR = DATA_V4_DIR / "temp_raw_data"
PROCESSED_DIR = DATA_V4_DIR / "processed_parquet"

# 引進 python-dotenv
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# FinMind API Token
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")


# 壓縮層級
COMPRESSION_LEVEL = 3

# 速率限制 (每請求秒數)
# 可透過環境變數覆蓋: RATE_LIMIT_DELAY (秒) 或 FINMIND_QUOTA_PER_HOUR (次/hr)
# 預設分級:
#   匿名:    300 req/hr → 12s/req
#   免費Token: 600 req/hr → 6s/req
#   付費帳號: 由 FINMIND_QUOTA_PER_HOUR 計算 (如 1600 → 2.25s → 安全取 2.5s)
_explicit_delay = os.getenv("RATE_LIMIT_DELAY")
_quota_per_hour = os.getenv("FINMIND_QUOTA_PER_HOUR")

if _explicit_delay:
    RATE_LIMIT_DELAY = float(_explicit_delay)
elif _quota_per_hour:
    # 加 10% 安全邊際
    RATE_LIMIT_DELAY = round(3600 / int(_quota_per_hour) * 1.1, 2)
elif FINMIND_API_TOKEN:
    RATE_LIMIT_DELAY = 6.0
else:
    RATE_LIMIT_DELAY = 12.0

# 重試設定
MAX_RETRIES = 5
BASE_DELAY = 1.0 # 秒
MAX_DELAY = 60.0 # 秒

# 日誌檔案
LOG_FILE = PROJECT_ROOT / "pipeline.log"

# V4.2 資源控制
V4_DOWNLOAD_WORKERS = int(os.getenv("V4_DOWNLOAD_WORKERS", "4"))
V4_PROCESS_WORKERS = int(os.getenv("V4_PROCESS_WORKERS", "2"))

# 環境偵測
IS_COLAB = "google.colab" in sys.modules
