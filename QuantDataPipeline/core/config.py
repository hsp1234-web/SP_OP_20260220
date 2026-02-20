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

# 引進 python-dotenv
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# FinMind API Token
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")


# 壓縮層級
COMPRESSION_LEVEL = 3

# 速率限制 (每請求秒數)
# 匿名: 300 requests/hour -> 12 seconds/request
# 註冊 (with token): 600 requests/hour -> 6 seconds/request
RATE_LIMIT_DELAY = 12.0 if not FINMIND_API_TOKEN else 6.0

# 重試設定
MAX_RETRIES = 5
BASE_DELAY = 1.0 # 秒
MAX_DELAY = 60.0 # 秒

# 日誌檔案
LOG_FILE = PROJECT_ROOT / "pipeline.log"
