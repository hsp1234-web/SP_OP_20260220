import os
import sys
from pathlib import Path

# 專案根目錄
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 資料目錄 (Parquet 檔案存放處)
# 如果有掛載 Drive 且指定 SYNC_TO_DRIVE = True，直接將實體 Parquet 檔儲存於 Drive，避免 Colab 結束時消失
# (注意：唯有 status.db 仍強制留在本地 SSD，避免發生 SQLite lock 導致 disk image is malformed)
if os.environ.get("SYNC_TO_DRIVE", "False").lower() == "true" and os.environ.get("DRIVE_PATH"):
    DATA_DIR = Path(os.environ["DRIVE_PATH"])
else:
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
