import os
import sys
from pathlib import Path

# Project Root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data Directory (where parquet files land)
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Database
DB_PATH = PROJECT_ROOT / "status.db"

# FinMind API
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")

# Compression
COMPRESSION_LEVEL = 3

# Rate Limiting (seconds per request)
# Anonymous: 300 requests/hour -> 12 seconds/request
# Registered (with token): 600 requests/hour -> 6 seconds/request
RATE_LIMIT_DELAY = 12.0 if not FINMIND_API_TOKEN else 6.0

# Retries
MAX_RETRIES = 5
BASE_DELAY = 1.0 # Seconds
MAX_DELAY = 60.0 # Seconds

# Logging
LOG_FILE = PROJECT_ROOT / "pipeline.log"
