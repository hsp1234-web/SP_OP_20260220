import requests
import time
import logging
from threading import Lock
from typing import Optional, Dict, Any

from core.db_metadata_manager import DBManager, get_db_manager
from core.config import FINMIND_API_TOKEN

logger = logging.getLogger("pipeline.http")

class HTTPSession:
    _instance: Optional["HTTPSession"] = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(HTTPSession, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.session = requests.Session()
        self.base_url = "https://api.finmindtrade.com/api/v4/data"
        self.token = FINMIND_API_TOKEN
        # 使用 get_db_manager() 取得單例
        self.db = get_db_manager()
        self._initialized = True
        logger.info("HTTPSession 已初始化，連線池準備就緒。")

    def get_data(self, dataset: str, data_id: str = "", start_date: str = "", end_date: str = "") -> Dict[str, Any]:
        """
        執行 API 請求並記錄統計資訊與日誌。
        """
        params = {
            "dataset": dataset,
            "data_id": data_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        if self.token:
            params["token"] = self.token

        start_time = time.perf_counter()
        try:
            # 設定 timeout 避免長時間掛起
            response = self.session.get(self.base_url, params=params, timeout=30)
            latency = (time.perf_counter() - start_time) * 1000

            # 確保狀態碼是 200，若為 429/502/503 會直接拋出 HTTPError
            if response.status_code != 200:
                raise Exception(f"HTTP 請求失敗，狀態碼: {response.status_code}")

            # 解析並統計
            try:
                res_json = response.json()
            except ValueError as e:
                logger.error(f"解析 JSON 回應失敗 (可能被 API 限流或防火牆阻擋)，狀態碼: {response.status_code}")
                raise Exception(f"收到非 JSON 回應 (被阻擋或伺服器異常)") from e

            data_count = len(res_json.get("data", []))

            # 存入資料庫統計
            self.db.log_api_call(dataset, response.status_code, data_count, latency)

            return res_json
        except requests.RequestException as e:
            logger.error(f"請求失敗: {e}")
            # 記錄失敗的請求
            latency = (time.perf_counter() - start_time) * 1000
            self.db.log_api_call(dataset, -1, 0, latency)
            raise e

def get_session() -> HTTPSession:
    """工廠函數：取得 HTTPSession 單例。"""
    return HTTPSession()
