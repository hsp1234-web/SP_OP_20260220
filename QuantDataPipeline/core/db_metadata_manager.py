import sqlite3
import threading
import logging
from pathlib import Path
from typing import Optional, List, Tuple
from threading import Lock

from .config import DB_PATH

logger = logging.getLogger("pipeline.db")

class DBManager:
    _instance: Optional["DBManager"] = None
    _lock = Lock()

    def __new__(cls, db_path: Path = DB_PATH):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DBManager, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    @classmethod
    def _reset_instance(cls):
        """重置 Singleton (僅供測試使用)"""
        with cls._lock:
            cls._instance = None

    def __init__(self, db_path: Path = DB_PATH):
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
        self._initialized = True
        logger.info("資料庫管理器已初始化 (單例模式)。")

    def _get_conn(self):
        """取得 Thread-local 資料庫連線"""
        if not hasattr(self._local, "conn"):
            # 開啟 WAL 模式確保高併發
            try:
                conn = sqlite3.connect(self.db_path)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                self._local.conn = conn
            except sqlite3.Error as e:
                logger.error(f"資料庫連線失敗: {e}")
                raise
        return self._local.conn

    def _init_db(self):
        """初始化資料庫表結構"""
        try:
            conn = self._get_conn()
            # 任務清單表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_registry (
                    task_id TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    data_id TEXT,
                    status INTEGER DEFAULT 0, -- 0: 待處理, 1: L1成功, 2: L2成功, 3: 跳過/空值
                    file_hash TEXT
                );
            """)
            # API 呼叫統計表 (新增)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_call_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset TEXT,
                    status_code INTEGER,
                    data_count INTEGER,
                    latency_ms REAL,
                    call_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"資料庫初始化失敗: {e}")
            raise

    def log_api_call(self, dataset: str, status_code: int, data_count: int, latency_ms: float):
        """寫入 API 呼叫統計紀錄"""
        try:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO api_call_stats (dataset, status_code, data_count, latency_ms) VALUES (?, ?, ?, ?)",
                (dataset, status_code, data_count, latency_ms)
            )
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"寫入 API 統計失敗: {e}")

    def register_task(self, task_id: str, trade_date: str, dataset_name: str, data_id: str = ""):
        """註冊新任務，若已存在則忽略"""
        try:
            conn = self._get_conn()
            conn.execute("""
                INSERT OR IGNORE INTO task_registry (task_id, trade_date, dataset_name, data_id, status)
                VALUES (?, ?, ?, ?, 0)
            """, (task_id, trade_date, dataset_name, data_id))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"註冊任務 {task_id} 失敗: {e}")

    def update_task_status(self, task_id: str, status: int, file_hash: Optional[str] = None):
        """更新任務狀態與檔案雜湊"""
        try:
            conn = self._get_conn()
            if file_hash:
                conn.execute("""
                    UPDATE task_registry SET status = ?, file_hash = ? WHERE task_id = ?
                """, (status, file_hash, task_id))
            else:
                conn.execute("""
                    UPDATE task_registry SET status = ? WHERE task_id = ?
                """, (status, task_id))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"更新任務 {task_id} 狀態失敗: {e}")

    def get_pending_tasks(self) -> List[Tuple[str, str, str, str]]:
        """取得所有待處理 (status=0) 的任務"""
        try:
            conn = self._get_conn()
            cursor = conn.execute("SELECT task_id, trade_date, dataset_name, data_id FROM task_registry WHERE status = 0")
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"取得待處理任務失敗: {e}")
            return []

    def get_task_status(self, task_id: str) -> Optional[int]:
        """取得特定任務的狀態"""
        try:
            conn = self._get_conn()
            cursor = conn.execute("SELECT status FROM task_registry WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            return row[0] if row else None
        except sqlite3.Error as e:
            logger.error(f"取得任務 {task_id} 狀態失敗: {e}")
            return None

    def get_tasks_by_status(self, status: int) -> List[Tuple[str, str, str, str]]:
        """取得指定狀態的所有任務"""
        try:
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT task_id, trade_date, dataset_name, data_id FROM task_registry WHERE status = ?",
                (status,)
            )
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"取得狀態 {status} 任務失敗: {e}")
            return []

    def reset_orphan_tasks(self, data_dir: Path) -> int:
        """
        清理孤兒狀態：DB 標記完成 (status=1) 但 Parquet 不存在。
        將其重置為 0 以便重試。回傳重置數量。
        """
        tasks = self.get_tasks_by_status(1)
        reset_count = 0
        for task_id, trade_date, dataset_name, data_id in tasks:
            year = trade_date.split("-")[0]
            filename = f"{data_id}_{trade_date}.parquet" if data_id else f"{trade_date}.parquet"
            parquet_path = data_dir / year / dataset_name / filename
            if not parquet_path.exists():
                logger.warning(f"孤兒任務 {task_id}: Parquet 不存在，重置狀態為 0")
                self.update_task_status(task_id, 0)
                reset_count += 1
        return reset_count

def get_db_manager() -> DBManager:
    """取得 DBManager 單例"""
    return DBManager()
