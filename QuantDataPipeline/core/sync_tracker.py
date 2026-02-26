"""
V4.2 同步追蹤器 (Sync Tracker)
使用 SQLite (WAL 模式) 追蹤每個資料集的下載進度，實現斷點續傳。
"""

import os
import sqlite3
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple, List, Set


class SyncTracker:
    """
    SQLite 斷點續傳追蹤器 (Thread-safe Singleton)
    
    追蹤每個資料集的：
    - oldest_date: 歷史回補已推進到的最舊日期
    - newest_date: 每日更新已補到的最新日期
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: Optional[str] = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, db_path: Optional[str] = None):
        if self._initialized:
            return
        
        if db_path is None:
            from core.config import PROJECT_ROOT
            db_path = str(PROJECT_ROOT / "sync_tracker.db")
        
        self._db_path = db_path
        self._local = threading.local()
        self._init_db()
        self._initialized = True

    def _get_conn(self) -> sqlite3.Connection:
        """取得當前執行緒的 SQLite 連線"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA busy_timeout=5000")
        return self._local.conn

    def _init_db(self):
        """建立資料表"""
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_tracker (
                dataset_name TEXT PRIMARY KEY,
                oldest_date  TEXT NOT NULL,
                newest_date  TEXT NOT NULL,
                total_files  INTEGER DEFAULT 0,
                last_update  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_dates (
                dataset_name TEXT,
                trade_date   TEXT,
                PRIMARY KEY (dataset_name, trade_date)
            )
        """)
        conn.commit()

    def _now_iso(self) -> str:
        """取得 ISO 格式的現在時間"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def register_dataset(self, dataset_name: str, default_start: str = "2010-01-01"):
        """
        冷啟動時初始化資料集記錄。
        若已存在則不覆蓋 (INSERT OR IGNORE)。
        """
        conn = self._get_conn()
        conn.execute(
            """INSERT OR IGNORE INTO sync_tracker 
               (dataset_name, oldest_date, newest_date, total_files, last_update)
               VALUES (?, ?, ?, 0, ?)""",
            (dataset_name, default_start, default_start, self._now_iso())
        )
        conn.commit()

    def get_progress(self, dataset_name: str) -> Optional[Tuple[str, str]]:
        """
        取得資料集的下載進度。
        回傳 (oldest_date, newest_date)，若不存在則回傳 None。
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT oldest_date, newest_date FROM sync_tracker WHERE dataset_name = ?",
            (dataset_name,)
        ).fetchone()
        return row if row else None

    def update_newest(self, dataset_name: str, date_str: str):
        """更新「每日更新」的最新進度"""
        conn = self._get_conn()
        conn.execute(
            """UPDATE sync_tracker 
               SET newest_date = ?, total_files = total_files + 1, last_update = ?
               WHERE dataset_name = ?""",
            (date_str, self._now_iso(), dataset_name)
        )
        self.mark_date_done(dataset_name, date_str)
        conn.commit()

    def update_oldest(self, dataset_name: str, date_str: str):
        """更新「歷史回補」的最舊進度"""
        conn = self._get_conn()
        conn.execute(
            """UPDATE sync_tracker 
               SET oldest_date = ?, total_files = total_files + 1, last_update = ?
               WHERE dataset_name = ?""",
            (date_str, self._now_iso(), dataset_name)
        )
        self.mark_date_done(dataset_name, date_str)
        conn.commit()

    def mark_date_done(self, dataset_name: str, date_str: str):
        """將特定日期標記為已完成 (防止重複下載)"""
        conn = self._get_conn()
        conn.execute(
            "INSERT OR IGNORE INTO sync_dates (dataset_name, trade_date) VALUES (?, ?)",
            (dataset_name, date_str)
        )
        conn.commit()

    def get_done_dates(self, dataset_name: str) -> Set[str]:
        """取得該資料集所有已完成的日期"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT trade_date FROM sync_dates WHERE dataset_name = ?",
            (dataset_name,)
        ).fetchall()
        return {r[0] for r in rows}

    def get_all_progress(self) -> List[Tuple[str, str, str, int, str]]:
        """取得所有資料集的進度"""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT dataset_name, oldest_date, newest_date, total_files, last_update "
            "FROM sync_tracker ORDER BY dataset_name"
        ).fetchall()
        return rows

    def get_missing_dates(
        self,
        dataset_name: str,
        trading_days: Set[str],
        scanned_files: Set[str],
    ) -> List[str]:
        """
        計算待補日期清單。結合資料庫記錄與磁碟掃描，確保 JSON 被刪除後仍不重複下載。
        """
        # 1. 先從 DB 取得所有已完成的日期
        db_done = self.get_done_dates(dataset_name)
        
        # 2. 合併磁碟現有檔案 (以防 DB 沒記到)
        all_completed = db_done | scanned_files
        
        # 3. 過濾掉已完成的
        missing = trading_days - all_completed
        return sorted(missing, reverse=True)

    def increment_file_count(self, dataset_name: str, count: int = 1):
        """增加已下載檔案計數"""
        conn = self._get_conn()
        conn.execute(
            """UPDATE sync_tracker 
               SET total_files = total_files + ?, last_update = ?
               WHERE dataset_name = ?""",
            (count, self._now_iso(), dataset_name)
        )
        conn.commit()

    def close(self):
        """關閉當前執行緒的連線"""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    @classmethod
    def _reset_instance(cls):
        """重置 Singleton (僅供測試用)"""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close()
            cls._instance = None
