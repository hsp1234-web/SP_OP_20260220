import sqlite3
import logging
from pathlib import Path
from typing import Optional, List, Tuple
from contextlib import contextmanager

from .config import DB_PATH

logger = logging.getLogger("pipeline.db")

class DBMetadataManager:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialize the database with WAL mode and create tables."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Enable Write-Ahead Logging for concurrency
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")

            # Create task_registry table
            # Added data_id column
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_registry (
                    task_id TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    data_id TEXT,
                    status INTEGER DEFAULT 0,
                    file_checksum TEXT
                );
            """)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database initialization failed: {e}")
            raise
        finally:
            conn.close()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            yield conn
        finally:
            conn.close()

    def register_task(self, task_id: str, trade_date: str, dataset_name: str, data_id: str = ""):
        """Register a new task or ignore if exists."""
        with self.get_connection() as conn:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO task_registry (task_id, trade_date, dataset_name, data_id, status)
                    VALUES (?, ?, ?, ?, 0)
                """, (task_id, trade_date, dataset_name, data_id))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Failed to register task {task_id}: {e}")

    def update_task_status(self, task_id: str, status: int, checksum: Optional[str] = None):
        """Update the status and checksum of a task."""
        with self.get_connection() as conn:
            try:
                if checksum:
                    conn.execute("""
                        UPDATE task_registry SET status = ?, file_checksum = ? WHERE task_id = ?
                    """, (status, checksum, task_id))
                else:
                    conn.execute("""
                        UPDATE task_registry SET status = ? WHERE task_id = ?
                    """, (status, task_id))
                conn.commit()
            except sqlite3.Error as e:
                logger.error(f"Failed to update task {task_id}: {e}")

    def get_pending_tasks(self) -> List[Tuple[str, str, str, str]]:
        """Get all tasks with status 0 (Pending). Returns (task_id, trade_date, dataset_name, data_id)."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT task_id, trade_date, dataset_name, data_id FROM task_registry WHERE status = 0")
            return cursor.fetchall()

    def get_task_status(self, task_id: str) -> Optional[int]:
        """Get the status of a specific task."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT status FROM task_registry WHERE task_id = ?", (task_id,))
            row = cursor.fetchone()
            return row[0] if row else None
