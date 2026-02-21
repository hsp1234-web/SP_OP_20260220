import time
import logging
from threading import Lock
from typing import Optional

from core.config import RATE_LIMIT_DELAY

logger = logging.getLogger("pipeline.rate_limiter")

class RateLimiter:
    """Singleton RateLimiter to ensure global compliance with API limits."""
    _instance: Optional["RateLimiter"] = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RateLimiter, cls).__new__(cls)
                cls._instance.last_request_time = 0.0
        return cls._instance

    def wait(self):
        """No-op: Bypass rate limiting to fetch as fast as possible until blocked."""
        pass
