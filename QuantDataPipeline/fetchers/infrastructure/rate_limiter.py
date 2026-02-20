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
        """Block until the configured rate limit delay has passed since the last request."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < RATE_LIMIT_DELAY:
                sleep_time = RATE_LIMIT_DELAY - elapsed
                logger.debug(f"Sleeping for {sleep_time:.2f}s to respect rate limit ({RATE_LIMIT_DELAY}s/req)")
                time.sleep(sleep_time)
            self.last_request_time = time.time()
