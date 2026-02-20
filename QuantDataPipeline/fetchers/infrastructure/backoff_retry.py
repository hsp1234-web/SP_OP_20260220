import time
import random
import logging
import functools
from typing import Type, Tuple, Union

from core.config import MAX_RETRIES, BASE_DELAY, MAX_DELAY

logger = logging.getLogger("pipeline.retry")

# 永久性錯誤關鍵字 — 遇到這些錯誤絕對不重試，立刻拋出
FATAL_KEYWORDS = [
    "user level", "update your user level",
    "please upgrade", "permission denied",
    "invalid token", "token expired",
    "unauthorized", "forbidden",
    "sponsor",
]


def _is_fatal_error(e: Exception) -> bool:
    """檢查是否為永久性錯誤 (帳號等級/Token/權限)，不應重試"""
    msg = str(e).lower()
    return any(kw in msg for kw in FATAL_KEYWORDS)


def exponential_backoff(
    retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    max_delay: float = MAX_DELAY,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,)
):
    """
    Decorator for exponential backoff retry with jitter.
    Catches specified exceptions and retries the function execution.
    永久性錯誤 (帳號等級不足/Token 無效) 會立即拋出，不浪費重試次數。
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    # 永久性錯誤：帳號等級、Token、權限 → 立即拋出
                    if _is_fatal_error(e):
                        logger.error(f"⛔ 永久性錯誤 (不重試): {e}")
                        raise e

                    retry_count += 1
                    if retry_count > retries:
                        logger.error(f"函式 {func.__name__} 於 {retries} 次重試後失敗。最後錯誤: {e}")
                        raise e

                    # Calculate delay: base * 2^attempt
                    delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)
                    # Add jitter: +/- 10%
                    jitter = random.uniform(-0.1 * delay, 0.1 * delay)
                    sleep_time = max(0, delay + jitter)

                    logger.warning(f"{func.__name__} 重試 {retry_count}/{retries} (原因: {type(e).__name__}: {e})。等待 {sleep_time:.2f} 秒...")
                    time.sleep(sleep_time)
        return wrapper
    return decorator
