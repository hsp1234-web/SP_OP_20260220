import time
import random
import logging
import functools
from typing import Type, Tuple, Union

from core.config import MAX_RETRIES, BASE_DELAY, MAX_DELAY

logger = logging.getLogger("pipeline.retry")

def exponential_backoff(
    retries: int = MAX_RETRIES,
    base_delay: float = BASE_DELAY,
    max_delay: float = MAX_DELAY,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,)
):
    """
    Decorator for exponential backoff retry with jitter.
    Catches specified exceptions and retries the function execution.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry_count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count > retries:
                        logger.error(f"Function {func.__name__} failed after {retries} retries. Last error: {e}")
                        raise e

                    # Calculate delay: base * 2^attempt
                    delay = min(base_delay * (2 ** (retry_count - 1)), max_delay)
                    # Add jitter: +/- 10%
                    jitter = random.uniform(-0.1 * delay, 0.1 * delay)
                    sleep_time = max(0, delay + jitter)

                    logger.warning(f"Retry {retry_count}/{retries} for {func.__name__} due to {type(e).__name__}: {e}. Sleeping {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
        return wrapper
    return decorator
