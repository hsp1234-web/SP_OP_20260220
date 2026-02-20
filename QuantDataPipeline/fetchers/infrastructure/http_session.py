import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional

def get_session(retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    """
    Configure and return a requests.Session with connection pooling and retries.
    This handles transport-level retries (connection errors, 5xx).
    Application-level retries (e.g. rate limits, 429) are handled by backoff_retry.py.
    """
    session = requests.Session()

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 504),
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "QuantDataPipeline/1.0",
        "Accept": "application/json",
    })

    return session
