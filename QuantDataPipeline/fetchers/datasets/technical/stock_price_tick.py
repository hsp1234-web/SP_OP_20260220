import polars as pl
from fetchers.infrastructure.http_session import get_session
from fetchers.infrastructure.rate_limiter import RateLimiter
from fetchers.infrastructure.backoff_retry import exponential_backoff
from fetchers.parsers.payload_builder import build_payload
from fetchers.parsers.finmind_extractor import extract_and_cast
from core.config import FINMIND_API_TOKEN

DATASET_NAME = "TaiwanStockPriceTick"
URL = "https://api.finmindtrade.com/api/v4/data"

@exponential_backoff()
def fetch(date: str, data_id: str) -> pl.DataFrame:
    """
    Fetch TaiwanStockPriceTick data for a specific date and stock_id.
    """
    session = get_session()
    limiter = RateLimiter()

    # Rate Limiting
    limiter.wait()

    # Build Payload
    params = build_payload(DATASET_NAME, date, date, data_id)
    if FINMIND_API_TOKEN:
        params["token"] = FINMIND_API_TOKEN

    # Execute Request
    response = session.get(URL, params=params)

    # Extract and Cast
    df = extract_and_cast(response, DATASET_NAME)

    return df
