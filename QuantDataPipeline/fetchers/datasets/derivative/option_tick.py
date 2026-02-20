import polars as pl
from fetchers.infrastructure.http_session import get_session
from fetchers.infrastructure.rate_limiter import RateLimiter
from fetchers.infrastructure.backoff_retry import exponential_backoff
from fetchers.parsers.finmind_extractor import extract_and_cast
from core.config import FINMIND_API_TOKEN

DATASET_NAME = "TaiwanOptionTick"

@exponential_backoff()
def fetch(date: str, data_id: str) -> pl.DataFrame:
    """
    抓取台灣期貨逐筆成交資訊 (TaiwanOptionTick)。
    """
    session = get_session()
    limiter = RateLimiter()

    # 速率限制
    limiter.wait()

    # 透過 HTTPSession 執行請求
    res_json = session.get_data(
        dataset=DATASET_NAME,
        data_id=data_id,
        start_date=date,
        end_date=date
    )

    # 萃取並轉換型別
    df = extract_and_cast(res_json, DATASET_NAME)

    return df
