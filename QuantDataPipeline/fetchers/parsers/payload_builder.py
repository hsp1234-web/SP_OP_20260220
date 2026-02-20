from typing import Dict, Optional

def build_payload(dataset: str, start_date: str, end_date: str, data_id: Optional[str] = None) -> Dict[str, str]:
    """
    Construct the payload for FinMind API requests.

    Args:
        dataset: Name of the dataset (e.g., TaiwanStockPriceTick)
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        data_id: Optional stock or contract ID

    Returns:
        Dictionary of query parameters.
    """
    payload = {
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date
    }
    if data_id:
        payload["data_id"] = data_id
    return payload
