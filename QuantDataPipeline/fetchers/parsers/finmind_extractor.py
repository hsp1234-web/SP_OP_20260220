import requests
import polars as pl
import logging
from typing import Dict, Any, Optional

from .schema_enforcer import enforce_schema

logger = logging.getLogger("pipeline.extractor")

def extract_and_cast(response: requests.Response, dataset_name: str) -> pl.DataFrame:
    """
    Process FinMind API response and convert to strictly typed Polars DataFrame.
    """
    # 1. Ensure HTTP connection success (handled by caller/retry, but good to check)
    response.raise_for_status()

    try:
        json_payload = response.json()
    except ValueError as e:
        logger.error(f"Failed to parse JSON response: {e}")
        raise ValueError("Invalid JSON response from API")

    # 2. Business logic error handling
    if json_payload.get("msg") != "success":
        msg = json_payload.get("msg", "Unknown error")
        logger.error(f"API returned error message: {msg}")
        raise ValueError(f"API Error: {msg}")

    data_list = json_payload.get("data", [])

    # 3. Empty data guard (e.g., holiday or suspended trading)
    if not data_list:
        logger.info(f"No data found for dataset {dataset_name}. Returning empty DataFrame.")
        return pl.DataFrame()

    # 4. Zero-copy conversion to Polars DataFrame
    try:
        # infer_schema_length=None forces Polars to scan all rows for schema inference, avoiding type errors
        df = pl.from_dicts(data_list, infer_schema_length=None)
    except Exception as e:
        logger.error(f"Failed to convert data list to DataFrame: {e}")
        raise e

    # 5. Strict Schema Enforcement
    try:
        df = enforce_schema(df, dataset_name)
    except Exception as e:
        logger.error(f"Schema enforcement failed: {e}")
        raise e

    return df
