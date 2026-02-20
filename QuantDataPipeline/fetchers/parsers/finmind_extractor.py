import polars as pl
import logging
from typing import Dict, Any, Union
import requests

from .schema_enforcer import enforce_schema

logger = logging.getLogger("pipeline.extractor")

def extract_and_cast(data_source: Union[requests.Response, Dict[str, Any]], dataset_name: str) -> pl.DataFrame:
    """
    處理 FinMind API 回應 (Response 或 JSON dict) 並轉換為嚴格型別的 Polars DataFrame。
    """
    json_payload = {}

    # 1. 處理輸入型別
    if isinstance(data_source, requests.Response):
        # 確保 HTTP 連線成功
        data_source.raise_for_status()
        try:
            json_payload = data_source.json()
        except ValueError as e:
            logger.error(f"無法解析 JSON 回應: {e}")
            raise ValueError("無效的 JSON 回應")
    elif isinstance(data_source, dict):
        json_payload = data_source
    else:
        raise TypeError(f"預期輸入 requests.Response 或 dict，實際得到 {type(data_source)}")

    # 2. 業務邏輯錯誤處理
    # API 可能傳回一個 dict, 或是帶有 msg 的結構
    if isinstance(json_payload, dict) and "msg" in json_payload:
        if json_payload.get("msg") != "success":
            msg = json_payload.get("msg", "未知錯誤")
            logger.error(f"API 回傳錯誤訊息: {msg}")
            raise ValueError(f"API 錯誤: {msg}")
        data_list = json_payload.get("data", [])
    elif isinstance(json_payload, list):
        data_list = json_payload
    else:
        logger.warning("非預期的 JSON 結構，無法提取 data 欄位")
        data_list = []

    # 3. 空資料防護 (例如假日或停止交易)
    if not data_list:
        logger.info(f"資料集 {dataset_name} 無資料。回傳空 DataFrame。")
        return pl.DataFrame()

    # 4. 轉換為 Polars DataFrame (Zero-copy)
    try:
        # infer_schema_length=None 強制掃描所有列以推斷 Schema
        df = pl.from_dicts(data_list, infer_schema_length=None)
    except Exception as e:
        logger.error(f"轉換資料列表為 DataFrame 失敗: {e}")
        raise e

    # 5. 嚴格 Schema 驗證
    try:
        df = enforce_schema(df, dataset_name)
    except Exception as e:
        logger.error(f"Schema 驗證失敗: {e}")
        raise e

    return df
