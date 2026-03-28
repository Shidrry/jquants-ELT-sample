import os
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Tuple

from google.cloud import storage, secretmanager

## 環境変数の取得
def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

## dev, prodの判定
def get_env_name() -> str:
    return require_env("ENV_NAME")

## project_idの取得
def get_project_id() -> str:
    url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
    headers = {"Metadata-Flavor": "Google"}
    r = requests.get(url, headers=headers, timeout=5)
    r.raise_for_status()
    return r.text

## 環境変数からlogical dateを取得、できなければ実行時刻
def get_logical_date():
    raw_value = os.environ.get("LOGICAL_DATE")
    if raw_value:
        logical_date = datetime.strptime(raw_value, "%Y-%m-%d").date()
    else:
        logical_date = datetime.now(ZoneInfo("Asia/Tokyo")).date()
    return logical_date, logical_date.strftime("%Y%m%d")

## secretを取得
def access_secret(secret_id: str, project_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name, timeout=10.0)
    return response.payload.data.decode("utf-8")

## dfをparquestへ変換
def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()

## GCSへアップロード
def upload_bytes_to_gcs(
    bucket_name: str,
    object_path: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(object_path)
    blob.upload_from_string(data, content_type=content_type)