import logging

import pandas as pd

from libs.config import jquants_lake_bucket
from libs.utils import access_secret, dataframe_to_parquet_bytes, get_env_name, get_logical_date, get_project_id, upload_bytes_to_gcs
from libs.utils_jquants import fetch_jquants_master

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
GCS_OBJECT = "master/dt={date}/master.parquet"
BUCKET_FN  = jquants_lake_bucket  # (project_id, env_name) -> str

def fetch_records(project_id: str, date_str: str) -> list[dict]:
    api_key = access_secret("jquants-api-key", project_id)
    return list(fetch_jquants_master(date_str, api_key))
# -------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, logical_date_str = get_logical_date()
    logging.info("Job executed as one at %s", logical_date_str)

    records = fetch_records(project_id, logical_date_str)
    if not records:
        raise RuntimeError(
            f"No records returned for {logical_date_str}. "
            f"Date may be out of the API plan's available range."
        )

    bucket = BUCKET_FN(project_id, env_name)
    object_path = GCS_OBJECT.format(date=logical_date.isoformat())
    df = pd.DataFrame(records)
    upload_bytes_to_gcs(bucket, object_path, dataframe_to_parquet_bytes(df))

    logging.info("Fetched listing master %s", {
        "logical_date": logical_date.isoformat(),
        "records": len(records),
        "bucket": bucket,
        "object_path": object_path,
    })


if __name__ == "__main__":
    main()
