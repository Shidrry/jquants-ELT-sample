from libs.config import jquants_lake_bucket, jquants_staging_dataset
from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_bigquery import load_gcs_parquet_to_bq

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
PIPELINE_NAME = "jquants_ohlcv"
GCS_OBJECT    = "ohlcv/dt={date}/daily_quotes.parquet"
BUCKET_FN     = jquants_lake_bucket      # (project_id, env_name) -> str
DATASET_FN    = jquants_staging_dataset  # (env_name) -> str
BQ_TABLE      = "daily_quotes"

# Date フィールドを STRING → DATE にキャストする。
# 数値フィールドは API が number 型で返すが、SAFE_CAST で型を保証する。
# Premium プラン限定フィールド（前場・後場）も含め * EXCEPT(Date) で一括取得する。
TRANSFORM_SQL = """
SELECT
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(Date AS STRING)) AS Date,
  * EXCEPT (Date)
FROM {source}
"""
# -------------------------------------------------------------------------


def main() -> None:
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, _ = get_logical_date()

    gcs_uri  = f"gs://{BUCKET_FN(project_id, env_name)}/{GCS_OBJECT.format(date=logical_date.isoformat())}"
    bq_dest  = f"{project_id}.{DATASET_FN(env_name)}.{BQ_TABLE}${logical_date.strftime('%Y%m%d')}"

    load_gcs_parquet_to_bq(project_id, env_name, gcs_uri, bq_dest, PIPELINE_NAME, logical_date, TRANSFORM_SQL, partition_field="Date")


if __name__ == "__main__":
    main()
