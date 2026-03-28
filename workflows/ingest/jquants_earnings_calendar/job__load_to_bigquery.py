from datetime import datetime
from zoneinfo import ZoneInfo

from libs.config import jquants_lake_bucket, jquants_staging_dataset
from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_bigquery import load_gcs_parquet_to_bq

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
PIPELINE_NAME = "jquants_earnings_calendar"
GCS_OBJECT    = "earnings_calendar/dt={date}/earnings_calendar.parquet"
BUCKET_FN     = jquants_lake_bucket      # (project_id, env_name) -> str
DATASET_FN    = jquants_staging_dataset  # (env_name) -> str
BQ_TABLE      = "earnings_calendar"

# Date フィールドを STRING → DATE にキャストする。
# その他フィールドは API 仕様通り STRING のため SAFE_CAST で型を保証する。
# パーティションは実際の実行日（JST）ベースの ingestion-time partitioning で管理する。
# logical_date でのバックフィル実行が過去パーティションを誤上書きしないよう、
# BQ パーティションキーおよび GCS パスには execution_date を使用する。
TRANSFORM_SQL = """
SELECT
  DATE('{EXECUTION_DATE}') AS execution_date,
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(Date     AS STRING)) AS Date,
  SAFE_CAST(Code     AS STRING) AS Code,
  SAFE_CAST(CoName   AS STRING) AS CoName,
  SAFE_CAST(FY       AS STRING) AS FY,
  SAFE_CAST(SectorNm AS STRING) AS SectorNm,
  SAFE_CAST(FQ       AS STRING) AS FQ,
  SAFE_CAST(Section  AS STRING) AS Section
FROM {source}
"""
# -------------------------------------------------------------------------


def main() -> None:
    env_name = get_env_name()
    project_id = get_project_id()
    logical_date, _ = get_logical_date()
    execution_date = datetime.now(ZoneInfo("Asia/Tokyo")).date()

    gcs_uri       = f"gs://{BUCKET_FN(project_id, env_name)}/{GCS_OBJECT.format(date=execution_date.isoformat())}"
    bq_dest       = f"{project_id}.{DATASET_FN(env_name)}.{BQ_TABLE}${execution_date.strftime('%Y%m%d')}"
    transform_sql = TRANSFORM_SQL.replace("{EXECUTION_DATE}", execution_date.isoformat())

    load_gcs_parquet_to_bq(project_id, env_name, gcs_uri, bq_dest, PIPELINE_NAME, logical_date, transform_sql, partition_field="execution_date")


if __name__ == "__main__":
    main()
