from libs.config import jquants_lake_bucket, jquants_staging_dataset
from libs.utils import get_env_name, get_logical_date, get_project_id
from libs.utils_bigquery import load_gcs_parquet_to_bq

# ---- 新しいパイプライン作成時にここだけ変更する -------------------------
PIPELINE_NAME = "jquants_master"
GCS_OBJECT    = "master/dt={date}/master.parquet"
BUCKET_FN     = jquants_lake_bucket      # (project_id, env_name) -> str
DATASET_FN    = jquants_staging_dataset  # (env_name) -> str
BQ_TABLE      = "master"

# Date フィールドを STRING → DATE にキャストする。
# その他フィールドは API 仕様通り STRING のため SAFE_CAST で型を保証する。
TRANSFORM_SQL = """
SELECT
  SAFE.PARSE_DATE('%Y-%m-%d', SAFE_CAST(Date     AS STRING)) AS Date,
  SAFE_CAST(Code     AS STRING) AS Code,
  SAFE_CAST(CoName   AS STRING) AS CoName,
  SAFE_CAST(CoNameEn AS STRING) AS CoNameEn,
  SAFE_CAST(S17      AS STRING) AS S17,
  SAFE_CAST(S17Nm    AS STRING) AS S17Nm,
  SAFE_CAST(S33      AS STRING) AS S33,
  SAFE_CAST(S33Nm    AS STRING) AS S33Nm,
  SAFE_CAST(ScaleCat AS STRING) AS ScaleCat,
  SAFE_CAST(Mkt      AS STRING) AS Mkt,
  SAFE_CAST(MktNm    AS STRING) AS MktNm,
  SAFE_CAST(Mrgn     AS STRING) AS Mrgn,
  SAFE_CAST(MrgnNm   AS STRING) AS MrgnNm
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
