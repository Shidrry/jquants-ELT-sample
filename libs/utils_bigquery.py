import logging
from datetime import date

from google.cloud import bigquery

from libs.utils_metadata import record_data_load

logger = logging.getLogger(__name__)


def load_gcs_parquet_to_bq(
    project_id: str,
    env_name: str,
    gcs_uri: str,
    bq_dest: str,
    pipeline_name: str,
    logical_date: date,
    transform_sql: str | None = None,
    partition_field: str | None = None,
) -> None:
    """GCS の Parquet を BigQuery にロードする。

    transform_sql が指定された場合は 2 ステップで処理する:
      1. GCS Parquet → 一時テーブル（型推論任せ、API 仕様通りの型）
      2. CAST SQL → 最終テーブルの該当パーティションに書き込み
      3. 一時テーブルを削除
    transform_sql には {source} プレースホルダーを含む SELECT 文を渡す。

    partition_field を指定すると、そのカラムでパーティションプルーニングが有効になる。
    Date カラムが logical_date と一致するテーブル（daily_quotes 等）に適用する。
    """
    client = bigquery.Client(project=project_id)

    if transform_sql is None:
        # 直接ロード（型変換なし）
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            ),
        )
        load_job = client.load_table_from_uri(gcs_uri, bq_dest, job_config=job_config)
        load_job.result()
        row_count = load_job.output_rows
    else:
        # 2 ステップロード
        base_table = bq_dest.split("$")[0]
        temp_table_id = f"{base_table}_temp_{logical_date.strftime('%Y%m%d')}"
        try:
            # Step 1: 一時テーブルに raw ロード
            load_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            load_job = client.load_table_from_uri(
                gcs_uri, temp_table_id, job_config=load_config
            )
            load_job.result()
            row_count = load_job.output_rows

            # Step 2: CAST SQL で最終パーティションに書き込み
            query = transform_sql.format(source=f"`{temp_table_id}`")
            query_config = bigquery.QueryJobConfig(
                destination=bq_dest,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field,
                ),
            )
            query_job = client.query(query, job_config=query_config)
            query_job.result()
        finally:
            # Step 3: 一時テーブルを必ず削除
            client.delete_table(temp_table_id, not_found_ok=True)

    logger.info("Loaded to BigQuery %s", {"source": gcs_uri, "destination": bq_dest, "rows": row_count})

    record_data_load(
        client,
        project_id,
        env_name,
        pipeline_name=pipeline_name,
        logical_date=logical_date,
        gcs_uri=gcs_uri,
        bq_destination=bq_dest,
        row_count=row_count,
    )
