from datetime import date, datetime, timezone

from google.cloud import bigquery

from libs.config import pipeline_metadata_dataset, DATA_LOADS_TABLE

_SCHEMA = [
    bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("logical_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("gcs_uri", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("bq_destination", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("row_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def record_data_load(
    client: bigquery.Client,
    project_id: str,
    env_name: str,
    *,
    pipeline_name: str,
    logical_date: date,
    gcs_uri: str,
    bq_destination: str,
    row_count: int,
) -> None:
    dataset_id = pipeline_metadata_dataset(env_name)
    table_ref = f"{project_id}.{dataset_id}.{DATA_LOADS_TABLE}"

    client.create_table(bigquery.Table(table_ref, schema=_SCHEMA), exists_ok=True)

    rows = [
        {
            "pipeline_name": pipeline_name,
            "logical_date": logical_date.isoformat(),
            "gcs_uri": gcs_uri,
            "bq_destination": bq_destination,
            "row_count": row_count,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }
    ]

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"Failed to insert pipeline run metadata: {errors}")
