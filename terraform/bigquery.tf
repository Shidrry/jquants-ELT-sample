# staging_jquants_{env} — jQuants raw データ格納先（GCS → BQ ロード先）
resource "google_bigquery_dataset" "staging_jquants" {
  for_each    = toset(["dev", "prod"])
  project     = var.project_id
  dataset_id  = "staging_jquants_${each.value}"
  location    = var.region
  description = "jQuants raw data (${each.value})"

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

# pipeline_metadata_{env} — データロード管理テーブル（data_loads テーブルを格納）
resource "google_bigquery_dataset" "pipeline_metadata" {
  for_each    = toset(["dev", "prod"])
  project     = var.project_id
  dataset_id  = "pipeline_metadata_${each.value}"
  location    = var.region
  description = "Pipeline metadata (${each.value})"

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

# mart_{env} — Dataform の成果物（report_stock_dashboard など）
resource "google_bigquery_dataset" "mart" {
  for_each    = toset(["dev", "prod"])
  project     = var.project_id
  dataset_id  = "mart_${each.value}"
  location    = var.region
  description = "Dataform output mart (${each.value})"

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

# -------------------------------------------------------
# BigQuery データセットレベル IAM
# dev: sa-worker-dev WRITER / sa-verification READER
# prod: sa-worker-prod WRITER / sa-worker-dev READER / sa-verification READER
# -------------------------------------------------------

locals {
  bq_datasets = {
    staging_jquants_dev    = google_bigquery_dataset.staging_jquants["dev"].dataset_id
    staging_jquants_prod   = google_bigquery_dataset.staging_jquants["prod"].dataset_id
    pipeline_metadata_dev  = google_bigquery_dataset.pipeline_metadata["dev"].dataset_id
    pipeline_metadata_prod = google_bigquery_dataset.pipeline_metadata["prod"].dataset_id
    mart_dev               = google_bigquery_dataset.mart["dev"].dataset_id
    mart_prod              = google_bigquery_dataset.mart["prod"].dataset_id
  }

  # dev データセット: sa-worker-dev WRITER
  bq_writer_dev = {
    for k, v in local.bq_datasets : k => v if endswith(k, "_dev")
  }
  # prod データセット: sa-worker-prod WRITER
  bq_writer_prod = {
    for k, v in local.bq_datasets : k => v if endswith(k, "_prod")
  }
  # prod データセット: sa-worker-dev READER（prod データを dev から参照できるよう）
  bq_reader_dev_on_prod = {
    for k, v in local.bq_datasets : k => v if endswith(k, "_prod")
  }
  # 全データセット: sa-verification READER
  bq_reader_verification = local.bq_datasets
}

resource "google_bigquery_dataset_access" "writer_dev" {
  for_each   = local.bq_writer_dev
  project    = var.project_id
  dataset_id = each.value
  role       = "WRITER"
  user_by_email = google_service_account.worker_dev.email
}

resource "google_bigquery_dataset_access" "writer_prod" {
  for_each   = local.bq_writer_prod
  project    = var.project_id
  dataset_id = each.value
  role       = "WRITER"
  user_by_email = google_service_account.worker_prod.email
}

resource "google_bigquery_dataset_access" "reader_dev_on_prod" {
  for_each   = local.bq_reader_dev_on_prod
  project    = var.project_id
  dataset_id = each.value
  role       = "READER"
  user_by_email = google_service_account.worker_dev.email
}

resource "google_bigquery_dataset_access" "reader_verification" {
  for_each   = local.bq_reader_verification
  project    = var.project_id
  dataset_id = each.value
  role       = "READER"
  user_by_email = google_service_account.verification.email
}
