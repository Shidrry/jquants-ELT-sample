# CI/CD 成果物バケット（アーキテクチャダイアグラムなど）
# build_deploy.yaml: gs://${PROJECT_ID}-build-artifacts/diagrams/...
resource "google_storage_bucket" "build_artifacts" {
  project                     = var.project_id
  name                        = "${var.project_id}-build-artifacts"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  depends_on = [google_project_service.apis["storage.googleapis.com"]]
}

# jQuants raw データ Lake（GCS → BigQuery のステージング）
# libs/config.py: lake-jquants-{project_id}-{env_name}
resource "google_storage_bucket" "jquants_lake" {
  for_each                    = toset(["dev", "prod"])
  project                     = var.project_id
  name                        = "lake-jquants-${var.project_id}-${each.value}"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  depends_on = [google_project_service.apis["storage.googleapis.com"]]
}

# バケットレベル IAM: dev バケット → sa-worker-dev、prod バケット → sa-worker-prod
locals {
  lake_worker_sa = {
    dev  = google_service_account.worker_dev.email
    prod = google_service_account.worker_prod.email
  }
}

resource "google_storage_bucket_iam_member" "jquants_lake_worker" {
  for_each = toset(["dev", "prod"])
  bucket   = google_storage_bucket.jquants_lake[each.value].name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${local.lake_worker_sa[each.value]}"
}
