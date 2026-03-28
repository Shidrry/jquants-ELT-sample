# -------------------------------------------------------
# Service Accounts
# -------------------------------------------------------

# Cloud Scheduler → dispatcher OIDC 用 SA
resource "google_service_account" "scheduler" {
  project      = var.project_id
  account_id   = "sa-scheduler-workflow"
  display_name = "sa-scheduler-workflow"
  description  = "スケジューラーの設定用アカウント"
}

# Cloud Run / Workflows / Jobs 実行用 SA（dev）
resource "google_service_account" "worker_dev" {
  project      = var.project_id
  account_id   = "sa-worker-dev"
  display_name = "sa-worker-dev"
}

# Cloud Run / Workflows / Jobs 実行用 SA（prod）
resource "google_service_account" "worker_prod" {
  project      = var.project_id
  account_id   = "sa-worker-prod"
  display_name = "sa-worker-prod"
}

# Cloud Build デプロイ用 SA
resource "google_service_account" "cloudbuild" {
  project      = var.project_id
  account_id   = "sa-cloudbuild"
  display_name = "Cloud Build deploy SA"
  description  = "workflow, jobのデプロイを担当するサービスアカウント"
}

# 検証用 SA
resource "google_service_account" "verification" {
  project      = var.project_id
  account_id   = "sa-verification"
  display_name = "sa-verification"
  description  = "基本的にすべてのデータを参照可能な検証用アカウント"
}

# -------------------------------------------------------
# Custom role: Cloud Run Job を overrides 付きで実行する権限
# roles/run.invoker は run.jobs.run のみ含み run.jobs.runWithOverrides を含まないため
# -------------------------------------------------------

resource "google_project_iam_custom_role" "run_job_runner_with_overrides" {
  project     = var.project_id
  role_id     = "runJobRunnerWithOverrides"
  title       = "Cloud Run Job Runner with Overrides"
  permissions = ["run.jobs.runWithOverrides"]
}

# -------------------------------------------------------
# IAM bindings — sa-worker-dev / sa-worker-prod
# -------------------------------------------------------

locals {
  worker_roles = [
    "roles/run.invoker",                  # Cloud Run Service 呼び出し・Job 実行
    "roles/run.viewer",                   # Cloud Run サービスメタデータ取得（services.get）
    "roles/workflows.invoker",            # Workflows 実行
    "roles/bigquery.jobUser",             # BigQuery クエリ実行
    "roles/dataform.editor",              # Dataform コンパイル / 実行
    "roles/secretmanager.secretAccessor", # シークレット読み取り
    "roles/iam.serviceAccountUser",       # SA の impersonate
    "roles/logging.logWriter",            # ログ書き込み
    "roles/logging.privateLogViewer",     # ログ閲覧
  ]

  worker_sas = {
    dev  = google_service_account.worker_dev.email
    prod = google_service_account.worker_prod.email
  }
}

resource "google_project_iam_member" "worker" {
  for_each = {
    for pair in setproduct(["dev", "prod"], local.worker_roles) :
    "${pair[0]}/${pair[1]}" => { env = pair[0], role = pair[1] }
  }

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${local.worker_sas[each.value.env]}"
}

resource "google_project_iam_member" "worker_run_job_runner_with_overrides" {
  for_each = local.worker_sas

  project = var.project_id
  role    = google_project_iam_custom_role.run_job_runner_with_overrides.name
  member  = "serviceAccount:${each.value}"
}

# -------------------------------------------------------
# IAM bindings — sa-scheduler-workflow
# -------------------------------------------------------

locals {
  scheduler_roles = [
    "roles/run.invoker",       # dispatcher（Cloud Run）を OIDC で呼び出す
    "roles/workflows.invoker", # Workflows 実行
    "roles/logging.logWriter", # ログ書き込み
  ]
}

resource "google_project_iam_member" "scheduler" {
  for_each = toset(local.scheduler_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

# -------------------------------------------------------
# IAM bindings — sa-cloudbuild
# -------------------------------------------------------

locals {
  cloudbuild_roles = [
    "roles/artifactregistry.writer",      # コンテナイメージ push
    "roles/cloudbuild.builds.builder",    # Cloud Build 実行
    "roles/cloudscheduler.admin",         # Cloud Scheduler 管理
    "roles/iam.serviceAccountUser",       # SA の impersonate
    "roles/run.admin",                    # Cloud Run 管理
    "roles/secretmanager.secretAccessor", # シークレット読み取り
    "roles/workflows.admin",              # Workflows 管理
  ]
}

resource "google_project_iam_member" "cloudbuild" {
  for_each = toset(local.cloudbuild_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.cloudbuild.email}"
}

# -------------------------------------------------------
# IAM bindings — sa-verification
# -------------------------------------------------------

resource "google_project_iam_member" "verification_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.verification.email}"
}

# -------------------------------------------------------
# IAM bindings — Workflows サービスエージェント → worker SA
# Cloud Workflows が googleapis.* コネクタ呼び出し時にトークンを生成できるよう設定
# -------------------------------------------------------

# Workflows サービスエージェントを明示的にプロビジョニング
# google_project_service だけではエージェント SA が作成されないため、
# google_project_service_identity で強制的に作成する
resource "google_project_service_identity" "workflows" {
  provider   = google-beta
  project    = var.project_id
  service    = "workflows.googleapis.com"
  depends_on = [google_project_service.apis["workflows.googleapis.com"]]
}

# SA 作成後の GCP 側伝播を待つ（即時 IAM バインディングすると SA not found になるため）
resource "time_sleep" "wait_for_workflows_sa" {
  depends_on      = [google_project_service_identity.workflows]
  create_duration = "30s"
}

locals {
  workflows_service_agent = "serviceAccount:${google_project_service_identity.workflows.email}"
}

# -------------------------------------------------------
# IAM bindings — Dataform サービスエージェント → Secret Manager
# Dataform が Git トークン Secret を取得するために必要
# -------------------------------------------------------

resource "google_project_service_identity" "dataform" {
  provider   = google-beta
  project    = var.project_id
  service    = "dataform.googleapis.com"
  depends_on = [google_project_service.apis["dataform.googleapis.com"]]
}

resource "google_secret_manager_secret_iam_member" "dataform_agent_secret_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets["dataform-github"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_project_service_identity.dataform.email}"
}

resource "google_service_account_iam_member" "workflows_token_creator" {
  for_each = {
    dev  = google_service_account.worker_dev.email
    prod = google_service_account.worker_prod.email
  }
  service_account_id = "projects/${var.project_id}/serviceAccounts/${each.value}"
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.workflows_service_agent

  depends_on = [time_sleep.wait_for_workflows_sa]
}
