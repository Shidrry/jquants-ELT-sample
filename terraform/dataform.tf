# build_deploy.yaml: dataform_repo = "stock-analytics-" + env
resource "google_dataform_repository" "pipeline" {
  provider = google-beta
  project  = var.project_id
  region   = var.region

  for_each = {
    dev = {
      branch          = "develop"
      service_account = google_service_account.worker_dev.email
    }
    prod = {
      branch          = "main"
      service_account = google_service_account.worker_prod.email
    }
  }

  name            = "stock-analytics-${each.key}"
  service_account = each.value.service_account

  git_remote_settings {
    url                                 = var.github_repo_url
    default_branch                      = each.value.branch
    authentication_token_secret_version = "projects/${var.project_id}/secrets/dataform-github/versions/latest"
  }

  depends_on = [google_project_service.apis["dataform.googleapis.com"]]
}
