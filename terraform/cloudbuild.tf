# github_repo_url から owner と repo 名を抽出
# 例: "https://github.com/your-org/your-repo.git" → ["your-org", "your-repo"]
locals {
  _github_parts    = regex("github\\.com/([^/]+)/([^/\\.]+)", var.github_repo_url)
  github_owner     = local._github_parts[0]
  github_repo_name = local._github_parts[1]
}

# dev / prod それぞれの Cloud Build トリガー
# 前提: Cloud Console で Cloud Build GitHub App をリポジトリにインストール済みであること
resource "google_cloudbuild_trigger" "deploy" {
  for_each = {
    dev = {
      name             = "dev-deploy"
      branch           = "^develop$"
      worker_sa        = google_service_account.worker_dev.email
      generate_diagram = "true"
    }
    prod = {
      name             = "prod-deploy"
      branch           = "^main$"
      worker_sa        = google_service_account.worker_prod.email
      generate_diagram = null
    }
  }

  project         = var.project_id
  name            = each.value.name
  location        = var.region
  service_account = "projects/${var.project_id}/serviceAccounts/${google_service_account.cloudbuild.email}"

  github {
    owner = local.github_owner
    name  = local.github_repo_name
    push {
      branch = each.value.branch
    }
  }

  filename = "build_deploy.yaml"

  substitutions = merge(
    {
      _ENV                    = each.key
      _SCHEDULER_SA_EMAIL     = google_service_account.scheduler.email
      _WORKFLOW_EXEC_SA_EMAIL = each.value.worker_sa
    },
    each.value.generate_diagram != null ? { _GENERATE_DIAGRAM = each.value.generate_diagram } : {}
  )

  depends_on = [google_project_service.apis["cloudbuild.googleapis.com"]]
}
