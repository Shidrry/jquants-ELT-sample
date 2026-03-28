# シェルのみ作成。実際の値は gcloud secrets versions add で設定すること。
locals {
  secrets = [
    "discord-webhook-url",
    "slack-webhook-url",
    "jquants-api-key",
    "jquants-mailaddress",
    "jquants-password",
    "github-token",
    "dataform-github",
  ]
}

resource "google_secret_manager_secret" "secrets" {
  for_each  = toset(local.secrets)
  project   = var.project_id
  secret_id = each.value

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}
