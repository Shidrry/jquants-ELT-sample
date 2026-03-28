resource "google_artifact_registry_repository" "data_pipeline" {
  project       = var.project_id
  location      = var.region
  repository_id = "data-pipeline"
  format        = "DOCKER"
  description   = "Docker images for the data pipeline"

  depends_on = [google_project_service.apis["artifactregistry.googleapis.com"]]
}
