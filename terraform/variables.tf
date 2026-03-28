variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-northeast1"
}

variable "github_repo_url" {
  description = "Dataform が参照する GitHub リポジトリの URL"
  type        = string
}
