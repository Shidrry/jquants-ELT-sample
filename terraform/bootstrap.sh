#!/usr/bin/env bash
# bootstrap.sh — Terraformのリモートステートバケットを作成し、初期化する
# 新規プロジェクトのセットアップ時に一度だけ実行する
#
# Usage:
#   cd terraform/
#   ./bootstrap.sh <GCP_PROJECT_ID> [REGION]
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Billing enabled on the GCP project

set -euo pipefail

PROJECT_ID="${1:?Usage: $0 <PROJECT_ID> [REGION]}"
REGION="${2:-asia-northeast1}"
BUCKET_NAME="${PROJECT_ID}-tfstate"

echo "=== Bootstrap Terraform for project: ${PROJECT_ID} ==="
echo "Region: ${REGION}"
echo "State bucket: ${BUCKET_NAME}"

# Enable required APIs for bootstrap
echo ""
echo "Enabling required GCP APIs..."
gcloud services enable storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  --project="${PROJECT_ID}"

# Create GCS bucket for Terraform state
echo ""
echo "Creating Terraform state bucket: gs://${BUCKET_NAME}..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "Bucket already exists, skipping creation."
else
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access
  echo "Bucket created."
fi

# Enable versioning on the state bucket
gcloud storage buckets update "gs://${BUCKET_NAME}" \
  --versioning \
  --project="${PROJECT_ID}"
echo "Versioning enabled on state bucket."

# Copy terraform.tfvars.template to terraform.tfvars if it doesn't exist
if [ ! -f "terraform.tfvars" ]; then
  cp terraform.tfvars.template terraform.tfvars
  # Replace placeholder with actual project ID
  sed -i "s|gcp-jquants-etl-sample|${PROJECT_ID}|g" terraform.tfvars
  echo ""
  echo "Created terraform.tfvars from template."
  echo "Please review and update terraform.tfvars before running terraform apply."
else
  echo "terraform.tfvars already exists, skipping."
fi

# Initialize Terraform with remote backend
echo ""
echo "Initializing Terraform..."
terraform init -backend-config="bucket=${BUCKET_NAME}"

echo ""
echo "=== Bootstrap complete! ==="
echo ""
echo "Next steps:"
echo "  1. Review terraform/terraform.tfvars and set your GitHub repo URL"
echo "  2. Run: terraform plan"
echo "  3. Run: terraform apply"
echo "  4. Set secrets in Secret Manager (see README for details)"
echo "  5. Connect GitHub repository to Cloud Build in the GCP Console"
