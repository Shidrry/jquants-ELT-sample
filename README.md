# jquants-ETL-sample

A production-grade, fully serverless ETL pipeline on GCP that ingests daily Japanese stock market data from the [J-Quants API](https://jpx-jquants.com/), stores it in BigQuery, and powers a Looker Studio monitoring dashboard — all deployed via Infrastructure as Code and GitOps CI/CD.

## Architecture

```
Cloud Scheduler (OIDC)
  └─► workflow_dispatcher (Cloud Run Service)
        └─► Cloud Workflows
              ├─► market_open (Cloud Run Service) ── skip if market closed
              ├─► ingest_to_gcs  (Cloud Run Job)  ── J-Quants API → GCS Parquet
              └─► load_to_bigquery (Cloud Run Job) ── GCS → BigQuery (partitioned)
                                                        │
                                                   Dataform (triggered after load)
                                                        └─► mart.report_stock_dashboard
                                                                   │
                                                            Looker Studio Dashboard
```

## Data Pipelines

| Pipeline | Data Source | Schedule (JST) | BigQuery Table |
|---|---|---|---|
| `jquants_ohlcv` | Daily OHLCV (bars) | 18:00 weekdays | `staging_jquants.daily_quotes` |
| `jquants_fins_summary` | Financial summary | 19:30 weekdays | `staging_jquants.fins_summary` |
| `jquants_fins_summary_confirmed` | Confirmed financials | 01:30 weekdays | `staging_jquants.fins_summary` |
| `jquants_earnings_calendar` | Earnings calendar | 19:30 weekdays | `staging_jquants.earnings_calendar` |
| `jquants_master` | Company master | 03:00 on 10th of month | `staging_jquants.master` |
| `daily_monitoring_dashboard` | Dataform transform | 18:20 weekdays | `mart.report_stock_dashboard` |

## Key Design Decisions

- **Serverless**: Cloud Run Jobs + Cloud Workflows + Cloud Scheduler — zero persistent servers, pay-per-use
- **GitOps CI/CD**: `build_deploy.yaml` is the single source of truth. Push to `develop` → dev deploy; push to `main` → prod deploy
- **Declarative pipelines**: Each `workflow.yaml` defines its own schedule and jobs via `__metadata__`. Adding a new pipeline requires only a new `workflow.yaml`
- **Two-stage BigQuery load**: GCS Parquet → temp table → CAST SQL → final partitioned table. Ensures type safety without schema management overhead
- **Market-aware scheduling**: Workflows skip execution on non-business days by checking the J-Quants calendar API
- **Self-healing cleanup**: Cloud Build deletes Cloud Run / Workflows / Scheduler resources that no longer exist in the repository
- **Secrets in Secret Manager**: Zero credentials in the repository. All API keys and tokens stored in GCP Secret Manager, accessed at runtime
- **Idempotent loads**: Daily partitions are overwritten on re-run (WRITE_TRUNCATE), making backfills safe

## GCP Services Used

| Service | Role |
|---|---|
| Cloud Run (Services) | `market_open` health check, `workflow_dispatcher` trigger bridge |
| Cloud Run (Jobs) | Per-pipeline ingest and load jobs |
| Cloud Workflows | Orchestration, market-open gating, error handling |
| Cloud Scheduler | Cron triggers via OIDC to dispatcher |
| Cloud Storage | Data lake (GCS Parquet, partitioned by date) |
| BigQuery | Staging tables (partitioned) + Dataform mart |
| Dataform | SQL transformations for reporting |
| Artifact Registry | Docker image registry |
| Cloud Build | CI/CD pipeline |
| Secret Manager | API keys and tokens |

## Repository Structure

```
.
├── build_deploy.yaml          # Cloud Build CI/CD pipeline (single source of truth)
├── Dockerfile                 # Shared container image for all jobs and services
├── requirements.txt           # Python dependencies
├── package.json               # Dataform Node.js dependencies (@dataform/core)
├── dataform.json              # Dataform project configuration
├── libs/                      # Shared Python libraries
│   ├── config.py              # GCP resource naming conventions
│   ├── utils.py               # GCS, Secret Manager, env helpers
│   ├── utils_bigquery.py      # BigQuery load with 2-stage CAST
│   ├── utils_discord.py       # Discord error notifications
│   ├── utils_jquants.py       # J-Quants API client (paginated)
│   └── utils_metadata.py      # Pipeline run metadata recording
├── services/
│   ├── market_open/           # FastAPI: checks if market is open via J-Quants
│   └── workflow_dispatcher/   # FastAPI: bridges Cloud Scheduler → Cloud Workflows
├── workflows/
│   ├── ingest/                # One directory per ingestion pipeline
│   │   ├── jquants_ohlcv/
│   │   │   ├── workflow.yaml          # Cloud Workflows definition + schedule metadata
│   │   │   ├── job__ingest_to_gcs.py  # Fetch from J-Quants API → GCS
│   │   │   └── job__load_to_bigquery.py # GCS Parquet → BigQuery
│   │   ├── jquants_fins_summary/
│   │   ├── jquants_fins_summary_confirmed/
│   │   ├── jquants_earnings_calendar/
│   │   └── jquants_master/
│   ├── transform/
│   │   └── daily_monitoring_dashboard/ # Triggers Dataform compilation + invocation
│   └── utils/
│       └── backfill_executor/  # Sequential backfill for a date range
├── definitions/               # Dataform SQL transformations
│   ├── sources/jquants/       # Source declarations (point to staging tables)
│   ├── intermediate/
│   │   └── int_daily_stock_metrics.sqlx  # Enriched metrics (price changes, flags, market cap)
│   └── reports/
│       └── report_stock_dashboard.sqlx   # Final Looker Studio table (last 5 trading days)
├── includes/
│   └── env.js                 # Dataform env vars (dataset names)
└── terraform/                 # Infrastructure as Code
    ├── bootstrap.sh           # One-time setup: create state bucket + terraform init
    ├── terraform.tfvars.template
    ├── main.tf                # Terraform backend (GCS) + provider config
    ├── variables.tf
    ├── apis.tf                # GCP API enablement
    ├── iam.tf                 # Service accounts + IAM bindings
    ├── gcs.tf                 # Data lake buckets
    ├── bigquery.tf            # BigQuery datasets + IAM
    ├── artifact_registry.tf   # Docker registry
    ├── cloudbuild.tf          # Cloud Build triggers (dev/prod)
    ├── dataform.tf            # Dataform repositories
    └── secret_manager.tf      # Secret shells (values set manually)
```

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Terraform >= 1.5
- GitHub repository connected to Cloud Build (via Cloud Build GitHub App)
- J-Quants API key ([sign up](https://jpx-jquants.com/))

## Deployment

### 1. Bootstrap Terraform

```bash
cd terraform/
./bootstrap.sh gcp-jquants-etl-sample asia-northeast1
```

This creates the GCS state bucket, generates `terraform.tfvars` from the template, and runs `terraform init`.

### 2. Configure variables

Edit `terraform/terraform.tfvars`:

```hcl
project_id      = "gcp-jquants-etl-sample"
region          = "asia-northeast1"
github_repo_url = "https://github.com/<YOUR_OWNER>/jquants-ETL-sample.git"
```

### 3. Apply infrastructure

```bash
cd terraform/
terraform plan
terraform apply
```

This provisions all GCP resources: service accounts, IAM roles, GCS buckets, BigQuery datasets, Artifact Registry, Cloud Build triggers, Dataform repositories, and Secret Manager shells.

### 4. Set secrets

```bash
# J-Quants API key (required)
echo -n "YOUR_JQUANTS_API_KEY" | \
  gcloud secrets versions add jquants-api-key \
  --project=gcp-jquants-etl-sample \
  --data-file=-

# Discord webhook for error notifications (required)
echo -n "<YOUR_DISCORD_WEBHOOK_URL>" | \
  gcloud secrets versions add discord-webhook-url \
  --project=gcp-jquants-etl-sample \
  --data-file=-

# GitHub token for Dataform Git sync (required)
echo -n "<YOUR_GITHUB_PERSONAL_ACCESS_TOKEN>" | \
  gcloud secrets versions add dataform-github \
  --project=gcp-jquants-etl-sample \
  --data-file=-
```

### 5. Connect GitHub to Cloud Build

In the GCP Console → Cloud Build → Triggers, connect the GitHub App to your repository. The `dev-deploy` and `prod-deploy` triggers are already configured by Terraform.

### 6. Deploy

Push to the `develop` branch to trigger the dev deployment:

```bash
git checkout -b develop
git push origin develop
```

Cloud Build will:
1. Build and push the Docker image to Artifact Registry
2. Deploy Cloud Run Services (market_open, workflow_dispatcher)
3. Deploy Cloud Run Jobs for each pipeline step
4. Deploy Cloud Workflows with rendered YAML
5. Create/update Cloud Scheduler jobs
6. Clean up orphaned resources

### Backfill

To backfill historical data for a pipeline, trigger the `backfill_executor` workflow manually from the GCP Console or via `gcloud`:

```bash
gcloud workflows run dev--backfill_executor \
  --location=asia-northeast1 \
  --data='{"workflow_name": "dev--jquants_ohlcv", "start": "2025-01-01", "end": "2025-01-31"}'
```

## Looker Studio Dashboard

The `mart.report_stock_dashboard` table is refreshed daily after market close. It contains the last 5 trading days of enriched stock metrics:

- **Price metrics**: OHLCV, adjusted close/volume, stop-high/low flags
- **Momentum**: Price change % (1d / 5d / 20d / 60d), turnover change %
- **Fundamentals**: Market cap (million JPY), last earnings date, next earnings date
- **Rankings**: Turnover rank within each market (Prime / Growth / Standard)

Connect Looker Studio to the `mart_prod.report_stock_dashboard` BigQuery table and set a daily refresh schedule.

## Example Output

The following is an example of what can be built on top of this pipeline. The application (not included in this repository) consumes the BigQuery mart tables and delivers daily analysis reports to Discord.

### Daily Sector Fund Flow Report (Discord)

![Daily sector fund flow report](docs/images/output_sample_daily_report.png)

The example above shows one of several analysis reports delivered each evening after market close. Reports include (but are not limited to):

- **Market summary**: Overall fund flow trends, new money / rotation signals
- **Sector analysis**: Top inflows/outflows by sector with specific stock highlights
- **Themes**: Momentum themes active on the day

## Environment Strategy

| Branch | Environment | BigQuery Datasets |
|---|---|---|
| `develop` | dev | `staging_jquants_dev`, `pipeline_metadata_dev`, `mart_dev` |
| `main` | prod | `staging_jquants_prod`, `pipeline_metadata_prod`, `mart_prod` |

Dev schedulers are automatically paused when a prod deployment runs.
