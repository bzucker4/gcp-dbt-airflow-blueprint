###############################################################################
# gcp-dbt-airflow-blueprint :: infrastructure/terraform_setup.tf
#
# PUBLIC PREVIEW: inputs and outputs only. The full file (~700 lines, validated
# against the hashicorp/google 6.50 provider schema and terraform-fmt clean)
# ships with the complete blueprint: https://payhip.com/b/YIq4E
#
# The full configuration provisions:
#   * Cloud Composer 2 (Airflow 2.x), dynamic workloads sizing (no machine types)
#   * BigQuery datasets: raw (read-only for dbt), staging, warehouse
#   * <prefix>-airflow     : runs Composer, ZERO BigQuery data access
#   * <prefix>-dbt-runner  : only BigQuery identity, reached by impersonation only
#   * <prefix>-deployer    : GitHub Actions identity via Workload Identity
#                            Federation, gated to one repo + one branch
#   * Composer bucket + versioned dbt state/artifact bucket with lifecycle rules
#   * Secret Manager as Airflow secrets backend, IAM-conditioned to airflow-*
###############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.30.0, < 7.0.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.30.0, < 7.0.0"
    }
  }
}

variable "project_id" {
  description = "GCP project that hosts Composer, BigQuery and the dbt artifact store."
  type        = string
}

variable "region" {
  description = "Region for Cloud Composer and the GCS buckets."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "environment" {
  description = "dev, staging or prod."
  type        = string
  default     = "prod"
}

variable "github_repository" {
  description = "owner/repo allowed to deploy via OIDC."
  type        = string
}

variable "composer_workloads" {
  description = "Airflow component CPU / memory / storage / autoscaling. Defaults are the lowest-cost profile that runs dbt reliably."
  type = object({
    scheduler_cpu        = number
    scheduler_memory_gb  = number
    scheduler_storage_gb = number
    scheduler_count      = number
    webserver_cpu        = number
    webserver_memory_gb  = number
    webserver_storage_gb = number
    worker_cpu           = number
    worker_memory_gb     = number
    worker_storage_gb    = number
    worker_min_count     = number
    worker_max_count     = number
  })
  default = {
    scheduler_cpu        = 0.5
    scheduler_memory_gb  = 2
    scheduler_storage_gb = 1
    scheduler_count      = 1
    webserver_cpu        = 0.5
    webserver_memory_gb  = 2
    webserver_storage_gb = 1
    worker_cpu           = 1
    worker_memory_gb     = 4
    worker_storage_gb    = 2
    worker_min_count     = 1
    worker_max_count     = 3
  }
}

variable "dbt_py_requirements" {
  description = "pip requirements for the isolated dbt virtualenv on workers (dbt is never installed into Composer itself)."
  type        = list(string)
  default     = ["dbt-core>=1.11.0,<1.12.0", "dbt-bigquery>=1.11.0,<1.12.0"]
}

# ---------------------------------------------------------------------------
# Resources (full version): google_project_service, google_service_account x3,
# google_project_iam_member, google_service_account_iam_member,
# google_bigquery_dataset + dataset-scoped IAM, google_storage_bucket x2 + IAM,
# google_secret_manager_secret, google_composer_environment,
# google_iam_workload_identity_pool + provider.
#
# Get the complete, deployable file: https://payhip.com/b/YIq4E
# ---------------------------------------------------------------------------
