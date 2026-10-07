locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "GCP project id these resources are created in. No default on purpose — pass explicitly via -var, TF_VAR_project, or a .tfvars file (never commit a .tfvars with real values)."
  type        = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "europe-west6"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "sp_500_data" #change this to a dataset name you want
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type        = string
  default     = "sp_500_data_table"
}

variable "alert_notification_email" {
  description = "Email address notified when the pipeline Cloud Run Job fails"
  type        = string
}

variable "github_repo" {
  description = "GitHub owner/repo admitted to Workload Identity Federation. Must be one repository, not an org."
  type        = string
  default     = "falowo95/sp500_ETL_Pipeline"

  validation {
    condition     = can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repo))
    error_message = "github_repo must be a single owner/name. Org-wide values and wildcards are rejected."
  }
}

variable "artifact_keep_count" {
  description = "Newest Artifact Registry versions kept per image. Older versions are deleted after 30 days."
  type        = number
  default     = 10
}

variable "pipeline_task_timeout" {
  description = "Cloud Run Job task timeout ceiling. A cold full-history backfill is much longer than a normal incremental run; billing is for actual runtime only."
  type        = string
  default     = "7200s"
}

variable "pipeline_memory" {
  description = "Cloud Run Job memory limit. Airflow, pandas, and dbt over the S&P 500 universe need more than 512Mi."
  type        = string
  default     = "2Gi"
}

variable "pipeline_schedule" {
  description = "Cloud Scheduler cron, UTC, for the daily pipeline run (after the US cash close)."
  type        = string
  default     = "0 22 * * *"
}

variable "bootstrap_job_image" {
  description = "Placeholder image used only on first create of the Cloud Run Job. CI replaces it; Terraform ignores later image changes. Do not execute the job until CI has published the real image."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}

variable "bootstrap_service_image" {
  description = "Placeholder image used only on first create of the dashboard Cloud Run Service. CI replaces it; Terraform ignores later image changes."
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
}
