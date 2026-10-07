terraform {
  required_version = ">= 1.3"
  # State lives in the bucket created once by terraform/bootstrap/ (see that
  # module's header comment) rather than on disk, so it survives a fresh
  # checkout and isn't at risk of being accidentally committed again.
  # Backend blocks can't reference variables; to point this config at a
  # different project, override at init time:
  #   terraform init -backend-config="bucket=<project>-tf-state"
  backend "gcs" {
    bucket = "dataengineering-378316-tf-state"
    prefix = "sp500-etl/state"
  }
  required_providers {
    google = {
      source = "hashicorp/google"
      # The committed .terraform.lock.hcl previously pinned 4.53.1 (from
      # when this config only had a bucket + dataset). That version
      # predates several resources/attributes added in this revamp — e.g.
      # google_secret_manager_secret's `replication { auto {} }` block
      # shape and Artifact Registry cleanup policies — so a real floor is
      # required rather than letting the lock file silently carry forward a
      # too-old pin.
      version = ">= 5.25"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  # No credentials block: resolves via Application Default Credentials —
  # a logged-in gcloud identity locally, or Workload Identity Federation
  # in CI (see .github/workflows/). Never a service-account key file; this
  # project has a documented past incident of exactly that kind of file
  # getting committed.
}

# Layout (one concern per file):
#   main.tf        backend, provider, data lake bucket, BigQuery dataset
#   pipeline.tf    Artifact Registry, pipeline Cloud Run Job + its IAM, secret, Scheduler
#   monitoring.tf  failure alert policy + email channel
#   dashboard.tf   public scale-to-zero dashboard Cloud Run Service + its IAM
#   cicd.tf        Workload Identity Federation + GitHub Actions identities
#   outputs.tf     WIF provider names and service-account emails for GitHub variables

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  # Pinned to this bucket's actual existing location (confirmed live via
  # `gcloud storage buckets describe`), NOT var.region. This bucket
  # pre-dates this revamp and already holds real pipeline data — GCS
  # bucket location can't be changed without deleting and recreating the
  # bucket, which `terraform plan` confirmed it would do if this were
  # var.region (default "europe-west6") instead. var.region is for the
  # brand-new compute resources (Cloud Run, Scheduler, Artifact Registry),
  # which have no such constraint.
  location = "EU"

  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  # Raw landing data is never meant to be public; this blocks any future
  # allUsers/allAuthenticatedUsers grant at the bucket level.
  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  # This bucket already holds pipeline data. Terraform must not empty it.
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  # Pinned to "US" — this dataset's actual, confirmed-live location, and
  # it already holds 600k+ real rows across 4 tables. BigQuery dataset
  # location is immutable short of delete+recreate; `terraform plan`
  # confirmed it would force exactly that replacement (destroying all
  # existing data) if this were var.region ("europe-west6") instead.
  location = "US"

  lifecycle {
    prevent_destroy = true
  }
}
