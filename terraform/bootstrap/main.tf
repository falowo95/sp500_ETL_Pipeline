# One-time bootstrap: creates the GCS bucket that holds the real Terraform
# state used by everything in ../main.tf. This has to live outside that
# state itself (you can't store a bucket's state in a bucket that doesn't
# exist yet), so it keeps its own local backend and is applied once, by
# hand, before the main config's backend is pointed at the bucket below.
#
#   cd terraform/bootstrap
#   terraform init
#   terraform apply -var="project=dataengineering-378316"

terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

variable "project" {
  description = "GCP project id to create the Terraform state bucket in"
  type        = string
}

variable "region" {
  description = "Region for the state bucket"
  type        = string
  default     = "europe-west6"
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "tf_state" {
  name                        = "${var.project}-tf-state"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }
}

output "state_bucket_name" {
  value = google_storage_bucket.tf_state.name
}
