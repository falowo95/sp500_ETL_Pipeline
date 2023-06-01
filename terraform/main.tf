terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials_file)
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}


# Compute Engine Instance
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_instance
resource "google_compute_instance" "compute_instance" {
  name         = "redundant-instance"  # Set the name of your instance
  machine_type = "e2-standard-4"  # Set the machine type for your instance
  zone         = "${var.region}-a"  # Set the zone for your instance

  boot_disk {
    initialize_params {
      image = "ubuntu-2004-focal-v20230213"  # Set the image for your instance
    }
  }

  network_interface {
    network = "default"  # Set the network for your instance
    access_config {
      // Ephemeral IP will be assigned automatically
    }
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash
    # Add your custom startup script commands here
    # Example: apt-get update && apt-get install -y apache2
    
  EOF
}
