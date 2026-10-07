# --- Portfolio dashboard (Cloud Run Service, public, scale-to-zero) ---
resource "google_service_account" "dashboard" {
  account_id   = "sp500-dashboard"
  display_name = "SP500 dashboard Cloud Run Service runtime identity"
}

# Read-only, scoped to the one dataset holding the marts + pipeline_runs.
resource "google_bigquery_dataset_iam_member" "dashboard_dataset_viewer" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.dashboard.email}"
}

# ACCEPTED EXCEPTION (project-level): same as pipeline_runner_job_user —
# running a query job can't be scoped below project level; it grants no
# data access by itself.
resource "google_project_iam_member" "dashboard_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_cloud_run_v2_service" "sp500_dashboard" {
  name     = "sp500-dashboard"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.dashboard.email
    scaling {
      min_instance_count = 0 # scale to zero: no idle cost
      max_instance_count = 2 # caps spend (and BigQuery query volume) under abuse
    }
    containers {
      # Placeholder for first creation only; CI owns the real image tag.
      image = var.bootstrap_service_image
      env {
        name  = "GCP_PROJECT_ID"
        value = var.project
      }
      env {
        name  = "GCP_BQ_DATASET"
        value = var.BQ_DATASET
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image, # deployed by CI (deploy.yml), not Terraform
    ]
  }
}

# Intentionally public: this is the portfolio's live demo.
resource "google_cloud_run_v2_service_iam_member" "dashboard_public" {
  name     = google_cloud_run_v2_service.sp500_dashboard.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}
