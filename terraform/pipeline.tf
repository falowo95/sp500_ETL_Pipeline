# Orchestration: a Cloud Run JOB (not a long-lived webserver/scheduler) run
# once daily by Cloud Scheduler. See airflow/entrypoint.sh — each execution
# runs the whole Airflow DAG once via `airflow dags test` against an
# ephemeral in-container SQLite metadata DB, then exits. No Cloud Composer,
# no always-on Celery/Redis/Postgres: every compute resource here bills
# only for actual execution seconds.

locals {
  image_registry = "${var.region}-docker.pkg.dev/${var.project}/${google_artifact_registry_repository.pipeline_images.repository_id}"
}

# --- Artifact Registry: one repo for both the pipeline and dashboard images ---
resource "google_artifact_registry_repository" "pipeline_images" {
  repository_id = "sp500-pipeline"
  location      = var.region
  format        = "DOCKER"
  description   = "Container images for the sp500 pipeline runner and dashboard"

  # Every merge to main pushes a new SHA-tagged ~2GB pipeline image. Without
  # cleanup, storage grows (and bills) forever. KEEP wins over DELETE, so the
  # newest versions per image always survive regardless of age — rollbacks
  # to recent commits stay possible.
  cleanup_policy_dry_run = false
  cleanup_policies {
    id     = "keep-most-recent"
    action = "KEEP"
    most_recent_versions {
      keep_count = var.artifact_keep_count
    }
  }
  cleanup_policies {
    id     = "delete-older-than-30d"
    action = "DELETE"
    condition {
      tag_state  = "ANY"
      older_than = "2592000s" # 30 days
    }
  }
}

# --- Pipeline runner identity: scoped grants only ---
resource "google_service_account" "pipeline_runner" {
  account_id   = "sp500-pipeline-runner"
  display_name = "SP500 pipeline Cloud Run Job runtime identity"
}

resource "google_secret_manager_secret" "api_tiingo" {
  secret_id = "api-tiingo"
  replication {
    auto {}
  }
  # The secret VALUE is never set here — add it out-of-band, after apply:
  #   printf '%s' 'YOUR_KEY' | gcloud secrets versions add api-tiingo \
  #     --project=<project> --data-file=-
  # Keeping it out of Terraform means it never touches state or plan output.
}

resource "google_secret_manager_secret_iam_member" "pipeline_runner_secret_access" {
  secret_id = google_secret_manager_secret.api_tiingo.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_runner_bucket_access" {
  bucket = google_storage_bucket.data-lake-bucket.name
  # Object CRUD only. Uniform bucket access is on, so object IAM/ACLs are unused.
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "pipeline_runner_dataset_access" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# ACCEPTED EXCEPTION (project-level): BigQuery job execution (running any
# load/query job) cannot be scoped below project level. jobUser grants only
# bigquery.jobs.create — it confers no data access by itself; data access is
# still limited to the dataset-scoped grant above.
resource "google_project_iam_member" "pipeline_runner_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

# Write-only. Stdout is collected by the Cloud Run service agent; this covers
# client-library log writes without granting log read or project editor.
resource "google_project_iam_member" "pipeline_runner_log_writer" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.pipeline_runner.email}"
}

resource "google_cloud_run_v2_job" "sp500_pipeline" {
  name     = "sp500-pipeline"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_runner.email
      # Generous on purpose: a cold start (or a newly added ticker) forces a
      # full-history backfill over ~500 sequential Tiingo calls, then dbt.
      # Billing is per actual second, so a long ceiling costs nothing on
      # normal (short, incremental) runs.
      timeout = var.pipeline_task_timeout
      # Whole-DAG retry is safe only because loads are MERGE-idempotent;
      # capped at 1 so a persistent failure doesn't burn compute repeatedly.
      max_retries = 1

      containers {
        # Placeholder used only when the job is first created (the real
        # image doesn't exist until CI pushes one). After that, CI owns the
        # image tag (deploy.yml), so Terraform ignores it — see lifecycle.
        image = var.bootstrap_job_image
        resources {
          limits = {
            cpu = "1"
            # Airflow (scheduler-less `dags test` + SQLite) plus a dbt
            # subprocess and pandas over ~500 tickers comfortably exceeds
            # 512Mi; OOM-kills would surface only as opaque task failures.
            memory = var.pipeline_memory
          }
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = var.project
        }
        env {
          name  = "GCP_GCS_BUCKET"
          value = google_storage_bucket.data-lake-bucket.name
        }
        env {
          name  = "GCP_BQ_DATASET"
          value = var.BQ_DATASET
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image, # deployed by CI (deploy.yml), not Terraform
    ]
  }
}

# --- Scheduler: separate identity for "who may trigger" vs "what the job can do" ---
resource "google_service_account" "scheduler_invoker" {
  account_id   = "sp500-scheduler-invoker"
  display_name = "Invokes the sp500 pipeline Cloud Run Job on a schedule"
}

# roles/run.invoker contains run.jobs.run (verified via
# `gcloud iam roles describe roles/run.invoker`), bound on this one job only.
resource "google_cloud_run_v2_job_iam_member" "scheduler_can_invoke" {
  name     = google_cloud_run_v2_job.sp500_pipeline.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_invoker.email}"
}

resource "google_cloud_scheduler_job" "sp500_pipeline_daily_trigger" {
  name      = "sp500-pipeline-daily-trigger"
  region    = var.region
  schedule  = var.pipeline_schedule
  time_zone = "UTC"
  # :run returns a long-running operation immediately; this only bounds the
  # API call itself, not the job execution.
  attempt_deadline = "60s"

  http_target {
    http_method = "POST"
    # Cloud Run Admin API v2 jobs.run — no Cloud Function middleman. Google
    # APIs (*.googleapis.com) require an OAuth access token; OIDC tokens are
    # only for calling your own Cloud Run services/functions.
    uri = "https://run.googleapis.com/v2/${google_cloud_run_v2_job.sp500_pipeline.id}:run"
    oauth_token {
      service_account_email = google_service_account.scheduler_invoker.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_can_invoke]
}
