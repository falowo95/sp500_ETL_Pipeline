# GitHub Actions identities. There is no google_service_account_key:
# workflows exchange a GitHub OIDC token via Workload Identity Federation.
#
# Two pools so a pull request cannot satisfy the deploy provider.
#   github_deploy  this repository AND refs/heads/main
#   github_ci      this repository, any ref (plan + ephemeral dbt datasets)
# Neither pool admits every repository in the org.

locals {
  github_repo_condition = "assertion.repository == \"${var.github_repo}\" && assertion.workflow_ref.startsWith(\"${var.github_repo}/\")"
  github_main_condition = "${local.github_repo_condition} && assertion.ref == \"refs/heads/main\""

  # Broad enough to apply this stack and push images. Not roles/owner or
  # roles/editor, and not roles/iam.serviceAccountKeyAdmin (no JSON keys).
  # secretmanager.admin can read secret payloads; the main-only pool is
  # what keeps pull requests away from that.
  github_deploy_roles = toset([
    "roles/artifactregistry.admin",
    "roles/bigquery.admin",
    "roles/cloudscheduler.admin",
    "roles/firebasehosting.admin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.workloadIdentityPoolAdmin",
    "roles/monitoring.admin",
    "roles/resourcemanager.projectIamAdmin",
    "roles/run.admin",
    "roles/secretmanager.admin",
    "roles/serviceusage.serviceUsageConsumer",
    "roles/storage.admin",
  ])
}

resource "google_service_account" "github_deploy" {
  account_id   = "sp500-github-deploy"
  display_name = "GitHub Actions deploy (main branch, this repository only)"
}

resource "google_service_account" "github_ci" {
  account_id   = "sp500-github-ci"
  display_name = "GitHub Actions CI (this repository, no production data-plane access)"
}

resource "google_iam_workload_identity_pool" "github_deploy" {
  workload_identity_pool_id = "sp500-github-deploy"
  display_name              = "GitHub deploy"
  description               = "OIDC from ${var.github_repo} on refs/heads/main only."
}

resource "google_iam_workload_identity_pool_provider" "github_deploy" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_deploy.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub main"
  attribute_condition                = local.github_main_condition
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_iam_workload_identity_pool" "github_ci" {
  workload_identity_pool_id = "sp500-github-ci"
  display_name              = "GitHub CI"
  description               = "OIDC from ${var.github_repo} only, including pull requests."
}

resource "google_iam_workload_identity_pool_provider" "github_ci" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_ci.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub repository"
  attribute_condition                = local.github_repo_condition
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "github_deploy_wif" {
  service_account_id = google_service_account.github_deploy.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_deploy.name}/attribute.repository/${var.github_repo}"
}

resource "google_service_account_iam_member" "github_ci_wif" {
  service_account_id = google_service_account.github_ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_ci.name}/attribute.repository/${var.github_repo}"
}

resource "google_project_iam_member" "github_deploy" {
  for_each = local.github_deploy_roles
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.github_deploy.email}"
}

# actAs on the runtime identities Cloud Run and Scheduler run as.
# Project-wide serviceAccountUser would allow impersonating every account.
resource "google_service_account_iam_member" "github_deploy_act_as" {
  for_each = {
    pipeline  = google_service_account.pipeline_runner.name
    dashboard = google_service_account.dashboard.name
    scheduler = google_service_account.scheduler_invoker.name
  }
  service_account_id = each.value
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.github_deploy.email}"
}

# Not roles/viewer: the dataset's legacy projectReaders group would turn
# that into read access on production tables. These roles refresh Terraform
# and expose metadata only. bigquery.user can create and own the ephemeral
# dbt CI dataset; it does not grant data access to sp_500_data.
resource "google_project_iam_member" "github_ci" {
  for_each = toset([
    "roles/artifactregistry.reader",
    "roles/bigquery.metadataViewer",
    "roles/bigquery.user",
    "roles/browser",
    "roles/cloudscheduler.viewer",
    "roles/iam.securityReviewer",
    "roles/iam.serviceAccountViewer",
    "roles/iam.workloadIdentityPoolViewer",
    "roles/monitoring.viewer",
    "roles/run.viewer",
    "roles/secretmanager.viewer",
  ])
  project = var.project
  role    = each.value
  member  = "serviceAccount:${google_service_account.github_ci.email}"
}

# Bucket metadata for terraform plan. legacyBucketReader does not include
# storage.objects.get, so landing-file contents stay unreadable.
resource "google_storage_bucket_iam_member" "github_ci_lake_metadata" {
  bucket = google_storage_bucket.data-lake-bucket.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.github_ci.email}"
}

# State bucket is created by terraform/bootstrap and is not in this state.
# objectViewer is enough because CI plans with -lock=false.
resource "google_storage_bucket_iam_member" "github_ci_state_read" {
  bucket = "${var.project}-tf-state"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.github_ci.email}"
}
