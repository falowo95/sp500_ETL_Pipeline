output "wif_provider" {
  description = "GitHub variable WIF_PROVIDER. Deploy pool: this repository on refs/heads/main only."
  value       = google_iam_workload_identity_pool_provider.github_deploy.name
}

output "wif_service_account" {
  description = "GitHub variable WIF_SERVICE_ACCOUNT. Deploy identity. No JSON key is created."
  value       = google_service_account.github_deploy.email
}

output "wif_ci_provider" {
  description = "GitHub variable WIF_CI_PROVIDER. CI pool: this repository, any ref."
  value       = google_iam_workload_identity_pool_provider.github_ci.name
}

output "wif_ci_service_account" {
  description = "GitHub variable WIF_CI_SERVICE_ACCOUNT. Plan and ephemeral BigQuery datasets. Cannot read the Tiingo secret."
  value       = google_service_account.github_ci.email
}
