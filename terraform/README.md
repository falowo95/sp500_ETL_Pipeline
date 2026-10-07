# Creating cloud infra via Terraform

Cloud infra which consists of a data lake bucket and a BigQuery dataset:

- The data lake stores landing files.
- The BigQuery dataset stores tables the pipeline and dbt write.

The rest of this directory also defines the Cloud Run Job, Cloud Scheduler, the dashboard service, failure alerts, and GitHub Workload Identity Federation. See the file list at the top of `main.tf`.

## How to run

Authenticate with Application Default Credentials (a user login or Workload Identity Federation). Do not use a service-account JSON key.

```bash
gcloud auth application-default login
cd terraform
terraform init
terraform plan \
  -var="project=YOUR_GCP_PROJECT" \
  -var="alert_notification_email=you@example.com"
terraform apply \
  -var="project=YOUR_GCP_PROJECT" \
  -var="alert_notification_email=you@example.com"
```

`project` and `alert_notification_email` have no defaults. `github_repo` defaults to `falowo95/sp500_ETL_Pipeline` and is the only repository admitted to Workload Identity Federation.

The first apply has to be done by an identity that can create IAM and the identity pools. After it succeeds, copy `terraform output` into GitHub repository variables:

- `WIF_PROVIDER` and `WIF_SERVICE_ACCOUNT` (deploy, main branch only)
- `WIF_CI_PROVIDER` and `WIF_CI_SERVICE_ACCOUNT` (pull-request plan and ephemeral dbt)

No JSON key is created. The Tiingo secret value is added out of band, not by Terraform.

The state bucket is created once from `bootstrap/` before `main.tf`'s GCS backend will init. See the header comment in `bootstrap/main.tf`.
