# Creating cloud infra via Terraform

Cloud infra wich consists of data lake and BQ dataset: 
- Data Lake would be used for storing all data: raw and clean
- BQ dataset would store tables for reports

## How to run
1. Copy all files from repo and change **variables.tf** up to your GCP Project ID. Here **variables.tf** code snippet with comments at strokes to change: 

```
**variables.tf**

variable "project" {
  description = "Your GCP Project ID"
  default = "dataengineering-378316" # Here should be your GCP Project ID
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6" # Here should be your GCP Project Region
  type = string
}
```
2. Activate terraform service account on GCP. Use those commands:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/google_credentials.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```
3. Initialize terraform state, check changes to infra plan and apply them. Use commands at terradorm dir: 
```bash
terraform init
terraform plan
terraform apply
```
