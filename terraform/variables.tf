locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "dataengineering-378316" //put yor own project id
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "sp_500_data" #change this to a dataset name you want
}

variable "TABLE_NAME" {
    description = "BigQuery Table"
    type = string 
    default = "sp_500_data_table"
}

variable "credentials_file" {
    description = "Path to your credentials file"
    default = "/Users/falowogbolahan/.google/credentials/google_credentials.json"
}
