# Project about 
This project is aimed at processing sp500 stock data.
The transformation steps taken enrich the dataset to facilitate the use of financial indicators to support business decision making by utilizing the dashboard developed in data studio.

## Dataset
[The stock data used is gotten from the tiingo api](https://api.tiingo.com/)

## Technologies
- **Google cloud platform** (GCP):
  - VM Instance to run project on it.
  - Cloud Storage to store raw and processed data.
  - BigQuery as data source for dashboard.
- **Terraform** to create cloud infrastructure.
- **Airflow** to run data pipelines as DAGs.
- **PySpark** to transform raw data.
- **Google data studio** to visualize data.

# Results 
## Cloud infrastructure
Except the VM Instance, all project infra setups with terraform: 
- Data Lake for all of the project data.
- BigQuery for transformed data tablels as source for dashboard.

## Data pipelines
The dataset data download, process and upload to cloud storage via Airflow DAGs:
1. **sp 500 data processing DAG** 
  - The dag handles the extraction of sp 500 stock daa from the tiingo api.
  - The upload to gcs task uploads the extracted data to GCS.
  - The transform data task uses stored data in GCS and transforms the data using pyspark.
  - The ingest data into bigquery task uploads the transformed data to BigQuery.


## Prereqs
- Anaconda
- Docker + Docker-compose
- GCP project
- Terraform

## Setup & Deploy
1. Create cloud infrasctructure via Terraform. Look at instructions at [terraform dir](https://github.com/falowo95/sp500_ETL_Pipeline/tree/main/terraform).
2. Run Airflow in docker and trigger DAGs. Look at instructions at [airflow dir](https://github.com/falowo95/sp500_ETL_Pipeline/tree/main/airflow).
3. Connect Google Data Studio dashboard to project BigQuery as a source.
