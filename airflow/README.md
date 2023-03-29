# Airflow DAGs

Airflow will be runned in docker-compose network. 

- `Dockerfile`, `docker-compose.yaml` and `requirements.txt` have everything and project spec to run Airflow in Docker containers. 

- `.env` text file with GCP and Airflow creds. Should be updated with user's GCP credentials before building images for Docker.

- **dag** dir contains code for DAGs and needed scripts: 
    - **sp_500_data_processing_dag.py**. contains dag definition for stock data processing with dag_id "SP_500_DATA_PIPELINE_v1".
    - **helper_functions.py**. contains helper functions for stock data procesing operations. 
    - **stock_data_transform.py**. Contains spark transformation function.


#### How to run
1. Copy `airflow` dir from repo.
2. Make dirs for logs and plugins in `/airflow`:
```
mkdir -p ./logs ./plugins ./creds
cd creds
create file s3 and follow below 

define aws credentials inside s3 file 
[airflow-xcoms]
aws_access_key_id = 
aws_secret_access_key = 
```
3. Edit `.env` file with your credits: 
- `GCP_PROJECT_ID`
- `GCP_GCS_BUCKET`

4. Build images for Docker.
```
make build 
```
5. Run Docker containers.
```
make up
```
6. terminate containers
```
make down
```
7. Now Airflow running on 8080 port, so can forward it and open in browser at localhost:8080.
