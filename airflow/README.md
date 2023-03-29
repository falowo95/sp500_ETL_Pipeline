# Airflow DAGs

To run Airflow, you'll need to set up a Docker Compose network. The necessary files and specifications are included in this repository:

- `Dockerfile`, `docker-compose.yaml` and `requirements.txt` files contain everything you need to run Airflow in Docker containers.

- `.env` You'll need to update the `.env`file with your GCP and Airflow credentials before building Docker images.

- **dag**  directory contains the code for the DAGs and necessary scripts:
    - **sp_500_data_processing_dag.py**.  defines the DAG for processing stock data with a `dag_id` "SP_500_DATA_PIPELINE_v1".
    - **helper_functions.py** contains helper functions for stock data processing operations.
    - **stock_data_transform.py** contains the Spark transformation function.

#### How to run
1. Copy the`airflow` directory from this repository to your local machine.
2. Create directories for logs, plugins, and credentials in the  `/airflow` directory::
```
mkdir -p ./logs ./plugins ./creds
cd creds
In the creds directory, create a file named s3 and add your AWS credentials in the following format:
```
[airflow-xcoms]
aws_access_key_id = 
aws_secret_access_key = 
```
3. Edit `.env` file with your credits: 
- `GCP_PROJECT_ID`
- `GCP_GCS_BUCKET`

4. Build Docker images:

```
make build 
```
5. Start Docker containers:
```
make up
```
6. To stop running containers:
```
make down
```
7. Once Airflow is running, you can access it on port 8080. You can forward this port and open it in your browser at localhost:8080.
