FROM apache/airflow:2.5.1




ENV TIINGO_API_KEY = 
ENV GCP_SERVICE_ACCOUNT_FILE  = "/opt/airflow/creds/dataengineering-378316-2bcbcf067f34.json"


COPY ./creds/dataengineering-378316-2bcbcf067f34.json /opt/airflow/creds/dataengineering-378316-2bcbcf067f34.json
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir -r /opt/requirements.txt