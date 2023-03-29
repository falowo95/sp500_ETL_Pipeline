# First-time build can take upto 10 mins.
FROM apache/airflow:2.2.3
ENV AIRFLOW_HOME=/opt/airflow
USER root
RUN apt-get update -qq \
    && apt-get install vim -qqq \
    && apt-get install wget -qqq
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin/:${PATH}"
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]
## GCP SDK files and Variables
ARG CLOUD_SDK_VERSION=369.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
    --bash-completion=false \
    --path-update=false \
    --usage-reporting=false \
    --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version
## Finish GCP SDK files and variables
## Spark files and Variables
ARG SPARK_VERSION=3.2.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin/:${PATH}"



RUN DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/spark.tgz" \
    && mkdir -p "${SPARK_HOME}" \
    && tar xzf "${TMP_DIR}/spark.tgz" -C "${SPARK_HOME}" --strip-components=1 \
    && gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar "${SPARK_HOME}"/jars/gcs-connector-hadoop2-2.1.1.jar \
    && rm -rf "${TMP_DIR}"

## Finish Spark files and Variables
## Start Python packages
COPY requirements.txt .
ENV PYTHONPATH="/usr/local/bin/python"
# /home/airflow/.local/lib/python3.7/site-packages
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"
RUN /usr/local/bin/python -m pip install --upgrade pip
USER airflow

RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install --user pyspark==3.2.1
# # Reference: https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html#constraints-files
# ARG AIRFLOW_VERSION=2.2.3
# ARG PYTHON_VERSION=3.7
# ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# RUN pip install "apache-airflow[postgres,google,ssh,pyspark]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
## Finish Python packages
WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID
