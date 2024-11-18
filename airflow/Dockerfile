FROM apache/airflow:2.10.3
ENV AIRFLOW_HOME=/opt/airflow

USER root

# Install basic utilities and Java
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        vim \
        default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-default
ENV PATH="${JAVA_HOME}/bin/:${PATH}"

# Install Google Cloud SDK
ARG CLOUD_SDK_VERSION=458.0.0
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

# Install Spark
ARG SPARK_VERSION=3.2.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/spark.tgz" \
    && mkdir -p "${SPARK_HOME}" \
    && tar xzf "${TMP_DIR}/spark.tgz" -C "${SPARK_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}"

# Download GCS connector separately (without using gsutil)
RUN curl -fL "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-2.1.1.jar" \
    --output "${SPARK_HOME}/jars/gcs-connector-hadoop2-2.1.1.jar"

# Set up Python environment
ENV PYTHONPATH="/usr/local/bin/python"
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.3-src.zip:$PYTHONPATH"

# Copy requirements and install Python packages
COPY requirements.txt .
RUN /usr/local/bin/python -m pip install --upgrade pip

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME
