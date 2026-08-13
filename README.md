# Airflow

Setup, architecture, and usage docs live in the [top-level README](../README.md).
This directory holds the Airflow project itself: DAGs, dbt project, Dockerfile,
Terraform, and tests.

## Screenshots

![System Architecture](docs/system-architecture.png)
![Pipeline Overview 1](docs/pipeline-overview-1.png)
![Pipeline Overview 2](docs/pipeline-overview-2.png)
![Pipeline Overview 3](docs/pipeline-overview-3.png)

## Quick commands

```bash
make build   # build the Airflow Docker images
make up      # start Airflow (http://localhost:8080)
make down    # stop Airflow
make tf-init # terraform init (terraform/)
make infra-up   # terraform apply
make infra-down # terraform destroy
```

