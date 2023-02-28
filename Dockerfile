FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY dags/first_dag.py /app/first_dag.py
COPY dags/etl_operation_functions.py /app/etl_operation_functions.py

ENTRYPOINT [ "python", "first_day.py" ]