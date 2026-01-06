FROM apache/airflow:3.1.0-python3.11

USER root
RUN apt-get update && apt-get install -y gcc build-essential

USER airflow

COPY requirements_airflow.txt /app/requirements_airflow.txt
COPY requirements_common.txt /app/requirements_common.txt

RUN pip install --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.11.txt" \
  -r /app/requirements_airflow.txt
