FROM apache/airflow:3.10-python3.11

USER root
RUN apt-get update && apt-get install -y gcc build-essential

COPY requirements_common.txt /app/requirements_common.txt
COPY requirements_airflow.txt /app/requirements_airflow.txt

USER airflow

RUN pip install --constraint \
  "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.10.txt" \
  -r /app/requirements_common.txt || true

RUN pip install \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.10.txt" \
  -r /app/requirements_airflow.txt

