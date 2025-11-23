FROM apache/airflow:2.9.1-python3.10

USER root
RUN apt-get update && apt-get install -y gcc build-essential

COPY requirements_common.txt /app/requirements_common.txt
COPY requirements_airflow.txt /app/requirements_airflow.txt

USER airflow

RUN pip install --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt" \
    -r /app/requirements_common.txt || true

RUN pip install --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt" \
    -r /app/requirements_airflow.txt
