FROM apache/airflow:3.1.0-python3.10

USER root
RUN apt-get update && apt-get install -y gcc build-essential

COPY requirements_common.txt /requirements_common.txt
COPY requirements_airflow.txt /requirements_airflow.txt

# chuyển về user "airflow"
USER airflow

RUN pip install --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.10.txt" \
    -r /requirements_common.txt || true

RUN pip install --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.10.txt" \
    -r /requirements_airflow.txt
