FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc build-essential

COPY requirements_common.txt /app/requirements_common.txt
COPY requirements_prefect.txt /app/requirements_prefect.txt

RUN pip install -r /app/requirements_common.txt
RUN pip install -r /app/requirements_prefect.txt

COPY . /app
WORKDIR /app

CMD ["prefect", "worker", "start", "-p", "default"]
