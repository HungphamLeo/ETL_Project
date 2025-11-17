FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc build-essential

COPY requirements_common.txt /requirements_common.txt
COPY requirements_prefect.txt /requirements_prefect.txt

RUN pip install --user -r /requirements_common.txt || true
RUN pip install -r /requirements_prefect.txt

COPY . /app
WORKDIR /app

CMD ["prefect", "worker", "start", "-p", "default"]
