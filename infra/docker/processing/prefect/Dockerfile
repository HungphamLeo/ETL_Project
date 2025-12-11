FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc build-essential curl

WORKDIR /app

COPY requirements_common.txt requirements_common.txt
COPY requirements_prefect.txt requirements_prefect.txt

RUN pip install --no-cache-dir -r requirements_common.txt
RUN pip install --no-cache-dir -r requirements_prefect.txt

COPY . /app

# Copy entrypoint
COPY infra/docker/prefect/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PREFECT_API_URL="http://prefect-server:4200/api"
ENV PYTHONPATH="/app:/app/shared:/app/platforms:/app/scripts"

ENTRYPOINT ["/entrypoint.sh"]
