FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc build-essential

# Copy đúng file requirements (không được COPY toàn bộ repo!)
COPY requirements_common.txt /app/requirements_common.txt
COPY requirements_prefect.txt /app/requirements_prefect.txt

RUN pip install --no-cache-dir -r /app/requirements_common.txt
RUN pip install --no-cache-dir -r /app/requirements_prefect.txt

# Tạo thư mục mount sẵn (Docker không tự tạo)
RUN mkdir -p /app/flows /app/scripts /app/shared /app/platforms

WORKDIR /app

# ENTRYPOINT rỗng → để compose override bằng "command"
ENTRYPOINT ["prefect", "worker", "start"]
