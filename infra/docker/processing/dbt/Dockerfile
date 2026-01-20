FROM python:3.11-slim

# ---- system dependencies
RUN apt-get update && apt-get install -y \
    gcc build-essential curl libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---- requirements (cache friendly)
COPY requirements_common.txt .
COPY requirements_dbt_postgres.txt .

RUN pip install --no-cache-dir -r requirements_common.txt \
    && pip install --no-cache-dir -r requirements_dbt_postgres.txt

# ---- dbt project
COPY analytics/dbt ./dbt
COPY shared ./shared

# ---- environment
ENV PYTHONPATH=/app:/app/shared
ENV DBT_PROFILES_DIR=/app/dbt/profiles
ENV DBT_PROJECT_DIR=/app/dbt

# ---- default command
ENTRYPOINT ["dbt"]
CMD ["--version"]
