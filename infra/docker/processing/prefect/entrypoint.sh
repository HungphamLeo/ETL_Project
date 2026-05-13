#!/bin/bash
set -e

# Tăng số lần retry
MAX_RETRIES=30
RETRY_COUNT=0

echo "Waiting for Prefect API to be ready..."
until curl -f http://prefect-server:4200/api/health; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "ERROR: Prefect API not ready after $MAX_RETRIES attempts"
    exit 1
  fi
  echo "Attempt $RETRY_COUNT/$MAX_RETRIES - Waiting 5s..."
  sleep 5
done

echo "Prefect API is ready!"
sleep 2  # Thêm buffer time

echo "Checking work pool..."
if ! prefect work-pool ls | grep -q "default-agent-pool"; then
  echo "Creating work pool..."
  prefect work-pool create default-agent-pool --type process || {
    echo "ERROR: Failed to create work pool"
    exit 1
  }
fi

echo "Starting Prefect Worker..."
exec prefect worker start --pool default-agent-pool --type process