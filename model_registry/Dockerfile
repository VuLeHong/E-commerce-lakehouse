FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip setuptools wheel

RUN python -m pip install --no-cache-dir --index-url https://pypi.org/simple \
    mlflow==2.19.0 \
    psycopg2-binary==2.9.10 \
    boto3==1.35.74