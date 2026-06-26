FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/python-engine/src
ENV STOCK_CONFIG_DIR=/app/config

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY python-engine/requirements.txt /app/python-engine/requirements.txt
RUN pip install --no-cache-dir -r /app/python-engine/requirements.txt

COPY python-engine /app/python-engine
COPY config /app/config
COPY docker/postgres/init/001_schema.sql /app/docker/postgres/init/001_schema.sql

WORKDIR /app

CMD ["python", "-m", "stock_selector.cli", "health-check"]
