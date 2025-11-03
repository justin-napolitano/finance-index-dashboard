FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# System deps for TLS, curl path used by yfinance, and building wheels if needed
#RUN apt-get update && apt-get install -y --no-install-recommends \
#    build-essential ca-certificates curl \
#    && update-ca-certificates \
#    && rm -rf /var/lib/apt/lists/*

#RUN apt-get update && apt-get install -y --no-install-recommends \
#    build-essential ca-certificates curl libcurl4-openssl-dev libssl-dev && \
#    update-ca-certificates && \
#    rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl libcurl4-openssl-dev libssl-dev && \
    update-ca-certificates && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Bring in code (including app/etl/* and migrations if this image also runs them)
COPY app ./app

