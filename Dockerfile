FROM python:3.12-slim

WORKDIR /app

# gcc + libc6-dev kept for any source-built deps (tgcrypto fallback).
# TDLib itself ships as a prebuilt binary via pytdbot[tdjson] — no compile needed.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

EXPOSE 8080

CMD ["python", "main.py"]
