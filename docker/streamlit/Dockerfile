# Dockerfile
FROM python:3.11
WORKDIR /app

# system deps (optional but handy for pandas/pyarrow)
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8504
