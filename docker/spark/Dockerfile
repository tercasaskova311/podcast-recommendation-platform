FROM bitnami/spark:3.5.6

USER root

WORKDIR /app

ENV PYTHONPATH="/opt/spark_jobs:${PYTHONPATH}"

# Installa ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

COPY ./docker/spark/requirements.txt /app

RUN pip3 install --no-cache-dir -r /app/requirements.txt
