FROM bitnami/spark:3.5.6

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

ENV PYTHONPATH="/opt/spark_jobs:${PYTHONPATH}"

# Installa ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r /requirements.txt