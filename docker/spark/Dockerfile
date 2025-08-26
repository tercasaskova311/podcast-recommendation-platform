FROM bitnami/spark:3.5.6

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

ENV PYTHONPATH="/opt/spark_jobs:${PYTHONPATH}"

# Installa ffmpeg
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r /requirements.txt

# Set working directory
WORKDIR /opt/spark/app

# Copy all scripts (streaming + batch)
COPY ./scripts/ /opt/spark/app/

# Entry point is spark-submit by default
ENTRYPOINT ["spark-submit"]

# Default to streaming pipeline, but overridable
CMD ["/opt/spark/app/streaming/streaming_user_events_pipeline.py"]