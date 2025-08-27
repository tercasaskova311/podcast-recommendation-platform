FROM bitnami/spark:3.5.6

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt

# ENV PYSPARK_PYTHON=python3 \
#     PYTHONPATH="/opt/spark_jobs:${PYTHONPATH}"

# # app code
# WORKDIR /opt/spark/app


# # Entry point is spark-submit by default
# ENTRYPOINT ["spark-submit"]

# # Default to streaming pipeline, but overridable
# CMD ["/opt/spark/app/streaming/streaming_user_events_pipeline.py"]
