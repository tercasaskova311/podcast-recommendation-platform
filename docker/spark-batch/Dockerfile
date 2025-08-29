# ./docker/spark-runner/Dockerfile
FROM bitnami/spark:3.5.6

# become root for installing Python deps
USER root

# install python deps
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
 && rm -f /requirements.txt

# optional: set Spark configs for Delta support
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf
ENV PYTHONUNBUFFERED=1

# this env var makes spark-submit find Delta + Kafka + Mongo connector automatically
ENV SPARK_PACKAGES="\
io.delta:delta-spark_2.12:3.2.0,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

# working dir will match project mount
WORKDIR /opt/project

# keep tini as entrypoint (already present in bitnami image)
ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD [ "bash" ]
