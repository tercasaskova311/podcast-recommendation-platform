FROM apache/airflow:2.9.0

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

# Installa Java e curl
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Installa Apache Spark 3.5.1 (ultima versione stabile)
RUN curl -fsSL https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.6-bin-hadoop3 /opt/spark

# Variabili ambiente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

# Install the Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt


