FROM apache/airflow:2.9.0

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

# Installa Java and all packages
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl ca-certificates unzip git wget ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Installa Apache Spark 3.5.6 (ultima versione stabile)
RUN curl -fsSL https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    ln -s /opt/spark-3.5.6-bin-hadoop3 /opt/spark

# Put models under /models
RUN mkdir -p /models && \
    cd /models && \
    wget -q https://alphacephei.com/vosk/models/vosk-model-small-en-us-0.15.zip && \
    apt-get update && apt-get install -y unzip && \
    unzip -q vosk-model-small-en-us-0.15.zip && \
    mv vosk-model-small-en-us-0.15 /models/vosk-small-en && \
    rm -f vosk-model-small-en-us-0.15.zip

# Variabili ambiente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python
# Cache for models (sentence-transformers)
ENV TRANSFORMERS_CACHE=/models/hf
ENV HF_HOME=/models/hf

USER airflow

# Install the Python dependencies
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch==2.2.2"
RUN pip install --no-cache-dir -r /requirements.txt


