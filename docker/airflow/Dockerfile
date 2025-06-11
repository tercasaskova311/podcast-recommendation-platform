FROM apache/airflow:2.9.0

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

USER airflow

# Install the Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt


