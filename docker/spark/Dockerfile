FROM bitnami/spark:latest

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /


RUN pip install --no-cache-dir -r /requirements.txt