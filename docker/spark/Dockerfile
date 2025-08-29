FROM bitnami/spark:3.5.6

USER root

# Copy the requirements.txt into the image
COPY ./requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt
