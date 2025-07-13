#FROM bitnami/spark:latest

#USER root

# Copy the requirements.txt into the image
#COPY ./requirements.txt /


#RUN pip install --no-cache-dir -r /requirements.txt

FROM --platform=linux/amd64 bitnami/spark:latest

USER root

WORKDIR /app

# Install Python + pip using Bitnami's helper
RUN install_packages python3 python3-pip

COPY ./docker/spark/requirements.txt /app

RUN pip3 install --no-cache-dir -r /app/requirements.txt
