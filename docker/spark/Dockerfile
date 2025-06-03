FROM bitnami/spark:latest

WORKDIR /app
#COPY ./spark /app - only for production, 
COPY ./docker/spark/requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt
