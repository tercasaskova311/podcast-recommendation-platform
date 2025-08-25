import os
from pyspark.sql import SparkSession

DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
MONGO_PKG = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

def get_spark(app_name: str) -> SparkSession:
    # If we are inside spark-submit, SparkContext already exists.
    in_submit = os.environ.get("SPARK_SUBMIT_MODE", "0") in ("1", "true", "True")

    b = SparkSession.builder.appName(app_name)
    if not in_submit:
        # Local dev or PythonOperator mode
        b = (b.master("local[*]")
             .config("spark.jars.packages", f"{DELTA_PKG},{MONGO_PKG}"))

    b = (b
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC"))

    return b.getOrCreate()