from pyspark.sql import SparkSession
import os

def get_spark(app_name="podcast-recs"):
    delta_pkg = "io.delta:delta-spark_2.12:3.2.0"                 # match your Spark 3.4/3.5 line
    mongo_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    return (SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", f"{delta_pkg},{mongo_pkg}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.mongodb.write.connection.uri", mongo_uri)  # can be overridden per-write
            .config("spark.sql.shuffle.partitions","4")
            .getOrCreate())
