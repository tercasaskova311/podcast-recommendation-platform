from pyspark.sql import SparkSession
from config.settings import SPARK_URL

def get_spark():
     return SparkSession.builder \
        .appName("SparkJob") \
        .master(SPARK_URL) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()