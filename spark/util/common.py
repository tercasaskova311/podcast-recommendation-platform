from pyspark.sql import SparkSession

def get_spark():
     return SparkSession.builder \
         .appName("SparkJob") \
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
         .getOrCreate()