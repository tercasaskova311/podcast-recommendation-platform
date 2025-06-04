from util.common import get_spark

def write_to_delta(df, path):
    df.write.format("delta").mode("overwrite").save(path)

def write_stream_to_delta(df, path):
    df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", path + "/_checkpoint") \
        .start(path) \
        .awaitTermination()

def read_delta(path):
    spark = get_spark()
    return spark.read.format("delta").load(path)

