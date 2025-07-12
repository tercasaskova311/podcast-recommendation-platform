
def read_episode_metadata(spark, KAFKA_URL, topic):
    return spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_URL) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(key AS STRING) AS episode_id", "CAST(value AS STRING) AS json_str")