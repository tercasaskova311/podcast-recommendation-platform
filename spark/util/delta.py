from delta.tables import DeltaTable

# def write_to_delta(df, path):
#     df.write.format("delta").mode("overwrite").save(path)

# def write_stream_to_delta(df, path):
#     df.writeStream \
#         .format("delta") \
#         .outputMode("append") \
#         .option("checkpointLocation", path + "/_checkpoint") \
#         .start(path) \
#         .awaitTermination()

# def read_delta(path):
#     spark = get_spark()
#     return spark.read.format("delta").load(path)

def get_unprocessed_episodes(spark, kafka_df, delta_path):
    if DeltaTable.isDeltaTable(spark, delta_path):
        existing = spark.read.format("delta").load(delta_path).select("episode_id")
        return kafka_df.join(existing, on="episode_id", how="left_anti")
    return kafka_df

def upsert_transcripts(spark, df, delta_path):
    if DeltaTable.isDeltaTable(spark, delta_path):
        DeltaTable.forPath(spark, delta_path).alias("target").merge(
            df.alias("source"),
            "target.episode_id = source.episode_id"
        ).whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)
