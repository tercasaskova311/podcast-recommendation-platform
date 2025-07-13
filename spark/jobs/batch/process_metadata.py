from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PodcastMetadataProcessing").getOrCreate()

#read mongo db???
episodes_raw = spark.read()

# Select only the fields you want and rename for clarity
episodes_selected = episodes_raw.select(
    col("id"),
    col("episode_title").alias("title"),
    col("audio_url"),         
    col("podcast").alias("podcast_name"),
    col("description") )        

from pyspark.sql.functions import coalesce, lit

episodes_clean = episodes_selected.withColumn(
    "description", coalesce(col("description"), lit(""))
)

# remove duplicates by id (important to avoid duplicate episodes)
episodes_clean = episodes_clean.dropDuplicates(["id"])

# Save cleaned episodes to parquet for data lake
episodes_clean.write.mode("overwrite").parquet("data_lake/episodes/")

spark.stop()
