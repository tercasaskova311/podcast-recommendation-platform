from pyspark.sql import SparkSession
from util.common import get_spark

def run_pipeline():
    spark = get_spark()

    #filtered_df = get_unprocessed_episodes(spark, kafka_df, DELTA_PATH_EPISODES)
    #transcript_df = process_batch(filtered_df)

    #if not transcript_df.rdd.isEmpty():
    #    upsert_transcripts(spark, transcript_df, DELTA_PATH_EPISODES)

    spark.stop()    

#GET ALL THE TRANSCRIPTS FROM DELTA WITH analyzed=false
#DOING SOME ANALYSTICS ON THOSE
#IF analyzed is still = false update tot true and SAVE AGGREGATED DATA ON MONGO AND DELTA