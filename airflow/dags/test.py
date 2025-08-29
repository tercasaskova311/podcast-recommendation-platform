# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.models.baseoperator import chain
# from airflow.providers.docker.operators.docker import DockerOperator

# # Common env for Spark jobs
# SPARK_PACKAGES = (
#     "io.delta:delta-spark_2.12:3.2.0,"
#     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
#     "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
# )

# COMMON_ENV = {
#     "KAFKA_URL": "kafka:9092",
#     "MONGO_URI": "mongodb://root:example@mongodb:27017",
#     "MONGO_DB": "podcasts",
#     "SPARK_PACKAGES": SPARK_PACKAGES,
#     "PYTHONUNBUFFERED": "1",
# }

# # Mount the project into the job container
# VOLUMES = [
#     # host path (from scheduler container) : target in job container : mode
#     "/opt/project:/opt/project:rw",
# ]

# # All spark-submit share these flags (local mode!)
# SPARK_BASE = (
#     "spark-submit "
#     "--master local[*] "
#     '--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension '
#     '--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog '
#     "--packages \"$SPARK_PACKAGES\" "
#     "--driver-memory 4g --conf spark.executor.memory=4g "
# )

# default_args = {"retries": 1}

# with DAG(
#     "spark_local_jobs",
#     default_args=default_args,
#     start_date=days_ago(1),
#     schedule_interval=None,  # trigger manually or set a cron
#     catchup=False,
#     tags=["spark", "local", "docker"],
# ) as dag:

#     # --- BATCH/JOB EXAMPLES ---
#     analyze_transcripts = DockerOperator(
#         task_id="analyze_transcripts",
#         image="spark-runner:local",
#         command=SPARK_BASE + " spark/pipelines/analyze_transcripts_pipeline.py",
#         network_mode="kafka-net",
#         auto_remove=True,
#         environment=COMMON_ENV,
#         volumes=VOLUMES,
#         working_dir="/opt/project",
#         do_xcom_push=False,
#     )

#     train_als = DockerOperator(
#         task_id="train_als",
#         image="spark-runner:local",
#         command=SPARK_BASE + " spark/pipelines/train_als_pipeline.py",
#         network_mode="kafka-net",
#         auto_remove=True,
#         environment={
#             **COMMON_ENV,
#             "ALS_RANK": "32",
#             "ALS_MAX_ITER": "10",
#             "ALS_REG": "0.08",
#             "ALS_ALPHA": "40.0",
#         },
#         volumes=VOLUMES,
#         working_dir="/opt/project",
#         do_xcom_push=False,
#     )

#     # Your three per-topic jobs (adapt script paths if needed)
#     transcripts_en = DockerOperator(
#         task_id="stream_transcripts_en_once",
#         image="spark-runner:local",
#         # For idempotent batches over a window; for infinite streams see below
#         command=SPARK_BASE + " spark/jobs/stream_transcripts_en.py --mode batch --since 24h",
#         network_mode="kafka-net",
#         auto_remove=True,
#         environment=COMMON_ENV,
#         volumes=VOLUMES,
#         working_dir="/opt/project",
#         do_xcom_push=False,
#     )

#     transcripts_foreign = DockerOperator(
#         task_id="stream_transcripts_foreign_once",
#         image="spark-runner:local",
#         command=SPARK_BASE + " spark/jobs/stream_transcripts_foreign.py --mode batch --since 24h",
#         network_mode="kafka-net",
#         auto_remove=True,
#         environment=COMMON_ENV,
#         volumes=VOLUMES,
#         working_dir="/opt/project",
#         do_xcom_push=False,
#     )

#     podcast_metadata = DockerOperator(
#         task_id="stream_podcast_metadata_once",
#         image="spark-runner:local",
#         command=SPARK_BASE + " spark/jobs/stream_podcast_metadata.py --mode batch --since 24h",
#         network_mode="kafka-net",
#         auto_remove=True,
#         environment=COMMON_ENV,
#         volumes=VOLUMES,
#         working_dir="/opt/project",
#         do_xcom_push=False,
#     )

#     # Example dependency: run transcripts first, then analyze, then train
#     chain(transcripts_en, transcripts_foreign, podcast_metadata, analyze_transcripts, train_als)
