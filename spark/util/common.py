# common/get_spark.py
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

# Default connectors (override with SPARK_JARS_PACKAGES or SPARK_JARS)
DEFAULT_PKGS = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",   # Kafka source
    "io.delta:delta-spark_2.12:3.1.0",                    # Delta Lake
    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0" # MongoDB v10 connector
])

def _in_spark_submit() -> bool:
    """
    Detect if we're running under spark-submit (so the submit command
    should decide the master & many confs). We avoid reading global
    SPARK_MASTER/SPARK_URL to prevent accidental overrides.
    """
    s = (os.getenv("PYSPARK_SUBMIT_ARGS") or "") + " " + (os.getenv("SPARK_SUBMIT_ARGS") or "")
    return "--master" in s or "spark-submit" in s

def get_spark(app_name: str, extra_conf: Optional[Dict[str, Any]] = None) -> SparkSession:
    """
    Unified SparkSession builder.

    Master selection:
      - If FORCE_MASTER is set -> use that (explicit opt-in per service/task).
      - Else if NOT in spark-submit -> use local[*] (plain Python / BashOperator).
      - Else (in spark-submit) -> do NOT set master here (respect submit command / operator).

    JARs/Packages:
      - If SPARK_JARS is set -> use those local jar paths (comma-separated).
      - Else set spark.jars.packages to SPARK_JARS_PACKAGES or DEFAULT_PKGS.

    Mongo:
      - If MONGO_URI is present, set input/output URIs for the v10 connector ("mongodb" format).

    Extra:
      - Always enable Delta SQL extensions and UTC session timezone.
      - Gentle defaults for streaming/shuffle; override via env or extra_conf.
    """
    force_master = (os.getenv("FORCE_MASTER") or "").strip()
    jars_paths   = (os.getenv("SPARK_JARS") or "").strip()                # e.g., "/opt/jars/a.jar,/opt/jars/b.jar"
    pkgs         = (os.getenv("SPARK_JARS_PACKAGES") or DEFAULT_PKGS).strip()
    mongo_uri    = (os.getenv("MONGO_URI") or "").strip()

    b = SparkSession.builder.appName(app_name)

    # --- Master logic ---
    if force_master:
        b = b.master(force_master)
    elif not _in_spark_submit():
        # Plain python: choose a sane default
        b = b.master("local[*]")
    # else: in spark-submit -> submit command decides

    # --- JARs / Packages (prefer explicit jars over packages) ---
    if jars_paths:
        b = b.config("spark.jars", jars_paths)
    elif pkgs:
        b = b.config("spark.jars.packages", pkgs)

    # --- Delta + basics ---
    b = (b
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.streaming.stopGracefullyOnShutdown", "true")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
    )

    # --- Mongo v10 convenience (format = "mongodb") ---
    if mongo_uri:
        b = (b
             .config("spark.mongodb.input.uri", mongo_uri)
             .config("spark.mongodb.output.uri", mongo_uri))

    # --- Per-job overrides ---
    if extra_conf:
        for k, v in extra_conf.items():
            b = b.config(k, str(v))

    return b.getOrCreate()



DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
MONGO_PKG = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

def get_spark_airflow(app_name: str) -> SparkSession:
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