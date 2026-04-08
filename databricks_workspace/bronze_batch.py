from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder \
    .appName("bkk_bronze_batch") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bkk_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

print("Connecting to Kafka to read the batch...")

raw_batch = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "bkk.dev.realtime.raw") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_batch = raw_batch.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bkk_schema).alias("data")) \
    .select("data.*")

print(f"Processing {parsed_batch.count()} new records...")

parsed_batch.write \
    .format("delta") \
    .mode("append") \
    .save("/opt/airflow/workspace/data/lakehouse/bronze/bkk_realtime")

print("Batch written successfully!")
spark.stop()
