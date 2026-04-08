from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col, from_json

spark = SparkSession.builder \
    .appName("bkk_bronze_igestion") \
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

print("Connecting to Kfka and starting the stream...")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bkk.dev.realtime.raw") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), bkk_schema).alias("data")) \
    .select("data.*")

query = parsed_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", ".data/lakehouse/checkpoints/bronze_bkk") \
    .start("./data/lakehouse/bronze/bkk_realtime")

query.awaitTermination()