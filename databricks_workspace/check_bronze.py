from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check_Bronze") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("./data/lakehouse/bronze/bkk_realtime")

print(f"Total records saved: {df.count()}")
df.show(5, truncate=False)