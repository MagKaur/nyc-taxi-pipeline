from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("read-test")
    .getOrCreate()
)

df = spark.read.parquet("data/raw/yellow_tripdata_2025-12.parquet")
print(df.limit(5).collect())

spark.stop()