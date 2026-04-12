from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("test")
    .getOrCreate()
)

df = spark.range(5)
print(df.collect())

spark.stop()