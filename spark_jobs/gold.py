from pyspark.sql import functions as F


def run_gold(spark, base_path: str):
    y = spark.read.parquet(f"{base_path}/silver/yellow_taxi_clean")
    lookup = spark.read.parquet(f"{base_path}/silver/taxi_zone_lookup")

    pu = lookup.alias("pu")
    do_ = lookup.alias("do_")

    gold = (
        y.alias("y")
        .join(
            pu,
            F.col("y.pulocationid") == F.col("pu.locationid"),
            "left"
        )
        .join(
            do_,
            F.col("y.dolocationid") == F.col("do_.locationid"),
            "left"
        )
        .filter(F.col("y.is_valid_record") == True)
        .groupBy(
            F.col("y.pickup_month").alias("month"),
            F.col("pu.zone").alias("pickup_zone"),
            F.col("do_.zone").alias("dropoff_zone"),
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum(F.col("y.total_amount")), 2).alias("total_revenue"),
            F.round(F.avg(F.col("y.trip_distance")), 2).alias("avg_distance"),
        )
        .withColumn(
            "gold_record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("month").cast("string"), F.lit("NULL")),
                    F.coalesce(F.col("pickup_zone"), F.lit("NULL")),
                    F.coalesce(F.col("dropoff_zone"), F.lit("NULL")),
                ),
                256,
            )
        )
        .select(
            "gold_record_id",
            "month",
            "pickup_zone",
            "dropoff_zone",
            "trip_count",
            "total_revenue",
            "avg_distance",
        )
    )

    (
        gold.write
        .mode("overwrite")
        .parquet(f"{base_path}/gold/taxi_metrics")
    )

    print("Gold loaded")