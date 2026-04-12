from pyspark.sql import functions as F


def run_quality_checks(spark, base_path: str):
    bronze = spark.read.parquet(f"{base_path}/bronze/yellow_taxi_raw")
    silver = spark.read.parquet(f"{base_path}/silver/yellow_taxi_clean")
    lookup = spark.read.parquet(f"{base_path}/silver/taxi_zone_lookup")

    invalid_time_records = bronze.filter(
        F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime")
    ).count()

    negative_values = bronze.filter(
        (F.col("trip_distance") < 0) | (F.col("total_amount") < 0)
    ).count()

    missing_location_lookup = (
        silver.alias("y")
        .join(
            lookup.alias("l"),
            F.col("y.pulocationid") == F.col("l.locationid"),
            "left"
        )
        .filter(F.col("l.locationid").isNull())
        .count()
    )

    invalid_records_silver = silver.filter(
        F.col("is_valid_record") == F.lit(False)
    ).count()

    print("Data quality checks")
    print("-------------------")
    print("invalid_time_records =", invalid_time_records)
    print("negative_values =", negative_values)
    print("missing_location_lookup =", missing_location_lookup)
    print("invalid_records_silver =", invalid_records_silver)