from pyspark.sql import functions as F


def _hash_columns(cols: list[str]) -> F.Column:
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols]
        ),
        256,
    )


def run_silver(spark, base_path: str):
    # -----------------------------------------
    # silver.taxi_zone_lookup
    # SQL equivalent:
    # SELECT DISTINCT
    #     LocationID AS locationid,
    #     TRIM(Borough) AS borough,
    #     TRIM("Zone") AS zone,
    #     TRIM(service_zone) AS service_zone
    # FROM bronze.taxi_zone_lookup_raw
    # WHERE LocationID IS NOT NULL;
    # -----------------------------------------
    lookup = spark.read.parquet(f"{base_path}/bronze/taxi_zone_lookup_raw")

    silver_lookup = (
        lookup
        .filter(F.col("LocationID").isNotNull())
        .select(
            F.col("LocationID").cast("int").alias("locationid"),
            F.trim(F.col("Borough")).alias("borough"),
            F.trim(F.col("Zone")).alias("zone"),
            F.trim(F.col("service_zone")).alias("service_zone"),
        )
        .distinct()
    )

    (
        silver_lookup.write
        .mode("overwrite")
        .parquet(f"{base_path}/silver/taxi_zone_lookup")
    )

    # -----------------------------------------
    # silver.yellow_taxi_clean
    # SQL equivalent:
    # SELECT
    #   ROW_NUMBER() OVER () AS trip_id,
    #   tpep_pickup_datetime AS pickup_datetime,
    #   tpep_dropoff_datetime AS dropoff_datetime,
    #   DATE_TRUNC('month', tpep_pickup_datetime) AS pickup_month,
    #   EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes,
    #   trip_distance,
    #   total_amount,
    #   PULocationID AS pulocationid,
    #   DOLocationID AS dolocationid,
    #   payment_type,
    #   CASE ... END AS is_valid_record
    # FROM bronze.yellow_taxi_raw;
    #
    # NOTE:
    # Instead of ROW_NUMBER() I use stable hash-based trip_id
    # for better idempotency.
    # -----------------------------------------
    trips = spark.read.parquet(f"{base_path}/bronze/yellow_taxi_raw")

    silver_trips = (
        trips
        .select(
            F.col("tpep_pickup_datetime").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            F.date_trunc("month", F.col("tpep_pickup_datetime")).alias("pickup_month"),
            (
                (
                    F.unix_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp")) -
                    F.unix_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"))
                ) / 60.0
            ).alias("trip_duration_minutes"),
            F.col("trip_distance"),
            F.col("total_amount"),
            F.col("PULocationID").cast("int").alias("pulocationid"),
            F.col("DOLocationID").cast("int").alias("dolocationid"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.when(F.col("tpep_pickup_datetime").isNull(), F.lit(False))
             .when(F.col("tpep_dropoff_datetime").isNull(), F.lit(False))
             .when(F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"), F.lit(False))
             .when(F.col("trip_distance") < 0, F.lit(False))
             .when(F.col("total_amount") < 0, F.lit(False))
             .otherwise(F.lit(True))
             .alias("is_valid_record"),
            F.col("raw_trip_id")
        )
        .withColumn(
            "trip_id",
            _hash_columns(
                [
                    "pickup_datetime",
                    "dropoff_datetime",
                    "pickup_month",
                    "trip_duration_minutes",
                    "trip_distance",
                    "total_amount",
                    "pulocationid",
                    "dolocationid",
                    "payment_type",
                    "raw_trip_id",
                ]
            )
        )
        .select(
            "trip_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_month",
            "trip_duration_minutes",
            "trip_distance",
            "total_amount",
            "pulocationid",
            "dolocationid",
            "payment_type",
            "is_valid_record",
        )
        .dropDuplicates(["trip_id"])
    )

    (
        silver_trips.write
        .mode("overwrite")
        .parquet(f"{base_path}/silver/yellow_taxi_clean")
    )

    print("Silver loaded")