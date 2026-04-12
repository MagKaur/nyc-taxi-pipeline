from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


TRIP_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "cbd_congestion_fee",
]


def _hash_columns(cols: list[str]) -> F.Column:
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols]
        ),
        256,
    )


def _manifest_path(base_path: str) -> str:
    return f"{base_path}/_metadata/processed_files"


def _read_manifest(spark, base_path: str) -> DataFrame | None:
    path = _manifest_path(base_path)
    if Path(path).exists():
        return spark.read.parquet(path)
    return None


def _write_manifest(df: DataFrame, base_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(_manifest_path(base_path))
    )


def run_bronze(spark, raw_path: str, base_path: str):
    Path(f"{base_path}/bronze").mkdir(parents=True, exist_ok=True)
    Path(f"{base_path}/_metadata").mkdir(parents=True, exist_ok=True)

    # -----------------------------------------
    # Trips: read all raw parquet files
    # -----------------------------------------
    trips_all = (
        spark.read.parquet(f"{raw_path}/yellow_tripdata_*.parquet")
        .select(*TRIP_COLUMNS)
        .withColumn("source_file", F.input_file_name())
        .withColumn("load_timestamp", F.current_timestamp())
        .withColumn(
            "raw_trip_id",
            _hash_columns(TRIP_COLUMNS + ["source_file"])
        )
    )

    # Find only new files using manifest
    manifest = _read_manifest(spark, base_path)

    current_files = (
        trips_all
        .select("source_file")
        .distinct()
    )

    if manifest is not None:
        new_files = current_files.join(manifest, on="source_file", how="left_anti")
    else:
        new_files = current_files

    new_file_count = new_files.count()

    if new_file_count > 0:
        trips_new = (
            trips_all
            .join(new_files, on="source_file", how="inner")
            .dropDuplicates(["raw_trip_id"])
        )

        bronze_trips_path = f"{base_path}/bronze/yellow_taxi_raw"

        if Path(bronze_trips_path).exists():
            existing = spark.read.parquet(bronze_trips_path)
            combined = (
                existing.unionByName(trips_new)
                .dropDuplicates(["raw_trip_id"])
            )
        else:
            combined = trips_new

        (
            combined.write
            .mode("overwrite")
            .parquet(bronze_trips_path)
        )

        new_manifest = (
            new_files
            .withColumn("processed_at", F.current_timestamp())
        )

        if manifest is not None:
            manifest_out = manifest.unionByName(new_manifest)
        else:
            manifest_out = new_manifest

        manifest_out = manifest_out.dropDuplicates(["source_file"])
        _write_manifest(manifest_out, base_path)

    # -----------------------------------------
    # Lookup: deterministic overwrite
    # -----------------------------------------
    lookup = (
        spark.read.option("header", True).csv(f"{raw_path}/taxi_zone_lookup.csv")
        .select("LocationID", "Borough", "Zone", "service_zone")
        .withColumn("source_file", F.lit("taxi_zone_lookup.csv"))
        .withColumn("load_timestamp", F.current_timestamp())
        .withColumn(
            "raw_lookup_id",
            _hash_columns(["LocationID", "Borough", "Zone", "service_zone", "source_file"])
        )
        .dropDuplicates(["raw_lookup_id"])
    )

    (
        lookup.write
        .mode("overwrite")
        .parquet(f"{base_path}/bronze/taxi_zone_lookup_raw")
    )

    print("Bronze loaded")