from argparse import ArgumentParser
from pyspark.sql import SparkSession

from bronze import run_bronze
from silver import run_silver
from gold import run_gold
from quality_check import run_quality_checks
from queue_ingestion import move_files_from_queue


def build_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-taxi-medallion")
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
        .getOrCreate()
    )


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "quality", "queue", "all"],
        required=True
    )
    parser.add_argument("--base-path", default="data/warehouse")
    parser.add_argument("--raw-path", default="data/raw")
    args = parser.parse_args()

    spark = build_spark()

    try:
        if args.stage in ("bronze", "all"):
            run_bronze(spark, args.raw_path, args.base_path)

        if args.stage in ("silver", "all"):
            run_silver(spark, args.base_path)

        if args.stage in ("gold", "all"):
            run_gold(spark, args.base_path)

        if args.stage in ("quality", "all"):
            run_quality_checks(spark, args.base_path)

        if args.stage in ("queue", "all"):
            move_files_from_queue("data/queue", args.raw_path)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()