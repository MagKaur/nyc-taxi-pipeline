from pathlib import Path

import dagster as dg

from dagster_project.assets import (
    queue_ingestion,
    bronze_load,
    silver_transform,
    gold_metrics,
    quality_checks,
)


etl_job = dg.define_asset_job(
    name="etl_job",
    selection=dg.AssetSelection.all(),
)


@dg.sensor(job=etl_job)
def queue_sensor():
    queue_path = Path("data/queue")

    files = [
        p for p in queue_path.iterdir()
        if p.is_file() and p.name != ".gitkeep"
    ]

    if not files:
        yield dg.SkipReason("No files in data/queue.")
        return

    run_key = "|".join(sorted(file.name for file in files))

    yield dg.RunRequest(
        run_key=run_key,
        run_config={},
    )


defs = dg.Definitions(
    assets=[
        queue_ingestion,
        bronze_load,
        silver_transform,
        gold_metrics,
        quality_checks,
    ],
    jobs=[etl_job],
    sensors=[queue_sensor],
)