from dagster import Definitions
from dagster_project.assets import bronze_load, silver_transform, gold_metrics, quality_checks

defs = Definitions(
    assets=[bronze_load, silver_transform, gold_metrics, quality_checks]
)