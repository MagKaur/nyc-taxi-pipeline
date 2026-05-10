import os
import sys
import subprocess
from pathlib import Path

import dagster as dg


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_SCRIPT = PROJECT_ROOT / "spark_jobs" / "main.py"


def _build_env() -> dict:
    env = os.environ.copy()

    env["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
    env["PATH"] = rf"{env['JAVA_HOME']}\bin;{env['PATH']}"

    env["HADOOP_HOME"] = r"C:\hadoop"
    env["hadoop.home.dir"] = r"C:\hadoop"
    env["PATH"] = rf"{env['HADOOP_HOME']}\bin;{env['PATH']}"

    return env


def _run_stage(stage: str) -> None:
    cmd = [
        sys.executable,
        str(MAIN_SCRIPT),
        "--stage", stage,
        "--base-path", "data/warehouse",
        "--raw-path", "data/raw",
    ]

    subprocess.run(
        cmd,
        check=True,
        env=_build_env(),
        cwd=str(PROJECT_ROOT),
    )


@dg.asset
def bronze_load():
    _run_stage("bronze")


@dg.asset(deps=[bronze_load])
def silver_transform():
    _run_stage("silver")


@dg.asset(deps=[silver_transform])
def gold_metrics():
    _run_stage("gold")


@dg.asset(deps=[gold_metrics])
def quality_checks():
    _run_stage("quality")

@dg.sensor(job=dg.define_asset_job("etl_job"))
def queue_sensor():
    queue_path = Path("data/queue")

    if any(queue_path.glob("*")):
        yield dg.RunRequest(
            run_key=None,
            run_config={}
        )