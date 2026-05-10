import os
import sys
import shutil
import subprocess
from pathlib import Path

import dagster as dg


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_SCRIPT = PROJECT_ROOT / "spark_jobs" / "main.py"

QUEUE_PATH = PROJECT_ROOT / "data" / "queue"
RAW_PATH = PROJECT_ROOT / "data" / "raw"


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
def queue_ingestion():
    QUEUE_PATH.mkdir(parents=True, exist_ok=True)
    RAW_PATH.mkdir(parents=True, exist_ok=True)

    files = [
        p for p in QUEUE_PATH.iterdir()
        if p.is_file() and p.name != ".gitkeep"
    ]

    for file in files:
        target = RAW_PATH / file.name

        if target.exists():
            file.unlink()
        else:
            shutil.move(str(file), str(target))

    print(f"Moved {len(files)} files from queue to raw")


@dg.asset(deps=[queue_ingestion])
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