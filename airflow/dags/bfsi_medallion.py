"""The medallion run as an Airflow DAG: bronze -> silver -> snapshot -> gold.

This is the same graph as `resources/medallion.job.yml`, expressed for a team
that schedules with Airflow rather than with Databricks Jobs. Both exist on
purpose: the bundle is how the pipeline runs *on* Databricks Free Edition, and
this DAG is how it would be run by a platform that already owns an Airflow
deployment and wants day-end lending stages sitting in the same scheduler as
everything else it operates.

The two are kept honest by `tests/test_airflow_dag.py`, which reads the bundle
YAML and asserts that this DAG has the same task keys and the same edges. Two
descriptions of one pipeline drift apart the moment nothing compares them, and
a scheduler quietly running a stale graph is a worse failure than one that will
not start: gold would report yesterday's numbers as though they were today's.

Two execution targets, chosen by `BFSI_AIRFLOW_TARGET`:

  * `local` (default) -- each stage is a `BashOperator` running
    `jobs/run_stage.py`, the same entrypoint the Databricks tasks use. Needs no
    cloud account, which is why the test suite exercises this path.

  * `databricks` -- each stage becomes a `DatabricksSubmitRunOperator` against
    serverless compute. This is the shape a real deployment uses, and it is kept
    behind the flag rather than made the default because importing the provider
    at parse time would make the DAG unloadable anywhere the provider is not
    installed. An Airflow DAG that fails to import does not surface as a failed
    task; it disappears from the UI, which is the least visible way for a
    pipeline to stop existing.

Scheduling note: `catchup=False` is deliberate. `--reporting-date` is the date
the day-end process runs *for*, and backfilling it would re-stamp historical
DPD from today's book rather than from the book as it stood -- the arrears
history is in the snapshot table, not reconstructable from a later state. A
genuine restatement is a deliberate windowed re-run, not a scheduler catching up.
"""

from __future__ import annotations

import os
import pendulum
from pathlib import Path

from airflow.models.dag import DAG
from airflow.models.param import Param

# Airflow 3 moved the standard operators into their own provider package and
# left the 2.x path raising a DeprecationWarning that became a hard error. A DAG
# pinned to one import works on exactly one major version, and the version an
# Airflow shop is on is not something this repository gets to choose.
try:                                            # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:                             # Airflow 2.x
    from airflow.operators.bash import BashOperator

# The repository root as seen from `airflow/dags/bfsi_medallion.py`. Airflow
# parses DAG files from wherever `dags_folder` points, so the path is derived
# rather than assumed to be the process working directory.
REPO_ROOT = Path(__file__).resolve().parents[2]

# Task keys are the contract with the Databricks bundle. The parity test
# compares this tuple against `resources/medallion.job.yml`, so renaming a stage
# in one place and not the other fails CI rather than production.
STAGES = ("bronze", "silver", "snapshot", "gold")

# Which stages take `--window-start`. Only the snapshot stage walks a window of
# month ends; giving it to the others would be silently accepted and ignored,
# which is the kind of thing that reads as understood until someone changes the
# argument parser.
WINDOW_STAGES = frozenset({"snapshot"})

TARGET = os.environ.get("BFSI_AIRFLOW_TARGET", "local")


def _stage_arguments(stage: str) -> list[str]:
    """CLI arguments for one stage, matching the bundle's task parameters."""
    args = [
        f"--stage={stage}",
        "--catalog={{ params.catalog }}",
        "--schema={{ params.schema }}",
    ]
    if stage in WINDOW_STAGES:
        args.append("--window-start={{ params.window_start }}")
    args.append("--reporting-date={{ params.reporting_date }}")
    if stage == "bronze":
        # Bronze stamps the generator version into lineage; the other stages
        # read it back rather than being told it again.
        args.append("--generator-version=1.0.0")
    return args


def _make_task(stage: str, dag: DAG):
    """One operator per stage, per the configured target."""
    if TARGET == "databricks":
        # Imported inside the branch, not at module scope: see the module
        # docstring on why a parse-time ImportError is the worst failure mode
        # available to a DAG.
        from airflow.providers.databricks.operators.databricks import (
            DatabricksSubmitRunOperator,
        )

        return DatabricksSubmitRunOperator(
            task_id=stage,
            dag=dag,
            databricks_conn_id=os.environ.get("BFSI_DATABRICKS_CONN", "databricks_default"),
            json={
                "run_name": f"bfsi-medallion-{stage}",
                "environments": [{"environment_key": "default",
                                  "spec": {"environment_version": "4"}}],
                "tasks": [{
                    "task_key": stage,
                    "environment_key": "default",
                    "spark_python_task": {
                        "python_file": "jobs/run_stage.py",
                        "parameters": _stage_arguments(stage),
                    },
                }],
            },
        )

    return BashOperator(
        task_id=stage,
        dag=dag,
        cwd=str(REPO_ROOT),
        bash_command="python jobs/run_stage.py " + " ".join(_stage_arguments(stage)),
    )


with DAG(
    dag_id="bfsi_lending_lakehouse_medallion",
    description="Day-end medallion run: bronze -> silver -> snapshot -> gold",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    # Day-end, after the book has closed. The pipeline is idempotent per
    # reporting date, so a re-run replaces that date's output rather than
    # appending to it.
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,          # mirrors the bundle's max_concurrent_runs: 1
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["bfsi", "lending", "medallion"],
    params={
        "catalog": Param("workspace", type="string"),
        "schema": Param("default", type="string"),
        "reporting_date": Param("2026-08-31", type="string"),
        "window_start": Param("2024-09-30", type="string"),
    },
) as dag:
    previous = None
    for _stage in STAGES:
        task = _make_task(_stage, dag)
        if previous is not None:
            previous >> task
        previous = task
