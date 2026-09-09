"""The Airflow DAG and the Databricks bundle must describe the same pipeline.

Two schedulers, one medallion graph. Nothing stops them drifting apart except a
test that reads both and compares them, so this is that test.

The failure this guards against is not hypothetical: rename a stage in
`resources/medallion.job.yml`, or insert a task between silver and snapshot, and
an Airflow deployment carries on running the old graph. Gold would still be
produced, on time, green in the UI, and reporting the wrong day's arrears. A
scheduler that fails loudly is recoverable; one that succeeds against a stale
graph is not.

Airflow is not a dependency of the pipeline itself, so these tests skip when it
is absent rather than failing. CI installs it in a dedicated job.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml", reason="PyYAML needed to read the bundle")

REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE = REPO_ROOT / "resources" / "medallion.job.yml"
DAG_FILE = REPO_ROOT / "airflow" / "dags" / "bfsi_medallion.py"


def _bundle_graph() -> tuple[list[str], set[tuple[str, str]]]:
    """Task keys in declaration order, and the set of (upstream, downstream) edges."""
    spec = yaml.safe_load(BUNDLE.read_text(encoding="utf-8"))
    tasks = spec["resources"]["jobs"]["medallion"]["tasks"]

    keys = [t["task_key"] for t in tasks]
    edges = {
        (dep["task_key"], t["task_key"])
        for t in tasks
        for dep in t.get("depends_on", [])
    }
    return keys, edges


def _load_dag():
    """Import the DAG module and hand back the DAG object.

    `DagBag` is deliberately not used: it swallows import errors into a report
    rather than raising, and a DAG that cannot be imported is exactly the
    failure this file exists to catch.
    """
    # `importorskip("airflow")` alone is not enough. A failed or partial install
    # leaves an importable `airflow` package behind with no `__version__`, which
    # sails past the skip and then fails later on a missing transitive
    # dependency -- reported as six broken assertions rather than "not
    # installed". Both the package and its own dependency are checked, and the
    # version attribute is what distinguishes a real install from a husk.
    airflow = pytest.importorskip("airflow", reason="Airflow not installed")
    if not getattr(airflow, "__version__", None):
        pytest.skip("partial/broken Airflow install: no __version__")
    pytest.importorskip("pendulum", reason="pendulum (an Airflow dependency) missing")

    import importlib.util

    # The DAG reads this at import time to choose its operator; pin it so the
    # test exercises the path that needs no cloud account.
    os.environ.setdefault("BFSI_AIRFLOW_TARGET", "local")

    spec = importlib.util.spec_from_file_location("bfsi_medallion", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_bundle_is_readable():
    """A guard on the guard: if the bundle stops parsing, say so here."""
    keys, edges = _bundle_graph()
    assert keys, "no tasks found in the bundle"
    assert edges, "bundle declares no dependencies -- stages would run in parallel"


def test_dag_imports():
    """An unimportable DAG vanishes from the Airflow UI rather than erroring."""
    dag = _load_dag()
    assert dag.dag_id == "bfsi_lending_lakehouse_medallion"


def test_task_keys_match_the_bundle():
    dag = _load_dag()
    bundle_keys, _ = _bundle_graph()
    assert sorted(t.task_id for t in dag.tasks) == sorted(bundle_keys)


def test_edges_match_the_bundle():
    """Same dependencies, so neither scheduler can run a stage out of order."""
    dag = _load_dag()
    _, bundle_edges = _bundle_graph()

    dag_edges = {
        (upstream.task_id, task.task_id)
        for task in dag.tasks
        for upstream in task.upstream_list
    }
    assert dag_edges == bundle_edges


def test_run_is_serialised():
    """Overlapping runs would let gold read a half-written silver."""
    dag = _load_dag()
    assert dag.max_active_runs == 1


def test_catchup_is_off():
    """Backfilling would re-stamp historical DPD from today's book.

    The arrears history lives in the snapshot table and is not reconstructable
    from a later state, so a scheduler catching up would silently rewrite it.
    """
    dag = _load_dag()
    assert dag.catchup is False


def test_only_snapshot_takes_a_window():
    """`--window-start` is meaningful to one stage; the parser ignores it elsewhere."""
    dag = _load_dag()
    for task in dag.tasks:
        command = getattr(task, "bash_command", "") or ""
        if task.task_id == "snapshot":
            assert "--window-start" in command
        else:
            assert "--window-start" not in command
