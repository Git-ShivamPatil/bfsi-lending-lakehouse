"""Incremental Delta loads: idempotency, late corrections, and the MERGE trap.

These need a Delta runtime, which needs Hadoop native libraries. On Windows that
means `winutils.exe`, so the whole module skips itself there and proves itself on
the Linux CI runners instead.

A separate Spark session from the rest of the suite: Delta has to be configured
at session construction, and Spark allows one session per JVM.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def dspark(tmp_path_factory):
    delta = pytest.importorskip("delta", reason="delta-spark is not installed")
    from pipeline.silver.upsert import delta_session

    warehouse = tmp_path_factory.mktemp("warehouse")
    try:
        spark = delta_session("incremental-tests")
    except Exception as exc:                       # pragma: no cover
        pytest.skip(f"Delta could not start (expected on Windows): {exc}")

    spark.sql("CREATE DATABASE IF NOT EXISTS inc")
    spark.sql("USE inc")
    yield spark
    spark.stop()


def _rows(spark, rows):
    return spark.createDataFrame(
        rows, "attempt_id string, loan_id string, amount double, status string")


def test_merge_is_idempotent(dspark):
    """Replaying the same batch must not change the table.

    Pipelines get re-run: a retry, a backfill, a job that failed after writing.
    If a replay doubles the row count, every downstream number is wrong and the
    failure is invisible until someone reconciles by hand.
    """
    from pipeline.silver.upsert import merge

    batch = _rows(dspark, [("A1", "L1", 100.0, "SUCCESS"),
                           ("A2", "L1", 200.0, "SUCCESS"),
                           ("A3", "L2", 300.0, "BOUNCED")])

    first = merge(dspark, batch, "inc.attempts_idem", ("attempt_id",))
    assert first["after"] == 3

    second = merge(dspark, batch, "inc.attempts_idem", ("attempt_id",))
    assert second["after"] == 3, "replaying a batch changed the table"
    assert second["inserted"] == 0

    got = {r.attempt_id: r.amount for r in dspark.table("inc.attempts_idem").collect()}
    assert got == {"A1": 100.0, "A2": 200.0, "A3": 300.0}


def test_a_corrected_row_updates_in_place(dspark):
    """A restated value must overwrite, not append a second version.

    Reporting entities revise submissions; a correction that lands as an extra
    row instead of an update double-counts the amount.
    """
    from pipeline.silver.upsert import merge

    merge(dspark, _rows(dspark, [("B1", "L9", 500.0, "BOUNCED")]),
          "inc.attempts_fix", ("attempt_id",))
    merge(dspark, _rows(dspark, [("B1", "L9", 500.0, "SUCCESS"),
                                 ("B2", "L9", 750.0, "SUCCESS")]),
          "inc.attempts_fix", ("attempt_id",))

    out = {r.attempt_id: (r.amount, r.status)
           for r in dspark.table("inc.attempts_fix").collect()}
    assert out == {"B1": (500.0, "SUCCESS"), "B2": (750.0, "SUCCESS")}


def test_duplicate_source_keys_make_merge_refuse(dspark):
    """Delta must refuse an ambiguous merge rather than pick a winner.

    This is the failure the silver de-duplication exists to prevent. Asserting
    that it really does fail is what makes the de-duplication a fix rather than
    a superstition.
    """
    from pipeline.silver.upsert import merge

    ambiguous = _rows(dspark, [("C1", "L1", 100.0, "SUCCESS"),
                               ("C1", "L1", 999.0, "BOUNCED")])
    merge(dspark, _rows(dspark, [("C1", "L1", 1.0, "SUCCESS")]),
          "inc.attempts_dupe", ("attempt_id",))

    with pytest.raises(Exception) as excinfo:
        merge(dspark, ambiguous, "inc.attempts_dupe", ("attempt_id",))

    message = str(excinfo.value).lower()
    assert "multiple source rows" in message or "merge" in message


def test_pipeline_output_is_safe_to_merge(dspark, tmp_path):
    """The real silver output must survive a merge -- twice.

    End to end: generate a book, run it through the rule engine, merge the
    result, and merge it again. The de-duplication in `apply_uniqueness` is what
    stands between the injected duplicate attempt ids and the failure above.
    """
    from generator.generate import main as generate
    from pipeline.bronze.ingest import SOURCES, add_lineage, read_source
    from pipeline.silver import clean as S
    from pipeline.silver.upsert import NATURAL_KEYS, merge

    raw = tmp_path / "raw"
    generate(["--loans", "1500", "--merchants", "60", "--months", "12",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(raw)])

    for name in SOURCES:
        add_lineage(read_source(dspark, raw.as_posix(), name),
                    "b1", "test").createOrReplaceTempView(f"bronze_{name}")

    cleaned = {}
    for entity in ["customers", "merchants", "loans", "emi_schedule",
                   "repayment_attempts"]:
        df = S.typed(dspark.table(f"bronze_{entity}"), entity)
        df = S.apply_row_rules(df, entity, "2026-08-31")
        df = S.apply_uniqueness(df, entity)
        df = S.apply_referential(df, entity, cleaned)
        good, _ = S.split(df, entity)
        cleaned[entity] = good.drop("_corrupt_record").cache()

    entity = "repayment_attempts"
    table = f"inc.silver_{entity}"
    first = merge(dspark, cleaned[entity], table, NATURAL_KEYS[entity])
    second = merge(dspark, cleaned[entity], table, NATURAL_KEYS[entity])

    assert first["after"] == second["after"], "second merge changed the table"
    assert second["inserted"] == 0

    n = dspark.table(table).count()
    distinct = dspark.table(table).select("attempt_id").distinct().count()
    assert n == distinct


def test_delta_history_records_every_write(dspark):
    """Time travel is the audit trail. It has to actually be there."""
    from pipeline.silver.upsert import merge

    for i in range(3):
        merge(dspark, _rows(dspark, [(f"H{i}", "L1", float(i), "SUCCESS")]),
              "inc.attempts_hist", ("attempt_id",))

    history = dspark.sql("DESCRIBE HISTORY inc.attempts_hist")
    assert history.count() >= 3
    ops = {r["operation"] for r in history.collect()}
    assert any("MERGE" in o.upper() for o in ops)
