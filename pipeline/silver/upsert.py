"""Incremental silver loads: a real Delta `MERGE INTO`, and the trap inside it.

The full-refresh path in `clean.py` is fine for a first load, but a lakehouse
that only ever overwrites is not a lakehouse -- it is a batch job with extra
steps. This module is the incremental path: each extract arrives as a batch,
lands in bronze by append, and is merged into silver on its natural key.

**The trap.** `MERGE INTO` requires that at most one source row matches each
target row. Feed it a source containing the same natural key twice and Delta
raises rather than silently picking a winner:

    Cannot perform Merge as multiple source rows matched ...

That is Delta protecting you from a non-deterministic write. The fix is not to
disable the check -- it is to collapse duplicates deterministically *before* the
merge, with an explicit ordering, which is what `clean.apply_uniqueness` does.
`tests/test_incremental.py` asserts both halves: that an un-deduplicated source
really does fail, and that the pipeline's own output really does not.

Delta needs a Hadoop runtime, which on Windows means `winutils.exe`. These paths
are therefore exercised on the Linux CI runners rather than locally, and the
tests skip themselves when Delta cannot start.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from ..common import Layout, get_spark

#: Natural key per entity -- what makes a row "the same row" across batches.
NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "merchants": ("merchant_id",),
    "loans": ("loan_id",),
    "emi_schedule": ("loan_id", "instalment_no"),
    "repayment_attempts": ("attempt_id",),
}


def delta_session(app: str = "lakehouse", master: str = "local[2]") -> SparkSession:
    """A Delta-enabled session, for tests and local runs.

    On Databricks this is unnecessary -- Delta is the default and the platform
    owns the session -- so it returns the ambient session if there is one.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ensure_table(spark: SparkSession, table: str, template: DataFrame) -> bool:
    """Create an empty Delta table shaped like `template` if it does not exist.

    Returns True when it created one. A merge into a table that does not exist
    yet is the other common first-run failure, and it is cheaper to handle here
    than to make every caller branch on it.
    """
    if spark.catalog.tableExists(table):
        return False
    template.limit(0).write.format("delta").saveAsTable(table)
    return True


def merge(spark: SparkSession, source: DataFrame, table: str,
          keys: tuple[str, ...]) -> dict[str, int]:
    """Upsert `source` into `table` on `keys`.

    `UPDATE SET *` / `INSERT *` rather than a hand-written column list, so a new
    column added upstream flows through instead of being silently dropped -- with
    the schema evolution that requires enabled explicitly rather than globally.
    """
    ensure_table(spark, table, source)

    view = f"_src_{abs(hash(table)) % 10**8}"
    source.createOrReplaceTempView(view)

    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    before = spark.table(table).count()

    spark.sql(f"""
        MERGE INTO {table} AS t
        USING {view} AS s
          ON {on}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    after = spark.table(table).count()
    return {"before": before, "after": after, "inserted": after - before}


def merge_all(spark: SparkSession, layout: Layout,
              frames: dict[str, DataFrame]) -> dict[str, dict[str, int]]:
    out = {}
    for entity, df in frames.items():
        keys = NATURAL_KEYS[entity]
        out[entity] = merge(spark, df, layout.table("silver", entity), keys)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reporting-date", required=True)
    args = ap.parse_args(argv)

    from .clean import apply_referential, apply_row_rules, apply_uniqueness, split, typed

    spark = get_spark()
    layout = Layout()
    order = ["customers", "merchants", "loans", "emi_schedule", "repayment_attempts"]

    cleaned: dict[str, DataFrame] = {}
    for entity in order:
        df = typed(spark.table(layout.table("bronze", entity)), entity)
        df = apply_row_rules(df, entity, args.reporting_date)
        df = apply_uniqueness(df, entity)
        df = apply_referential(df, entity, cleaned)
        good, _bad = split(df, entity)
        cleaned[entity] = good.drop("_corrupt_record")

    for entity, stats in merge_all(spark, layout, cleaned).items():
        print(f"silver_{entity}: {stats['before']:,} -> {stats['after']:,} "
              f"(+{stats['inserted']:,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
