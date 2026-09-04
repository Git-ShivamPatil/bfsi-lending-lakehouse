"""Shared plumbing: session, naming, and the one switch that lets the same code
run on Databricks Free Edition and inside a CI runner.

Free Edition is serverless-only, which constrains this module more than it looks:

* Only Spark Connect APIs are available -- no RDD APIs anywhere in this project.
* Compute configuration is fixed, so nothing here tunes shuffle partitions on
  Databricks; it only does so locally, where the default of 200 would make a
  10-row unit test take longer than the assertion it is proving.
* Scala, JARs and Maven coordinates are unavailable, so everything is PySpark.

Locally (and in CI) there is no Delta runtime unless it is installed, so the
write format is a parameter. The tests exercise the transformations, which is
where the logic lives; the storage format is a deployment detail.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class Layout:
    """Where tables live, and in what format.

    The catalog and schema default to `workspace.default`, which Free Edition
    pre-provisions, rather than to a catalog of our own. That is not a style
    choice: creating a new catalog on Free Edition has a reported failure --
    "Metastore storage root URL does not exist. Default Storage is enabled in
    your account..." -- and a runbook whose first cell fails is worse than one
    that uses the catalog that is already there.
    """

    catalog: str = "workspace"
    schema: str = "default"
    fmt: str = "delta"
    #: Unity Catalog volume that GitHub Actions pushes raw extracts into.
    #: Free Edition cannot reach an external object store -- there is no account
    #: console, so there is no way to mint the storage credential an external
    #: location needs -- so the landing zone is a managed volume and CI does the
    #: fetching. See .github/workflows/databricks.yml.
    raw_volume: str = "/Volumes/workspace/default/raw"
    qualified: bool = True

    def table(self, layer: str, name: str) -> str:
        t = f"{layer}_{name}"
        if not self.qualified:
            return t
        return f"{self.catalog}.{self.schema}.{t}"


def local_layout(tmp: str) -> Layout:
    """Layout for a test run: unqualified names, parquet, a temp landing zone."""
    return Layout(fmt="parquet", raw_volume=tmp, qualified=False)


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or "SPARK_HOME" not in os.environ and _has_dbutils()


def _has_dbutils() -> bool:
    try:                                  # pragma: no cover - environment probe
        import IPython

        return IPython.get_ipython() is not None and "dbutils" in IPython.get_ipython().user_ns
    except Exception:
        return False


def get_spark(app: str = "bfsi-lending-lakehouse") -> SparkSession:
    """Return the ambient session on Databricks, or build a small local one.

    `getActiveSession` is checked first because on Databricks the platform has
    already created the session and building another one is either ignored or an
    error, depending on the runtime.
    """
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    return (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        # 200 shuffle partitions against a few thousand test rows is all
        # scheduling overhead and no work.
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        # ANSI mode is the Spark 4 default and is what Databricks runs, so the
        # tests must fail the same way production would on a bad cast.
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def write_table(df, layout: Layout, layer: str, name: str, mode: str = "overwrite",
                partition_by: list[str] | None = None):
    """Write one table, honouring the layout's format.

    Deliberately does NOT partition on Databricks: liquid clustering is the
    current recommendation and is incompatible with both partitioning and
    ZORDER, and at this project's scale (single-digit GB) partitioning would
    only produce small files.

    `overwriteSchema` is set for full-refresh Delta writes, and the reason is
    worth stating because the default bit everyone at least once. Delta's
    `mode("overwrite")` replaces the *data* and keeps the *schema*, so the first
    run after a gold query changes shape fails with

        [DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing
        to the Delta table.

    which is exactly what happened here when the portfolio summary went from two
    GNPA columns to one. For a table whose schema is entirely defined by the
    query that produces it, replacing the schema is the correct semantics for a
    full refresh.

    It is emphatically NOT correct for the incremental path. `upsert.py` merges
    into silver and wants `mergeSchema` -- additive evolution -- so that a new
    column upstream flows through without dropping the columns already there.
    Replacing a schema you meant to evolve silently deletes data.
    """
    target = layout.table(layer, name)
    writer = df.write.format(layout.fmt).mode(mode)
    if layout.fmt == "delta" and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by and layout.fmt != "delta":
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target)
    return target
