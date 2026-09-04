# Databricks notebook source
# MAGIC %md
# MAGIC # BFSI Lending Lakehouse — end-to-end run
# MAGIC
# MAGIC Runs bronze → silver → snapshot → gold on **Databricks Free Edition**.
# MAGIC
# MAGIC **Before you run this**, once per workspace:
# MAGIC
# MAGIC 1. Add this repo under **Workspace → Git folders → Add Git folder**
# MAGIC    (included in Free Edition).
# MAGIC 2. Create the landing volume — cell below. The catalog and schema are
# MAGIC    `workspace.default`, which Free Edition already provides.
# MAGIC 3. Get the generated CSVs into the volume. Free Edition is serverless-only
# MAGIC    and has no account console, so there is no way to mint the storage
# MAGIC    credential an external location needs: the workspace **cannot read an
# MAGIC    external S3 bucket**. Push instead — `.github/workflows/databricks.yml`
# MAGIC    does it through the Files API, or upload by hand from Catalog Explorer.
# MAGIC
# MAGIC Free Edition gives one 2X-Small SQL warehouse, five concurrent job tasks
# MAGIC and Spark Connect APIs only — no RDD APIs, no Scala, no JARs. Everything
# MAGIC here stays inside that envelope.

# COMMAND ----------

# `workspace.default` is pre-provisioned on Free Edition. Do NOT create a
# catalog of your own here: CREATE CATALOG has a reported failure mode on Free
# Edition ("Metastore storage root URL does not exist. Default Storage is
# enabled in your account..."), and there is nothing this project needs that a
# separate catalog would give it.
CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "raw"

REPORTING_DATE = "2026-08-31"
WINDOW_START = "2024-09-30"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

RAW = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print("landing zone:", RAW)
display(dbutils.fs.ls(RAW))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate the book
# MAGIC
# MAGIC The generator is pure standard library, so it runs on the driver with
# MAGIC nothing installed. Skip this cell if the CSVs are already in the volume.

# COMMAND ----------

import sys

sys.path.insert(0, "..")  # repo root, when run from notebooks/

from generator.generate import main as generate

generate([
    "--loans", "150000",
    "--merchants", "1400",
    "--months", "24",
    "--seed", "42",
    "--as-of", REPORTING_DATE,
    "--out", RAW,
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — land the extracts verbatim

# COMMAND ----------

from pipeline.bronze.ingest import ingest_all
from pipeline.common import Layout

layout = Layout(catalog=CATALOG, schema=SCHEMA, fmt="delta", raw_volume=RAW)

counts = ingest_all(spark, layout, RAW, batch_id=REPORTING_DATE,
                    generator_version="1.0.0")
for name, n in counts.items():
    print(f"bronze_{name}: {n:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — type, validate, quarantine

# COMMAND ----------

from pipeline.silver.clean import build as build_silver

for name, n in build_silver(spark, layout, REPORTING_DATE).items():
    print(f"silver_{name}: {n:,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What the rule repository caught, by dimension. This is the table to
# MAGIC -- open first: an empty quarantine means the rules are not running.
# MAGIC SELECT rule_id, COUNT(*) AS findings
# MAGIC FROM   workspace.default.silver_quarantine
# MAGIC GROUP  BY rule_id
# MAGIC ORDER  BY findings DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot — the day-end process
# MAGIC
# MAGIC Stamped with the calendar date the run is *for*, never `current_date()`,
# MAGIC so re-running a historical date reproduces the historical answer.

# COMMAND ----------

from pipeline.silver.snapshot import build as build_snapshot

snap = build_snapshot(spark, layout, WINDOW_START, REPORTING_DATE,
                      write_off_dpd=180)
print(f"silver_loan_snapshot: {snap.count():,} loan-date rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

from pipeline.gold.metrics import build as build_gold

for name, n in build_gold(spark, layout).items():
    print(f"gold_{name}: {n:,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_date, live_loans, principal_outstanding,
# MAGIC        gnpa_ratio, par_30, par_90
# MAGIC FROM   workspace.default.gold_portfolio_summary
# MAGIC ORDER  BY snapshot_date DESC
# MAGIC LIMIT  12

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Roll-rate matrix for the latest month end. Pivoted here rather than in
# MAGIC -- the gold table, because the long form is what a BI tool wants.
# MAGIC SELECT from_bucket,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = 'CURRENT' THEN roll_rate END), 4) AS to_current,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = '1-30'    THEN roll_rate END), 4) AS to_1_30,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = '31-60'   THEN roll_rate END), 4) AS to_31_60,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = '61-90'   THEN roll_rate END), 4) AS to_61_90,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = '90+'     THEN roll_rate END), 4) AS to_90_plus,
# MAGIC        ROUND(SUM(CASE WHEN to_bucket = 'CLOSED'  THEN roll_rate END), 4) AS closed
# MAGIC FROM   workspace.default.gold_roll_rate
# MAGIC WHERE  from_date = (SELECT MAX(from_date) FROM workspace.default.gold_roll_rate)
# MAGIC GROUP  BY from_bucket
# MAGIC ORDER  BY from_bucket

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional — liquid clustering
# MAGIC
# MAGIC Not partitioning is deliberate. Liquid clustering is the current
# MAGIC Databricks recommendation and is incompatible with both partitioning and
# MAGIC `ZORDER`; at this project's scale partitioning would only produce small
# MAGIC files. Clustering the snapshot on its two most-filtered columns is worth
# MAGIC it once the table is large.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.silver_loan_snapshot
# MAGIC   CLUSTER BY (snapshot_date, dpd_bucket);
# MAGIC OPTIMIZE workspace.default.silver_loan_snapshot;
