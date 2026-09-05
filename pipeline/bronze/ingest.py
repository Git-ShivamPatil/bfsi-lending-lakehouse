"""Bronze: land the source extracts verbatim, defects included.

The one rule of this layer is that it does not clean anything. Every column is
read as a string and every row is kept, because a regulatory pipeline has to be
able to answer "what exactly did the source send us, and when" months later --
and because a load that rejects bad rows at the door destroys the evidence you
need to tell the source system it is broken.

Typing, de-duplication and quarantine all happen in silver, where the decision is
recorded rather than implied.
"""

from __future__ import annotations

import argparse
from pathlib import PurePosixPath

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from ..common import Layout, get_spark, write_table

#: Bronze reads everything as a string on purpose. Declaring the columns (rather
#: than inferring) means a source that silently adds, drops or reorders a column
#: shows up as a schema mismatch instead of a column of nulls.
SOURCES: dict[str, list[str]] = {
    "merchants": [
        "merchant_id", "merchant_name", "category", "city", "city_tier",
        "pincode", "onboarded_at",
    ],
    "customers": [
        "customer_id", "city", "city_tier", "pincode", "age_band", "score_band",
        "bureau_score", "kyc_status", "created_at",
    ],
    "loans": [
        "loan_id", "customer_id", "merchant_id", "product", "principal",
        "tenure_months", "apr", "subvention_pct", "disbursed_at",
    ],
    "emi_schedule": [
        "loan_id", "instalment_no", "due_date", "emi_amount",
        "principal_component", "interest_component",
    ],
    "repayment_attempts": [
        "attempt_id", "loan_id", "instalment_no", "due_date", "amount", "mode",
        "status", "bounce_reason", "paid_at", "attempted_at",
    ],
}


def _schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in columns])


def read_source(spark: SparkSession, root: str, name: str) -> DataFrame:
    path = str(PurePosixPath(root) / f"{name}.csv")
    df = (
        spark.read.option("header", "true")
        # Anything that does not fit the declared schema lands in _corrupt_record
        # rather than being silently dropped.
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(_schema(SOURCES[name] + ["_corrupt_record"]))
        .csv(path)
    )
    return df


def add_lineage(df: DataFrame, batch_id: str, generator_version: str) -> DataFrame:
    """Stamp every row with where it came from and when it arrived.

    Not cited to anything, because it does not need to be: a pipeline that has to
    defend a number to a regulator six months after it was filed needs to be able
    to say which file the number came from, which batch loaded it and which
    version of the producer wrote it. The four columns below are the cheapest
    possible version of that, and they cost one pass over data already in memory.
    """
    return (
        # `_metadata.file_path`, not `input_file_name()`. The latter was removed
        # in DBR 17.3 LTS, so the old call would have failed on exactly the
        # platform this project targets. The hidden `_metadata` column is the
        # supported replacement and is available on any file-based read in
        # Spark 3.3+, so it also works unchanged on the CI matrix.
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_generator_version", F.lit(generator_version))
    )


def ingest_all(spark: SparkSession, layout: Layout, root: str, batch_id: str,
               generator_version: str = "unknown") -> dict[str, int]:
    counts = {}
    for name in SOURCES:
        df = add_lineage(read_source(spark, root, name), batch_id, generator_version)
        write_table(df, layout, "bronze", name, mode="overwrite")
        counts[name] = df.count()
    return counts


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw", default=None, help="landing zone; defaults to the layout volume")
    ap.add_argument("--batch-id", required=True)
    ap.add_argument("--generator-version", default="unknown")
    args = ap.parse_args(argv)

    spark = get_spark()
    layout = Layout()
    counts = ingest_all(spark, layout, args.raw or layout.raw_volume,
                        args.batch_id, args.generator_version)
    for name, n in counts.items():
        print(f"bronze_{name}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
