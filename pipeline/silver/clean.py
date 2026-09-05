"""Silver: type the raw strings, apply the rule repository, quarantine the rest.

Three things happen here, in this order, and the order matters:

1. **Cast with `try_cast`.** ANSI mode is on -- it is the Spark 4 and Databricks
   default -- so a plain CAST of 'abc' to DOUBLE raises instead of returning
   NULL, and one bad row would fail the whole job. `try_cast` returns NULL
   instead, which turns a crash into a data-quality finding that the rules below
   can catch and attribute.

2. **Apply the rules.** Row-level, then uniqueness, then referential. Anything
   failing a REJECT rule is written to a quarantine table with the rule it broke,
   never silently dropped: an unexplained row-count difference between bronze and
   silver is the thing that destroys trust in a pipeline.

3. **De-duplicate deterministically.** A source that sends the same natural key
   twice makes `MERGE INTO` non-deterministic -- Delta raises rather than picking
   a winner for you. Collapsing duplicates with an explicit, ordered
   `row_number()` before the merge is the fix, and the ordering has to be stable
   or two runs disagree.
"""

from __future__ import annotations

import argparse

from pyspark.sql import Column, DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from validation import rules as R

from ..common import Layout, get_spark, write_table

#: Target types per entity. Columns absent from a map stay strings.
CASTS: dict[str, dict[str, str]] = {
    "customers": {"bureau_score": "INT", "created_at": "DATE"},
    "applications": {
        "applied_at": "DATE", "cart_amount": "DOUBLE", "tenure_months": "INT",
    },
    "merchants": {"onboarded_at": "DATE"},
    "loans": {
        "principal": "DOUBLE", "tenure_months": "INT", "apr": "DOUBLE",
        "subvention_pct": "DOUBLE", "disbursed_at": "DATE",
        "cart_amount": "DOUBLE", "down_payment": "DOUBLE",
    },
    "emi_schedule": {
        "instalment_no": "INT", "due_date": "DATE", "emi_amount": "DOUBLE",
        "principal_component": "DOUBLE", "interest_component": "DOUBLE",
    },
    "repayment_attempts": {
        "instalment_no": "INT", "due_date": "DATE", "amount": "DOUBLE",
        "paid_at": "DATE", "attempted_at": "DATE",
    },
}

#: Tie-breaker used when collapsing duplicate natural keys, per entity. Stable
#: ordering is what makes the de-duplication reproducible run to run.
DEDUPE_ORDER: dict[str, list[str]] = {
    "applications": ["applied_at"],
    "loans": ["disbursed_at"],
    "emi_schedule": ["due_date"],
    "repayment_attempts": ["attempted_at", "status"],
}


def typed(df: DataFrame, entity: str) -> DataFrame:
    """Cast declared columns, tolerating garbage rather than raising on it."""
    out = df
    for col, target in CASTS.get(entity, {}).items():
        if col in out.columns:
            out = out.withColumn(col, F.expr(f"try_cast({col} AS {target})"))
    return out


def _failed_rule_ids(rules: tuple[R.Rule, ...]) -> Column:
    """Array of the rule ids this row breaks.

    A rule whose expression evaluates to NULL is treated as failed: an
    unevaluable check is not a passing check.
    """
    checks = [
        F.when(~F.coalesce(F.expr(r.expression), F.lit(False)), F.lit(r.rule_id))
        for r in rules
    ]
    if not checks:
        return F.array().cast("array<string>")
    return F.array_compact(F.array(*checks))


def apply_row_rules(df: DataFrame, entity: str, reporting_date: str) -> DataFrame:
    row_rules = R.for_entity(entity, "ROW")
    return (
        df.withColumn("reporting_date", F.lit(reporting_date).cast("date"))
        .withColumn("_failed_rules", _failed_rule_ids(row_rules))
    )


def apply_uniqueness(df: DataFrame, entity: str) -> DataFrame:
    """Flag every row after the first for each natural key.

    `row_number()` rather than `dropDuplicates`, because dropDuplicates gives no
    account of what it removed and picks an arbitrary survivor.
    """
    out = df
    for rule in R.for_entity(entity, "UNIQUENESS"):
        order = [F.col(c).asc_nulls_last() for c in DEDUPE_ORDER.get(entity, [])]
        order.append(F.monotonically_increasing_id().asc())
        w = Window.partitionBy(*[F.col(c) for c in rule.unique_key]).orderBy(*order)
        out = (
            out.withColumn("_rn", F.row_number().over(w))
            .withColumn(
                "_failed_rules",
                F.when(F.col("_rn") > 1,
                       F.array_union(F.col("_failed_rules"), F.array(F.lit(rule.rule_id))))
                .otherwise(F.col("_failed_rules")),
            )
            .drop("_rn")
        )
    return out


def apply_referential(df: DataFrame, entity: str, parents: dict[str, DataFrame]) -> DataFrame:
    """Left-anti semantics expressed as a broadcast left join, so the failing
    rows stay in the frame and can be attributed rather than vanishing."""
    out = df
    for rule in R.for_entity(entity, "REFERENTIAL"):
        child_col, parent_entity, parent_col = rule.references
        parent = parents.get(parent_entity)
        if parent is None:
            continue
        keys = parent.select(F.col(parent_col).alias("_pk")).distinct()
        out = (
            out.join(F.broadcast(keys), out[child_col] == F.col("_pk"), "left")
            .withColumn(
                "_failed_rules",
                F.when(F.col("_pk").isNull() & F.col(child_col).isNotNull(),
                       F.array_union(F.col("_failed_rules"), F.array(F.lit(rule.rule_id))))
                .otherwise(F.col("_failed_rules")),
            )
            .drop("_pk")
        )
    return out


def split(df: DataFrame, entity: str) -> tuple[DataFrame, DataFrame]:
    """Partition into (clean, quarantined) using rule severity.

    WARN findings ride along on the clean row so the gold layer can report them;
    only REJECT findings pull a row out of the book.
    """
    reject_ids = [r.rule_id for r in R.for_entity(entity) if r.severity == "REJECT"]
    rejected = F.size(F.array_intersect(
        F.col("_failed_rules"), F.array(*[F.lit(i) for i in reject_ids]))) > 0

    clean = df.filter(~rejected).withColumnRenamed("_failed_rules", "_warnings")
    quarantine = (
        df.filter(rejected)
        .withColumn("_entity", F.lit(entity))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    return clean, quarantine


def build(spark: SparkSession, layout: Layout, reporting_date: str) -> dict[str, int]:
    """Run every entity through the pipeline and write silver + quarantine."""
    bronze = {e: spark.table(layout.table("bronze", e)) for e in R.entities()}
    typed_frames = {e: typed(df, e) for e, df in bronze.items()}

    # Parents must be cleaned before children can be checked against them,
    # otherwise a child row is validated against a customer that is itself about
    # to be quarantined.
    order = ["customers", "merchants", "applications", "loans",
             "emi_schedule", "repayment_attempts"]
    clean_frames: dict[str, DataFrame] = {}
    quarantines = []
    counts: dict[str, int] = {}

    for entity in order:
        df = typed_frames[entity]
        df = apply_row_rules(df, entity, reporting_date)
        df = apply_uniqueness(df, entity)
        df = apply_referential(df, entity, clean_frames)
        clean, bad = split(df, entity)

        clean = clean.drop("_corrupt_record")
        write_table(clean, layout, "silver", entity)

        # Read the parent back from the table that was just written, rather than
        # caching the unmaterialised frame. Two reasons, and the second is the
        # load-bearing one:
        #
        #   * a child's referential check should run against the rows that were
        #     actually persisted, not against a plan that might still be
        #     recomputed differently; and
        #   * `.cache()` raises on Databricks serverless, which is all Free
        #     Edition offers -- the DataFrame and SQL caching APIs are not
        #     available there. Caching here would have made this pipeline
        #     impossible to run on the one platform it is written for.
        clean_frames[entity] = spark.table(layout.table("silver", entity))
        counts[entity] = clean_frames[entity].count()

        # `_corrupt_record` rides along into the quarantine rather than being
        # dropped with the clean frame. It is the raw text of a row the CSV
        # reader could not fit to the declared schema, and it is the only
        # evidence of what the source actually sent -- which is exactly what
        # someone needs six months later to tell an upstream team their extract
        # is broken. A quarantine that records the rule but not the row is an
        # audit trail with the interesting half missing.
        quarantines.append(
            bad.select("_entity", "_quarantined_at",
                       F.explode("_failed_rules").alias("rule_id"),
                       F.col("_batch_id"),
                       F.col("_corrupt_record"))
        )

    q = quarantines[0]
    for extra in quarantines[1:]:
        q = q.unionByName(extra)
    write_table(q, layout, "silver", "quarantine")
    counts["quarantine"] = q.count()
    return counts


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--reporting-date", required=True)
    args = ap.parse_args(argv)

    spark = get_spark()
    counts = build(spark, Layout(), args.reporting_date)
    for name, n in counts.items():
        print(f"silver_{name}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
