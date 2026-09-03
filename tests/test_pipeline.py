"""The medallion pipeline, driven end to end through temp views.

Nothing here writes a table. The transformations are pure DataFrame functions and
the gold layer is plain SQL over views, so the suite exercises the real logic
without needing a warehouse directory -- which keeps it runnable on any machine
and in CI.

The test that matters most is `test_gold_gnpa_matches_the_independent_backtest`:
the Spark pipeline and the pure-Python backtest implement the same definitions
twice, independently, and are required to agree. One of them being wrong is far
more likely than both being wrong the same way.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from pipeline.gold import metrics as G
from pipeline.silver import clean as S
from pipeline.silver import snapshot as SN
from validation import rules as R
from validation.backtest import report

REPORTING_DATE = "2026-08-31"


@pytest.fixture(scope="session")
def silver(spark, raw_views):
    """Run bronze views through the silver rule engine, in dependency order."""
    order = ["customers", "merchants", "loans", "emi_schedule", "repayment_attempts"]
    clean_frames = {}
    quarantines = {}

    for entity in order:
        df = S.typed(spark.table(f"bronze_{entity}"), entity)
        df = S.apply_row_rules(df, entity, REPORTING_DATE)
        df = S.apply_uniqueness(df, entity)
        df = S.apply_referential(df, entity, clean_frames)
        good, bad = S.split(df, entity)
        good = good.drop("_corrupt_record").cache()
        clean_frames[entity] = good
        quarantines[entity] = bad.cache()
        good.createOrReplaceTempView(f"silver_{entity}")

    return clean_frames, quarantines


@pytest.fixture(scope="session")
def snapshot(spark, silver):
    SN.month_ends(spark, "2024-09-30", REPORTING_DATE).createOrReplaceTempView(
        "snapshot_dates")
    df = spark.sql(SN.SNAPSHOT_SQL.format(write_off_dpd=180)).cache()
    df.createOrReplaceTempView("silver_loan_snapshot")
    return df


# ---------------------------------------------------------------------------
# silver
# ---------------------------------------------------------------------------


def test_try_cast_turns_bad_numerics_into_nulls_not_a_crash(spark):
    """Under ANSI mode a plain CAST raises. The pipeline must survive garbage."""
    df = spark.createDataFrame(
        [("L1", "12000.50"), ("L2", "not-a-number"), ("L3", "")],
        "loan_id string, principal string")
    out = S.typed(df, "loans").collect()

    assert out[0]["principal"] == pytest.approx(12000.50)
    assert out[1]["principal"] is None
    assert out[2]["principal"] is None


def test_every_rule_expression_compiles(spark, silver):
    """A typo in a rule expression must fail here, not at 3am in a job run."""
    frames, _ = silver
    for entity in R.entities():
        df = frames[entity].withColumn(
            "reporting_date", F.lit(REPORTING_DATE).cast("date"))
        for rule in R.for_entity(entity, "ROW"):
            df.select(F.expr(rule.expression)).limit(1).collect()


def test_negative_principal_is_quarantined_not_silently_dropped(silver):
    frames, quarantines = silver
    q = quarantines["loans"]

    rejected = q.filter(F.array_contains("_failed_rules", "LOAN_002"))
    assert rejected.count() > 0, "the generator injects negative principals"
    assert frames["loans"].filter("principal <= 0").count() == 0


def test_orphan_repayments_are_caught_by_the_referential_rule(silver):
    _, quarantines = silver
    orphans = quarantines["repayment_attempts"].filter(
        F.array_contains("_failed_rules", "REPAY_007"))
    assert orphans.count() > 0


def test_duplicate_attempt_ids_collapse_to_one_surviving_row(silver):
    """The case that makes a naive MERGE INTO non-deterministic."""
    frames, quarantines = silver

    dupes = quarantines["repayment_attempts"].filter(
        F.array_contains("_failed_rules", "REPAY_008"))
    assert dupes.count() > 0, "the generator injects duplicate repayment rows"

    surviving = frames["repayment_attempts"]
    assert surviving.count() == surviving.select("attempt_id").distinct().count()


def test_emi_components_must_reconcile(silver):
    frames, quarantines = silver
    breaks = quarantines["emi_schedule"].filter(
        F.array_contains("_failed_rules", "SCHED_003"))
    assert breaks.count() > 0

    survivors = frames["emi_schedule"].filter(
        "abs(emi_amount - (principal_component + interest_component)) > 0.05")
    assert survivors.count() == 0


def test_warnings_ride_along_rather_than_removing_the_row(silver):
    """A missing pincode is a WARN: the loan still counts toward the book."""
    frames, _ = silver
    warned = frames["customers"].filter(F.array_contains("_warnings", "CUST_002"))
    assert warned.count() > 0
    assert warned.filter("pincode IS NOT NULL AND pincode <> ''").count() == 0


# ---------------------------------------------------------------------------
# snapshot
# ---------------------------------------------------------------------------


def test_snapshot_dpd_and_classification_agree(snapshot):
    """SMA staging must be a pure function of DPD, with no overlap or gap."""
    mismatches = snapshot.filter("""
        NOT (
            (dpd = 0                  AND asset_classification = 'STANDARD') OR
            (dpd BETWEEN 1  AND 30    AND asset_classification = 'SMA-0')    OR
            (dpd BETWEEN 31 AND 60    AND asset_classification = 'SMA-1')    OR
            (dpd BETWEEN 61 AND 90    AND asset_classification = 'SMA-2')    OR
            (dpd > 90                 AND asset_classification = 'NPA')
        )
    """)
    assert mismatches.count() == 0


def test_snapshot_is_reproducible_for_a_historical_date(spark, snapshot):
    """Re-stamping an old date must give the same answer -- the day-end process
    is stamped with the date it is run *for*, not the date it is run *on*."""
    def measure(df):
        return (df.filter("snapshot_date = DATE'2026-03-31'")
                .agg(F.sum("principal_outstanding").alias("os"),
                     # Summing a boolean is not valid under ANSI mode; the
                     # predicate has to become an integer first.
                     F.sum(F.when(F.col("dpd") > 90, 1).otherwise(0)).alias("npa_loans"))
                .collect()[0])

    first = measure(snapshot)
    again = measure(spark.sql(SN.SNAPSHOT_SQL.format(write_off_dpd=180)))

    assert first["os"] == pytest.approx(again["os"], rel=1e-9)
    assert first["npa_loans"] == again["npa_loans"]


def test_outstanding_never_exceeds_disbursed_principal(snapshot):
    assert snapshot.filter("principal_outstanding > principal + 0.01").count() == 0


# ---------------------------------------------------------------------------
# gold
# ---------------------------------------------------------------------------


def test_roll_rates_sum_to_one_within_each_origin_bucket(spark, snapshot):
    roll = spark.sql(G.ROLL_RATE_SQL)
    off = (roll.groupBy("from_date", "from_bucket")
           .agg(F.sum("roll_rate").alias("total"))
           # roll_rate is ROUNDed to 5dp for readability, so a partition with n
           # destination buckets can drift by up to n * 5e-6. The tolerance has
           # to exceed that or the test is measuring the rounding, not the maths.
           .filter("abs(total - 1.0) > 1e-4"))
    assert off.count() == 0


def test_vintage_curve_is_monotonic_within_every_cohort(spark, snapshot):
    """Cumulative 'ever 30+' cannot fall as a cohort ages."""
    vintage = spark.sql(G.VINTAGE_SQL)
    w = "PARTITION BY cohort_month ORDER BY months_on_book"
    regressions = (
        vintage.withColumn("prev", F.expr(f"LAG(rate_30plus) OVER ({w})"))
        .filter("prev IS NOT NULL AND rate_30plus < prev - 1e-9")
    )
    assert regressions.count() == 0


def test_gold_gnpa_matches_the_independent_backtest(spark, snapshot, raw_views):
    """Two implementations, one definition. They have to agree.

    The Spark path goes bronze -> rules -> snapshot -> gold SQL. The backtest is
    a few hundred lines of pure Python over the same CSVs. Agreement to within a
    few basis points is the strongest evidence either of them is right.
    """
    spark_gnpa = (spark.sql(G.PORTFOLIO_SQL)
                  .filter(f"snapshot_date = DATE'{REPORTING_DATE}'")
                  .collect()[0]["gnpa_ratio"])
    python_gnpa = report(raw_views, date.fromisoformat(REPORTING_DATE))["gnpa_pct"]

    assert spark_gnpa == pytest.approx(python_gnpa, abs=0.002), (
        f"Spark says GNPA {spark_gnpa:.4%}, the independent backtest says "
        f"{python_gnpa:.4%} -- one of the two definitions has drifted")


def test_dq_scorecard_covers_every_rule_that_fired(spark, silver):
    G.rules_dim(spark).createOrReplaceTempView("rules_dim")

    _, quarantines = silver
    q = None
    for entity, df in quarantines.items():
        part = df.select(F.lit(entity).alias("_entity"),
                         F.current_timestamp().alias("_quarantined_at"),
                         F.explode("_failed_rules").alias("rule_id"),
                         F.col("_batch_id"))
        q = part if q is None else q.unionByName(part)
    q.createOrReplaceTempView("silver_quarantine")

    scorecard = spark.sql(G.DQ_SQL)
    assert scorecard.count() > 0
    # Every finding must resolve to a rule with a description -- an unjoinable
    # rule id means the repository and the pipeline have drifted apart.
    assert scorecard.filter("description IS NULL").count() == 0
    assert set(r["dimension"] for r in scorecard.collect()) <= {
        "ACCURACY", "COMPLETENESS", "TIMELINESS", "CONSISTENCY"}
