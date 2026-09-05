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
    order = ["customers", "merchants", "applications", "loans",
             "emi_schedule", "repayment_attempts"]
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
    df = spark.sql(SN.snapshot_sql(180)).cache()
    df.createOrReplaceTempView("silver_loan_snapshot")
    return df


@pytest.fixture
def restores_the_silver_views(spark, silver):
    """Let one test replace the silver temp views, then put them back.

    `SNAPSHOT_SQL` reads views by fixed name, so a test that needs a hand-built
    book of its own has no choice but to clobber the session-scoped ones. If it
    does not restore them, every later test that re-runs the snapshot query
    silently reads the three-row fixture instead of the generated book -- which
    is precisely what happened the first time this was written, and it surfaced
    as a reproducibility failure in a completely unrelated test.
    """
    yield
    frames, _ = silver
    for entity, df in frames.items():
        df.createOrReplaceTempView(f"silver_{entity}")
    SN.month_ends(spark, "2024-09-30", REPORTING_DATE).createOrReplaceTempView(
        "snapshot_dates")


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


def test_snapshot_classification_partitions_dpd_with_no_gap_or_overlap(snapshot):
    """Classification is a pure function of DPD and the threshold in force.

    SMA bands are the single-column NBFC table at para 18 of the RBI (NBFC -
    Resolution of Stressed Assets) Directions, 2025: up to 30 days, more than 30
    and up to 60, more than 60 and up to 90.

    NPA is compared against `npa_dpd_threshold` rather than a literal 90,
    because for a Base Layer NBFC that threshold moved: 150 days from 31 Mar
    2024, 120 from 31 Mar 2025, 90 from 31 Mar 2026 (IRACP Directions, 2025,
    paras 43-44). NPA is tested before the SMA bands, so an account past the
    threshold is an NPA regardless of where it falls in the SMA table.
    """
    mismatches = snapshot.filter("""
        NOT (
            (dpd = 0                               AND asset_classification = 'STANDARD') OR
            (dpd BETWEEN 1 AND 30                  AND asset_classification = 'SMA-0')    OR
            (dpd BETWEEN 31 AND 60                 AND asset_classification = 'SMA-1')    OR
            (dpd > 60 AND dpd <= npa_dpd_threshold AND asset_classification = 'SMA-2')    OR
            (dpd > npa_dpd_threshold               AND asset_classification = 'NPA')
        )
    """)
    assert mismatches.count() == 0
    assert snapshot.filter("asset_classification = 'SMA-0'").count() > 0


def test_the_npa_threshold_follows_the_base_layer_glide_path(snapshot):
    """The threshold in force has to move with the calendar, not with today.

    A synthetic book spanning 2024 to 2026 that stamps NPA at 90 days throughout
    is applying a rule that had not yet come into force for a Base Layer NBFC
    over most of its own window. This asserts the three steps actually appear in
    the snapshot, keyed off the snapshot date rather than the run date.
    """
    by_date = {r["snapshot_date"]: r["npa_dpd_threshold"]
               for r in snapshot.select("snapshot_date", "npa_dpd_threshold")
               .distinct().collect()}

    assert by_date[date(2024, 9, 30)] == 150, "150-day norm applied from 31 Mar 2024"
    assert by_date[date(2025, 3, 31)] == 120, "120-day norm from 31 Mar 2025"
    assert by_date[date(2026, 2, 28)] == 120, "still 120 the month before the step"
    assert by_date[date(2026, 3, 31)] == 90, "90-day norm from 31 Mar 2026"
    assert by_date[date(2026, 8, 31)] == 90

    # And the classification actually uses it: before 31 Mar 2026 there must
    # exist accounts past 90 DPD that are still not NPA, or the glide path is
    # being computed and then ignored.
    lenient = snapshot.filter(
        "snapshot_date < DATE'2026-03-31' AND dpd > 90 "
        "AND asset_classification <> 'NPA'")
    assert lenient.count() > 0, (
        "no account sits above 90 DPD without being an NPA before the 90-day "
        "norm applied -- the threshold column is not reaching the CASE")


def test_partial_payment_does_not_upgrade_the_account(spark, restores_the_silver_views):
    """RBI (NBFC - IRACP) Directions, 2025, para 24: upgrade only on full arrears.

        Loan accounts classified as NPAs may be upgraded as 'standard' asset
        only if entire arrears of interest and principal are paid by the
        borrower.

    Built as a hand-made three-instalment loan rather than drawn from the
    generated book, because the property needs a specific shape: instalment one
    unpaid, instalments two and three settled. A borrower who has paid two of
    three instalments has paid most of what is owed and is still not standard,
    and DPD still runs from the *oldest* unpaid due date rather than the most
    recent one.

    This falls out of the arrears anchor rather than being coded as a rule,
    which is exactly why it needs a test -- nothing in the SQL says "upgrade",
    so a well-meaning change from MIN to MAX would silently grant amnesty to
    every partially-paying account in the book.
    """
    spark.createDataFrame(
        [("L1", "C1", "M1", "NO_COST_EMI", 3000.0, 3, date(2026, 1, 15))],
        "loan_id string, customer_id string, merchant_id string, product string, "
        "principal double, tenure_months int, disbursed_at date",
    ).createOrReplaceTempView("silver_loans")

    spark.createDataFrame(
        [("L1", 1, date(2026, 2, 15), 1000.0, 1000.0),
         ("L1", 2, date(2026, 3, 15), 1000.0, 1000.0),
         ("L1", 3, date(2026, 4, 15), 1000.0, 1000.0)],
        "loan_id string, instalment_no int, due_date date, emi_amount double, "
        "principal_component double",
    ).createOrReplaceTempView("silver_emi_schedule")

    # Instalments 2 and 3 settled on time. Instalment 1 never paid.
    spark.createDataFrame(
        [("A2", "L1", 2, date(2026, 3, 15), 1000.0, "SUCCESS", date(2026, 3, 15)),
         ("A3", "L1", 3, date(2026, 4, 15), 1000.0, "SUCCESS", date(2026, 4, 15))],
        "attempt_id string, loan_id string, instalment_no int, due_date date, "
        "amount double, status string, paid_at date",
    ).createOrReplaceTempView("silver_repayment_attempts")

    SN.month_ends(spark, "2026-02-28", "2026-06-30").createOrReplaceTempView(
        "snapshot_dates")
    rows = {r["snapshot_date"]: r
            for r in spark.sql(SN.snapshot_sql(180)).collect()}

    # 31 May: instalment 1 is 105 days overdue even though two later
    # instalments have been settled in full.
    may = rows[date(2026, 5, 31)]
    assert may["oldest_unpaid_due"] == date(2026, 2, 15)
    assert may["dpd"] == 105
    assert may["asset_classification"] == "NPA"

    # And it does not drift back down the ladder as later instalments settle.
    for snap in (date(2026, 4, 30), date(2026, 5, 31), date(2026, 6, 30)):
        assert rows[snap]["oldest_unpaid_due"] == date(2026, 2, 15), (
            f"{snap}: the arrears anchor moved off the oldest unpaid instalment, "
            f"which would upgrade an account that has not cleared its arrears")


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
    again = measure(spark.sql(SN.snapshot_sql(180)))

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
    """Two implementations, two definitions, four numbers that have to agree.

    The Spark path goes bronze -> rules -> snapshot -> gold SQL. The backtest is
    a few hundred lines of pure Python over the same CSVs. Agreement to within a
    few basis points is the strongest evidence either of them is right.

    Both GNPA measures are checked. The write-off-inclusive one is the harder of
    the two to get right, because it depends on reconstructing *when* each
    account was written off, and the two implementations derive that date by
    different routes -- SQL `DATE_ADD` off the arrears anchor on one side,
    `timedelta` arithmetic off the observed DPD on the other.
    """
    import generator.config as C

    row = (spark.sql(G.PORTFOLIO_SQL.format(
                lookback=C.GNPA_WRITE_OFF_LOOKBACK_DAYS))
           .filter(f"snapshot_date = DATE'{REPORTING_DATE}'")
           .collect()[0])
    py = report(raw_views, date.fromisoformat(REPORTING_DATE))

    assert row["gnpa_ratio"] == pytest.approx(py["gnpa_pct"], abs=0.002), (
        f"GNPA: Spark says {row['gnpa_ratio']:.4%}, the independent backtest "
        f"says {py['gnpa_pct']:.4%} -- the definitions have drifted")

    # The write-off window has to be matching something, or the write-off
    # reconstruction could be silently returning nothing and nobody would know.
    assert row["wo_loans_in_window"] > 0, "no write-offs in the trailing window"
    assert row["write_off_principal_in_window"] > 0


def test_the_funnel_rates_are_rates(spark, silver):
    """Approval and conversion must be bounded, and the funnel must narrow.

    The interesting line in FUNNEL_SQL is the denominator: approval rate is
    stated on *decisioned* applications rather than on all applications
    received. Where a pipeline has a decisioning lag, counting undecided
    applications as implicit rejections makes the most recent month look worse
    and gets read as a policy tightening that never happened.
    """
    rows = spark.sql(G.FUNNEL_SQL).collect()
    assert rows, "no applications reached the funnel query"

    for r in rows:
        assert r["applications"] >= r["approved"] >= r["converted"], (
            f"{r['month']} {r['channel']}: the funnel widens, which is not a funnel")
        for col in ("approval_rate", "conversion_of_approved", "application_to_loan"):
            assert 0.0 <= r[col] <= 1.0, f"{col} is not a rate: {r[col]}"
        assert r["avg_cart_amount"] > 0


def test_first_payment_default_uses_null_safe_anti_join(spark, silver, snapshot):
    """FPD must survive an orphan repayment row with a NULL loan_id.

    The generator injects orphan repayments on purpose. `NOT IN` against a
    subquery containing a single NULL returns no rows at all, which reports a
    clean 0% first-payment default across the entire book -- a number nobody
    questions, because a low FPD is what everyone hopes to see. FPD_SQL uses
    NOT EXISTS, and this asserts the answer is neither zero nor everything.
    """
    rows = spark.sql(G.FPD_SQL).collect()
    assert rows, "no merchant cleared the volume floor"

    total_loans = sum(r["loans"] for r in rows)
    total_fpd = sum(r["fpd_loans"] for r in rows)
    rate = total_fpd / total_loans

    assert 0.0 < rate < 0.5, (
        f"book-wide first-payment default is {rate:.4%}, which is either zero "
        f"(the NOT IN null trap) or implausibly high")
    for r in rows:
        assert 0 <= r["fpd_loans"] <= r["loans"]
        assert 0.0 <= r["fpd_rate"] <= 1.0


def test_ecl_staging_partitions_the_book_exactly_once(spark, snapshot):
    """Every live loan lands in exactly one Ind AS 109 stage, and the exposures
    reconcile back to the portfolio total. A staging rule with a gap or an
    overlap silently under- or over-provisions."""
    G.ecl_parameters(spark).createOrReplaceTempView("ecl_parameters")
    ecl = spark.sql(G.ECL_SQL)

    latest = snapshot.filter("NOT is_written_off").agg(
        F.sum("principal_outstanding").alias("os"),
        F.count("*").alias("n")).collect()[0]

    staged = ecl.agg(F.sum("exposure_at_default").alias("os"),
                     F.sum("loans").alias("n")).collect()[0]

    assert staged["n"] == latest["n"], "loans lost or double-counted by staging"
    assert staged["os"] == pytest.approx(latest["os"], rel=1e-6)
    assert set(r["ecl_stage"] for r in ecl.collect()) <= {
        "STAGE_1", "STAGE_2", "STAGE_3"}


def test_ecl_provision_rises_with_stage(spark, snapshot):
    """Coverage must be monotonic across stages, or the parameters are wrong."""
    G.ecl_parameters(spark).createOrReplaceTempView("ecl_parameters")
    cov = {r["ecl_stage"]: r["provision_coverage"]
           for r in spark.sql(G.ECL_SQL).collect()}
    assert cov["STAGE_1"] < cov["STAGE_2"] < cov["STAGE_3"]


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
