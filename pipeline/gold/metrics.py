"""Gold: the tables a lending analyst actually opens.

Every table here reads from `silver_loan_snapshot`, so the delinquency definition
is stated once and never re-derived. The SQL is written out rather than expressed
through the DataFrame API because these are the queries a reviewer will want to
read -- the roll-rate matrix and the vintage triangle are the two that carry the
analytical weight, and both are window-function problems rather than group-bys.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F

from validation import rules as R

from ..common import Layout, get_spark, write_table

# ---------------------------------------------------------------------------
# portfolio position
# ---------------------------------------------------------------------------

PORTFOLIO_SQL = """
SELECT snapshot_date,
       COUNT(*)                                                  AS live_loans,
       ROUND(SUM(principal_outstanding), 2)                      AS principal_outstanding,
       ROUND(SUM(CASE WHEN dpd > 90 THEN principal_outstanding END), 2)
                                                                 AS npa_outstanding,
       -- GNPA is measured on advances still on the book, so written-off
       -- accounts are excluded from both numerator and denominator; counting
       -- them would double-recognise a loss already taken.
       ROUND(SUM(CASE WHEN dpd > 90 THEN principal_outstanding END)
             / NULLIF(SUM(principal_outstanding), 0), 5)         AS gnpa_ratio,
       ROUND(SUM(CASE WHEN dpd > 30 THEN principal_outstanding END)
             / NULLIF(SUM(principal_outstanding), 0), 5)         AS par_30,
       ROUND(SUM(CASE WHEN dpd > 60 THEN principal_outstanding END)
             / NULLIF(SUM(principal_outstanding), 0), 5)         AS par_60,
       ROUND(SUM(CASE WHEN dpd > 90 THEN principal_outstanding END)
             / NULLIF(SUM(principal_outstanding), 0), 5)         AS par_90,
       ROUND(AVG(principal), 2)                                  AS avg_ticket_size
FROM   silver_loan_snapshot
WHERE  NOT is_written_off
GROUP  BY snapshot_date
ORDER  BY snapshot_date
"""

BUCKET_MIX_SQL = """
SELECT snapshot_date,
       dpd_bucket,
       asset_classification,
       COUNT(*)                                                  AS loans,
       ROUND(SUM(principal_outstanding), 2)                      AS principal_outstanding,
       ROUND(SUM(principal_outstanding)
             / SUM(SUM(principal_outstanding)) OVER (PARTITION BY snapshot_date), 5)
                                                                 AS share_of_book
FROM   silver_loan_snapshot
WHERE  NOT is_written_off
GROUP  BY snapshot_date, dpd_bucket, asset_classification
"""

# ---------------------------------------------------------------------------
# roll rates
# ---------------------------------------------------------------------------
#
# The question a roll-rate matrix answers is "of the accounts sitting in bucket X
# at month end, where were they a month later". It is a self-join in disguise,
# and LEAD over a per-loan window is both faster and easier to get right than
# joining the snapshot to itself on a date offset -- a self-join silently drops
# loans whose next snapshot is missing, whereas LEAD makes that a NULL you can
# see and account for.

ROLL_RATE_SQL = """
WITH month_end AS (
    SELECT loan_id, snapshot_date, dpd_bucket, principal_outstanding
    FROM   silver_loan_snapshot
    WHERE  snapshot_date = last_day(snapshot_date)
      AND  NOT is_written_off
),
transitions AS (
    SELECT loan_id,
           snapshot_date                                            AS from_date,
           dpd_bucket                                               AS from_bucket,
           principal_outstanding,
           LEAD(dpd_bucket)    OVER w                               AS to_bucket,
           LEAD(snapshot_date) OVER w                               AS to_date
    FROM   month_end
    WINDOW w AS (PARTITION BY loan_id ORDER BY snapshot_date)
)
SELECT from_date,
       from_bucket,
       -- A loan with no following snapshot has closed: it left the book rather
       -- than rolling anywhere, and lumping it in with 'CURRENT' would flatter
       -- the cure rate.
       COALESCE(to_bucket, 'CLOSED')                                AS to_bucket,
       COUNT(*)                                                     AS loans,
       ROUND(SUM(principal_outstanding), 2)                         AS principal_outstanding,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY from_date, from_bucket), 5)
                                                                    AS roll_rate
FROM   transitions
WHERE  to_date IS NULL OR months_between(to_date, from_date) BETWEEN 0.9 AND 1.1
GROUP  BY from_date, from_bucket, COALESCE(to_bucket, 'CLOSED')
"""

# ---------------------------------------------------------------------------
# vintage
# ---------------------------------------------------------------------------
#
# Vintage curves have exactly one methodological trap: every cohort must be
# observed at the SAME months-on-book. Reading each cohort "as at today" compares
# a three-month-old book with a two-year-old one and produces a curve that slopes
# purely because of age.

VINTAGE_SQL = """
WITH observation_end AS (
    SELECT MAX(snapshot_date) AS last_date FROM silver_loan_snapshot
),
-- The month on book at which each loan FIRST breached, or NULL if it never did.
-- Reducing to a first-breach month is what lets the curve stay cumulative after
-- a loan leaves the book: a loan that went delinquent and then closed must keep
-- counting as bad for its cohort, and a running MAX over surviving snapshot rows
-- cannot do that -- it silently drops the loan from both numerator and
-- denominator, which is how a cumulative curve ends up sloping downwards.
first_breach AS (
    SELECT loan_id,
           MIN(CASE WHEN dpd > 30 THEN months_on_book END) AS first_30plus_mob,
           MIN(CASE WHEN dpd > 90 THEN months_on_book END) AS first_90plus_mob
    FROM   silver_loan_snapshot
    WHERE  snapshot_date = last_day(snapshot_date)
    GROUP  BY loan_id
),
-- The cohort denominator is fixed: every loan disbursed in the month, counted at
-- every MOB it has had the opportunity to reach, open or closed.
cohort AS (
    SELECT l.loan_id,
           date_trunc('month', l.disbursed_at)                       AS cohort_month,
           CAST(months_between((SELECT last_date FROM observation_end),
                               l.disbursed_at) AS INT)               AS observable_mob
    FROM   silver_loans l
),
mob AS (
    SELECT explode(sequence(1, 24)) AS months_on_book
)
SELECT c.cohort_month,
       m.months_on_book,
       COUNT(*)                                                      AS loans,
       SUM(CASE WHEN f.first_30plus_mob <= m.months_on_book THEN 1 ELSE 0 END)
                                                                     AS loans_ever_30plus,
       ROUND(SUM(CASE WHEN f.first_30plus_mob <= m.months_on_book THEN 1 ELSE 0 END)
             / COUNT(*), 5)                                          AS rate_30plus,
       ROUND(SUM(CASE WHEN f.first_90plus_mob <= m.months_on_book THEN 1 ELSE 0 END)
             / COUNT(*), 5)                                          AS rate_90plus
FROM   cohort c
CROSS  JOIN mob m
LEFT   JOIN first_breach f ON f.loan_id = c.loan_id
WHERE  m.months_on_book <= c.observable_mob
GROUP  BY c.cohort_month, m.months_on_book
HAVING COUNT(*) >= 30
"""

# ---------------------------------------------------------------------------
# collections
# ---------------------------------------------------------------------------

COLLECTION_SQL = """
WITH billed AS (
    SELECT date_trunc('month', due_date) AS month,
           SUM(emi_amount)               AS billed_amount,
           COUNT(*)                      AS instalments_billed
    FROM   silver_emi_schedule
    GROUP  BY date_trunc('month', due_date)
),
collected AS (
    SELECT date_trunc('month', paid_at)  AS month,
           SUM(amount)                   AS collected_amount,
           COUNT(*)                      AS instalments_collected
    FROM   silver_repayment_attempts
    WHERE  status = 'SUCCESS' AND paid_at IS NOT NULL
    GROUP  BY date_trunc('month', paid_at)
),
attempts AS (
    SELECT date_trunc('month', attempted_at) AS month,
           mode,
           COUNT(*)                                                   AS attempts,
           SUM(CASE WHEN status = 'BOUNCED' THEN 1 ELSE 0 END)        AS bounces
    FROM   silver_repayment_attempts
    GROUP  BY date_trunc('month', attempted_at), mode
),
bounce AS (
    SELECT month,
           SUM(attempts)                                              AS attempts,
           SUM(bounces)                                               AS bounces,
           ROUND(SUM(bounces) / NULLIF(SUM(attempts), 0), 5)          AS bounce_rate
    FROM   attempts
    GROUP  BY month
)
SELECT b.month,
       ROUND(b.billed_amount, 2)                                      AS billed_amount,
       ROUND(COALESCE(c.collected_amount, 0), 2)                      AS collected_amount,
       -- Collection efficiency: of what was billed this month, what came back
       -- this month. The demanding version of the metric -- it does not get
       -- credit for arrears collected from earlier months.
       ROUND(COALESCE(c.collected_amount, 0) / NULLIF(b.billed_amount, 0), 5)
                                                                      AS collection_efficiency,
       bo.attempts,
       bo.bounces,
       bo.bounce_rate
FROM   billed b
LEFT   JOIN collected c ON b.month = c.month
LEFT   JOIN bounce    bo ON b.month = bo.month
ORDER  BY b.month
"""

# ---------------------------------------------------------------------------
# risk concentration -- the fraud/risk angle
# ---------------------------------------------------------------------------

MERCHANT_RISK_SQL = """
WITH latest AS (
    SELECT * FROM silver_loan_snapshot
    WHERE  snapshot_date = (SELECT MAX(snapshot_date) FROM silver_loan_snapshot)
),
per_merchant AS (
    SELECT l.merchant_id,
           m.merchant_name,
           m.category,
           m.city_tier,
           COUNT(*)                                                   AS live_loans,
           ROUND(SUM(l.principal_outstanding), 2)                     AS principal_outstanding,
           ROUND(SUM(CASE WHEN l.dpd > 30 THEN l.principal_outstanding END)
                 / NULLIF(SUM(l.principal_outstanding), 0), 5)        AS par_30
    FROM   latest l
    JOIN   silver_merchants m ON l.merchant_id = m.merchant_id
    GROUP  BY l.merchant_id, m.merchant_name, m.category, m.city_tier
    HAVING COUNT(*) >= 20
)
SELECT *,
       ROUND(AVG(par_30) OVER (PARTITION BY category), 5)             AS category_par_30,
       -- How many standard deviations above its own category a merchant sits.
       -- Portfolio-level PAR hides the merchant that is quietly the problem.
       ROUND((par_30 - AVG(par_30) OVER (PARTITION BY category))
             / NULLIF(STDDEV_SAMP(par_30) OVER (PARTITION BY category), 0), 3)
                                                                      AS par_30_zscore,
       RANK() OVER (PARTITION BY category ORDER BY par_30 DESC)       AS risk_rank_in_category
FROM   per_merchant
"""

# ---------------------------------------------------------------------------
# data-quality scorecard
# ---------------------------------------------------------------------------

DQ_SQL = """
SELECT q.rule_id,
       r.entity,
       r.dimension,
       r.severity,
       r.description,
       COUNT(*) AS findings
FROM   silver_quarantine q
JOIN   rules_dim r ON q.rule_id = r.rule_id
GROUP  BY q.rule_id, r.entity, r.dimension, r.severity, r.description
ORDER  BY findings DESC
"""


def rules_dim(spark: SparkSession) -> DataFrame:
    """The rule repository as a dimension table, so findings join to their text."""
    rows = [
        (r.rule_id, r.entity, r.dimension, r.severity, r.kind, r.description)
        for r in R.RULES
    ]
    return spark.createDataFrame(
        rows, "rule_id string, entity string, dimension string, "
              "severity string, kind string, description string")


def build(spark: SparkSession, layout: Layout) -> dict[str, int]:
    for entity in ("loans", "emi_schedule", "repayment_attempts", "merchants",
                   "customers", "quarantine", "loan_snapshot"):
        spark.table(layout.table("silver", entity)).createOrReplaceTempView(
            f"silver_{entity}")
    rules_dim(spark).createOrReplaceTempView("rules_dim")

    tables = {
        "portfolio_summary": PORTFOLIO_SQL,
        "bucket_mix": BUCKET_MIX_SQL,
        "roll_rate": ROLL_RATE_SQL,
        "vintage": VINTAGE_SQL,
        "collection_efficiency": COLLECTION_SQL,
        "merchant_risk": MERCHANT_RISK_SQL,
        "dq_scorecard": DQ_SQL,
    }

    counts = {}
    for name, sql in tables.items():
        df = spark.sql(sql)
        write_table(df, layout, "gold", name)
        counts[name] = df.count()
    return counts


def main(argv=None) -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args(argv)
    spark = get_spark()
    for name, n in build(spark, Layout()).items():
        print(f"gold_{name}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
