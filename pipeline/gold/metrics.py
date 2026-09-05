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

#
# `gnpa_ratio` is 90+ DPD over gross advances still on the book. That is the
# plain ratio CRISIL states for Snapmint at 2.0%, and it is what the generator
# is calibrated to.
#
# `write_off_principal_in_window` is emitted beside it because the same document
# reports a *second* and quite different metric -- "90+ dpd including last 12
# months write-offs / Disbursements" -- whose denominator is disbursements, not
# advances. That makes it a loss rate on origination rather than a GNPA, and
# folding write-offs into an advances denominator produces a number belonging to
# neither ratio. This project shipped exactly that mistake for one commit.

PORTFOLIO_SQL = """
WITH agg AS (
    SELECT snapshot_date,
           COUNT_IF(NOT is_written_off)                              AS live_loans,
           SUM(CASE WHEN NOT is_written_off
                    THEN principal_outstanding ELSE 0 END)           AS os_on_book,
           SUM(CASE WHEN NOT is_written_off AND dpd > 90
                    THEN principal_outstanding ELSE 0 END)           AS npa_on_book,
           SUM(CASE WHEN NOT is_written_off AND dpd > 30
                    THEN principal_outstanding ELSE 0 END)           AS par30_on_book,
           SUM(CASE WHEN NOT is_written_off AND dpd > 60
                    THEN principal_outstanding ELSE 0 END)           AS par60_on_book,
           -- Written off within the trailing year of this snapshot date.
           SUM(CASE WHEN is_written_off
                     AND DATEDIFF(snapshot_date, written_off_at) <= {lookback}
                    THEN principal_outstanding ELSE 0 END)           AS wo_in_window,
           COUNT_IF(is_written_off
                    AND DATEDIFF(snapshot_date, written_off_at) <= {lookback})
                                                                     AS wo_loans_in_window,
           AVG(CASE WHEN NOT is_written_off THEN principal END)      AS avg_ticket_size
    FROM   silver_loan_snapshot
    GROUP  BY snapshot_date
)
SELECT snapshot_date,
       live_loans,
       ROUND(os_on_book, 2)                                          AS principal_outstanding,
       ROUND(npa_on_book, 2)                                         AS npa_outstanding,
       wo_loans_in_window,
       ROUND(wo_in_window, 2)                                        AS write_off_principal_in_window,
       ROUND(npa_on_book / NULLIF(os_on_book, 0), 5)                 AS gnpa_ratio,
       ROUND(par30_on_book / NULLIF(os_on_book, 0), 5)               AS par_30,
       ROUND(par60_on_book / NULLIF(os_on_book, 0), 5)               AS par_60,
       ROUND(npa_on_book / NULLIF(os_on_book, 0), 5)                 AS par_90,
       ROUND(avg_ticket_size, 2)                                     AS avg_ticket_size
FROM   agg
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
# the checkout funnel
# ---------------------------------------------------------------------------
#
# The two metrics every checkout-finance job advertisement leads with, and
# neither was computable while the book started at disbursal.
#
# The denominator choice is the whole question. Approval rate is stated on
# *decisioned* applications, not on all applications received -- otherwise the
# most recent period always looks worse, because some of it has not been decided
# yet, and somebody reads a reporting lag as a policy tightening. Here every
# application carries a decision, so the two coincide; the query is written the
# careful way anyway, because it will not always be true.

FUNNEL_SQL = """
SELECT date_trunc('month', applied_at)                              AS month,
       channel,
       COUNT(*)                                                     AS applications,
       COUNT_IF(decision = 'APPROVED')                              AS approved,
       COUNT_IF(converted = 'Y')                                    AS converted,
       ROUND(COUNT_IF(decision = 'APPROVED')
             / NULLIF(COUNT_IF(decision IN ('APPROVED', 'DECLINED')), 0), 5)
                                                                    AS approval_rate,
       -- Of what we approved, what actually disbursed. The gap is the down
       -- payment: approved at checkout, then abandoned rather than paying the
       -- 25-33% up front. That is a product problem, not a credit one.
       ROUND(COUNT_IF(converted = 'Y')
             / NULLIF(COUNT_IF(decision = 'APPROVED'), 0), 5)        AS conversion_of_approved,
       -- End to end, which is the number a founder asks for.
       ROUND(COUNT_IF(converted = 'Y') / NULLIF(COUNT(*), 0), 5)     AS application_to_loan,
       ROUND(AVG(cart_amount), 2)                                    AS avg_cart_amount
FROM   silver_applications
GROUP  BY date_trunc('month', applied_at), channel
ORDER  BY month, channel
"""

DECLINE_MIX_SQL = """
SELECT decline_reason,
       COUNT(*)                                                     AS declines,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 5)                   AS share_of_declines
FROM   silver_applications
WHERE  decision = 'DECLINED'
GROUP  BY decline_reason
ORDER  BY declines DESC
"""

# ---------------------------------------------------------------------------
# first-payment default
# ---------------------------------------------------------------------------
#
# FPD is the metric a checkout lender watches most closely, and it answers a
# different question from lifetime default. A merchant with high FPD and ordinary
# GNPA is an onboarding or fraud problem; ordinary FPD with high GNPA is an
# underwriting one. They call for opposite responses, so they are computed apart.
#
# NOT EXISTS rather than NOT IN, deliberately: the settlement set can contain a
# NULL loan_id -- the generator injects orphan repayments precisely because real
# extracts do -- and NOT IN against a set containing one NULL returns no rows at
# all, reporting a clean 0% first-payment default across the entire book.

FPD_SQL = """
WITH first_instalment AS (
    SELECT loan_id, MIN(due_date) AS due_date
    FROM   silver_emi_schedule
    WHERE  instalment_no = 1
    GROUP  BY loan_id
),
eligible AS (
    SELECT l.loan_id, l.merchant_id, l.principal, f.due_date
    FROM   silver_loans l
    JOIN   first_instalment f ON f.loan_id = l.loan_id
    -- Only loans whose first instalment has had a fair chance to settle.
    WHERE  f.due_date <= date_sub((SELECT MAX(snapshot_date) FROM silver_loan_snapshot), 30)
),
defaulted AS (
    SELECT e.loan_id,
           e.merchant_id,
           e.principal,
           CASE WHEN NOT EXISTS (
                    SELECT 1 FROM silver_repayment_attempts r
                    WHERE  r.loan_id = e.loan_id
                      AND  r.instalment_no = 1
                      AND  r.status = 'SUCCESS'
                      AND  r.paid_at IS NOT NULL
                      AND  datediff(r.paid_at, e.due_date) <= 30)
                THEN 1 ELSE 0 END                                   AS is_fpd
    FROM   eligible e
)
SELECT d.merchant_id,
       m.merchant_name,
       m.category,
       COUNT(*)                                                     AS loans,
       SUM(d.is_fpd)                                                AS fpd_loans,
       ROUND(SUM(d.is_fpd) / COUNT(*), 5)                           AS fpd_rate,
       ROUND(AVG(d.principal), 2)                                   AS avg_ticket,
       ROUND(AVG(SUM(d.is_fpd) / COUNT(*)) OVER (PARTITION BY m.category), 5)
                                                                    AS category_fpd_rate
FROM   defaulted d
JOIN   silver_merchants m ON m.merchant_id = d.merchant_id
GROUP  BY d.merchant_id, m.merchant_name, m.category
HAVING COUNT(*) >= 25
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
    SELECT loan_id, snapshot_date, dpd_bucket, principal_outstanding, is_written_off
    FROM   silver_loan_snapshot
    WHERE  snapshot_date = last_day(snapshot_date)
),
transitions AS (
    SELECT loan_id,
           snapshot_date                                            AS from_date,
           dpd_bucket                                               AS from_bucket,
           principal_outstanding,
           is_written_off                                           AS from_written_off,
           LEAD(dpd_bucket)     OVER w                              AS to_bucket,
           LEAD(is_written_off) OVER w                              AS to_written_off,
           LEAD(snapshot_date)  OVER w                              AS to_date
    FROM   month_end
    WINDOW w AS (PARTITION BY loan_id ORDER BY snapshot_date)
)
SELECT from_date,
       from_bucket,
       -- Three ways to leave, and they are not the same event:
       --   WRITTEN_OFF -- the account crossed the write-off threshold. A loss.
       --   CLOSED      -- no following snapshot at all, so the loan was repaid
       --                  in full and left the book. A good outcome.
       --   a bucket    -- it rolled, cured, or stayed put.
       -- Collapsing the first two into 'CLOSED' reports a write-off as though
       -- it were a successful payoff, which flatters the cure rate and hides
       -- exactly the number a credit committee is looking for.
       CASE WHEN to_written_off      THEN 'WRITTEN_OFF'
            WHEN to_bucket IS NULL   THEN 'CLOSED'
            ELSE to_bucket
       END                                                          AS to_bucket,
       COUNT(*)                                                     AS loans,
       ROUND(SUM(principal_outstanding), 2)                         AS principal_outstanding,
       ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY from_date, from_bucket), 5)
                                                                    AS roll_rate
FROM   transitions
-- The origin has to be a live account: an already-written-off loan has left the
-- book and cannot roll anywhere.
WHERE  NOT from_written_off
  AND  (to_date IS NULL OR months_between(to_date, from_date) BETWEEN 0.9 AND 1.1)
GROUP  BY from_date,
          from_bucket,
          CASE WHEN to_written_off    THEN 'WRITTEN_OFF'
               WHEN to_bucket IS NULL THEN 'CLOSED'
               ELSE to_bucket
          END
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
# Ind AS 109 expected credit loss
# ---------------------------------------------------------------------------
#
# Staging is a pure function of DPD here, which is the simplification worth being
# explicit about: a real implementation also stages on qualitative triggers
# (restructuring, forbearance, watch-list) and on a relative deterioration in PD
# since origination, not only on an absolute day count. The PD and LGD inputs are
# assumptions -- see generator/config.py -- so the provision has the right shape
# but the number is not quotable.

ECL_SQL = """
SELECT snapshot_date,
       CASE
           WHEN dpd <= 30 THEN 'STAGE_1'
           WHEN dpd <= 90 THEN 'STAGE_2'
           ELSE 'STAGE_3'
       END                                                         AS ecl_stage,
       COUNT(*)                                                    AS loans,
       ROUND(SUM(principal_outstanding), 2)                        AS exposure_at_default,
       MAX(p.pd)                                                   AS pd_assumed,
       MAX(p.lgd)                                                  AS lgd_assumed,
       ROUND(SUM(principal_outstanding) * MAX(p.pd) * MAX(p.lgd), 2)
                                                                   AS expected_credit_loss,
       ROUND(MAX(p.pd) * MAX(p.lgd), 5)                            AS provision_coverage
FROM   silver_loan_snapshot s
JOIN   ecl_parameters p
       ON p.stage = CASE
                        WHEN s.dpd <= 30 THEN 'STAGE_1'
                        WHEN s.dpd <= 90 THEN 'STAGE_2'
                        ELSE 'STAGE_3'
                    END
WHERE  NOT is_written_off
GROUP  BY snapshot_date,
          CASE WHEN dpd <= 30 THEN 'STAGE_1'
               WHEN dpd <= 90 THEN 'STAGE_2'
               ELSE 'STAGE_3' END
"""


def ecl_parameters(spark: SparkSession) -> DataFrame:
    """PD/LGD assumptions as a table, so the provision joins to its inputs
    instead of hard-coding them inside the SQL."""
    from generator import config as C

    rows = [(stage, float(v["pd"]), float(v["lgd"]))
            for stage, v in C.ECL_PARAMETERS.items()]
    return spark.createDataFrame(rows, "stage string, pd double, lgd double")


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
                   "customers", "applications", "quarantine", "loan_snapshot"):
        spark.table(layout.table("silver", entity)).createOrReplaceTempView(
            f"silver_{entity}")
    rules_dim(spark).createOrReplaceTempView("rules_dim")
    ecl_parameters(spark).createOrReplaceTempView("ecl_parameters")

    from generator import config as C

    tables = {
        "portfolio_summary": PORTFOLIO_SQL.format(
            lookback=C.GNPA_WRITE_OFF_LOOKBACK_DAYS),
        "funnel": FUNNEL_SQL,
        "decline_mix": DECLINE_MIX_SQL,
        "first_payment_default": FPD_SQL,
        "bucket_mix": BUCKET_MIX_SQL,
        "roll_rate": ROLL_RATE_SQL,
        "vintage": VINTAGE_SQL,
        "collection_efficiency": COLLECTION_SQL,
        "merchant_risk": MERCHANT_RISK_SQL,
        "ecl_staging": ECL_SQL,
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
