"""Silver: stamp every loan's position at each reporting date.

This is the day-end process. RBI's clarification of 12 November 2021 fixed that
SMA and NPA classification happens as part of the day-end run and is stamped with
the calendar date that run is for -- not the date the job happened to execute.
So the snapshot date is an explicit column, never `current_date()`, and a rerun
of an old date has to reproduce the old answer exactly.

Everything downstream -- bucket mix, roll rates, vintage curves, GNPA -- reads
this one table, so the delinquency definition exists in exactly one place.

The grain is (loan_id, snapshot_date). Month-end by default, because a daily
grain over a 24-month book is ~100x the rows for no analytical gain; the
`--daily-tail` option additionally stamps the most recent N days, which is what
a collections team actually watches.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..common import Layout, get_spark, write_table

SNAPSHOT_SQL = """
WITH settled AS (
    -- Earliest successful payment per instalment. A bounce followed by a
    -- successful retry settles the instalment on the retry date, so MIN over
    -- successes is the settlement date -- not the first attempt.
    SELECT loan_id, instalment_no, MIN(paid_at) AS settled_at
    FROM   silver_repayment_attempts
    WHERE  status = 'SUCCESS' AND paid_at IS NOT NULL
    GROUP  BY loan_id, instalment_no
),
schedule AS (
    SELECT s.loan_id,
           s.instalment_no,
           s.due_date,
           s.emi_amount,
           s.principal_component,
           p.settled_at
    FROM   silver_emi_schedule s
    LEFT   JOIN settled p
           ON  s.loan_id = p.loan_id
           AND s.instalment_no = p.instalment_no
),
-- Bound each loan to the dates it could plausibly be on the book for, so the
-- cross join stays proportional to loan-months rather than loans x all dates.
loan_window AS (
    SELECT l.loan_id,
           l.customer_id,
           l.merchant_id,
           l.product,
           l.principal,
           l.tenure_months,
           l.disbursed_at,
           add_months(l.disbursed_at, l.tenure_months + 12) AS horizon
    FROM   silver_loans l
),
stamped AS (
    SELECT w.loan_id,
           w.customer_id,
           w.merchant_id,
           w.product,
           w.principal,
           w.disbursed_at,
           d.snapshot_date,
           -- Outstanding principal: every instalment not settled *as at this
           -- snapshot date*. Comparing against the snapshot rather than against
           -- "now" is what makes a historical rerun reproducible.
           SUM(CASE WHEN sch.settled_at IS NULL OR sch.settled_at > d.snapshot_date
                    THEN sch.principal_component ELSE 0 END)          AS principal_outstanding,
           -- Oldest instalment that is both due and unsettled: the arrears anchor.
           MIN(CASE WHEN sch.due_date <= d.snapshot_date
                     AND (sch.settled_at IS NULL OR sch.settled_at > d.snapshot_date)
                    THEN sch.due_date END)                            AS oldest_unpaid_due,
           SUM(CASE WHEN sch.due_date <= d.snapshot_date
                    THEN sch.emi_amount ELSE 0 END)                   AS billed_to_date,
           SUM(CASE WHEN sch.settled_at IS NOT NULL
                     AND sch.settled_at <= d.snapshot_date
                    THEN sch.emi_amount ELSE 0 END)                   AS collected_to_date
    FROM   loan_window w
    JOIN   snapshot_dates d
           ON  d.snapshot_date >= w.disbursed_at
           AND d.snapshot_date <= w.horizon
    JOIN   schedule sch
           ON  sch.loan_id = w.loan_id
    GROUP  BY w.loan_id, w.customer_id, w.merchant_id, w.product, w.principal,
              w.disbursed_at, d.snapshot_date
)
SELECT loan_id,
       customer_id,
       merchant_id,
       product,
       principal,
       disbursed_at,
       snapshot_date,
       ROUND(principal_outstanding, 2)                     AS principal_outstanding,
       ROUND(billed_to_date, 2)                            AS billed_to_date,
       ROUND(collected_to_date, 2)                         AS collected_to_date,
       oldest_unpaid_due,
       COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) AS dpd,
       -- Months on book, the x-axis of every vintage curve.
       CAST(MONTHS_BETWEEN(snapshot_date, disbursed_at) AS INT) AS months_on_book,
       CASE
           WHEN COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) = 0  THEN 'CURRENT'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 30             THEN '1-30'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 60             THEN '31-60'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 90             THEN '61-90'
           ELSE '90+'
       END                                                 AS dpd_bucket,
       -- RBI SMA/NPA classification for non-revolving facilities.
       CASE
           WHEN COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) = 0  THEN 'STANDARD'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 30             THEN 'SMA-0'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 60             THEN 'SMA-1'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) <= 90             THEN 'SMA-2'
           ELSE 'NPA'
       END                                                 AS asset_classification,
       COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) > {write_off_dpd}
                                                           AS is_written_off,
       -- The date the account crossed the write-off threshold, not the date the
       -- job noticed. A ratio that folds in "the last twelve months of
       -- write-offs" -- which is how CRISIL states Snapmint's GNPA -- needs to
       -- know when each one happened, and deriving it from the arrears anchor
       -- keeps it reproducible for a historical rerun.
       CASE WHEN COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) > {write_off_dpd}
            THEN DATE_ADD(oldest_unpaid_due, {write_off_dpd} + 1)
       END                                                 AS written_off_at
FROM   stamped
WHERE  principal_outstanding > 0.005
"""


def month_ends(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Month-end dates in [start, end], inclusive."""
    return spark.sql(f"""
        SELECT DISTINCT last_day(d) AS snapshot_date
        FROM   (SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 MONTH)) AS d)
        WHERE  last_day(d) <= DATE'{end}'
    """)


def daily_tail(spark: SparkSession, end: str, days: int) -> DataFrame:
    return spark.sql(f"""
        SELECT explode(sequence(date_sub(DATE'{end}', {days}), DATE'{end}',
                                INTERVAL 1 DAY)) AS snapshot_date
    """)


def build(spark: SparkSession, layout: Layout, start: str, end: str,
          write_off_dpd: int = 180, tail_days: int = 0) -> DataFrame:
    for entity in ("loans", "emi_schedule", "repayment_attempts"):
        spark.table(layout.table("silver", entity)).createOrReplaceTempView(
            f"silver_{entity}")

    dates = month_ends(spark, start, end)
    if tail_days:
        dates = dates.unionByName(daily_tail(spark, end, tail_days)).distinct()
    dates.createOrReplaceTempView("snapshot_dates")

    df = spark.sql(SNAPSHOT_SQL.format(write_off_dpd=write_off_dpd))
    write_table(df, layout, "silver", "loan_snapshot")
    return df


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--write-off-dpd", type=int, default=180)
    ap.add_argument("--daily-tail", type=int, default=0,
                    help="also stamp the last N calendar days")
    args = ap.parse_args(argv)

    spark = get_spark()
    df = build(spark, Layout(), args.start, args.end,
               args.write_off_dpd, args.daily_tail)
    print(f"silver_loan_snapshot: {df.count():,} loan-date rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
