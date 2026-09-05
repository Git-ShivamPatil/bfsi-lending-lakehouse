"""Silver: stamp every loan's position at each reporting date.

This is the day-end process, and it is built to a rule rather than to taste. The
governing instrument for an NBFC is the Reserve Bank of India (Non-Banking
Financial Companies - Income Recognition, Asset Classification and Provisioning)
Directions, 2025 -- RBI/DOR/2025-26/356, 28 November 2025. Para 18:

    An NBFC shall flag a borrower account as overdue, if so, as part of their
    day-end processes for the due date, irrespective of the time of running such
    processes.

and para 19:

    classification of borrower accounts as SMA as well as NPA shall be done as
    part of day-end process for the relevant date and the SMA or NPA
    classification date shall be the calendar date for which the day end process
    is run.

That is why the snapshot date is an explicit column and never `current_date()`,
and why re-running an old date has to reproduce the old answer exactly. "The date
the job happened to run" is not an answer the regulator accepts, and
`test_snapshot_is_reproducible_for_a_historical_date` holds the line on it.

Para 24 fixes the upgrade rule:

    Loan accounts classified as NPAs may be upgraded as 'standard' asset only if
    entire arrears of interest and principal are paid by the borrower.

This falls out of measuring DPD from the *oldest* unpaid instalment rather than
the most recent one: paying instalment three while instalment one is still
outstanding moves nothing. `test_partial_payment_does_not_upgrade_the_account`
asserts it, because it is the kind of behaviour that is easy to break with a
well-meaning change to the arrears anchor.

**Two thresholds, on purpose.** `asset_classification` follows the Base Layer
glide path in paras 43-44 of the IRACP Directions -- NPA at more than 150 days
from 31 Mar 2024, 120 from 31 Mar 2025, 90 only from 31 Mar 2026, against a base
rule of more than 180 -- so a book spanning 2024 to 2026 is not stamped with a
rule that had not yet come into force. `dpd_bucket`, and the GNPA built on it,
stay on a fixed 90 days, because that is the basis the published figure the
generator is calibrated against is stated on. Conflating a regulatory
classification with a risk metric is how a book ends up unable to reconcile to
either.

The SMA bands come from a different instrument again -- para 18 of the RBI
(NBFC - Resolution of Stressed Assets) Directions, 2025 -- and the NBFC table is
a single column. The two-column "loans other than revolving facilities" split
belongs to the bank instrument and does not apply here.

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
       -- The NPA threshold in force for an NBFC-Base-Layer on this snapshot
       -- date, per paras 43-44. Emitted as a column rather than hidden in the
       -- CASE below so that a reviewer can see which rule was applied to which
       -- date without re-deriving it.
       {npa_threshold_sql}                                 AS npa_dpd_threshold,
       -- Regulatory asset classification. SMA bands are the single-column NBFC
       -- table at para 18 of the Resolution of Stressed Assets Directions, 2025
       -- (up to 30 / more than 30 up to 60 / more than 60 up to 90). NPA is
       -- tested FIRST and against the glide-path threshold, which is why this
       -- column can disagree with the 90+ delinquency bucket above on the same
       -- row at any snapshot before 31 March 2026.
       --
       -- One honest edge: the SMA table stops at 90 days, but the Base Layer NPA
       -- threshold was above 90 for most of this book's window. An account 100
       -- days overdue in 2025 is therefore past the end of the SMA table and not
       -- yet an NPA -- a band the instruments do not name. It is reported as
       -- SMA-2, the deepest category that exists, rather than given an invented
       -- label.
       CASE
           WHEN COALESCE(DATEDIFF(snapshot_date, oldest_unpaid_due), 0) = 0  THEN 'STANDARD'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) > {npa_threshold_sql} THEN 'NPA'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) > 60             THEN 'SMA-2'
           WHEN DATEDIFF(snapshot_date, oldest_unpaid_due) > 30             THEN 'SMA-1'
           ELSE 'SMA-0'
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


def npa_threshold_sql(glide_path=None) -> str:
    """A CASE expression giving the NPA day count in force on `snapshot_date`.

    Built from `config.NPA_DPD_GLIDE_PATH` rather than written out, so the
    thresholds and their effective dates live in one auditable place next to
    their citation. Emitted newest-first: the most recent effective date that
    has already passed wins.
    """
    from generator import config as C

    steps = sorted(glide_path or C.NPA_DPD_GLIDE_PATH, reverse=True)
    whens = "\n           ".join(
        f"WHEN snapshot_date >= DATE'{effective}' THEN {days}"
        for effective, days in steps)
    # Before the earliest step, the pre-glide-path Base Layer threshold applies.
    return f"CASE\n           {whens}\n           ELSE 180\n       END"


def snapshot_sql(write_off_dpd: int = 180, glide_path=None) -> str:
    """The snapshot query, with both thresholds resolved.

    A single entry point so that callers cannot accidentally format one
    placeholder and forget the other -- which would leave a literal
    `{npa_threshold_sql}` in the SQL and fail at parse time rather than
    silently, but still waste everyone's afternoon.
    """
    return SNAPSHOT_SQL.format(write_off_dpd=write_off_dpd,
                               npa_threshold_sql=npa_threshold_sql(glide_path))


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

    df = spark.sql(snapshot_sql(write_off_dpd))
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
