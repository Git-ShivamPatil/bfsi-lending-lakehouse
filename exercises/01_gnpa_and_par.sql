-- E01 -- GNPA and PAR by month.
--
-- The whole exercise is in the ELSE 0. A conditional SUM over a month with no
-- qualifying rows returns NULL, not zero, and a GNPA of NULL reads downstream
-- as "we have no data for this month" rather than "this month had no bad
-- debt". On a young book that silently removes the early months from every
-- chart that drops nulls.
--
-- NULLIF on the denominator is the other half: under ANSI mode -- the Spark 4
-- and Databricks default -- dividing by zero raises DIVIDE_BY_ZERO rather than
-- returning NULL, so a month where the whole book is written off would fail the
-- job instead of reporting a gap.
SELECT snapshot_date,
       ROUND(SUM(CASE WHEN NOT is_written_off
                      THEN principal_outstanding ELSE 0 END), 2)      AS principal_outstanding,
       ROUND(SUM(CASE WHEN NOT is_written_off AND dpd > 90
                      THEN principal_outstanding ELSE 0 END), 2)      AS npa_outstanding,
       ROUND(SUM(CASE WHEN NOT is_written_off AND dpd > 90
                      THEN principal_outstanding ELSE 0 END)
             / NULLIF(SUM(CASE WHEN NOT is_written_off
                               THEN principal_outstanding ELSE 0 END), 0), 5) AS gnpa_ratio,
       ROUND(SUM(CASE WHEN NOT is_written_off AND dpd > 30
                      THEN principal_outstanding ELSE 0 END)
             / NULLIF(SUM(CASE WHEN NOT is_written_off
                               THEN principal_outstanding ELSE 0 END), 0), 5) AS par_30
FROM   silver_loan_snapshot
GROUP  BY snapshot_date
ORDER  BY snapshot_date
