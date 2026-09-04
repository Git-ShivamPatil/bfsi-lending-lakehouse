-- E01, written the way it is usually written.
--
-- Filters the written-off accounts out in a WHERE clause, which is fine, and
-- then omits the ELSE 0, which is not: January has no account past 90 DPD, so
-- the conditional SUM is NULL, the ratio is NULL, and the month reports as
-- missing rather than clean.
SELECT snapshot_date,
       ROUND(SUM(principal_outstanding), 2)                           AS principal_outstanding,
       ROUND(SUM(CASE WHEN dpd > 90 THEN principal_outstanding END), 2)
                                                                      AS npa_outstanding,
       ROUND(SUM(CASE WHEN dpd > 90 THEN principal_outstanding END)
             / SUM(principal_outstanding), 5)                         AS gnpa_ratio,
       ROUND(SUM(CASE WHEN dpd > 30 THEN principal_outstanding END)
             / SUM(principal_outstanding), 5)                         AS par_30
FROM   silver_loan_snapshot
WHERE  NOT is_written_off
GROUP  BY snapshot_date
ORDER  BY snapshot_date
