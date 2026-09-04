-- E05, written the way it is usually written.
--
-- The classic gaps-and-islands trick: number the rows twice, once overall and
-- once within the delinquent ones, and group on the difference. It is correct
-- whenever every month has a row -- and L2 has no March row at all, so both
-- counters step over the hole together, the difference stays constant, and
-- February and April are reported as one continuous two-month spell.
WITH numbered AS (
    SELECT loan_id,
           snapshot_date,
           dpd,
           ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY snapshot_date)
             - ROW_NUMBER() OVER (PARTITION BY loan_id, CASE WHEN dpd > 0 THEN 1 ELSE 0 END
                                  ORDER BY snapshot_date) AS grp
    FROM   silver_loan_snapshot
),
spells AS (
    SELECT loan_id, grp, COUNT(*) AS months, MIN(snapshot_date) AS began
    FROM   numbered
    WHERE  dpd > 0
    GROUP  BY loan_id, grp
)
SELECT loan_id, months AS longest_streak_months, began
FROM   (SELECT loan_id, months, began,
               ROW_NUMBER() OVER (PARTITION BY loan_id
                                  ORDER BY months DESC, began) AS rn
        FROM   spells)
WHERE  rn = 1
ORDER  BY loan_id
