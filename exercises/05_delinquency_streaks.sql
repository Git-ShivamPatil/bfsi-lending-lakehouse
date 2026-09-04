-- E05 -- Longest delinquency streak.
--
-- Islands are found by calendar distance, not by list position. A new spell
-- starts when the previous delinquent snapshot is more than one month back --
-- or when there is no previous one at all -- and the running SUM of those
-- starts numbers the spells.
--
-- Doing it this way is what makes a *missing* month behave like the break it
-- really is. The row-number-difference technique cannot see the hole.
WITH delinquent AS (
    SELECT loan_id, snapshot_date
    FROM   silver_loan_snapshot
    WHERE  dpd > 0
),
marked AS (
    SELECT loan_id,
           snapshot_date,
           CASE
               WHEN LAG(snapshot_date) OVER w IS NULL                          THEN 1
               WHEN ROUND(MONTHS_BETWEEN(snapshot_date,
                                         LAG(snapshot_date) OVER w)) > 1       THEN 1
               ELSE 0
           END AS starts_spell
    FROM   delinquent
    WINDOW w AS (PARTITION BY loan_id ORDER BY snapshot_date)
),
spelled AS (
    SELECT loan_id,
           snapshot_date,
           SUM(starts_spell) OVER (
               PARTITION BY loan_id ORDER BY snapshot_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS spell_id
    FROM   marked
),
spells AS (
    SELECT loan_id, spell_id, COUNT(*) AS months, MIN(snapshot_date) AS began
    FROM   spelled
    GROUP  BY loan_id, spell_id
)
SELECT loan_id, months AS longest_streak_months, began
FROM   (SELECT loan_id, months, began,
               ROW_NUMBER() OVER (PARTITION BY loan_id
                                  ORDER BY months DESC, began) AS rn
        FROM   spells)
WHERE  rn = 1
ORDER  BY loan_id
