-- E02 -- Cumulative collections per loan.
--
-- The frame is stated explicitly, and that is the entire answer. Omit it and
-- you get RANGE, which sums over every row sharing the current row's ORDER BY
-- value -- so two payments on the same day both report the day's closing
-- total instead of their own running one.
--
-- Ordered by running_total as well as by date because within a tie the row
-- order is arbitrary: the set of running totals is defined, which row carries
-- which is not.
SELECT loan_id,
       paid_at,
       ROUND(SUM(amount) OVER (
           PARTITION BY loan_id
           ORDER BY paid_at
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ), 2) AS running_collected
FROM   silver_repayment_attempts
WHERE  status = 'SUCCESS' AND paid_at IS NOT NULL
ORDER  BY loan_id, paid_at, running_collected
