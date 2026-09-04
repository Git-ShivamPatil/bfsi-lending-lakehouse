-- E02, written the way it is usually written.
--
-- No frame clause. That is not "the default sensible thing" -- it is RANGE
-- BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, and on the two payments dated
-- 5 January it reports 150 for both.
SELECT loan_id,
       paid_at,
       ROUND(SUM(amount) OVER (
           PARTITION BY loan_id
           ORDER BY paid_at
       ), 2) AS running_collected
FROM   silver_repayment_attempts
WHERE  status = 'SUCCESS' AND paid_at IS NOT NULL
ORDER  BY loan_id, paid_at, running_collected
