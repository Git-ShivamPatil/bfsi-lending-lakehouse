-- E03, written the way it is usually written.
--
-- NOT IN against a subquery that contains a NULL. Every merchant reports zero
-- first-payment defaults, which looks like good news rather than a bug.
WITH defaulted AS (
    SELECT l.loan_id, l.merchant_id
    FROM   silver_loans l
    WHERE  l.loan_id NOT IN (SELECT loan_id FROM silver_first_instalment_settled)
)
SELECT l.merchant_id,
       COUNT(*)                                                        AS loans,
       COUNT(d.loan_id)                                                AS fpd_loans,
       ROUND(COUNT(d.loan_id) / COUNT(*), 5)                           AS fpd_rate,
       RANK() OVER (ORDER BY COUNT(d.loan_id) / COUNT(*) DESC)         AS fpd_rank
FROM   silver_loans l
LEFT   JOIN defaulted d ON d.loan_id = l.loan_id
GROUP  BY l.merchant_id
ORDER  BY fpd_rank, l.merchant_id
