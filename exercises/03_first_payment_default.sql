-- E03 -- First-payment default by merchant.
--
-- NOT EXISTS, not NOT IN. The settlement list contains a NULL loan_id, and
-- under NOT IN that one NULL makes the predicate UNKNOWN for every candidate
-- row, so the query returns nothing and the book reports 0% FPD.
--
-- NOT EXISTS asks a different question -- "is there a matching row?" -- and a
-- NULL simply fails to match, which is the intended semantics.
SELECT l.merchant_id,
       COUNT(*)                                                        AS loans,
       COUNT(*) FILTER (
           WHERE NOT EXISTS (SELECT 1
                             FROM   silver_first_instalment_settled s
                             WHERE  s.loan_id = l.loan_id)
       )                                                               AS fpd_loans,
       ROUND(COUNT(*) FILTER (
           WHERE NOT EXISTS (SELECT 1
                             FROM   silver_first_instalment_settled s
                             WHERE  s.loan_id = l.loan_id)
       ) / COUNT(*), 5)                                                AS fpd_rate,
       RANK() OVER (ORDER BY COUNT(*) FILTER (
           WHERE NOT EXISTS (SELECT 1
                             FROM   silver_first_instalment_settled s
                             WHERE  s.loan_id = l.loan_id)
       ) / COUNT(*) DESC)                                              AS fpd_rank
FROM   silver_loans l
GROUP  BY l.merchant_id
ORDER  BY fpd_rank, l.merchant_id
