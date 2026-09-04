-- E04 -- Prove a re-run changed nothing.
--
-- EXCEPT ALL in both directions. ALL is the whole point: the set version
-- de-duplicates, so a row that exists twice on one side and once on the other
-- compares equal and the discrepancy disappears -- which is exactly the
-- corruption an idempotency check is looking for.
--
-- Comparing whole rows also sidesteps the NULL-equality problem entirely.
-- EXCEPT ALL uses null-safe row comparison, so a column going from NULL to 720
-- shows up as one row on each side without anyone having to remember <=>.
SELECT 'before' AS side, loan_id, status, bureau_score
FROM   (SELECT loan_id, status, bureau_score FROM silver_before
        EXCEPT ALL
        SELECT loan_id, status, bureau_score FROM silver_after)
UNION ALL
SELECT 'after' AS side, loan_id, status, bureau_score
FROM   (SELECT loan_id, status, bureau_score FROM silver_after
        EXCEPT ALL
        SELECT loan_id, status, bureau_score FROM silver_before)
ORDER  BY side, loan_id, bureau_score
