-- E04, written the way it is usually written.
--
-- Plain EXCEPT. It de-duplicates, so L3 -- present twice before the replay and
-- once after -- reports as identical, and the one discrepancy that indicates a
-- genuinely non-idempotent write is the one this misses.
SELECT 'before' AS side, loan_id, status, bureau_score
FROM   (SELECT loan_id, status, bureau_score FROM silver_before
        EXCEPT
        SELECT loan_id, status, bureau_score FROM silver_after)
UNION ALL
SELECT 'after' AS side, loan_id, status, bureau_score
FROM   (SELECT loan_id, status, bureau_score FROM silver_after
        EXCEPT
        SELECT loan_id, status, bureau_score FROM silver_before)
ORDER  BY side, loan_id, bureau_score
