"""E04 -- Prove a re-run changed nothing.

Question
--------
Given the silver table before and after replaying yesterday's batch, return
every row that differs, **in either direction**. The table has nullable columns
and may contain genuine duplicate rows.

The trap
--------
Two of them, and they hide each other.

1. `EXCEPT` is a *set* operator: it de-duplicates. If one side holds a row twice
   and the other holds it once, `EXCEPT` reports nothing -- the duplicate has
   vanished from the comparison, which is precisely the corruption an
   idempotency check exists to find. `EXCEPT ALL` preserves multiplicity.

2. Joining on `a.col = b.col` never matches two NULLs, because `NULL = NULL` is
   UNKNOWN. A row whose only change is NULL becoming 720 either disappears or
   reports as a spurious insert-plus-delete depending on the join shape. The
   null-safe operator `<=>` is the fix -- though with `EXCEPT ALL` the problem
   does not arise at all, which is itself the lesson.

This is the query form of reconciliation, which is what regulatory return work
actually consists of, and it is the direct counterpart of the pipeline's own
`MERGE INTO` idempotency test.
"""

VIEWS = {
    "silver_before": (
        "loan_id string, status string, bureau_score int",
        [
            ("L1", "OPEN", 700),
            ("L2", "OPEN", None),      # only difference is a NULL becoming a value
            ("L3", "OPEN", 650),
            ("L3", "OPEN", 650),       # genuine duplicate, present twice
        ],
    ),
    "silver_after": (
        "loan_id string, status string, bureau_score int",
        [
            ("L1", "OPEN", 700),
            ("L2", "OPEN", 720),
            ("L3", "OPEN", 650),       # ... and only once here
        ],
    ),
}

# Hand-computed, ordered by side then loan_id.
#   L1  identical on both sides                          -> not reported
#   L2  NULL -> 720, so one row on each side             -> two rows
#   L3  present twice before, once after                 -> one surplus 'before'
EXPECTED = [
    ("after", "L2", "OPEN", 720),
    ("before", "L2", "OPEN", None),
    ("before", "L3", "OPEN", 650),
]
