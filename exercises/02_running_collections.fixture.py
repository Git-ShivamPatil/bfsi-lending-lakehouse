"""E02 -- Cumulative collections per loan.

Question
--------
For each loan, a running total of the amount collected, ordered by payment date.
**Two instalments settled on the same day must each show the correct running
total for their own row.**

The trap
--------
`SUM(x) OVER (PARTITION BY p ORDER BY d)` has an implicit frame, and it is
`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` -- not `ROWS`. Under RANGE
the frame covers every row that is a *peer* of the current row under the ORDER
BY, so two payments dated the same day both see the whole day's total. You get
150 and 150 where the answer is 100 and 150.

This is the most quietly wrong of all the window-function defaults, because on
data with no ties the two frames agree exactly -- so it passes every test until
a real dataset contains two events on one day, which a payments table always
does.

Note the query deliberately does not return `amount`. Within a tie the physical
row order is arbitrary, so which row carries which running total is not
determined; the *multiset* of running totals is. The fixture asserts the part
that is actually defined.
"""

from datetime import date

VIEWS = {
    "silver_repayment_attempts": (
        "attempt_id string, loan_id string, paid_at date, amount double, status string",
        [
            # Two settlements on one day -- the whole exercise.
            ("A1", "L1", date(2026, 1, 5), 100.0, "SUCCESS"),
            ("A2", "L1", date(2026, 1, 5), 50.0, "SUCCESS"),
            ("A3", "L1", date(2026, 1, 20), 25.0, "SUCCESS"),
            # A bounce must not count towards collections at all.
            ("A4", "L1", None, 40.0, "BOUNCED"),
            # A second loan, so the PARTITION BY is doing something.
            ("A5", "L2", date(2026, 1, 9), 70.0, "SUCCESS"),
        ],
    ),
}

# Hand-computed, ordered by loan_id, paid_at, running_total.
#   L1  05 Jan  100          (first of the two same-day payments)
#   L1  05 Jan  100 + 50     = 150
#   L1  20 Jan  150 + 25     = 175
#   L2  09 Jan  70
EXPECTED = [
    ("L1", date(2026, 1, 5), 100.0),
    ("L1", date(2026, 1, 5), 150.0),
    ("L1", date(2026, 1, 20), 175.0),
    ("L2", date(2026, 1, 9), 70.0),
]
