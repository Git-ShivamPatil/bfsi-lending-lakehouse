"""E05 -- Longest delinquency streak.

Question
--------
For each loan, the longest run of consecutive month-ends spent at 1+ DPD, and
the month that run began. **A loan that was delinquent, cured, and relapsed has
two runs, not one** -- and a month with no snapshot row at all is a break in the
run, not a continuation of it.

The trap
--------
The textbook gaps-and-islands technique subtracts one `ROW_NUMBER` from another
and groups on the difference. It works perfectly, right up until a row is
*missing* rather than merely non-delinquent -- and then it welds two separate
spells into one, because both row numbers advance in step across the hole.

The fix is to island on the actual calendar distance between consecutive
delinquent snapshots rather than on their positions in a list.

The credit meaning is the follow-up question. An account that has rolled into
delinquency twice is a materially different risk from one that has been
continuously late for the same total number of months, and a memoryless cure
model cannot tell them apart -- which is a limitation this repository's own
generator has and documents.
"""

from datetime import date

JAN, FEB, MAR = date(2026, 1, 31), date(2026, 2, 28), date(2026, 3, 31)
APR, MAY, JUN = date(2026, 4, 30), date(2026, 5, 31), date(2026, 6, 30)

VIEWS = {
    "silver_loan_snapshot": (
        "loan_id string, snapshot_date date, dpd int",
        [
            # L1: delinquent Jan-Feb, cures in March, relapses Apr-Jun.
            # Two spells; the longer is three months from April.
            ("L1", JAN, 5),
            ("L1", FEB, 10),
            ("L1", MAR, 0),
            ("L1", APR, 3),
            ("L1", MAY, 7),
            ("L1", JUN, 12),

            # L2: delinquent in February and again in April, with NO March row
            # at all. Two spells of one month each -- but the row-number trick
            # sees them as adjacent and reports a single two-month streak.
            ("L2", JAN, 0),
            ("L2", FEB, 4),
            ("L2", APR, 6),
        ],
    ),
}

# Hand-computed.
#   L1  spells: {Jan,Feb} = 2, {Apr,May,Jun} = 3   -> longest 3, began 30 Apr
#   L2  spells: {Feb} = 1, {Apr} = 1               -> longest 1, began 28 Feb
EXPECTED = [
    ("L1", 3, APR),
    ("L2", 1, FEB),
]
