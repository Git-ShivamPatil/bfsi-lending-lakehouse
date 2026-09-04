"""E01 -- GNPA and PAR by month.

Question
--------
From `silver_loan_snapshot`, return for each snapshot_date the principal
outstanding, the NPA outstanding (dpd > 90) and GNPA as a ratio, excluding
written-off accounts. **Your output must be correct for a month in which no
account is yet 90+ DPD.**

The trap
--------
`SUM(CASE WHEN dpd > 90 THEN principal_outstanding END)` over a month with no
NPA returns NULL, not 0 -- so the ratio is NULL and the month reads as "no
data" rather than "no bad debt". On a young book that is most of the early
months, and it silently drops them out of any chart that filters nulls.

This one is not hypothetical: it was live in this repository's own
PORTFOLIO_SQL until the gold layer was rewritten.
"""

from datetime import date

JAN, FEB, MAR = date(2026, 1, 31), date(2026, 2, 28), date(2026, 3, 31)

VIEWS = {
    "silver_loan_snapshot": (
        "loan_id string, snapshot_date date, dpd int, "
        "principal_outstanding double, is_written_off boolean",
        [
            # January: a clean month. Nothing is 90+ yet -- this is the month
            # the naive query reports NULL for.
            ("L1", JAN, 0, 1000.0, False),
            ("L2", JAN, 45, 500.0, False),

            # February: L2 has rolled into NPA.
            ("L1", FEB, 0, 900.0, False),
            ("L2", FEB, 95, 500.0, False),

            # March: a written-off account, which must leave both sides of the
            # ratio, plus a fresh NPA.
            ("L1", MAR, 0, 800.0, False),
            ("L2", MAR, 125, 500.0, False),
            ("L3", MAR, 200, 300.0, True),
        ],
    ),
}

# Hand-computed.
#   JAN  os = 1000 + 500 = 1500,  npa = 0            -> 0.0
#   FEB  os = 900 + 500  = 1400,  npa = 500          -> 500/1400  = 0.357142857
#   MAR  os = 800 + 500  = 1300,  npa = 500          -> 500/1300  = 0.384615385
#        L3 is written off and appears in neither figure.
#   PAR-30 counts dpd > 30, so February is 500/1400 and January is 500/1500.
EXPECTED = [
    (JAN, 1500.0, 0.0, 0.0, 0.33333),
    (FEB, 1400.0, 500.0, 0.35714, 0.35714),
    (MAR, 1300.0, 500.0, 0.38462, 0.38462),
]
