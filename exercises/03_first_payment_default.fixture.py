"""E03 -- First-payment default by merchant.

Question
--------
For each merchant, the share of its loans whose **first instalment was never
settled**. Rank merchants by that rate.

The trap
--------
`NOT IN (subquery)` returns *no rows at all* when the subquery yields a single
NULL. The comparison `loan_id <> NULL` is UNKNOWN rather than TRUE, and
`NOT IN` needs every comparison to be TRUE, so the whole predicate collapses to
UNKNOWN for every candidate row.

That produces a clean-looking **0% first-payment default** across the entire
book -- a number nobody questions, because a low FPD is what everyone hopes to
see. `NOT EXISTS` uses different null semantics and returns the true answer.

The NULL is not contrived: this repository's generator deliberately injects
orphan repayment rows, which is exactly how a NULL or unmatched `loan_id` gets
into a settlement subquery in real life.

Merchant-originated FPD is also the most Snapmint-shaped question in the pack.
On a checkout-finance book a merchant whose loans stop paying at instalment one
is usually a fraud signal, not a credit one.
"""

VIEWS = {
    "silver_loans": (
        "loan_id string, merchant_id string",
        [
            ("L1", "M1"),
            ("L2", "M1"),
            ("L3", "M1"),
            ("L4", "M2"),
            ("L5", "M2"),
        ],
    ),
    # Settlements of instalment 1 only. The NULL loan_id is the orphan row.
    "silver_first_instalment_settled": (
        "loan_id string",
        [
            ("L1",),
            ("L4",),
            (None,),
        ],
    ),
}

# Hand-computed.
#   M1: loans L1, L2, L3. Only L1 settled -> 2 of 3 defaulted = 0.66667
#   M2: loans L4, L5. Only L4 settled     -> 1 of 2 defaulted = 0.5
#   Ranked by fpd_rate descending: M1 then M2.
EXPECTED = [
    ("M1", 3, 2, 0.66667, 1),
    ("M2", 2, 1, 0.5, 2),
]
