"""Calibration constants for the synthetic lending book.

Every number here is either (a) sourced from a public document, cited inline, or
(b) an explicit modelling assumption, labelled as one. Nothing is invented and
presented as fact -- the point of a synthetic book is that a reader can audit the
assumptions, and an assumption you cannot defend is worse than no number at all.

The book models an Indian no-cost-EMI / checkout-finance lender: small tickets,
short tenures, merchant subvention, NACH/UPI-autopay collection.
"""

from __future__ import annotations

# --------------------------------------------------------------------------
# Sourced -- these have public references
# --------------------------------------------------------------------------

#: Target gross NPA at the end of the simulation window, as a share of closing
#: principal outstanding. CRISIL Ratings' rationale for Snapmint Financial
#: Services Pvt Ltd (16 Apr 2026) reports GNPA of 2.0% as at 31 Dec 2025 on an
#: AUM of Rs 615 crore. The generator back-tests against this in
#: `validation/backtest.py` and fails the build if it drifts more than
#: GNPA_TOLERANCE away.
TARGET_GNPA = 0.020
GNPA_TOLERANCE = 0.006

#: RBI SMA buckets for loans other than revolving facilities. Days-past-due
#: ranges per the IRACP master circular and the RBI clarification of
#: 12 Nov 2021, which also fixed that classification happens as part of the
#: day-end process and is stamped with the calendar date that process is run for.
SMA_BUCKETS = (
    ("SMA-0", 1, 30),
    ("SMA-1", 31, 60),
    ("SMA-2", 61, 90),
)
NPA_DPD_THRESHOLD = 90

#: Days past due at which an account is written off and leaves the active book.
#: Lenders' technical write-off policies vary and Snapmint publishes none; 180 DPD
#: is the common small-ticket unsecured convention. This matters more than it
#: looks: without a write-off rule, defaulted loans accumulate in the book for
#: ever and GNPA drifts up without limit, which is the single most common way a
#: synthetic lending book gives itself away.
WRITE_OFF_DPD = 180

#: The book grows month on month rather than being a fixed cohort. Snapmint's
#: disbursements ran Rs 1,760 crore in 9M FY26 against an AUM of Rs 615 crore
#: (CRISIL, 31 Dec 2025) -- a book turning over roughly three times a year and
#: still growing. Without growth, short-tenure loans mature and close while
#: defaults persist, and the GNPA denominator collapses.
MONTHLY_GROWTH_RATE = 0.055

#: Standard delinquency buckets used in Indian portfolio reporting. `0` is
#: current; the rest are inclusive day ranges. 90+ is open-ended.
DPD_BUCKETS = (
    ("CURRENT", 0, 0),
    ("1-30", 1, 30),
    ("31-60", 31, 60),
    ("61-90", 61, 90),
    ("90+", 91, 10_000),
)

#: Tenures Snapmint documents on its own product pages.
TENURES_MONTHS = (3, 6, 9, 12)

#: Under the RBI (Digital Lending) Directions, 2025 (issued 8 May 2025), a
#: Default Loss Guarantee is capped at 5% of the total disbursed amount of the
#: specified loan portfolio. The gold layer reports DLG utilisation against this
#: cap; it is a reporting constant, not a simulation input.
DLG_CAP_PCT = 0.05

# --------------------------------------------------------------------------
# Modelling assumptions -- NOT sourced. Documented so a reader can disagree.
# --------------------------------------------------------------------------

#: Snapmint does not publish an average ticket size, so this is assumed from the
#: consumer-durables/no-cost-EMI segment generally: a right-skewed distribution
#: with most tickets between Rs 3,000 and Rs 60,000. Implemented as a lognormal
#: on the natural log of rupees.
TICKET_LOG_MEAN = 9.4          # exp(9.4) ~ Rs 12,100 median
TICKET_LOG_SIGMA = 0.62
TICKET_FLOOR = 1_500
TICKET_CEILING = 250_000

#: Bureau score bands and their share of originations. Assumed. A checkout
#: lender skews to thin-file and near-prime customers relative to a bank.
#: `pd_multiplier` scales the baseline hazard of a missed instalment.
SCORE_BANDS = (
    # label,        share,  score range,     pd_multiplier
    ("NTC",         0.22,   (None, None),    1.90),   # new to credit, no score
    ("300-649",     0.14,   (300, 649),      2.60),
    ("650-699",     0.19,   (650, 699),      1.45),
    ("700-749",     0.24,   (700, 749),      0.80),
    ("750-799",     0.16,   (750, 799),      0.42),
    ("800+",        0.05,   (800, 900),      0.22),
)

#: Baseline probability that a scheduled instalment fails at first presentation,
#: before the score multiplier and the month-on-book shape are applied. Assumed.
#: NACH debit returns run materially higher than this across the industry, but
#: most bounces on a small-ticket EMI book cure within the same cycle, so this
#: is the hazard of *entering* delinquency, not of a single failed presentation.
BASE_INSTALMENT_MISS_RATE = 0.0075

#: Month-on-book shape for that hazard. Early-life defaults dominate a
#: checkout-finance book: the first instalment carries mandate-registration
#: failures, and risk decays as the customer demonstrates repayment. Index 0 is
#: the first instalment. Assumed.
MOB_HAZARD_SHAPE = (2.35, 1.30, 1.00, 0.86, 0.78, 0.72, 0.68, 0.65, 0.62, 0.60, 0.58, 0.57)

#: Probability a delinquent account cures in a given month, by the bucket it is
#: sitting in. Curing gets harder the deeper the bucket -- the classic roll-rate
#: shape. Assumed, but calibrated so the resulting GNPA lands near TARGET_GNPA.
CURE_RATES = {
    "1-30": 0.62,
    "31-60": 0.34,
    "61-90": 0.19,
    "90+": 0.05,
}

#: Collection modes and their share of attempts. Assumed.
COLLECTION_MODES = (
    ("NACH", 0.58),
    ("UPI_AUTOPAY", 0.31),
    ("MANUAL", 0.11),
)

#: Bounce reason codes, sampled when a presentation fails. These mirror the
#: NPCI return-reason vocabulary in shape; the exact code set a lender sees
#: depends on its sponsor bank, so treat these as representative.
BOUNCE_REASONS = (
    ("INSUFFICIENT_FUNDS", 0.61),
    ("MANDATE_NOT_REGISTERED", 0.11),
    ("ACCOUNT_CLOSED", 0.07),
    ("PAYMENT_STOPPED", 0.06),
    ("TECHNICAL_DECLINE", 0.09),
    ("SIGNATURE_MISMATCH", 0.06),
)

#: Festive seasonality on origination volume, by calendar month (1 = January).
#: Indian consumer-durable financing peaks across the Sep-Nov festive window.
#: Assumed multipliers on a flat baseline.
MONTH_SEASONALITY = {
    1: 0.88, 2: 0.84, 3: 0.95, 4: 0.98, 5: 1.02, 6: 0.94,
    7: 0.92, 8: 1.05, 9: 1.34, 10: 1.62, 11: 1.28, 12: 1.06,
}

#: Merchant category mix. Assumed, shaped to a checkout-finance book.
MERCHANT_CATEGORIES = (
    ("ELECTRONICS", 0.31),
    ("MOBILE", 0.24),
    ("FASHION", 0.14),
    ("FURNITURE", 0.11),
    ("JEWELLERY", 0.08),
    ("TRAVEL", 0.06),
    ("EDUCATION", 0.06),
)

#: City tiers, share of book, and a small risk adjustment. Snapmint documents a
#: tier-2/3 skew. Shares assumed.
CITY_TIERS = (
    ("TIER_1", 0.28, 0.88),
    ("TIER_2", 0.37, 1.00),
    ("TIER_3", 0.35, 1.14),
)

#: Merchant subvention on a no-cost EMI: the merchant funds the interest so the
#: customer pays only principal. Assumed range, sampled per loan.
SUBVENTION_PCT_RANGE = (0.04, 0.14)

#: Annualised percentage rate charged on interest-bearing (not no-cost) loans.
APR_RANGE = (0.16, 0.34)

#: Share of originations that are no-cost EMI rather than interest-bearing.
NO_COST_EMI_SHARE = 0.71

# --------------------------------------------------------------------------
# Deliberate data-quality defects
# --------------------------------------------------------------------------
#
# A synthetic book with no defects cannot demonstrate a validation layer, and a
# pipeline that has never seen bad data proves nothing. These are injected at
# known rates so the validation suite has a ground truth to be scored against:
# `validation/` must find them, and the expected counts are asserted in tests.

DEFECT_RATES = {
    #: pincode blank -- completeness failure
    "missing_pincode": 0.011,
    #: repayment row emitted twice with the same natural key -- the case that
    #: makes a naive MERGE INTO non-deterministic
    "duplicate_repayment": 0.004,
    #: paid_at after the extract date -- timeliness/accuracy failure
    "future_dated_repayment": 0.0015,
    #: repayment whose loan_id is absent from the loan master -- referential
    #: integrity failure across entities
    "orphan_repayment": 0.0022,
    #: bureau score outside the valid 300-900 domain
    "score_out_of_range": 0.0040,
    #: instalment amount that does not reconcile to principal + interest
    "emi_reconciliation_break": 0.0026,
    #: mojibake in a merchant name -- encoding failure that survives ingestion
    "merchant_name_mojibake": 0.006,
    #: negative principal -- domain violation
    "negative_principal": 0.0020,
}

#: Bumping this invalidates cached generated data; the pipeline records it
#: alongside every bronze load so a table can be traced to the generator that
#: produced it.
GENERATOR_VERSION = "1.0.0"
