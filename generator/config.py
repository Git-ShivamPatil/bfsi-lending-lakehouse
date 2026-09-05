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

#: Target gross NPA at the end of the simulation window. CRISIL Ratings'
#: rationale for Snapmint Financial Services Pvt Ltd (16 Apr 2026) reports GNPA
#: of 2.0% as at 31 Dec 2025 on an AUM of Rs 615 crore.
#:
#: The rationale says it plainly and without qualification: "Its GNPA improved
#: from ~3.1% as on March 31, 2025, to ~2.0% as on December 31, 2025". That is
#: an ordinary gross NPA ratio -- 90+ DPD over gross advances still on the book
#: -- and it is what `gnpa_pct` is gated against.
#:
#: **There is a second, different ratio in the same document, and confusing the
#: two is the trap.** CRISIL separately reports "90+ dpd including last 12
#: months write-offs **/ Disbursements**" at 2.7% (Dec 25), 4.8% (Mar 25) and
#: 6.3% (Mar 24). Its denominator is *disbursements for the period*, not
#: advances, so it is a loss-rate-on-origination measure and not a GNPA at all.
#: A reading that folds write-offs into a gross-advances denominator and matches
#: the result to the 2.0% belongs to neither ratio; this project did exactly
#: that for one commit and made the book roughly three times too clean before
#: the primary source was re-read.
TARGET_GNPA = 0.020
GNPA_TOLERANCE = 0.006

#: The window of write-offs the supplementary CRISIL ratio folds in, in days.
GNPA_WRITE_OFF_LOOKBACK_DAYS = 365

#: CRISIL's supplementary ratio, reported alongside the GNPA above but *not*
#: gated on, because its denominator convention is ambiguous from the outside:
#: "Disbursements" for a nine-month reporting period could be the period figure
#: or a trailing twelve months, and the two differ materially on a book growing
#: this fast. `validation/backtest.py` computes it on a trailing-twelve-month
#: denominator and says so. Reported for shape, not matched to a target.
TARGET_WRITE_OFF_RATIO_ON_DISBURSEMENTS = 0.027

#: Special-mention-account tagging. The SMA table for an NBFC lives in the
#: **Reserve Bank of India (Non-Banking Financial Companies - Resolution of
#: Stressed Assets) Directions, 2025** -- RBI/DOR/2025-26/357,
#: DOR.STR.REC.276/21.04.048/2025-26, 28 November 2025 -- at para 18:
#:
#:     A NBFC shall recognise incipient stress in loan accounts, immediately on
#:     default, by classifying such assets as special mention accounts (SMA) as
#:     per the following categories:
#:         SMA-0   Up to 30 days
#:         SMA-1   More than 30 days and up to 60 days
#:         SMA-2   More than 60 days and up to 90 days
#:
#: Two details this project got wrong before checking, both of which are the
#: bank/NBFC conflation an interviewer looks for:
#:
#: * The SMA table is **not** in the IRACP instrument, and never was -- it comes
#:   from the resolution-of-stressed-assets line, originally the June 2019
#:   Prudential Framework. IRACP (RBI/DOR/2025-26/356) carries the NPA rules.
#: * There is **no "loans other than revolving facilities" qualifier for an
#:   NBFC**. The word "revolving" appears in neither 2025 NBFC instrument; the
#:   two-column split is the *bank* table (RBI/DOR/2025-26/165, para 15). The
#:   NBFC table is a single column keyed only on days overdue.
#:
#: The day counts are stated as inclusive integer ranges here because that is
#: what the code needs; RBI's own boundary wording is "up to" / "more than ...
#: and up to", which coincides for whole days.
SMA_BUCKETS = (
    ("SMA-0", 1, 30),
    ("SMA-1", 31, 60),
    ("SMA-2", 61, 90),
)

#: Days overdue at which an asset becomes an NPA -- **and it is not simply 90
#: for this entity.** Para 43 of the IRACP Directions, 2025 (RBI/DOR/2025-26/356,
#: DOR.STR.REC.No.275/21.04.048/2025-26, 28 November 2025, updated to 1 July
#: 2026) sets the NBFC-Base-Layer threshold at more than 180 days, and para 44
#: phases it down:
#:
#:     more than 150 days   by 31 March 2024
#:     more than 120 days   by 31 March 2025
#:     more than  90 days   by 31 March 2026
#:
#: NBFC-Middle and Upper Layer are at 90 days already (para 51). The layer test
#: is **asset size, not AUM** -- Base Layer is non-deposit-taking NBFCs below
#: Rs 1,000 crore of assets (Scale Based Regulation Directions, 2025,
#: RBI/DOR/2025-26/339, para 10) -- and the modelled entity sits there on
#: ~Rs 938 crore of total assets, not on its ~Rs 615 crore AUM. Reaching the
#: right layer by the wrong measure is a standard interview trap.
#:
#: So a book spanning 2024-2026 cannot stamp NPA at 90 days throughout without
#: being anachronistic for most of its own window. `NPA_DPD_GLIDE_PATH` is
#: applied by `pipeline/silver/snapshot.py` to `asset_classification`.
NPA_DPD_GLIDE_PATH = (
    ("2024-03-31", 150),
    ("2025-03-31", 120),
    ("2026-03-31", 90),
)

#: The fixed 90-day threshold, used for the *delinquency* measures -- the 90+
#: bucket, GNPA, PAR-90 -- as distinct from regulatory asset classification.
#: This is deliberate rather than lazy: CRISIL reports Snapmint's GNPA on a 90+
#: dpd basis, and the calibration gate compares against that figure, so the risk
#: metric has to stay on a fixed 90 days even while the regulatory
#: classification follows the glide path above. The two answer different
#: questions and the repo keeps them apart.
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

#: Ind AS 109 expected-credit-loss staging, keyed off days past due.
#: Stage 1 is performing (12-month ECL); Stage 2 is a significant increase in
#: credit risk (lifetime ECL); Stage 3 is credit-impaired, which lines up with
#: the 90-DPD NPA threshold above.
ECL_STAGE_DPD = (
    ("STAGE_1", 0, 30),
    ("STAGE_2", 31, 90),
    ("STAGE_3", 91, 10_000),
)

#: PD and LGD by stage. **Assumed**, and the weakest numbers in this file: a real
#: implementation derives PD from the observed roll-rate matrix and LGD from
#: realised recoveries, neither of which this book models. They are here so the
#: staging produces a provision figure with the right shape, not so the figure
#: can be quoted.
ECL_PARAMETERS = {
    "STAGE_1": {"pd": 0.030, "lgd": 0.65},
    "STAGE_2": {"pd": 0.280, "lgd": 0.65},
    "STAGE_3": {"pd": 1.000, "lgd": 0.65},
}

# --------------------------------------------------------------------------
# Modelling assumptions -- NOT sourced. Documented so a reader can disagree.
# --------------------------------------------------------------------------

#: Ticket size. The *shape* is an assumption -- a lognormal on the natural log of
#: rupees, which is the usual form for consumer-durable tickets -- but the mean
#: it has to hit is sourced, and the sourcing needs care because the rationale
#: contains two different ticket numbers:
#:
#:   "average ticket size ranging from Rs 3,500 to Rs 25,000"
#:   "As of December 31, 2025, the average ticket size for the overall
#:    portfolio was Rs 3,500."
#:
#: The first is a range *across products*. The second is the portfolio average,
#: and it is the one a book-level mean has to match. Reading the range as a band
#: the mean may sit anywhere inside lets the book run four times too large --
#: which it did, at Rs 14,604.
#:
#: Arithmetic settles it independently of the wording. At Rs 3,500 the published
#: disbursement series (Rs 634 cr FY24, Rs 1,177 cr FY25, Rs 1,760 cr 9M FY26)
#: implies roughly 10.2 million loans, consistent with CRISIL's "15+ million
#: transactions" since inception. At Rs 14,604 it implies 2.4 million, which
#: cannot be reconciled with the same document.
TICKET_LOG_MEAN = 7.97         # exp(7.97) ~ Rs 2,890 median
TICKET_LOG_SIGMA = 0.62
TICKET_FLOOR = 500
TICKET_CEILING = 60_000

#: Sourced. CRISIL Ratings, Snapmint Financial Services Pvt Ltd, 16 Apr 2026:
#: the overall-portfolio average ticket at 31 Dec 2025. The realised mean ticket
#: of the generated book is gated against this, with a tolerance wide enough to
#: absorb the lognormal's sampling noise but not a mis-set mean.
TARGET_ATS = 3_500
ATS_TOLERANCE = 400

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
#:
#: This is the calibration lever: it is the one parameter tuned to hit
#: TARGET_GNPA, and everything else in this file is held fixed while it moves.
#: It briefly went to 0.0016 while the project was matching a write-off-inclusive
#: ratio against the plain GNPA -- two different measures, and the mistake made
#: the book about three times too clean before the rating rationale was re-read
#: word for word. Ticket size does not enter the hazard, so the two calibration
#: gates are independent and can be tuned one at a time.
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
