# What the book actually says

A pipeline that produces no conclusion is a pipeline nobody needed. This is the
read of the 150,000-loan book at 2026-08-31 — the same one the dashboard renders
and the tests gate.

**Every finding below is labelled**, because with synthetic data the labels are
the analysis:

- **`[BOOK]`** — a property of the modelled portfolio worth acting on.
- **`[ARTIFACT]`** — a property of how the data was made. Real for this book,
  not evidence about real lending.
- **`[METHOD]`** — a finding about measurement rather than about the portfolio.

An earlier version of this document was mostly `[ARTIFACT]`, and said so: ticket
size did not predict risk because ticket size did not enter the model, merchant
concentration was flat because merchants were drawn uniformly, and every vintage
cohort lay on top of every other because nothing drifted. Those were honest
labels on a thin model. The model was then thickened, and most of what follows
has moved from `[ARTIFACT]` to `[BOOK]`.

Reproduce any of it with `python -m validation.backtest --data data/raw --as-of
2026-08-31`, or from the gold tables after a pipeline run.

---

## 1. The measurement chain recovers the injected risk ordering exactly `[METHOD]`

`generator/config.py` assigns each bureau band a hazard multiplier — 2.60 for
300–649, 1.90 for new-to-credit, down to 0.22 for 800+. Between those and a GNPA
number sit a decisioning funnel, a delinquency state machine, a cure model, a
180-day write-off rule, month-end snapshotting and a value-weighted ratio.

| Score band | Hazard multiplier | Loans | GNPA | First-payment default |
|---|---:|---:|---:|---:|
| 300–649 | 2.60 | 6,149 | **3.97%** | 6.76% |
| NTC | 1.90 | 11,469 | 2.72% | 5.49% |
| 650–699 | 1.45 | 12,052 | 2.21% | 4.01% |
| 700–749 | 0.80 | 17,458 | 1.52% | 2.33% |
| 750–799 | 0.42 | 12,463 | 0.94% | 1.45% |
| 800+ | 0.22 | 3,844 | 0.71% | 0.94% |

All six bands come out in the configured order, on both measures. That is a
round-trip test of the whole stack, and it is the most useful thing this cut
shows — not a discovery about credit, but evidence that nothing between the
generator and the gold layer scrambles the signal.

Note the loan counts are no longer proportional to the band shares in the config:
approval rate varies by band, so 300–649 applies as often as before and appears
far less often in the book. The funnel is doing what a funnel does.

---

## 2. New-to-credit is an onboarding problem, not a credit problem `[BOOK]`

This is the finding to act on, and it only exists because first-payment default
and lifetime default can now move independently.

| Score band | FPD | GNPA | **FPD ÷ GNPA** |
|---|---:|---:|---:|
| NTC | 5.49% | 2.72% | **2.02** |
| 650–699 | 4.01% | 2.21% | 1.82 |
| 300–649 | 6.76% | 3.97% | 1.70 |
| 700–749 | 2.33% | 1.52% | 1.53 |
| 750–799 | 1.45% | 0.94% | 1.54 |
| 800+ | 0.94% | 0.71% | **1.33** |

New-to-credit borrowers fail at instalment one **twice as often as their
lifetime default rate would predict** — a ratio of 2.02 against 1.33 for the
prime band. Their absolute GNPA is mid-pack, better than 300–649. So the NTC
failure is concentrated at the first instalment and does not persist.

That is not what a credit problem looks like. A credit problem gets worse with
exposure; this gets better. It is an *activation* problem — an e-mandate that
never registered, a first debit presented against an account that was not ready.

**What I would do with it.** Do not tighten NTC credit policy; the surviving book
says the underwriting is fine, and NTC is the growth segment for a checkout
lender. Fix mandate registration for first-time borrowers and re-present
instalment one rather than letting it roll. Treating this as a credit signal
would shrink the funnel to solve a plumbing problem.

**Why this is trustworthy here and was not before.** In an earlier version FPD
was a near-constant 1.3–1.6× multiple of GNPA across every band — the signature
of a single shared hazard measured twice. The spread is now 1.33 to 2.02, and it
is widest exactly where a real checkout lender sees it.

---

## 3. The three-month product is the riskiest per rupee `[BOOK]`

| Tenure | Loans | GNPA |
|---|---:|---:|
| 3 months | 7,398 | **3.05%** |
| 6 months | 13,053 | 2.02% |
| 9 months | 18,395 | 1.86% |
| 12 months | 24,589 | 1.58% |

Risk falls monotonically with tenure, and the short end is nearly **twice** the
long end — the opposite of the intuition that a shorter loan is a safer loan.

The mechanism is exposure-weighted time. Delinquency hazard is heavily
front-loaded: the first instalment carries about 2.3× the baseline and decays
from there. A three-month loan spends *its entire life* inside that window; a
twelve-month loan amortises most of its balance through the quiet tail.

**What I would do with it.** Price the three-month tier as though it were riskier
than the six, because per rupee outstanding it is. If merchant subvention is
negotiated per tenure, that is the tier where the discount is most likely to be
under-covering losses.

---

## 4. Larger tickets are riskier, and the gradient is shallow `[BOOK]`

| Ticket decile | GNPA |
|---|---:|
| D01 (smallest) | 1.61% |
| D03 | 1.49% |
| D05 | 2.17% |
| D08 | 1.98% |
| D10 (largest) | **2.17%** |

A ~35% relative gradient from the bottom decile to the top, with real noise in
the middle — D03 is the cleanest decile and D05 is as bad as D10.

Two things follow. The direction is genuine: a larger ticket stretches a
small-ticket borrower, and it is in the model deliberately. But the ordering
between adjacent deciles is not — at ~130 bad accounts per decile, a 20%
difference between neighbours is inside the noise, and reading D03 as "our best
segment" would be over-fitting to a sample.

**What I would do with it.** Use the top-versus-bottom contrast, not the decile
ranking. If a ticket-size policy is worth having, it is worth having on a cut
coarse enough to be stable.

---

## 5. Underwriting has improved, and the vintage triangle now shows it `[BOOK]`

Cumulative 30+ by months-on-book, by disbursal cohort:

| Cohort | MOB 3 | MOB 6 | MOB 9 | MOB 12 |
|---|---:|---:|---:|---:|
| 2024-09 | 5.46% | 8.68% | 10.24% | **11.17%** |
| 2025-01 | 4.32% | 7.12% | 8.83% | 9.45% |
| 2025-05 | 4.41% | 7.25% | 8.57% | 9.61% |
| 2025-09 | 3.72% | 6.23% | 7.61% | — |
| 2026-01 | 3.86% | 5.99% | — | — |

Each cohort sits below the one before it at equal months-on-book. The
September-2024 book reaches 11.2% ever-30+ by MOB 12; the January-2025 book
reaches 9.4%. At MOB 3 the series runs 5.46% → 4.32% → 4.41% → 3.72% → 3.86%.

That is what a vintage triangle is *for*. A curve peeling away from its
neighbours is a policy change, a channel change or a growth push showing up in
the data, and the whole methodological apparatus — equal months-on-book,
cumulative rather than point-in-time, a fixed cohort denominator — exists to make
that comparison legitimate.

The direction is calibrated to something published: Snapmint's own GNPA improved
from 6.3% to 3.1% to 2.0% across FY24 to 9M FY26. The *magnitude* of the drift is
an assumption.

---

## 6. Most of the merchant risk ranking is still luck `[METHOD]`

The worst merchant runs a **12.8% GNPA** against a book average of 1.94% — on 30
loans.

Concentration has made this better than it was, not solved. The largest partners
now carry enough volume for their rates to mean something: the fourth-worst
merchant has 82 loans, which is a usable sample. But the *top* of the ranking is
still occupied by merchants near the volume floor, where two bad accounts
produce a double-digit rate.

A sorted list is therefore still the wrong instrument. What the gold layer
computes instead is a z-score against the merchant's own category mean with a
minimum volume floor.

**What I would do with it.** Never open a merchant conversation with a sorted
list. Set a volume floor, test against the category, and require the signal to
persist across two consecutive months. A merchant whose PAR spikes once and
reverts was never the problem.

---

## 7. Merchant concentration is now realistic in shape and unverified in degree `[ARTIFACT]`

| | Share of exposure |
|---|---:|
| Top 10 merchants | **67.8%** |
| Top 50 | 84.4% |
| Top 100 | 89.2% |

Volume follows a power law, which is the right *shape*: a real checkout book
concentrates hard into a few anchor brands, and that concentration is why
merchant risk is a portfolio question rather than a tail one.

The *degree* is an assumption and this is probably too concentrated. Snapmint
publishes a merchant count (1,500+ brands) but no volume distribution, so there
is nothing to calibrate against. The previous version had the top ten holding
1.2% of exposure, which was clearly wrong in the other direction; 67.8% is
plausible for a book anchored on a handful of large partners and is not evidence
that it is right. The Zipf exponent is a dial, and it is labelled as one.

---

## 8. Several segment cuts still have no statistical power `[METHOD]`

The configured city-tier gradient is 14% — Tier 1 at 0.88, Tier 3 at 1.14 — and
at 150,000 loans that difference is around two standard errors on the 90+ counts.
It does not reliably come back out in the right order.

This is worth stating because it cuts both ways. A 14% effect that fails to
appear is not evidence the effect is absent; and if a segment cut on a *real*
book of this size showed a 14% difference, it would not be significant either.

**What I would do with it.** Do the arithmetic before the interpretation. The
expected count, not the observed rate, tells you whether a segment cut can carry
a decision.

---

## What I would fix next

1. **Model presentation-level retries** separately from delinquency entry. The
   bounce rate reads 4.3% of attempts where Indian NACH returns run far higher,
   because a bounce that cures within its own cycle is never emitted.
2. **Let decline reasons depend on the policy that produced them.** Approval rate
   varies by bureau band; the *reason* for a given decline is drawn from one
   fixed mix, so it cannot be segmented the way a real rule engine's output could.
3. **Calibrate the concentration exponent** against something published, or state
   more loudly that it is a dial.

## What this analysis is not

It is a read of a **synthetic** book. Every application, customer, merchant, loan
and repayment is generated. The value in doing it at all is that the pipeline
gets exercised against the questions someone would really ask, and that the
labels are an honest inventory of where the model is thinner than the reality it
stands in for.

Three numbers are anchored to published figures — GNPA at 2.0%, average ticket at
₹3,500, and the write-off ratio on disbursements at 2.7%, all from CRISIL's
16 Apr 2026 rationale for Snapmint Financial Services Pvt Ltd. The first two are
gated in CI. The third is not, which is what makes it the most interesting of the
three: it is computed and reported without ever being optimised against, and it
lands at 2.761%.
