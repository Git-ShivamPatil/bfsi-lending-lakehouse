# What the book actually says

A pipeline that produces no conclusion is a pipeline nobody needed. This is the
read of the 150,000-loan book at 2026-08-31 — the same one the dashboard
renders and the tests gate.

**Every finding below is labelled.** Some are statements about lending. Others
are statements about *this generator*, which is a different and much less
interesting thing, and presenting the second as the first is the single easiest
way to mislead with synthetic data:

- **`[BOOK]`** — a property of the modelled portfolio, and the kind of thing
  you would act on.
- **`[ARTIFACT]`** — a property of how the data was made. Real for this book,
  not evidence about real lending.
- **`[METHOD]`** — a finding about measurement rather than about the portfolio.

Reproduce any of it with `python -m validation.backtest --data data/raw --as-of
2026-08-31`, or from the gold tables after a pipeline run.

---

## 1. New-to-credit is an onboarding problem, not a credit problem `[BOOK]`

The headline reading of the bureau bands is unsurprising — thin and low files
are riskier:

| Score band | Loans | GNPA (by value) | First-payment default |
|---|---:|---:|---:|
| 300–649 | 8,767 | 1.13% | 1.19% |
| 650–699 | 11,855 | 0.84% | 0.99% |
| 700–749 | 15,153 | 0.56% | 0.72% |
| 750–799 | 10,052 | 0.60% | 0.52% |
| 800+ | 3,044 | 0.40% | 0.45% |
| **NTC** | **13,852** | **0.75%** | **1.20%** |

The interesting row is the last one. New-to-credit customers have the **highest
first-payment default of any band — above even the 300–649 sub-prime tier** —
and yet their seasoned GNPA lands mid-pack, better than 650–699.

Those two facts together say the NTC failure is concentrated at instalment one
and does not persist. That is not what a credit problem looks like. A credit
problem gets worse with exposure; this gets better. It looks like an
*activation* problem — a mandate that never registered, a first debit presented
against an account that was not ready.

The bounce-reason mix supports it: `MANDATE_NOT_REGISTERED` is **11.5%** of all
bounces, the second-largest reason after insufficient funds.

**What I would do with it.** Do not tighten NTC credit policy — the surviving
book says the underwriting is fine. Fix the mandate-registration flow for
first-time borrowers and re-present the first instalment rather than letting it
roll. On a checkout-finance book where thin-file customers are the growth
segment, treating this as a credit signal would shrink the funnel to solve a
plumbing problem.

---

## 2. The 750–799 inversion is a weighting artifact, not a paradox `[METHOD]`

Read the table above again: **750–799 has a worse GNPA than 700–749** (0.60% vs
0.56%), which inverts the whole point of a bureau score. This is exactly the
"credit score paradox" that gets written up as an insight.

It is not one. By **loan count** the bands order correctly:

| Score band | 90+ loans | Loans | 90+ rate by count | GNPA by value |
|---|---:|---:|---:|---:|
| 700–749 | 149 | 15,153 | 0.983% | 0.559% |
| 750–799 | 98 | 10,052 | 0.975% | 0.601% |

By count 750–799 is marginally *better*, as it should be. By value it is worse.
The entire inversion is a handful of large tickets in the 750–799 band going
bad — value-weighting amplifies them, count-weighting does not.

**What I would do with it.** Nothing, except report both. A single number that
flips sign depending on its weighting is not a finding, and the useful habit is
to compute a segment cut both ways before believing either. Provisioning is
value-weighted so the value view is the one that matters financially; policy is
per-application so the count view is the one that matters operationally.

---

## 3. The three-month product is the riskiest per rupee `[BOOK]`

| Tenure | Loans | GNPA (by value) |
|---|---:|---:|
| 3 months | 7,063 | **0.87%** |
| 6 months | 12,931 | 0.65% |
| 9 months | 18,405 | 0.63% |
| 12 months | 24,324 | 0.81% |

Risk is U-shaped in tenure, and the short end is the worse end — which is the
opposite of the intuition that a shorter loan is a safer loan.

The mechanism is exposure-weighted time. Delinquency hazard on this book is
heavily front-loaded: the first instalment carries roughly 2.3× the baseline
hazard and it decays from there. A three-month loan spends *its entire life* in
that early window, while a twelve-month loan amortises most of its balance
during the low-hazard tail. The twelve-month figure then rises again for the
ordinary reason — more instalments is more chances to miss.

**What I would do with it.** Price the three-month product as though it were
riskier than the six, because per rupee outstanding it is. If subvention is
negotiated per tenure, the three-month tier is the one where the merchant
discount is most likely to be under-covering losses.

---

## 4. Ticket size is not predictive — and that is the model, not the market `[ARTIFACT]`

Across value-based deciles from ₹5,468 to ₹26,688 and above, GNPA wanders
between 0.57% and 0.84% with no trend. D05 is the best decile and D08 the
worst, which is noise.

This is **not** a finding about lending. Ticket size does not enter the hazard
function in `generator/config.py` at all — the miss probability is a function of
bureau band, city tier and month-on-book, and nothing else. A book where ticket
size does not predict risk is a book that was built that way.

Real consumer-durable portfolios generally do show a ticket effect in both
directions: very small tickets skew to thinner files, very large ones to
stretch borrowers. Modelling it is on the list.

---

## 5. The configured city-tier risk has vanished into the noise `[ARTIFACT]`

`config.py` sets explicit tier multipliers — Tier 1 at 0.88, Tier 2 at 1.00,
Tier 3 at 1.14 — so Tier 3 should be the worst. Observed:

| City tier | Loans | GNPA |
|---|---:|---:|
| Tier 1 | 17,684 | 0.70% |
| Tier 2 | 23,157 | 0.76% |
| Tier 3 | 21,882 | 0.72% |

Tier 2 reads worst and Tier 3 sits in the middle. The configured ordering is not
recoverable from the output.

This is a **direct consequence of calibrating to a 2.0% write-off-inclusive
GNPA**: the book has to be clean enough that only ~735 accounts are 90+ at any
month end, spread across three tiers, and at those counts a 14% relative
difference in hazard is well inside sampling noise. The signal is really there
in the generator; the book is simply too small and too clean to see it.

Worth stating plainly because it cuts the other way too — if a segment cut on a
*real* book of this size showed a 14% effect, it would not be significant
either.

---

## 6. Nearly all of the merchant risk ranking is luck `[METHOD]`

The worst merchant in the book runs a **14.6% GNPA** against a book average of
0.73% — a twentyfold outlier, on 48 loans.

It is almost certainly nothing. The book-wide 90+ rate by count is 735 / 62,723
= **1.17%**, and merchants average about 45 live loans, so the *expected* number
of 90+ accounts per merchant is around **0.5**. A merchant with two bad loans —
which happens constantly by chance — lands at the top of a GNPA ranking.

A ranking is therefore the wrong instrument. What the gold layer computes
instead is a z-score against the merchant's own category mean, with a minimum
volume floor, which at least asks whether the deviation is large relative to
the spread. Even that is weak at 45 loans.

**What I would do with it.** Never open a merchant-risk conversation with a
sorted list. Set a volume floor, test against the category, and require the
signal to persist across two consecutive months before acting — a merchant
whose PAR spikes for one month and reverts was never the problem.

---

## 7. Merchant concentration is unrealistically flat `[ARTIFACT]`

| | Share of exposure |
|---|---:|
| Top 10 merchants | 1.2% |
| Top 50 | 5.4% |
| Top 100 | 10.1% |

Across 1,400 merchants the book is almost perfectly uniform, because loans are
assigned to merchants by uniform random choice.

A real checkout-finance book is nothing like this. Volume concentrates hard into
a few large brands and marketplaces, and that concentration is *why* merchant
risk analysis matters — a single large partner going bad is a portfolio event,
not a tail event. This book cannot demonstrate that, and the merchant-risk gold
table is consequently a shape rather than a result.

---

## 8. Credit quality is stationary across vintages `[ARTIFACT]`

Cumulative 30+ by months-on-book, by disbursal cohort:

| Cohort | MOB 3 | MOB 6 | MOB 9 | MOB 12 |
|---|---:|---:|---:|---:|
| 2024-09 | 1.12% | 2.21% | 2.91% | 3.33% |
| 2025-01 | 1.31% | 2.71% | 3.58% | 3.99% |
| 2025-05 | 1.19% | 2.66% | 3.30% | 3.90% |
| 2025-08 | 1.40% | 2.65% | 3.56% | 3.97% |
| 2025-11 | 1.36% | 2.58% | 3.40% | — |
| 2026-02 | 1.20% | 2.38% | — | — |

Twelve cohorts land between 3.33% and 3.99% at MOB 12. The curves are
well-formed — monotonic, properly cumulative, comparable at equal months on
book, which is the thing the vintage SQL exists to get right — but they are also
*flat*, because the generator's hazard parameters do not change over time.

Real vintage analysis is interesting precisely when the curves move: a policy
change, a new channel, or a growth push shows up as one cohort peeling away from
its neighbours. Nothing here peels away, so the triangle demonstrates correct
methodology on a portfolio with no story in it.

Making the hazard drift with cohort — and then detecting the drift — is the
single change that would make this table worth reading.

---

## What I would fix first, in order

1. **The mandate-registration path for new-to-credit borrowers** (§1). It is the
   only finding here that is both real and directly actionable, and it is worth
   more than any credit-policy change the rest of the data would support.
2. **Re-price the three-month tenure** (§3), or check that subvention on it
   covers the loss rate.
3. **Merchant concentration and cohort drift in the generator** (§7, §8). Until
   those exist, two of the gold tables demonstrate methodology rather than
   producing findings, and the README should keep saying so.
4. **A ticket-size term in the hazard** (§4), so the decile cut means something.

## What this analysis is not

It is a read of a **synthetic** book. Every customer, merchant, loan and
repayment is generated. The value of doing it at all is that the pipeline gets
exercised against the questions someone would really ask, and that the
`[ARTIFACT]` labels above are an honest inventory of where the model is thinner
than the reality it stands in for.

The one number here that is anchored to something published is GNPA, calibrated
to CRISIL's 2.0% for Snapmint Financial Services Pvt Ltd at 31 Dec 2025 — and
that anchoring is on the write-off-inclusive basis CRISIL states, which is a
different ratio from the on-book measure. See the README.
