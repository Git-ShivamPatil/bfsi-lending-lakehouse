# What the book actually says

A pipeline that produces no conclusion is a pipeline nobody needed. This is the
read of the 150,000-loan book at 2026-08-31 — the same one the dashboard renders
and the tests gate.

**Every finding below is labelled**, because with synthetic data the labels are
the analysis. Some of these are statements about lending; more of them are
statements about *this generator*, and presenting the second sort as the first is
the easiest way to mislead with a book you made yourself:

- **`[BOOK]`** — a property of the modelled portfolio worth acting on.
- **`[ARTIFACT]`** — a property of how the data was made. Real for this book,
  not evidence about real lending.
- **`[METHOD]`** — a finding about measurement rather than about the portfolio.

Reproduce any of it with `python -m validation.backtest --data data/raw --as-of
2026-08-31`, or from the gold tables after a pipeline run.

---

## 1. The measurement chain recovers the injected risk ordering exactly `[METHOD]`

`generator/config.py` assigns each bureau band a hazard multiplier — 2.60 for
300–649, 1.90 for new-to-credit, down to 0.22 for 800+. Those multipliers act on
a per-instalment miss probability, three layers below anything the gold layer
sees. Between them and a GNPA number sit a delinquency state machine, a cure
model, a 180-day write-off rule, month-end snapshotting and a value-weighted
ratio.

The observed ordering:

| Score band | Hazard multiplier | Loans | GNPA | First-payment default |
|---|---:|---:|---:|---:|
| 300–649 | 2.60 | 8,897 | **3.26%** | 4.90% |
| NTC | 1.90 | 13,963 | 2.49% | 4.00% |
| 650–699 | 1.45 | 11,942 | 2.01% | 3.07% |
| 700–749 | 0.80 | 15,213 | 1.14% | 1.78% |
| 750–799 | 0.42 | 10,082 | 0.92% | 1.22% |
| 800+ | 0.22 | 3,040 | 0.47% | 0.63% |

**All six bands come out in the configured order, on both measures.** That is a
round-trip test of the whole stack — hazard in, GNPA out — and it is the single
most useful thing this cut shows. It is not a discovery about credit; the
gradient was put there. It is evidence that nothing between the generator and the
gold layer is scrambling the signal.

**The interesting part is that the gradient compresses.** The hazard ratio
between the worst and best band is 11.8×. The realised GNPA ratio is **6.9×**.
Cure and write-off both act as compressors: worse accounts get more chances to
cure before 90 days, and the ones that do not are removed from the ratio
entirely at 180. A risk grade always looks flatter in outcome than in
underwriting, and this is that effect with a known ground truth attached.

---

## 2. The three-month product is the riskiest per rupee `[BOOK]`

| Tenure | Loans | GNPA |
|---|---:|---:|
| 3 months | 7,249 | **2.55%** |
| 6 months | 13,087 | 1.94% |
| 9 months | 18,499 | 1.60% |
| 12 months | 24,302 | 1.72% |

Risk falls with tenure and then turns back up, and the short end is the worse
end — the opposite of the intuition that a shorter loan is a safer loan.

The mechanism is exposure-weighted time. Delinquency hazard here is heavily
front-loaded: the first instalment carries about 2.3× the baseline and it decays
from there. A three-month loan spends *its entire life* inside that window,
while a twelve-month loan amortises most of its balance during the quiet tail.
The twelve-month figure then rises again for the ordinary reason — more
instalments is more chances to miss.

**What I would do with it.** Price the three-month tier as though it were riskier
than the six. If merchant subvention is negotiated per tenure, that is the tier
where the discount is most likely to be under-covering losses.

---

## 3. Early default and lifetime default are the same signal here `[ARTIFACT]`

Across every bureau band, first-payment default is a near-constant multiple of
GNPA — between **1.3× and 1.6×**, with no pattern in the residual.

On a real checkout-finance book those two numbers separate, and the separation is
the useful part: a segment with high FPD and ordinary GNPA is failing at
onboarding — mandate registration, a first debit presented before the account is
ready — while a segment with ordinary FPD and high GNPA is failing at
underwriting. They call for opposite responses.

This book cannot make that distinction, because it has no distinct
first-instalment mechanism. The month-on-book hazard shape front-loads risk
smoothly for everyone; nothing models a mandate that never activated. The
`MANDATE_NOT_REGISTERED` bounce reason exists in the data — 10.9% of all
bounces — but it is sampled independently of everything else rather than
concentrating in first instalments where it belongs.

Giving the first instalment its own failure mode is the change that would make
this cut worth reading.

---

## 4. Ticket size does not predict risk — because the model says nothing about it `[ARTIFACT]`

Across value-based deciles from ₹1,307 to ₹6,393 and above, GNPA sits between
1.65% and 1.95% with no trend.

This is **not** a finding about lending. Ticket size does not enter the hazard
function at all: the miss probability is a function of bureau band, city tier and
month-on-book, and nothing else. A book where ticket does not predict risk is a
book that was built that way, and the flat decile curve is the proof rather than
the discovery.

The same applies to product type — no-cost EMI reads 1.88% against 1.73% for
interest-bearing, and the difference is noise, because product does not enter the
hazard either.

---

## 5. The configured city-tier gradient does not come back out `[METHOD]`

| City tier | Configured multiplier | Loans | GNPA | 90+ accounts |
|---|---:|---:|---:|---:|
| Tier 1 | 0.88 | 17,779 | 1.74% | 344 |
| Tier 2 | 1.00 | 23,333 | **1.97%** | 539 |
| Tier 3 | 1.14 | 22,025 | 1.78% | 468 |

Tier 3 is configured as the riskiest and reads *better* than Tier 2. Unlike §1,
where a 11.8× gradient survived the whole chain, a **14%** one does not.

The arithmetic says why. The difference between Tier 2 and Tier 3 is 71 accounts
on a base where the combined standard error is about 32 — roughly two standard
errors, which is neither clearly signal nor clearly noise. A 14% relative effect
needs several times this much mass before a segment cut can see it.

**What I would do with it.** Treat it as a power calculation, not a result. If a
segment cut on a *real* book of this size showed a 14% difference, it would not
be significant either, and reporting it as though the ordering meant something
is how spurious policy gets made.

---

## 6. Almost all of the merchant risk ranking is luck `[METHOD]`

The worst merchant in the book runs a **28.9% GNPA** against a book average of
1.84% — a fifteenfold outlier, on 38 loans.

It is almost certainly nothing. The book-wide 90+ rate by count is 1,351 / 63,137
= **2.14%**, and merchants average about 45 live loans, so the *expected* number
of 90+ accounts per merchant is around **one**. A merchant that happens to have
three or four lands at the top of a GNPA ranking, and with 1,400 merchants a
handful will always have three or four.

A sorted list is therefore the wrong instrument. What the gold layer computes
instead is a z-score against the merchant's own category mean with a minimum
volume floor, which at least asks whether the deviation is large relative to the
spread — though even that is weak at 45 loans.

**What I would do with it.** Never open a merchant conversation with a sorted
list. Set a volume floor, test against the category, and require the signal to
persist across two consecutive months before acting. A merchant whose PAR spikes
once and reverts was never the problem.

---

## 7. Merchant concentration is unrealistically flat `[ARTIFACT]`

| | Share of exposure |
|---|---:|
| Top 10 merchants | 1.2% |
| Top 50 | 5.4% |
| Top 100 | 10.2% |

Across 1,400 merchants the book is almost perfectly uniform, because loans are
assigned to merchants by uniform random choice.

A real checkout-finance book is nothing like this. Volume concentrates hard into
a few large brands, and that concentration is *why* merchant risk analysis
matters: one large partner going bad is a portfolio event, not a tail event. This
book cannot demonstrate that, so the merchant-risk gold table is a shape rather
than a result — and §6 is the reason it could not carry weight even if it were.

---

## 8. Credit quality is stationary across vintages `[ARTIFACT]`

Cumulative 30+ by months-on-book, by disbursal cohort:

| Cohort | MOB 3 | MOB 6 | MOB 9 | MOB 12 |
|---|---:|---:|---:|---:|
| 2024-09 | 3.84% | 5.90% | 7.36% | 8.26% |
| 2025-01 | 3.07% | 5.53% | 6.96% | 7.68% |
| 2025-05 | 2.84% | 5.33% | 7.09% | 8.03% |
| 2025-08 | 3.28% | 6.02% | 7.63% | 8.32% |
| 2025-11 | 3.48% | 5.79% | 7.34% | — |
| 2026-02 | 3.80% | 5.87% | — | — |

Twelve cohorts land between 7.7% and 8.3% at MOB 12. The curves are well
formed — monotonic, properly cumulative, comparable at equal months on book,
which is the whole thing the vintage SQL exists to get right — and they are also
*flat*, because the generator's hazard parameters do not change over time.

Real vintage analysis earns its keep when the curves move: a policy change, a new
acquisition channel or a growth push shows up as one cohort peeling away from its
neighbours. Nothing peels away here, so the triangle demonstrates correct
methodology on a portfolio with no story in it.

---

## What I would fix first, in order

1. **Give the first instalment its own failure mode** (§3). It is the single
   change that would turn FPD from a scaled copy of GNPA into an independent
   signal, and first-payment default is the metric a checkout lender watches
   most closely.
2. **Concentrate merchant volume and let hazard vary by merchant** (§6, §7).
   Two gold tables currently demonstrate methodology rather than produce
   findings, and they will keep doing so until a merchant can actually be bad.
3. **Let hazard drift by cohort** (§8), so the vintage triangle has something to
   show.
4. **Put ticket size into the hazard** (§4), so the decile cut means something.

## What this analysis is not

It is a read of a **synthetic** book. Every customer, merchant, loan and
repayment is generated. The value in doing it at all is that the pipeline gets
exercised against the questions someone would really ask, and that the
`[ARTIFACT]` labels are an honest inventory of where the model is thinner than
the reality it stands in for.

Two numbers here are anchored to something published — GNPA at 2.0% and the
average ticket at ₹3,500, both from CRISIL's 16 Apr 2026 rationale for Snapmint
Financial Services Pvt Ltd. Reading those two correctly took more care than
computing anything on this page; see the README.
