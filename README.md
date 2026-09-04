# BFSI Lending Lakehouse

Roll-rate matrices, vintage triangles, delinquency buckets and an IND-AS 109
provision for an Indian no-cost-EMI / checkout-finance book — computed twice,
independently, and calibrated against a figure the lender's rating agency
actually published.

The delivery mechanism is a medallion lakehouse in PySpark on **Databricks Free
Edition**, with a validation-rule repository modelled on how RBI return
validation works. But the analytics are the point: the pipeline exists so the
numbers have somewhere to come from.

It runs at **zero cost**, on free tiers only, and every headline number below
came out of a real run rather than an illustration.

**→ [Live dashboard](https://git-shivampatil.github.io/bfsi-lending-lakehouse/)**
— portfolio position, roll-rate matrix, vintage triangle and data-quality
scorecard, rebuilt by CI on every push.

### Where each part has actually run

Being straight about this matters more than the claim it costs me:

| | |
|---|---|
| Generator, back-test, dashboard | Verified locally **and** in CI |
| Medallion pipeline (bronze → gold) | Verified in CI on PySpark **3.5.3 and 4.2.0** |
| Incremental `MERGE INTO`, Delta time travel | Verified in CI (needs a Delta runtime) |
| Databricks Free Edition | **Run end to end on 2026-09-05**, from a Git folder, on serverless. Figures below. |
| The deployment bundle | Schema-valid and serverless-clean in CI on every push. **Not yet deployed** — see the caveat below. |

**What the Databricks run produced.** [`notebooks/run_pipeline.py`](notebooks/run_pipeline.py)
executed top to bottom against `workspace.default`: the generator on the driver
(pure standard library, nothing installed), then bronze → silver → snapshot →
gold.

| | |
|---|---|
| Bronze rows landed | 1,400 · 105,000 · 150,000 · 1,124,484 · **810,475** |
| Gold tables built | 8 — portfolio 24, bucket mix 113, roll rate 373, vintage 300, collections 36, merchant risk 1,400, ECL staging 68, DQ scorecard 9 |
| GNPA at 2026-08-31, CRISIL basis | **1.907%** |
| GNPA at 2026-08-31, on the surviving book | **0.733%** |
| Live loans / principal outstanding | 62,503 · ₹53.62 crore |

The bronze count reconciles exactly, which is the check worth making: the
generator emitted 807,166 repayment attempts and deliberately injected 3,309
duplicate keys, and 807,166 + 3,309 = 810,475. The quarantine reconciles the
same way — 3,309 findings for the duplicate-key rule, 2,980 for the EMI
reconciliation break, 305 for out-of-domain bureau scores, each equal to the
number injected.

**Two things that had to be fixed before any of it ran**, both invisible to CI
because CI runs open-source Spark on a local master where both work:

- `clean.py` cached the cleaned frame. The DataFrame and SQL caching APIs raise
  on serverless compute, which is all Free Edition has.
- `ingest.py` stamped lineage with `input_file_name()`, removed in DBR 17.3 LTS.
  The supported replacement is the hidden `_metadata` column.

**What is still not proven.** The bundle in [`databricks.yml`](databricks.yml)
has never been deployed — it is checked against the CLI's own JSON schema on
every push, which catches a malformed task or a cluster block serverless would
reject, but schema-valid is not deployable. The run above was driven from a Git
folder and a notebook, not from `databricks bundle deploy`.

---

## What it does

```mermaid
flowchart LR
    G["<b>Generator</b><br/>pure stdlib, seeded<br/>150k loans"] --> L["<b>Landing</b><br/>Unity Catalog volume<br/>on the driver, or pushed by CI"]
    L --> B["<b>Bronze</b><br/>verbatim, all strings<br/>lineage stamped"]
    B --> S["<b>Silver</b><br/>try_cast + 29 rules<br/>quarantine, not drop"]
    S --> N["<b>Snapshot</b><br/>day-end DPD stamping<br/>SMA-0/1/2, NPA"]
    N --> D["<b>Gold</b><br/>roll rates, vintage,<br/>collections, DQ scorecard"]
    S -.-> Q["<b>Quarantine</b><br/>every rejected row,<br/>with the rule it broke"]
    Q --> D
```

Data reaches the lakehouse **by being put there, never by being fetched**, and
that is a constraint rather than a preference. Databricks Free Edition has no
account console — so there is no way to create the storage credential that a
Unity Catalog external location requires, and the workspace therefore cannot
read an external S3 bucket at all. "S3 is the lake, Databricks queries it" is
not buildable here.

Two ways in, then. The run on 2026-09-05 used the first:

- **On the driver.** The generator has no third-party dependencies, so it runs
  inside the workspace and writes straight into the volume. Nothing to upload,
  no token, no secret.
- **Pushed from CI.** [`.github/workflows/databricks.yml`](.github/workflows/databricks.yml)
  generates the book on a GitHub runner and sends it through the Files API,
  which is the path a real pipeline with a real upstream would take.

See [Cost](#cost-actually-zero).

---

## Results from the shipped configuration

`--loans 150000 --months 24 --seed 42 --as-of 2026-08-31`

| | |
|---|---|
| Loans / customers / merchants | 150,000 · 105,000 · 1,400 |
| EMI schedule rows | 1,124,484 |
| Repayment attempts | 807,166 |
| Raw extract size | 130 MB CSV |
| Generation time | **64 s** on a 2-core i3, no third-party dependency |
| Independent back-test | 43 s |
| Open loans at as-of | 62,723 |
| Principal outstanding | ₹53.93 crore |
| Average ticket | ₹14,604 |
| Written off (>180 DPD) | 2,209 loans, of which 1,938 in the last 12 months |

**Portfolio position at 2026-08-31**

| Bucket | Share of book by value | Loans |
|---|---:|---:|
| Current | 97.87% | 61,105 |
| 1–30 DPD (SMA-0) | 0.61% | 334 |
| 31–60 DPD (SMA-1) | 0.45% | 299 |
| 61–90 DPD (SMA-2) | 0.34% | 250 |
| 90+ DPD (NPA) | **0.73%** | 735 |

Median monthly collection efficiency **98.8%**.

### Why those numbers are the right ones

The book is calibrated against a published figure, not tuned until it looked
plausible: CRISIL's April 2026 rating rationale for Snapmint Financial Services
Pvt Ltd reports **GNPA of 2.0%** as at 31 Dec 2025. The generator targets it and
CI fails the build if it drifts more than 60 bps:

```
GNPA (CRISIL basis: 90+ incl. trailing-12m write-offs) 2.084% vs target 2.000%
  (tolerance +/-0.600%) -> drift 0.084%
GNPA (90+ on the surviving book, for contrast) 0.732%
Average ticket Rs 14,604 vs published range Rs 3,500-Rs 25,000
```

#### The definition matters more than the number

CRISIL does not say "GNPA 2.0%". It says **"90+ dpd including last 12 months'
write-offs"** — so a year of written-off principal sits in *both* the numerator
and the denominator. That is a different ratio from 90+ DPD on the surviving
book, which is the one a pipeline reaches for by default.

On this book the two read **2.08% and 0.73%**. They are not close, and for a
long time this project computed the second and compared it to a target published
on the first. That is an apples-to-oranges comparison, and a credit analyst
finds it in one question.

Both are now computed on both sides — Spark SQL off the arrears anchor, pure
Python off the observed DPD — and [the parity test](tests/test_pipeline.py)
asserts the two implementations agree on *each*. The write-off-inclusive measure
is the harder one to get right, because it depends on reconstructing when each
account crossed the threshold, and the two implementations get there by
different routes.

Matching the definitions up showed the book had been about three times too
risky. Correcting it moved the entry hazard from 0.0075 to 0.0016 — found by
sweep, because the response is strongly sublinear: a cleaner book also closes
faster and shrinks its own denominator.

Beyond the definition, holding the target across seeds took three modelling
corrections that a naive generator gets wrong, and each one is a real property
of a lending book:

1. **The book has to grow.** With a fixed loan count spread over 24 months,
   short-tenure loans mature and close while defaults persist, the denominator
   collapses, and GNPA drifts up without limit. The first version of this
   generator produced **24.9%**.
2. **Write-offs have to leave the book** — and then come back for this one
   ratio. Accounts past 180 DPD are excluded from gross advances, or losses get
   counted twice; the CRISIL measure then adds the recent ones back deliberately.
3. **Vintage curves have to be cumulative and measured at equal months-on-book.**
   More on that below — it is the subtlest of the three.

A second published anchor guards the ticket distribution. The same CRISIL
document puts the average ticket at **₹3,500–₹25,000**, and the build fails if
the generated mean lands outside it. The distribution's *shape* is still an
assumption; the band it has to land in is not.

---

## The two parts that are actually hard

### Vintage curves

A vintage curve compares disbursal cohorts at **equal months-on-book**. Reading
each cohort "as at today" compares a three-month-old book against a two-year-old
one and produces a slope caused purely by age.

It must also be **cumulative** — *ever* 30+ by MOB *m*, not *at* 30+ at MOB *m*.
The point-in-time version falls whenever accounts cure, so cohorts appear to
improve as they season.

The trap underneath both: a naive `MAX(...) OVER (PARTITION BY loan_id)` over
surviving snapshot rows still produces a curve that slopes downwards, because a
loan that went delinquent and then closed silently leaves *both* the numerator
and the denominator. The fix is to reduce each loan to the MOB at which it first
breached, then hold the cohort denominator fixed — see
[`VINTAGE_SQL`](pipeline/gold/metrics.py). A test asserts monotonicity, and it
caught exactly this bug during development.

### Roll rates

"Of the accounts in bucket X at month end, where were they a month later." It is
a self-join in disguise, but `LEAD` over a per-loan window is both faster and
safer: a self-join on a date offset silently drops loans whose next snapshot is
missing, whereas `LEAD` makes that a `NULL` you have to account for.

That `NULL` then has to be split, which is the part this got wrong at first.
There are **three** ways to leave a bucket, not two:

- `WRITTEN_OFF` — crossed 180 DPD and was taken as a loss;
- `CLOSED` — repaid in full and left the book;
- a bucket — rolled, cured, or stayed put.

The first version called both exits `CLOSED`, which reports a write-off as
though it were a successful payoff. That flatters the cure rate and hides
exactly the number a credit committee is looking for. An account already written
off is also no longer a valid *origin* — it has left the book and cannot roll
anywhere.

---

## The validation layer

29 rules across 5 entities — 22 rejecting, 7 warning — held as **configuration rather than code** in
[`validation/rules.py`](validation/rules.py), so the rule set can be reviewed by
someone who does not read PySpark. Each rule carries a data-quality dimension —
Accuracy, Completeness, Timeliness, Consistency — which is the ACTC framing RBI
uses in its Supervisory Data Quality Index.

Three ideas borrowed from how regulatory return validation genuinely works:

- **Element-level and cross-element checks are different things.** A value can be
  individually valid and still contradict another field (`emi_amount` ≠
  `principal_component + interest_component`) or another entity (a repayment
  whose `loan_id` is not on the loan master).
- **Severity matters.** `REJECT` quarantines the row; `WARN` lets it through and
  counts it. A lender that dropped every customer with a missing pincode would
  understate its own book.
- **Nothing is dropped silently.** Rejected rows go to a quarantine table tagged
  with the rule they broke. An unexplained row-count gap between bronze and
  silver is what destroys trust in a pipeline.

The generator **injects defects on purpose** at known rates — duplicate
repayment keys, orphan foreign keys, future-dated payments, reconciliation
breaks, mojibake, out-of-domain bureau scores — so the validation layer is scored
against a ground truth instead of grading its own homework. A test asserts that
every defect type actually appears at the fixture size; two of them originally
did not, which meant the rules guarding them were passing vacuously.

---

## Incremental loads, and the trap inside `MERGE INTO`

A lakehouse that only ever overwrites is a batch job with extra steps.
[`pipeline/silver/upsert.py`](pipeline/silver/upsert.py) is the incremental
path: batches append to bronze and merge into silver on a natural key.

`MERGE INTO` requires at most one source row per target row. Give it a source
with a duplicated key and Delta **refuses**:

```
Cannot perform Merge as multiple source rows matched ...
```

That is Delta refusing a non-deterministic write, and the fix is not to disable
the check — it is to collapse duplicates with an explicit, stable ordering
*before* merging, which is what `apply_uniqueness` does. The tests assert both
halves: that an un-deduplicated source really does fail, and that the pipeline's
own output really does not. Plus idempotency (replaying a batch must not change
the table), in-place correction of restated rows, and that Delta history records
every write.

## Expected credit loss

[`ECL_SQL`](pipeline/gold/metrics.py) stages the book under IND-AS 109 —
Stage 1 (≤30 DPD, 12-month ECL), Stage 2 (31–90, lifetime), Stage 3 (90+,
credit-impaired) — and computes a provision as EAD × PD × LGD. Tests assert the
staging partitions the book exactly once and that coverage rises with stage.

The PD and LGD inputs are **assumptions and the weakest numbers in the repo**: a
real implementation derives PD from the observed roll-rate matrix and LGD from
realised recoveries, neither of which this book models. The staging is real; the
provision figure has the right shape but is not quotable.

## Correctness

The Spark pipeline and [`validation/backtest.py`](validation/backtest.py)
implement the same definitions **twice, independently** — the second in a few
hundred lines of pure Python over the same CSVs. They are required to agree:

```python
assert spark_gnpa == pytest.approx(python_gnpa, abs=0.002)
```

Two implementations agreeing is much stronger evidence than one implementation
passing its own assertions. **30 tests**, all green, covering determinism,
amortisation reconciliation, ANSI-mode `try_cast` behaviour, rule compilation,
quarantine correctness, snapshot reproducibility for historical dates, roll-rate
closure, vintage monotonicity, ECL staging completeness, Delta idempotency, both
published calibration gates, and cross-implementation parity on *both* GNPA
definitions.

One honest note on that parity. At fixture scale the two agree comfortably. At
the full 150k book the Spark gold layer reported a CRISIL-basis GNPA of
**1.907%** on Databricks against the Python back-test's **2.084%** — an 18 bp
gap, inside the ±20 bp the test allows but not by much. Both sit well inside the
±60 bp calibration tolerance, and the on-book measure agrees to a single basis
point (0.733% vs 0.732%), which points at the write-off-window reconstruction
rather than the delinquency logic. Tightening that is on the list below rather
than written up as though it were already understood.

---

## Run it

Nothing beyond Python 3.9+ is needed for the data itself:

```bash
python -m generator --loans 150000 --months 24 --seed 42 --as-of 2026-08-31 --out data/raw
python -m validation.backtest --data data/raw --as-of 2026-08-31 --strict
```

The dashboard needs nothing either:

```bash
python -m docs.build_dashboard --data data/raw --as-of 2026-08-31 --out docs/index.html
```

The pipeline needs PySpark. The Delta tests additionally need a Hadoop runtime,
so they skip on Windows and run on Linux:

```bash
pip install pyspark delta-spark pytest
python -m pytest tests/ -q -m "not delta"   # everywhere
python -m pytest tests/ -q -m delta         # Linux / CI
```

### On Databricks Free Edition

This is the path that was actually walked on 2026-09-05, and it needs no
credentials anywhere — the repository is public, so the Git folder clones
without auth, and the generator runs on the driver rather than the data being
uploaded:

1. Sign up at **databricks.com/learn/free-edition**. Not the *free trial* —
   that is a different product, it is 14 days, and it converts to
   pay-as-you-go. Free Edition has no payment method attached at all.
2. **Workspace → Create → Git folder**, pointed at this repository. GitHub is
   detected automatically.
3. Open `notebooks/run_pipeline.py` and **Run all**. The first cell creates the
   landing volume in `workspace.default`; the rest is bronze → silver →
   snapshot → gold.

Nothing is installed on the cluster. Serverless is explicit that installing
PySpark, or anything depending on it, terminates the session — which the
generator sidesteps by having no third-party dependencies at all.

To drive it from CI instead, [`.github/workflows/databricks.yml`](.github/workflows/databricks.yml)
generates the book on a runner and pushes it into the volume through the Files
API. That path needs a personal access token in repository secrets, because
Free Edition has no account console and therefore no service principals and no
OAuth machine-to-machine.

Or run the stages directly, which is what the job tasks do:

```bash
python jobs/run_stage.py --stage bronze   --reporting-date 2026-08-31
python jobs/run_stage.py --stage silver   --reporting-date 2026-08-31
python jobs/run_stage.py --stage snapshot --window-start 2024-09-30 --reporting-date 2026-08-31
python jobs/run_stage.py --stage gold
```

---

## Cost: actually zero

| Component | Tier used | Why it stays free |
|---|---|---|
| Compute + storage | Databricks **Free Edition** | Perpetually free, serverless. Replaced Community Edition, retired 1 Jan 2026. |
| CI | GitHub Actions | Free for public repos on standard runners. |
| Object storage | Optional, S3-compatible | Not required — the landing zone is a Unity Catalog volume. |

Free Edition's limits shape the design: one 2X-Small SQL warehouse, 5 concurrent
job tasks, one active pipeline per type, **Spark Connect APIs only — no RDD
APIs**, no Scala, no JARs, no Maven coordinates, no init scripts, DBFS disabled.
The project stays inside a 1–5 GB, 10–50M row envelope and makes no TB-scale
claim. Exceeding the daily quota stops compute for the rest of the day; it does
not delete anything.

Two constraints that changed the architecture rather than merely annoying it:

- **The workspace cannot read an external S3 bucket.** Not because a document
  forbids it, but because Free Edition has no account console — so there is no
  way to create the storage credential a Unity Catalog external location
  requires. "S3 is the lake, Databricks queries it" is not buildable here, and
  the landing zone is a managed volume that data is *pushed* into.
- **No dashboard can be shared publicly.** No edition of Databricks offers
  anonymous access to a published dashboard, and Free Edition has one account
  with no SSO or SCIM, so there is nobody to share to. That is why the public
  artefact is a GitHub Pages page built by CI, not a Databricks dashboard link.

And one trap worth naming, because it bills silently: the Databricks **Free
Trial** is not Free Edition. It is 14 days, and when the credits run out or day
15 arrives the account converts to pay-as-you-go — via AWS Marketplace, against
the payment method on the AWS account. Free Edition has no payment method
attached at all. The two signup doors look nearly identical and both say "free".

---

## What is real and what is not

Being precise about this is the point of the project, not a disclaimer.

**Sourced, with references in [`generator/config.py`](generator/config.py):** the
2.0% GNPA target (CRISIL, Snapmint Financial Services Pvt Ltd, 16 Apr 2026);
RBI SMA-0/1/2 day-count bands and the day-end stamping rule from the IRACP
clarification of 12 Nov 2021; the 5% Default Loss Guarantee cap from the RBI
(Digital Lending) Directions, 2025.

**Assumptions, labelled as such:** ticket-size distribution, bureau-band mix and
their PD multipliers, cure rates by bucket, festive seasonality, merchant
category mix, bounce-reason weights. Snapmint publishes no average ticket size,
so none is claimed.

**Entirely synthetic:** every customer, merchant, loan and repayment. No real
lending data is used, and none of it is scraped from anywhere.

## Limitations, and what I would do differently

- **Roll rates are computed on month-end snapshots**, so an account that goes
  delinquent and cures within a single month is invisible. A daily grain would
  catch it; at 24 months × 150k loans that is ~100× the rows for a question this
  book does not need to answer. `--daily-tail` stamps a recent window for
  exactly that reason.
- **The cure model is memoryless.** Real accounts that have rolled twice behave
  differently from first-time delinquents; a state-dependent cure probability
  would produce a more realistic roll-rate matrix.
- **ECL staging keys off DPD alone.** A real implementation also stages on
  qualitative triggers — restructuring, forbearance, watch-list — and on
  relative PD deterioration since origination, not only an absolute day count.
- **No Default Loss Guarantee modelling.** The RBI (Digital Lending) Directions,
  2025 cap DLG at 5% of the disbursed portfolio, but this book has no lending
  service provider and no guarantee arrangement, so there is nothing honest to
  compute. A constant for the cap was removed rather than given an invented use.
- **No streaming path.** Everything is batch. A CDC feed via Debezium into a
  bronze append would be closer to how a real lender ingests.
- **The `_corrupt_record` column is dropped after silver** rather than being
  retained for forensics. It should be kept with the quarantine.
- **The bounce rate is not a bounce rate.** It reads 0.73% of attempts, and
  NACH debit returns across the Indian industry run far higher than that. The
  model's hazard is the probability of *entering delinquency*, not of a single
  failed presentation — a bounce that cures within the same cycle is never
  emitted at all. The column name promises more than the model delivers, and
  recalibrating the book made the gap wider. Modelling presentation-level
  retries separately from delinquency entry is the fix.
- **The write-off window reconstruction is the weakest link in parity.** The
  Spark and Python implementations agree to a basis point on the on-book GNPA
  and to 18 bp on the write-off-inclusive one. Both derive the write-off date
  from the arrears anchor, so the residual is probably grain — month-end rows
  versus an as-of computation — but "probably" is not "measured".
- **The bundle has never been deployed.** It is schema-checked on every push
  and it is written to the serverless constraints, but the end-to-end run went
  through a Git folder and a notebook.
- **A lending book this clean is a modelling choice, not a fact.** Matching a
  2.0% write-off-inclusive GNPA on a 12-month-tenor book forces a low entry
  hazard, and the resulting delinquency stock at any single month end is thin
  enough that some distributional tests need pooling across the window to have
  any statistical power at all.
