# SQL exercises

Lending analytics questions over this repository's own silver tables, each one
built around a trap that returns a **plausible wrong answer** rather than an
error.

That is the selection rule. A query that fails loudly teaches nothing worth
testing; a query that quietly reports 0% first-payment default, or drops the
months where nothing had gone bad yet, is the one that reaches a credit
committee. Every exercise here is a mistake that produces a number somebody
would believe.

## How each exercise is built

Three files, and the third is the point:

| | |
|---|---|
| `NN_name.sql` | the answer |
| `NN_name.naive.sql` | the same question written the way it is usually written |
| `NN_name.fixture.py` | a small input, and the expected output **computed by hand** |

[`tests/test_exercises.py`](../tests/test_exercises.py) discovers all of them and
asserts two things:

```
answer == expected      the query is right
naive  != expected      the trap it is teaching is real
```

The second assertion is what makes the pack worth anything. Testing only that
the correct query passes says nothing about whether the question contained a
trap at all — and it is very easy to write a fixture that is too simple to
distinguish the two. Pinning the *wrong* answer proves the distinction exists.

This is the same argument [`validation/backtest.py`](../validation/backtest.py)
makes for the pipeline — two independent implementations that have to agree —
extended to the query layer. The oracle here is a person with a pencil rather
than a second program.

## What this does not prove

The fixtures are tens of rows. They prove **semantics**, not performance: that
`RANGE` and `ROWS` give different answers, not that either one is fast on a
billion rows. Nothing here is evidence about execution plans, shuffle
behaviour, skew or join strategy, and it would be dishonest to present it that
way.

## Portability

Written for Spark SQL, and run in CI on **PySpark 3.5.3 and 4.2.0** — the same
matrix as the pipeline, for the same reason: a query that quietly depends on a
4.x-only feature should fail here rather than on someone else's cluster.

- **`QUALIFY`** is in the Spark 4.2 grammar and absent from 3.5 (Databricks has
  had it since DBR 10.4). The harness probes for it at session start rather
  than assuming, and skips the exercises that need it with a reason. Exercises
  tagged `REQUIRES_QUALIFY` therefore report as skipped on the 3.5 leg and run
  on the 4.2 leg — which is the boundary being demonstrated, not a gap.
- **ANSI mode** is on. It is the default from Spark 4.0 and on Databricks
  Runtime 17.0+, and it changes real behaviour: an invalid `CAST` raises instead
  of returning NULL, and division by zero raises instead of returning NULL.
  Several exercises depend on that, and `try_cast` and `NULLIF` are the escape
  hatches.

## The exercises

| | Question | What it exercises | The trap |
|---|---|---|---|
| 01 | GNPA and PAR by month | Multi-stage aggregation, conditional sums | `SUM(CASE WHEN … THEN x END)` over a month with no NPA is NULL, not 0, so clean months read as missing. Was live in this repo's own gold layer. |
| 02 | Running collections per loan | Window frames | The default frame is `RANGE`, not `ROWS`. Two payments dated the same day both report the day's closing total. |
| 03 | First-payment default by merchant | Anti-joins, `FILTER`, ranking | `NOT IN` against a subquery containing one NULL returns **no rows** — a clean-looking 0% FPD across the whole book. |
| 04 | Prove a re-run changed nothing | Set operators, null-safe comparison | `EXCEPT` de-duplicates, so the duplicated row — the one indicating a non-idempotent write — compares equal and vanishes. |
| 05 | Longest delinquency streak | Gaps and islands | The row-number-difference technique welds two spells into one when a month's row is *missing* rather than merely non-delinquent. |

## Running them

They are part of the ordinary suite:

```bash
python -m pytest tests/test_exercises.py -q
```

No warehouse and no Delta runtime is needed — every fixture is registered as a
temp view, which is also why the whole suite runs on Windows without a Hadoop
`winutils.exe`.
