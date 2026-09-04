"""The generator's contract: deterministic, calibrated, and defective on purpose.

No Spark here -- these run in under a second and catch the failures that would
otherwise surface as confusing pipeline results.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date

from generator import config as C
from generator.generate import add_months, amortise, main as generate
from validation.backtest import load, month_end_snapshots, report


def _rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_same_seed_reproduces_the_book_byte_for_byte(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    for out in (a, b):
        generate(["--loans", "800", "--merchants", "40", "--months", "12",
                  "--seed", "7", "--as-of", "2026-08-31", "--out", str(out)])

    for name in ("loans.csv", "customers.csv", "emi_schedule.csv",
                 "repayment_attempts.csv", "merchants.csv"):
        assert (a / name).read_bytes() == (b / name).read_bytes(), name


def test_different_seeds_produce_different_books(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(["--loans", "800", "--seed", "1", "--as-of", "2026-08-31", "--out", str(a)])
    generate(["--loans", "800", "--seed", "2", "--as-of", "2026-08-31", "--out", str(b)])
    assert (a / "loans.csv").read_bytes() != (b / "loans.csv").read_bytes()


def test_amortisation_principal_components_sum_to_the_disbursed_amount():
    for principal, rate, tenure in [(10_000, 0.0, 6), (37_500, 0.22, 12),
                                    (1_999.99, 0.16, 3), (250_000, 0.34, 9)]:
        rows = amortise(principal, rate, tenure)
        assert len(rows) == tenure
        assert abs(sum(p for _, p, _ in rows) - principal) < 0.01, (principal, rate)
        # And each instalment reconciles to its own components.
        for emi, prin, intr in rows:
            assert abs(emi - (prin + intr)) < 0.011


def test_month_arithmetic_clamps_to_short_months():
    assert add_months(date(2026, 1, 31), 1) == date(2026, 2, 28)
    assert add_months(date(2024, 1, 31), 1) == date(2024, 2, 29)   # leap
    assert add_months(date(2026, 8, 31), 12) == date(2027, 8, 31)


def test_every_defect_type_fires_at_the_standard_fixture_size(tmp_path):
    """Every injected defect must appear at the size the pipeline tests use.

    This is the contract between the generator and the validation suite: a rule
    can only be proven to work if the data reliably contains something for it to
    catch. Two defects were originally rare enough that a 6,000-loan book
    contained none of them, so the rules guarding them were passing vacuously.
    Keep this size in step with `tests/conftest.py`.
    """
    generate(["--loans", "6000", "--merchants", "150", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "_manifest.json").read_text(encoding="utf-8"))

    assert manifest["counts"]["loans"] == 6000
    assert set(manifest["injected_defects"]) == set(C.DEFECT_RATES)

    missing = [d for d, n in manifest["injected_defects"].items() if n == 0]
    assert not missing, (
        f"no rows carry these defects, so their rules are untested: {missing}. "
        f"Either raise the rate in config.DEFECT_RATES or enlarge the fixture.")


def test_book_is_calibrated_to_the_published_gnpa(tmp_path):
    """The headline calibration gate, run across seeds so it is not seed-luck.

    Gated on the CRISIL-basis measure -- 90+ DPD including the trailing twelve
    months of write-offs -- because that is the basis the published 2.0% is
    stated on. The on-book measure is a different number entirely and matching
    it against this target would be meaningless.
    """
    for seed in (42, 7, 99, 2026):
        generate(["--loans", "8000", "--merchants", "200", "--months", "24",
                  "--seed", str(seed), "--as-of", "2026-08-31",
                  "--out", str(tmp_path / str(seed))])
        out = report(tmp_path / str(seed), date(2026, 8, 31))
        drift = abs(out["gnpa_pct_crisil_basis"] - C.TARGET_GNPA)
        assert drift <= C.GNPA_TOLERANCE, (
            f"seed {seed}: GNPA {out['gnpa_pct_crisil_basis']:.3%} drifted "
            f"{drift:.3%} from target {C.TARGET_GNPA:.3%}")


def test_average_ticket_lands_in_the_published_range(tmp_path):
    """The second published anchor.

    CRISIL puts Snapmint's average ticket at Rs 3,500-Rs 25,000. The ticket
    distribution's shape is an assumption, but the band it lands in is not, so
    the shape is not free to drift.
    """
    generate(["--loans", "8000", "--merchants", "200", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    ats = report(tmp_path, date(2026, 8, 31))["avg_ticket_size"]
    lo, hi = C.TARGET_ATS_RANGE
    assert lo <= ats <= hi, (
        f"average ticket Rs {ats:,.0f} is outside the published "
        f"Rs {lo:,}-Rs {hi:,} range")


def test_delinquency_ladder_is_monotonic(tmp_path):
    """Deeper buckets must hold less of the book than shallower ones.

    A synthetic book that has more 61-90 than 1-30 is not a lending book:
    accounts cure on the way down the ladder, so each deeper rung holds less.

    Measured over **every month end in the window**, pooled, rather than at the
    single as-of date. That is not a convenience -- it is what the property
    actually claims. Once the book was recalibrated to the published GNPA it
    became clean enough that a single date holds only a few dozen accounts per
    bucket, and at that sample the ordering is decided by one large-ticket loan
    landing in one bucket rather than another: seed 7 at 40,000 loans inverts
    1-30 and 31-60 by a single basis point. Pooling gives ~24x the sample from
    the same generated book and tests the steady state instead of one draw.

    90+ is deliberately excluded. It is open-ended -- accounts accumulate there
    until write-off at 180 DPD, while every other bucket is a 30-day window --
    so it is expected to hold more than the rungs above it, and including it
    would assert something false.
    """
    generate(["--loans", "8000", "--merchants", "200", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])

    as_of = date(2026, 8, 31)
    manifest = json.loads((tmp_path / "_manifest.json").read_text(encoding="utf-8"))
    start = date.fromisoformat(manifest["window_start"])

    loans, schedule, paid, _extra = load(tmp_path, as_of)
    pooled = defaultdict(float)
    for p in month_end_snapshots(loans, schedule, paid, start, as_of):
        if not p["written_off"]:
            pooled[p["bucket"]] += p["outstanding"]

    total = sum(pooled.values())
    assert total > 0
    mix = {k: v / total for k, v in pooled.items()}

    assert mix["CURRENT"] > 0.90
    assert mix.get("1-30", 0) >= mix.get("31-60", 0) >= mix.get("61-90", 0), (
        f"delinquency ladder is not monotonic: "
        f"1-30 {mix.get('1-30', 0):.5f}, 31-60 {mix.get('31-60', 0):.5f}, "
        f"61-90 {mix.get('61-90', 0):.5f}")


def test_vintage_curves_do_not_improve_with_age(tmp_path):
    """Within a cohort, the 30+ rate at MOB 12 cannot be below the rate at MOB 3.

    Delinquency is cumulative in this definition -- an account that went bad
    stays counted -- so a decreasing curve means the measurement is wrong.
    """
    generate(["--loans", "12000", "--merchants", "250", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    curves = report(tmp_path, date(2026, 8, 31))["vintage_30plus_by_cohort_at_mob"]

    checked = 0
    for cohort, per_mob in curves.items():
        # Keys are ints in-process and strings once the report has been through
        # JSON, so normalise before comparing.
        by_mob = {int(k): v for k, v in per_mob.items()}
        mobs = sorted(by_mob)
        for earlier, later in zip(mobs, mobs[1:]):
            assert by_mob[later] >= by_mob[earlier], (
                f"{cohort}: ever-30+ fell from {by_mob[earlier]:.3f} at MOB{earlier} "
                f"to {by_mob[later]:.3f} at MOB{later}; a cumulative curve cannot "
                f"decrease, so the metric is being computed point-in-time")
            checked += 1
    assert checked > 10, "not enough cohort/MOB pairs to be a real check"
