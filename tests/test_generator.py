"""The generator's contract: deterministic, calibrated, and defective on purpose.

No Spark here -- these run in under a second and catch the failures that would
otherwise surface as confusing pipeline results.
"""

from __future__ import annotations

import csv
import json
from datetime import date

from generator import config as C
from generator.generate import add_months, amortise, main as generate
from validation.backtest import report


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
    """The headline calibration gate, run across seeds so it is not seed-luck."""
    for seed in (42, 7, 99, 2026):
        generate(["--loans", "8000", "--merchants", "200", "--months", "24",
                  "--seed", str(seed), "--as-of", "2026-08-31",
                  "--out", str(tmp_path / str(seed))])
        out = report(tmp_path / str(seed), date(2026, 8, 31))
        drift = abs(out["gnpa_pct"] - C.TARGET_GNPA)
        assert drift <= C.GNPA_TOLERANCE, (
            f"seed {seed}: GNPA {out['gnpa_pct']:.3%} drifted {drift:.3%} "
            f"from target {C.TARGET_GNPA:.3%}")


def test_delinquency_ladder_is_monotonic(tmp_path):
    """Deeper buckets must hold less of the book than shallower ones.

    A synthetic book that has more 61-90 than 1-30 is not a lending book.
    """
    generate(["--loans", "8000", "--merchants", "200", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    mix = report(tmp_path, date(2026, 8, 31))["bucket_mix_by_value"]

    assert mix["CURRENT"] > 0.90
    assert mix.get("1-30", 0) >= mix.get("31-60", 0)
    assert mix.get("31-60", 0) >= mix.get("61-90", 0)


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
