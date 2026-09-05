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

    Gated on the plain gross NPA ratio -- 90+ DPD over gross advances -- because
    that is what the published 2.0% is. The same rationale carries a second
    metric, "90+ dpd including last 12 months write-offs / Disbursements", whose
    denominator is disbursements rather than advances; matching this target to
    that ratio is the mistake this gate exists to prevent.
    """
    seeds = (42, 7, 99, 2026)
    observed = []
    for seed in seeds:
        generate(["--loans", "25000", "--merchants", "500", "--months", "24",
                  "--seed", str(seed), "--as-of", "2026-08-31",
                  "--out", str(tmp_path / str(seed))])
        observed.append(report(tmp_path / str(seed), date(2026, 8, 31))["gnpa_pct"])

    mean = sum(observed) / len(observed)
    drift = abs(mean - C.TARGET_GNPA)
    assert drift <= C.GNPA_TOLERANCE, (
        f"mean GNPA {mean:.3%} across seeds {seeds} drifted {drift:.3%} from "
        f"target {C.TARGET_GNPA:.3%}; observed "
        f"{[f'{o:.3%}' for o in observed]}")

    # Individual seeds get a wider band, and the reason is a property of the
    # book rather than a weakness of the test. Merchant volume follows a power
    # law, so a small book is dominated by a handful of merchants, and whether
    # those few drew high or low risk multipliers moves the whole portfolio.
    # That is realistic -- concentration is exactly why one large partner going
    # bad is a portfolio event for a checkout lender -- but it means a
    # single-seed, single-draw estimate is not a stable thing to gate on.
    for seed, o in zip(seeds, observed):
        assert abs(o - C.TARGET_GNPA) <= 2 * C.GNPA_TOLERANCE, (
            f"seed {seed}: GNPA {o:.3%} is far enough from target to be a "
            f"regression rather than concentration luck")


def test_average_ticket_matches_the_published_portfolio_figure(tmp_path):
    """The second published anchor, and the one that needs care to read.

    The rationale contains two ticket numbers: "average ticket size ranging from
    Rs 3,500 to Rs 25,000", which is a range across products, and "As of
    December 31, 2025, the average ticket size for the overall portfolio was
    Rs 3,500", which is the portfolio mean. A book-level average has to match the
    second. Treating the first as a band the mean may sit anywhere inside let
    this book run four times too large.
    """
    generate(["--loans", "8000", "--merchants", "200", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    ats = report(tmp_path, date(2026, 8, 31))["avg_ticket_size"]
    assert abs(ats - C.TARGET_ATS) <= C.ATS_TOLERANCE, (
        f"average ticket Rs {ats:,.0f} is off the published "
        f"Rs {C.TARGET_ATS:,} by more than Rs {C.ATS_TOLERANCE:,}")


def test_the_funnel_narrows_and_only_narrows(tmp_path):
    """Applications >= approved >= converted, and loans are exactly the tail.

    The book used to begin at disbursal, which made approval rate and checkout
    conversion -- the first two questions anyone asks a checkout lender --
    uncomputable. They are now derived from the data rather than asserted, so
    the thing worth testing is that the funnel is internally consistent.
    """
    generate(["--loans", "6000", "--merchants", "150", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])
    manifest = json.loads((tmp_path / "_manifest.json").read_text(encoding="utf-8"))
    f = manifest["funnel"]

    assert f["applications"] >= f["approved"] >= f["converted"]
    assert f["converted"] == manifest["counts"]["loans"], (
        "every converted application must produce exactly one loan")
    assert 0.0 < f["approval_rate"] < 1.0
    assert 0.0 < f["conversion_of_approved"] < 1.0

    rows = _rows(tmp_path / "applications.csv")
    assert len(rows) == f["applications"]

    # A declined application must not carry a loan, and every converted one must
    # name the loan it became -- the join the whole funnel rests on.
    loan_ids = {r["loan_id"] for r in _rows(tmp_path / "loans.csv")}
    for r in rows:
        if r["decision"] == "DECLINED":
            assert r["loan_id"] == "" and r["decline_reason"]
        if r["converted"] == "Y":
            assert r["loan_id"] in loan_ids


def test_first_payment_default_has_its_own_mechanism(tmp_path):
    """Mandate failures must concentrate in instalment one, not spread evenly.

    This is the difference between first-payment default being an independent
    signal and being a scaled copy of lifetime default. On a real checkout book
    the two separate, and the separation is the useful part: high FPD with
    ordinary GNPA is an activation failure -- a mandate that never registered --
    while ordinary FPD with high GNPA is an underwriting failure.

    Before the e-mandate was modelled, FPD was a near-constant 1.3-1.6x multiple
    of GNPA across every bureau band, which is what a single shared hazard
    produces and what makes the segment cut worthless.
    """
    generate(["--loans", "8000", "--merchants", "200", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])

    first, later = defaultdict(int), defaultdict(int)
    for r in _rows(tmp_path / "repayment_attempts.csv"):
        if r["status"] != "BOUNCED":
            continue
        bucket = first if r["instalment_no"] == "1" else later
        bucket[r["bounce_reason"]] += 1

    def share(d):
        total = sum(d.values())
        return d.get("MANDATE_NOT_REGISTERED", 0) / total if total else 0.0

    first_share, later_share = share(first), share(later)
    assert first_share > 2 * later_share, (
        f"mandate failures are not concentrated in instalment one: "
        f"{first_share:.3f} of first-instalment bounces vs {later_share:.3f} "
        f"of later ones -- first-payment default is not an independent signal")


def test_bounce_reasons_are_possible_for_their_collection_mode(tmp_path):
    """A UPI autopay mandate cannot return a signature mismatch.

    Changes no metric in this repository. It is the kind of detail that tells
    anyone who has worked a collections queue whether the payment rails were
    modelled or waved at.
    """
    generate(["--loans", "6000", "--merchants", "150", "--months", "24",
              "--seed", "42", "--as-of", "2026-08-31", "--out", str(tmp_path)])

    seen = defaultdict(set)
    for r in _rows(tmp_path / "repayment_attempts.csv"):
        if r["status"] == "BOUNCED" and r["bounce_reason"]:
            seen[r["mode"]].add(r["bounce_reason"])

    for mode, reasons in seen.items():
        allowed = {code for code, _ in C.BOUNCE_REASONS_BY_MODE[mode]}
        # The mandate-failure path deliberately forces its own reason on NACH.
        allowed.add("MANDATE_NOT_REGISTERED")
        assert reasons <= allowed, (
            f"{mode} returned {reasons - allowed}, which that instrument "
            f"cannot produce")


def test_delinquency_ladder_is_monotonic(tmp_path):
    """Deeper buckets must hold less of the book than shallower ones.

    A synthetic book that has more 61-90 than 1-30 is not a lending book:
    accounts cure on the way down the ladder, so each deeper rung holds less.

    Measured over **every month end in the window**, pooled, rather than at the
    single as-of date. That is not a convenience -- it is what the property
    actually claims, and it is also what makes the assertion robust. At one date
    a fixture-sized book holds only tens of accounts per bucket, and at that
    sample the ordering can be decided by a single large-ticket loan landing in
    one bucket rather than another. Pooling gives ~24x the sample from the same
    generated book and tests the steady state instead of one draw.

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
