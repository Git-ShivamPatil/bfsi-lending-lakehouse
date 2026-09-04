"""Score the generated book against published industry reality.

A synthetic dataset is only worth putting on a resume if someone can check that it
behaves like the thing it claims to model. This reads the raw extracts with the
standard library alone and reports the metrics a lending analyst would actually
open first -- then fails the build if the headline number drifts.

    python -m validation.backtest --data data/raw --as-of 2026-08-31

It deliberately duplicates a little of what the silver/gold layers do in PySpark.
That redundancy is the point: if the Spark pipeline and this independent
implementation disagree, one of them is wrong, and
`tests/test_pipeline.py::test_gold_gnpa_matches_the_independent_backtest` checks
they do not -- on both GNPA definitions.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from generator import config as C


def read(path: Path):
    with path.open(newline="", encoding="utf-8") as fh:
        yield from csv.DictReader(fh)


def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def bucket_for(dpd: int) -> str:
    for label, lo, hi in C.DPD_BUCKETS:
        if lo <= dpd <= hi:
            return label
    return "90+"


def sma_stage(dpd: int) -> str:
    if dpd > C.NPA_DPD_THRESHOLD:
        return "NPA"
    for label, lo, hi in C.SMA_BUCKETS:
        if lo <= dpd <= hi:
            return label
    return "STANDARD"


def load(data: Path, as_of: date):
    """Load the extracts and apply the same cleaning the silver layer applies."""
    loans = {}
    for r in read(data / "loans.csv"):
        principal = float(r["principal"])
        if principal <= 0:               # domain violation -- quarantined
            continue
        loans[r["loan_id"]] = {
            "customer_id": r["customer_id"],
            "principal": principal,
            "tenure": int(r["tenure_months"]),
            "product": r["product"],
            "disbursed_at": date.fromisoformat(r["disbursed_at"]),
        }

    schedule = defaultdict(dict)
    for r in read(data / "emi_schedule.csv"):
        if r["loan_id"] not in loans:
            continue
        schedule[r["loan_id"]][int(r["instalment_no"])] = {
            "due": date.fromisoformat(r["due_date"]),
            "emi": float(r["emi_amount"]),
            "principal": float(r["principal_component"]),
        }

    # De-duplicate on the natural key, keeping the first success. This is exactly
    # the collapse a MERGE INTO needs before it can be deterministic.
    paid: dict[tuple[str, int], date] = {}
    attempts = 0
    bounced = 0
    orphans = 0
    collected_by_month: dict[str, float] = defaultdict(float)

    for r in read(data / "repayment_attempts.csv"):
        attempts += 1
        lid = r["loan_id"]
        if lid not in loans:
            orphans += 1
            continue
        if r["status"] == "BOUNCED":
            bounced += 1
            continue
        if not r["paid_at"]:
            continue
        pd_ = date.fromisoformat(r["paid_at"])
        if pd_ > as_of:                  # future-dated -- quarantined
            continue
        key = (lid, int(r["instalment_no"]))
        if key not in paid or pd_ < paid[key]:
            paid[key] = pd_
            collected_by_month[month_key(pd_)] += float(r["amount"])

    return loans, schedule, paid, {
        "attempts": attempts, "bounced": bounced, "orphans": orphans,
        "collected_by_month": collected_by_month,
    }


def position(lid, loan, inst, paid, as_of: date):
    """One loan's position at an arbitrary date, or None if it is not on the book.

    `as_of` is a free parameter rather than a global because vintage curves have
    to be measured at equal months-on-book across cohorts -- measuring every
    cohort at today's date compares a 3-month-old book with a 24-month-old one
    and produces a curve that slopes for no reason but age.
    """
    outstanding = 0.0
    oldest_unpaid_due = None
    for n, s in sorted(inst.items()):
        if s["due"] > as_of and (lid, n) not in paid:
            outstanding += s["principal"]
            continue
        pd_ = paid.get((lid, n))
        if pd_ is not None and pd_ <= as_of:
            continue                      # settled by this date
        outstanding += s["principal"]
        if s["due"] <= as_of and oldest_unpaid_due is None:
            oldest_unpaid_due = s["due"]

    if outstanding <= 0.005:
        return None                       # closed

    dpd = (as_of - oldest_unpaid_due).days if oldest_unpaid_due else 0
    return {
        "loan_id": lid,
        "disbursed_at": loan["disbursed_at"],
        "outstanding": outstanding,
        "dpd": dpd,
        "bucket": bucket_for(dpd),
        "stage": sma_stage(dpd),
        "written_off": dpd > C.WRITE_OFF_DPD,
    }


def written_off_on(row, as_of: date) -> date:
    """The date an account crossed the write-off threshold.

    Write-off happens on the first day past `WRITE_OFF_DPD`, so an account
    sitting at `dpd` today crossed it `dpd - WRITE_OFF_DPD - 1` days ago. This
    reconstruction holds because a cure resets the arrears anchor: an account
    that is still delinquent at `dpd` has been continuously so since its oldest
    unpaid instalment fell due.
    """
    return as_of - timedelta(days=row["dpd"] - C.WRITE_OFF_DPD - 1)


def portfolio(loans, schedule, paid, as_of: date):
    """Per-loan position as at `as_of`, written-off accounts included and flagged."""
    rows = []
    for lid, loan in loans.items():
        inst = schedule.get(lid, {})
        if not inst:
            continue
        p = position(lid, loan, inst, paid, as_of)
        if p is not None:
            rows.append(p)
    return rows


def month_end_snapshots(loans, schedule, paid, start: date, as_of: date):
    """Per-loan position at every month end in the window.

    The pure-Python equivalent of `silver_loan_snapshot`. Each loan is only
    evaluated across the months it could plausibly be on the book for, which
    keeps this proportional to loan-months rather than loans x all dates.
    """
    ends = []
    d = date(start.year, start.month, 1)
    while d <= as_of:
        nxt = add_months_date(d, 1)
        last = nxt - timedelta(days=1)
        if last <= as_of:
            ends.append(last)
        d = nxt

    for lid, loan in loans.items():
        inst = schedule.get(lid, {})
        if not inst:
            continue
        horizon = add_months_date(loan["disbursed_at"], loan["tenure"] + 12)
        for snap in ends:
            if snap < loan["disbursed_at"] or snap > horizon:
                continue
            p = position(lid, loan, inst, paid, snap)
            if p is None:
                continue
            p["snapshot_date"] = snap
            p["months_on_book"] = (snap.year - loan["disbursed_at"].year) * 12 + (
                snap.month - loan["disbursed_at"].month)
            yield p


def vintage_curve(loans, schedule, paid, as_of: date, mobs=(3, 6, 9, 12),
                  max_mob: int = 12):
    """Share of each disbursal cohort that has EVER been 30+ DPD by month-on-book.

    Two decisions make this the standard vintage curve rather than a plausible
    -looking one:

    * **Equal months-on-book.** A cohort only appears at a given MOB if it has
      actually been on the book that long, so the curve never compares a seasoned
      cohort against an immature one -- which would produce a slope caused purely
      by age.
    * **Cumulative, not point-in-time.** "Ever 30+ by MOB m", not "30+ at MOB m".
      A point-in-time reading falls when accounts cure, so cohorts appear to
      improve with age and the curve stops being comparable across cohorts.
      Cumulative is monotonic by construction, which is why it is the convention.
    """
    out: dict[str, dict[int, list[int]]] = {}
    wanted = sorted(set(mobs))

    for lid, loan in loans.items():
        inst = schedule.get(lid, {})
        if not inst:
            continue
        cohort = month_key(loan["disbursed_at"])

        ever_bad = False
        for m in range(1, max_mob + 1):
            obs = add_months_date(loan["disbursed_at"], m)
            if obs > as_of:
                break
            p = position(lid, loan, inst, paid, obs)
            if p is not None and p["dpd"] > 30:
                ever_bad = True
            if m in wanted:
                slot = out.setdefault(cohort, {}).setdefault(m, [0, 0])
                slot[0] += 1
                slot[1] += 1 if ever_bad else 0

    curve = {}
    for cohort, per_mob in sorted(out.items()):
        curve[cohort] = {
            m: round(bad / n, 4) for m, (n, bad) in sorted(per_mob.items()) if n >= 30
        }
    return {k: v for k, v in curve.items() if v}


def add_months_date(d: date, n: int) -> date:
    y, m = divmod(d.year * 12 + (d.month - 1) + n, 12)
    m += 1
    leap = y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)
    last = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def report(data: Path, as_of: date) -> dict:
    loans, schedule, paid, extra = load(data, as_of)
    everything = portfolio(loans, schedule, paid, as_of)

    # Written-off accounts leave the active book. GNPA is measured on gross
    # advances still on the balance sheet, so including them would double-count
    # losses already recognised.
    written_off = [r for r in everything if r["written_off"]]
    book = [r for r in everything if not r["written_off"]]

    total_os = sum(r["outstanding"] for r in book)
    by_bucket = defaultdict(float)
    cnt_bucket = defaultdict(int)
    for r in book:
        by_bucket[r["bucket"]] += r["outstanding"]
        cnt_bucket[r["bucket"]] += 1

    npa_os = sum(r["outstanding"] for r in book
                 if r["dpd"] > C.NPA_DPD_THRESHOLD)

    # CRISIL's supplementary ratio: "90+ dpd including last 12 months
    # write-offs / Disbursements". Note the denominator -- it is disbursements
    # for the period, NOT gross advances, which makes this a loss rate on
    # origination rather than a second GNPA. Folding write-offs into an advances
    # denominator produces a number belonging to neither ratio.
    #
    # "Disbursements" over a nine-month reporting period is ambiguous from
    # outside the company, so this uses a trailing twelve months and says so
    # rather than pretending the convention is settled. Reported, not gated.
    recent_write_offs = [
        r for r in written_off
        if (as_of - written_off_on(r, as_of)).days <= C.GNPA_WRITE_OFF_LOOKBACK_DAYS
    ]
    wo_os = sum(r["outstanding"] for r in recent_write_offs)
    window_start = as_of - timedelta(days=C.GNPA_WRITE_OFF_LOOKBACK_DAYS)
    disbursed_ttm = sum(l["principal"] for l in loans.values()
                        if l["principal"] > 0 and l["disbursed_at"] > window_start)
    loss_on_disbursements = ((npa_os + wo_os) / disbursed_ttm) if disbursed_ttm else 0.0

    tickets = [l["principal"] for l in loans.values() if l["principal"] > 0]
    avg_ticket = sum(tickets) / len(tickets) if tickets else 0.0

    # Billing by month, for collection efficiency.
    billed = defaultdict(float)
    for lid, inst in schedule.items():
        for _n, s in inst.items():
            if s["due"] <= as_of:
                billed[month_key(s["due"])] += s["emi"]

    ce = {}
    for m in sorted(billed):
        b = billed[m]
        if b > 0:
            ce[m] = round(extra["collected_by_month"].get(m, 0.0) / b, 4)

    vintage = vintage_curve(loans, schedule, paid, as_of)

    ce_values = [v for v in ce.values()][:-1]   # drop the part-month at the end

    return {
        "as_of": as_of.isoformat(),
        "open_loans": len(book),
        "principal_outstanding": round(total_os, 2),
        # Gross NPA: 90+ DPD over gross advances on the book. This is the plain
        # ratio CRISIL states as 2.0%, and the one the build is gated on.
        "gnpa_pct": round(npa_os / total_os, 5) if total_os else 0.0,
        # CRISIL's separate loss-on-origination ratio. Different denominator,
        # different meaning; reported for shape, not matched to a target.
        "write_off_incl_90plus_over_disbursements_ttm": round(loss_on_disbursements, 5),
        "disbursed_trailing_12m": round(disbursed_ttm, 2),
        "write_offs_in_window": len(recent_write_offs),
        "write_off_principal_in_window": round(wo_os, 2),
        "avg_ticket_size": round(avg_ticket, 2),
        "bucket_mix_by_value": {
            k: round(v / total_os, 5) for k, v in sorted(by_bucket.items())
        } if total_os else {},
        "bucket_mix_by_count": dict(sorted(cnt_bucket.items())),
        "bounce_rate_of_attempts": round(extra["bounced"] / extra["attempts"], 5)
        if extra["attempts"] else 0.0,
        "orphan_repayment_rows": extra["orphans"],
        "collection_efficiency_by_month": ce,
        "collection_efficiency_median": round(sorted(ce_values)[len(ce_values) // 2], 4)
        if ce_values else None,
        "written_off_loans": len(written_off),
        "written_off_principal": round(sum(r["outstanding"] for r in written_off), 2),
        "vintage_30plus_by_cohort_at_mob": vintage,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", type=Path, default=Path("data/raw"))
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--strict", action="store_true",
                    help="exit non-zero if GNPA drifts outside the tolerance")
    args = ap.parse_args(argv)

    as_of = date.fromisoformat(args.as_of)
    out = report(args.data, as_of)
    print(json.dumps(out, indent=2))

    failures = []

    gnpa = out["gnpa_pct"]
    drift = abs(gnpa - C.TARGET_GNPA)
    print(f"\nGNPA (90+ DPD over gross advances) {gnpa:.3%} vs target "
          f"{C.TARGET_GNPA:.3%} (tolerance +/-{C.GNPA_TOLERANCE:.3%}) "
          f"-> drift {drift:.3%}")
    if drift > C.GNPA_TOLERANCE:
        failures.append("GNPA outside tolerance; recalibrate generator/config.py")

    ats = out["avg_ticket_size"]
    ats_drift = abs(ats - C.TARGET_ATS)
    print(f"Average ticket Rs {ats:,.0f} vs published Rs {C.TARGET_ATS:,} "
          f"(tolerance +/-Rs {C.ATS_TOLERANCE:,}) -> drift Rs {ats_drift:,.0f}")
    if ats_drift > C.ATS_TOLERANCE:
        failures.append(f"average ticket Rs {ats:,.0f} is off the published "
                        f"Rs {C.TARGET_ATS:,}")

    # Reported, not gated -- the denominator convention is our reading.
    print(f"90+ incl. trailing-12m write-offs / trailing-12m disbursements "
          f"{out['write_off_incl_90plus_over_disbursements_ttm']:.3%} "
          f"(CRISIL reports 2.7% at 31 Dec 2025 on its own denominator)")

    for f in failures:
        print(f"FAIL: {f}")
    return 1 if (args.strict and failures) else 0


if __name__ == "__main__":
    raise SystemExit(main())
