"""Generate a synthetic Indian checkout-finance (no-cost EMI) loan book.

Pure standard library, deterministic under a seed, no third-party dependency --
so anyone can reproduce the exact dataset on any machine with Python 3.9+ and
nothing installed:

    python -m generator --loans 150000 --months 24 --seed 42 --out data/raw

What it emits are *source-system extracts*, not analytics tables. Everything
derived -- days past due, SMA stage, roll rates, vintage curves -- is computed
downstream in the medallion pipeline, because computing them here would mean the
pipeline was never tested against anything.

Deliberate data-quality defects are injected at the rates in `config.DEFECT_RATES`
and reported in the run manifest, so the validation layer can be scored against a
known ground truth rather than graded on its own homework.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from . import config as C

# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def weighted_choice(rng: random.Random, pairs):
    """Pick from ((value, weight), ...). Weights need not sum to 1."""
    total = sum(w for _, w in pairs)
    r = rng.random() * total
    upto = 0.0
    for value, weight in pairs:
        upto += weight
        if r <= upto:
            return value
    return pairs[-1][0]


def add_months(d: date, n: int) -> date:
    """Month arithmetic that clamps to the end of a short month.

    A loan disbursed on the 31st has instalments due on the 30th, the 28th and so
    on -- collections systems clamp rather than skip, and the pipeline's date
    joins have to cope with it.
    """
    y, m = divmod(d.year * 12 + (d.month - 1) + n, 12)
    m += 1
    last = [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return date(y, m, min(d.day, last))


def money(x: float) -> float:
    """Round to paise. Money that does not round is money that will not reconcile."""
    return round(x + 1e-9, 2)


def amortise(principal: float, annual_rate: float, tenure: int):
    """Equal-instalment amortisation, returning (emi, principal_i, interest_i) rows.

    The final instalment absorbs the rounding residue so the principal components
    sum exactly to the disbursed amount -- otherwise every book fails its own
    reconciliation check by a few paise per loan.
    """
    if annual_rate <= 0:
        emi = money(principal / tenure)
        rows = []
        remaining = principal
        for i in range(tenure):
            p = emi if i < tenure - 1 else money(remaining)
            rows.append((money(p), money(p), 0.0))
            remaining -= p
        return rows

    r = annual_rate / 12.0
    emi = principal * r * (1 + r) ** tenure / ((1 + r) ** tenure - 1)
    emi = money(emi)
    rows = []
    balance = principal
    for i in range(tenure):
        interest = money(balance * r)
        if i == tenure - 1:
            p = money(balance)
            rows.append((money(p + interest), p, interest))
            balance = 0.0
        else:
            p = money(emi - interest)
            rows.append((emi, p, interest))
            balance = money(balance - p)
    return rows


# ---------------------------------------------------------------------------
# reference data
# ---------------------------------------------------------------------------

_CITIES = {
    "TIER_1": ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata", "Pune"],
    "TIER_2": ["Nagpur", "Indore", "Bhopal", "Coimbatore", "Kochi", "Surat", "Jaipur",
               "Lucknow", "Nashik", "Vadodara", "Patna", "Ludhiana"],
    "TIER_3": ["Solapur", "Jalgaon", "Bilaspur", "Hisar", "Warangal", "Tirupati",
               "Rourkela", "Dhule", "Karimnagar", "Bathinda", "Shimoga", "Anand"],
}

_MERCHANT_STEMS = [
    "Shree", "Balaji", "New", "Royal", "Krishna", "Sai", "Ganesh", "Modern",
    "National", "Star", "Galaxy", "Metro", "Prime", "Classic", "Deluxe",
]
_MERCHANT_TAILS = [
    "Electronics", "Mobiles", "Traders", "Enterprises", "Retail", "Emporium",
    "Collection", "Stores", "Sales", "Agencies",
]


def _mojibake(s: str) -> str:
    """Latin-1/UTF-8 round-trip damage -- the encoding bug that survives ingestion."""
    return s.encode("utf-8").decode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# entities
# ---------------------------------------------------------------------------


@dataclass
class Merchant:
    merchant_id: str
    merchant_name: str
    category: str
    city: str
    city_tier: str
    pincode: str
    onboarded_at: date


@dataclass
class Customer:
    customer_id: str
    city: str
    city_tier: str
    pincode: str
    age_band: str
    score_band: str
    bureau_score: int | None
    kyc_status: str
    created_at: date


@dataclass
class Loan:
    loan_id: str
    customer_id: str
    merchant_id: str
    product: str
    principal: float
    tenure_months: int
    apr: float
    subvention_pct: float
    disbursed_at: date
    schedule: list = field(default_factory=list)   # (due_date, emi, prin, intr)
    attempts: list = field(default_factory=list)   # repayment attempt rows


# ---------------------------------------------------------------------------
# generation
# ---------------------------------------------------------------------------


class BookGenerator:
    def __init__(self, seed: int, months: int, n_loans: int, as_of: date):
        self.rng = random.Random(seed)
        self.seed = seed
        self.months = months
        self.n_loans = n_loans
        self.as_of = as_of
        self.start = add_months(as_of, -months)
        self.defects: dict[str, int] = {k: 0 for k in C.DEFECT_RATES}

    # -- reference ---------------------------------------------------------

    def merchants(self, n: int) -> list[Merchant]:
        rng = self.rng
        out = []
        for i in range(n):
            tier = weighted_choice(rng, [(t, s) for t, s, _ in C.CITY_TIERS])
            city = rng.choice(_CITIES[tier])
            name = f"{rng.choice(_MERCHANT_STEMS)} {rng.choice(_MERCHANT_TAILS)}"
            if rng.random() < C.DEFECT_RATES["merchant_name_mojibake"]:
                name = _mojibake(name + " – Pvt Ltd")
                self.defects["merchant_name_mojibake"] += 1
            out.append(Merchant(
                merchant_id=f"M{i:06d}",
                merchant_name=name,
                category=weighted_choice(rng, C.MERCHANT_CATEGORIES),
                city=city,
                city_tier=tier,
                pincode=f"{rng.randint(110000, 855999)}",
                onboarded_at=self.start - timedelta(days=rng.randint(0, 900)),
            ))
        return out

    def customers(self, n: int) -> list[Customer]:
        rng = self.rng
        out = []
        for i in range(n):
            tier = weighted_choice(rng, [(t, s) for t, s, _ in C.CITY_TIERS])
            band = weighted_choice(rng, [(b, s) for b, s, _, _ in C.SCORE_BANDS])
            lo, hi = dict((b, rng_) for b, _, rng_, _ in C.SCORE_BANDS)[band]
            score = None if lo is None else rng.randint(lo, hi)

            if score is not None and rng.random() < C.DEFECT_RATES["score_out_of_range"]:
                score = rng.choice([0, -1, 999, 1200])
                self.defects["score_out_of_range"] += 1

            pincode = f"{rng.randint(110000, 855999)}"
            if rng.random() < C.DEFECT_RATES["missing_pincode"]:
                pincode = ""
                self.defects["missing_pincode"] += 1

            out.append(Customer(
                customer_id=f"C{i:07d}",
                city=rng.choice(_CITIES[tier]),
                city_tier=tier,
                pincode=pincode,
                age_band=weighted_choice(rng, (
                    ("18-24", 0.21), ("25-34", 0.38), ("35-44", 0.24),
                    ("45-54", 0.12), ("55+", 0.05))),
                score_band=band,
                bureau_score=score,
                kyc_status=weighted_choice(rng, (("FULL_KYC", 0.93), ("MIN_KYC", 0.07))),
                created_at=self.start - timedelta(days=rng.randint(0, 1200)),
            ))
        return out

    # -- origination -------------------------------------------------------

    def _disbursal_date(self) -> date:
        """Sample a disbursal date with festive seasonality and book growth.

        Growth compounds month on month, so recent cohorts dominate originations
        -- which is what keeps a real lender's GNPA denominator fresh.
        """
        rng = self.rng
        weights = []
        for k in range(self.months):
            m = add_months(self.start, k)
            growth = (1 + C.MONTHLY_GROWTH_RATE) ** k
            weights.append((m, C.MONTH_SEASONALITY[m.month] * growth))
        chosen = weighted_choice(rng, weights)
        span = (add_months(chosen, 1) - chosen).days
        return chosen + timedelta(days=rng.randrange(span))

    def loans(self, customers: list[Customer], merchants: list[Merchant]) -> list[Loan]:
        rng = self.rng
        out = []
        for i in range(self.n_loans):
            cust = rng.choice(customers)
            merch = rng.choice(merchants)

            principal = math.exp(rng.gauss(C.TICKET_LOG_MEAN, C.TICKET_LOG_SIGMA))
            principal = money(min(max(principal, C.TICKET_FLOOR), C.TICKET_CEILING))
            if rng.random() < C.DEFECT_RATES["negative_principal"]:
                principal = -principal
                self.defects["negative_principal"] += 1

            tenure = rng.choice(C.TENURES_MONTHS)
            no_cost = rng.random() < C.NO_COST_EMI_SHARE
            apr = 0.0 if no_cost else round(rng.uniform(*C.APR_RANGE), 4)
            subv = round(rng.uniform(*C.SUBVENTION_PCT_RANGE), 4) if no_cost else 0.0

            disbursed = self._disbursal_date()
            schedule_rows = amortise(abs(principal), apr, tenure)
            schedule = []
            for k, (emi, prin, intr) in enumerate(schedule_rows):
                if rng.random() < C.DEFECT_RATES["emi_reconciliation_break"]:
                    emi = money(emi * rng.uniform(1.03, 1.18))
                    self.defects["emi_reconciliation_break"] += 1
                schedule.append((add_months(disbursed, k + 1), emi, prin, intr))

            out.append(Loan(
                loan_id=f"L{i:08d}",
                customer_id=cust.customer_id,
                merchant_id=merch.merchant_id,
                product="NO_COST_EMI" if no_cost else "INTEREST_BEARING_EMI",
                principal=principal,
                tenure_months=tenure,
                apr=apr,
                subvention_pct=subv,
                disbursed_at=disbursed,
                schedule=schedule,
            ))
        return out

    # -- repayment behaviour ----------------------------------------------

    def simulate(self, loans: list[Loan], by_customer: dict[str, Customer]):
        """Walk each loan's schedule and emit repayment attempts.

        The state machine is deliberately simple and legible: an account is either
        current or delinquent. A current account misses an instalment with a
        hazard scaled by bureau band, city tier and month-on-book; a delinquent
        account cures with a probability that falls as it ages, which is what
        produces a realistic roll-rate matrix downstream.
        """
        rng = self.rng
        pd_by_band = {b: m for b, _, _, m in C.SCORE_BANDS}
        risk_by_tier = {t: r for t, _, r in C.CITY_TIERS}
        attempt_seq = 0

        for loan in loans:
            cust = by_customer[loan.customer_id]
            hazard_scale = pd_by_band[cust.score_band] * risk_by_tier[cust.city_tier]
            delinquent_since = None   # index of the oldest unpaid instalment

            for idx, (due, emi, _prin, _intr) in enumerate(loan.schedule):
                if due > self.as_of:
                    break   # not yet billed

                mob = C.MOB_HAZARD_SHAPE[min(idx, len(C.MOB_HAZARD_SHAPE) - 1)]
                attempt_seq += 1

                if delinquent_since is None:
                    hazard = min(0.95, C.BASE_INSTALMENT_MISS_RATE * hazard_scale * mob)
                    if rng.random() < hazard:
                        delinquent_since = idx
                        mode = weighted_choice(rng, C.COLLECTION_MODES)
                        loan.attempts.append(self._attempt(
                            attempt_seq, loan, idx, due, emi, mode,
                            "BOUNCED", due, rng))
                        continue

                    # paid, with a little jitter around the due date
                    paid = due + timedelta(days=rng.choice([0, 0, 0, 1, 1, 2, 3]))
                    if paid > self.as_of:
                        paid = self.as_of
                    mode = weighted_choice(rng, C.COLLECTION_MODES)
                    loan.attempts.append(self._attempt(
                        attempt_seq, loan, idx, due, emi, mode, "SUCCESS", paid, rng))
                else:
                    # already behind: this instalment is missed too, and we test
                    # for a cure of the whole arrears position
                    # Bucket as at *this* cycle, not as at the extract date --
                    # the cure probability has to reflect how far behind the
                    # account was when the collection attempt happened.
                    bucket = self._bucket_for(
                        (due - loan.schedule[delinquent_since][0]).days)
                    mode = weighted_choice(rng, C.COLLECTION_MODES)
                    loan.attempts.append(self._attempt(
                        attempt_seq, loan, idx, due, emi, mode, "BOUNCED", due, rng))

                    if rng.random() < C.CURE_RATES.get(bucket, 0.10):
                        # clear every arrear instalment, dated in this cycle
                        cure_day = due + timedelta(days=rng.randint(1, 25))
                        if cure_day > self.as_of:
                            cure_day = self.as_of
                        for j in range(delinquent_since, idx + 1):
                            attempt_seq += 1
                            d_j, emi_j = loan.schedule[j][0], loan.schedule[j][1]
                            loan.attempts.append(self._attempt(
                                attempt_seq, loan, j, d_j, emi_j, "MANUAL",
                                "SUCCESS", cure_day, rng))
                        delinquent_since = None

    def _bucket_for(self, dpd: int) -> str:
        for label, lo, hi in C.DPD_BUCKETS:
            if lo <= dpd <= hi:
                return label
        return "90+"

    def _attempt(self, seq, loan, idx, due, emi, mode, status, event_date, rng):
        reason = weighted_choice(rng, C.BOUNCE_REASONS) if status == "BOUNCED" else ""
        paid_at = event_date

        if status == "SUCCESS" and rng.random() < C.DEFECT_RATES["future_dated_repayment"]:
            paid_at = self.as_of + timedelta(days=rng.randint(1, 40))
            self.defects["future_dated_repayment"] += 1

        loan_id = loan.loan_id
        if rng.random() < C.DEFECT_RATES["orphan_repayment"]:
            loan_id = f"L9{rng.randint(0, 9_999_999):07d}"
            self.defects["orphan_repayment"] += 1

        return {
            "attempt_id": f"A{seq:09d}",
            "loan_id": loan_id,
            "instalment_no": idx + 1,
            "due_date": due.isoformat(),
            "amount": f"{emi:.2f}",
            "mode": mode,
            "status": status,
            "bounce_reason": reason,
            "paid_at": paid_at.isoformat() if status == "SUCCESS" else "",
            "attempted_at": (event_date if status == "BOUNCED" else paid_at).isoformat(),
        }


# ---------------------------------------------------------------------------
# emit
# ---------------------------------------------------------------------------


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--loans", type=int, default=150_000)
    ap.add_argument("--customers", type=int, default=0,
                    help="default: 70%% of --loans, so repeat borrowers exist")
    ap.add_argument("--merchants", type=int, default=1_400)
    ap.add_argument("--months", type=int, default=24)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--as-of", default=None, help="YYYY-MM-DD, default today")
    ap.add_argument("--out", default="data/raw", type=Path)
    args = ap.parse_args(argv)

    as_of = date.fromisoformat(args.as_of) if args.as_of else date.today()
    n_cust = args.customers or int(args.loans * 0.70)

    gen = BookGenerator(args.seed, args.months, args.loans, as_of)

    merchants = gen.merchants(args.merchants)
    customers = gen.customers(n_cust)
    loans = gen.loans(customers, merchants)
    gen.simulate(loans, {c.customer_id: c for c in customers})

    out = Path(args.out)

    write_csv(out / "merchants.csv", (
        {"merchant_id": m.merchant_id, "merchant_name": m.merchant_name,
         "category": m.category, "city": m.city, "city_tier": m.city_tier,
         "pincode": m.pincode, "onboarded_at": m.onboarded_at.isoformat()}
        for m in merchants),
        ["merchant_id", "merchant_name", "category", "city", "city_tier",
         "pincode", "onboarded_at"])

    write_csv(out / "customers.csv", (
        {"customer_id": c.customer_id, "city": c.city, "city_tier": c.city_tier,
         "pincode": c.pincode, "age_band": c.age_band, "score_band": c.score_band,
         "bureau_score": "" if c.bureau_score is None else c.bureau_score,
         "kyc_status": c.kyc_status, "created_at": c.created_at.isoformat()}
        for c in customers),
        ["customer_id", "city", "city_tier", "pincode", "age_band", "score_band",
         "bureau_score", "kyc_status", "created_at"])

    write_csv(out / "loans.csv", (
        {"loan_id": l.loan_id, "customer_id": l.customer_id,
         "merchant_id": l.merchant_id, "product": l.product,
         "principal": f"{l.principal:.2f}", "tenure_months": l.tenure_months,
         "apr": f"{l.apr:.4f}", "subvention_pct": f"{l.subvention_pct:.4f}",
         "disbursed_at": l.disbursed_at.isoformat()}
        for l in loans),
        ["loan_id", "customer_id", "merchant_id", "product", "principal",
         "tenure_months", "apr", "subvention_pct", "disbursed_at"])

    def schedule_rows():
        for l in loans:
            for k, (due, emi, prin, intr) in enumerate(l.schedule):
                yield {"loan_id": l.loan_id, "instalment_no": k + 1,
                       "due_date": due.isoformat(), "emi_amount": f"{emi:.2f}",
                       "principal_component": f"{prin:.2f}",
                       "interest_component": f"{intr:.2f}"}

    write_csv(out / "emi_schedule.csv", schedule_rows(),
              ["loan_id", "instalment_no", "due_date", "emi_amount",
               "principal_component", "interest_component"])

    def attempt_rows():
        rng = random.Random(args.seed ^ 0x5EED)
        for l in loans:
            for a in l.attempts:
                yield a
                # the duplicate-key case that breaks a naive MERGE INTO
                if rng.random() < C.DEFECT_RATES["duplicate_repayment"]:
                    gen.defects["duplicate_repayment"] += 1
                    yield dict(a)

    write_csv(out / "repayment_attempts.csv", attempt_rows(),
              ["attempt_id", "loan_id", "instalment_no", "due_date", "amount",
               "mode", "status", "bounce_reason", "paid_at", "attempted_at"])

    manifest = {
        "generator_version": C.GENERATOR_VERSION,
        "seed": args.seed,
        "as_of": as_of.isoformat(),
        "window_start": gen.start.isoformat(),
        "months": args.months,
        "counts": {
            "merchants": len(merchants),
            "customers": len(customers),
            "loans": len(loans),
            "emi_schedule": sum(len(l.schedule) for l in loans),
            "repayment_attempts": sum(len(l.attempts) for l in loans),
        },
        "injected_defects": gen.defects,
    }
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
