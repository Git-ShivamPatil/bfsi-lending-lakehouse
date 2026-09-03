"""A configurable validation-rule repository for the lending book.

Modelled on how regulatory return validation actually works rather than on how
data-quality tooling usually markets itself. Three ideas are borrowed directly
from RBI's published CIMS design material and its supervisory data-quality work:

* **rules live as configuration, not code** -- adding a check is adding a row
  here, so the rule set can be reviewed by someone who does not read PySpark;
* **element-level and cross-element checks are different things** -- a value can
  be individually valid and still contradict another field, or another return;
* **every rule is tagged with a data-quality dimension** -- Accuracy,
  Completeness, Timeliness, Consistency, which is the ACTC framing RBI's
  Supervisory Data Quality Index uses.

Severity is the other axis. REJECT quarantines the row so it never reaches the
gold layer; WARN lets it through but counts it, because a lender that dropped
every row with a missing pincode would understate its own book.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Dimension = Literal["ACCURACY", "COMPLETENESS", "TIMELINESS", "CONSISTENCY"]
Severity = Literal["REJECT", "WARN"]
Kind = Literal["ROW", "REFERENTIAL", "UNIQUENESS"]


@dataclass(frozen=True)
class Rule:
    rule_id: str
    entity: str
    dimension: Dimension
    severity: Severity
    kind: Kind
    description: str
    #: For ROW rules: a Spark SQL boolean expression that must evaluate TRUE for
    #: the row to pass. Written against the *typed* silver columns.
    expression: str = ""
    #: For REFERENTIAL rules: (column, parent_entity, parent_column).
    references: tuple[str, str, str] | None = None
    #: For UNIQUENESS rules: the natural key that must not repeat.
    unique_key: tuple[str, ...] | None = None


RULES: tuple[Rule, ...] = (
    # ---------------- customers ----------------
    Rule("CUST_001", "customers", "COMPLETENESS", "REJECT", "ROW",
         "Customer id is mandatory",
         "customer_id IS NOT NULL AND customer_id <> ''"),
    Rule("CUST_002", "customers", "COMPLETENESS", "WARN", "ROW",
         "Pincode should be present for geographic reporting",
         "pincode IS NOT NULL AND pincode <> ''"),
    Rule("CUST_003", "customers", "ACCURACY", "REJECT", "ROW",
         "Bureau score, when present, must fall in the 300-900 domain",
         "bureau_score IS NULL OR (bureau_score BETWEEN 300 AND 900)"),
    Rule("CUST_004", "customers", "ACCURACY", "WARN", "ROW",
         "Pincode must be six digits and must not start with zero",
         "pincode IS NULL OR pincode = '' OR pincode RLIKE '^[1-9][0-9]{5}$'"),
    Rule("CUST_005", "customers", "CONSISTENCY", "WARN", "ROW",
         "A new-to-credit customer should not carry a bureau score",
         "NOT (score_band = 'NTC' AND bureau_score IS NOT NULL)"),

    # ---------------- merchants ----------------
    Rule("MERC_001", "merchants", "COMPLETENESS", "REJECT", "ROW",
         "Merchant id is mandatory",
         "merchant_id IS NOT NULL AND merchant_id <> ''"),
    Rule("MERC_002", "merchants", "ACCURACY", "WARN", "ROW",
         "Merchant name must be printable ASCII; mojibake indicates an "
         "encoding fault upstream that will corrupt any grouping on name",
         "merchant_name RLIKE '^[\\\\x20-\\\\x7E]+$'"),

    # ---------------- loans ----------------
    Rule("LOAN_001", "loans", "COMPLETENESS", "REJECT", "ROW",
         "Loan id is mandatory",
         "loan_id IS NOT NULL AND loan_id <> ''"),
    Rule("LOAN_002", "loans", "ACCURACY", "REJECT", "ROW",
         "Disbursed principal must be strictly positive",
         "principal > 0"),
    Rule("LOAN_003", "loans", "ACCURACY", "REJECT", "ROW",
         "Tenure must be one of the offered tenures",
         "tenure_months IN (3, 6, 9, 12)"),
    Rule("LOAN_004", "loans", "ACCURACY", "REJECT", "ROW",
         "APR must be a non-negative fraction below 1",
         "apr >= 0 AND apr < 1"),
    Rule("LOAN_005", "loans", "CONSISTENCY", "WARN", "ROW",
         "A no-cost EMI loan carries merchant subvention and zero customer APR",
         "NOT (product = 'NO_COST_EMI' AND (apr > 0 OR subvention_pct <= 0))"),
    Rule("LOAN_006", "loans", "TIMELINESS", "REJECT", "ROW",
         "Disbursal cannot be in the future relative to the reporting date",
         "disbursed_at <= reporting_date"),
    Rule("LOAN_007", "loans", "CONSISTENCY", "REJECT", "REFERENTIAL",
         "Every loan must belong to a known customer",
         references=("customer_id", "customers", "customer_id")),
    Rule("LOAN_008", "loans", "CONSISTENCY", "WARN", "REFERENTIAL",
         "Every loan should originate at a known merchant",
         references=("merchant_id", "merchants", "merchant_id")),
    Rule("LOAN_009", "loans", "CONSISTENCY", "REJECT", "UNIQUENESS",
         "Loan id must be unique in the loan master",
         unique_key=("loan_id",)),

    # ---------------- emi_schedule ----------------
    Rule("SCHED_001", "emi_schedule", "COMPLETENESS", "REJECT", "ROW",
         "Schedule rows need a loan and an instalment number",
         "loan_id IS NOT NULL AND instalment_no IS NOT NULL"),
    Rule("SCHED_002", "emi_schedule", "ACCURACY", "REJECT", "ROW",
         "Instalment numbers are one-based",
         "instalment_no >= 1"),
    Rule("SCHED_003", "emi_schedule", "CONSISTENCY", "REJECT", "ROW",
         "Cross-element check: the instalment must equal its own principal and "
         "interest components, to the paisa",
         "abs(emi_amount - (principal_component + interest_component)) <= 0.05"),
    Rule("SCHED_004", "emi_schedule", "CONSISTENCY", "REJECT", "REFERENTIAL",
         "Every schedule row must belong to a loan on the master",
         references=("loan_id", "loans", "loan_id")),
    Rule("SCHED_005", "emi_schedule", "CONSISTENCY", "REJECT", "UNIQUENESS",
         "One row per loan per instalment",
         unique_key=("loan_id", "instalment_no")),

    # ---------------- repayment_attempts ----------------
    Rule("REPAY_001", "repayment_attempts", "COMPLETENESS", "REJECT", "ROW",
         "Attempt id and loan id are mandatory",
         "attempt_id IS NOT NULL AND loan_id IS NOT NULL"),
    Rule("REPAY_002", "repayment_attempts", "ACCURACY", "REJECT", "ROW",
         "Attempt status must be a known code",
         "status IN ('SUCCESS', 'BOUNCED')"),
    Rule("REPAY_003", "repayment_attempts", "ACCURACY", "REJECT", "ROW",
         "Collected amount must be positive",
         "amount > 0"),
    Rule("REPAY_004", "repayment_attempts", "CONSISTENCY", "REJECT", "ROW",
         "Cross-element check: a successful attempt must carry a payment date, "
         "and a bounced one must carry a reason",
         "(status = 'SUCCESS' AND paid_at IS NOT NULL) OR "
         "(status = 'BOUNCED' AND bounce_reason IS NOT NULL AND bounce_reason <> '')"),
    Rule("REPAY_005", "repayment_attempts", "TIMELINESS", "REJECT", "ROW",
         "A payment cannot be dated after the reporting date",
         "paid_at IS NULL OR paid_at <= reporting_date"),
    Rule("REPAY_006", "repayment_attempts", "TIMELINESS", "WARN", "ROW",
         "A payment before its own due date is possible but unusual enough to flag",
         "paid_at IS NULL OR paid_at >= date_sub(due_date, 7)"),
    Rule("REPAY_007", "repayment_attempts", "CONSISTENCY", "REJECT", "REFERENTIAL",
         "Every repayment must map to a loan on the master",
         references=("loan_id", "loans", "loan_id")),
    Rule("REPAY_008", "repayment_attempts", "CONSISTENCY", "REJECT", "UNIQUENESS",
         "Attempt id must be unique; duplicates make a MERGE non-deterministic",
         unique_key=("attempt_id",)),
)


def for_entity(entity: str, kind: Kind | None = None) -> tuple[Rule, ...]:
    return tuple(r for r in RULES
                 if r.entity == entity and (kind is None or r.kind == kind))


def entities() -> tuple[str, ...]:
    seen: list[str] = []
    for r in RULES:
        if r.entity not in seen:
            seen.append(r.entity)
    return tuple(seen)


def by_id(rule_id: str) -> Rule:
    for r in RULES:
        if r.rule_id == rule_id:
            return r
    raise KeyError(rule_id)
