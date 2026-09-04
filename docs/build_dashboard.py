"""Render the gold layer as a self-contained static dashboard.

    python -m docs.build_dashboard --data data/raw --as-of 2026-08-31 --out docs/index.html

No JavaScript libraries, no CDN, no build step: charts are inline SVG and the
whole page is one file, so it deploys to GitHub Pages for nothing and still
renders if every external host is blocked. That constraint is also why it is
readable -- a reviewer can open the source and see exactly where a number came
from.

It reads the same CSV extracts the Spark pipeline reads and computes the same
metrics through `validation.backtest`, so the page cannot drift from the
pipeline's definitions without a test failing.
"""

from __future__ import annotations

import argparse
import html
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from generator import config as C
from validation.backtest import load, month_end_snapshots, report

# ---------------------------------------------------------------------------
# palette -- one hue ramp for severity, so the eye reads depth as risk
# ---------------------------------------------------------------------------

BUCKET_ORDER = ["CURRENT", "1-30", "31-60", "61-90", "90+"]
BUCKET_COLOUR = {
    "CURRENT": "#1f6f54",
    "1-30": "#7d9b3f",
    "31-60": "#c79235",
    "61-90": "#c25f2b",
    "90+": "#a32d21",
    "CLOSED": "#8a8f98",
}


def heat(value: float, vmax: float) -> str:
    """Background for a matrix cell: white through to the deep-risk red."""
    if vmax <= 0:
        return "#fff"
    t = max(0.0, min(1.0, value / vmax)) ** 0.6
    r = int(255 + (163 - 255) * t)
    g = int(255 + (45 - 255) * t)
    b = int(255 + (33 - 255) * t)
    return f"rgb({r},{g},{b})"


def pct(x, dp=2):
    return "—" if x is None else f"{x * 100:.{dp}f}%"


def crore(x):
    return f"₹{x / 1e7:.2f} cr"


# ---------------------------------------------------------------------------
# charts
# ---------------------------------------------------------------------------


def stacked_bars(series, width=980, height=210, pad=34):
    """Bucket mix over time as a 100% stacked column chart."""
    months = [m for m, _ in series]
    if not months:
        return ""
    n = len(months)
    bw = (width - pad * 2) / n * 0.72
    step = (width - pad * 2) / n
    out = [f'<svg viewBox="0 0 {width} {height}" role="img" '
           f'aria-label="Delinquency bucket mix by month">']

    for i, (month, mix) in enumerate(series):
        x = pad + i * step + (step - bw) / 2
        y = 12.0
        for bucket in BUCKET_ORDER:
            share = mix.get(bucket, 0.0)
            h = share * (height - 46)
            if h <= 0.15:
                continue
            out.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{h:.1f}" '
                f'fill="{BUCKET_COLOUR[bucket]}"><title>{month} · {bucket} · '
                f'{share * 100:.2f}%</title></rect>')
            y += h
        if i % max(1, n // 12) == 0:
            out.append(f'<text x="{x + bw / 2:.1f}" y="{height - 8}" '
                       f'text-anchor="middle" class="tick">{month[2:]}</text>')

    out.append("</svg>")
    return "".join(out)


def line_chart(points, width=980, height=190, pad=40, lo=None, hi=None):
    """Single series with a filled area under it."""
    if not points:
        return ""
    vals = [v for _, v in points]
    lo = min(vals) if lo is None else lo
    hi = max(vals) if hi is None else hi
    span = (hi - lo) or 1.0
    n = len(points)
    step = (width - pad * 2) / max(1, n - 1)

    def xy(i, v):
        return (pad + i * step, 14 + (1 - (v - lo) / span) * (height - 48))

    coords = [xy(i, v) for i, (_, v) in enumerate(points)]
    path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}"
                    for i, (x, y) in enumerate(coords))
    area = (path + f" L{coords[-1][0]:.1f},{height - 34:.1f}"
                   f" L{coords[0][0]:.1f},{height - 34:.1f} Z")

    out = [f'<svg viewBox="0 0 {width} {height}" role="img" '
           f'aria-label="Monthly collection efficiency">',
           f'<path d="{area}" fill="rgba(31,111,84,.12)"/>',
           f'<path d="{path}" fill="none" stroke="#1f6f54" stroke-width="2"/>']

    for i, ((label, v), (x, y)) in enumerate(zip(points, coords)):
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.6" fill="#1f6f54">'
                   f'<title>{label} · {v * 100:.2f}%</title></circle>')
        if i % max(1, n // 12) == 0:
            out.append(f'<text x="{x:.1f}" y="{height - 8}" text-anchor="middle" '
                       f'class="tick">{label[2:]}</text>')

    out.append(f'<text x="4" y="18" class="tick">{hi * 100:.0f}%</text>')
    out.append(f'<text x="4" y="{height - 40}" class="tick">{lo * 100:.0f}%</text>')
    out.append("</svg>")
    return "".join(out)


# ---------------------------------------------------------------------------
# metric assembly
# ---------------------------------------------------------------------------


def build_metrics(data: Path, as_of: date):
    loans, schedule, paid, extra = load(data, as_of)
    summary = report(data, as_of)

    start = min(l["disbursed_at"] for l in loans.values())
    snaps = list(month_end_snapshots(loans, schedule, paid, start, as_of))

    # bucket mix by month, by value
    by_month = defaultdict(lambda: defaultdict(float))
    total_month = defaultdict(float)
    for s in snaps:
        if s["written_off"]:
            continue
        key = s["snapshot_date"].isoformat()[:7]
        by_month[key][s["bucket"]] += s["outstanding"]
        total_month[key] += s["outstanding"]

    mix_series = []
    for month in sorted(by_month):
        tot = total_month[month] or 1.0
        mix_series.append((month, {b: v / tot for b, v in by_month[month].items()}))

    # roll rates: bucket now vs bucket next month, per loan.
    # Written-off and closed are tracked separately -- a write-off is a loss and
    # a closure is a full repayment, and reporting the first as the second
    # flatters the cure rate. Mirrors ROLL_RATE_SQL in pipeline/gold/metrics.py.
    per_loan = defaultdict(dict)
    for s in snaps:
        per_loan[s["loan_id"]][s["snapshot_date"].isoformat()[:7]] = (
            "WRITTEN_OFF" if s["written_off"] else s["bucket"])

    months = sorted(total_month)
    idx = {m: i for i, m in enumerate(months)}
    roll = defaultdict(lambda: defaultdict(int))
    for _lid, seq in per_loan.items():
        for m, bucket in seq.items():
            # An account that has already been written off has left the book and
            # cannot roll anywhere, so it is never an origin.
            if bucket == "WRITTEN_OFF":
                continue
            i = idx.get(m)
            if i is None or i + 1 >= len(months):
                continue
            roll[bucket][seq.get(months[i + 1], "CLOSED")] += 1

    roll_rates = {}
    for frm, dests in roll.items():
        tot = sum(dests.values()) or 1
        roll_rates[frm] = {d: v / tot for d, v in dests.items()}

    # vintage triangle
    vint = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    first_bad = {}
    for s in snaps:
        if s["dpd"] > 30:
            lid = s["loan_id"]
            mob = s["months_on_book"]
            if lid not in first_bad or mob < first_bad[lid]:
                first_bad[lid] = mob
    for lid, loan in loans.items():
        cohort = loan["disbursed_at"].isoformat()[:7]
        observable = (as_of.year - loan["disbursed_at"].year) * 12 + (
            as_of.month - loan["disbursed_at"].month)
        for mob in range(1, 13):
            if mob > observable:
                break
            cell = vint[cohort][mob]
            cell[0] += 1
            if first_bad.get(lid, 10 ** 6) <= mob:
                cell[1] += 1

    vintage = {
        c: {m: bad / n for m, (n, bad) in sorted(per.items()) if n >= 30}
        for c, per in sorted(vint.items())
    }
    vintage = {k: v for k, v in vintage.items() if v}

    manifest = {}
    mpath = data / "_manifest.json"
    if mpath.exists():
        manifest = json.loads(mpath.read_text(encoding="utf-8"))

    return summary, mix_series, roll_rates, vintage, manifest


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------

CSS = """
:root{--ink:#16191d;--soft:#5b626b;--line:#e2e5ea;--bg:#fbfcfd;--card:#fff;
--accent:#1f6f54}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.55 "Segoe UI",system-ui,-apple-system,Roboto,sans-serif}
.wrap{max-width:1060px;margin:0 auto;padding:34px 20px 60px}
h1{font-size:27px;margin:0 0 4px;letter-spacing:-.2px}
h2{font-size:15px;text-transform:uppercase;letter-spacing:.9px;color:var(--soft);
margin:34px 0 10px;font-weight:650}
.sub{color:var(--soft);margin:0 0 6px}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:12px;margin-top:18px}
.tile{background:var(--card);border:1px solid var(--line);border-radius:9px;padding:13px 15px}
.tile .k{font-size:11.5px;text-transform:uppercase;letter-spacing:.6px;color:var(--soft)}
.tile .v{font-size:23px;font-weight:660;margin-top:3px;letter-spacing:-.4px}
.tile .n{font-size:11.5px;color:var(--soft);margin-top:2px}
.card{background:var(--card);border:1px solid var(--line);border-radius:9px;padding:14px 16px}
.legend{display:flex;flex-wrap:wrap;gap:13px;margin-top:9px;font-size:12.5px;color:var(--soft)}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
table{border-collapse:collapse;font-size:12.5px;width:100%}
th,td{border:1px solid var(--line);padding:5px 7px;text-align:right}
th{background:#f4f6f8;font-weight:620;color:var(--soft)}
td:first-child,th:first-child{text-align:left;font-variant-numeric:tabular-nums}
.scroll{overflow-x:auto}
.tick{font-size:10px;fill:#8b929b}
.note{font-size:12.5px;color:var(--soft);margin-top:9px}
code{background:#f1f3f6;padding:1px 5px;border-radius:4px;font-size:12.5px}
a{color:var(--accent)}
"""


def render(summary, mix_series, roll_rates, vintage, manifest, as_of) -> str:
    counts = manifest.get("counts", {})
    defects = manifest.get("injected_defects", {})

    tiles = [
        ("Principal outstanding", crore(summary["principal_outstanding"]),
         f"{summary['open_loans']:,} live loans"),
        # 90+ DPD over gross advances -- the plain ratio CRISIL states at 2.0%.
        ("GNPA", pct(summary["gnpa_pct"]),
         f"90+ over advances · target {C.TARGET_GNPA:.1%}"),
        ("Avg ticket", f"&#8377;{summary['avg_ticket_size']:,.0f}",
         f"published &#8377;{C.TARGET_ATS:,}"),
        # PAR-30 is portfolio at risk beyond 30 days, so 1-30 DPD is excluded as
        # well as CURRENT. Summing everything that is not CURRENT would report
        # PAR-0 under a PAR-30 label.
        ("PAR&nbsp;30", pct(sum(v for k, v in summary["bucket_mix_by_value"].items()
                                if k in ("31-60", "61-90", "90+"))),
         "31+ DPD by value"),
        ("Collection efficiency", pct(summary["collection_efficiency_median"], 1),
         "median month"),
        ("Bounce rate", pct(summary["bounce_rate_of_attempts"], 2), "of all attempts"),
        ("Written off", f"{summary['written_off_loans']:,}",
         f">{C.WRITE_OFF_DPD} DPD, off book"),
    ]

    tile_html = "".join(
        f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'
        f'<div class="n">{n}</div></div>' for k, v, n in tiles)

    legend = "".join(
        f'<span><i style="background:{BUCKET_COLOUR[b]}"></i>{b}</span>'
        for b in BUCKET_ORDER)

    # roll-rate matrix
    dests = BUCKET_ORDER + ["WRITTEN_OFF", "CLOSED"]
    rr = ['<div class="scroll"><table><tr><th>From \\ to</th>'
          + "".join(f"<th>{d}</th>" for d in dests) + "</tr>"]
    for frm in BUCKET_ORDER:
        row = roll_rates.get(frm, {})
        rr.append(f"<tr><td>{frm}</td>")
        for d in dests:
            v = row.get(d, 0.0)
            bg = heat(v, 1.0) if d != "CLOSED" else "#fff"
            rr.append(f'<td style="background:{bg}">{v * 100:.1f}%</td>'
                      if v else '<td style="color:#c3c8cf">·</td>')
        rr.append("</tr>")
    rr.append("</table></div>")

    # vintage triangle
    mobs = sorted({m for per in vintage.values() for m in per})
    vt = ['<div class="scroll"><table><tr><th>Cohort</th>'
          + "".join(f"<th>MOB {m}</th>" for m in mobs) + "</tr>"]
    vmax = max((v for per in vintage.values() for v in per.values()), default=0.0)
    for cohort, per in vintage.items():
        vt.append(f"<tr><td>{cohort}</td>")
        for m in mobs:
            if m in per:
                vt.append(f'<td style="background:{heat(per[m], vmax)}">'
                          f'{per[m] * 100:.1f}%</td>')
            else:
                vt.append('<td style="color:#c3c8cf">·</td>')
        vt.append("</tr>")
    vt.append("</table></div>")

    ce_points = sorted(summary["collection_efficiency_by_month"].items())[1:-1]

    defect_rows = "".join(
        f"<tr><td>{html.escape(k.replace('_', ' '))}</td><td>{v:,}</td></tr>"
        for k, v in sorted(defects.items(), key=lambda kv: -kv[1]))

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BFSI Lending Lakehouse — portfolio dashboard</title>
<style>{CSS}</style></head><body><div class="wrap">

<h1>BFSI Lending Lakehouse</h1>
<p class="sub">Synthetic Indian no-cost-EMI portfolio · position as at
<strong>{as_of.isoformat()}</strong> ·
<a href="https://github.com/Git-ShivamPatil/bfsi-lending-lakehouse">source on GitHub</a></p>
<p class="sub">{counts.get('loans', 0):,} loans ·
{counts.get('emi_schedule', 0):,} instalments ·
{counts.get('repayment_attempts', 0):,} repayment attempts. Every figure is
computed from the same extracts the PySpark medallion pipeline reads.</p>

<div class="tiles">{tile_html}</div>

<h2>Delinquency bucket mix by month, by value</h2>
<div class="card">{stacked_bars(mix_series)}<div class="legend">{legend}</div></div>

<h2>Monthly collection efficiency</h2>
<div class="card">{line_chart(ce_points)}
<p class="note">Of what was billed in a month, the share collected in that same
month — the demanding definition: it takes no credit for arrears recovered from
earlier months.</p></div>

<h2>Roll-rate matrix</h2>
<div class="card">{''.join(rr)}
<p class="note">Where accounts in each bucket at month end were a month later.
<code>CLOSED</code> is a loan repaid in full; <code>WRITTEN_OFF</code> is one
that crossed {C.WRITE_OFF_DPD} DPD and was taken as a loss. Those are opposite
outcomes, so they are counted apart — and folding either into
<code>CURRENT</code> would flatter the cure rate.</p></div>

<h2>Vintage: ever 30+ DPD by months on book</h2>
<div class="card">{''.join(vt)}
<p class="note">Cumulative, and measured at equal months on book, so cohorts are
comparable. A point-in-time reading falls when accounts cure and makes cohorts
look like they improve with age.</p></div>

<h2>Data quality — defects injected on purpose</h2>
<div class="card"><div class="scroll"><table>
<tr><th>Defect</th><th>Rows</th></tr>{defect_rows}</table></div>
<p class="note">The generator corrupts the book at known rates so the 29-rule
validation layer is scored against a ground truth rather than grading its own
homework. Every one of these is caught and quarantined with the rule it broke.</p></div>

<p class="note" style="margin-top:30px">Generated by
<code>docs/build_dashboard.py</code> · no JavaScript, no external requests.</p>
</div></body></html>
"""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", type=Path, default=Path("data/raw"))
    ap.add_argument("--as-of", required=True)
    ap.add_argument("--out", type=Path, default=Path("docs/index.html"))
    args = ap.parse_args(argv)

    as_of = date.fromisoformat(args.as_of)
    summary, mix, roll, vintage, manifest = build_metrics(args.data, as_of)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(summary, mix, roll, vintage, manifest, as_of),
                        encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
