"""One entrypoint for every medallion stage, for Databricks job tasks.

Why this file exists rather than the job pointing straight at
`pipeline/bronze/ingest.py`: the pipeline modules use relative imports
(`from ..common import Layout`), which work under `python -m pipeline.bronze.ingest`
and fail under a bare `python pipeline/bronze/ingest.py` with

    ImportError: attempted relative import with no known parent package

A Databricks `spark_python_task` does the second of those. So the task points
here instead, and this module puts the repository root on `sys.path` before
importing anything, which makes the packages importable by name.

    python jobs/run_stage.py --stage silver --reporting-date 2026-08-31

Each stage is the same callable the CLI modules expose, so there is exactly one
implementation of each stage and this file only chooses between them.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Must happen before the pipeline imports below: on Databricks the working
# directory is the job's, not the repo's, and `pipeline` is only importable once
# its parent is on the path.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.common import Layout, get_spark  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--stage", required=True,
                    choices=("bronze", "silver", "snapshot", "gold"))
    ap.add_argument("--catalog", default="workspace")
    ap.add_argument("--schema", default="default")
    ap.add_argument("--raw", default=None,
                    help="landing zone; defaults to the layout volume")
    ap.add_argument("--reporting-date", default=None)
    ap.add_argument("--window-start", default=None)
    ap.add_argument("--generator-version", default="unknown")
    ap.add_argument("--write-off-dpd", type=int, default=180)
    args = ap.parse_args(argv)

    layout = Layout(catalog=args.catalog, schema=args.schema, fmt="delta",
                    raw_volume=f"/Volumes/{args.catalog}/{args.schema}/raw")
    spark = get_spark(f"bfsi-{args.stage}")
    raw = args.raw or layout.raw_volume

    def need(name: str, value):
        if value is None:
            ap.error(f"--{name} is required for --stage {args.stage}")
        return value

    if args.stage == "bronze":
        from pipeline.bronze.ingest import ingest_all
        counts = ingest_all(spark, layout, raw,
                            need("reporting-date", args.reporting_date),
                            args.generator_version)

    elif args.stage == "silver":
        from pipeline.silver.clean import build
        counts = build(spark, layout, need("reporting-date", args.reporting_date))

    elif args.stage == "snapshot":
        from pipeline.silver.snapshot import build
        df = build(spark, layout,
                   need("window-start", args.window_start),
                   need("reporting-date", args.reporting_date),
                   write_off_dpd=args.write_off_dpd)
        counts = {"loan_snapshot": df.count()}

    else:
        from pipeline.gold.metrics import build
        counts = build(spark, layout)

    for name, n in counts.items():
        print(f"{args.stage}_{name}: {n:,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
