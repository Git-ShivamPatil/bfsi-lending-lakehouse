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


def _repo_root() -> Path:
    """Locate the repository root without relying on `__file__`.

    A serverless `spark_python_task` does not run this file as a script. There is
    no `__main__` module and no `runpy`: the platform reads the bytes and
    evaluates them inside an ipykernel command namespace, roughly

        with open(filename, "rb") as f:
            exec(compile(f.read(), filename, 'exec'))

    and `exec` of a code object binds no `__file__`. Deriving the root from
    `__file__` therefore worked everywhere except the one place this file exists
    to serve, and the first deployed run died here with

        NameError: name '__file__' is not defined

    The code object's own `co_filename` is whatever the platform handed to
    `compile`, which is the absolute `/Workspace/.../jobs/run_stage.py` on a job
    task and the script path under `python jobs/run_stage.py`. So it survives
    both paths where `__file__` survives only one.

    The `pipeline` directory is checked rather than assumed: a candidate that
    resolved against the wrong working directory would otherwise put a plausible
    but wrong root on `sys.path` and fail later, further from the cause.
    `tests/test_job_entrypoint.py` reproduces the platform's execution model, so
    this is regression-tested without needing a Databricks account.
    """
    for candidate in (globals().get("__file__"),
                      sys._getframe().f_code.co_filename):
        if not candidate or candidate.startswith("<"):
            continue                      # "<string>", "<stdin>": no file identity
        root = Path(candidate).resolve().parents[1]
        if (root / "pipeline").is_dir():
            return root
    raise RuntimeError(
        "cannot locate the repository root: run_stage.py was evaluated with no "
        "usable file identity, and no candidate parent holds the pipeline package")


# Must happen before the pipeline imports below: on Databricks the working
# directory is the job's, not the repo's, and `pipeline` is only importable once
# its parent is on the path.
REPO_ROOT = _repo_root()
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


def cli(argv=None) -> None:
    """Run a stage and translate the result into a process exit code.

    `raise SystemExit(main())` is the idiomatic trailer for a script, and it is
    wrong here. A serverless `spark_python_task` evaluates this file inside an
    ipykernel command (see `_repo_root`), where `SystemExit` is not a clean exit
    but an exception that escapes the cell -- so the platform marks the task
    FAILED on a *successful* run. The first green bronze stage reported all six
    tables and then failed with

        SystemExit: 0
        UserWarning: To exit: use 'exit', 'quit', or Ctrl-D.

    Success therefore has to return rather than raise. A non-zero result still
    raises, because a task that fails must fail: silently returning would hand
    the platform a green run over a broken stage, which is the worse of the two
    directions to be wrong in.
    """
    code = main(argv)
    if code:
        raise SystemExit(code)


if __name__ == "__main__":
    cli()
