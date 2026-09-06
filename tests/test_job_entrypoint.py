"""The job entrypoint, executed the way a serverless task executes it.

This file exists because of a bug that reached a real deployment. The bundle was
schema-valid in CI on every push, `databricks bundle validate` passed against the
live workspace, the deploy succeeded -- and the first task died on line 29 of
`jobs/run_stage.py` with `NameError: name '__file__' is not defined`.

The cause is that a serverless `spark_python_task` does **not** run the file as a
script. There is no `__main__` module and no `runpy`. The platform reads the
bytes and evaluates them inside an ipykernel command namespace:

    with open(filename, "rb") as f:
        exec(compile(f.read(), filename, 'exec'))

`exec` of a code object does not bind `__file__`. So the single thing
`run_stage.py` exists to do -- put the repository root on `sys.path` so the
`pipeline` package is importable by name -- was the thing that failed.

What makes this worth a test rather than a comment: the failure needs no
Databricks account to reproduce. Three lines of `exec(compile(...))` with
`__file__` withheld reproduce it exactly. Of the platform bugs this project has
hit, it is the first that CI can actually guard, so it is guarded here.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINT = REPO_ROOT / "jobs" / "run_stage.py"


def exec_like_serverless(path: Path, name: str) -> dict:
    """Evaluate `path` the way a serverless spark_python_task does.

    `__file__` is deliberately absent from the namespace, because that is the
    whole point: the platform gives the module no file identity, only the
    filename it passed to `compile`.
    """
    namespace = {"__name__": name, "__builtins__": __builtins__}
    code = compile(path.read_bytes(), str(path), "exec")
    exec(code, namespace)  # noqa: S102 -- reproducing the platform, deliberately
    return namespace


def test_the_entrypoint_does_not_need_dunder_file():
    """The regression. Before the fix this raised NameError on import."""
    namespace = exec_like_serverless(ENTRYPOINT, "not_main")

    assert "__file__" not in namespace, (
        "the test is not reproducing the platform if __file__ is present")


def test_the_entrypoint_finds_the_repository_root_without_dunder_file():
    """Finding *a* root is not enough -- it has to be the right one.

    A fallback that silently resolves against the working directory would pass
    the test above and then put the wrong directory on sys.path in production,
    where the working directory is the job's rather than the repository's.
    """
    namespace = exec_like_serverless(ENTRYPOINT, "not_main")

    assert namespace["REPO_ROOT"] == REPO_ROOT
    assert (namespace["REPO_ROOT"] / "pipeline").is_dir()
    assert str(namespace["REPO_ROOT"]) in sys.path


def test_a_successful_stage_does_not_raise_system_exit():
    """The second bug this file guards, and the more embarrassing one.

    `raise SystemExit(main())` is the idiomatic trailer for a script. Inside the
    ipykernel command a serverless task runs in, `SystemExit` is not a clean exit
    -- it is an exception that escapes the cell, so the platform marks the task
    FAILED. The first bronze stage that actually worked reported all six table
    counts and was then recorded as a failure with `SystemExit: 0`.

    `cli` looks `main` up in its module globals, which is the namespace `exec`
    was handed, so substituting it here needs no patching machinery.
    """
    namespace = exec_like_serverless(ENTRYPOINT, "not_main")
    namespace["main"] = lambda argv=None: 0

    namespace["cli"]()  # must simply return -- any raise here fails the task


def test_a_failed_stage_still_raises_so_the_task_goes_red():
    """The other direction. A stage that fails must not report success.

    Returning quietly on failure would hand Databricks a green run over a broken
    stage, and downstream tasks would build on whatever bronze half-wrote.
    """
    import pytest

    namespace = exec_like_serverless(ENTRYPOINT, "not_main")
    namespace["main"] = lambda argv=None: 3

    with pytest.raises(SystemExit) as excinfo:
        namespace["cli"]()

    assert excinfo.value.code == 3


def test_the_stage_argument_is_constrained_to_the_four_layers():
    """A typo in the bundle YAML should fail loudly, not run the wrong stage.

    The parameters live in `resources/medallion.job.yml`, far from this code, and
    nothing type-checks the gap between them.
    """
    namespace = exec_like_serverless(ENTRYPOINT, "not_main")

    import pytest

    with pytest.raises(SystemExit):
        namespace["main"](["--stage", "bronz"])


def test_every_stage_the_bundle_asks_for_is_a_stage_the_entrypoint_knows():
    """Close that gap in the direction that actually matters.

    Reads the deployed job specification and checks each `--stage=` parameter
    against the entrypoint's own choices, so renaming a stage in one place and
    not the other fails here rather than on a serverless task ten minutes into a
    run.
    """
    import re

    spec = (REPO_ROOT / "resources" / "medallion.job.yml").read_text(
        encoding="utf-8")
    asked = set(re.findall(r'--stage=(\w+)', spec))

    assert asked, "no --stage parameters found; has the job spec moved?"

    source = ENTRYPOINT.read_text(encoding="utf-8")
    known = set(re.findall(r'choices=\("(\w+)", "(\w+)", "(\w+)", "(\w+)"\)',
                           source)[0])

    assert asked <= known, f"job spec asks for unknown stages: {asked - known}"
