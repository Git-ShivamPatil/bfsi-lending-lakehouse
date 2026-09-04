"""Check the Declarative Automation Bundle without a Databricks workspace.

`databricks bundle validate` needs credentials -- it resolves the target
workspace before it will say anything -- so on a machine with no workspace it
fails with an auth error and tells you nothing about whether the bundle is
actually well-formed. That is a bad deal for a repository whose whole premise is
that it costs nothing to run: the bundle would be the one artefact nobody could
check.

The CLI will however emit its own JSON schema offline, via `databricks bundle
schema`. Validating the merged bundle against that catches the errors worth
catching at this stage -- a misspelled key, a task type that does not exist, a
`new_cluster` block that would be rejected on serverless -- and it catches them
in CI, on every push, with no account and no token.

What this does NOT prove is that the bundle deploys. Schema-valid is not
deployable: it says nothing about whether the workspace path exists, whether the
environment version is available, or whether the volume has been created. The
README is explicit about that distinction.

    python -m tools.validate_bundle
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def merged_bundle(root: Path) -> dict:
    """Merge databricks.yml with everything it includes, as the CLI would."""
    import yaml

    bundle = yaml.safe_load((root / "databricks.yml").read_text(encoding="utf-8"))
    for path in sorted((root / "resources").glob("*.yml")):
        part = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for key, value in part.items():
            if key != "resources":
                bundle[key] = value
                continue
            bundle.setdefault("resources", {})
            for kind, items in value.items():
                bundle["resources"].setdefault(kind, {}).update(items)
    return bundle


def cli_schema() -> dict:
    """Ask the installed CLI for its bundle schema."""
    exe = shutil.which("databricks")
    if exe is None:
        raise SystemExit("databricks CLI not on PATH; install it or skip this check")
    out = subprocess.run([exe, "bundle", "schema"], capture_output=True, check=True)
    # The CLI writes a UTF-8 BOM on Windows, which json.loads rejects outright.
    return json.loads(out.stdout.decode("utf-8-sig"))


def forbidden_on_serverless(bundle: dict) -> list[str]:
    """Free Edition is serverless-only, and the schema does not know that.

    Defining a cluster is schema-valid and undeployable there, which is exactly
    the sort of error that only shows up after someone has signed up for an
    account. Cheaper to fail here.
    """
    problems = []
    jobs = bundle.get("resources", {}).get("jobs", {})
    for name, job in jobs.items():
        if "job_clusters" in job:
            problems.append(f"job {name!r} defines job_clusters; serverless has none")
        for task in job.get("tasks", []):
            key = task.get("task_key", "?")
            if "new_cluster" in task or "existing_cluster_id" in task:
                problems.append(f"task {name}.{key} pins a cluster")
            for lib in task.get("libraries", []):
                if "jar" in lib or "maven" in lib:
                    problems.append(
                        f"task {name}.{key} needs a JAR or Maven coordinate, "
                        "neither of which serverless supports")
    return problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", type=Path, default=REPO_ROOT)
    args = ap.parse_args(argv)

    bundle = merged_bundle(args.root)

    import jsonschema

    errors = sorted(jsonschema.Draft7Validator(cli_schema()).iter_errors(bundle),
                    key=lambda e: list(e.path))
    for e in errors:
        where = "/".join(str(p) for p in e.path) or "<root>"
        print(f"SCHEMA {where}: {e.message}")

    problems = forbidden_on_serverless(bundle)
    for p in problems:
        print(f"SERVERLESS {p}")

    if errors or problems:
        print(f"\nFAIL: {len(errors)} schema error(s), {len(problems)} serverless problem(s)")
        return 1

    jobs = bundle.get("resources", {}).get("jobs", {})
    tasks = sum(len(j.get("tasks", [])) for j in jobs.values())
    print(f"OK: {len(jobs)} job(s), {tasks} task(s), schema-valid and "
          "serverless-compatible")
    return 0


if __name__ == "__main__":
    sys.exit(main())
