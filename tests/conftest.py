"""Test fixtures.

The Spark session is session-scoped because starting a JVM per test dominates
the runtime of a suite this size. Everything is driven through temp views rather
than `saveAsTable`, so the suite never needs a warehouse directory -- which also
means it runs on Windows without a Hadoop `winutils.exe` on PATH.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from pipeline.common import get_spark

    s = get_spark("bfsi-lending-lakehouse-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def generated(tmp_path_factory):
    """A small, real generated book -- the same code path the project ships."""
    from generator.generate import main as generate

    out = tmp_path_factory.mktemp("raw")
    generate([
        "--loans", "6000", "--merchants", "150", "--months", "24",
        "--seed", "42", "--as-of", "2026-08-31", "--out", str(out),
    ])
    return out


@pytest.fixture(scope="session")
def raw_views(spark, generated):
    """Register the generated CSVs as bronze-shaped temp views (all strings)."""
    from pipeline.bronze.ingest import SOURCES, add_lineage, read_source

    for name in SOURCES:
        df = add_lineage(read_source(spark, generated.as_posix(), name),
                         batch_id="test-batch", generator_version="test")
        df.createOrReplaceTempView(f"bronze_{name}")
    return generated


REPORTING_DATE = date(2026, 8, 31)
