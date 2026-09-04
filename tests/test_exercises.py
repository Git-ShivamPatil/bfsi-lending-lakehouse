"""Run every SQL exercise against a hand-computed oracle.

The design is the same argument `validation/backtest.py` makes for the pipeline,
applied to the query layer. Each exercise ships three files:

    NN_name.sql          the answer
    NN_name.naive.sql    the same question written the way it is usually written
    NN_name.fixture.py   a small input, and the expected output computed BY HAND

and this module asserts two things about them:

    answer  == expected      the query is right
    naive   != expected      the trap it is teaching is real

The second assertion is the one that makes the pack worth anything. A test that
only checks the correct query passes tells you nothing about whether the
question had a trap in it; pinning the *wrong* answer is what proves the
distinction exists and that the exercise is about something.

Fixtures are tens of rows, hand-computed. That proves semantics, not
performance -- see exercises/README.md, which says so out loud.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

EXERCISES = Path(__file__).resolve().parents[1] / "exercises"

#: Float comparison tolerance. Rates are rounded in the SQL itself; this only
#: absorbs the last-bit noise of a double round-trip.
TOL = 1e-9


def _load_fixture(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cases():
    if not EXERCISES.is_dir():
        return []
    return sorted(EXERCISES.glob("*.fixture.py"))


def _ids(path: Path) -> str:
    return path.name.removesuffix(".fixture.py")


@pytest.fixture(scope="session")
def supports_qualify(spark) -> bool:
    """QUALIFY is in the Spark 4.2 grammar and absent from 3.5.

    Probed rather than assumed, because the CI matrix runs both and the point of
    the fallback exercises is that the boundary is real. Databricks has had it
    since DBR 10.4.
    """
    try:
        spark.sql("SELECT 1 AS x QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1").collect()
        return True
    except Exception:
        return False


def _register(spark, fixture) -> None:
    for name, (schema, rows) in fixture.VIEWS.items():
        spark.createDataFrame(rows, schema).createOrReplaceTempView(name)


def _rows(spark, sql_path: Path):
    return [tuple(r) for r in spark.sql(sql_path.read_text(encoding="utf-8")).collect()]


def _same(a, b) -> bool:
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for va, vb in zip(ra, rb):
            if isinstance(va, float) or isinstance(vb, float):
                if va is None or vb is None:
                    if va is not vb:
                        return False
                elif abs(va - vb) > TOL:
                    return False
            elif va != vb:
                return False
    return True


@pytest.mark.parametrize("fixture_path", _cases(), ids=_ids)
def test_exercise_answer_matches_the_hand_computed_oracle(
        spark, supports_qualify, fixture_path):
    fixture = _load_fixture(fixture_path)
    if getattr(fixture, "REQUIRES_QUALIFY", False) and not supports_qualify:
        pytest.skip("QUALIFY requires Spark 4.2+ or DBR 10.4+")

    stem = fixture_path.name.removesuffix(".fixture.py")
    _register(spark, fixture)

    got = _rows(spark, EXERCISES / f"{stem}.sql")
    assert _same(got, fixture.EXPECTED), (
        f"{stem}: the answer does not match the hand-computed oracle\n"
        f"  expected: {fixture.EXPECTED}\n"
        f"  got:      {got}")


@pytest.mark.parametrize("fixture_path", _cases(), ids=_ids)
def test_the_naive_version_really_does_get_it_wrong(
        spark, supports_qualify, fixture_path):
    """The trap has to be real.

    If the naive query happens to agree with the oracle, the exercise is not
    teaching anything and the fixture is not exercising the trap it claims to.
    """
    fixture = _load_fixture(fixture_path)
    if getattr(fixture, "REQUIRES_QUALIFY", False) and not supports_qualify:
        pytest.skip("QUALIFY requires Spark 4.2+ or DBR 10.4+")

    stem = fixture_path.name.removesuffix(".fixture.py")
    naive_path = EXERCISES / f"{stem}.naive.sql"
    _register(spark, fixture)

    try:
        got = _rows(spark, naive_path)
    except Exception:
        # Some traps do not return a wrong answer -- they raise. ANSI-mode
        # division by zero and an invalid cast both do. That still counts as
        # differing from the oracle.
        return

    assert not _same(got, fixture.EXPECTED), (
        f"{stem}: the naive version agrees with the oracle, so the trap this "
        f"exercise claims to teach is not being exercised by the fixture")
