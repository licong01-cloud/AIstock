"""Compact quality contracts: denominator closure, basis identity and no writes."""

from contextlib import contextmanager

import pytest

from backend.services.factor_research.quality import BASIS, diagnose
from backend.services.factor_research.quality_repository import quality_report

DATES = {"as_of": "2026-08-31", "history_start": "2025-01-01", "recent_start": "2026-07-01"}


def catalog(ident=1, name="m_a", **extra):
    return {
        "id": ident,
        "factor_name": name,
        "source": "manual",
        "is_available": True,
        "expression": "close/open",
        **extra,
    }


def metric(ident=1, **extra):
    row = dict.fromkeys(BASIS, "basis")
    row.update(
        id=ident,
        factor_name="m_a",
        factor_catalog_id=1,
        calc_batch_id="batch",
        data_start="2025-01-01",
        data_end="2026-06-30",
        snapshot_date="2026-08-31",
        direction=1,
        eval_window="history",
        ic_mean=0.1,
        coverage=0.9,
        n_trading_days=360,
    )
    row.update(extra)
    return row


def report(catalogs=None, metrics=None, correlations=None):
    return diagnose(catalogs or [catalog()], metrics or [], [], correlations or [], **DATES)


def test_denominator_keeps_disabled_missing_and_ambiguous_sources():
    catalogs = [catalog(), catalog(2, source="rdagent", is_available=False), catalog(3, "m_b")]
    result = report(catalogs, [metric(factor_catalog_id=None)])
    assert result["summary"]["total"] == 3
    assert result["summary"]["reviewed"] + result["summary"]["pending"] == 3
    assert all("name_only_source_ambiguous" in row["reasons"] for row in result["factors"][:2])
    assert result["factors"][1]["is_available_unchanged"] is False


@pytest.mark.parametrize(
    "updates,reason",
    [
        ({"ic_mean": float("nan")}, "ic_unavailable"),
        ({"coverage": float("inf")}, "nonfinite_coverage"),
        ({"n_trading_days": 0}, "metric_sample_unavailable"),
        ({"data_end": "2026-09-01"}, "metric_after_as_of"),
    ],
)
def test_bad_metrics_are_pending_not_silent_zero(updates, reason):
    factor = report(metrics=[metric(**updates)])["factors"][0]
    assert reason in factor["reasons"] and factor["status"] == "pending"


def test_diagnosis_is_order_invariant_and_repository_is_read_only():
    catalogs, metrics = [catalog(), catalog(2, "m_b")], [metric()]
    expected = report(catalogs, metrics)
    assert report(list(reversed(catalogs)), list(reversed(metrics))) == expected
    statements = []

    class Cursor:
        def execute(self, sql, _params=None):
            statements.append(sql.strip().split()[0].upper())

        def fetchall(self):
            return []

    class Repo:
        @contextmanager
        def cursor(self):
            yield Cursor()

    actual = quality_report(Repo(), **DATES)
    assert statements and set(statements) == {"SET", "SELECT"}
    assert actual["official_writes"] is False
