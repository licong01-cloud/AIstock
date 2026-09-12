from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.advisory_historical_range.comparison import (
    build_historical_range_comparison,
)
from backend.services.advisory_historical_range.query_repository import (
    HistoricalRangeQueryError,
    PostgresHistoricalRangeQueryRepository,
)
from backend.services.advisory_historical_range.service import HistoricalRangeApplicationService


def _fact(
    run_id: str,
    *,
    batch_id: str = "ahrb_1",
    policy_hash: str = "a" * 64,
    code_hash: str = "b" * 64,
    summary: bool = True,
    value: str = "0.10",
) -> dict[str, Any]:
    return {
        "range_run_id": run_id,
        "batch_id": batch_id,
        "research_program_id": f"program_{run_id}",
        "package_id": f"package_{run_id}",
        "package_version": "v1",
        "manifest_sha256": "c" * 64,
        "status": "COMPLETED",
        "summary_id": f"summary_{run_id}" if summary else None,
        "summary_version": 2 if summary else None,
        "summary_artifact_hash": "d" * 64 if summary else None,
        "summary_policy_hash": policy_hash if summary else None,
        "producer_code_hash": code_hash if summary else None,
        "metrics": [
            {
                "metric_key": "list:return:h5",
                "status": "AVAILABLE",
                "value": value,
                "coverage": {"sample_count": 12},
            }
        ] if summary else None,
        "unavailable_metrics": [],
        "day_status_counts": {"COMPLETE": 10, "VALID_NO_CANDIDATE": 2},
        "omitted_metric_counts": {"available_daily_recall": 3, "unavailable_daily_recall": 7},
    }


def test_comparable_summaries_compute_candidate_minus_baseline() -> None:
    result = build_historical_range_comparison(
        batch_id="ahrb_1",
        baseline=_fact("run_base", value="0"),
        candidate=_fact("run_candidate", value="0.125"),
    )

    assert result["comparability"]["status"] == "COMPARABLE"
    assert result["metrics"][0]["delta"] == "0.125"
    assert result["interpretation"] == {
        "delta_semantics": "CANDIDATE_MINUS_BASELINE",
        "winner_declared": False,
        "significance_claimed": False,
    }
    assert result["day_support"]["baseline"]["successful_day_count"] == 12
    assert result["day_support"]["baseline"]["valid_no_candidate_day_count"] == 2
    assert result["omitted_diagnostics"]["baseline"] == {
        "available_daily_recall": 3,
        "unavailable_daily_recall": 7,
    }


def test_missing_summary_is_typed_incomplete_and_never_fabricates_delta() -> None:
    result = build_historical_range_comparison(
        batch_id="ahrb_1",
        baseline=_fact("run_base", summary=False),
        candidate=_fact("run_candidate"),
    )

    assert result["comparability"]["status"] == "INCOMPLETE_EVIDENCE"
    assert result["comparability"]["blockers"] == ["BASELINE_SUMMARY_UNAVAILABLE"]
    assert result["metrics"][0]["baseline"]["status"] == "NOT_REPORTED"
    assert result["metrics"][0]["delta"] is None


def test_legacy_summary_without_complete_identity_is_not_treated_as_comparable() -> None:
    candidate = _fact("run_candidate")
    candidate["producer_code_hash"] = None
    result = build_historical_range_comparison(
        batch_id="ahrb_1",
        baseline=_fact("run_base"),
        candidate=candidate,
    )

    assert result["comparability"]["status"] == "INCOMPLETE_EVIDENCE"
    assert result["comparability"]["blockers"] == ["CANDIDATE_SUMMARY_IDENTITY_INCOMPLETE"]
    assert result["metrics"][0]["delta"] is None


@pytest.mark.parametrize(
    ("field", "replacement", "reason"),
    [
        ("summary_policy_hash", "e" * 64, "SUMMARY_POLICY_HASH_MISMATCH"),
        ("producer_code_hash", "f" * 64, "PRODUCER_CODE_HASH_MISMATCH"),
    ],
)
def test_identity_mismatch_is_incompatible(field: str, replacement: str, reason: str) -> None:
    candidate = _fact("run_candidate")
    candidate[field] = replacement
    result = build_historical_range_comparison(
        batch_id="ahrb_1",
        baseline=_fact("run_base"),
        candidate=candidate,
    )

    assert result["comparability"]["status"] == "INCOMPATIBLE"
    assert reason in result["comparability"]["blockers"]
    assert result["metrics"][0]["delta"] is None


def test_cross_batch_and_same_run_requests_fail_closed() -> None:
    with pytest.raises(HistoricalRangeQueryError) as same:
        build_historical_range_comparison(
            batch_id="ahrb_1",
            baseline=_fact("run_1"),
            candidate=_fact("run_1"),
        )
    assert same.value.reason_code == "ADVISORY_HR_COMPARISON_RUNS_NOT_DISTINCT"

    with pytest.raises(HistoricalRangeQueryError) as cross_batch:
        build_historical_range_comparison(
            batch_id="ahrb_1",
            baseline=_fact("run_1"),
            candidate=_fact("run_2", batch_id="ahrb_2"),
        )
    assert cross_batch.value.reason_code == "ADVISORY_HR_COMPARISON_BATCH_MISMATCH"


def test_unavailable_and_missing_metric_states_are_preserved_without_delta() -> None:
    baseline = _fact("run_base")
    baseline["metrics"] = []
    baseline["unavailable_metrics"] = [
        {
            "metric_key": "list:return:h5:odds",
            "status": "UNAVAILABLE",
            "value": None,
            "reason_code": "POSITIVE_OR_NEGATIVE_DENOMINATOR_EMPTY",
            "coverage": {"numeric_return_count": 4},
        }
    ]
    result = build_historical_range_comparison(
        batch_id="ahrb_1",
        baseline=baseline,
        candidate=_fact("run_candidate"),
    )

    assert result["comparability"]["status"] == "COMPARABLE"
    assert result["comparability"]["warnings"] == ["METRIC_KEY_SET_DIFFERS"]
    by_key = {item["metric_key"]: item for item in result["metrics"]}
    assert by_key["list:return:h5:odds"]["baseline"]["status"] == "UNAVAILABLE"
    assert by_key["list:return:h5:odds"]["candidate"]["status"] == "NOT_REPORTED"
    assert by_key["list:return:h5:odds"]["delta"] is None


def test_non_finite_metric_fails_closed() -> None:
    with pytest.raises(HistoricalRangeQueryError) as raised:
        build_historical_range_comparison(
            batch_id="ahrb_1",
            baseline=_fact("run_base"),
            candidate=_fact("run_candidate", value="NaN"),
        )
    assert raised.value.reason_code == "ADVISORY_HR_COMPARISON_METRIC_INVALID"


def test_invalid_day_support_fails_closed() -> None:
    candidate = _fact("run_candidate")
    candidate["day_status_counts"] = {"COMPLETE": -1}
    with pytest.raises(HistoricalRangeQueryError) as raised:
        build_historical_range_comparison(
            batch_id="ahrb_1",
            baseline=_fact("run_base"),
            candidate=candidate,
        )
    assert raised.value.reason_code == "ADVISORY_HR_COMPARISON_DAY_SUPPORT_INVALID"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("omitted_metric_counts", None),
        ("omitted_metric_counts", {"available_daily_recall": 0}),
        (
            "omitted_metric_counts",
            {"available_daily_recall": 0, "unavailable_daily_recall": -1},
        ),
    ],
)
def test_summary_requires_complete_nonnegative_omitted_diagnostic_counts(
    field: str,
    value: object,
) -> None:
    candidate = _fact("run_candidate")
    candidate[field] = value
    with pytest.raises(HistoricalRangeQueryError) as raised:
        build_historical_range_comparison(
            batch_id="ahrb_1",
            baseline=_fact("run_base"),
            candidate=candidate,
        )
    assert raised.value.reason_code == "ADVISORY_HR_COMPARISON_SUMMARY_INVALID"


class _Query:
    def __init__(self, facts: list[dict[str, Any]]) -> None:
        self.facts = facts
        self.calls: list[tuple[str, ...]] = []

    def get_comparison_facts(self, range_run_ids: tuple[str, ...]) -> list[dict[str, Any]]:
        self.calls.append(range_run_ids)
        return self.facts


def test_application_service_reads_both_runs_once_and_preserves_roles() -> None:
    query = _Query([_fact("candidate"), _fact("baseline")])
    runtime = SimpleNamespace(query=query)
    service = HistoricalRangeApplicationService(runtime_factory=lambda: runtime)

    result = service.compare_runs(
        batch_id="ahrb_1",
        baseline_range_run_id="baseline",
        candidate_range_run_id="candidate",
    )

    assert query.calls == [("baseline", "candidate")]
    assert result["baseline"]["range_run_id"] == "baseline"
    assert result["candidate"]["range_run_id"] == "candidate"


def test_application_service_reports_missing_run_identity() -> None:
    runtime = SimpleNamespace(query=_Query([_fact("baseline")]))
    service = HistoricalRangeApplicationService(runtime_factory=lambda: runtime)

    with pytest.raises(HistoricalRangeQueryError) as raised:
        service.compare_runs(
            batch_id="ahrb_1",
            baseline_range_run_id="baseline",
            candidate_range_run_id="candidate",
        )
    assert raised.value.reason_code == "ADVISORY_HR_RESOURCE_NOT_FOUND"


class _Cursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.statement = ""
        self.params: tuple[Any, ...] = ()

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, statement: str, params: tuple[Any, ...]) -> None:
        self.statement = statement
        self.params = params

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.session: dict[str, Any] = {}
        self.rolled_back = False

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def set_session(self, **kwargs: Any) -> None:
        self.session = kwargs

    def cursor(self, **_kwargs: Any) -> _Cursor:
        return self._cursor

    def rollback(self) -> None:
        self.rolled_back = True


def test_repository_uses_one_read_only_compact_projection() -> None:
    rows = [_fact("run_a"), _fact("run_b")]
    for row in rows:
        row["metric_total_count"] = 4
        row["unavailable_metric_total_count"] = 7
    cursor = _Cursor(rows)
    connection = _Connection(cursor)
    repository = PostgresHistoricalRangeQueryRepository(conn_factory=lambda: connection)

    rows = repository.get_comparison_facts(("run_a", "run_b"))

    assert len(rows) == 2
    assert connection.session == {
        "isolation_level": "REPEATABLE READ",
        "readonly": True,
        "autocommit": False,
    }
    assert connection.rolled_back is True
    assert "SELECT summary.*" not in cursor.statement
    assert "summary_artifact_ref" not in cursor.statement
    assert cursor.statement.count("jsonb_path_query_array(") == 2
    assert "latest.summary_json->'metrics'" in cursor.statement
    assert "latest.summary_json->'unavailable_metrics'" in cursor.statement
    assert cursor.params[0] == cursor.params[1]
    assert "strategy|conditional" in cursor.params[0]
    assert cursor.params[2] == ["run_a", "run_b"]
    assert rows[0]["omitted_metric_counts"] == {
        "available_daily_recall": 3,
        "unavailable_daily_recall": 7,
    }
