from __future__ import annotations

from datetime import date

import pytest

from backend.routers import qe_archive as qe_archive_router
from backend.services.qe_archive.long_trend_repository import QELongTrendEvaluationResultRepository
from backend.services.qe_archive.repository import QEArchiveRepository


class _Cursor:
    def __init__(self, handler):  # type: ignore[no-untyped-def]
        self._handler = handler
        self._rows: list[dict[str, object]] = []
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql: str, params=()) -> None:  # type: ignore[no-untyped-def]
        normalized = " ".join(sql.split())
        values = tuple(params)
        self.calls.append((normalized, values))
        self._rows = self._handler(normalized, values)

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, handler):  # type: ignore[no-untyped-def]
        self.cursor_value = _Cursor(handler)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self.cursor_value


def test_run_options_are_business_searchable_and_bounded() -> None:
    row = {
        "value": "qear_run_internal",
        "task_name": "长期趋势基准演进",
        "run_type": "evolution_loop",
        "model_type": "LSTM",
        "label_horizon": 20,
        "completed_at": "2026-07-18T12:05:00+08:00",
    }

    def handler(sql: str, params: tuple[object, ...]) -> list[dict[str, object]]:
        assert "LEFT JOIN qe_evolution_tasks" in sql
        assert "t.task_name" in sql
        assert "r.run_id ILIKE" not in sql
        assert "LIMIT %s" in sql
        assert params[-1] == 50
        assert params.count("%LSTM%") == 5
        return [row]

    connection = _Connection(handler)
    repository = QEArchiveRepository(connection_provider=lambda: connection)

    result = repository.query_operator_run_options(
        search="LSTM",
        run_type="evolution_loop",
        model_type="LSTM",
        label_horizon=20,
        completed_from=date(2026, 7, 1),
        completed_to=date(2026, 7, 31),
        limit=500,
    )

    assert result == {"items": [row], "limit": 50}


def test_long_trend_options_aggregate_without_metric_browser_scan() -> None:
    task = {"value": "task-internal", "task_name": "长期趋势候选", "evaluation_count": 1}
    snapshot = {"value": "snapshot-internal", "latest_evaluation_asof": "2026-06-30"}
    sector = {"value": "801010", "sector_name": "农林牧渔", "evaluation_count": 1}

    def handler(sql: str, params: tuple[object, ...]) -> list[dict[str, object]]:
        assert "SELECT *" not in sql
        assert "e.status IN ('succeeded', 'partial')" in sql
        assert "platform_delivery_status_json->>'db' = 'published'" in sql
        assert params[-1] == 50
        if "GROUP BY e.parent_task_id" in sql:
            assert "qe_evolution_tasks" in sql
            return [task]
        if "GROUP BY e.outcome_dataset_snapshot_id" in sql:
            return [snapshot]
        if "WITH sector_options AS" in sql:
            assert "market.sw_index_member" in sql
            assert "m.metric_key = 'sector_signal_path'" in sql
            assert "LEFT JOIN LATERAL" in sql
            return [sector]
        raise AssertionError(sql)

    connection = _Connection(handler)
    repository = QELongTrendEvaluationResultRepository(connection_provider=lambda: connection)
    repository.ensure_schema_ready = lambda: None  # type: ignore[method-assign]

    result = repository.query_operator_options(
        search="长期",
        evaluation_asof_from=date(2026, 1, 1),
        evaluation_asof_to=date(2026, 7, 31),
        limit=500,
    )

    assert result == {"tasks": [task], "snapshots": [snapshot], "sectors": [sector], "limit": 50}
    assert len(connection.cursor_value.calls) == 3


def test_run_options_router_preserves_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Repository:
        def query_operator_run_options(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"items": [], "limit": kwargs["limit"]}

    monkeypatch.setattr(qe_archive_router, "get_repository", lambda: _Repository())
    response = qe_archive_router.query_qe_archive_operator_run_options(
        search="LSTM",
        run_type="evolution_loop",
        model_type="LSTM",
        label_horizon=20,
        completed_from=date(2026, 7, 1),
        completed_to=date(2026, 7, 31),
        limit=30,
    )

    assert response["data"]["items"] == []
    assert captured["completed_from"] == date(2026, 7, 1)
    assert captured["label_horizon"] == 20


def test_long_trend_options_router_preserves_same_vintage_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _Repository:
        def query_operator_options(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"tasks": [], "snapshots": [], "sectors": [], "limit": kwargs["limit"]}

    monkeypatch.setattr(qe_archive_router, "QELongTrendEvaluationResultRepository", _Repository)
    response = qe_archive_router.query_qe_archive_long_trend_options(
        search="长期",
        task_id="task-internal",
        outcome_dataset_snapshot_id="snapshot-internal",
        evaluation_asof_from=date(2026, 1, 1),
        evaluation_asof_to=date(2026, 7, 31),
        limit=30,
    )

    assert response["snapshots"] == []
    assert captured["task_id"] == "task-internal"
    assert captured["outcome_dataset_snapshot_id"] == "snapshot-internal"
