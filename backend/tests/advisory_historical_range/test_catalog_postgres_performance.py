from __future__ import annotations

import threading
import time
import tracemalloc
import json
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.catalog_planner import (
    HistoricalRangeSourceInputUnavailable,
)
from backend.services.advisory_historical_range.catalog_postgres import (
    PostgresHistoricalRangeCatalogExecutor,
    _PostgresRequirementResolver,
    _canonical_bytes,
    _requirement_work_weight,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCatalogPhase,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
)
from backend.tests.advisory_historical_range.conftest import digest, resolved_request


OBSERVED_AT = datetime(2026, 8, 5, 1, tzinfo=UTC)


def _plan(count: int) -> HistoricalRangeSourceRequirementPlanV1:
    resolved = resolved_request()
    requirements = tuple(
        HistoricalRangeSourceRequirementV1(
            requirement_id=f"requirement-{ordinal:03d}",
            source_role="package_runtime_assets",
            dataset_id="strategy_pkg.package_manifest_assets",
            query_template_id="frozen_artifact_identity",
            query_template_version="v1",
            query_template_hash=digest("frozen-query"),
            parameter_template={"content_hash": digest(f"content-{ordinal}"), "row_count": 1},
            partition_ref_template=f"package:pkg-test/{ordinal:03d}",
            decision_trade_date=date(2026, 6, 2),
            required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
            missing_reason_code="ADVISORY_HR_PACKAGE_RUNTIME_ASSET_UNAVAILABLE",
        )
        for ordinal in range(1, count + 1)
    )
    return HistoricalRangeSourceRequirementPlanV1(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=resolved.frozen_programs,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        requirements=requirements,
    )


class _ConnectionTracker:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active_connections = 0
        self.max_active_connections = 0
        self.imported_snapshots: list[str] = []

    def connect(self) -> _Connection:
        return _Connection(self)

    def enter(self) -> None:
        with self.lock:
            self.active_connections += 1
            self.max_active_connections = max(
                self.max_active_connections,
                self.active_connections,
            )

    def exit(self) -> None:
        with self.lock:
            self.active_connections -= 1


class _ControlCursor:
    def __init__(self, tracker: _ConnectionTracker) -> None:
        self.tracker = tracker
        self.row = None

    def __enter__(self) -> _ControlCursor:
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def execute(self, query, params=None) -> None:  # noqa: ANN001
        normalized = " ".join(str(query).split())
        if "pg_export_snapshot" in normalized:
            self.row = {"observed_at": OBSERVED_AT, "snapshot_id": "snapshot-shared"}
            return
        assert normalized == "SET TRANSACTION SNAPSHOT %s"
        assert params == ("snapshot-shared",)
        with self.tracker.lock:
            self.tracker.imported_snapshots.append(params[0])
        time.sleep(0.02)

    def fetchone(self):  # noqa: ANN201
        return self.row


class _Connection:
    def __init__(self, tracker: _ConnectionTracker) -> None:
        self.tracker = tracker

    def __enter__(self) -> _Connection:
        self.tracker.enter()
        return self

    def __exit__(self, *_args) -> bool:
        self.tracker.exit()
        return False

    def set_session(self, **_kwargs) -> None:  # noqa: ANN003
        return None

    def cursor(self, **_kwargs) -> _ControlCursor:  # noqa: ANN003
        return _ControlCursor(self.tracker)

    def rollback(self) -> None:
        return None


def test_catalog_chunk_uses_one_snapshot_and_bounded_parallel_connections() -> None:
    tracker = _ConnectionTracker()
    result = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=6,
    ).resolve_chunk(
        plan=_plan(12),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolved_members={},
        chunk_size=12,
    )

    assert [delta.ordinal for delta in result.checkpoint.member_delta] == list(range(1, 13))
    assert tracker.imported_snapshots == ["snapshot-shared"] * 6
    assert 3 <= tracker.max_active_connections <= 7
    assert tracker.active_connections == 0


def _member(requirement: HistoricalRangeSourceRequirementV1) -> HistoricalRangeSourceRevisionMemberV1:
    return HistoricalRangeSourceRevisionMemberV1(
        requirement_id=requirement.requirement_id,
        source_role=requirement.source_role,
        dataset_id=requirement.dataset_id,
        partition_ref=requirement.partition_ref_template,
        decision_trade_date=requirement.decision_trade_date,
        query_template_id=requirement.query_template_id,
        query_template_version=requirement.query_template_version,
        query_template_hash=requirement.query_template_hash,
        parameter_hash=requirement.parameter_template_hash,
        row_count=1,
        content_hash=digest(requirement.requirement_id),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=OBSERVED_AT,
    )


def test_parallel_prefetch_replays_first_failure_at_original_ordinal(monkeypatch) -> None:  # noqa: ANN001
    tracker = _ConnectionTracker()

    def resolve(self, *, requirement, **_kwargs):  # noqa: ANN001, ANN202
        if requirement.requirement_id in {"requirement-002", "requirement-004"}:
            raise HistoricalRangeSourceInputUnavailable(
                requirement.missing_reason_code,
                "missing test input",
                context={"requirement_id": requirement.requirement_id},
            )
        return _member(requirement)

    monkeypatch.setattr(_PostgresRequirementResolver, "resolve", resolve)
    result = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=4,
    ).resolve_chunk(
        plan=_plan(6),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolved_members={},
        chunk_size=6,
    )

    assert [delta.ordinal for delta in result.checkpoint.member_delta] == [1]
    assert result.checkpoint.next_requirement_ordinal == 2
    assert result.checkpoint.unresolved_requirement_delta[0].ordinal == 2
    assert result.checkpoint.unresolved_requirement_delta[0].context == {
        "requirement_id": "requirement-002"
    }


def test_worker_connection_failure_is_replayed_at_earliest_affected_ordinal(monkeypatch) -> None:  # noqa: ANN001
    tracker = _ConnectionTracker()
    executor = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=3,
    )

    def resolve_batch(*, requirements, **_kwargs):  # noqa: ANN001, ANN202
        if any(item.requirement_id == "requirement-002" for item in requirements):
            raise RuntimeError("worker connection failed")
        return {item.requirement_id: _member(item) for item in requirements}

    monkeypatch.setattr(executor, "_resolve_requirement_batch", resolve_batch)
    with pytest.raises(RuntimeError, match="worker connection failed"):
        executor.resolve_chunk(
            plan=_plan(6),
            catalog_generation=1,
            phase=HistoricalRangeCatalogPhase.DISCOVER,
            start_ordinal=1,
            resolved_members={},
            chunk_size=6,
        )


class _StreamingCursor:
    def __init__(self, row_count: int) -> None:
        self.remaining = row_count
        self.max_batch = 0
        self.itersize = 0
        self.closed = False
        self.description = None

    def __enter__(self) -> _StreamingCursor:
        return self

    def __exit__(self, *_args) -> bool:
        self.closed = True
        return False

    def execute(self, _sql, _params) -> None:  # noqa: ANN001
        return None

    def fetchmany(self, size: int):  # noqa: ANN201
        self.description = (SimpleNamespace(name="payload", type_code=3802),)
        count = min(size, self.remaining)
        self.remaining -= count
        self.max_batch = max(self.max_batch, count)
        return [
            {"payload": {"trade_date": "2026-06-02", "ts_code": f"{self.remaining + index:06d}.SZ"}}
            for index in range(count)
        ]


class _StreamingConnection:
    def __init__(self, row_count: int) -> None:
        self.cursor_instance = _StreamingCursor(row_count)
        self.cursor_names: list[str] = []

    def cursor(self, *, name: str, cursor_factory):  # noqa: ANN001, ANN201
        assert cursor_factory is not None
        self.cursor_names.append(name)
        return self.cursor_instance


def test_stream_query_uses_named_cursor_and_fixed_memory_batches() -> None:
    conn = _StreamingConnection(row_count=20_000)
    resolver = _PostgresRequirementResolver(
        cur=object(),
        conn=conn,
        observed_at=OBSERVED_AT,
        stream_fetch_size=64,
    )

    tracemalloc.start()
    row_count, content_hash, _schema_hash = resolver._stream_query("SELECT payload", ())
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert row_count == 20_000
    assert len(content_hash) == 64
    assert len(conn.cursor_names) == 1
    assert conn.cursor_names[0].startswith("ahr_catalog_")
    assert conn.cursor_instance.itersize == 64
    assert conn.cursor_instance.max_batch == 64
    assert conn.cursor_instance.closed is True
    assert peak < 32 * 1024 * 1024


@pytest.mark.parametrize(
    "payload",
    [
        {"plain": 12.34, "count": 7, "nullable": None, "unicode": "科技股"},
        {"small_exponent": 1e-7, "large_exponent": 1e20, "negative_zero": -0.0},
        {"nested": [1e-6, {"observed_at": OBSERVED_AT}]},
    ],
)
def test_fast_canonical_bytes_preserve_existing_hash_bytes(payload) -> None:  # noqa: ANN001
    expected = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        ensure_ascii=False,
    ).encode("utf-8")

    assert _canonical_bytes(payload) == expected


def test_parallel_scheduler_accounts_for_window_span() -> None:
    short, long = _plan(2).requirements
    short = short.model_copy(
        update={
            "query_template_id": "historical_market_history_window",
            "parameter_template": {"start_date": "2026-01-01", "trade_date": "2026-01-31"},
        }
    )
    long = long.model_copy(
        update={
            "query_template_id": "historical_market_history_window",
            "parameter_template": {"start_date": "2025-01-01", "trade_date": "2026-01-31"},
        }
    )

    assert _requirement_work_weight(long) > _requirement_work_weight(short) * 10


@pytest.mark.parametrize("max_workers", [0, 7])
def test_catalog_executor_rejects_unbounded_worker_counts(max_workers: int) -> None:
    with pytest.raises(ValueError, match="max_workers"):
        PostgresHistoricalRangeCatalogExecutor(
            conn_factory=lambda: None,
            max_workers=max_workers,
        )


def test_invalid_catalog_request_fails_before_opening_database_connection() -> None:
    tracker = _ConnectionTracker()
    with pytest.raises(ValueError, match="catalog_generation"):
        PostgresHistoricalRangeCatalogExecutor(conn_factory=tracker.connect).resolve_chunk(
            plan=_plan(1),
            catalog_generation=0,
            phase=HistoricalRangeCatalogPhase.DISCOVER,
            start_ordinal=1,
            resolved_members={},
        )

    assert tracker.max_active_connections == 0
