from __future__ import annotations

import hashlib
import json
import pickle
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from types import SimpleNamespace

import psycopg2
import pytest

from backend.services.advisory_historical_range import catalog_postgres as catalog_postgres_module
from backend.services.advisory_historical_range.catalog_planner import (
    HistoricalRangeSourceInputUnavailable,
)
from backend.services.advisory_historical_range.catalog_postgres import (
    PostgresHistoricalRangeCatalogExecutor,
    _PostgresRequirementResolver,
    _RequirementResolutionFailure,
    _canonical_bytes,
    _canonicalize_payload_batch,
    _plan_uses_source_cache,
    _requirement_uses_process_worker,
    _requirement_work_weight,
    _resolve_requirement_batch_process,
)
from backend.services.advisory_historical_range.catalog_source_cache import (
    CatalogSourceCacheError,
    CatalogSourceFileCache,
    canonical_payload_bytes,
    frame_payloads,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCatalogPhase,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
)
from backend.tests.advisory_historical_range.conftest import digest, resolved_request
from backend.services.advisory_historical_range.composition import (
    explicit_historical_range_connection_factory,
)


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


def _cacheable_plan(count: int) -> HistoricalRangeSourceRequirementPlanV1:
    base = _plan(count)
    return base.model_copy(
        update={
            "requirements": tuple(
                requirement.model_copy(
                    update={
                        "source_role": "pit_universe",
                        "dataset_id": "market.stock_universe_pit_spans",
                        "query_template_id": "historical_pit_universe_existing_readonly",
                        "parameter_template": {
                            "trade_date": "2026-06-02",
                            "universe_key": "shsz_st_pit_active_v1",
                        },
                    }
                )
                for requirement in base.requirements
            )
        }
    )


def _batch_b_source_plan() -> HistoricalRangeSourceRequirementPlanV1:
    base = _plan(6)
    templates = (
        (
            "historical_pit_universe_existing_readonly",
            {"trade_date": "2026-06-02", "universe_key": "shsz_st_pit_active_v1"},
        ),
        (
            "historical_trading_calendar_window",
            {"range_start": "2026-05-01", "trade_date": "2026-06-02"},
        ),
        (
            "historical_market_history_window",
            {"start_date": "2026-05-01", "trade_date": "2026-06-02", "universe_key": "shsz_st_pit_active_v1"},
        ),
        ("historical_decision_mark_daily_market", {"trade_date": "2026-06-02"}),
        ("historical_decision_mark_market_state", {"trade_date": "2026-06-02"}),
        (
            "historical_fundamental_moneyflow_window",
            {"start_date": "2026-05-01", "trade_date": "2026-06-02", "universe_key": "shsz_st_pit_active_v1"},
        ),
    )
    return base.model_copy(
        update={
            "requirements": tuple(
                requirement.model_copy(
                    update={
                        "source_role": f"source-{index}",
                        "query_template_id": query_id,
                        "parameter_template": parameters,
                    }
                )
                for index, (requirement, (query_id, parameters)) in enumerate(
                    zip(base.requirements, templates, strict=True),
                    start=1,
                )
            )
        }
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


def test_weight_balancing_preserves_original_ordinal_inside_worker_batch(monkeypatch) -> None:  # noqa: ANN001
    tracker = _ConnectionTracker()
    executor = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=1,
    )
    first, second = _plan(2).requirements
    first = first.model_copy(
        update={
            "query_template_id": "historical_market_history_window",
            "parameter_template": {"start_date": "2026-01-01", "trade_date": "2026-01-31"},
        }
    )
    second = second.model_copy(
        update={
            "query_template_id": "historical_market_history_window",
            "parameter_template": {"start_date": "2025-01-01", "trade_date": "2026-01-31"},
        }
    )
    base = _plan(2)
    plan = base.model_copy(update={"requirements": (first, second)})
    observed: list[list[str]] = []

    def resolve_batch(*, requirements, **_kwargs):  # noqa: ANN001, ANN202
        observed.append([item.requirement_id for item in requirements])
        return {item.requirement_id: _member(item) for item in requirements}

    monkeypatch.setattr(executor, "_resolve_requirement_batch", resolve_batch)
    executor.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolved_members={},
        chunk_size=2,
    )

    assert observed == [["requirement-001", "requirement-002"]]


def test_generic_requirement_failure_stops_same_transaction_batch(monkeypatch) -> None:  # noqa: ANN001
    tracker = _ConnectionTracker()
    executor = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=1,
    )
    requirements = _plan(3).requirements
    calls: list[str] = []
    failure = RuntimeError("database transaction aborted")

    def resolve(self, *, requirement, **_kwargs):  # noqa: ANN001, ANN202
        calls.append(requirement.requirement_id)
        if requirement.requirement_id == "requirement-002":
            raise failure
        return _member(requirement)

    monkeypatch.setattr(_PostgresRequirementResolver, "resolve", resolve)
    result = executor._resolve_requirement_batch(
        requirements=requirements,
        dependency_source={},
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        expected_members={},
        observed_at=OBSERVED_AT,
        snapshot_id="snapshot-shared",
    )

    assert calls == ["requirement-001", "requirement-002"]
    assert result["requirement-001"].requirement_id == "requirement-001"
    assert result["requirement-002"].message == str(failure)
    assert result["requirement-003"] == result["requirement-002"]


def test_parallel_prefetch_resolves_dependency_waves_before_dependents(monkeypatch) -> None:  # noqa: ANN001
    tracker = _ConnectionTracker()
    executor = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=tracker.connect,
        max_workers=2,
    )
    base = _plan(3)
    dependent = base.requirements[1].model_copy(
        update={"depends_on_requirement_ids": (base.requirements[0].requirement_id,)}
    )
    plan = base.model_copy(
        update={
            "requirements": (
                base.requirements[0],
                dependent,
                base.requirements[2],
            )
        }
    )
    observed: dict[str, tuple[str, ...]] = {}

    def resolve_batch(*, requirements, dependency_source, **_kwargs):  # noqa: ANN001, ANN202
        for requirement in requirements:
            observed[requirement.requirement_id] = tuple(sorted(dependency_source))
        return {item.requirement_id: _member(item) for item in requirements}

    monkeypatch.setattr(executor, "_resolve_requirement_batch", resolve_batch)
    result = executor.resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolved_members={},
        chunk_size=3,
    )

    assert observed == {
        "requirement-001": (),
        "requirement-002": ("requirement-001",),
        "requirement-003": (),
    }
    assert [delta.ordinal for delta in result.checkpoint.member_delta] == [1, 2, 3]


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

    def close(self) -> None:
        self.closed = True

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


class _FailingStreamingCursor(_StreamingCursor):
    def execute(self, _sql, _params) -> None:  # noqa: ANN001
        raise RuntimeError("stream execute failed")


class _FailingStreamingConnection(_StreamingConnection):
    def __init__(self) -> None:
        self.cursor_instance = _FailingStreamingCursor(0)
        self.cursor_names = []


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


def test_stream_cursor_is_closed_when_execute_fails() -> None:
    conn = _FailingStreamingConnection()
    resolver = _PostgresRequirementResolver(
        cur=object(),
        conn=conn,
        observed_at=OBSERVED_AT,
    )

    with pytest.raises(RuntimeError, match="stream execute failed"):
        resolver._stream_query("SELECT payload", ())

    assert conn.cursor_instance.closed is True


@pytest.mark.parametrize(
    "payload",
    [
        {"plain": 12.34, "count": 7, "nullable": None, "label": "technology"},
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


def test_process_payloads_and_results_are_picklable_with_exact_framed_bytes() -> None:
    payloads = tuple(
        json.dumps(
            {"trade_date": "2026-06-02", "ts_code": f"{index:06d}.SZ", "close_li": 12.34},
            separators=(",", ":"),
        )
        for index in range(512)
    )
    expected, expected_count, expected_missing = _canonicalize_payload_batch(payloads, ())
    actual, actual_count, actual_missing = _canonicalize_payload_batch(
        pickle.loads(pickle.dumps(payloads)),
        (),
    )
    failure = _RequirementResolutionFailure(
        category="worker_error",
        message="test",
        exception_type="builtins.RuntimeError",
        traceback_text="trace",
    )

    assert actual == expected
    assert actual_count == expected_count == 512
    assert actual_missing is expected_missing is False
    assert pickle.loads(pickle.dumps(failure)) == failure


def test_process_worker_closes_connection_even_when_rollback_fails(monkeypatch) -> None:  # noqa: ANN001
    class FailingConnection:
        def __init__(self) -> None:
            self.closed = False

        def set_session(self, **_kwargs) -> None:  # noqa: ANN003
            raise RuntimeError("worker transaction failed")

        def rollback(self) -> None:
            raise RuntimeError("worker rollback failed")

        def close(self) -> None:
            self.closed = True

    conn = FailingConnection()
    monkeypatch.setattr(psycopg2, "connect", lambda _dsn: conn)

    with pytest.raises(RuntimeError, match="worker transaction failed") as error:
        _resolve_requirement_batch_process(
            "dbname=test",
            _plan(1).requirements,
            {},
            HistoricalRangeCatalogPhase.DISCOVER,
            {},
            OBSERVED_AT,
            "snapshot-shared",
            1000,
        )

    assert conn.closed is True
    assert any("worker rollback failed" in note for note in error.value.__notes__)


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


def test_process_worker_selection_only_requires_database_backed_sources() -> None:
    frozen, hmm, market = _plan(3).requirements
    hmm = hmm.model_copy(update={"query_template_id": "historical_hmm_frozen_evidence_bundle"})
    market = market.model_copy(update={"query_template_id": "historical_market_history_window"})

    assert _requirement_uses_process_worker(frozen) is False
    assert _requirement_uses_process_worker(hmm) is False
    assert _requirement_uses_process_worker(market) is True


@pytest.mark.parametrize("max_workers", [0, 25])
def test_catalog_executor_rejects_unbounded_worker_counts(max_workers: int) -> None:
    with pytest.raises(ValueError, match="max_workers"):
        PostgresHistoricalRangeCatalogExecutor(
            conn_factory=lambda: None,
            max_workers=max_workers,
        )


def test_explicit_connection_factory_carries_same_env_worker_dsn(monkeypatch) -> None:  # noqa: ANN001
    values = {
        "TDX_DB_HOST": "127.0.0.1",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock-test",
        "TDX_DB_USER": "worker-test",
        "TDX_DB_PASSWORD": "secret-test",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    factory = explicit_historical_range_connection_factory()
    parsed = psycopg2.extensions.parse_dsn(
        getattr(factory, "_aistock_process_worker_dsn")
    )

    assert parsed["host"] == values["TDX_DB_HOST"]
    assert parsed["port"] == values["TDX_DB_PORT"]
    assert parsed["dbname"] == values["TDX_DB_NAME"]
    assert parsed["user"] == values["TDX_DB_USER"]
    assert parsed["password"] == values["TDX_DB_PASSWORD"]


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


def _write_complete_source_cache(
    *,
    root,
    plan: HistoricalRangeSourceRequirementPlanV1,
) -> CatalogSourceFileCache:
    cache = CatalogSourceFileCache(
        root=root,
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
    )
    with cache._connect(cache.path) as conn:
        cache._create_schema(conn)
        conn.execute(
            "INSERT INTO pit VALUES(?,?,?,?)",
            ("shsz_st_pit_active_v1", "000001.SZ", "2020-01-01", "2099-12-31"),
        )
        conn.execute(
            "INSERT INTO cache_manifest(singleton,payload_json) VALUES(1,?)",
            (
                json.dumps(
                    {
                        "schema_version": "advisory_historical_range_catalog_source_cache_v1",
                        "planning_identity_hash": plan.planning_identity_hash,
                        "requirement_plan_hash": plan.requirement_plan_hash,
                        "catalog_generation": 1,
                        "phase": HistoricalRangeCatalogPhase.DISCOVER.value,
                        "observed_at": OBSERVED_AT.isoformat(),
                        "status": "COMPLETE",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        conn.commit()
    return cache


def test_complete_source_cache_resolves_without_database_or_process_pool(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    plan = _cacheable_plan(2)
    _write_complete_source_cache(root=tmp_path, plan=plan)

    def forbidden_connection():  # noqa: ANN202
        raise AssertionError("completed cache must not reconnect to PostgreSQL")

    def forbidden_process_pool(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("file-cache catalog mode must not spawn a process pool")

    monkeypatch.setattr(catalog_postgres_module, "ProcessPoolExecutor", forbidden_process_pool)
    result = PostgresHistoricalRangeCatalogExecutor(
        conn_factory=forbidden_connection,
        process_worker_dsn="dbname=must-not-be-used",
        source_cache_root=tmp_path,
    ).resolve_chunk(
        plan=plan,
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        start_ordinal=1,
        resolved_members={},
        chunk_size=2,
    )

    assert [item.member.row_count for item in result.checkpoint.member_delta] == [1, 1]
    assert not any(
        thread.name.startswith("advisory-catalog-cache")
        for thread in threading.enumerate()
    )


def test_source_cache_mode_rejects_formal_and_hmm_special_contracts() -> None:
    plan = _cacheable_plan(1)
    requirement = plan.requirements[0]
    formal = requirement.model_copy(
        update={
            "parameter_template": {
                **requirement.parameter_template,
                "formal_partition_key": {"trade_date": "2026-06-02"},
            }
        }
    )
    hmm = requirement.model_copy(
        update={"query_template_id": "historical_hmm_frozen_evidence_bundle"}
    )

    assert _plan_uses_source_cache(plan) is True
    assert _plan_uses_source_cache(_plan(1)) is False
    assert _plan_uses_source_cache(plan.model_copy(update={"requirements": (formal,)})) is False
    assert _plan_uses_source_cache(plan.model_copy(update={"requirements": (hmm,)})) is False


class _EmptyBulkCursor:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements
        self.itersize = 0

    def execute(self, sql, _params) -> None:  # noqa: ANN001
        self._statements.append(" ".join(str(sql).split()))

    def fetchmany(self, _size: int) -> list[dict]:
        return []

    def close(self) -> None:
        return None


class _EmptyBulkConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self, *, name: str, cursor_factory):  # noqa: ANN001, ANN201
        assert name.startswith("ahr_bulk_cache_")
        assert cursor_factory is not None
        return _EmptyBulkCursor(self.statements)


def test_bulk_extract_statement_count_is_independent_of_requirement_count(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(psycopg2.extras, "register_default_jsonb", lambda *_args, **_kwargs: None)
    statement_counts: list[int] = []
    for count in (1, 512):
        root = tmp_path / str(count)
        root.mkdir()
        source = _EmptyBulkConnection()
        CatalogSourceFileCache(
            root=root,
            plan=_cacheable_plan(count),
            catalog_generation=1,
            phase=HistoricalRangeCatalogPhase.DISCOVER,
        ).ensure(conn=source, observed_at=OBSERVED_AT)
        statement_counts.append(len(source.statements))

    assert statement_counts == [1, 1]

    target_root = tmp_path / "batch-b"
    target_root.mkdir()
    target_source = _EmptyBulkConnection()
    CatalogSourceFileCache(
        root=target_root,
        plan=_batch_b_source_plan(),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
    ).ensure(conn=target_source, observed_at=OBSERVED_AT)
    assert len(target_source.statements) == 10


def test_corrupt_source_cache_fails_loudly_without_database_fallback(tmp_path) -> None:  # noqa: ANN001
    cache = CatalogSourceFileCache(
        root=tmp_path,
        plan=_cacheable_plan(1),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
    )
    cache.path.write_bytes(b"not-a-sqlite-database")

    with pytest.raises(CatalogSourceCacheError, match="readback failed"):
        cache.ready_observed_at()


def test_concurrent_duplicate_cache_requests_compute_content_once(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cache = _write_complete_source_cache(root=tmp_path, plan=_cacheable_plan(1))
    original = CatalogSourceFileCache._retrospective_content_on_connection
    calls = 0
    calls_lock = threading.Lock()

    def tracked(**kwargs):  # noqa: ANN003, ANN202
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.02)
        return original(**kwargs)

    monkeypatch.setattr(
        CatalogSourceFileCache,
        "_retrospective_content_on_connection",
        staticmethod(tracked),
    )
    parameters = {
        "trade_date": "2026-06-02",
        "universe_key": "shsz_st_pit_active_v1",
    }
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                lambda _index: cache.retrospective_content(
                    "historical_pit_universe_existing_readonly",
                    parameters,
                ),
                range(8),
            )
        )

    assert calls == 1
    assert len(set(results)) == 1


def test_file_cache_hashes_match_existing_catalog_contract_for_batch_b_sources(tmp_path) -> None:  # noqa: ANN001
    cache = CatalogSourceFileCache(
        root=tmp_path,
        plan=_cacheable_plan(1),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
    )
    history_payloads = (
        canonical_payload_bytes(
            {
                "trade_date": "2026-06-01",
                "ts_code": "000001.SZ",
                "open_li": 10.0,
                "high_li": 11.0,
                "low_li": 9.5,
                "close_li": 10.5,
                "volume_hand": 1000.0,
                "amount_li": 10500.0,
                "adj_factor": 1.2,
            }
        ),
        canonical_payload_bytes(
            {
                "trade_date": "2026-06-02",
                "ts_code": "000001.SZ",
                "open_li": 10.5,
                "high_li": 11.5,
                "low_li": 10.0,
                "close_li": 11.0,
                "volume_hand": 1200.0,
                "amount_li": 13200.0,
                "adj_factor": 1.2,
            }
        ),
    )
    daily_payload = canonical_payload_bytes(
        {
            "trade_date": "2026-06-02",
            "ts_code": "000001.SZ",
            "close_li": 11.0,
            "adj_factor": 1.2,
        }
    )
    calendar_payloads = (
        canonical_payload_bytes({"cal_date": "2026-06-01", "is_trading": True}),
        canonical_payload_bytes({"cal_date": "2026-06-02", "is_trading": True}),
    )
    fundamental_payloads = {
        dataset: canonical_payload_bytes(
            {"trade_date": "2026-06-02", "ts_code": "000001.SZ", "value": index}
        )
        for index, dataset in enumerate(
            ("daily_basic", "moneyflow_ts", "bak_basic", "cyq_perf", "sector_data"),
            start=1,
        )
    }
    with cache._connect(cache.path) as conn:
        cache._create_schema(conn)
        conn.execute(
            "INSERT INTO pit VALUES(?,?,?,?)",
            ("shsz_st_pit_active_v1", "000001.SZ", "2020-01-01", "2099-12-31"),
        )
        conn.executemany(
            "INSERT INTO calendar VALUES(?,?)",
            ((day, payload) for day, payload in zip(("2026-06-01", "2026-06-02"), calendar_payloads, strict=True)),
        )
        conn.executemany(
            "INSERT INTO market VALUES(?,?,?,?,?,?)",
            (
                ("2026-06-01", "000001.SZ", 1, history_payloads[0], canonical_payload_bytes({"trade_date": "2026-06-01", "ts_code": "000001.SZ", "close_li": 10.5, "adj_factor": 1.2}), 0),
                ("2026-06-02", "000001.SZ", 1, history_payloads[1], daily_payload, 0),
            ),
        )
        conn.execute("INSERT INTO stock_basic VALUES(?,?,?,?)", ("000001.SZ", "1991-04-03", None, "L"))
        conn.executemany(
            "INSERT INTO fundamental VALUES(?,?,?,?)",
            ((dataset, "2026-06-02", "000001.SZ", payload) for dataset, payload in fundamental_payloads.items()),
        )
        conn.commit()

    common = {
        "universe_key": "shsz_st_pit_active_v1",
        "start_date": "2026-06-01",
        "trade_date": "2026-06-02",
    }
    pit = cache.retrospective_content("historical_pit_universe_existing_readonly", common)
    calendar = cache.retrospective_content(
        "historical_trading_calendar_window",
        {"range_start": "2026-06-01", "trade_date": "2026-06-02"},
    )
    market = cache.retrospective_content("historical_market_history_window", common)
    daily = cache.retrospective_content("historical_decision_mark_daily_market", common)
    state = cache.retrospective_content("historical_decision_mark_market_state", common)
    fundamental = cache.retrospective_content("historical_fundamental_moneyflow_window", common)

    assert pit[:2] == (1, canonical_json_sha256(["000001.SZ"]))
    assert calendar[:2] == frame_payloads(calendar_payloads)
    assert market[:2] == frame_payloads(history_payloads)
    assert daily[:2] == frame_payloads((daily_payload,))
    expected_state = canonical_payload_bytes(
        {
            "ts_code": "000001.SZ",
            "list_date": "1991-04-03",
            "delist_date": None,
            "list_status": "L",
            "suspended": False,
            "pit_eligible": True,
        }
    )
    assert state[:2] == frame_payloads((expected_state,))
    composite = hashlib.sha256()
    for dataset, payload in fundamental_payloads.items():
        _, content_hash = frame_payloads((payload,))
        marker = canonical_payload_bytes(
            {"dataset_name": dataset, "content_hash": content_hash, "row_count": 1}
        )
        composite.update(len(marker).to_bytes(8, "big"))
        composite.update(marker)
    assert fundamental[:2] == (5, composite.hexdigest())


def test_file_cache_market_window_hashing_keeps_memory_bounded(tmp_path) -> None:  # noqa: ANN001
    cache = CatalogSourceFileCache(
        root=tmp_path,
        plan=_cacheable_plan(1),
        catalog_generation=1,
        phase=HistoricalRangeCatalogPhase.DISCOVER,
    )
    row_count = 20_000
    with cache._connect(cache.path) as conn:
        cache._create_schema(conn)
        conn.executemany(
            "INSERT INTO pit VALUES(?,?,?,?)",
            (
                ("shsz_st_pit_active_v1", f"{index:06d}.SZ", "2020-01-01", "2099-12-31")
                for index in range(row_count)
            ),
        )
        conn.executemany(
            "INSERT INTO market VALUES(?,?,?,?,?,?)",
            (
                (
                    "2026-06-02",
                    f"{index:06d}.SZ",
                    1,
                    canonical_payload_bytes(
                        {"trade_date": "2026-06-02", "ts_code": f"{index:06d}.SZ", "adj_factor": 1.0}
                    ),
                    canonical_payload_bytes(
                        {"trade_date": "2026-06-02", "ts_code": f"{index:06d}.SZ", "close_li": 1.0, "adj_factor": 1.0}
                    ),
                    0,
                )
                for index in range(row_count)
            ),
        )
        conn.commit()

    tracemalloc.start()
    resolved = cache.retrospective_content(
        "historical_market_history_window",
        {
            "universe_key": "shsz_st_pit_active_v1",
            "start_date": "2026-06-02",
            "trade_date": "2026-06-02",
        },
    )
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert resolved[0] == row_count
    assert peak < 32 * 1024 * 1024
