from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore

from backend.services.advisory_historical_range.models import (
    REASON_DAY_PLAN_CONFLICT,
    REASON_IDEMPOTENCY_CONFLICT,
    HistoricalRangeArtifactBindingsV1,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
)
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangeRepository,
)
from backend.tests.advisory_historical_range.conftest import date_plan, digest
from backend.tests.advisory_historical_range.conftest import research_spec, resolved_request


class _FakeCursor:
    def __init__(self, *, run: dict[str, Any]) -> None:
        self.run = run
        self.days: dict[str, dict[str, Any]] = {}
        self.commands: list[str] = []
        self._one: dict[str, Any] | None = None
        self.rowcount = 0

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        normalized = " ".join(query.split())
        self.commands.append(normalized)
        params = params or ()
        self.rowcount = 0
        self._one = None
        if normalized.startswith("SELECT r.*, b.trade_date_count"):
            self._one = dict(self.run)
        elif normalized.startswith("INSERT INTO app.advisory_historical_range_day_run"):
            day_run_id, range_run_id, trade_date, ordinal, previous_id = params
            if day_run_id not in self.days:
                self.days[day_run_id] = {
                    "day_run_id": day_run_id,
                    "range_run_id": range_run_id,
                    "decision_trade_date": trade_date,
                    "ordinal": ordinal,
                    "previous_day_run_id": previous_id,
                }
                self.rowcount = 1
        elif normalized.startswith("SELECT day_run_id, range_run_id"):
            self._one = dict(self.days.get(str(params[0]), {})) or None
        elif normalized.startswith("UPDATE app.advisory_historical_range_run"):
            next_count, next_cursor, range_run_id, expected_version = params
            if range_run_id == self.run["range_run_id"] and expected_version == self.run["row_version"]:
                self.run["materialized_day_count"] = next_count
                self.run["day_plan_cursor_ordinal"] = next_cursor
                self.run["row_version"] += 1
                self.rowcount = 1
        else:
            raise AssertionError(f"unexpected SQL in fake repository test: {normalized}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, **_kwargs: Any) -> _FakeCursor:
        return self._cursor


class _CreateDatabase:
    def __init__(self) -> None:
        self.batches: dict[str, dict[str, Any]] = {}
        self.aliases: dict[str, dict[str, Any]] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.operations: dict[str, dict[str, Any]] = {}

    def connection(self) -> "_CreateConnection":
        return _CreateConnection(self)


class _CreateConnection:
    def __init__(self, database: _CreateDatabase) -> None:
        self.database = database

    def __enter__(self) -> "_CreateConnection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def cursor(self, **_kwargs: Any) -> "_CreateCursor":
        return _CreateCursor(self.database)


class _CreateCursor:
    def __init__(self, database: _CreateDatabase) -> None:
        self.database = database
        self._one: dict[str, Any] | None = None
        self._all: list[dict[str, Any]] = []
        self.rowcount = 0

    def __enter__(self) -> "_CreateCursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        sql = " ".join(query.split())
        params = params or ()
        self._one = None
        self._all = []
        self.rowcount = 0
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            return
        if sql.startswith("SELECT batch.* FROM app.advisory_historical_range_request_key"):
            alias = self.database.aliases.get(str(params[0]))
            self._one = dict(self.database.batches[alias["batch_id"]]) if alias is not None else None
            return
        if sql.startswith("SELECT * FROM app.advisory_historical_range_batch WHERE request_payload_sha256"):
            self._one = next(
                (
                    dict(batch)
                    for batch in self.database.batches.values()
                    if batch["request_payload_sha256"] == params[0]
                ),
                None,
            )
            return
        if sql.startswith("SELECT previous.batch_id"):
            self._one = None
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_batch"):
            values = list(params)
            batch = {
                "batch_id": values[0],
                "request_id": values[1],
                "client_idempotency_key": values[2],
                "user_request_semantic_hash": values[3],
                "request_payload_sha256": values[4],
                "supersedes_batch_id": values[5],
                "start_trade_date": values[6],
                "end_trade_date": values[7],
                "calendar_id": values[8],
                "calendar_version": values[9],
                "ordered_trade_dates_hash": values[10],
                "date_plan_ref": values[11].adapted,
                "date_plan_hash": values[12],
                "source_revision_catalog_hash": values[13],
                "selection_semantics_version": values[14],
                "selection_semantics_hash": values[15],
                "list_semantics_version": values[16],
                "list_semantics_hash": values[17],
                "per_program_input_warmup_ranges_hash": values[18],
                "program_count": values[19],
                "trade_date_count": values[20],
                "planned_day_count": values[21],
                "artifact_root_identity_hash": values[22],
                "row_version": 1,
            }
            self.database.batches[str(batch["batch_id"])] = batch
            self.rowcount = 1
            self._one = {"batch_id": batch["batch_id"]}
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_request_key"):
            key = str(params[0])
            if key not in self.database.aliases:
                self.database.aliases[key] = {
                    "client_idempotency_key": key,
                    "batch_id": params[1],
                    "request_id": params[2],
                    "request_payload_sha256": params[3],
                    "request_artifact_ref": params[4].adapted,
                    "request_artifact_hash": params[5],
                }
                self.rowcount = 1
            return
        if sql.startswith("SELECT batch_id, request_payload_sha256, request_artifact_ref, request_artifact_hash"):
            alias = self.database.aliases.get(str(params[0]))
            self._one = dict(alias) if alias is not None else None
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_run"):
            self.database.runs[str(params[0])] = {
                "range_run_id": params[0],
                "batch_id": params[1],
                "research_program_id": params[2],
            }
            self.rowcount = 1
            return
        if sql.startswith("SELECT COUNT(*) FILTER (WHERE day.status IN ('COMPLETE', 'VALID_NO_CANDIDATE'))"):
            self._one = {
                "successful_day_count": 0,
                "terminal_failed_day_count": 0,
                "completed_program_count": 0,
                "failed_program_count": 0,
                "waiting_program_count": 0,
                "retryable_program_count": 0,
                "partial_program_count": 0,
                "recoverable_program_count": len(self.database.runs),
            }
            return
        if sql.startswith("UPDATE app.advisory_historical_range_batch SET successful_day_count"):
            batch = self.database.batches[str(params[8])]
            keys = (
                "successful_day_count",
                "terminal_failed_day_count",
                "completed_program_count",
                "failed_program_count",
                "waiting_program_count",
                "retryable_program_count",
                "partial_program_count",
                "recoverable_program_count",
            )
            changed = any(batch.get(key, 0) != params[index] for index, key in enumerate(keys))
            if changed:
                for index, key in enumerate(keys):
                    batch[key] = params[index]
                batch["row_version"] += 1
                self.rowcount = 1
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_operation"):
            self.database.operations[str(params[0])] = {
                "operation_id": params[0],
                "batch_id": params[1],
                "operation_type": params[2],
                "operation_idempotency_key": params[3],
                "request_payload_sha256": params[4],
                "expected_row_version": params[5],
            }
            self.rowcount = 1
            return
        if sql.startswith("SELECT range_run_id FROM app.advisory_historical_range_run"):
            self._all = sorted(
                (
                    {"range_run_id": run["range_run_id"], "research_program_id": run["research_program_id"]}
                    for run in self.database.runs.values()
                    if run["batch_id"] == params[0]
                ),
                key=lambda row: row["research_program_id"],
            )
            return
        if sql.startswith("SELECT operation_id FROM app.advisory_historical_range_operation"):
            self._one = next(
                (
                    {"operation_id": operation["operation_id"]}
                    for operation in self.database.operations.values()
                    if operation["batch_id"] == params[0] and operation["operation_type"] == "CREATE"
                ),
                None,
            )
            return
        raise AssertionError(f"unexpected SQL in create repository test: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._all


def _artifact_bindings(
    resolved: Any,
    store: HistoricalRangeArtifactStore,
) -> HistoricalRangeArtifactBindingsV1:
    request_payload = resolved.model_dump(mode="json")
    date_payload = resolved.date_plan.model_dump(mode="json")
    request = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="phase1r_r1",
        payload_schema_version=str(request_payload["schema_version"]),
        resolved_request_hash=resolved.request_payload_sha256,
        payload=request_payload,
    )
    date_plan_artifact = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        producer_contract_version="phase1r_r1",
        payload_schema_version=str(date_payload["schema_version"]),
        resolved_request_hash=resolved.request_payload_sha256,
        payload=date_payload,
        upstream_refs=(request.ref,),
    )
    frozen_refs = {
        program.research_program_id: store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.FROZEN_PROGRAM,
            producer_contract_version="phase1r_r1",
            payload_schema_version=program.schema_version,
            resolved_request_hash=resolved.request_payload_sha256,
            range_run_id=resolved.range_run_id(program.research_program_id),
            payload=program.model_dump(mode="json"),
            upstream_refs=(request.ref, date_plan_artifact.ref),
        ).ref
        for program in resolved.frozen_programs
    }
    return HistoricalRangeArtifactBindingsV1(
        request_ref=request.ref,
        date_plan_ref=date_plan_artifact.ref,
        frozen_program_refs=frozen_refs,
        artifact_root_identity_hash=store.root_identity_hash,
    )


def _date_plan_ref(
    *,
    store: HistoricalRangeArtifactStore,
    resolved_request_hash: str,
    plan: Any,
) -> HistoricalRangeArtifactRefV1:
    return store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        producer_contract_version="phase1r_r1",
        payload_schema_version="advisory_historical_range_date_plan_v1",
        resolved_request_hash=resolved_request_hash,
        payload=plan.model_dump(mode="json"),
    ).ref


def test_repository_requires_explicit_connection_factory(tmp_path: Path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    with pytest.raises(ValueError):
        PostgresHistoricalRangeRepository(conn_factory=None, artifact_store=store)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PostgresHistoricalRangeRepository(conn_factory=lambda: None, artifact_store=None)  # type: ignore[arg-type]


def test_create_batch_converges_multiple_program_request_and_binds_every_retry_key(tmp_path: Path) -> None:
    specs = (
        research_spec(name="short rebound", package_id="pkg_short"),
        research_spec(name="long trend", package_id="pkg_long"),
    )
    first_request = resolved_request(specs=specs, client_key="create-key-1")
    same_semantics = resolved_request(
        specs=specs,
        client_key="create-key-2",
        request_id="request-2",
    )
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)

    first = repository.create_batch(
        resolved=first_request,
        artifacts=_artifact_bindings(first_request, store),
    )
    second = repository.create_batch(
        resolved=same_semantics,
        artifacts=_artifact_bindings(same_semantics, store),
    )

    assert first.idempotent is False
    assert second.idempotent is True
    assert first.batch_id == second.batch_id
    assert len(first.range_run_ids) == 2
    assert set(database.aliases) == {"create-key-1", "create-key-2"}
    assert {run["batch_id"] for run in database.runs.values()} == {first.batch_id}


def test_bound_idempotency_key_rejects_different_resolved_semantics(tmp_path: Path) -> None:
    initial = resolved_request(client_key="fixed-key")
    changed = resolved_request(
        specs=(research_spec(target_count=9),),
        client_key="fixed-key",
        request_id="changed-request",
    )
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)
    repository.create_batch(resolved=initial, artifacts=_artifact_bindings(initial, store))

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        repository.create_batch(resolved=changed, artifacts=_artifact_bindings(changed, store))
    assert exc_info.value.reason_code == REASON_IDEMPOTENCY_CONFLICT


def test_create_batch_rejects_missing_exact_artifact_before_database_write(tmp_path: Path) -> None:
    resolved = resolved_request(client_key="missing-artifact")
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    artifacts = _artifact_bindings(resolved, store)
    (store.root / artifacts.request_ref.relative_path).unlink()
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)

    with pytest.raises(HistoricalRangeContractError):
        repository.create_batch(resolved=resolved, artifacts=artifacts)
    assert database.batches == {}


def test_1200_day_plan_materializes_in_stable_500_row_chunks_without_calendar_query(tmp_path: Path) -> None:
    start = date(2020, 1, 1)
    dates = tuple(start + timedelta(days=index) for index in range(1200))
    plan = date_plan(trade_dates=dates)
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    resolved_hash = digest("resolved-request")
    ref = _date_plan_ref(store=store, resolved_request_hash=resolved_hash, plan=plan)
    run = {
        "range_run_id": "range-1",
        "day_plan_ref": ref.model_dump(mode="json"),
        "day_plan_hash": ref.semantic_content_hash,
        "request_payload_sha256": resolved_hash,
        "trade_date_count": 1200,
        "day_plan_cursor_ordinal": 0,
        "materialized_day_count": 0,
        "row_version": 1,
    }
    cursor = _FakeCursor(run=run)
    repository = PostgresHistoricalRangeRepository(conn_factory=lambda: _FakeConnection(cursor), artifact_store=store)

    first = repository.materialize_day_plan_chunk(
        range_run_id="range-1",
        date_plan=plan,
        date_plan_ref=ref,
        expected_cursor_ordinal=0,
    )
    second = repository.materialize_day_plan_chunk(
        range_run_id="range-1",
        date_plan=plan,
        date_plan_ref=ref,
        expected_cursor_ordinal=500,
    )
    third = repository.materialize_day_plan_chunk(
        range_run_id="range-1",
        date_plan=plan,
        date_plan_ref=ref,
        expected_cursor_ordinal=1000,
    )

    assert (len(first.entries), len(second.entries), len(third.entries)) == (500, 500, 200)
    assert third.exhausted is True
    assert run["day_plan_cursor_ordinal"] == 1200
    assert len(cursor.days) == 1200
    assert not any("trading_calendar" in command for command in cursor.commands)


def test_day_plan_exact_retry_rejects_same_identity_with_different_date(tmp_path: Path) -> None:
    plan = date_plan()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    resolved_hash = digest("resolved-request")
    ref = _date_plan_ref(store=store, resolved_request_hash=resolved_hash, plan=plan)
    run = {
        "range_run_id": "range-1",
        "day_plan_ref": ref.model_dump(mode="json"),
        "day_plan_hash": ref.semantic_content_hash,
        "request_payload_sha256": resolved_hash,
        "trade_date_count": 3,
        "day_plan_cursor_ordinal": 0,
        "materialized_day_count": 0,
        "row_version": 1,
    }
    cursor = _FakeCursor(run=run)
    repository = PostgresHistoricalRangeRepository(conn_factory=lambda: _FakeConnection(cursor), artifact_store=store)
    expected_id = (
        "ahrd_"
        + digest(
            {
                "range_run_id": "range-1",
                "decision_trade_date": plan.ordered_trade_dates[0],
                "ordinal": 1,
            }
        )[:32]
    )
    cursor.days[expected_id] = {
        "day_run_id": expected_id,
        "range_run_id": "range-1",
        "decision_trade_date": date(2025, 1, 1),
        "ordinal": 1,
        "previous_day_run_id": None,
    }

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        repository.materialize_day_plan_chunk(
            range_run_id="range-1",
            date_plan=plan,
            date_plan_ref=ref,
            expected_cursor_ordinal=0,
            chunk_size=1,
        )
    assert exc_info.value.reason_code == REASON_DAY_PLAN_CONFLICT


def test_day_plan_rejects_ref_whose_payload_hash_is_not_the_supplied_plan(tmp_path: Path) -> None:
    plan = date_plan()
    ref = HistoricalRangeArtifactRefV1(
        artifact_kind=HistoricalRangeArtifactKind.DATE_PLAN,
        relative_path=f"date-plans/{digest('plan-envelope')}.json",
        producer_contract_version="phase1r_r1",
        payload_schema_version=plan.schema_version,
        semantic_content_hash=digest("plan-envelope"),
        payload_sha256=digest("different-plan-payload"),
        file_sha256=digest("plan-file"),
    )
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(
        conn_factory=lambda: _FakeConnection(
            _FakeCursor(
                run={
                    "range_run_id": "range-1",
                    "day_plan_ref": ref.model_dump(mode="json"),
                    "day_plan_hash": ref.semantic_content_hash,
                    "trade_date_count": 3,
                    "day_plan_cursor_ordinal": 0,
                    "materialized_day_count": 0,
                    "row_version": 1,
                }
            )
        ),
        artifact_store=store,
    )

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        repository.materialize_day_plan_chunk(
            range_run_id="range-1",
            date_plan=plan,
            date_plan_ref=ref,
            expected_cursor_ordinal=0,
        )
    assert exc_info.value.reason_code == REASON_DAY_PLAN_CONFLICT


def test_running_lease_heartbeat_and_expired_takeover_are_distinct() -> None:
    now = datetime.now(UTC)
    running = {
        "status": "RUNNING",
        "attempt_no": 2,
        "current_fencing_token": 7,
        "lease_expires_at": now + timedelta(minutes=2),
    }
    PostgresHistoricalRangeRepository._require_running_lease_update(
        current=running,
        attempt_no=2,
        fencing_token=7,
        lease_expires_at=now + timedelta(minutes=3),
        entity="day",
    )
    with pytest.raises(HistoricalRangeContractError):
        PostgresHistoricalRangeRepository._require_running_lease_update(
            current=running,
            attempt_no=3,
            fencing_token=8,
            lease_expires_at=now + timedelta(minutes=4),
            entity="day",
        )

    expired = dict(running)
    expired["lease_expires_at"] = now - timedelta(seconds=1)
    PostgresHistoricalRangeRepository._require_running_lease_update(
        current=expired,
        attempt_no=3,
        fencing_token=8,
        lease_expires_at=now + timedelta(minutes=2),
        entity="day",
    )
