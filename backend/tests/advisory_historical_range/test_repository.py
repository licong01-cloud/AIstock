from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore

from backend.services.advisory_historical_range.models import (
    REASON_DAY_PLAN_CONFLICT,
    REASON_IDEMPOTENCY_CONFLICT,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
    HistoricalRangePlanningArtifactBindingsV1,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSourceRequirementV1,
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
        self.operation_attempts: dict[tuple[str, int], dict[str, Any]] = {}

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
        if sql.startswith("INSERT INTO app.advisory_historical_range_batch"):
            values = list(params)
            batch = {
                "batch_id": values[0],
                "request_id": values[1],
                "client_idempotency_key": values[2],
                "user_request_semantic_hash": values[3],
                "planning_identity_hash": values[4],
                "requirement_plan_ref": values[5].adapted,
                "requirement_plan_hash": values[6],
                "requirement_plan_artifact_hash": values[7],
                "start_trade_date": values[8],
                "end_trade_date": values[9],
                "calendar_id": values[10],
                "calendar_version": values[11],
                "ordered_trade_dates_hash": values[12],
                "selection_semantics_version": values[13],
                "selection_semantics_hash": values[14],
                "list_semantics_version": values[15],
                "list_semantics_hash": values[16],
                "per_program_input_warmup_ranges_hash": values[17],
                "program_count": values[18],
                "trade_date_count": values[19],
                "planned_day_count": values[20],
                "artifact_root_identity_hash": values[22],
                "status": "PLANNING",
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
                    "user_request_semantic_hash": params[3],
                    "planning_identity_hash": params[4],
                    "requirement_plan_ref": params[5].adapted,
                    "requirement_plan_hash": params[6],
                    "requirement_plan_artifact_hash": params[7],
                }
                self.rowcount = 1
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_operation ("):
            self.database.operations[str(params[0])] = {
                "operation_id": params[0],
                "batch_id": params[1],
                "operation_type": params[2],
                "operation_idempotency_key": params[3],
                "request_payload_sha256": params[4],
                "planning_identity_hash": params[5],
                "expected_row_version": params[6],
            }
            self.rowcount = 1
            return
        if sql.startswith("UPDATE app.advisory_historical_range_operation SET status = 'RUNNING'"):
            operation = self.database.operations[str(params[4])]
            operation.update(
                {
                    "status": "RUNNING",
                    "row_version": 2,
                    "attempt_no": 1,
                    "worker_id": params[0],
                    "lease_token": params[1],
                    "lease_expires_at": params[2],
                    "fencing_token": 1,
                    "started_at": params[3],
                }
            )
            self._one = dict(operation)
            self.rowcount = 1
            return
        if sql.startswith("INSERT INTO app.advisory_historical_range_operation_attempt"):
            row = {
                "attempt_id": params[0],
                "operation_id": params[1],
                "attempt_no": params[2],
                "worker_id": params[3],
                "lease_token": params[4],
                "fencing_token": params[5],
                "status": params[6],
                "input_cursor_json": params[7].adapted if params[7] is not None else None,
                "result_cursor_json": params[8].adapted if params[8] is not None else None,
                "input_hash": params[9],
                "result_hash": params[10],
                "attempt_receipt_ref": params[11].adapted if params[11] is not None else None,
                "attempt_receipt_hash": params[12],
                "reason_codes_json": params[13].adapted,
                "error_json": params[14].adapted if params[14] is not None else None,
                "started_at": params[15],
                "finished_at": params[16],
            }
            self.database.operation_attempts[(str(params[1]), int(params[2]))] = row
            self.rowcount = 1
            return
        if sql.startswith("SELECT attempt_id, operation_id, attempt_no"):
            row = self.database.operation_attempts.get((str(params[0]), int(params[1])))
            self._one = dict(row) if row is not None else None
            return
        if sql.startswith("UPDATE app.advisory_historical_range_operation SET status = 'COMPLETED'"):
            operation = self.database.operations[str(params[3])]
            operation.update(
                {
                    "status": "COMPLETED",
                    "row_version": 3,
                    "result_status": "PLANNING_CREATED",
                    "result_ref": params[0].adapted,
                    "result_hash": params[1],
                    "finished_at": params[2],
                }
            )
            self._one = {"operation_id": operation["operation_id"]}
            self.rowcount = 1
            return
        if sql.startswith("SELECT operation_id, operation_type FROM app.advisory_historical_range_operation"):
            self._all = [
                {"operation_id": operation["operation_id"], "operation_type": operation["operation_type"]}
                for operation in self.database.operations.values()
                if operation["batch_id"] == params[0]
            ]
            return
        raise AssertionError(f"unexpected SQL in create repository test: {sql}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._all


def _planning_request(
    *, specs: tuple[Any, ...] | None = None, client_key: str = "planning-key"
) -> HistoricalRangeSourceRequirementPlanV1:
    resolved = resolved_request(specs=specs, client_key=client_key)
    return HistoricalRangeSourceRequirementPlanV1(
        request=resolved.request,
        date_plan=resolved.date_plan,
        frozen_programs=resolved.frozen_programs,
        query_contract_hash=digest("historical-query-contract"),
        calendar_identity_hash=digest("calendar-identity"),
        code_release_hash=resolved.frozen_programs[0].code_release_hash,
        requirements=(
            HistoricalRangeSourceRequirementV1(
                requirement_id="universe",
                source_role="pit_universe",
                dataset_id="market.stock_universe_pit",
                query_template_id="StockUniversePitService.get_eligible_codes",
                query_template_version="v1",
                query_template_hash=digest("universe-query"),
                parameter_template={"trade_date": "${decision_trade_date}"},
                partition_ref_template="shsz_st_pit_active_v1/${decision_trade_date}",
                required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
                missing_reason_code="ADVISORY_HR_PIT_INPUT_UNAVAILABLE",
            ),
        ),
    )


def _planning_bindings(
    plan: HistoricalRangeSourceRequirementPlanV1,
    store: HistoricalRangeArtifactStore,
) -> HistoricalRangePlanningArtifactBindingsV1:
    artifact = store.publish_planning_payload(
        artifact_kind=HistoricalRangeArtifactKind.SOURCE_REQUIREMENT_PLAN,
        planning_identity_hash=plan.planning_identity_hash,
        batch_id=plan.batch_id,
        catalog_generation=1,
        producer_contract_version="phase1r_r2b",
        payload_schema_version=plan.schema_version,
        payload=plan.model_dump(mode="json"),
    )
    return HistoricalRangePlanningArtifactBindingsV1(
        requirement_plan_ref=artifact.ref,
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


def test_create_planning_batch_is_exact_retry_and_creates_no_program_runs(tmp_path: Path) -> None:
    specs = (
        research_spec(name="short rebound", package_id="pkg_short"),
        research_spec(name="long trend", package_id="pkg_long"),
    )
    plan = _planning_request(specs=specs, client_key="create-key-1")
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)

    bindings = _planning_bindings(plan, store)
    first = repository.create_planning_batch(plan=plan, artifacts=bindings)
    second = repository.create_planning_batch(plan=plan, artifacts=bindings)

    assert first.idempotent is False
    assert second.idempotent is True
    assert first.batch_id == second.batch_id
    assert first.create_operation_id == second.create_operation_id
    assert first.catalog_operation_id == second.catalog_operation_id
    assert database.runs == {}
    assert database.batches[first.batch_id]["status"] == "PLANNING"


def test_bound_idempotency_key_rejects_different_planning_semantics(tmp_path: Path) -> None:
    initial = _planning_request(client_key="fixed-key")
    changed = _planning_request(specs=(research_spec(target_count=9),), client_key="fixed-key")
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)
    repository.create_planning_batch(plan=initial, artifacts=_planning_bindings(initial, store))

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        repository.create_planning_batch(plan=changed, artifacts=_planning_bindings(changed, store))
    assert exc_info.value.reason_code == REASON_IDEMPOTENCY_CONFLICT


def test_create_planning_batch_rejects_missing_exact_artifact_before_database_write(tmp_path: Path) -> None:
    plan = _planning_request(client_key="missing-artifact")
    database = _CreateDatabase()
    store = HistoricalRangeArtifactStore(root=tmp_path / "phase1r")
    artifacts = _planning_bindings(plan, store)
    (store.root / artifacts.requirement_plan_ref.relative_path).unlink()
    repository = PostgresHistoricalRangeRepository(conn_factory=database.connection, artifact_store=store)

    with pytest.raises(HistoricalRangeContractError):
        repository.create_planning_batch(plan=plan, artifacts=artifacts)
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
