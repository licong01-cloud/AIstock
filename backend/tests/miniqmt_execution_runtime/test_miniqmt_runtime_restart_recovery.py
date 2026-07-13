from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    JsonFileMiniQMTExecutionRuntimeRepository,
    PostgresMiniQMTExecutionRuntimeRepository,
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntimeClient,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOmsState,
)
from backend.services.miniqmt_execution_runtime.repository import (
    MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV,
    MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV,
    MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV,
    MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV,
    default_miniqmt_execution_runtime_repository,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide


def _config() -> MiniQMTExecutionRuntimeConfig:
    return MiniQMTExecutionRuntimeConfig(
        runtime_id="mqrt_phase2_restart_recovery",
        account_group_id="ag_minqmt_main_sim",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_phase2_restart",
    )


def test_restart_recovery_rebuilds_active_state_and_syncs_broker_before_new_orders(tmp_path) -> None:
    store_path = tmp_path / "runtime-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_gateway = FakeMiniQMTGateway()
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=first_gateway)
    first_runtime.start()
    algo = first_runtime.create_algo_instance(
        parent_intent_id="intent_sell_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=1000,
        algo_code="BEST_LIMIT_MINIQMT",
    )
    child = first_runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=500, price=10.1)
    submitted_before_restart = len(first_gateway.submitted_orders)

    recovery_gateway = FakeMiniQMTGateway(
        orders=[
            {
                "broker_order_id": child.broker_order_id,
                "stock_code": "000001.SZ",
                "status": "SUBMITTED",
                "order_volume": 500,
            }
        ],
        trades=[],
        positions=[{"stock_code": "000001.SZ", "can_sell": 500}],
    )
    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    restarted_runtime = MiniQMTExecutionRuntime(config=_config(), repository=recovered_repo, gateway=recovery_gateway)

    snapshot = restarted_runtime.recover()

    assert len(recovery_gateway.submitted_orders) == 0
    assert submitted_before_restart == 1
    assert snapshot.runtime.oms_state == MiniQMTOmsState.RECONCILED
    assert [item.algo_instance_id for item in snapshot.active_algo_instances] == [algo.algo_instance_id]
    assert [item.child_order_id for item in snapshot.active_child_orders] == [child.child_order_id]
    assert snapshot.broker_orders[0]["broker_order_id"] == child.broker_order_id
    assert snapshot.broker_synced_before_new_orders is True

    event_types = [event.event_type for event in snapshot.events]
    broker_synced_index = event_types.index(MiniQMTExecutionEventType.BROKER_SYNCED)
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED in event_types[:broker_synced_index]
    assert MiniQMTExecutionEventType.CHILD_ORDER_SUBMITTED not in event_types[broker_synced_index + 1 :]


def test_restart_recovery_terminalizes_active_algo_when_all_children_cancelled(tmp_path) -> None:
    store_path = tmp_path / "runtime-store.json"
    repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    first_runtime = MiniQMTExecutionRuntime(config=_config(), repository=repo, gateway=FakeMiniQMTGateway())
    first_runtime.start()
    algo = first_runtime.create_algo_instance(
        parent_intent_id="intent_flatten_000001",
        strategy_slot_id="slot_alpha_001",
        symbol="000001.SZ",
        side=OrderSide.SELL,
        target_quantity=1000,
        algo_code="OPERATOR_FLATTEN",
    )
    child = first_runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=1000, price=10.1)
    repo.upsert_child_order(child.model_copy(update={"status": MiniQMTChildOrderStatus.CANCELLED}))

    recovered_repo = JsonFileMiniQMTExecutionRuntimeRepository(store_path)
    restarted_runtime = MiniQMTExecutionRuntime(
        config=_config(),
        repository=recovered_repo,
        gateway=FakeMiniQMTGateway(orders=[], trades=[], positions=[]),
    )

    snapshot = restarted_runtime.recover()

    assert snapshot.active_algo_instances == []
    assert snapshot.active_child_orders == []
    stored_algo = recovered_repo.list_algo_instances(_config().runtime_id, active_only=False)[0]
    assert stored_algo.status == MiniQMTAlgoInstanceStatus.CANCELLED
    assert stored_algo.metadata["terminalized_by_runtime"] is True
    assert stored_algo.metadata["terminalized_reason"] == "process_restart_recovery"
    runtime_record = recovered_repo.get_runtime(_config().runtime_id)
    assert runtime_record is not None
    assert runtime_record.metadata["last_recovery_terminalized_orphaned_algo_instance_ids"] == [algo.algo_instance_id]
    event_payloads = [event.payload for event in snapshot.events if event.event_type == MiniQMTExecutionEventType.ALGO_ACTION_EMITTED]
    assert any(payload.get("action_type") == "TERMINALIZE_ORPHANED_ALGO" for payload in event_payloads)



def test_default_runtime_repository_is_postgres_and_does_not_create_runtime_state_json(tmp_path, monkeypatch) -> None:
    store_path = tmp_path / "product-runtime-store.json"
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(store_path))
    monkeypatch.delenv(MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV, raising=False)
    monkeypatch.delenv(MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV, raising=False)

    repo = default_miniqmt_execution_runtime_repository()
    client = MiniQMTExecutionRuntimeClient(repository=repo)

    assert isinstance(repo, PostgresMiniQMTExecutionRuntimeRepository)
    assert isinstance(client.repository, PostgresMiniQMTExecutionRuntimeRepository)
    assert not store_path.exists()


def test_jsonfile_repository_requires_explicit_test_only_opt_in(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV, "jsonfile")
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(tmp_path / "runtime-state.json"))
    monkeypatch.delenv(MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV, raising=False)

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        default_miniqmt_execution_runtime_repository()

    assert exc_info.value.context["reason_code"] == "MINIQMT_RUNTIME_JSONFILE_REPOSITORY_TEST_ONLY"
    assert exc_info.value.context["stage"] == "MINIQMT_RUNTIME_REPOSITORY_FACTORY"
    assert exc_info.value.context["jsonfile_production_fallback"] is False

    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_JSONFILE_TEST_ONLY_ENV, "1")
    repo = default_miniqmt_execution_runtime_repository()
    assert isinstance(repo, JsonFileMiniQMTExecutionRuntimeRepository)


def test_in_memory_repository_is_explicit_test_only_and_not_default(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(tmp_path / "unused-default.json"))
    monkeypatch.delenv(MINIQMT_EXECUTION_RUNTIME_REPOSITORY_ENV, raising=False)

    default_client = MiniQMTExecutionRuntimeClient()
    test_client = MiniQMTExecutionRuntimeClient(repository=InMemoryMiniQMTExecutionRuntimeRepository())

    assert isinstance(default_client.repository, PostgresMiniQMTExecutionRuntimeRepository)
    assert isinstance(test_client.repository, InMemoryMiniQMTExecutionRuntimeRepository)


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn
        self.rowcount = 0
        self._fetchone: Any = None
        self._fetchall: list[Any] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.conn.executed.append((sql, params or ()))
        normalized = " ".join(sql.split()).lower()
        self.rowcount = 0
        self._fetchone = None
        self._fetchall = []
        runtime_id = str((params or ("",))[0]) if params else ""
        if "select * from qmt_strategy.execution_runtime where runtime_id" in normalized:
            self._fetchone = self.conn.runtimes.get(runtime_id)
        elif "select * from qmt_strategy.execution_runtime" in normalized:
            self._fetchall = list(self.conn.runtimes.values())
        elif "select coalesce(max(sequence), 0)" in normalized:
            self._fetchone = (self.conn.last_sequence.get(runtime_id, 0),)
        elif "insert into qmt_strategy.execution_runtime_event" in normalized:
            self.conn.last_sequence[str((params or ())[1])] = int((params or ())[2])
        elif "insert into qmt_strategy.execution_runtime (" in normalized:
            params_tuple = params or ()
            self.conn.runtimes[str(params_tuple[0])] = {
                "runtime_id": params_tuple[0],
                "account_group_id": params_tuple[1],
                "trade_date": params_tuple[2],
                "mode": params_tuple[3],
                "event_loop_state": params_tuple[4],
                "gateway_state": params_tuple[5],
                "oms_state": params_tuple[6],
                "runtime_config_hash": params_tuple[7],
                "last_event_sequence": params_tuple[8],
                "metadata": {},
                "created_at": params_tuple[10],
                "updated_at": params_tuple[11],
            }
        elif "update qmt_strategy.execution_runtime_event" in normalized:
            self.rowcount = 1
        elif "update qmt_strategy.execution_algo_instance" in normalized:
            self.rowcount = 1
        elif "update qmt_strategy.execution_child_order" in normalized:
            self.rowcount = 1

    def fetchone(self):  # noqa: ANN201
        return self._fetchone

    def fetchall(self) -> list[Any]:
        return list(self._fetchall)


class _FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.runtimes: dict[str, dict[str, Any]] = {}
        self.last_sequence: dict[str, int] = {}

    def cursor(self, *args: Any, **kwargs: Any) -> _FakeCursor:  # noqa: ARG002
        return _FakeCursor(self)


@contextmanager
def _fake_conn_factory(*args: Any, **kwargs: Any):  # noqa: ANN202, ARG001
    yield _FAKE_CONN


_FAKE_CONN = _FakeConnection()


def _runtime_record(runtime_id: str = "mqrt_pg_incremental"):
    from backend.services.miniqmt_execution_runtime import MiniQMTExecutionRuntimeRecord

    return MiniQMTExecutionRuntimeRecord(
        runtime_id=runtime_id,
        account_group_id="ag_pg",
        trade_date=date(2026, 6, 9),
        runtime_config_hash="runtime_hash_pg",
    )


def test_postgres_repository_writes_incremental_rows_and_prunes_without_json_dump(tmp_path, monkeypatch) -> None:
    _FAKE_CONN.executed.clear()
    _FAKE_CONN.runtimes.clear()
    _FAKE_CONN.last_sequence.clear()
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_STORE_PATH_ENV, str(tmp_path / "runtime-state.json"))
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_MAX_EVENTS_PER_RUNTIME", "1")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_RETAIN_EVENTS_PER_RUNTIME", "1")
    monkeypatch.setenv(MINIQMT_EXECUTION_RUNTIME_PRUNE_EVERY_WRITES_ENV, "1")
    repo = PostgresMiniQMTExecutionRuntimeRepository(conn_factory=_fake_conn_factory)
    runtime = repo.upsert_runtime(_runtime_record())
    repo.append_event(
        __import__("backend.services.miniqmt_execution_runtime", fromlist=["MiniQMTExecutionEvent"]).MiniQMTExecutionEvent(
            event_id="mqrtevt_pg_1",
            runtime_id=runtime.runtime_id,
            sequence=1,
            event_type=MiniQMTExecutionEventType.RUNTIME_CREATED,
            event_time=datetime(2026, 6, 9, 9, 30, tzinfo=UTC),
            source="runtime",
            payload={"source": "unit"},
        )
    )

    sql = "\n".join(statement for statement, _params in _FAKE_CONN.executed).lower()
    assert "insert into qmt_strategy.execution_runtime" in sql
    assert "insert into qmt_strategy.execution_runtime_event" in sql
    assert "update qmt_strategy.execution_runtime_event" in sql
    assert "json.dumps" not in sql
    assert not (tmp_path / "runtime-state.json").exists()


def test_miniqmt_runtime_repository_ddl_has_forward_and_rollback_contract() -> None:
    forward = Path("backend/migrations/miniqmt_execution_runtime_repository_20260707.sql").read_text(encoding="utf-8").lower()
    rollback = Path("backend/migrations/miniqmt_execution_runtime_repository_20260707.rollback.sql").read_text(encoding="utf-8").lower()
    for table in (
        "qmt_strategy.execution_runtime",
        "qmt_strategy.execution_runtime_event",
        "qmt_strategy.execution_algo_instance",
        "qmt_strategy.execution_child_order",
    ):
        assert f"create table if not exists {table}" in forward
        assert f"drop table if exists {table}" in rollback
    assert "controlled ddl only" in forward
    assert "do not run from service startup" in forward
    assert "archived_at" in forward
    assert "create index if not exists" in forward
    assert "on delete cascade" not in forward
