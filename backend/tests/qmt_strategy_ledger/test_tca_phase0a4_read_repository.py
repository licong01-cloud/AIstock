from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any, Mapping

import pytest

from backend.services.qmt_strategy_ledger.tca_read_repository import ExecutionTcaReadRepository
from backend.services.qmt_strategy_ledger.tca_read_service import TcaActiveReadVersion, TcaReadError
from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256


TRADE_DATE = date(2026, 7, 10)
SNAPSHOT_STARTED = datetime(2026, 7, 10, 7, 0, tzinfo=UTC)


def test_parent_head_keeps_legacy_invalid_parent_readable_without_audit_timestamp_ordering() -> None:
    cursor = _ReadCursor(
        parent_rows=(
            _parent(parent_revision=2, eligibility_class="LEGACY_UNRECOVERABLE"),
            _parent(parent_revision=1, eligibility_class="ELIGIBLE_NOW"),
        )
    )

    parent = ExecutionTcaReadRepository().get_parent(parent_intent_id="parent-1", cursor=cursor)

    assert parent is not None
    assert parent["parent_revision"] == 2
    assert parent["eligibility_class"] == "LEGACY_UNRECOVERABLE"
    assert "created_at" not in cursor.sql.lower()
    assert "ORDER BY b.parent_revision DESC" in cursor.sql


def test_list_uses_documented_keyset_order_and_typed_terminal_state() -> None:
    cursor = _ReadCursor(
        parent_rows=(
            _parent(parent_intent_id="parent-a", parent_revision=1, eligible_quantity=0),
            _parent(parent_intent_id="parent-b", parent_revision=1, eligible_quantity=100),
            _parent(parent_intent_id="parent-c", parent_revision=1, eligible_quantity=100),
        ),
        active_by_parent={
            "parent-b": (
                _result(
                    result_id="res-b",
                    parent_intent_id="parent-b",
                    generation=1,
                    deadline_residual_quantity=0,
                ),
            ),
            "parent-c": (
                _result(
                    result_id="res-c",
                    parent_intent_id="parent-c",
                    generation=1,
                    deadline_residual_quantity=10,
                ),
            ),
        },
    )

    page = ExecutionTcaReadRepository().list_parents(
        binding_id="binding-1",
        trade_date=TRADE_DATE,
        limit=2,
        active_version=_active_version(),
        terminal_state=None,
        cursor=cursor,
    )

    assert [row["terminal_state"] for row in page.parents] == ["NO_ELIGIBLE", "COMPLETED_BY_DEADLINE"]
    assert page.next_key == (TRADE_DATE, "parent-b", 1)
    assert "ORDER BY b.trade_date ASC, b.parent_intent_id ASC, b.parent_revision ASC" in cursor.sql
    assert "created_at" not in cursor.sql.lower()


def test_active_result_uses_completed_receipt_generation_not_created_at() -> None:
    cursor = _ReadCursor(
        active_rows=(
            _result(
                result_id="res-1",
                generation=1,
                source_started=SNAPSHOT_STARTED,
                created_at=SNAPSHOT_STARTED + timedelta(days=5),
                completed_receipt_ids=("receipt-1",),
            ),
            _result(
                result_id="res-2",
                generation=2,
                supersedes="res-1",
                source_started=SNAPSHOT_STARTED + timedelta(minutes=1),
                created_at=SNAPSHOT_STARTED - timedelta(days=5),
                completed_receipt_ids=("receipt-2",),
            ),
        )
    )

    selection = ExecutionTcaReadRepository().get_tca(
        parent_intent_id="parent-1",
        parent_revision=1,
        snapshot_kind="RECONCILED_FINAL",
        active_version=_active_version(),
        cursor=cursor,
    )

    assert selection.selection_mode == "ACTIVE_HEAD"
    assert selection.result["tca_result_id"] == "res-2"
    assert "created_at" not in cursor.sql.lower()
    assert "ORDER BY r.result_series_key ASC, r.result_generation ASC" in cursor.sql


def test_as_of_and_receipt_selection_follow_one_validated_series() -> None:
    first = _result(
        result_id="res-1",
        generation=1,
        source_started=SNAPSHOT_STARTED,
        completed_receipt_ids=("receipt-1",),
    )
    second = _result(
        result_id="res-2",
        generation=2,
        supersedes="res-1",
        source_started=SNAPSHOT_STARTED + timedelta(minutes=1),
        completed_receipt_ids=("receipt-2",),
    )
    repository = ExecutionTcaReadRepository()

    as_of = repository.get_tca(
        parent_intent_id="parent-1",
        parent_revision=1,
        snapshot_kind="RECONCILED_FINAL",
        active_version=_active_version(),
        as_of=SNAPSHOT_STARTED + timedelta(seconds=30),
        cursor=_ReadCursor(active_rows=(first, second)),
    )
    by_receipt = repository.get_tca(
        parent_intent_id="parent-1",
        parent_revision=1,
        snapshot_kind="RECONCILED_FINAL",
        active_version=_active_version(),
        receipt_id="receipt-1",
        cursor=_ReadCursor(active_rows=(first, second)),
    )

    assert as_of.selection_mode == "AS_OF"
    assert as_of.result["tca_result_id"] == "res-1"
    assert by_receipt.selection_mode == "RECEIPT"
    assert by_receipt.result["tca_result_id"] == "res-1"


def test_result_generation_gap_or_receipt_asof_conflict_is_loud() -> None:
    repository = ExecutionTcaReadRepository()
    with pytest.raises(TcaReadError) as fork:
        repository.get_tca(
            parent_intent_id="parent-1",
            parent_revision=1,
            snapshot_kind="RECONCILED_FINAL",
            active_version=_active_version(),
            cursor=_ReadCursor(
                active_rows=(
                    _result(result_id="res-1", generation=1),
                    _result(result_id="res-3", generation=3, supersedes="res-1"),
                )
            ),
        )
    with pytest.raises(TcaReadError) as conflict:
        repository.get_tca(
            parent_intent_id="parent-1",
            parent_revision=1,
            snapshot_kind="RECONCILED_FINAL",
            active_version=_active_version(),
            receipt_id="receipt-1",
            as_of=SNAPSHOT_STARTED,
            cursor=_ReadCursor(),
        )
    with pytest.raises(TcaReadError) as explicit_as_of:
        repository.get_tca(
            parent_intent_id="parent-1",
            parent_revision=1,
            snapshot_kind="RECONCILED_FINAL",
            tca_version="res-1",
            as_of=SNAPSHOT_STARTED,
            cursor=_ReadCursor(),
        )

    assert fork.value.reason_code == "ADAPTIVE_IS_TCA_CHAIN_FORK"
    assert fork.value.http_status == 409
    assert conflict.value.reason_code == "ADAPTIVE_IS_TCA_SELECTION_CONFLICT"
    assert conflict.value.http_status == 400
    assert explicit_as_of.value.reason_code == "ADAPTIVE_IS_TCA_SELECTION_CONFLICT"


def test_active_tuple_and_explicit_receipt_filter_cannot_select_another_series_or_receipt() -> None:
    repository = ExecutionTcaReadRepository()
    unexpected_series = _result(result_id="res-1")
    unexpected_series["result_series_key"] = "b" * 64
    with pytest.raises(TcaReadError) as unexpected:
        repository.get_tca(
            parent_intent_id="parent-1",
            parent_revision=1,
            snapshot_kind="RECONCILED_FINAL",
            active_version=_active_version(),
            cursor=_ReadCursor(active_rows=(unexpected_series,)),
        )

    first = _result(result_id="res-1", generation=1, completed_receipt_ids=("receipt-1",))
    second = _result(
        result_id="res-2",
        generation=2,
        supersedes="res-1",
        completed_receipt_ids=("receipt-2",),
    )
    with pytest.raises(TcaReadError) as wrong_receipt:
        repository.get_tca(
            parent_intent_id="parent-1",
            parent_revision=1,
            snapshot_kind="RECONCILED_FINAL",
            tca_version="res-2",
            receipt_id="receipt-1",
            cursor=_ReadCursor(explicit_rows=(second,), series_rows=(first, second)),
        )

    assert unexpected.value.reason_code == "ADAPTIVE_IS_TCA_CHAIN_FORK"
    assert wrong_receipt.value.reason_code == "ADAPTIVE_IS_TCA_RESULT_NOT_FOUND"


def test_explicit_result_validates_its_series_and_read_snapshot_is_repeatable_read_only() -> None:
    explicit = _result(result_id="res-2", generation=2, supersedes="res-1")
    first = _result(result_id="res-1", generation=1)
    cursor = _ReadCursor(explicit_rows=(explicit,), series_rows=(first, explicit))
    connection = _ReadConnection(cursor)
    repository = ExecutionTcaReadRepository(conn_factory=lambda: connection)

    selection = repository.get_tca(
        parent_intent_id="parent-1",
        parent_revision=1,
        snapshot_kind="RECONCILED_FINAL",
        tca_version="res-2",
    )

    assert selection.selection_mode == "EXPLICIT"
    assert selection.result["tca_result_id"] == "res-2"
    assert connection.session_calls == [{"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}]


def _active_version() -> TcaActiveReadVersion:
    values = {
        "calculator_version": "calculator-v1",
        "formula_version": "formula-v1",
        "schema_version": "schema-v1",
        "query_version": "query-v1",
        "benchmark_policy_version": "benchmark-v1",
        "mark_policy_version": "mark-v1",
        "fee_policy_version": "fee-v1",
        "trade_provenance_policy_version": "trade-v1",
    }
    return TcaActiveReadVersion.from_mapping({**values, "config_sha256": canonical_json_sha256(values)})


def _parent(
    *,
    parent_intent_id: str = "parent-1",
    parent_revision: int = 1,
    eligible_quantity: int | None = 100,
    eligibility_class: str = "ELIGIBLE_NOW",
) -> dict[str, Any]:
    return {
        "parent_intent_id": parent_intent_id,
        "parent_revision": parent_revision,
        "binding_id": "binding-1",
        "trade_date": TRADE_DATE,
        "environment": "SIM",
        "eligible_quantity": eligible_quantity,
        "eligibility_class": eligibility_class,
    }


def _result(
    *,
    result_id: str = "res-1",
    parent_intent_id: str = "parent-1",
    parent_revision: int = 1,
    snapshot_kind: str = "RECONCILED_FINAL",
    generation: int = 1,
    supersedes: str | None = None,
    source_started: datetime = SNAPSHOT_STARTED,
    created_at: datetime = SNAPSHOT_STARTED,
    deadline_residual_quantity: int | None = 0,
    completed_receipt_ids: tuple[str, ...] = ("receipt-1",),
) -> dict[str, Any]:
    version = _active_version().as_mapping()
    result_series_key = canonical_json_sha256(
        {
            "parent_intent_id": parent_intent_id,
            "parent_revision": parent_revision,
            "snapshot_kind": snapshot_kind,
            **version,
        }
    )
    return {
        "tca_result_id": result_id,
        "result_series_key": result_series_key,
        "result_generation": generation,
        "supersedes_tca_result_id": supersedes,
        "parent_intent_id": parent_intent_id,
        "parent_revision": parent_revision,
        "snapshot_kind": snapshot_kind,
        "result_status": "FINAL",
        "source_snapshot_started_at": source_started,
        "created_at": created_at,
        "deadline_residual_quantity": deadline_residual_quantity,
        "completion_by_deadline_quantity": "1" if deadline_residual_quantity == 0 else "0.9",
        "completed_receipt_ids": list(completed_receipt_ids),
        **version,
    }


class _ReadCursor:
    def __init__(
        self,
        *,
        parent_rows: tuple[Mapping[str, Any], ...] = (),
        active_rows: tuple[Mapping[str, Any], ...] = (),
        active_by_parent: Mapping[str, tuple[Mapping[str, Any], ...]] | None = None,
        explicit_rows: tuple[Mapping[str, Any], ...] = (),
        series_rows: tuple[Mapping[str, Any], ...] = (),
    ) -> None:
        self.parent_rows = parent_rows
        self.active_rows = active_rows
        self.active_by_parent = dict(active_by_parent or {})
        self.explicit_rows = explicit_rows
        self.series_rows = series_rows
        self.current: tuple[Mapping[str, Any], ...] = ()
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def __enter__(self) -> "_ReadCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    @property
    def sql(self) -> str:
        return "\n".join(statement for statement, _ in self.calls)

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.calls.append((sql, params))
        if "execution_parent_benchmark b" in sql and "execution_parent_tca r" not in sql:
            self.current = self.parent_rows
        elif "r.tca_result_id = %s" in sql:
            self.current = self.explicit_rows
        elif "r.result_series_key = %s" in sql:
            self.current = self.series_rows
        else:
            self.current = self.active_by_parent.get(str(params[0]), self.active_rows)

    def fetchall(self) -> list[Mapping[str, Any]]:
        return list(self.current)


class _ReadConnection:
    def __init__(self, cursor: _ReadCursor) -> None:
        self._cursor = cursor
        self.session_calls: list[dict[str, Any]] = []

    def __enter__(self) -> "_ReadConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def set_session(self, **kwargs: Any) -> None:
        self.session_calls.append(kwargs)

    def cursor(self, **_: Any) -> _ReadCursor:
        return self._cursor
