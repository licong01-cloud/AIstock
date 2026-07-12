from __future__ import annotations

from datetime import timedelta

from backend.execution_algos.adaptive_is.contracts import EvidenceCaptureType
from backend.services.miniqmt_execution_runtime.models import MiniQMTExecutionEventType
from backend.services.miniqmt_execution_runtime.repository import PostgresMiniQMTExecutionRuntimeRepository
from backend.services.qmt_strategy_ledger.tca_read_repository import ExecutionTcaParentPage
from backend.services.simulation_runtime.tca_read_api import ExecutionTcaReadService

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import (
    CLOCK_AT,
    TRADE_DATE,
    _observation,
    _runtime_controller,
)
from backend.tests.miniqmt_execution_runtime.test_quote_evidence import _evidence
from backend.tests.simulation_runtime.test_tca_read_api import _ExportRepository, _config, _parent


class _QuoteControlTcaRepository(_ExportRepository):
    def __init__(self) -> None:
        super().__init__()
        self.snapshot_count = 0

    def read_snapshot(self):  # type: ignore[no-untyped-def]
        original = super().read_snapshot()

        class _Snapshot:
            def __enter__(inner_self):  # type: ignore[no-untyped-def]
                self.snapshot_count += 1
                return original.__enter__()

            def __exit__(inner_self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
                return original.__exit__(exc_type, exc, tb)

        return _Snapshot()

    def list_parents(self, **_: object) -> ExecutionTcaParentPage:
        self.calls.append("list_parents")
        parent = {
            **_parent(),
            "parent_intent_id": "parent-p1e",
            "trade_date": TRADE_DATE,
            "binding_id": "binding-p1e",
            "binding_hash": "e" * 64,
            "runtime_id": "runtime-p1e",
        }
        return ExecutionTcaParentPage(parents=(parent,), next_key=None)


def _complete_runtime_evidence():  # type: ignore[no-untyped-def]
    controller, runtime, _gateway, repository = _runtime_controller()
    assignment = next(iter(controller.assignments.values()))
    revision = assignment.revision
    assert revision is not None
    record = repository.get_runtime(runtime.config.runtime_id)
    assert record is not None
    repository.upsert_runtime(
        record.model_copy(
            update={
                "metadata": {
                    **record.metadata,
                    "quote_control": {
                        "revision": revision.canonical_payload(),
                        "assignments": [assignment.canonical_payload()],
                    },
                }
            }
        )
    )
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    child = repository.list_child_orders(runtime.config.runtime_id, active_only=False)[0]
    runtime.record_trade_event(
        broker_order_id=str(child.broker_order_id),
        quantity=100,
        price=10.01,
        payload={"trade_id": "trade-p1e", "trade_time_utc": CLOCK_AT.isoformat(), "cumulative_quantity": 100},
    )
    context = controller.context_store.snapshot()
    assert context is not None
    for sequence, (seconds, source_time) in enumerate(
        ((60, "09310000"), (300, "09350000"), (900, "09450000")), start=2
    ):
        observation = _observation(
            context,
            sequence=sequence,
            source_time=source_time,
            received_at_utc=CLOCK_AT + timedelta(seconds=seconds),
            received_monotonic_ns=2_000_000_000 + seconds * 1_000_000_000,
        )
        controller.evidence_coordinator.observe(observation)
    cadence = _evidence(
        EvidenceCaptureType.CADENCE_AGGREGATE,
        runtime_id=runtime.config.runtime_id,
        trade_date=TRADE_DATE,
        policy_sha256=revision.quote_policy_sha256,
        config_sha256=controller.config_sha256,
        adapter_sha256=revision.adapter_sha256,
        code_sha256=revision.code_sha256,
        schema_sha256=revision.evidence_schema_sha256,
    )
    controller.evidence_coordinator.enqueue(cadence, event_type=MiniQMTExecutionEventType.QUOTE_OBSERVED)
    controller.evidence_coordinator.drain_markouts(now_utc=CLOCK_AT + timedelta(seconds=901))
    controller.evidence_coordinator.flush(now_utc=CLOCK_AT + timedelta(seconds=901))
    final_record = repository.get_runtime(runtime.config.runtime_id)
    assert final_record is not None
    repository.upsert_runtime(
        final_record.model_copy(
            update={
                "metadata": {
                    **final_record.metadata,
                    "quote_control": {
                        "revision": revision.canonical_payload(),
                        "assignments": [assignment.canonical_payload()],
                    },
                }
            }
        )
    )
    return controller, repository


def test_v2_export_rebuilds_assignment_quote_depth_age_cadence_child_trade_and_markouts() -> None:
    controller, runtime_repository = _complete_runtime_evidence()
    tca_repository = _QuoteControlTcaRepository()
    service = ExecutionTcaReadService(
        repository=tca_repository,
        config_provider=_config,
        runtime_repository=runtime_repository,
    )

    export = service.export_execution_evidence(
        binding_id="binding-p1e",
        trade_date=TRADE_DATE,
        evidence_version="miniqmt_execution_tca_evidence_v2",
    )

    assert (
        export.manifest["missing_link_count"],
        export.manifest["duplicate_child_count"],
        export.manifest["revision_conflict_count"],
        export.manifest["hash_conflict_count"],
        export.manifest["identity_conflict_count"],
        export.manifest["assignment_missing_count"],
        export.manifest["missing_action_link_count"],
        export.manifest["missing_child_link_count"],
        export.manifest["missing_trade_mark_count"],
    ) == (0, 0, 0, 0, 0, 0, 0, 0, 0)
    assert export.manifest["quote_control_complete"] is True
    assert export.manifest["five_level_coverage"] == 1.0
    assert export.manifest["age_coverage"] == 1.0
    assert export.manifest["cadence_aggregate_count"] == 1
    assert export.manifest["markout_coverage"] == {"60": 1.0, "300": 1.0, "900": 1.0}
    assert export.manifest["config_sha256_set"] == [controller.config_sha256]
    assert {record["record_kind"] for record in export.records} >= {
        "CONTROL_REVISION",
        "PARENT_ASSIGNMENT",
        "ACTION_INPUT",
        "ACTION_EVENT",
        "CHILD_EVENT",
        "CHILD_RECEIPT",
        "TRADE_ANCHOR",
        "MARKOUT",
        "CADENCE_AGGREGATE",
    }


def test_v2_export_marks_missing_duplicate_or_revision_conflict_incomplete() -> None:
    _controller, runtime_repository = _complete_runtime_evidence()
    runtime = runtime_repository.get_runtime("runtime-p1e")
    assert runtime is not None
    metadata = dict(runtime.metadata)
    quote_control = dict(metadata["quote_control"])
    quote_control["assignments"] = [*quote_control["assignments"], dict(quote_control["assignments"][0])]
    metadata["quote_control"] = quote_control
    runtime_repository.upsert_runtime(runtime.model_copy(update={"metadata": metadata}))
    service = ExecutionTcaReadService(
        repository=_QuoteControlTcaRepository(),
        config_provider=_config,
        runtime_repository=runtime_repository,
    )

    export = service.export_execution_evidence(
        binding_id="binding-p1e",
        trade_date=TRADE_DATE,
        evidence_version="miniqmt_execution_tca_evidence_v2",
    )

    assert export.manifest["quote_control_complete"] is False
    assert export.manifest["missing_link_count"] >= 1


def test_v1_export_is_byte_stable_and_v2_uses_one_bounded_read_snapshot() -> None:
    _controller, runtime_repository = _complete_runtime_evidence()
    repository = _QuoteControlTcaRepository()
    service = ExecutionTcaReadService(
        repository=repository, config_provider=_config, runtime_repository=runtime_repository
    )

    first_v1 = service.export_execution_evidence(binding_id="binding-p1e", trade_date=TRADE_DATE)
    second_v1 = service.export_execution_evidence(binding_id="binding-p1e", trade_date=TRADE_DATE)
    service.export_execution_evidence(
        binding_id="binding-p1e",
        trade_date=TRADE_DATE,
        evidence_version="miniqmt_execution_tca_evidence_v2",
    )

    assert first_v1 == second_v1
    assert repository.snapshot_count == 3
    assert repository.calls.count("list_parents") == 3


def test_postgres_quote_control_read_uses_tca_owned_cursor_and_bounded_runtime_ids() -> None:
    _controller, runtime_repository = _complete_runtime_evidence()
    runtime = runtime_repository.get_runtime("runtime-p1e")
    events = runtime_repository.list_events("runtime-p1e", include_archived=True)
    assert runtime is not None and events

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, object]] = []
            self._rows: list[dict[str, object]] = []

        def execute(self, sql: str, params: object) -> None:
            self.calls.append((sql, params))
            self._rows = (
                [runtime.model_dump(mode="python")]
                if "execution_runtime\n" in sql
                else [event.model_dump(mode="python") for event in events]
            )

        def fetchall(self):  # type: ignore[no-untyped-def]
            return list(self._rows)

    cursor = Cursor()
    repository = PostgresMiniQMTExecutionRuntimeRepository(
        conn_factory=lambda: (_ for _ in ()).throw(AssertionError("must use external cursor"))
    )

    runtimes, snapshot_events = repository.read_quote_control_snapshot(
        cursor=cursor,
        runtime_ids=("runtime-p1e", "runtime-p1e", ""),
        include_archived=False,
    )

    assert [item.runtime_id for item in runtimes] == ["runtime-p1e"]
    assert [item.event_id for item in snapshot_events] == [item.event_id for item in events]
    assert len(cursor.calls) == 2
    assert cursor.calls[0][1] == (["runtime-p1e"],)
    assert "archived_at IS NULL" in cursor.calls[1][0]
