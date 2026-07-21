from __future__ import annotations

import asyncio
from typing import Any, Mapping

from backend.services.quantevolver.qe_active_execution_capacity import (
    QEActiveExecutionImportService,
    QEActiveExecutionCapacityService,
    set_qe_capacity_queue_only_nodes,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceSubmissionInspection,
)


class _ReservationRepository:
    def __init__(self) -> None:
        self.imported: list[dict[str, Any]] = []

    def preflight_schema(self, *, raise_on_error: bool = False) -> object:
        assert raise_on_error is True
        return object()

    def import_legacy_active_execution(self, spec: Any, *, remote_status: str) -> Mapping[str, Any]:
        row = {
            "node_id": spec.node_id,
            "qe_task_id": spec.qe_task_id,
            "qe_loop_id": spec.qe_loop_id,
            "remote_status": remote_status,
        }
        self.imported.append(row)
        return row


class _CandidateCursor:
    def __init__(self, result_batches: list[list[Mapping[str, Any]]]) -> None:
        self._result_batches = iter(result_batches)
        self._rows: list[Mapping[str, Any]] = []

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        return None

    def execute(self, _query: str, _params: Any) -> None:
        self._rows = next(self._result_batches)

    def fetchall(self) -> list[Mapping[str, Any]]:
        return self._rows


class _CandidateConnection:
    def __init__(self, result_batches: list[list[Mapping[str, Any]]]) -> None:
        self._cursor = _CandidateCursor(result_batches)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        return None

    def cursor(self, **_kwargs: Any) -> _CandidateCursor:
        return self._cursor


class _WorkspaceClient:
    def __init__(self, node_id: str, states: Mapping[str, str]) -> None:
        self.node_id = node_id
        self.states = states

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type: Any, _exc: Any, _tb: Any) -> None:
        return None

    async def inspect_loop_submission(self, task_id: str, loop_id: str):
        state = self.states[self.node_id]
        receipt_status = "running" if state == "active" else "not_reserved"
        return QEWorkspaceSubmissionInspection(
            schema_version="qe_submission_receipt_v1",
            task_id=task_id,
            loop_id=loop_id,
            status=receipt_status,
            submission_intent_hash="a" * 64 if receipt_status != "not_reserved" else None,
            request_digest="b" * 64 if receipt_status != "not_reserved" else None,
        )

    async def get_loop_status(self, _task_id: str, _loop_id: str) -> Mapping[str, Any]:
        state = self.states[self.node_id]
        if state == "active":
            return {"status": "running"}
        if state == "terminal":
            return {"status": "completed"}
        return {"status": "not_found"}


class _ImportService(QEActiveExecutionImportService):
    def __init__(
        self,
        *,
        repository: _ReservationRepository,
        candidates: list[Mapping[str, Any]],
        nodes: list[str],
        states: Mapping[str, str],
    ) -> None:
        super().__init__(
            reservation_repository=repository,  # type: ignore[arg-type]
            connection_provider=lambda: None,
            workspace_client_factory=lambda node_id: _WorkspaceClient(node_id, states),
        )
        self.candidates = candidates
        self.nodes = nodes

    def _discover_candidates(self) -> list[Mapping[str, Any]]:
        return [dict(item) for item in self.candidates]

    def _list_compute_node_ids(self) -> list[str]:
        return list(self.nodes)


class _DatabaseImportService(QEActiveExecutionImportService):
    def __init__(
        self,
        *,
        repository: _ReservationRepository,
        connection: _CandidateConnection,
    ) -> None:
        super().__init__(
            reservation_repository=repository,  # type: ignore[arg-type]
            connection_provider=lambda: connection,
        )

    def _list_compute_node_ids(self) -> list[str]:
        return ["wsl2-5080"]


def _candidate(node_id: str | None = None) -> dict[str, Any]:
    return {
        "source_kind": "qe_experiment",
        "source_execution_id": "exp_1",
        "node_id": node_id,
        "qe_task_id": "qe_task_1",
        "qe_loop_id": "Loop1",
        "source_status": "running",
    }


def _group_candidate(*, source_id: str, parent_status: str | None) -> dict[str, Any]:
    return {
        "source_kind": "qe_multi_alpha_group",
        "source_execution_id": source_id,
        "node_id": "wsl2-5080",
        "qe_task_id": "qe_task_1",
        "qe_loop_id": None,
        "source_status": "running",
        "parent_status": parent_status,
    }


def test_discovery_excludes_terminal_parent_groups_but_keeps_nonterminal_rows(
    caplog: Any,
) -> None:
    terminal_rows = [
        _group_candidate(source_id=f"terminal:{status}", parent_status=status)
        for status in sorted(QEActiveExecutionImportService.TERMINAL_PARENT_STATUSES)
    ]
    pending = _group_candidate(source_id="pending:sector", parent_status="pending")
    unknown = _group_candidate(source_id="unknown:sector", parent_status=None)
    connection = _CandidateConnection([[], [], [*terminal_rows, pending, unknown]])
    service = QEActiveExecutionImportService(
        reservation_repository=_ReservationRepository(),  # type: ignore[arg-type]
        connection_provider=lambda: connection,
    )

    with caplog.at_level("WARNING"):
        candidates = service._discover_candidates()  # noqa: SLF001 - direct regression point

    assert [row["source_execution_id"] for row in candidates] == [
        "pending:sector",
        "unknown:sector",
    ]
    assert "Skipped 9 stale active QE multi-alpha group rows" in caplog.text


def test_terminal_parent_stale_group_does_not_force_node_queue_only() -> None:
    repository = _ReservationRepository()
    connection = _CandidateConnection(
        [[], [], [_group_candidate(source_id="failed:sector", parent_status="failed")]]
    )
    service = _DatabaseImportService(repository=repository, connection=connection)

    try:
        result = asyncio.run(service.import_current_active_sources_verified())
        assert result.discovered_count == 0
        assert result.unresolved == ()
        assert result.queue_only_nodes == {}
        assert repository.imported == []
    finally:
        set_qe_capacity_queue_only_nodes({})


def test_nonterminal_parent_with_incomplete_identity_remains_queue_only() -> None:
    repository = _ReservationRepository()
    connection = _CandidateConnection(
        [[], [], [_group_candidate(source_id="pending:sector", parent_status="pending")]]
    )
    service = _DatabaseImportService(repository=repository, connection=connection)

    try:
        result = asyncio.run(service.import_current_active_sources_verified())
        assert result.discovered_count == 1
        assert len(result.unresolved) == 1
        assert result.unresolved[0]["reason_code"] == "qe_capacity_identity_unresolved"
        assert set(result.queue_only_nodes) == {"wsl2-5080"}
        assert repository.imported == []
    finally:
        set_qe_capacity_queue_only_nodes({})


def test_missing_node_is_resolved_by_exactly_one_active_workspace() -> None:
    repository = _ReservationRepository()
    service = _ImportService(
        repository=repository,
        candidates=[_candidate()],
        nodes=["wsl2-5080", "rdagent-node1"],
        states={"wsl2-5080": "absent", "rdagent-node1": "active"},
    )
    try:
        result = asyncio.run(service.import_current_active_sources_verified())
        assert result.imported_count == 1
        assert repository.imported[0]["node_id"] == "rdagent-node1"
        assert result.unresolved == ()
        assert result.queue_only_nodes == {}
    finally:
        set_qe_capacity_queue_only_nodes({})


def test_ambiguous_active_workspace_keeps_only_related_nodes_queue_only() -> None:
    repository = _ReservationRepository()
    service = _ImportService(
        repository=repository,
        candidates=[_candidate()],
        nodes=["wsl2-5080", "rdagent-node1", "unused-node"],
        states={
            "wsl2-5080": "active",
            "rdagent-node1": "active",
            "unused-node": "absent",
        },
    )
    try:
        result = asyncio.run(service.import_current_active_sources_verified())
        assert result.imported_count == 0
        assert len(result.unresolved) == 1
        assert set(result.queue_only_nodes) == {"wsl2-5080", "rdagent-node1"}
        assert QEActiveExecutionCapacityService.queue_only_diagnostics("unused-node") == ()
    finally:
        set_qe_capacity_queue_only_nodes({})


def test_configured_unreachable_node_is_conservatively_imported_and_diagnosed() -> None:
    class _UnavailableClient(_WorkspaceClient):
        async def inspect_loop_submission(self, _task_id: str, _loop_id: str):
            from backend.services.quantevolver.qe_workspace_client import (
                QEWorkspaceSubmissionTransportError,
            )

            raise QEWorkspaceSubmissionTransportError(
                "offline",
                reason_code="qe_workspace_submission_inspection_unavailable",
            )

    repository = _ReservationRepository()
    service = _ImportService(
        repository=repository,
        candidates=[_candidate("wsl2-5080")],
        nodes=["wsl2-5080"],
        states={"wsl2-5080": "absent"},
    )
    service._workspace_client_factory = lambda node_id: _UnavailableClient(  # type: ignore[attr-defined]
        node_id,
        {"wsl2-5080": "absent"},
    )
    try:
        result = asyncio.run(service.import_current_active_sources_verified())
        assert result.imported_count == 1
        assert len(result.unresolved) == 1
        assert result.unresolved[0]["reason_code"] == "qe_capacity_remote_verification_unavailable"
        assert result.queue_only_nodes == {}
    finally:
        set_qe_capacity_queue_only_nodes({})
