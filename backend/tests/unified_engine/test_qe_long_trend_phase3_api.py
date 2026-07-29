from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.routers import qe_archive as qe_archive_router
from backend.routers import quantevolver_evolution as evolution_router
from backend.services.quantevolver.long_trend_api_service import (
    LongTrendCreateRequest,
    QELongTrendAPIService,
    QELongTrendAPIServiceError,
    _backtest_freq,
    _label_horizon,
    _merge_data_actions,
    _strategy_topk,
)
from backend.services.quantevolver.long_trend_snapshot_resolver import ResolvedDatasetSnapshot
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceDatasetIdentity


class _Cursor:
    def __init__(self, rows):  # type: ignore[no-untyped-def]
        self._rows = iter(rows)
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql, _params):  # type: ignore[no-untyped-def]
        self._row = next(self._rows)

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, rows):  # type: ignore[no-untyped-def]
        self._cursor = _Cursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self._cursor


def _api_service_with_rows(rows):  # type: ignore[no-untyped-def]
    service = QELongTrendAPIService.__new__(QELongTrendAPIService)
    service._connection_provider = lambda: _Connection(rows)
    return service


def _identity(snapshot_id: str) -> QEWorkspaceDatasetIdentity:
    return QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_v1",
        complete=True,
        reason_code=None,
        missing=(),
        acquisition_suggestions=(),
        dataset={"dataset_manifest_sha256": "f" * 64},
        long_trend_snapshot={
            "snapshot_id": snapshot_id,
            "manifest_sha256": "a" * 64,
            "start_date": "2018-08-01",
            "end_date": "2026-06-30",
            "lineage_parent_ids": [],
            "files": {},
        },
    )


def test_public_create_uses_only_profile_and_registered_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Service:
        async def create_or_update(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"evaluation_id": "qelt_" + "a" * 64, "status": "queued"}

    monkeypatch.setattr(evolution_router, "QELongTrendAPIService", _Service)
    body = evolution_router.LongTrendEvaluationCreateBody(
        profile_id="qe_long_trend_v1",
        outcome_dataset_snapshot_id="snapshot-20260630",
    )

    result = asyncio.run(
        evolution_router.create_or_update_long_trend_evaluation(
            task_id="task-1",
            loop_index=3,
            body=body,
        )
    )

    assert result["status"] == "queued"
    request = captured["request"]
    assert request.profile_id == "qe_long_trend_v1"  # type: ignore[attr-defined]
    assert request.outcome_dataset_snapshot_id == "snapshot-20260630"  # type: ignore[attr-defined]
    assert not hasattr(request, "node_id")
    assert not hasattr(request, "outcome_data_root_uri")


def test_public_create_maps_typed_snapshot_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Service:
        async def create_or_update(self, **_kwargs):  # type: ignore[no-untyped-def]
            raise evolution_router.QELongTrendSnapshotResolutionError(
                "ambiguous snapshot",
                reason_code="QELT_SNAPSHOT_IDENTITY_AMBIGUOUS",
            )

    monkeypatch.setattr(evolution_router, "QELongTrendAPIService", _Service)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            evolution_router.create_or_update_long_trend_evaluation(
                task_id="task-1",
                loop_index=3,
                body=evolution_router.LongTrendEvaluationCreateBody(
                    outcome_dataset_snapshot_id="snapshot-20260630"
                ),
            )
        )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["reason_code"] == "QELT_SNAPSHOT_IDENTITY_AMBIGUOUS"


def test_public_create_maps_invalid_profile_to_bad_request() -> None:
    with pytest.raises(HTTPException) as exc_info:
        evolution_router._raise_long_trend_http(
            QELongTrendAPIServiceError("bad profile", reason_code="QELT_PROFILE_INVALID")
        )
    assert exc_info.value.status_code == 400


def test_public_error_does_not_expose_internal_control_payload() -> None:
    with pytest.raises(HTTPException) as exc_info:
        evolution_router._raise_long_trend_http(
            evolution_router.QELongTrendControlRepositoryError(
                "immutable request_json mismatch at /secret/worker/path"
            )
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["message"] == "F-014 operation failed"
    assert "/secret/worker/path" not in str(exc_info.value.detail)


def test_archive_quality_endpoint_preserves_bounded_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class _Repository:
        def query_quality(self, **kwargs):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            return {"items": [], "next_cursor": None, "limit": kwargs["limit"]}

    monkeypatch.setattr(qe_archive_router, "QELongTrendEvaluationResultRepository", _Repository)

    result = qe_archive_router.query_qe_archive_long_trend_quality(
        evaluation_id=None,
        run_id=None,
        task_id=None,
        loop_index=None,
        outcome_dataset_snapshot_id="snapshot-20260630",
        model_type="LGBM",
        label_horizon=60,
        evaluation_asof=None,
        metric_key="rank_ic",
        horizon=120,
        sector_code="801010",
        family_status=None,
        entry_execution_status=None,
        exit_execution_status=None,
        limit=100,
        cursor=None,
    )

    assert result == {"items": [], "next_cursor": None, "limit": 100}
    assert captured["outcome_dataset_snapshot_id"] == "snapshot-20260630"
    assert captured["model_type"] == "LGBM"
    assert captured["label_horizon"] == 60
    assert captured["metric_key"] == "rank_ic"
    assert captured["horizon"] == 120


def test_archive_run_feature_snapshot_and_node_are_authoritative() -> None:
    service = _api_service_with_rows(
        [
            {
                "task_id": "task-1",
                "task_node_id": "wsl",
                "task_label_horizon": 60,
                "loop_id": "Loop3",
                "loop_index": 3,
                "loop_node_id": "wsl",
                "loop_status": "completed",
                "config_json": {},
            },
            {
                "run_id": "run-1",
                "node_id": "wsl",
                "source_system": "quantevolver",
                "run_type": "evolution_loop",
                "model_type": "LGBM",
                "label_horizon": 60,
                "dataset_snapshot_id": "dataset-snapshot",
                "feature_snapshot_id": "feature-snapshot",
            },
        ]
    )

    context = service._load_loop_context(task_id="task-1", loop_index=3)

    assert context["node_id"] == "wsl"
    assert context["feature_snapshot_id"] == "feature-snapshot"


def test_conflicting_loop_and_archive_nodes_fail_loudly() -> None:
    service = _api_service_with_rows(
        [
            {
                "task_id": "task-1",
                "task_node_id": "wsl",
                "task_label_horizon": 60,
                "loop_id": "Loop3",
                "loop_index": 3,
                "loop_node_id": "wsl",
                "loop_status": "completed",
                "config_json": {},
            },
            {
                "run_id": "run-1",
                "node_id": "remote",
                "source_system": "quantevolver",
                "run_type": "evolution_loop",
                "model_type": "LGBM",
                "label_horizon": 60,
                "dataset_snapshot_id": "dataset-snapshot",
                "feature_snapshot_id": "feature-snapshot",
            },
        ]
    )

    with pytest.raises(QELongTrendAPIServiceError) as exc_info:
        service._load_loop_context(task_id="task-1", loop_index=3)

    assert exc_info.value.reason_code == "QELT_NODE_IDENTITY_CONFLICT"


def test_loop_context_database_failure_is_typed_as_unavailable() -> None:
    service = QELongTrendAPIService.__new__(QELongTrendAPIService)
    service._connection_provider = lambda: (_ for _ in ()).throw(OSError("database offline"))

    with pytest.raises(QELongTrendAPIServiceError) as exc_info:
        service._load_loop_context(task_id="task-1", loop_index=3)

    assert exc_info.value.reason_code == "QELT_RESULT_PERSISTENCE_UNAVAILABLE"


def test_create_or_update_resolves_both_snapshots_and_submits_ready_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    class _ClientContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class _Resolver:
        async def resolve_requested_snapshot(self, *, node_id, requested_snapshot_id, client, snapshot_role="outcome"):
            del client
            calls.append((snapshot_role, requested_snapshot_id))
            return ResolvedDatasetSnapshot(
                node_id=node_id,
                requested_snapshot_id=requested_snapshot_id,
                root_uri=f"/allowlisted/{snapshot_role}",
                identity=_identity(requested_snapshot_id),
                data_action=None,
            )

        @staticmethod
        def unresolved_archived_feature(*, node_id):
            raise AssertionError(node_id)

    class _Control:
        @staticmethod
        def get(_evaluation_id):
            return {"status": "queued", "run_id": "run-1"}

    class _ResultRepository:
        @staticmethod
        def find_materializable_candidates(**_kwargs):
            return ()

    class _Phase2:
        control_repository = _Control()

        def __init__(self):
            self.prepared_request = None
            self.submitted = False

        async def prepare_long_trend_only_resolved(self, **kwargs):  # type: ignore[no-untyped-def]
            self.prepared_request = kwargs["resolved_request"]
            return SimpleNamespace(
                evaluation_id="qelt_" + "a" * 64,
                control_row={"status": "queued", "run_id": "run-1"},
                ready_for_node=True,
                data_action_plan=(),
            )

        async def submit(self, **_kwargs):
            self.submitted = True

    phase2 = _Phase2()
    service = QELongTrendAPIService(
        snapshot_resolver=_Resolver(),  # type: ignore[arg-type]
        result_repository=_ResultRepository(),  # type: ignore[arg-type]
        artifact_store=object(),  # type: ignore[arg-type]
        phase2_service=phase2,  # type: ignore[arg-type]
    )
    service._load_loop_context = lambda **_kwargs: {  # type: ignore[method-assign]
        "node_id": "wsl",
        "run_id": "run-1",
        "feature_snapshot_id": "feature-20260630",
        "label_horizon": 60,
        "config_json": {"backtest_freq": "1day", "strategy_params": {"topk": 25}},
    }
    monkeypatch.setattr(
        "backend.services.quantevolver.long_trend_api_service.QEWorkspaceClient.for_node",
        classmethod(lambda _cls, _node_id: _ClientContext()),
    )

    result = asyncio.run(
        service.create_or_update(
            task_id="task-1",
            loop_index=3,
            request=LongTrendCreateRequest(
                profile_id="qe_long_trend_v1",
                outcome_dataset_snapshot_id="outcome-20260728",
            ),
        )
    )

    assert calls == [("feature", "feature-20260630"), ("outcome", "outcome-20260728")]
    assert phase2.prepared_request.feature_data_root_uri == "/allowlisted/feature"
    assert phase2.prepared_request.outcome_data_root_uri == "/allowlisted/outcome"
    assert phase2.submitted is True
    assert result["evaluation_id"].startswith("qelt_")


def test_terminal_existing_evaluation_materializes_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ClientContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class _Resolver:
        async def resolve_requested_snapshot(self, *, node_id, requested_snapshot_id, client, snapshot_role="outcome"):
            del client
            return ResolvedDatasetSnapshot(
                node_id=node_id,
                requested_snapshot_id=requested_snapshot_id,
                root_uri="/allowlisted",
                identity=_identity(requested_snapshot_id),
                data_action=None,
            )

    class _Control:
        @staticmethod
        def get(_evaluation_id):
            return {"status": "partial", "artifact_manifest_sha256": "a" * 64, "run_id": "run-1"}

    class _ResultRepository:
        @staticmethod
        def find_materializable_candidates(**_kwargs):
            return ()

    class _Phase2:
        control_repository = _Control()

        async def prepare_long_trend_only_resolved(self, **_kwargs):
            return SimpleNamespace(
                evaluation_id="qelt_" + "a" * 64,
                control_row={},
                ready_for_node=True,
                data_action_plan=(),
            )

        async def submit(self, **_kwargs):
            raise AssertionError("terminal replay must not submit a new worker")

    service = QELongTrendAPIService(
        snapshot_resolver=_Resolver(),  # type: ignore[arg-type]
        result_repository=_ResultRepository(),  # type: ignore[arg-type]
        artifact_store=object(),  # type: ignore[arg-type]
        phase2_service=_Phase2(),  # type: ignore[arg-type]
    )
    service._load_loop_context = lambda **_kwargs: {  # type: ignore[method-assign]
        "node_id": "wsl",
        "run_id": "run-1",
        "feature_snapshot_id": "feature-1",
        "label_horizon": 60,
        "config_json": {},
    }
    service._materialize_existing = lambda _evaluation_id: SimpleNamespace(  # type: ignore[method-assign]
        control_row={"status": "partial", "run_id": "run-1", "platform_delivery_status_json": {"db": "published"}}
    )
    monkeypatch.setattr(
        "backend.services.quantevolver.long_trend_api_service.QEWorkspaceClient.for_node",
        classmethod(lambda _cls, _node_id: _ClientContext()),
    )

    result = asyncio.run(
        service.create_or_update(
            task_id="task-1",
            loop_index=3,
            request=LongTrendCreateRequest("qe_long_trend_v1", "outcome-1"),
        )
    )

    assert result["platform_delivery_status"] == {"db": "published"}


def test_existing_terminal_cas_materializes_before_snapshot_resolution() -> None:
    evaluation_id = "qelt_" + "c" * 64

    class _ResultRepository:
        @staticmethod
        def find_materializable_candidates(**kwargs):
            assert kwargs == {
                "run_id": "run-1",
                "task_id": "task-1",
                "loop_index": 4,
                "profile_id": "qe_long_trend_v1",
                "outcome_dataset_snapshot_id": "outcome-1",
            }
            return ({"evaluation_id": evaluation_id},)

    class _Resolver:
        async def resolve_requested_snapshot(self, **_kwargs):
            raise AssertionError("existing CAS replay must not resolve snapshots")

    service = QELongTrendAPIService(
        snapshot_resolver=_Resolver(),  # type: ignore[arg-type]
        result_repository=_ResultRepository(),  # type: ignore[arg-type]
        artifact_store=object(),  # type: ignore[arg-type]
        phase2_service=object(),  # type: ignore[arg-type]
    )
    service._load_loop_context = lambda **_kwargs: {  # type: ignore[method-assign]
        "node_id": "wsl",
        "run_id": "run-1",
    }
    service._materialize_existing = lambda actual_id: SimpleNamespace(  # type: ignore[method-assign]
        control_row={
            "evaluation_id": actual_id,
            "status": "partial",
            "run_id": "run-1",
            "family_status_json": {"signal_path": {"status": "COMPUTED"}},
            "platform_delivery_status_json": {"cas": "published", "db": "published"},
            "data_action_plan_json": [{"action": "restore_execution_evidence"}],
            "reason_code": None,
        }
    )

    result = asyncio.run(
        service.create_or_update(
            task_id="task-1",
            loop_index=4,
            request=LongTrendCreateRequest("qe_long_trend_v1", "outcome-1"),
        )
    )

    assert result == {
        "evaluation_id": evaluation_id,
        "status": "partial",
        "run_id": "run-1",
        "task_id": "task-1",
        "loop_index": 4,
        "ready_for_node": False,
        "family_status": {"signal_path": {"status": "COMPUTED"}},
        "platform_delivery_status": {"cas": "published", "db": "published"},
        "data_action_plan": [{"action": "restore_execution_evidence"}],
        "reason_code": None,
    }


def test_multiple_terminal_cas_candidates_fail_without_materializing() -> None:
    class _ResultRepository:
        @staticmethod
        def find_materializable_candidates(**_kwargs):
            return (
                {"evaluation_id": "qelt_" + "a" * 64},
                {"evaluation_id": "qelt_" + "b" * 64},
            )

    service = QELongTrendAPIService(
        result_repository=_ResultRepository(),  # type: ignore[arg-type]
        artifact_store=object(),  # type: ignore[arg-type]
        phase2_service=object(),  # type: ignore[arg-type]
    )
    service._load_loop_context = lambda **_kwargs: {  # type: ignore[method-assign]
        "node_id": "wsl",
        "run_id": "run-1",
    }
    service._materialize_existing = lambda _evaluation_id: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("ambiguous candidates must not materialize")
    )

    with pytest.raises(QELongTrendAPIServiceError) as exc_info:
        asyncio.run(
            service.create_or_update(
                task_id="task-1",
                loop_index=4,
                request=LongTrendCreateRequest("qe_long_trend_v1", "outcome-1"),
            )
        )

    assert exc_info.value.reason_code == "QELT_CONTROL_STATE_CONFLICT"
    assert exc_info.value.context["matches"] == ["qelt_" + "a" * 64, "qelt_" + "b" * 64]


def test_api_config_extractors_are_strict() -> None:
    assert _backtest_freq({"freq": "1day"}) == "1day"
    assert _backtest_freq(None) is None
    assert _label_horizon({"config_json": {"label_horizon": 120}}) == 120
    assert _strategy_topk({"strategy_params": {"topk": 25}}) == 25
    assert _strategy_topk(None) is None
    with pytest.raises(QELongTrendAPIServiceError, match="must be positive"):
        _strategy_topk({"topk": 0})
    with pytest.raises(QELongTrendAPIServiceError, match="must be an integer"):
        _strategy_topk({"topk": 25.5})
    with pytest.raises(QELongTrendAPIServiceError, match="label_horizon is invalid"):
        _label_horizon({"config_json": {"label_horizon": 60.5}})


def test_api_data_actions_merge_current_acquisition_evidence_without_accepting_corrupt_state() -> None:
    assert _merge_data_actions(
        [{"action": "register_snapshot", "attempt": 1}],
        ({"action": "register_snapshot", "attempt": 2},),
    ) == [
        {"action": "register_snapshot", "attempt": 1},
        {"action": "register_snapshot", "attempt": 2},
    ]
    with pytest.raises(QELongTrendAPIServiceError) as exc_info:
        _merge_data_actions({"unexpected": True}, ())
    assert exc_info.value.reason_code == "QELT_CONTROL_STATE_CONFLICT"
