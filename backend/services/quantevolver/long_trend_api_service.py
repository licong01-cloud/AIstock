"""Public QE-only orchestration facade for F-014 Phase 3."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.long_trend_repository import QELongTrendEvaluationResultRepository
from backend.services.quantevolver.experiment_config import normalize_label_horizon
from backend.services.quantevolver.long_trend_artifact_store import QELongTrendArtifactStore
from backend.services.quantevolver.long_trend_artifact_resolver import (
    resolve_long_trend_recorder_artifacts,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    EVALUATOR_VERSION,
    PROFILE_ID_V1,
    QELongTrendError,
    QELongTrendReason,
    get_long_trend_profile,
)
from backend.services.quantevolver.long_trend_evaluation_control_repository import QE_RUN_TYPES, QE_SOURCE_SYSTEMS
from backend.services.quantevolver.long_trend_evaluation_phase2 import (
    QELongTrendPhase2Service,
    ResolvedLongTrendEvaluationRequest,
)
from backend.services.quantevolver.long_trend_snapshot_resolver import (
    QELongTrendSnapshotResolver,
    ResolvedDatasetSnapshot,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient, QEWorkspaceDatasetIdentity
from backend.services.quantevolver.results_only_retry import (
    ResultsOnlyGateError,
    load_authoritative_recorder_ref,
)


class QELongTrendAPIServiceError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class LongTrendCreateRequest:
    profile_id: str
    outcome_dataset_snapshot_id: str


class QELongTrendAPIService:
    def __init__(
        self,
        *,
        connection_provider: Callable[[], Any] | None = None,
        snapshot_resolver: QELongTrendSnapshotResolver | None = None,
        result_repository: QELongTrendEvaluationResultRepository | None = None,
        artifact_store: QELongTrendArtifactStore | None = None,
        phase2_service: QELongTrendPhase2Service | None = None,
    ) -> None:
        self._connection_provider = connection_provider or get_conn
        self.snapshot_resolver = snapshot_resolver or (
            QELongTrendSnapshotResolver(connection_provider=connection_provider)
            if connection_provider is not None
            else QELongTrendSnapshotResolver()
        )
        self.result_repository = result_repository or (
            QELongTrendEvaluationResultRepository(connection_provider=connection_provider)
            if connection_provider is not None
            else QELongTrendEvaluationResultRepository()
        )
        self.artifact_store = artifact_store or QELongTrendArtifactStore()
        self.phase2_service = phase2_service or QELongTrendPhase2Service(
            result_repository=self.result_repository
        )

    async def create_or_update(
        self,
        *,
        task_id: str,
        loop_index: int,
        request: LongTrendCreateRequest,
    ) -> dict[str, Any]:
        if request.profile_id != PROFILE_ID_V1:
            raise QELongTrendAPIServiceError(
                f"unsupported long-trend profile: {request.profile_id}",
                reason_code="QELT_PROFILE_INVALID",
            )
        context = await asyncio.to_thread(
            self._load_loop_context,
            task_id=task_id,
            loop_index=loop_index,
        )
        existing_candidates = await asyncio.to_thread(
            self.result_repository.find_materializable_candidates,
            run_id=str(context["run_id"]),
            task_id=task_id,
            loop_index=loop_index,
            profile_id=request.profile_id,
            outcome_dataset_snapshot_id=request.outcome_dataset_snapshot_id,
        )
        if len(existing_candidates) > 1:
            raise QELongTrendAPIServiceError(
                "multiple terminal CAS evaluations match the requested QE Loop identity",
                reason_code="QELT_CONTROL_STATE_CONFLICT",
                context={
                    "task_id": task_id,
                    "loop_index": loop_index,
                    "matches": [str(item["evaluation_id"]) for item in existing_candidates],
                },
            )
        if existing_candidates:
            evaluation_id = str(existing_candidates[0]["evaluation_id"])
            persisted = await asyncio.to_thread(self._materialize_existing, evaluation_id)
            row = dict(persisted.control_row)
            return {
                "evaluation_id": evaluation_id,
                "status": row.get("status"),
                "run_id": row.get("run_id"),
                "task_id": task_id,
                "loop_index": loop_index,
                "ready_for_node": False,
                "family_status": row.get("family_status_json") or {},
                "platform_delivery_status": row.get("platform_delivery_status_json") or {},
                "data_action_plan": row.get("data_action_plan_json") or [],
                "reason_code": row.get("reason_code"),
            }
        node_id = str(context["node_id"])
        async with QEWorkspaceClient.for_node(node_id) as client:
            archived_feature_snapshot_id = str(
                context.get("feature_snapshot_id") or context.get("dataset_snapshot_id") or ""
            ).strip()
            feature = (
                await self.snapshot_resolver.resolve_requested_snapshot(
                    node_id=node_id,
                    requested_snapshot_id=archived_feature_snapshot_id,
                    client=client,
                    snapshot_role="feature",
                )
                if archived_feature_snapshot_id
                else self.snapshot_resolver.unresolved_archived_feature(node_id=node_id)
            )
            outcome = await self.snapshot_resolver.resolve_requested_snapshot(
                node_id=node_id,
                requested_snapshot_id=request.outcome_dataset_snapshot_id,
                client=client,
            )
            resolved = ResolvedLongTrendEvaluationRequest(
                profile_id=request.profile_id,
                evaluator_version=EVALUATOR_VERSION,
                feature_data_root_uri=feature.root_uri or "",
                outcome_data_root_uri=outcome.root_uri,
                backtest_freq=_backtest_freq(context.get("config_json")),
                requested_outcome_snapshot_id=request.outcome_dataset_snapshot_id,
                feature_identity=feature.identity or _missing_snapshot_identity(
                    reason_code=str(feature.data_action.get("reason_code"))
                    if feature.data_action is not None
                    else "QELT_ARCHIVED_FEATURE_SNAPSHOT_ID_UNAVAILABLE",
                    role="feature",
                ),
                outcome_identity=outcome.identity,
                data_actions=tuple(
                    dict(item)
                    for item in (feature.data_action, outcome.data_action)
                    if item is not None
                ),
            )
            prepared = await self.phase2_service.prepare_long_trend_only_resolved(
                run_id=str(context["run_id"]),
                task_id=task_id,
                loop_index=loop_index,
                node_id=node_id,
                resolved_request=resolved,
                registration_catalog={},
                label_horizon=_label_horizon(context),
                strategy_topk=_strategy_topk(context.get("config_json")),
                client=client,
            )
            row = (
                await asyncio.to_thread(
                    self.phase2_service.control_repository.get,
                    prepared.evaluation_id,
                )
            ) or prepared.control_row
            if (
                str(row.get("status") or "") in {"succeeded", "partial", "failed", "cancelled"}
                and row.get("artifact_manifest_sha256")
            ):
                persisted = await asyncio.to_thread(self._materialize_existing, prepared.evaluation_id)
                row = dict(persisted.control_row)
            elif prepared.ready_for_node:
                await self.phase2_service.submit(
                    prepared=prepared,
                    task_id=task_id,
                    loop_index=loop_index,
                    client=client,
                )
                row = (
                    await asyncio.to_thread(
                        self.phase2_service.control_repository.get,
                        prepared.evaluation_id,
                    )
                ) or row
        current_actions = _merge_data_actions(
            row.get("data_action_plan_json"),
            prepared.data_action_plan,
        )
        return {
            "evaluation_id": prepared.evaluation_id,
            "status": row.get("status"),
            "run_id": row.get("run_id"),
            "task_id": task_id,
            "loop_index": loop_index,
            "ready_for_node": prepared.ready_for_node,
            "family_status": row.get("family_status_json") or {},
            "platform_delivery_status": row.get("platform_delivery_status_json") or {},
            "data_action_plan": current_actions,
            "reason_code": row.get("reason_code"),
        }

    async def preview_inputs(
        self,
        *,
        task_id: str,
        loop_index: int,
        request: LongTrendCreateRequest,
    ) -> dict[str, Any]:
        """Inspect historical F-014 inputs without creating any platform state."""

        try:
            profile = get_long_trend_profile(request.profile_id)
        except QELongTrendError as exc:
            raise QELongTrendAPIServiceError(
                "unsupported long-trend profile",
                reason_code=exc.reason_code,
                context=exc.context,
            ) from exc
        context = await asyncio.to_thread(
            self._load_loop_context,
            task_id=task_id,
            loop_index=loop_index,
        )
        node_id = str(context["node_id"])
        loop_id = f"Loop{int(loop_index)}"
        data_actions: list[dict[str, Any]] = []
        warnings: list[str] = []
        artifact_rows: list[dict[str, Any]] = []
        inventory_ready = False

        async with QEWorkspaceClient.for_node(node_id) as client:
            archived_feature_snapshot_id = str(
                context.get("feature_snapshot_id") or context.get("dataset_snapshot_id") or ""
            ).strip()
            feature = (
                await self.snapshot_resolver.resolve_requested_snapshot(
                    node_id=node_id,
                    requested_snapshot_id=archived_feature_snapshot_id,
                    client=client,
                    snapshot_role="feature",
                )
                if archived_feature_snapshot_id
                else self.snapshot_resolver.unresolved_archived_feature(node_id=node_id)
            )
            outcome = await self.snapshot_resolver.resolve_requested_snapshot(
                node_id=node_id,
                requested_snapshot_id=request.outcome_dataset_snapshot_id,
                client=client,
                snapshot_role="outcome",
            )
            for item in (feature.data_action, outcome.data_action):
                if item is not None:
                    data_actions.append(dict(item))

            backtest_freq = _backtest_freq(context.get("config_json"))
            if not backtest_freq:
                reason_code = QELongTrendReason.PROFILE_UNSUPPORTED_RUN_MODE.value
                warnings.append(reason_code)
                artifact_rows = _unavailable_artifact_rows(
                    reason_code=reason_code,
                    action="archive_authoritative_backtest_frequency",
                )
                data_actions.append(
                    {
                        "action": "archive_authoritative_backtest_frequency",
                        "reason_code": reason_code,
                        "input_name": "backtest_freq",
                        "task_id": task_id,
                        "loop_index": int(loop_index),
                    }
                )
            else:
                try:
                    recorder_ref = await load_authoritative_recorder_ref(
                        client,
                        task_id=task_id,
                        loop_id=loop_id,
                        node_id=node_id,
                    )
                    catalog = await client.list_workspace_files(task_id, loop_id)
                    inventory = resolve_long_trend_recorder_artifacts(
                        task_id=task_id,
                        loop_id=loop_id,
                        recorder_ref=recorder_ref,
                        catalog=catalog,
                        backtest_freq=backtest_freq,
                    )
                    inventory_ready = True
                    warnings.extend(inventory.warnings)
                    # Preview has a stable input contract: a missing optional
                    # artifact must remain visible instead of disappearing
                    # from the response when the resolver has no catalog row.
                    for name in _PREVIEW_ARTIFACT_NAMES:
                        artifact = inventory.artifacts.get(name)
                        available = artifact is not None
                        reason_code = None if available else _artifact_missing_reason(name)
                        artifact_rows.append(
                            {
                                "input_name": name,
                                "category": "recorder_artifact",
                                "available": available,
                                "reason_code": reason_code,
                                "artifact": _public_artifact_summary(artifact),
                            }
                        )
                        if not available:
                            data_actions.append(
                                {
                                    "action": "archive_missing_qe_recorder_artifact",
                                    "reason_code": reason_code,
                                    "input_name": name,
                                    "task_id": task_id,
                                    "loop_index": int(loop_index),
                                }
                            )
                except ResultsOnlyGateError as exc:
                    reason_code = QELongTrendReason.RECORDER_REF_MISSING.value
                    artifact_rows = _unavailable_artifact_rows(
                        reason_code=reason_code,
                        action="restore_authoritative_qe_recorder_reference",
                    )
                    warnings.append(reason_code)
                    data_actions.append(
                        {
                            "action": "restore_authoritative_qe_recorder_reference",
                            "reason_code": reason_code,
                            "source_reason_code": exc.reason_code,
                            "input_name": "recorder_ref",
                            "task_id": task_id,
                            "loop_index": int(loop_index),
                        }
                    )
                except QELongTrendError as exc:
                    if exc.reason_code == QELongTrendReason.NON_QE_SOURCE_REJECTED.value:
                        raise QELongTrendAPIServiceError(
                            "workspace catalog does not match the authoritative QE identity",
                            reason_code=exc.reason_code,
                            context=exc.context,
                        ) from exc
                    artifact_rows = _unavailable_artifact_rows(
                        reason_code=exc.reason_code,
                        action="repair_qe_workspace_catalog",
                    )
                    warnings.append(exc.reason_code)
                    data_actions.append(
                        {
                            "action": "repair_qe_workspace_catalog",
                            "reason_code": exc.reason_code,
                            "input_name": "workspace_catalog",
                            "task_id": task_id,
                            "loop_index": int(loop_index),
                        }
                    )
                except Exception as exc:
                    reason_code = QELongTrendReason.WORKSPACE_CATALOG_PARTIAL.value
                    artifact_rows = _unavailable_artifact_rows(
                        reason_code=reason_code,
                        action="restore_qe_workspace_catalog",
                    )
                    warnings.append(reason_code)
                    data_actions.append(
                        {
                            "action": "restore_qe_workspace_catalog",
                            "reason_code": reason_code,
                            "input_name": "workspace_catalog",
                            "task_id": task_id,
                            "loop_index": int(loop_index),
                            "error_type": type(exc).__name__,
                        }
                    )

        dataset_rows = [
            _dataset_preview_row("feature_dataset_identity", feature),
            _dataset_preview_row("outcome_dataset_identity", outcome),
        ]
        return {
            "schema_version": "qe_long_trend_input_preview_v1",
            "task_id": task_id,
            "loop_index": int(loop_index),
            "run_id": str(context["run_id"]),
            "node_id": node_id,
            "profile": {
                "profile_id": profile.profile_id,
                "profile_sha256": profile.profile_sha256,
                "horizons": list(profile.horizons),
                "barriers": list(profile.barriers),
                "calendar_slices": list(profile.calendar_slices),
            },
            "requested_outcome_dataset_snapshot_id": request.outcome_dataset_snapshot_id,
            "backtest_freq": _backtest_freq(context.get("config_json")),
            "dataset_inputs": dataset_rows,
            "artifact_inputs": artifact_rows,
            "ready_for_node": feature.resolved and outcome.resolved and inventory_ready,
            "technical_readiness_only": True,
            "research_gate": False,
            "data_action_plan": _dedupe_preview_actions(data_actions),
            "warnings": sorted(set(warnings)),
            "side_effects": {
                "training_started": False,
                "backtest_started": False,
                "evaluation_created": False,
                "control_row_created": False,
                "worker_submitted": False,
                "cas_written": False,
                "database_written": False,
            },
        }

    def materialize_existing(self, evaluation_id: str) -> dict[str, Any]:
        persisted = self._materialize_existing(evaluation_id)
        return {
            "evaluation_id": persisted.evaluation_id,
            "metric_count": persisted.metric_count,
            "artifact_count": persisted.artifact_count,
            "replayed": persisted.replayed,
            "platform_delivery_status": persisted.control_row.get("platform_delivery_status_json") or {},
        }

    def _materialize_existing(self, evaluation_id: str):
        manifest = self.artifact_store.load_manifest(evaluation_id)
        worker_terminal = self.artifact_store.load_json_artifact(
            evaluation_id=evaluation_id,
            artifact_type="worker_terminal_receipt",
        )
        _published, published_meta = self.artifact_store.load_published_compact_receipt(evaluation_id)
        return self.result_repository.persist_published_receipt(
            evaluation_id=evaluation_id,
            worker_terminal=worker_terminal,
            manifest=manifest,
            published_meta=published_meta,
            lease=None,
        )

    def _load_loop_context(self, *, task_id: str, loop_index: int) -> dict[str, Any]:
        try:
            return self._load_loop_context_from_db(task_id=task_id, loop_index=loop_index)
        except QELongTrendAPIServiceError:
            raise
        except Exception as exc:
            raise QELongTrendAPIServiceError(
                "cannot load authoritative QE task/Loop context",
                reason_code="QELT_RESULT_PERSISTENCE_UNAVAILABLE",
                context={"task_id": task_id, "loop_index": loop_index},
            ) from exc

    def _load_loop_context_from_db(self, *, task_id: str, loop_index: int) -> dict[str, Any]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT t.task_id, t.node_id AS task_node_id, t.label_horizon AS task_label_horizon,
                           l.loop_id, l.loop_index, l.node_id AS loop_node_id,
                           l.status AS loop_status, l.config_json
                    FROM qe_evolution_tasks t
                    JOIN qe_evolution_loops l ON l.task_id = t.task_id
                    WHERE t.task_id = %s AND l.loop_index = %s
                    """,
                    (task_id, int(loop_index)),
                )
                loop = cur.fetchone()
                if loop is None:
                    raise QELongTrendAPIServiceError(
                        "QE task/Loop does not exist",
                        reason_code="QELT_NON_QE_SOURCE_REJECTED",
                        context={"task_id": task_id, "loop_index": loop_index},
                    )
                cur.execute(
                    """
                    SELECT r.run_id, r.node_id, r.source_system, r.run_type, r.model_type, r.label_horizon,
                           dc.dataset_snapshot_id, dc.feature_snapshot_id
                    FROM qe_archive.run r
                    LEFT JOIN LATERAL (
                        SELECT context.dataset_snapshot_id, context.feature_snapshot_id
                        FROM qe_archive.run_data_context context
                        WHERE context.run_id = r.run_id
                        ORDER BY (context.context_type = 'primary') DESC, context.created_at DESC, context.id DESC
                        LIMIT 1
                    ) dc ON TRUE
                    WHERE r.task_id = %s AND r.loop_index = %s
                      AND r.source_system = ANY(%s) AND r.run_type = ANY(%s)
                    ORDER BY r.is_latest_attempt DESC, r.archived_at DESC NULLS LAST, r.created_at DESC
                    LIMIT 1
                    """,
                    (task_id, int(loop_index), list(QE_SOURCE_SYSTEMS), list(QE_RUN_TYPES)),
                )
                archive_run = cur.fetchone()
        if archive_run is None:
            raise QELongTrendAPIServiceError(
                "F-014 long_trend_only requires an archived QE run identity",
                reason_code="QELT_ARCHIVE_RUN_UNAVAILABLE",
                context={"task_id": task_id, "loop_index": loop_index},
            )
        node_values = {
            source: str(value).strip()
            for source, value in {
                "loop": loop.get("loop_node_id"),
                "task": loop.get("task_node_id"),
                "archive_run": archive_run.get("node_id"),
            }.items()
            if str(value or "").strip()
        }
        distinct_nodes = set(node_values.values())
        if len(distinct_nodes) > 1:
            raise QELongTrendAPIServiceError(
                "QE task/Loop and archived run disagree on the authoritative compute node",
                reason_code="QELT_NODE_IDENTITY_CONFLICT",
                context={"node_bindings": node_values},
            )
        node_id = next(iter(distinct_nodes), "")
        if not node_id:
            raise QELongTrendAPIServiceError(
                "QE task/Loop has no authoritative compute node",
                reason_code="QELT_NODE_IDENTITY_UNAVAILABLE",
            )
        return {**dict(loop), **dict(archive_run), "node_id": node_id}


def _backtest_freq(config: Any) -> str | None:
    if not isinstance(config, Mapping):
        return None
    value = config.get("backtest_freq") or config.get("freq")
    return str(value).strip() if value else None


_PREVIEW_ARTIFACT_NAMES = (
    "prediction",
    "label",
    "params",
    "portfolio_report",
    "positions",
    "indicator_summary",
    "indicator_object",
    "orders",
    "trades",
)


def _artifact_missing_reason(name: str) -> str:
    return {
        "prediction": QELongTrendReason.PREDICTION_ARTIFACT_MISSING.value,
        "positions": QELongTrendReason.POSITION_ARTIFACT_MISSING.value,
        "indicator_object": QELongTrendReason.INDICATOR_OBJECT_MISSING.value,
    }.get(name, f"QELT_{name.upper()}_ARTIFACT_MISSING")


def _public_artifact_summary(artifact: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if artifact is None:
        return None
    return {
        key: artifact.get(key)
        for key in ("sha256", "size_bytes", "parser_contract")
        if artifact.get(key) is not None
    }


def _unavailable_artifact_rows(*, reason_code: str, action: str) -> list[dict[str, Any]]:
    return [
        {
            "input_name": name,
            "category": "recorder_artifact",
            "available": False,
            "reason_code": reason_code,
            "artifact": None,
            "action": action,
        }
        for name in _PREVIEW_ARTIFACT_NAMES
    ]


def _dataset_preview_row(
    input_name: str,
    resolved: ResolvedDatasetSnapshot,
) -> dict[str, Any]:
    identity = resolved.identity
    snapshot = identity.long_trend_snapshot if identity is not None else None
    return {
        "input_name": input_name,
        "category": "dataset_identity",
        "available": resolved.resolved,
        "requested_snapshot_id": resolved.requested_snapshot_id or None,
        "snapshot_id": snapshot.get("snapshot_id") if isinstance(snapshot, Mapping) else None,
        "manifest_sha256": snapshot.get("manifest_sha256") if isinstance(snapshot, Mapping) else None,
        "reason_code": (
            resolved.data_action.get("reason_code")
            if resolved.data_action is not None
            else None
        ),
        "data_action": dict(resolved.data_action) if resolved.data_action is not None else None,
    }


def _dedupe_preview_actions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if row not in result:
            result.append(dict(row))
    return result


def _label_horizon(context: Mapping[str, Any]) -> int | None:
    config = context.get("config_json")
    value = config.get("label_horizon") if isinstance(config, Mapping) else None
    if value is None:
        value = context.get("label_horizon")
    if value is None:
        value = context.get("task_label_horizon")
    if value is None:
        return None
    try:
        return normalize_label_horizon(value, field_name="archived QE label_horizon")
    except ValueError as exc:
        raise QELongTrendAPIServiceError(
            "archived QE label_horizon is invalid",
            reason_code="QELT_PROFILE_INVALID",
        ) from exc


def _strategy_topk(config: Any) -> int | None:
    if not isinstance(config, Mapping):
        return None
    strategy = config.get("strategy_params")
    value = strategy.get("topk") if isinstance(strategy, Mapping) else None
    if value is None:
        value = config.get("topk")
    if value is None:
        value = config.get("strategy_topk")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise QELongTrendAPIServiceError(
            "strategy_topk must be an integer",
            reason_code="QELT_PROFILE_INVALID",
        )
    result = value
    if result < 1:
        raise QELongTrendAPIServiceError(
            "strategy_topk must be positive",
            reason_code="QELT_PROFILE_INVALID",
        )
    return result


def _missing_snapshot_identity(*, reason_code: str, role: str) -> QEWorkspaceDatasetIdentity:
    return QEWorkspaceDatasetIdentity(
        schema_version="qe_dataset_identity_evidence_v1",
        complete=False,
        reason_code=reason_code,
        missing=(f"{role}_dataset_snapshot_id",),
        acquisition_suggestions=("archive_feature_snapshot_identity_for_qe_run",),
        dataset=None,
        long_trend_snapshot=None,
        long_trend_snapshot_reason=reason_code,
        detail=f"archived QE run does not currently expose a resolvable {role} snapshot identity",
    )


def _merge_data_actions(stored: Any, current: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    rows = stored or []
    if not isinstance(rows, list) or any(not isinstance(item, Mapping) for item in rows):
        raise QELongTrendAPIServiceError(
            "long-trend control data_action_plan_json is invalid",
            reason_code="QELT_CONTROL_STATE_CONFLICT",
        )
    merged = [dict(item) for item in rows]
    for action in current:
        if action not in merged:
            merged.append(dict(action))
    return merged
