from __future__ import annotations

import json
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx
import pandas as pd

from backend.services.model_store import ModelStoreService
from backend.services.multi_alpha.combine_backtest import (
    REQUEST_SNAPSHOT_KEY,
    CombineBacktestRequest,
    MultiAlphaCombineBacktestError,
    apply_pred_backtest_overrides,
    build_prediction_only_legs,
    combine_legs,
    is_rank_fusion_scheme,
    ingest_enhanced_metrics,
    json_mapping,
    maybe_upload_combined_prediction,
    parse_request,
    per_window_weights_payload,
    prepare_pred_backtest_workspace,
    runtime_backtest_config,
    weights_payload,
    write_qlib_prediction,
)
from backend.services.multi_alpha.durable_models import (
    OwnershipToken,
    artifact_manifest_hash_for,
    make_attempt_id,
    make_remote_task_id,
    submission_intent_hash_for,
)
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder, MultiAlphaPanelError
from backend.services.multi_alpha.remote_dispatch import (
    WorkspaceArtifactSyncClient,
    _remote_paths,
    _remote_small_files,
    _remote_wsl_command,
    _resolve_l2_artifact_path,
    get_compute_node_info,
)
from backend.services.quantevolver.qe_active_execution_capacity import (
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionOutcome,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionSource,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceFileNotFound,
)
from backend.services.quantevolver.qe_execution_reservation import (
    make_qe_execution_reservation_id,
)


ARTIFACT_MANIFEST_SCHEMA = "multi_alpha_child_artifact_manifest_v1"
RESULT_MANIFEST_SCHEMA = "multi_alpha_child_result_manifest_v1"


class DurableExecutionAdapterError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class DurableChildNotComputable(DurableExecutionAdapterError):
    """The requested child formula has no computable result for frozen inputs."""


@dataclass(frozen=True)
class DurableChildMaterialization:
    run: Mapping[str, Any]
    child: Mapping[str, Any]
    attempt: Mapping[str, Any]
    request: CombineBacktestRequest
    prediction_frame: pd.DataFrame
    weights: Mapping[str, Any]
    per_window_weights: tuple[Mapping[str, Any], ...]
    workspace: Path


@dataclass(frozen=True)
class DurablePublishedArtifacts:
    workspace: Path
    prediction_path: Path
    artifact_manifest_path: Path
    artifact_manifest: Mapping[str, Any]


@dataclass(frozen=True)
class DurableSubmissionIntent:
    run_id: str
    child_id: str
    attempt_id: str
    attempt_no: int
    node_id: str
    qe_task_id: str
    qe_loop_id: str
    submission_intent_hash: str


@dataclass(frozen=True)
class DurableRemoteInspection:
    receipt: Any
    status: Mapping[str, Any]


@dataclass(frozen=True)
class DurableCollectedResult:
    metrics: Mapping[str, Any]
    result_manifest: Mapping[str, Any]
    result_manifest_path: Path


class QEWorkspacePredBacktestAdapter:
    """Full durable child adapter over the existing combine and QE primitives."""

    def __init__(
        self,
        *,
        repository: MultiAlphaDurableRepository | None = None,
        panel_builder: MultiAlphaPanelBuilder | None = None,
        prediction_loader: Any | None = None,
        label_loader: Any | None = None,
        model_store: ModelStoreService | None = None,
        workspace_root: str | Path | None = None,
        workspace_client_factory: Any = QEWorkspaceClient.for_node,
        node_resolver: Any = get_compute_node_info,
        artifact_client_factory: Any | None = None,
        submission_coordinator: QEWorkspaceSubmissionCoordinator | None = None,
    ) -> None:
        self._repository = repository or MultiAlphaDurableRepository()
        self._model_store = model_store or ModelStoreService()
        self._prediction_loader = prediction_loader
        self._panel_builder = panel_builder or MultiAlphaPanelBuilder(
            model_store=self._model_store,
            prediction_loader=prediction_loader,
            label_loader=label_loader,
        )
        self._workspace_root = Path(
            workspace_root
            or os.getenv("AISTOCK_MULTI_ALPHA_BACKTEST_ROOT")
            or "rdagent_assets/multi_alpha_combine_backtests"
        )
        self._workspace_client_factory = workspace_client_factory
        self._node_resolver = node_resolver
        self._artifact_client_factory = artifact_client_factory
        self._submission_coordinator = (
            submission_coordinator or QEWorkspaceSubmissionCoordinator()
        )

    def materialize_child_input(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> DurableChildMaterialization:
        run = self._required_row("run", run_id, self._repository.get_run(run_id))
        child = self._required_row("child", child_id, self._repository.get_child(child_id))
        attempt = self._repository.get_attempt(attempt_id)
        if attempt is None:
            expected_initial_attempt_id = make_attempt_id(child_id, 1)
            if attempt_id != expected_initial_attempt_id:
                raise DurableExecutionAdapterError(
                    "only the deterministic initial attempt identity may materialize before attempt persistence",
                    reason_code="multi_alpha_materialization_attempt_identity_invalid",
                    context={
                        "child_id": child_id,
                        "expected_attempt_id": expected_initial_attempt_id,
                        "actual_attempt_id": attempt_id,
                    },
                )
            attempt = {
                "attempt_id": attempt_id,
                "child_id": child_id,
                "attempt_no": 1,
                "retry_mode": "initial",
                "retry_of_attempt_id": None,
            }
        if str(child.get("run_id")) != run_id or str(attempt.get("child_id")) != child_id:
            raise DurableExecutionAdapterError(
                "durable child/attempt scope does not match the requested run",
                reason_code="multi_alpha_durable_scope_mismatch",
                context={"run_id": run_id, "child_id": child_id, "attempt_id": attempt_id},
            )
        input_manifest = json_mapping(
            child.get("input_manifest_json"),
            field_name="input_manifest_json",
        )
        expected_input_hash = artifact_manifest_hash_for(input_manifest)
        if expected_input_hash != str(child.get("input_manifest_hash") or ""):
            raise DurableExecutionAdapterError(
                "durable child input manifest hash does not match persisted content",
                reason_code="multi_alpha_child_input_manifest_mismatch",
                context={
                    "child_id": child_id,
                    "expected": expected_input_hash,
                    "actual": child.get("input_manifest_hash"),
                },
            )
        request = self._request_from_run(run)
        try:
            prediction_frame, weights, per_window_weights = self._materialize_prediction(
                child=child,
                request=request,
            )
        except MultiAlphaCombineBacktestError as exc:
            if exc.reason_code in {
                "scheme_not_computable",
                "prediction_window_empty",
                "seed_prediction_empty",
                "seed_ensemble_empty",
            }:
                raise DurableChildNotComputable(
                    str(exc),
                    reason_code=exc.reason_code,
                    context=exc.context,
                ) from exc
            raise
        except MultiAlphaPanelError as exc:
            if exc.reason_code in {
                "prediction_window_empty",
                "label_window_empty",
                "prediction_label_no_overlap",
                "panel_coverage_below_threshold",
            }:
                raise DurableChildNotComputable(
                    str(exc),
                    reason_code=exc.reason_code,
                    context=exc.context,
                ) from exc
            raise
        workspace = self._attempt_workspace(run_id, child_id, attempt_id)
        return DurableChildMaterialization(
            run=run,
            child=child,
            attempt=attempt,
            request=request,
            prediction_frame=prediction_frame,
            weights=weights,
            per_window_weights=per_window_weights,
            workspace=workspace,
        )

    def request_from_run(self, run: Mapping[str, Any]) -> CombineBacktestRequest:
        return self._request_from_run(run)

    def load_published_artifacts(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> DurablePublishedArtifacts:
        child = self._required_row("child", child_id, self._repository.get_child(child_id))
        attempt = self._required_row(
            "attempt",
            attempt_id,
            self._repository.get_attempt(attempt_id),
        )
        if str(child.get("run_id")) != run_id or str(attempt.get("child_id")) != child_id:
            raise DurableExecutionAdapterError(
                "published artifact scope does not match the durable hierarchy",
                reason_code="multi_alpha_durable_scope_mismatch",
            )
        workspace = self._attempt_workspace(run_id, child_id, attempt_id)
        manifest_path = workspace / "artifact_manifest.json"
        manifest = self._read_json_if_exists(manifest_path)
        if manifest is None:
            raise DurableExecutionAdapterError(
                "durable artifact manifest is missing",
                reason_code="multi_alpha_artifact_manifest_missing",
                context={"path": str(manifest_path)},
            )
        self._verify_published_manifest(
            manifest,
            workspace=workspace,
            expected_input_manifest_hash=str(child["input_manifest_hash"]),
        )
        return DurablePublishedArtifacts(
            workspace=workspace,
            prediction_path=workspace / "combined_prediction.pkl",
            artifact_manifest_path=manifest_path,
            artifact_manifest=manifest,
        )

    def load_collected_metrics(self, artifacts: DurablePublishedArtifacts) -> Mapping[str, Any]:
        result_path = artifacts.workspace / "qlib_results_enhanced.json"
        if not result_path.exists():
            raise DurableExecutionAdapterError(
                "durable enhanced result is missing",
                reason_code="multi_alpha_child_result_missing",
                context={"path": str(result_path)},
            )
        return ingest_enhanced_metrics(result_path)

    def load_materialization_metadata(
        self,
        artifacts: DurablePublishedArtifacts,
    ) -> Mapping[str, Any]:
        path = artifacts.workspace / "materialization.json"
        payload = self._read_json_if_exists(path)
        if payload is None:
            raise DurableExecutionAdapterError(
                "durable materialization metadata is missing",
                reason_code="multi_alpha_materialization_metadata_missing",
                context={"path": str(path)},
            )
        return payload

    def publish_artifacts(
        self,
        materialization: DurableChildMaterialization,
    ) -> DurablePublishedArtifacts:
        workspace = materialization.workspace
        manifest_path = workspace / "artifact_manifest.json"
        existing = self._read_json_if_exists(manifest_path)
        if existing is not None:
            self._verify_published_manifest(
                existing,
                workspace=workspace,
                expected_input_manifest_hash=str(
                    materialization.child["input_manifest_hash"]
                ),
            )
            return DurablePublishedArtifacts(
                workspace=workspace,
                prediction_path=workspace / "combined_prediction.pkl",
                artifact_manifest_path=manifest_path,
                artifact_manifest=existing,
            )

        workspace.parent.mkdir(parents=True, exist_ok=True)
        staging = workspace.parent / f".{workspace.name}.publish.{uuid.uuid4().hex}.tmp"
        if staging.exists():
            raise DurableExecutionAdapterError(
                "unexpected artifact staging path collision",
                reason_code="multi_alpha_artifact_staging_conflict",
                context={"staging": str(staging)},
            )
        staging.mkdir(parents=False)
        try:
            prediction_path = staging / "combined_prediction.pkl"
            write_qlib_prediction(materialization.prediction_frame, prediction_path)
            backtest_config = runtime_backtest_config(materialization.request)
            prepare_pred_backtest_workspace(
                workspace=staging,
                backtest_config=backtest_config,
            )
            apply_pred_backtest_overrides(
                workspace=staging,
                backtest_config=backtest_config,
            )
            materialization_payload = {
                "schema_version": "multi_alpha_child_materialization_v1",
                "run_id": materialization.run["id"],
                "child_id": materialization.child["child_id"],
                "attempt_id": materialization.attempt["attempt_id"],
                "child_key": materialization.child["child_key"],
                "child_kind": materialization.child["child_kind"],
                "weighting_scheme": materialization.child.get("weighting_scheme"),
                "dropped_leg_id": materialization.child.get("dropped_leg_id"),
                "weights": dict(materialization.weights),
                "per_window_weights": [dict(item) for item in materialization.per_window_weights],
                "input_manifest_hash": materialization.child["input_manifest_hash"],
            }
            self._atomic_write_json(staging / "materialization.json", materialization_payload)
            files = self._publish_staging_tree(staging=staging, workspace=workspace)
            manifest = {
                "schema_version": ARTIFACT_MANIFEST_SCHEMA,
                "run_id": materialization.run["id"],
                "child_id": materialization.child["child_id"],
                "attempt_id": materialization.attempt["attempt_id"],
                "input_manifest_hash": materialization.child["input_manifest_hash"],
                "prediction_file": "combined_prediction.pkl",
                "files": files,
            }
            manifest["manifest_hash"] = artifact_manifest_hash_for(manifest)
            self._atomic_write_json(manifest_path, manifest)
            self._verify_published_manifest(
                manifest,
                workspace=workspace,
                expected_input_manifest_hash=str(
                    materialization.child["input_manifest_hash"]
                ),
            )
            return DurablePublishedArtifacts(
                workspace=workspace,
                prediction_path=workspace / "combined_prediction.pkl",
                artifact_manifest_path=manifest_path,
                artifact_manifest=manifest,
            )
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def prepare_submission_intent(
        self,
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
        attempt: Mapping[str, Any],
        node_id: str,
    ) -> DurableSubmissionIntent:
        attempt_no = int(attempt.get("attempt_no") or 0)
        qe_task_id = make_remote_task_id(
            str(run["id"]),
            str(child["child_id"]),
            attempt_no,
        )
        qe_loop_id = "Loop1"
        submission_hash = submission_intent_hash_for(
            child_id=str(child["child_id"]),
            attempt_no=attempt_no,
            retry_mode=str(attempt["retry_mode"]),
            retry_of_attempt_id=attempt.get("retry_of_attempt_id"),
            node_id=node_id,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
        )
        persisted_identity = {
            "node_id": attempt.get("node_id"),
            "qe_task_id": attempt.get("qe_task_id"),
            "qe_loop_id": attempt.get("qe_loop_id"),
            "submission_intent_hash": attempt.get("submission_intent_hash"),
        }
        expected_identity = {
            "node_id": node_id,
            "qe_task_id": qe_task_id,
            "qe_loop_id": qe_loop_id,
            "submission_intent_hash": submission_hash,
        }
        conflicts = {
            key: {"expected": expected_identity[key], "actual": actual}
            for key, actual in persisted_identity.items()
            if actual not in (None, "") and str(actual) != str(expected_identity[key])
        }
        if conflicts:
            raise DurableExecutionAdapterError(
                "persisted remote submission identity does not match the deterministic attempt identity",
                reason_code="multi_alpha_submission_identity_conflict",
                context={"attempt_id": attempt.get("attempt_id"), "conflicts": conflicts},
            )
        return DurableSubmissionIntent(
            run_id=str(run["id"]),
            child_id=str(child["child_id"]),
            attempt_id=str(attempt["attempt_id"]),
            attempt_no=attempt_no,
            node_id=node_id,
            qe_task_id=qe_task_id,
            qe_loop_id=qe_loop_id,
            submission_intent_hash=submission_hash,
        )

    async def submit(
        self,
        *,
        artifacts: DurablePublishedArtifacts,
        intent: DurableSubmissionIntent,
        attempt_token: OwnershipToken,
    ) -> QEWorkspaceSubmissionOutcome:
        run = self._required_row("run", intent.run_id, self._repository.get_run(intent.run_id))
        child = self._required_row(
            "child",
            intent.child_id,
            self._repository.get_child(intent.child_id),
        )
        attempt = self._required_row(
            "attempt",
            intent.attempt_id,
            self._repository.get_attempt(intent.attempt_id),
        )
        if (
            str(child.get("run_id")) != intent.run_id
            or str(attempt.get("child_id")) != intent.child_id
        ):
            raise DurableExecutionAdapterError(
                "submission intent does not belong to the persisted durable hierarchy",
                reason_code="multi_alpha_submission_intent_scope_mismatch",
            )
        request = self._request_from_run(run)
        node = self._node_resolver(intent.node_id)
        artifact_client = (
            self._artifact_client_factory(intent.node_id)
            if self._artifact_client_factory is not None
            else WorkspaceArtifactSyncClient.for_node(intent.node_id)
        )
        l2_path = _resolve_l2_artifact_path(
            workspace=artifacts.workspace,
            backtest_config=request.backtest_config,
        )
        l2_manifest = artifact_client.ensure_artifact(l2_path, node_id=intent.node_id)
        prediction_manifest = artifact_client.ensure_artifact(
            artifacts.prediction_path,
            node_id=intent.node_id,
        )
        remote_paths = _remote_paths(
            node=node,
            backtest_config=request.backtest_config,
            artifact_sha256=str(l2_manifest["sha256"]),
            artifact_manifest=l2_manifest,
            prediction_artifact_sha256=str(prediction_manifest["sha256"]),
        )
        wsl_command = _remote_wsl_command(
            workspace=artifacts.workspace,
            remote_paths=remote_paths,
            backtest_config=request.backtest_config,
        )
        experiment_files = _remote_small_files(
            workspace=artifacts.workspace,
            pred_pkl=artifacts.prediction_path,
            include_prediction=False,
        )

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            return self._repository.claim_attempt_submission_in_transaction(
                cur,
                attempt_id=intent.attempt_id,
                token=attempt_token,
                node_id=intent.node_id,
                qe_task_id=intent.qe_task_id,
                qe_loop_id=intent.qe_loop_id,
                submission_intent_hash=intent.submission_intent_hash,
                artifact_manifest=artifacts.artifact_manifest,
            )

        def record_waiting(
            cur: Any,
            active_count: int,
            node_capacity: int,
        ) -> Mapping[str, Any] | None:
            return self._repository.record_attempt_waiting_capacity_in_transaction(
                cur,
                attempt_id=intent.attempt_id,
                token=attempt_token,
                node_id=intent.node_id,
                active_count=active_count,
                node_capacity=node_capacity,
            )

        node_parallelism = json_mapping(
            run.get("node_parallelism_json"),
            field_name="node_parallelism_json",
        )
        requested_capacity = node_parallelism.get(intent.node_id)
        source = QEWorkspaceSubmissionSource(
            source_kind="multi_alpha_durable_attempt",
            source_execution_id=intent.attempt_id,
            node_id=intent.node_id,
            submission_intent_hash=intent.submission_intent_hash,
            owner_id=attempt_token.owner_id,
            claim_source=claim_source,
            record_waiting_capacity=record_waiting,
            requested_node_capacity=requested_capacity,
        )
        payload = QEWorkspaceSubmissionPayload(
            task_id=intent.qe_task_id,
            loop_index=1,
            config={
                "source": "multi_alpha_durable_pred_backtest_v1",
                "run_id": intent.run_id,
                "child_id": intent.child_id,
                "attempt_id": intent.attempt_id,
                "artifact_manifest": dict(artifacts.artifact_manifest),
                "l2_artifact_manifest": dict(l2_manifest),
                "prediction_artifact_manifest": dict(prediction_manifest),
                "remote_paths": remote_paths,
            },
            experiment_files=experiment_files,
            wsl_command=wsl_command,
        )
        client = self._workspace_client_factory(intent.node_id)
        async with client:
            return await self._submission_coordinator.submit(
                client=client,
                source=source,
                payload=payload,
            )

    async def inspect_remote(
        self,
        *,
        intent: DurableSubmissionIntent,
    ) -> DurableRemoteInspection:
        client = self._workspace_client_factory(intent.node_id)
        async with client:
            receipt = await client.inspect_loop_submission(
                intent.qe_task_id,
                intent.qe_loop_id,
                submission_intent_hash=intent.submission_intent_hash,
            )
            if receipt.status == "not_reserved":
                return DurableRemoteInspection(
                    receipt=receipt,
                    status={"status": "not_reserved"},
                )
            if receipt.submission_intent_hash != intent.submission_intent_hash:
                raise DurableExecutionAdapterError(
                    "QE Workspace receipt belongs to a different durable attempt intent",
                    reason_code="multi_alpha_submission_identity_conflict",
                    context={
                        "task_id": intent.qe_task_id,
                        "loop_id": intent.qe_loop_id,
                        "expected_submission_intent_hash": intent.submission_intent_hash,
                        "actual_submission_intent_hash": receipt.submission_intent_hash,
                    },
                )
            try:
                status = await client.get_loop_status(intent.qe_task_id, intent.qe_loop_id)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    raise
                status = {
                    "status": "reserved_not_started",
                    "receipt_status": receipt.status,
                }
        if not isinstance(status, Mapping) or not str(status.get("status") or "").strip():
            raise DurableExecutionAdapterError(
                "QE Workspace returned an invalid loop status payload",
                reason_code="qe_workspace_loop_status_invalid",
                context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
            )
        return DurableRemoteInspection(receipt=receipt, status=dict(status))

    def record_remote_terminal(
        self,
        *,
        intent: DurableSubmissionIntent,
        owner_id: str,
        remote_status: str,
    ) -> Mapping[str, Any]:
        def unused_claim(_cur: Any) -> Mapping[str, Any] | None:
            raise DurableExecutionAdapterError(
                "terminal reservation reconciliation must not reclaim the source attempt",
                reason_code="multi_alpha_terminal_source_claim_unexpected",
            )

        source = QEWorkspaceSubmissionSource(
            source_kind="multi_alpha_durable_attempt",
            source_execution_id=intent.attempt_id,
            node_id=intent.node_id,
            submission_intent_hash=intent.submission_intent_hash,
            owner_id=owner_id,
            claim_source=unused_claim,
            record_waiting_capacity=lambda _cur, _active, _limit: unused_claim(_cur),
        )
        outcome = QEWorkspaceSubmissionOutcome(
            state="receipt_recovered",
            task_id=intent.qe_task_id,
            loop_id=intent.qe_loop_id,
            reservation_id=make_qe_execution_reservation_id(
                "multi_alpha_durable_attempt",
                intent.attempt_id,
            ),
            reservation_status="reconciling",
            remote_status=remote_status,
            active_count=0,
            node_capacity=0,
            duplicate_replay=True,
            remote_acceptance_unknown=False,
        )
        return self._submission_coordinator.record_authoritative_remote_status(
            source=source,
            outcome=outcome,
            remote_status=remote_status,
        )

    async def collect_result(
        self,
        *,
        intent: DurableSubmissionIntent,
        artifacts: DurablePublishedArtifacts,
        execution_deadline_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> DurableCollectedResult:
        client = self._workspace_client_factory(intent.node_id)
        try:
            async with client:
                raw = await client.get_workspace_file(
                    intent.qe_task_id,
                    intent.qe_loop_id,
                    "qlib_results_enhanced.json",
                )
        except QEWorkspaceFileNotFound as exc:
            raise DurableExecutionAdapterError(
                "QE Workspace completed result is not visible yet",
                reason_code="multi_alpha_child_result_not_visible",
                context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise DurableExecutionAdapterError(
                    "QE Workspace completed result is not visible yet",
                    reason_code="multi_alpha_child_result_not_visible",
                    context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
                ) from exc
            raise DurableExecutionAdapterError(
                "QE Workspace result transport returned an HTTP failure",
                reason_code="qe_workspace_result_transport_unavailable",
                context={
                    "task_id": intent.qe_task_id,
                    "loop_id": intent.qe_loop_id,
                    "status_code": exc.response.status_code,
                },
            ) from exc
        except httpx.HTTPError as exc:
            raise DurableExecutionAdapterError(
                "QE Workspace result transport is temporarily unavailable",
                reason_code="qe_workspace_result_transport_unavailable",
                context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
            ) from exc
        except RuntimeError as exc:
            cause = exc.__cause__
            if isinstance(cause, httpx.HTTPStatusError) and cause.response.status_code == 404:
                raise DurableExecutionAdapterError(
                    "QE Workspace completed result is not visible yet",
                    reason_code="multi_alpha_child_result_not_visible",
                    context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
                ) from exc
            if isinstance(cause, httpx.HTTPError):
                raise DurableExecutionAdapterError(
                    "QE Workspace result transport is temporarily unavailable",
                    reason_code="qe_workspace_result_transport_unavailable",
                    context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
                ) from exc
            raise
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DurableExecutionAdapterError(
                    "QE Workspace result is not valid JSON",
                    reason_code="multi_alpha_child_result_invalid",
                    context={"task_id": intent.qe_task_id, "loop_id": intent.qe_loop_id},
                ) from exc
        if not isinstance(raw, Mapping) or not raw:
            raise DurableExecutionAdapterError(
                "QE Workspace result must be a non-empty JSON object",
                reason_code="multi_alpha_child_result_invalid",
                context={
                    "task_id": intent.qe_task_id,
                    "loop_id": intent.qe_loop_id,
                    "result_type": type(raw).__name__,
                },
            )
        raw_metrics = dict(raw)
        raw_result_path = artifacts.workspace / "qlib_results_enhanced.json"
        existing_raw = self._read_json_if_exists(raw_result_path)
        if existing_raw is not None and existing_raw != raw_metrics:
            raise DurableExecutionAdapterError(
                "durable enhanced result already exists with different content",
                reason_code="multi_alpha_child_result_conflict",
                context={"path": str(raw_result_path)},
            )
        if existing_raw is None:
            self._atomic_write_json(raw_result_path, raw_metrics)
        metrics = ingest_enhanced_metrics(raw_result_path)
        run = self._required_row("run", intent.run_id, self._repository.get_run(intent.run_id))
        child = self._required_row(
            "child",
            intent.child_id,
            self._repository.get_child(intent.child_id),
        )
        request = self._request_from_run(run)
        prediction_store_manifest = maybe_upload_combined_prediction(
            run_id=intent.run_id,
            backtest_name=str(child["child_key"]),
            pred_pkl=artifacts.prediction_path,
            node_id=intent.node_id,
            backtest_config=request.backtest_config,
        )
        metrics["prediction_store_manifest"] = prediction_store_manifest
        metrics["pred_persisted"] = prediction_store_manifest is not None
        metrics_hash = artifact_manifest_hash_for(metrics)
        deadline_evidence = {
            str(kind): dict(payload)
            for kind, payload in (execution_deadline_evidence or {}).items()
            if str(kind) in {"scheme", "run"} and isinstance(payload, Mapping)
        }
        if len(deadline_evidence) != len(execution_deadline_evidence or {}):
            raise DurableExecutionAdapterError(
                "execution deadline evidence must contain scheme/run objects",
                reason_code="multi_alpha_deadline_evidence_invalid",
                context={"attempt_id": intent.attempt_id},
            )
        result_manifest = {
            "schema_version": RESULT_MANIFEST_SCHEMA,
            "run_id": intent.run_id,
            "child_id": intent.child_id,
            "attempt_id": intent.attempt_id,
            "node_id": intent.node_id,
            "qe_task_id": intent.qe_task_id,
            "qe_loop_id": intent.qe_loop_id,
            "submission_intent_hash": intent.submission_intent_hash,
            "artifact_manifest_hash": artifacts.artifact_manifest["manifest_hash"],
            "raw_result_file": raw_result_path.name,
            "raw_result_hash": artifact_manifest_hash_for(raw_metrics),
            "metrics_hash": metrics_hash,
            "prediction_store_manifest": prediction_store_manifest,
            "completed_after_deadline": bool(deadline_evidence),
        }
        if deadline_evidence:
            result_manifest["execution_deadline"] = deadline_evidence
        result_manifest["manifest_hash"] = artifact_manifest_hash_for(result_manifest)
        result_path = artifacts.workspace / "result_manifest.json"
        existing = self._read_json_if_exists(result_path)
        if existing is not None and existing != result_manifest:
            raise DurableExecutionAdapterError(
                "durable result manifest already exists with different content",
                reason_code="multi_alpha_result_manifest_conflict",
                context={"path": str(result_path)},
            )
        if existing is None:
            self._atomic_write_json(result_path, result_manifest)
        return DurableCollectedResult(
            metrics=metrics,
            result_manifest=result_manifest,
            result_manifest_path=result_path,
        )

    def _materialize_prediction(
        self,
        *,
        child: Mapping[str, Any],
        request: CombineBacktestRequest,
    ) -> tuple[pd.DataFrame, Mapping[str, Any], tuple[Mapping[str, Any], ...]]:
        child_kind = str(child["child_kind"])
        scheme = str(child.get("weighting_scheme") or "")
        needs_panel_metrics = child_kind != "baseline" and not is_rank_fusion_scheme(scheme)
        if needs_panel_metrics:
            legs = self._panel_builder.build_combiner_legs(
                legs=request.roster,
                oos_start=request.oos_start,
                oos_end=request.oos_end,
                topk=request.topk,
                min_date_coverage=request.min_date_coverage,
            )
        else:
            legs = build_prediction_only_legs(
                request,
                prediction_loader=self._prediction_loader,
                model_store=self._model_store,
            )
        leg_by_id = {leg.leg_id: leg for leg in legs}
        if child_kind == "baseline":
            baseline_leg_id = str(request.baseline_leg_id or "")
            if baseline_leg_id not in leg_by_id:
                raise MultiAlphaCombineBacktestError(
                    "baseline leg is absent from the frozen roster",
                    reason_code="baseline_leg_missing",
                    leg_id=baseline_leg_id or None,
                )
            return leg_by_id[baseline_leg_id].pred_frame.copy(), {}, ()
        selected_legs = list(legs)
        if child_kind == "loo":
            dropped_leg_id = str(child.get("dropped_leg_id") or "")
            selected_legs = [leg for leg in selected_legs if leg.leg_id != dropped_leg_id]
        result = combine_legs(legs=selected_legs, scheme=scheme, request=request)
        frame = result.combined_score_frame.rename(columns={"combined_score": "score"})
        return (
            frame,
            weights_payload(result, scheme=scheme, request=request),
            tuple(per_window_weights_payload(result, scheme=scheme)),
        )

    @staticmethod
    def _request_from_run(run: Mapping[str, Any]) -> CombineBacktestRequest:
        backtest_config = json_mapping(
            run.get("backtest_config_json"),
            field_name="backtest_config_json",
        )
        snapshot = backtest_config.get(REQUEST_SNAPSHOT_KEY)
        if not isinstance(snapshot, Mapping):
            raise DurableExecutionAdapterError(
                "durable run is missing the exact frozen combine request snapshot",
                reason_code="multi_alpha_request_snapshot_missing",
                context={"run_id": run.get("id")},
            )
        return parse_request(dict(snapshot))

    def _attempt_workspace(self, run_id: str, child_id: str, attempt_id: str) -> Path:
        for field_name, value, prefix in (
            ("run_id", run_id, "macb_"),
            ("child_id", child_id, "macbc_"),
            ("attempt_id", attempt_id, "macba_"),
        ):
            if not str(value).startswith(prefix) or Path(str(value)).name != str(value):
                raise DurableExecutionAdapterError(
                    "durable workspace identity is invalid",
                    reason_code="multi_alpha_workspace_identity_invalid",
                    context={"field": field_name, "value": value},
                )
        return self._workspace_root / run_id / child_id / attempt_id

    @staticmethod
    def _required_row(kind: str, identity: str, row: Mapping[str, Any] | None) -> Mapping[str, Any]:
        if row is None:
            raise DurableExecutionAdapterError(
                f"durable {kind} does not exist",
                reason_code=f"multi_alpha_durable_{kind}_not_found",
                context={f"{kind}_id": identity},
            )
        return dict(row)

    def _publish_staging_tree(self, *, staging: Path, workspace: Path) -> dict[str, Any]:
        files: dict[str, Any] = {}
        for source in sorted(staging.rglob("*")):
            if source.is_dir():
                continue
            if not source.is_file():
                raise DurableExecutionAdapterError(
                    "artifact staging contains a non-regular file",
                    reason_code="multi_alpha_artifact_file_invalid",
                    context={"path": str(source)},
                )
            relative = source.relative_to(staging)
            destination = workspace / relative
            digest, size = self._sha256_file(source)
            if destination.exists():
                existing_digest, existing_size = self._sha256_file(destination)
                if (existing_digest, existing_size) != (digest, size):
                    raise DurableExecutionAdapterError(
                        "durable artifact path already contains different bytes",
                        reason_code="multi_alpha_artifact_publish_conflict",
                        context={"path": str(destination)},
                    )
            else:
                self._atomic_copy_file(source, destination)
            files[relative.as_posix()] = {"sha256": digest, "size": size}
        if "combined_prediction.pkl" not in files:
            raise DurableExecutionAdapterError(
                "published child artifacts are missing combined_prediction.pkl",
                reason_code="multi_alpha_prediction_artifact_missing",
            )
        return files

    def _verify_published_manifest(
        self,
        manifest: Mapping[str, Any],
        *,
        workspace: Path,
        expected_input_manifest_hash: str,
    ) -> None:
        if manifest.get("schema_version") != ARTIFACT_MANIFEST_SCHEMA:
            raise DurableExecutionAdapterError(
                "durable artifact manifest schema is unsupported",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        if manifest.get("input_manifest_hash") != expected_input_manifest_hash:
            raise DurableExecutionAdapterError(
                "durable artifact manifest belongs to different frozen inputs",
                reason_code="multi_alpha_artifact_manifest_conflict",
            )
        without_hash = dict(manifest)
        actual_manifest_hash = without_hash.pop("manifest_hash", None)
        expected_manifest_hash = artifact_manifest_hash_for(without_hash)
        if actual_manifest_hash != expected_manifest_hash:
            raise DurableExecutionAdapterError(
                "durable artifact manifest hash is invalid",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        files = manifest.get("files")
        if not isinstance(files, Mapping) or not files:
            raise DurableExecutionAdapterError(
                "durable artifact manifest has no file inventory",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        for relative_name, metadata in files.items():
            if not isinstance(metadata, Mapping):
                raise DurableExecutionAdapterError(
                    "durable artifact file metadata is invalid",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
            path = workspace / str(relative_name)
            digest, size = self._sha256_file(path)
            if digest != metadata.get("sha256") or size != int(metadata.get("size") or -1):
                raise DurableExecutionAdapterError(
                    "durable artifact bytes do not match the published manifest",
                    reason_code="multi_alpha_artifact_hash_mismatch",
                    context={"path": str(path)},
                )

    @staticmethod
    def _sha256_file(path: Path) -> tuple[str, int]:
        import hashlib

        digest = hashlib.sha256()
        size = 0
        try:
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    size += len(chunk)
                    digest.update(chunk)
        except OSError as exc:
            raise DurableExecutionAdapterError(
                "failed to read durable artifact",
                reason_code="multi_alpha_artifact_read_failed",
                context={"path": str(path), "message": str(exc)},
            ) from exc
        if size <= 0:
            raise DurableExecutionAdapterError(
                "durable artifact is empty",
                reason_code="multi_alpha_artifact_empty",
                context={"path": str(path)},
            )
        return digest.hexdigest(), size

    @staticmethod
    def _atomic_copy_file(source: Path, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.tmp"
        try:
            with source.open("rb") as reader, temp.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())
            os.replace(temp, destination)
            QEWorkspacePredBacktestAdapter._fsync_directory(destination.parent)
        finally:
            if temp.exists():
                temp.unlink()

    @staticmethod
    def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        temp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
        try:
            with temp.open("xb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
            QEWorkspacePredBacktestAdapter._fsync_directory(path.parent)
        finally:
            if temp.exists():
                temp.unlink()

    @staticmethod
    def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DurableExecutionAdapterError(
                "durable JSON artifact is unreadable",
                reason_code="multi_alpha_artifact_manifest_invalid",
                context={"path": str(path), "message": str(exc)},
            ) from exc
        if not isinstance(payload, dict):
            raise DurableExecutionAdapterError(
                "durable JSON artifact must contain an object",
                reason_code="multi_alpha_artifact_manifest_invalid",
                context={"path": str(path)},
            )
        return payload

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name == "nt":
            return
        descriptor = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
