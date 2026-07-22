"""QE-only orchestration for F-014 Phase 2 normal and historical entries."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from backend.services.quantevolver.experiment_config import LongTrendEvaluationOptIn
from backend.services.quantevolver.long_trend_artifact_resolver import (
    RecorderArtifactInventory,
    resolve_long_trend_recorder_artifacts,
)
from backend.services.quantevolver.long_trend_artifact_store import QELongTrendArtifactStore
from backend.services.quantevolver.long_trend_evaluation_bundle import (
    QELongTrendEvaluatorBundle,
    build_long_trend_evaluator_bundle,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    EVALUATOR_VERSION,
    QEDatasetSnapshotIdentity,
    QELongTrendReason,
    build_evaluation_id,
    canonical_input_manifest,
    canonical_sha256,
    get_long_trend_profile,
    typed_null,
)
from backend.services.quantevolver.long_trend_evaluation_control_repository import (
    QELongTrendEvaluationControlRepository,
    QELongTrendEvaluationControlSpec,
)
from backend.services.quantevolver.qe_dataset_contract import QE_DATASET_CONTRACT_ID
from backend.services.quantevolver.qe_resource_phase_service import QEResourcePhaseService
from backend.services.quantevolver.qe_workspace_client import (
    QELongTrendJobInspection,
    QELongTrendJobReceipt,
    QELongTrendWorkspaceError,
    QEWorkspaceClient,
    QEWorkspaceDatasetIdentity,
)
from backend.services.quantevolver.results_only_retry import ResultsOnlyGateError, load_authoritative_recorder_ref

CONTROL_SECRET_ROOT_ENV = "QE_LONG_TREND_CONTROL_SECRET_ROOT"
WORKER_INPUT_ARTIFACTS = frozenset(
    {
        "prediction",
        "label",
        "positions",
        "portfolio_report",
        "indicator_summary",
        "indicator_object",
        "orders",
        "trades",
    }
)


class QELongTrendPhase2Error(RuntimeError):
    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class PreparedLongTrendEvaluation:
    evaluation_id: str
    control_row: dict[str, Any]
    request_payload: dict[str, Any] | None
    resource_token: str
    ready_for_node: bool
    data_action_plan: tuple[dict[str, Any], ...]


class QELongTrendControlSecretStore:
    def __init__(self, root: str | Path | None = None) -> None:
        configured = str(os.getenv(CONTROL_SECRET_ROOT_ENV) or "").strip()
        self.root = Path(root or configured or (Path(__file__).resolve().parents[3] / "rdagent_assets" / "long_trend_evaluation_control_secrets"))

    def load_or_create(self, evaluation_id: str, *, session_id: str, source_run_key: str) -> tuple[str, bool]:
        path = self._path(evaluation_id)
        if path.is_file():
            payload = _read_json(path)
            if payload.get("session_id") != session_id or payload.get("source_run_key") != source_run_key:
                raise QELongTrendPhase2Error(
                    "persisted qelt resource secret belongs to a different control identity",
                    reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
                )
            token = str(payload.get("token") or "")
            if not token:
                raise QELongTrendPhase2Error(
                    "persisted qelt resource secret has no token",
                    reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
                )
            return token, False
        token = secrets.token_urlsafe(32)
        _atomic_json(
            path,
            {"evaluation_id": evaluation_id, "session_id": session_id, "source_run_key": source_run_key, "token": token},
            mode=0o600,
        )
        return token, True

    def _path(self, evaluation_id: str) -> Path:
        if not evaluation_id.startswith("qelt_") or "/" in evaluation_id or "\\" in evaluation_id:
            raise ValueError("invalid evaluation_id for control secret store")
        return self.root / f"{evaluation_id}.json"

    def load(self, evaluation_id: str, *, session_id: str, source_run_key: str) -> str:
        path = self._path(evaluation_id)
        if not path.is_file():
            raise QELongTrendPhase2Error(
                "durable qelt resource secret is missing during recovery",
                reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
            )
        payload = _read_json(path)
        if payload.get("session_id") != session_id or payload.get("source_run_key") != source_run_key:
            raise QELongTrendPhase2Error(
                "durable qelt resource secret identity mismatch during recovery",
                reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
            )
        token = str(payload.get("token") or "")
        if not token:
            raise QELongTrendPhase2Error(
                "durable qelt resource secret token is empty",
                reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
            )
        return token


class QELongTrendPhase2Service:
    def __init__(
        self,
        *,
        control_repository: QELongTrendEvaluationControlRepository | None = None,
        resource_service: QEResourcePhaseService | None = None,
        secret_store: QELongTrendControlSecretStore | None = None,
        owner_id: str | None = None,
        repo_root: str | Path | None = None,
    ) -> None:
        self.control_repository = control_repository or QELongTrendEvaluationControlRepository()
        self.resource_service = resource_service or QEResourcePhaseService()
        self.secret_store = secret_store or QELongTrendControlSecretStore()
        self.owner_id = str(owner_id or f"qelt_{os.getpid()}_{secrets.token_hex(6)}")
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()

    async def prepare_normal_postprocess(
        self,
        *,
        task_id: str,
        loop_index: int,
        node_id: str,
        opt_in: LongTrendEvaluationOptIn,
        registration_catalog: Mapping[str, Any],
        label_horizon: int | None,
        strategy_topk: int | None,
        client: QEWorkspaceClient,
        run_id: str | None = None,
        frozen_identity: Mapping[str, Any] | None = None,
        expected_recorder_ref: Mapping[str, Any] | None = None,
    ) -> PreparedLongTrendEvaluation:
        return await self._prepare(
            task_id=task_id,
            loop_index=loop_index,
            node_id=node_id,
            opt_in=opt_in,
            registration_catalog=registration_catalog,
            label_horizon=label_horizon,
            strategy_topk=strategy_topk,
            client=client,
            run_id=run_id,
            frozen_identity=frozen_identity,
            expected_recorder_ref=expected_recorder_ref,
        )

    async def prepare_long_trend_only(
        self,
        *,
        run_id: str,
        task_id: str,
        loop_index: int,
        node_id: str,
        opt_in: LongTrendEvaluationOptIn,
        registration_catalog: Mapping[str, Any],
        label_horizon: int | None,
        strategy_topk: int | None,
        client: QEWorkspaceClient,
        frozen_identity: Mapping[str, Any] | None = None,
        expected_recorder_ref: Mapping[str, Any] | None = None,
    ) -> PreparedLongTrendEvaluation:
        return await self._prepare(
            task_id=task_id,
            loop_index=loop_index,
            node_id=node_id,
            opt_in=opt_in,
            registration_catalog=registration_catalog,
            label_horizon=label_horizon,
            strategy_topk=strategy_topk,
            client=client,
            run_id=run_id,
            frozen_identity=frozen_identity,
            expected_recorder_ref=expected_recorder_ref,
        )

    async def _prepare(
        self,
        *,
        task_id: str,
        loop_index: int,
        node_id: str,
        opt_in: LongTrendEvaluationOptIn,
        registration_catalog: Mapping[str, Any],
        label_horizon: int | None,
        strategy_topk: int | None,
        client: QEWorkspaceClient,
        run_id: str | None,
        frozen_identity: Mapping[str, Any] | None,
        expected_recorder_ref: Mapping[str, Any] | None,
    ) -> PreparedLongTrendEvaluation:
        loop_id = f"Loop{int(loop_index)}"
        environment = await client.get_execution_environment()
        capability = environment.manifest.get("capabilities", {}).get("qe_long_trend_evaluation_v1")
        if not isinstance(capability, Mapping):
            raise QELongTrendPhase2Error(
                "QE node does not declare qe_long_trend_evaluation_v1",
                reason_code=QELongTrendReason.NODE_CAPABILITY_UNAVAILABLE.value,
            )
        bundle = build_long_trend_evaluator_bundle(
            repo_root=self.repo_root,
            execution_environment={
                "execution_environment_snapshot_id": environment.execution_environment_snapshot_id,
                "execution_environment_manifest_sha256": environment.execution_environment_manifest_sha256,
                "manifest": environment.manifest,
            },
        )
        try:
            recorder_ref = await load_authoritative_recorder_ref(
                client, task_id=task_id, loop_id=loop_id, node_id=node_id
            )
        except ResultsOnlyGateError as exc:
            raise QELongTrendPhase2Error(
                "authoritative QE Recorder reference is unavailable",
                reason_code=QELongTrendReason.RECORDER_REF_MISSING.value,
                context={
                    "source_reason_code": exc.reason_code,
                    "artifact": exc.artifact,
                    "details": exc.details,
                },
            ) from exc
        if expected_recorder_ref is not None:
            expected_pair = {
                "experiment_id": str(expected_recorder_ref.get("experiment_id") or ""),
                "recorder_id": str(expected_recorder_ref.get("recorder_id") or ""),
            }
            actual_pair = {
                "experiment_id": str(recorder_ref.get("experiment_id") or ""),
                "recorder_id": str(recorder_ref.get("recorder_id") or ""),
            }
            if expected_pair != actual_pair:
                raise QELongTrendPhase2Error(
                    "adapter recorder identity differs from authoritative node recorder ref",
                    reason_code=QELongTrendReason.RECORDER_REF_MISSING.value,
                    context={"expected": expected_pair, "actual": actual_pair},
                )
        live_catalog = await client.list_workspace_files(task_id, loop_id)
        merged_catalog = _merge_registration_catalog(live_catalog, registration_catalog)
        inventory = resolve_long_trend_recorder_artifacts(
            task_id=task_id,
            loop_id=loop_id,
            recorder_ref=recorder_ref,
            catalog=merged_catalog,
            backtest_freq=opt_in.backtest_freq,
        )
        feature_identity = await client.get_dataset_identity(node_id=node_id, data_root_uri=opt_in.feature_data_root_uri)
        outcome_identity = await client.get_dataset_identity(node_id=node_id, data_root_uri=opt_in.outcome_data_root_uri)
        if frozen_identity is not None:
            _require_frozen_identity(
                frozen_identity,
                bundle=bundle,
                environment_snapshot_id=environment.execution_environment_snapshot_id,
                environment_manifest_sha256=environment.execution_environment_manifest_sha256,
                feature_identity=feature_identity,
                outcome_identity=outcome_identity,
                profile_sha256=get_long_trend_profile(opt_in.profile_id).profile_sha256,
            )
        feature_snapshot, feature_action = _long_trend_snapshot(feature_identity, family="feature")
        outcome_snapshot, outcome_action = _long_trend_snapshot(outcome_identity, family="outcome")
        actions = tuple(item for item in (feature_action, outcome_action) if item is not None)
        profile = get_long_trend_profile(opt_in.profile_id)
        input_hashes = _input_artifact_hashes(inventory)
        input_hashes["catalog_digest"] = _recorder_catalog_digest(inventory)
        input_hashes["catalog_completeness"] = inventory.catalog_completeness
        input_manifest = canonical_input_manifest(input_hashes)
        input_manifest["evaluation_parameters"] = {
            "label_horizon": label_horizon if label_horizon is not None else typed_null("label_horizon"),
            "strategy_topk": strategy_topk if strategy_topk is not None else typed_null("strategy_topk"),
        }
        input_manifest_sha = canonical_sha256(input_manifest)
        parent_identity = _evaluation_parent_identity(task_id=task_id, loop_index=loop_index)
        evaluation_id = build_evaluation_id(
            run_id=parent_identity,
            profile_sha256=profile.profile_sha256,
            evaluator_source_sha256=bundle.evaluator_source_sha256,
            execution_environment_manifest_sha256=environment.execution_environment_manifest_sha256,
            feature_dataset_manifest_sha256=(feature_snapshot.manifest_sha256 if feature_snapshot else None),
            outcome_dataset_manifest_sha256=(outcome_snapshot.manifest_sha256 if outcome_snapshot else None),
            input_manifest_sha256=input_manifest_sha,
        )
        session_id = f"qers_qelt_{evaluation_id[5:29]}"
        source_run_key = f"qelt:{evaluation_id}"
        token, _created_secret = self.secret_store.load_or_create(
            evaluation_id, session_id=session_id, source_run_key=source_run_key
        )
        request_payload = None
        if feature_snapshot is not None and outcome_snapshot is not None:
            callback_url = _resource_callback_url()
            request_payload = self._request_payload(
                evaluation_id=evaluation_id,
                run_id=parent_identity,
                task_id=task_id,
                loop_index=loop_index,
                node_id=node_id,
                opt_in=opt_in,
                profile_sha256=profile.profile_sha256,
                bundle=bundle,
                feature_snapshot=feature_snapshot,
                outcome_snapshot=outcome_snapshot,
                input_manifest_sha=input_manifest_sha,
                input_hashes=input_hashes,
                inventory=inventory,
                catalog_digest=input_hashes["catalog_digest"],
                label_horizon=label_horizon,
                strategy_topk=strategy_topk,
                session_id=session_id,
                source_run_key=source_run_key,
                resource_token=token,
                callback_url=callback_url,
            )
            request_sha = _request_sha(request_payload)
        else:
            request_sha = canonical_sha256(
                {"evaluation_id": evaluation_id, "platform_status": "dataset_identity_incomplete", "actions": actions}
            )
        spec = QELongTrendEvaluationControlSpec(
            evaluation_id=evaluation_id,
            run_id=run_id,
            parent_task_id=task_id,
            parent_loop_index=int(loop_index),
            profile_id=profile.profile_id,
            profile_sha256=profile.profile_sha256,
            evaluator_version=EVALUATOR_VERSION,
            evaluator_source_sha256=bundle.evaluator_source_sha256,
            execution_environment_snapshot_id=environment.execution_environment_snapshot_id,
            execution_environment_manifest_sha256=environment.execution_environment_manifest_sha256,
            bundle_sha256=bundle.bundle_sha256,
            qe_dataset_contract_id=QE_DATASET_CONTRACT_ID,
            feature_dataset_snapshot_id=feature_snapshot.snapshot_id if feature_snapshot else None,
            feature_dataset_manifest_sha256=feature_snapshot.manifest_sha256 if feature_snapshot else None,
            outcome_dataset_snapshot_id=outcome_snapshot.snapshot_id if outcome_snapshot else None,
            outcome_dataset_manifest_sha256=outcome_snapshot.manifest_sha256 if outcome_snapshot else None,
            input_manifest_sha256=input_manifest_sha,
            node_id=node_id,
            request_sha=request_sha,
            request_json=(
                {key: value for key, value in request_payload.items() if key != "resource_session_token"}
                if request_payload is not None
                else {"platform_status": "dataset_identity_incomplete", "data_action_plan": list(actions)}
            ),
            resource_session_id=session_id,
        )
        row = self.control_repository.create_or_get_queued(
            spec,
            qelt_resource={
                "session_id": session_id,
                "source_run_key": source_run_key,
                "token_sha256": hashlib.sha256(token.encode("utf-8")).hexdigest(),
            },
        )
        if request_payload is None and row["status"] not in {"partial", "failed", "cancelled", "succeeded"}:
            claimed = self.control_repository.claim(evaluation_id, owner_id=self.owner_id)
            lease = self.control_repository.lease_from(claimed)
            row = self.control_repository.transition(
                lease,
                expected_statuses=("queued",),
                updates={
                    "status": "partial",
                    "reason_code": "QELT_DATASET_IDENTITY_INCOMPLETE",
                    "reason_json": {"message": "node dataset identity is incomplete for F-014"},
                    "platform_delivery_status_json": {"worker": "not_submitted", "cas": "awaiting_data"},
                    "data_action_plan_json": list(actions),
                },
                release_owner=True,
            )
            self._record_local_resource_terminal(
                token=token,
                session_id=session_id,
                source_run_key=source_run_key,
                task_id=task_id,
                loop_index=loop_index,
                node_id=node_id,
                evaluation_id=evaluation_id,
            )
        return PreparedLongTrendEvaluation(
            evaluation_id=evaluation_id,
            control_row=row,
            request_payload=request_payload,
            resource_token=token,
            ready_for_node=request_payload is not None,
            data_action_plan=actions,
        )

    async def submit(
        self,
        *,
        prepared: PreparedLongTrendEvaluation,
        task_id: str,
        loop_index: int,
        client: QEWorkspaceClient,
    ) -> QELongTrendJobReceipt | None:
        if not prepared.ready_for_node or prepared.request_payload is None:
            return None
        row = prepared.control_row
        if row["status"] in {"submitted", "running", "collecting", "succeeded", "partial", "failed", "cancelled"} and row.get("job_id"):
            inspection = await client.inspect_long_trend_evaluation(
                task_id=task_id,
                loop_id=f"Loop{int(loop_index)}",
                evaluation_id=prepared.evaluation_id,
            )
            return QELongTrendJobReceipt(
                schema_version=inspection.schema_version,
                task_id=inspection.task_id,
                loop_id=inspection.loop_id,
                evaluation_id=inspection.evaluation_id,
                job_id=inspection.job_id,
                request_sha=inspection.request_sha,
                status=inspection.status,
                duplicate_replay=True,
                current_attempt_id=inspection.current_attempt_id,
                execution_environment_snapshot_id=str(row["execution_environment_snapshot_id"]),
                execution_environment_manifest_sha256=str(row["execution_environment_manifest_sha256"]),
            )
        claimed = self.control_repository.claim(prepared.evaluation_id, owner_id=self.owner_id)
        lease = self.control_repository.lease_from(claimed)
        submitting = self.control_repository.transition(
            lease,
            expected_statuses=("queued", "remote_state_unknown"),
            updates={"status": "submitting"},
        )
        lease = self.control_repository.lease_from(submitting)
        try:
            receipt = await client.submit_long_trend_evaluation(
                task_id=task_id,
                loop_id=f"Loop{int(loop_index)}",
                evaluation_id=prepared.evaluation_id,
                request_payload=prepared.request_payload,
            )
        except QELongTrendWorkspaceError as exc:
            next_status = "remote_state_unknown" if exc.reason_code == "QELT_NODE_STATE_UNKNOWN" else "failed"
            self.control_repository.transition(
                lease,
                expected_statuses=("submitting",),
                updates={
                    "status": next_status,
                    "reason_code": exc.reason_code,
                    "reason_json": {"message": str(exc), "context": exc.context},
                },
                release_owner=True,
            )
            if next_status == "failed":
                self.resource_service.mark_session_terminal(
                    str(submitting["resource_session_id"]),
                    status="failed",
                    reason_code=exc.reason_code,
                )
            raise
        if receipt.request_sha != str(submitting["request_sha"]):
            self.control_repository.transition(
                lease,
                expected_statuses=("submitting",),
                updates={
                    "status": "failed",
                    "reason_code": QELongTrendReason.NODE_JOB_IDENTITY_CONFLICT.value,
                    "reason_json": {"expected_request_sha": submitting["request_sha"], "actual_request_sha": receipt.request_sha},
                },
                release_owner=True,
            )
            self.resource_service.mark_session_terminal(
                str(submitting["resource_session_id"]),
                status="failed",
                reason_code=QELongTrendReason.NODE_JOB_IDENTITY_CONFLICT.value,
            )
            raise QELongTrendPhase2Error(
                "RD job receipt request hash differs from durable control row",
                reason_code=QELongTrendReason.NODE_JOB_IDENTITY_CONFLICT.value,
            )
        self.resource_service.mark_session_submitted(str(submitting["resource_session_id"]))
        self.control_repository.transition(
            lease,
            expected_statuses=("submitting",),
            updates={
                "status": "submitted",
                "job_id": receipt.job_id,
                "current_attempt_id": receipt.current_attempt_id,
                "platform_delivery_status_json": {"worker": receipt.status, "cas": "awaiting_worker"},
            },
            release_owner=True,
        )
        return receipt

    async def inspect(
        self,
        *,
        evaluation_id: str,
        task_id: str,
        loop_index: int,
        client: QEWorkspaceClient,
    ) -> QELongTrendJobInspection:
        return await client.inspect_long_trend_evaluation(
            task_id=task_id,
            loop_id=f"Loop{int(loop_index)}",
            evaluation_id=evaluation_id,
        )

    async def collect_and_publish(
        self,
        *,
        evaluation_id: str,
        task_id: str,
        loop_index: int,
        client: QEWorkspaceClient,
        artifact_store: QELongTrendArtifactStore | None = None,
    ) -> dict[str, Any]:
        inspection = await self.inspect(
            evaluation_id=evaluation_id,
            task_id=task_id,
            loop_index=loop_index,
            client=client,
        )
        if inspection.status not in {"succeeded", "partial", "failed", "cancelled"}:
            return {"status": "awaiting_worker", "evaluation_id": evaluation_id, "remote_status": inspection.status}
        claimed = self.control_repository.claim(evaluation_id, owner_id=self.owner_id, lease_seconds=300)
        lease = self.control_repository.lease_from(claimed)
        collecting = self.control_repository.transition(
            lease,
            expected_statuses=("submitted", "running", "remote_state_unknown", "collecting"),
            updates={"status": "collecting", "current_attempt_id": inspection.current_attempt_id},
        )
        lease = self.control_repository.lease_from(collecting)
        try:
            worker_terminal, manifest, published, published_meta, by_type = await self._publish_remote_artifacts(
                evaluation_id=evaluation_id,
                task_id=task_id,
                loop_index=loop_index,
                client=client,
                artifact_store=artifact_store,
            )
        except Exception as exc:
            self.control_repository.transition(
                lease,
                expected_statuses=("collecting",),
                updates={
                    "status": "remote_state_unknown",
                    "reason_code": getattr(exc, "reason_code", QELongTrendReason.ARTIFACT_STREAM_INTERRUPTED.value),
                    "reason_json": {"error_type": type(exc).__name__, "message": str(exc)},
                    "platform_delivery_status_json": {"worker": inspection.status, "cas": "collect_failed"},
                },
                release_owner=True,
            )
            raise
        terminal_status = str(worker_terminal.get("status") or "failed")
        if terminal_status not in {"succeeded", "partial", "failed", "cancelled"}:
            terminal_status = "failed"
        updated = self.control_repository.transition(
            lease,
            expected_statuses=("collecting",),
            updates={
                "status": terminal_status,
                "worker_terminal_sha256": by_type["worker_terminal_receipt"]["sha256"],
                "artifact_store_run_key": evaluation_id,
                "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
                "family_status_json": worker_terminal.get("family_status") or {},
                "platform_delivery_status_json": published.get("platform_delivery_status") or {},
                "data_action_plan_json": worker_terminal.get("data_action_plan") or [],
                "stats_json": {
                    **dict(worker_terminal.get("stats") or {}),
                    "published_compact_receipt": published_meta,
                },
                "reason_code": worker_terminal.get("reason_code"),
                "reason_json": worker_terminal.get("reason_json") or {},
            },
            release_owner=True,
        )
        return {
            "status": terminal_status,
            "evaluation_id": evaluation_id,
            "artifact_manifest_uri": manifest["uri"],
            "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
            "control_row_version": updated["row_version"],
        }

    async def _publish_remote_artifacts(
        self,
        *,
        evaluation_id: str,
        task_id: str,
        loop_index: int,
        client: QEWorkspaceClient,
        artifact_store: QELongTrendArtifactStore | None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
        catalog = await client.list_long_trend_artifacts(
            task_id=task_id,
            loop_id=f"Loop{int(loop_index)}",
            evaluation_id=evaluation_id,
        )
        by_type = _node_artifact_catalog_by_type(catalog)
        store = artifact_store or QELongTrendArtifactStore()
        store.ensure_ready()
        with tempfile.TemporaryDirectory(
            prefix=f"qelt_collect_{evaluation_id[5:13]}_",
            dir=store.root / "tmp",
        ) as tmp_name:
            temp_root = Path(tmp_name)
            files: dict[str, Path] = {}
            for artifact_type, item in by_type.items():
                target = temp_root / Path(str(item["relative_path"])).name
                await client.stream_long_trend_artifact(
                    task_id=task_id,
                    loop_id=f"Loop{int(loop_index)}",
                    evaluation_id=evaluation_id,
                    artifact_path=str(item["relative_path"]),
                    destination=target,
                    expected_sha256=str(item["sha256"]),
                    expected_size_bytes=int(item["size_bytes"]),
                )
                files[artifact_type] = target
            worker_terminal = _read_json(files["worker_terminal_receipt"])
            manifest = store.publish(
                evaluation_id=evaluation_id,
                worker_terminal=worker_terminal,
                artifact_files=files,
                expected_catalog=by_type,
            )
            worker_compact = _read_json(files["worker_compact_receipt"])
            published = {
                **worker_compact,
                "schema_version": "qe_long_trend_published_compact_v1",
                "receipt_stage": "cas_published",
                "worker_terminal_sha256": by_type["worker_terminal_receipt"]["sha256"],
                "artifact_manifest_uri": manifest["uri"],
                "artifact_manifest_sha256": manifest["artifact_manifest_sha256"],
                "platform_delivery_status": {
                    **dict(worker_compact.get("platform_delivery_status") or {}),
                    "cas": "published",
                },
            }
            published_meta = store.publish_compact_receipt(
                evaluation_id=evaluation_id,
                receipt=published,
            )
        return worker_terminal, manifest, published, published_meta, by_type

    async def cancel_attempt(
        self,
        *,
        evaluation_id: str,
        task_id: str,
        loop_index: int,
        client: QEWorkspaceClient,
    ) -> dict[str, Any]:
        inspection = await self.inspect(
            evaluation_id=evaluation_id,
            task_id=task_id,
            loop_index=loop_index,
            client=client,
        )
        if inspection.status in {"succeeded", "partial", "failed", "cancelled"}:
            return {"status": "already_terminal", "evaluation_id": evaluation_id}
        if not inspection.current_attempt_id or not inspection.process_identity:
            raise QELongTrendPhase2Error(
                "typed cancel requires the current attempt and process identity",
                reason_code=QELongTrendReason.NODE_PROCESS_IDENTITY_CONFLICT.value,
            )
        return await client.cancel_long_trend_evaluation(
            task_id=task_id,
            loop_id=f"Loop{int(loop_index)}",
            evaluation_id=evaluation_id,
            expected_attempt_id=inspection.current_attempt_id,
            expected_process_identity=inspection.process_identity,
            expected_request_sha=inspection.request_sha,
        )

    async def reconcile(self, *, row: Mapping[str, Any], client: QEWorkspaceClient) -> dict[str, Any]:
        evaluation_id = str(row["evaluation_id"])
        row = self.control_repository.bind_available_archive_run(evaluation_id)
        task_id = str(row["parent_task_id"])
        loop_index = int(row["parent_loop_index"])
        if not row.get("job_id") and row.get("status") in {"queued", "submitting", "remote_state_unknown"}:
            request = dict(row.get("request_json") or {})
            if request.get("schema_version") != "qe_long_trend_job_request_v1":
                return {"status": "awaiting_data", "evaluation_id": evaluation_id}
            token = self.secret_store.load(
                evaluation_id,
                session_id=str(row["resource_session_id"]),
                source_run_key=f"qelt:{evaluation_id}",
            )
            request["resource_session_token"] = token
            prepared = PreparedLongTrendEvaluation(
                evaluation_id=evaluation_id,
                control_row=dict(row),
                request_payload=request,
                resource_token=token,
                ready_for_node=True,
                data_action_plan=(),
            )
            receipt = await self.submit(
                prepared=prepared,
                task_id=task_id,
                loop_index=loop_index,
                client=client,
            )
            return {"status": receipt.status if receipt else "awaiting_data", "evaluation_id": evaluation_id}
        inspection = await self.inspect(
            evaluation_id=evaluation_id,
            task_id=task_id,
            loop_index=loop_index,
            client=client,
        )
        if inspection.status in {"succeeded", "partial", "failed", "cancelled"}:
            return await self.collect_and_publish(
                evaluation_id=evaluation_id,
                task_id=task_id,
                loop_index=loop_index,
                client=client,
            )
        return {"status": inspection.status, "evaluation_id": evaluation_id}

    async def reconcile_nonterminal(self, *, limit: int = 100) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for row in self.control_repository.list_nonterminal(limit=limit):
            try:
                async with QEWorkspaceClient.for_node(str(row["node_id"])) as client:
                    results.append(await self.reconcile(row=row, client=client))
            except QELongTrendWorkspaceError as exc:
                if exc.reason_code != QELongTrendReason.NODE_STATE_UNKNOWN.value:
                    results.append({"evaluation_id": row["evaluation_id"], "status": "platform_error", "reason_code": exc.reason_code})
                else:
                    results.append({"evaluation_id": row["evaluation_id"], "status": "remote_state_unknown"})
            except Exception as exc:
                results.append(
                    {
                        "evaluation_id": row["evaluation_id"],
                        "status": "platform_error",
                        "reason_code": getattr(exc, "reason_code", type(exc).__name__),
                        "message": str(exc),
                    }
                )
        return results

    def _request_payload(self, **values: Any) -> dict[str, Any]:
        inventory: RecorderArtifactInventory = values["inventory"]
        bundle: QELongTrendEvaluatorBundle = values["bundle"]
        feature_snapshot: QEDatasetSnapshotIdentity = values["feature_snapshot"]
        outcome_snapshot: QEDatasetSnapshotIdentity = values["outcome_snapshot"]
        return {
            "schema_version": "qe_long_trend_job_request_v1",
            "evaluation_id": values["evaluation_id"],
            "run_id": values["run_id"],
            "node_id": values["node_id"],
            "profile_id": values["opt_in"].profile_id,
            "profile_sha256": values["profile_sha256"],
            "evaluator_version": values["opt_in"].evaluator_version,
            "evaluator_source_sha256": bundle.evaluator_source_sha256,
            "execution_environment_snapshot_id": bundle.execution_environment_snapshot_id,
            "execution_environment_manifest_sha256": bundle.execution_environment_manifest_sha256,
            "bundle_sha256": bundle.bundle_sha256,
            "qe_dataset_contract_id": QE_DATASET_CONTRACT_ID,
            "feature_snapshot": asdict(feature_snapshot),
            "outcome_snapshot": asdict(outcome_snapshot),
            "feature_data_root_uri": values["opt_in"].feature_data_root_uri,
            "outcome_data_root_uri": values["opt_in"].outcome_data_root_uri,
            "input_manifest_sha256": values["input_manifest_sha"],
            "input_artifact_hashes": values["input_hashes"],
            "artifact_paths": {
                name: (item.get("relative_path") if item else None)
                for name, item in inventory.artifacts.items()
                if name in WORKER_INPUT_ARTIFACTS
            },
            "artifact_hashes": {
                name: str(item.get("sha256"))
                for name, item in inventory.artifacts.items()
                if name in WORKER_INPUT_ARTIFACTS
                and item is not None
                and isinstance(item.get("sha256"), str)
            },
            "recorder_ref": {"experiment_id": inventory.experiment_id, "recorder_id": inventory.recorder_id},
            "catalog_digest": values["catalog_digest"],
            "catalog_completeness": inventory.catalog_completeness,
            "backtest_freq": values["opt_in"].backtest_freq,
            "evaluation_asof": outcome_snapshot.end_date,
            "label_horizon": values["label_horizon"],
            "strategy_topk": values["strategy_topk"],
            "bundle": bundle.request_payload(),
            "resource_session": {
                "session_id": values["session_id"],
                "source_run_key": values["source_run_key"],
            },
            "resource_session_token": values["resource_token"],
            "resource_callback_url": values["callback_url"],
            "parser_timeout_seconds": 300,
        }

    def _record_local_resource_terminal(self, **values: Any) -> None:
        common = {
            "session_id": values["session_id"],
            "source_run_key": values["source_run_key"],
            "task_id": values["task_id"],
            "loop_id": f"Loop{int(values['loop_index'])}",
            "loop_index": int(values["loop_index"]),
            "node_id": values["node_id"],
        }
        self.resource_service.ingest_event(
            token=values["token"],
            payload={
                **common,
                "sequence_no": 1,
                "phase": "long_trend_eval",
                "phase_status": "not_submitted",
                "metadata": {"evaluation_id": values["evaluation_id"]},
            },
        )
        self.resource_service.ingest_event(
            token=values["token"],
            payload={
                **common,
                "sequence_no": 2,
                "phase": "completed",
                "phase_status": "partial",
                "reason_code": "QELT_DATASET_IDENTITY_INCOMPLETE",
                "metadata": {"evaluation_id": values["evaluation_id"]},
            },
        )


def _merge_registration_catalog(live: Mapping[str, Any], registered: Mapping[str, Any]) -> dict[str, Any]:
    if live.get("catalog_completeness") not in {"complete", "partial"}:
        raise QELongTrendPhase2Error("live QE catalog completeness is invalid", reason_code="QELT_WORKSPACE_CATALOG_PARTIAL")
    registered_rows = registered.get("files")
    if not isinstance(registered_rows, list):
        raise QELongTrendPhase2Error("registration catalog files must be an array", reason_code="QELT_RECORDER_REF_MISSING")
    live_rows = {str(item.get("relative_path") or ""): dict(item) for item in live.get("files", []) if isinstance(item, Mapping)}
    for item in registered_rows:
        if not isinstance(item, Mapping):
            raise QELongTrendPhase2Error("registration catalog entry is invalid", reason_code="QELT_RECORDER_REF_MISSING")
        path = str(item.get("relative_path") or "")
        live_item = live_rows.get(path)
        if live_item is None or int(live_item.get("size_bytes") or -1) != int(item.get("size_bytes") or -2):
            raise QELongTrendPhase2Error(
                f"registration catalog differs from node catalog for {path!r}",
                reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
            )
        digest = str(item.get("sha256") or "")
        if len(digest) != 64:
            raise QELongTrendPhase2Error(
                f"registration catalog lacks sha256 for {path!r}",
                reason_code=QELongTrendReason.ARTIFACT_HASH_MISMATCH.value,
            )
        live_item["sha256"] = digest
        live_item["parser_contract"] = item.get("parser_contract")
    return {**dict(live), "files": list(live_rows.values())}


def _node_artifact_catalog_by_type(catalog: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    suffixes = {
        "/artifacts/signal_observations.parquet": "signal_observations",
        "/artifacts/holding_episodes.parquet": "holding_episodes",
        "/artifacts/worker_compact_receipt.json": "worker_compact_receipt",
        "/artifacts/worker_terminal_receipt.json": "worker_terminal_receipt",
    }
    result: dict[str, dict[str, Any]] = {}
    for item in catalog.get("artifacts") or []:
        if not isinstance(item, Mapping):
            continue
        path = "/" + str(item.get("relative_path") or "").replace("\\", "/").lstrip("/")
        artifact_type = next((name for suffix, name in suffixes.items() if path.endswith(suffix)), None)
        if artifact_type is None:
            continue
        if artifact_type in result:
            raise QELongTrendPhase2Error(
                f"node artifact catalog contains duplicate {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        digest = str(item.get("sha256") or "")
        size = item.get("size_bytes")
        if len(digest) != 64 or not isinstance(size, int) or size < 0:
            raise QELongTrendPhase2Error(
                f"node artifact catalog metadata is invalid for {artifact_type}",
                reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
            )
        result[artifact_type] = dict(item)
    missing = sorted({"worker_compact_receipt", "worker_terminal_receipt"} - set(result))
    if missing:
        raise QELongTrendPhase2Error(
            f"node terminal catalog is missing required receipts: {missing}",
            reason_code=QELongTrendReason.ARTIFACT_SCHEMA_MISMATCH.value,
        )
    return result


def _long_trend_snapshot(
    identity: QEWorkspaceDatasetIdentity,
    *,
    family: str,
) -> tuple[QEDatasetSnapshotIdentity | None, dict[str, Any] | None]:
    raw = identity.long_trend_snapshot
    if isinstance(raw, Mapping):
        return (
            QEDatasetSnapshotIdentity(
                snapshot_id=str(raw["snapshot_id"]),
                manifest_sha256=str(raw["manifest_sha256"]),
                start_date=str(raw["start_date"]),
                end_date=str(raw["end_date"]),
                lineage_parent_ids=tuple(str(value) for value in raw.get("lineage_parent_ids", [])),
            ),
            None,
        )
    return (
        None,
        {
            "action": "publish_or_repair_qe_long_trend_snapshot_identity",
            "source_candidates": ["qe_dataset_manifest.json", "meta.json", "daily_pv.h5", "sector_data.h5"],
            "required_fields": ["snapshot_id", "manifest_sha256", "start_date", "end_date"],
            "time_range": {},
            "historical_backfill": True,
            "recoverable_family": family,
            "reason_code": identity.long_trend_snapshot_reason or identity.reason_code,
            "missing": list(identity.missing),
            "acquisition_suggestions": list(identity.acquisition_suggestions),
        },
    )


def _input_artifact_hashes(inventory: RecorderArtifactInventory) -> dict[str, Any]:
    field_names = {
        "prediction": "prediction_sha256",
        "label": "label_sha256",
        "positions": "position_sha256",
        "portfolio_report": "portfolio_report_sha256",
        "indicator_summary": "indicator_frame_sha256",
        "indicator_object": "indicator_object_sha256",
        "orders": "order_sha256",
        "trades": "trade_sha256",
        "params": "params_sha256",
    }
    values: dict[str, Any] = {}
    for name, item in inventory.artifacts.items():
        digest = str(item.get("sha256") or "") if item else ""
        field_name = field_names.get(name, f"{name}_sha256")
        values[field_name] = digest if len(digest) == 64 else typed_null(field_name)
    return values


def _request_sha(payload: Mapping[str, Any]) -> str:
    public = dict(payload)
    public.pop("resource_session_token", None)
    return canonical_sha256(public)


def _recorder_catalog_digest(inventory: RecorderArtifactInventory) -> str:
    return canonical_sha256(
        {
            "schema_version": "qe_long_trend_recorder_inventory_v1",
            "task_id": inventory.task_id,
            "loop_id": inventory.loop_id,
            "experiment_id": inventory.experiment_id,
            "recorder_id": inventory.recorder_id,
            "artifact_prefix": inventory.artifact_prefix,
            "backtest_freq": inventory.backtest_freq,
            "catalog_completeness": inventory.catalog_completeness,
            "input_manifest_sha256": inventory.input_manifest_sha256,
        }
    )


def _evaluation_parent_identity(*, task_id: str, loop_index: int) -> str:
    normalized_task = str(task_id or "").strip()
    normalized_loop = int(loop_index)
    if not normalized_task or "/" in normalized_task or "\\" in normalized_task or normalized_loop < 1:
        raise QELongTrendPhase2Error(
            "QE task/Loop parent identity is invalid",
            reason_code=QELongTrendReason.NON_QE_SOURCE_REJECTED.value,
        )
    return f"qe_task_loop:{normalized_task}:Loop{normalized_loop}"


def _resource_callback_url() -> str:
    base = next(
        (
            str(os.getenv(name) or "").strip()
            for name in ("AISTOCK_QE_CALLBACK_BASE_URL", "AISTOCK_BACKEND_CALLBACK_BASE_URL", "AISTOCK_BACKEND_BASE_URL")
            if str(os.getenv(name) or "").strip()
        ),
        "",
    )
    if not base.startswith(("http://", "https://")):
        raise QELongTrendPhase2Error(
            "QE long-trend resource callback requires an explicit AIstock http(s) base URL",
            reason_code=QELongTrendReason.CONTROL_STATE_CONFLICT.value,
        )
    base = base.rstrip("/")
    path = "/api/v1/quantevolver/evolution/webhook/loop-resource-phase"
    return base + (path.removeprefix("/api/v1") if base.endswith("/api/v1") else path)


def _require_frozen_identity(
    frozen: Mapping[str, Any],
    *,
    bundle: QELongTrendEvaluatorBundle,
    environment_snapshot_id: str,
    environment_manifest_sha256: str,
    feature_identity: QEWorkspaceDatasetIdentity,
    outcome_identity: QEWorkspaceDatasetIdentity,
    profile_sha256: str,
) -> None:
    expected = {
        "profile_sha256": profile_sha256,
        "evaluator_source_sha256": bundle.evaluator_source_sha256,
        "bundle_sha256": bundle.bundle_sha256,
        "execution_environment_snapshot_id": environment_snapshot_id,
        "execution_environment_manifest_sha256": environment_manifest_sha256,
        "feature_dataset": {
            "complete": feature_identity.complete,
            "dataset": feature_identity.dataset,
            "long_trend_snapshot": feature_identity.long_trend_snapshot,
            "long_trend_snapshot_reason": feature_identity.long_trend_snapshot_reason,
        },
        "outcome_dataset": {
            "complete": outcome_identity.complete,
            "dataset": outcome_identity.dataset,
            "long_trend_snapshot": outcome_identity.long_trend_snapshot,
            "long_trend_snapshot_reason": outcome_identity.long_trend_snapshot_reason,
        },
    }
    actual = dict(frozen)
    if actual != expected:
        raise QELongTrendPhase2Error(
            "normal Loop F-014 frozen identity changed between submission and registration",
            reason_code=QELongTrendReason.EXECUTION_ENVIRONMENT_MISMATCH.value,
            context={"frozen": actual, "current": expected},
        )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any], *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        os.chmod(tmp, mode)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        _atomic_replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _atomic_replace(source: Path, target: Path) -> None:
    if os.name != "nt":
        os.replace(source, target)
        return
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    move_file_ex = kernel32.MoveFileExW
    move_file_ex.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
    move_file_ex.restype = wintypes.BOOL
    if not move_file_ex(str(source), str(target), 0x00000001 | 0x00000008):
        error = ctypes.get_last_error()
        raise OSError(error, f"MoveFileExW write-through replace failed: {source} -> {target}")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
