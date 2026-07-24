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
    RUNTIME_EXTERNAL_DATA_LINK_NAMES,
    CombineBacktestRequest,
    MultiAlphaCombineBacktestError,
    apply_pred_backtest_overrides,
    build_prediction_only_legs,
    combine_legs,
    is_rank_fusion_scheme,
    ingest_enhanced_metrics,
    is_runtime_external_data_link,
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
    DurableContractError,
    OwnershipToken,
    artifact_manifest_hash_for,
    make_attempt_id,
    make_remote_task_id,
    submission_intent_hash_for,
)
from backend.services.multi_alpha.durable_identity import (
    DurableExecutionIdentityResolver,
    validate_execution_identity,
)
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepository
from backend.services.multi_alpha.panels import MultiAlphaPanelBuilder, MultiAlphaPanelError
from backend.services.multi_alpha.remote_dispatch import (
    WorkspaceArtifactSyncClient,
    _remote_paths,
    _remote_small_files,
    _remote_wsl_command,
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


def _runtime_external_data_bindings(backtest_config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Describe QE data links excluded from the portable child artifact set.

    Completed Windows/DrvFS QE workspaces expose the canonical H5 bundle as
    Linux links.  Durable remote execution binds the selected node's own QE
    dataset and must not dereference or upload those workstation-local links.
    Listing directory entry names is safe on Windows even when stat/open is
    not, so the exclusion remains explicit in both materialization metadata
    and the immutable artifact manifest.
    """

    raw_root = str(backtest_config.get("runtime_template_dir") or "").strip()
    if not raw_root:
        return []
    root = Path(raw_root)
    try:
        present_names = {
            entry.name
            for entry in root.iterdir()
            if is_runtime_external_data_link(entry)
        }
    except OSError as exc:
        raise DurableExecutionAdapterError(
            "durable runtime template entries cannot be enumerated",
            reason_code="multi_alpha_runtime_template_scan_failed",
            context={"path": str(root), "error_type": type(exc).__name__, "message": str(exc)},
        ) from exc
    return [
        {
            "name": name,
            "binding": "node_canonical_qe_data",
            "published": False,
        }
        for name in sorted(RUNTIME_EXTERNAL_DATA_LINK_NAMES & present_names)
    ]


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


def _remote_loop_index_from_intent(qe_loop_id: str) -> int:
    normalized = str(qe_loop_id or "").strip()
    suffix = normalized[4:] if normalized.startswith("Loop") else ""
    if not suffix.isdigit() or int(suffix) < 1:
        raise DurableExecutionAdapterError(
            "durable submission intent has an invalid QE loop identity",
            reason_code="multi_alpha_remote_loop_identity_invalid",
            context={"qe_loop_id": qe_loop_id},
        )
    return int(suffix)


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
    execution_identity_hash: str | None = None
    execution_environment_snapshot_id: str | None = None
    execution_environment_manifest_sha256: str | None = None


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
        recovery_materializer: Any | None = None,
        recovery_materializer_identity_resolver: Any | None = None,
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
        self._execution_identity_resolver = DurableExecutionIdentityResolver(
            model_store=self._model_store,
        )
        if recovery_materializer is not None and recovery_materializer_identity_resolver is None:
            raise ValueError(
                "custom recovery_materializer requires an exact identity resolver",
            )
        self._recovery_materializer = (
            recovery_materializer or self._materialize_frozen_recovery
        )
        self._recovery_materializer_identity_resolver = (
            recovery_materializer_identity_resolver
            or self._resolve_builtin_recovery_materializer_identity
        )

    def recovery_materializer_identity_for_run(
        self,
        source_run: Mapping[str, Any],
    ) -> dict[str, str]:
        request = self._request_from_run(source_run)
        identity = self._recovery_materializer_identity_resolver(
            source_run=dict(source_run),
            request=request,
        )
        if not isinstance(identity, Mapping) or not identity:
            raise DurableExecutionAdapterError(
                "QE recovery materializer identity resolver returned no identity",
                reason_code="rematerialize_recovery_code_identity_missing",
            )
        return {str(key): str(value) for key, value in identity.items()}

    def _resolve_builtin_recovery_materializer_identity(
        self,
        *,
        source_run: Mapping[str, Any],
        request: CombineBacktestRequest,
    ) -> dict[str, str]:
        del source_run
        return self._execution_identity_resolver.resolve_materializer_identity(
            request=request,
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

    def load_recovery_source_result_payload(
        self,
        *,
        source_run_id: str,
        source_child_id: str,
        source_attempt_id: str,
    ) -> dict[str, Any]:
        """Return verified source data needed by explicit reference/derived rows."""

        source_attempt = self._required_row(
            "attempt",
            source_attempt_id,
            self._repository.get_attempt(source_attempt_id),
        )
        if str(source_attempt.get("status") or "") != "succeeded":
            raise DurableExecutionAdapterError(
                "recovery reference source attempt is not succeeded",
                reason_code="results_only_artifact_missing",
                context={"source_attempt_id": source_attempt_id, "status": source_attempt.get("status")},
            )
        artifacts = self.load_published_artifacts(
            run_id=source_run_id,
            child_id=source_child_id,
            attempt_id=source_attempt_id,
        )
        return {
            "metrics": dict(self.load_collected_metrics(artifacts)),
            "materialization_metadata": dict(self.load_materialization_metadata(artifacts)),
            "artifact_manifest": dict(artifacts.artifact_manifest),
            "result_manifest": dict(source_attempt.get("result_manifest_json") or {}),
        }

    def publish_artifacts(
        self,
        materialization: DurableChildMaterialization,
    ) -> DurablePublishedArtifacts:
        workspace = materialization.workspace
        input_manifest = json_mapping(
            materialization.child.get("input_manifest_json"),
            field_name="input_manifest_json",
        )
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
            external_data_bindings = _runtime_external_data_bindings(backtest_config)
            durable_backtest_config = {
                **backtest_config,
                "_exclude_runtime_external_data_links": True,
            }
            prepare_pred_backtest_workspace(
                workspace=staging,
                backtest_config=durable_backtest_config,
            )
            apply_pred_backtest_overrides(
                workspace=staging,
                backtest_config=backtest_config,
            )
            l2_artifact_path = self._stage_required_l2_artifact(
                staging=staging,
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
                "external_runtime_data_bindings": external_data_bindings,
            }
            self._atomic_write_json(staging / "materialization.json", materialization_payload)
            files = self._publish_staging_tree(staging=staging, workspace=workspace)
            l2_artifact = {
                "path": l2_artifact_path,
                **dict(files[l2_artifact_path]),
            }
            manifest = {
                "schema_version": ARTIFACT_MANIFEST_SCHEMA,
                "run_id": materialization.run["id"],
                "child_id": materialization.child["child_id"],
                "attempt_id": materialization.attempt["attempt_id"],
                "input_manifest_hash": materialization.child["input_manifest_hash"],
                "prediction_file": "combined_prediction.pkl",
                "l2_artifact": l2_artifact,
                "files": files,
                "external_runtime_data_bindings": external_data_bindings,
                "execution_identity": input_manifest.get("execution_identity"),
                "execution_identity_hash": input_manifest.get("execution_identity_hash"),
                "execution_identity_evidence": input_manifest.get("execution_identity_evidence"),
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

    def stage_backtest_only_recovery_artifacts(
        self,
        *,
        source_run_id: str,
        source_child_id: str,
        source_attempt_id: str,
        successor_run_id: str,
        successor_child_id: str,
        successor_attempt_id: str,
        successor_input_manifest_hash: str,
        source_lineage_hash: str,
    ) -> DurablePublishedArtifacts:
        """Copy verified frozen prediction/runtime artifacts into successor storage.

        This path is deliberately not a hard-link or path-alias optimization: the
        successor must survive a permitted source workspace deletion.  Files are
        copied from the verified source manifest through a private staging tree,
        checked byte-for-byte, and only then become visible at the preallocated
        successor workspace identity.  No database row is created by this method.
        """

        source = self.load_published_artifacts(
            run_id=source_run_id,
            child_id=source_child_id,
            attempt_id=source_attempt_id,
        )
        workspace = self._attempt_workspace(
            successor_run_id,
            successor_child_id,
            successor_attempt_id,
        )
        existing = self._read_json_if_exists(workspace / "artifact_manifest.json")
        if existing is not None:
            self._verify_published_manifest(
                existing,
                workspace=workspace,
                expected_input_manifest_hash=successor_input_manifest_hash,
            )
            if existing.get("recovery_source_lineage_hash") != source_lineage_hash:
                raise DurableExecutionAdapterError(
                    "successor artifact workspace belongs to a different recovery lineage",
                    reason_code="recovery_artifact_publish_conflict",
                    context={"workspace": str(workspace)},
                )
            return DurablePublishedArtifacts(
                workspace=workspace,
                prediction_path=workspace / "combined_prediction.pkl",
                artifact_manifest_path=workspace / "artifact_manifest.json",
                artifact_manifest=existing,
            )

        source_files = source.artifact_manifest.get("files")
        if not isinstance(source_files, Mapping):
            raise DurableExecutionAdapterError(
                "source artifact manifest has no verified file inventory",
                reason_code="backtest_prediction_missing",
                context={"source_attempt_id": source_attempt_id},
            )
        workspace.parent.mkdir(parents=True, exist_ok=True)
        staging = workspace.parent / f".{workspace.name}.recovery.{uuid.uuid4().hex}.tmp"
        if staging.exists():
            raise DurableExecutionAdapterError(
                "recovery artifact staging path collision",
                reason_code="recovery_artifact_publish_conflict",
                context={"staging": str(staging)},
            )
        staging.mkdir(parents=False)
        try:
            source_root = source.workspace.resolve()
            for raw_relative, metadata in sorted(source_files.items()):
                if not isinstance(metadata, Mapping):
                    raise DurableExecutionAdapterError(
                        "source artifact manifest file metadata is invalid",
                        reason_code="backtest_prediction_missing",
                        context={"source_attempt_id": source_attempt_id, "relative_path": raw_relative},
                    )
                relative = Path(str(raw_relative))
                if (
                    not relative.parts
                    or relative.is_absolute()
                    or any(part in {"", ".", ".."} for part in relative.parts)
                ):
                    raise DurableExecutionAdapterError(
                        "source artifact manifest contains an unsafe relative path",
                        reason_code="recovery_artifact_publish_conflict",
                        context={"relative_path": str(raw_relative)},
                    )
                source_path = (source_root / relative).resolve()
                try:
                    source_path.relative_to(source_root)
                except ValueError as exc:
                    raise DurableExecutionAdapterError(
                        "source artifact path escapes its verified workspace",
                        reason_code="recovery_artifact_publish_conflict",
                        context={"relative_path": str(raw_relative)},
                    ) from exc
                if source_path.is_symlink() or not source_path.is_file():
                    raise DurableExecutionAdapterError(
                        "source artifact must be a regular non-symlink file",
                        reason_code="recovery_artifact_publish_conflict",
                        context={"path": str(source_path)},
                    )
                digest, size = self._sha256_file(source_path)
                if digest != metadata.get("sha256") or size != int(metadata.get("size") or -1):
                    raise DurableExecutionAdapterError(
                        "source artifact bytes do not match its frozen manifest",
                        reason_code="backtest_prediction_hash_mismatch",
                        context={"path": str(source_path)},
                    )
                self._atomic_copy_file(source_path, staging / relative)
            files = self._publish_staging_tree(staging=staging, workspace=workspace)
            manifest = {
                "schema_version": ARTIFACT_MANIFEST_SCHEMA,
                "run_id": successor_run_id,
                "child_id": successor_child_id,
                "attempt_id": successor_attempt_id,
                "input_manifest_hash": successor_input_manifest_hash,
                "prediction_file": "combined_prediction.pkl",
                "files": files,
                "recovery_source": {
                    "source_run_id": source_run_id,
                    "source_child_id": source_child_id,
                    "source_attempt_id": source_attempt_id,
                },
                "recovery_source_lineage_hash": source_lineage_hash,
            }
            manifest["manifest_hash"] = artifact_manifest_hash_for(manifest)
            self._atomic_write_json(workspace / "artifact_manifest.json", manifest)
            self._verify_published_manifest(
                manifest,
                workspace=workspace,
                expected_input_manifest_hash=successor_input_manifest_hash,
            )
            return DurablePublishedArtifacts(
                workspace=workspace,
                prediction_path=workspace / "combined_prediction.pkl",
                artifact_manifest_path=workspace / "artifact_manifest.json",
                artifact_manifest=manifest,
            )
        finally:
            if staging.exists():
                shutil.rmtree(staging)

    def stage_rematerialized_recovery_artifacts(
        self,
        *,
        source_run: Mapping[str, Any],
        source_child: Mapping[str, Any],
        source_attempt_id: str,
        successor_run_spec: Any,
        successor_child_spec: Any,
        successor_attempt_spec: Any,
        source_lineage: Mapping[str, Any],
    ) -> DurablePublishedArtifacts:
        """Materialize through the registered frozen-input provider.

        This is not a convenience alias for ``materialize_child_input``.  That
        method is intentionally allowed to use the ordinary initial-run path;
        using it here would silently substitute the currently deployed
        materializer, request defaults, or mutable inputs for a historical
        recovery.  The production default is the built-in verified provider;
        a custom provider is accepted only together with its identity resolver.
        Both receive the persisted source manifests verbatim.
        """

        source_input = source_lineage.get("source_input_manifest")
        source_input_hash = str(source_lineage.get("source_input_manifest_hash") or "")
        persisted_source_input = source_child.get("input_manifest_json")
        persisted_source_hash = str(source_child.get("input_manifest_hash") or "")
        materializer_identity = source_lineage.get("recovery_materializer_identity")
        if not isinstance(source_input, Mapping) or not source_input_hash:
            raise DurableExecutionAdapterError(
                "rematerialized recovery is missing the frozen source input manifest",
                reason_code="rematerialize_source_identity_missing",
                context={"source_child_id": source_child.get("child_id")},
            )
        if (
            not isinstance(persisted_source_input, Mapping)
            or artifact_manifest_hash_for(dict(source_input)) != source_input_hash
            or source_input_hash != persisted_source_hash
            or dict(source_input) != dict(persisted_source_input)
        ):
            raise DurableExecutionAdapterError(
                "rematerialized recovery source input differs from its persisted immutable manifest",
                reason_code="source_lineage_mismatch",
                context={"source_child_id": source_child.get("child_id")},
            )
        if not isinstance(materializer_identity, Mapping) or not materializer_identity:
            raise DurableExecutionAdapterError(
                "rematerialized recovery is missing the frozen materializer identity",
                reason_code="rematerialize_recovery_code_identity_missing",
                context={"source_child_id": source_child.get("child_id")},
            )
        successor_workspace = self._attempt_workspace(
            str(successor_run_spec.run_id),
            str(successor_child_spec.child_id),
            str(successor_attempt_spec.attempt_id),
        )
        existing = self._read_json_if_exists(successor_workspace / "artifact_manifest.json")
        expected_lineage_hash = artifact_manifest_hash_for(dict(source_lineage))
        if existing is not None:
            self._verify_published_manifest(
                existing,
                workspace=successor_workspace,
                expected_input_manifest_hash=str(successor_child_spec.input_manifest_hash),
            )
            if (
                existing.get("recovery_source_lineage_hash") != expected_lineage_hash
                or existing.get("recovery_materializer_identity_hash")
                != artifact_manifest_hash_for(dict(materializer_identity))
            ):
                raise DurableExecutionAdapterError(
                    "rematerialized successor workspace belongs to a different frozen recovery identity",
                    reason_code="recovery_artifact_publish_conflict",
                    context={"workspace": str(successor_workspace)},
                )
            return DurablePublishedArtifacts(
                workspace=successor_workspace,
                prediction_path=successor_workspace / "combined_prediction.pkl",
                artifact_manifest_path=successor_workspace / "artifact_manifest.json",
                artifact_manifest=existing,
            )

        materialization = self._recovery_materializer(
            source_run=dict(source_run),
            source_child=dict(source_child),
            source_attempt_id=source_attempt_id,
            source_input_manifest=dict(source_input),
            source_input_manifest_hash=source_input_hash,
            recovery_materializer_identity=dict(materializer_identity),
            successor_run_spec=successor_run_spec,
            successor_child_spec=successor_child_spec,
            successor_attempt_spec=successor_attempt_spec,
            successor_workspace=successor_workspace,
        )
        if not isinstance(materialization, DurableChildMaterialization):
            raise DurableExecutionAdapterError(
                "frozen-input recovery materializer returned an invalid materialization object",
                reason_code="recovery_materializer_unavailable",
                context={"returned_type": type(materialization).__name__},
            )
        expected_identity = {
            "run_id": str(successor_run_spec.run_id),
            "child_id": str(successor_child_spec.child_id),
            "attempt_id": str(successor_attempt_spec.attempt_id),
            "input_manifest_hash": str(successor_child_spec.input_manifest_hash),
        }
        actual_identity = {
            "run_id": materialization.run.get("id"),
            "child_id": materialization.child.get("child_id"),
            "attempt_id": materialization.attempt.get("attempt_id"),
            "input_manifest_hash": materialization.child.get("input_manifest_hash"),
        }
        if actual_identity != expected_identity or materialization.workspace != successor_workspace:
            raise DurableExecutionAdapterError(
                "frozen-input recovery materializer returned a mismatched successor identity",
                reason_code="source_lineage_mismatch",
                context={"expected": expected_identity, "actual": actual_identity},
            )
        published = self.publish_artifacts(materialization)
        manifest = dict(published.artifact_manifest)
        manifest.update(
            {
                "recovery_source": {
                    "source_run_id": source_run.get("id"),
                    "source_child_id": source_child.get("child_id"),
                    "source_attempt_id": source_attempt_id,
                },
                "recovery_source_lineage_hash": expected_lineage_hash,
                "recovery_materializer_identity_hash": artifact_manifest_hash_for(
                    dict(materializer_identity)
                ),
            }
        )
        manifest.pop("manifest_hash", None)
        manifest["manifest_hash"] = artifact_manifest_hash_for(manifest)
        self._atomic_write_json(published.artifact_manifest_path, manifest)
        self._verify_published_manifest(
            manifest,
            workspace=successor_workspace,
            expected_input_manifest_hash=str(successor_child_spec.input_manifest_hash),
        )
        return DurablePublishedArtifacts(
            workspace=successor_workspace,
            prediction_path=successor_workspace / "combined_prediction.pkl",
            artifact_manifest_path=published.artifact_manifest_path,
            artifact_manifest=manifest,
        )

    def _materialize_frozen_recovery(
        self,
        *,
        source_run: Mapping[str, Any],
        source_child: Mapping[str, Any],
        source_attempt_id: str,
        source_input_manifest: Mapping[str, Any],
        source_input_manifest_hash: str,
        recovery_materializer_identity: Mapping[str, Any],
        successor_run_spec: Any,
        successor_child_spec: Any,
        successor_attempt_spec: Any,
        successor_workspace: Path,
    ) -> DurableChildMaterialization:
        """Recompute one prediction from frozen request and verified source bytes."""

        del source_child, source_attempt_id, source_input_manifest_hash
        request = self._request_from_run(source_run)
        current_identity = self.recovery_materializer_identity_for_run(source_run)
        if dict(recovery_materializer_identity) != current_identity:
            raise DurableExecutionAdapterError(
                "recovery materializer code identity changed after preview",
                reason_code="rematerialize_recovery_code_identity_changed",
                context={
                    "preview_identity": dict(recovery_materializer_identity),
                    "current_identity": current_identity,
                },
            )
        self._verify_frozen_prediction_sources(
            request=request,
            source_input_manifest=source_input_manifest,
        )
        run_row = {
            "id": str(successor_run_spec.run_id),
            "backtest_config_json": dict(successor_run_spec.backtest_config),
        }
        child_row = {
            "child_id": str(successor_child_spec.child_id),
            "run_id": str(successor_child_spec.run_id),
            "child_key": str(successor_child_spec.child_key),
            "child_kind": str(successor_child_spec.child_kind),
            "weighting_scheme": successor_child_spec.weighting_scheme,
            "dropped_leg_id": successor_child_spec.dropped_leg_id,
            "input_manifest_json": dict(successor_child_spec.input_manifest),
            "input_manifest_hash": str(successor_child_spec.input_manifest_hash),
        }
        attempt_row = {
            "attempt_id": str(successor_attempt_spec.attempt_id),
            "child_id": str(successor_attempt_spec.child_id),
            "attempt_no": int(successor_attempt_spec.attempt_no),
            "retry_mode": str(successor_attempt_spec.retry_mode),
            "source_attempt_id": successor_attempt_spec.source_attempt_id,
        }
        prediction_frame, weights, per_window_weights = self._materialize_prediction(
            child=child_row,
            request=request,
        )
        return DurableChildMaterialization(
            run=run_row,
            child=child_row,
            attempt=attempt_row,
            request=request,
            prediction_frame=prediction_frame,
            weights=weights,
            per_window_weights=per_window_weights,
            workspace=successor_workspace,
        )

    def _verify_frozen_prediction_sources(
        self,
        *,
        request: CombineBacktestRequest,
        source_input_manifest: Mapping[str, Any],
    ) -> None:
        execution_identity = source_input_manifest.get("execution_identity")
        if not isinstance(execution_identity, Mapping):
            raise DurableExecutionAdapterError(
                "frozen source input has no complete execution identity",
                reason_code="rematerialize_source_identity_missing",
            )
        execution_identity_hash = source_input_manifest.get("execution_identity_hash")
        if not isinstance(execution_identity_hash, str) or not execution_identity_hash:
            raise DurableExecutionAdapterError(
                "frozen source input has no execution identity hash",
                reason_code="rematerialize_source_identity_missing",
            )
        try:
            validate_execution_identity(
                payload=execution_identity,
                identity_hash=execution_identity_hash,
            )
        except DurableContractError as exc:
            raise DurableExecutionAdapterError(
                "frozen source execution identity is invalid",
                reason_code="source_lineage_mismatch",
                context={
                    "source_reason_code": exc.reason_code,
                    "source_context": dict(exc.context),
                },
            ) from exc
        raw_sources = execution_identity.get("prediction_sources")
        if not isinstance(raw_sources, list):
            raise DurableExecutionAdapterError(
                "frozen execution identity has no prediction source list",
                reason_code="rematerialize_source_identity_missing",
            )
        expected = {
            (str(leg.leg_id), str(seed_run_id))
            for leg in request.roster
            for seed_run_id in leg.seed_run_ids
        }
        observed: set[tuple[str, str]] = set()
        for raw in raw_sources:
            if not isinstance(raw, Mapping):
                raise DurableExecutionAdapterError(
                    "frozen prediction source identity is not an object",
                    reason_code="rematerialize_source_identity_missing",
                )
            leg_id = str(raw.get("leg_id") or "")
            seed_run_id = str(raw.get("seed_run_id") or "")
            expected_hash = str(raw.get("artifact_sha256") or "")
            observed.add((leg_id, seed_run_id))
            try:
                path = self._model_store.prediction_path(run_id=seed_run_id)
                actual_hash, _size = self._sha256_file(path)
            except Exception as exc:
                raise DurableExecutionAdapterError(
                    "frozen prediction source is unavailable for rematerialization",
                    reason_code="backtest_prediction_missing",
                    context={
                        "leg_id": leg_id,
                        "seed_run_id": seed_run_id,
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    },
                ) from exc
            if actual_hash != expected_hash:
                raise DurableExecutionAdapterError(
                    "frozen prediction source bytes changed before rematerialization",
                    reason_code="backtest_prediction_hash_mismatch",
                    context={
                        "leg_id": leg_id,
                        "seed_run_id": seed_run_id,
                        "expected": expected_hash,
                        "actual": actual_hash,
                    },
                )
        if observed != expected:
            raise DurableExecutionAdapterError(
                "frozen prediction source roster differs from the persisted request",
                reason_code="source_lineage_mismatch",
                context={
                    "expected": sorted(expected),
                    "observed": sorted(observed),
                },
            )

    def prepare_submission_intent(
        self,
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
        attempt: Mapping[str, Any],
        node_id: str,
    ) -> DurableSubmissionIntent:
        execution_binding = self._execution_binding_for_submission(run=run, child=child)
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
            execution_identity_hash=execution_binding["execution_identity_hash"],
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
            execution_identity_hash=execution_binding["execution_identity_hash"],
            execution_environment_snapshot_id=execution_binding[
                "execution_environment_snapshot_id"
            ],
            execution_environment_manifest_sha256=execution_binding[
                "execution_environment_manifest_sha256"
            ],
        )

    @staticmethod
    def _execution_binding_for_submission(
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
    ) -> dict[str, str | None]:
        input_manifest = json_mapping(
            child.get("input_manifest_json"),
            field_name="input_manifest_json",
        )
        raw_identity = input_manifest.get("execution_identity")
        raw_hash = input_manifest.get("execution_identity_hash")
        if raw_identity is None and raw_hash is None:
            # The run holds explicit incomplete evidence.  It remains a valid QE
            # research execution, but does not claim an exact remote identity.
            return {
                "execution_identity_hash": None,
                "execution_environment_snapshot_id": None,
                "execution_environment_manifest_sha256": None,
            }
        if not isinstance(raw_identity, Mapping) or not isinstance(raw_hash, str):
            raise DurableExecutionAdapterError(
                "child execution identity is partially persisted",
                reason_code="multi_alpha_execution_identity_invalid",
                context={"child_id": child.get("child_id")},
            )
        identity = validate_execution_identity(
            payload=raw_identity,
            identity_hash=raw_hash,
        )
        run_identity = run.get("execution_identity_json")
        run_hash = run.get("execution_identity_hash")
        if not isinstance(run_identity, Mapping) or run_hash != identity.identity_hash or dict(run_identity) != dict(identity.payload):
            raise DurableExecutionAdapterError(
                "run and child execution identity do not match",
                reason_code="multi_alpha_execution_identity_hash_mismatch",
                context={"run_id": run.get("id"), "child_id": child.get("child_id")},
            )
        runtime = identity.payload.get("runtime")
        if not isinstance(runtime, Mapping):
            raise DurableExecutionAdapterError(
                "execution identity runtime section is absent",
                reason_code="multi_alpha_execution_identity_invalid",
                context={"child_id": child.get("child_id")},
            )
        snapshot_id = runtime.get("execution_environment_snapshot_id")
        manifest_hash = runtime.get("execution_environment_manifest_sha256")
        if not isinstance(snapshot_id, str) or not snapshot_id or not isinstance(manifest_hash, str) or not manifest_hash:
            raise DurableExecutionAdapterError(
                "execution identity has no owning-node environment binding",
                reason_code="multi_alpha_execution_identity_incomplete",
                context={"child_id": child.get("child_id")},
            )
        return {
            "execution_identity_hash": identity.identity_hash,
            "execution_environment_snapshot_id": snapshot_id,
            "execution_environment_manifest_sha256": manifest_hash,
        }

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
        artifact_binding = {
            "execution_identity_hash": artifacts.artifact_manifest.get("execution_identity_hash"),
            "execution_environment_snapshot_id": None,
            "execution_environment_manifest_sha256": None,
        }
        if intent.execution_identity_hash is not None:
            artifact_identity = artifacts.artifact_manifest.get("execution_identity")
            if not isinstance(artifact_identity, Mapping):
                raise DurableExecutionAdapterError(
                    "published artifact manifest lost the durable execution identity",
                    reason_code="multi_alpha_execution_identity_hash_mismatch",
                    context={"attempt_id": intent.attempt_id},
                )
            validated_artifact_identity = validate_execution_identity(
                payload=artifact_identity,
                identity_hash=str(artifact_binding["execution_identity_hash"] or ""),
            )
            runtime = validated_artifact_identity.payload.get("runtime")
            if not isinstance(runtime, Mapping):
                raise DurableExecutionAdapterError(
                    "published artifact execution identity has no runtime section",
                    reason_code="multi_alpha_execution_identity_invalid",
                    context={"attempt_id": intent.attempt_id},
                )
            artifact_binding["execution_environment_snapshot_id"] = runtime.get(
                "execution_environment_snapshot_id"
            )
            artifact_binding["execution_environment_manifest_sha256"] = runtime.get(
                "execution_environment_manifest_sha256"
            )
        expected_binding = {
            "execution_identity_hash": intent.execution_identity_hash,
            "execution_environment_snapshot_id": intent.execution_environment_snapshot_id,
            "execution_environment_manifest_sha256": intent.execution_environment_manifest_sha256,
        }
        if artifact_binding != expected_binding:
            raise DurableExecutionAdapterError(
                "published artifact execution identity differs from the frozen remote submission intent",
                reason_code="multi_alpha_execution_identity_hash_mismatch",
                context={"attempt_id": intent.attempt_id, "expected": expected_binding, "actual": artifact_binding},
            )
        request = self._request_from_run(run)
        node = self._node_resolver(intent.node_id)
        artifact_client = (
            self._artifact_client_factory(intent.node_id)
            if self._artifact_client_factory is not None
            else WorkspaceArtifactSyncClient.for_node(intent.node_id)
        )
        l2_path = self._published_l2_artifact_path(artifacts)
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
        remote_loop_index = _remote_loop_index_from_intent(intent.qe_loop_id)
        submission_backtest_config = {
            **dict(request.backtest_config),
            "remote_task_id": intent.qe_task_id,
            "remote_loop_index": remote_loop_index,
        }
        wsl_command = _remote_wsl_command(
            workspace=artifacts.workspace,
            remote_paths=remote_paths,
            backtest_config=submission_backtest_config,
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
            loop_index=remote_loop_index,
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
            execution_identity_hash=intent.execution_identity_hash,
            execution_environment_snapshot_id=intent.execution_environment_snapshot_id,
            execution_environment_manifest_sha256=intent.execution_environment_manifest_sha256,
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

    def _stage_required_l2_artifact(
        self,
        *,
        staging: Path,
        backtest_config: Mapping[str, Any],
    ) -> str:
        """Place the exact L2 parquet inside the immutable child workspace.

        Relative paths are resolved after the runtime template is copied.  An
        explicit absolute source is copied into the canonical workspace name
        so dispatch never depends on mutable workstation-local bytes.
        """

        raw = backtest_config.get("combined_factors_path") or backtest_config.get(
            "l2_artifact_path"
        )
        if raw is None:
            relative = Path("combined_factors_df.parquet")
            source = staging / relative
        else:
            configured = Path(str(raw))
            if configured.is_absolute():
                relative = Path("combined_factors_df.parquet")
                source = configured
            else:
                relative = configured
                source = staging / relative
        if (
            not relative.parts
            or relative.is_absolute()
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise DurableExecutionAdapterError(
                "configured L2 artifact path is not a safe workspace-relative path",
                reason_code="multi_alpha_l2_artifact_path_invalid",
                context={"configured_path": str(raw), "workspace": str(staging)},
            )
        if source.is_symlink() or not source.is_file():
            raise DurableExecutionAdapterError(
                "durable materialization requires a readable regular L2 factors parquet",
                reason_code="multi_alpha_l2_artifact_missing",
                context={
                    "configured_path": str(raw) if raw is not None else None,
                    "resolved_source": str(source),
                    "workspace": str(staging),
                },
            )
        destination = staging / relative
        if source != destination:
            if destination.exists():
                source_digest = self._sha256_file(source)
                destination_digest = self._sha256_file(destination)
                if source_digest != destination_digest:
                    raise DurableExecutionAdapterError(
                        "configured L2 artifact conflicts with runtime template bytes",
                        reason_code="multi_alpha_l2_artifact_conflict",
                        context={"source": str(source), "destination": str(destination)},
                    )
            else:
                self._atomic_copy_file(source, destination)
        self._sha256_file(destination)
        return relative.as_posix()

    def _published_l2_artifact_path(
        self,
        artifacts: DurablePublishedArtifacts,
    ) -> Path:
        binding = artifacts.artifact_manifest.get("l2_artifact")
        if not isinstance(binding, Mapping):
            raise DurableExecutionAdapterError(
                "durable artifact manifest has no L2 artifact binding",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        relative = Path(str(binding.get("path") or ""))
        if (
            not relative.parts
            or relative.is_absolute()
            or any(part in {"", ".", ".."} for part in relative.parts)
        ):
            raise DurableExecutionAdapterError(
                "durable artifact manifest L2 path is invalid",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        return artifacts.workspace / relative

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
        identity = manifest.get("execution_identity")
        identity_hash = manifest.get("execution_identity_hash")
        identity_evidence = manifest.get("execution_identity_evidence")
        if identity is None:
            if identity_hash is not None:
                raise DurableExecutionAdapterError(
                    "artifact manifest has an execution identity hash without identity content",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
            if identity_evidence is not None and (
                not isinstance(identity_evidence, Mapping)
                or identity_evidence.get("complete") is not False
            ):
                raise DurableExecutionAdapterError(
                    "artifact manifest incomplete execution identity evidence is invalid",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
        else:
            if not isinstance(identity, Mapping) or not isinstance(identity_hash, str):
                raise DurableExecutionAdapterError(
                    "artifact manifest execution identity is partially persisted",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
            try:
                validate_execution_identity(payload=identity, identity_hash=identity_hash)
            except Exception as exc:
                raise DurableExecutionAdapterError(
                    "artifact manifest execution identity hash is invalid",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"error_type": type(exc).__name__, "message": str(exc)},
                ) from exc
            if not isinstance(identity_evidence, Mapping) or identity_evidence.get("complete") is not True:
                raise DurableExecutionAdapterError(
                    "artifact manifest complete execution identity evidence is invalid",
                    reason_code="multi_alpha_artifact_manifest_invalid",
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
        l2_artifact = manifest.get("l2_artifact")
        if not isinstance(l2_artifact, Mapping):
            raise DurableExecutionAdapterError(
                "durable artifact manifest has no L2 artifact binding",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        l2_relative_name = str(l2_artifact.get("path") or "")
        l2_file_metadata = files.get(l2_relative_name)
        if not isinstance(l2_file_metadata, Mapping) or dict(l2_artifact) != {
            "path": l2_relative_name,
            **dict(l2_file_metadata),
        }:
            raise DurableExecutionAdapterError(
                "durable artifact manifest L2 binding does not match its file inventory",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        for relative_name, metadata in files.items():
            if not isinstance(metadata, Mapping):
                raise DurableExecutionAdapterError(
                    "durable artifact file metadata is invalid",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
            relative = Path(str(relative_name))
            if (
                not relative.parts
                or relative.is_absolute()
                or any(part in {"", ".", ".."} for part in relative.parts)
            ):
                raise DurableExecutionAdapterError(
                    "durable artifact manifest contains an unsafe relative path",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"relative_path": str(relative_name)},
                )
            root = workspace.resolve()
            candidate = root / relative
            if candidate.is_symlink():
                raise DurableExecutionAdapterError(
                    "durable artifact must not be a symlink",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"path": str(candidate)},
                )
            path = candidate.resolve()
            try:
                path.relative_to(root)
            except ValueError as exc:
                raise DurableExecutionAdapterError(
                    "durable artifact path escapes its workspace",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"relative_path": str(relative_name)},
                ) from exc
            if not path.is_file():
                raise DurableExecutionAdapterError(
                    "durable artifact must be a regular non-symlink file",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"path": str(path)},
                )
            digest, size = self._sha256_file(path)
            if digest != metadata.get("sha256") or size != int(metadata.get("size") or -1):
                raise DurableExecutionAdapterError(
                    "durable artifact bytes do not match the published manifest",
                    reason_code="multi_alpha_artifact_hash_mismatch",
                    context={"path": str(path)},
                )
        external_bindings = manifest.get("external_runtime_data_bindings", [])
        if not isinstance(external_bindings, list):
            raise DurableExecutionAdapterError(
                "durable artifact external runtime data bindings are invalid",
                reason_code="multi_alpha_artifact_manifest_invalid",
            )
        observed_binding_names: set[str] = set()
        for binding in external_bindings:
            if not isinstance(binding, Mapping):
                raise DurableExecutionAdapterError(
                    "durable artifact external runtime data binding is not an object",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                )
            name = str(binding.get("name") or "")
            if (
                name not in RUNTIME_EXTERNAL_DATA_LINK_NAMES
                or Path(name).name != name
                or binding.get("binding") != "node_canonical_qe_data"
                or binding.get("published") is not False
                or name in observed_binding_names
                or name in files
            ):
                raise DurableExecutionAdapterError(
                    "durable artifact external runtime data binding is inconsistent",
                    reason_code="multi_alpha_artifact_manifest_invalid",
                    context={"binding": dict(binding)},
                )
            observed_binding_names.add(name)

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
