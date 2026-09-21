"""Official code-owned adapters for monthly release stages after SOURCE.

The adapters deliberately separate data-plane execution from evidence
construction.  Executors perform the bounded build/readback operation and
return typed results; this module derives every monthly receipt field from
files and prior stage identities.  API, CLI and operation state therefore
cannot submit a PASS string, an executable command, or a claimed hash.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_registry import OfficialMonthlyProducerRegistry
from .monthly_source_producer import AuditedMonthlySourceProducer
from .monthly_stage_adapter import MonthlyStageArtifact, MonthlyStageResult
from .monthly_unified import (
    COMPONENTS,
    CONSUMER_READBACK_SCHEMA,
    NODE_REGISTRATION_SCHEMA,
    RELEASE_CLOSURE_SCHEMA,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
    TELEMETRY_COUNT_FIELDS,
)
from .monthly_worker import ProducerContext


DERIVED_ASSET_REGISTRY_SCHEMA = "aistock_dataset_derived_asset_registry_v1"
OFFICIAL_ADAPTER_VERSION = "1"


class OfficialMonthlyAdapterError(RuntimeError):
    """A code-owned executor returned incomplete or inconsistent evidence."""


def _contract_digest(stage: str) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "schema_version": "aistock_monthly_official_stage_contract_v1",
                "stage": stage,
                "adapter_version": OFFICIAL_ADAPTER_VERSION,
                "evidence_mode": "typed_files_and_readback",
            }
        )
    ).hexdigest()


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def _plain_file(path: Path, *, label: str) -> Path:
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise OfficialMonthlyAdapterError(f"{label} is unavailable") from exc
    if _is_link(path) or not resolved.is_file():
        raise OfficialMonthlyAdapterError(f"{label} must be a regular non-link file")
    return resolved


def _artifact(
    roots: Path | Sequence[Path], path: Path, *, label: str
) -> MonthlyStageArtifact:
    registered = (roots,) if isinstance(roots, Path) else tuple(roots)
    resolved_roots = tuple(root.resolve(strict=True) for root in registered)
    if not resolved_roots or len(set(resolved_roots)) != len(resolved_roots):
        raise OfficialMonthlyAdapterError("registered artifact roots are empty or duplicated")
    resolved = _plain_file(path, label=label)
    matches = [root for root in resolved_roots if resolved.is_relative_to(root)]
    if len(matches) != 1:
        raise OfficialMonthlyAdapterError(
            f"{label} must resolve under exactly one registered artifact root"
        )
    relative = resolved.relative_to(matches[0]).as_posix()
    return MonthlyStageArtifact(relative, resolved)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _content_ref(
    roots: Path | Sequence[Path], path: Path, *, label: str
) -> dict[str, Any]:
    item = _artifact(roots, path, label=label)
    return {
        "id": item.artifact_id,
        "sha256": _sha256(item.path),
        "size": item.path.stat().st_size,
    }


def _read_canonical_object(path: Path, *, label: str) -> Mapping[str, Any]:
    resolved = _plain_file(path, label=label)
    try:
        raw = resolved.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfficialMonthlyAdapterError(f"{label} is not readable JSON") from exc
    if not isinstance(value, Mapping) or raw != canonical_json_bytes(value) + b"\n":
        raise OfficialMonthlyAdapterError(f"{label} must be canonical JSON")
    return value


def _write_canonical_exclusive(path: Path, value: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = canonical_json_bytes(value) + b"\n"
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise OfficialMonthlyAdapterError("official stage evidence already exists") from exc
    return path


def _stage_root(artifact_root: Path, context: ProducerContext) -> Path:
    root = (
        artifact_root
        / "monthly"
        / context.operation_id
        / context.stage.lower()
        / f"attempt-{context.attempt}"
    )
    root.mkdir(parents=True, exist_ok=False)
    return root


def _prior_scope(context: ProducerContext, stage: str) -> Mapping[str, Any]:
    receipt = context.prior_receipts.get(stage)
    scope = receipt.get("scope") if isinstance(receipt, Mapping) else None
    if not isinstance(scope, Mapping):
        raise OfficialMonthlyAdapterError(f"{context.stage} requires the {stage} receipt")
    return scope


def _manifest_identity(context: ProducerContext) -> str:
    scope = _prior_scope(context, "BUILD")
    value = str(scope.get("dataset_manifest_sha256") or "")
    ensure_sha256(value, field="dataset_manifest_sha256")
    return value


def _adapter_roots(adapter: Any) -> tuple[Path, ...]:
    return (adapter.artifact_root, *adapter.additional_artifact_roots)


@dataclass(frozen=True, slots=True)
class StageWorkload:
    source_rows_read: int = 0
    computed_rows: int = 0
    bytes_transferred: int = 0

    def __post_init__(self) -> None:
        values = (self.source_rows_read, self.computed_rows, self.bytes_transferred)
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("stage workload counts must be non-negative integers")


def _counts(
    *,
    started: float,
    outputs: Sequence[MonthlyStageArtifact],
    inputs: Sequence[MonthlyStageArtifact],
    workload: StageWorkload,
) -> dict[str, int]:
    values = {
        "unexplained_gap_count": 0,
        "source_rows_read": workload.source_rows_read,
        "computed_rows": workload.computed_rows,
        "files_written": len(outputs),
        "bytes_written": sum(item.path.stat().st_size for item in outputs),
        "bytes_transferred": workload.bytes_transferred,
        "bytes_hashed": sum(item.path.stat().st_size for item in (*inputs, *outputs)),
        "elapsed_ms": max(0, int((time.monotonic() - started) * 1000)),
    }
    if set(values) != set(TELEMETRY_COUNT_FIELDS):  # pragma: no cover - contract sentinel
        raise OfficialMonthlyAdapterError("official telemetry contract drifted")
    return values


@dataclass(frozen=True, slots=True)
class BuildExecution:
    manifest_path: Path
    component_artifacts: tuple[Path, ...]
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()


class MonthlyBuildExecutor(Protocol):
    def execute(
        self, context: ProducerContext, *, component_actions: Mapping[str, str]
    ) -> BuildExecution: ...


@dataclass(frozen=True, slots=True)
class OfficialBuildAdapter:
    artifact_root: Path
    executor: MonthlyBuildExecutor
    additional_artifact_roots: tuple[Path, ...] = ()
    stage: str = "BUILD"
    adapter_id: str = "aistock.monthly.build.official"
    adapter_version: str = OFFICIAL_ADAPTER_VERSION
    contract_sha256: str = _contract_digest("BUILD")

    def execute(self, context: ProducerContext) -> MonthlyStageResult:
        started = time.monotonic()
        source_scope = _prior_scope(context, "SOURCE")
        actions = source_scope.get("component_actions")
        if not isinstance(actions, Mapping) or set(actions) != set(COMPONENTS):
            raise OfficialMonthlyAdapterError("SOURCE did not freeze every component action")
        result = self.executor.execute(
            context,
            component_actions={str(key): str(value) for key, value in actions.items()},
        )
        manifest = _read_canonical_object(result.manifest_path, label="dataset manifest")
        identity = str(manifest.get("dataset_manifest_sha256") or "")
        ensure_sha256(identity, field="dataset_manifest_sha256")
        output_paths = (result.manifest_path, *result.component_artifacts)
        if len({path.resolve(strict=True) for path in output_paths}) != len(output_paths):
            raise OfficialMonthlyAdapterError("build output files are duplicated")
        inputs = tuple(
            _artifact(_adapter_roots(self), path, label="build input")
            for path in result.input_artifacts
        )
        outputs = tuple(
            _artifact(_adapter_roots(self), path, label="build output") for path in output_paths
        )
        predecessor = context.plan.get("predecessor")
        if not isinstance(predecessor, Mapping):
            raise OfficialMonthlyAdapterError("build predecessor is unavailable")
        return MonthlyStageResult(
            scope={
                "dataset_manifest_sha256": identity,
                "dataset_manifest_ref": _content_ref(
                    _adapter_roots(self), result.manifest_path, label="dataset manifest"
                ),
                "predecessor_manifest_sha256": predecessor.get("dataset_manifest_sha256"),
                "component_actions": dict(actions),
            },
            input_artifacts=inputs,
            output_artifacts=outputs,
            counts=_counts(started=started, outputs=outputs, inputs=inputs, workload=result.workload),
        )


@dataclass(frozen=True, slots=True)
class DerivedAsset:
    asset_id: str
    path: Path
    schema_version: str

    def __post_init__(self) -> None:
        if not self.asset_id.strip() or not self.schema_version.strip():
            raise ValueError("derived asset identity is incomplete")


@dataclass(frozen=True, slots=True)
class DeriveExecution:
    assets: tuple[DerivedAsset, ...]
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()


class MonthlyDeriveExecutor(Protocol):
    def execute(self, context: ProducerContext, *, dataset_manifest_sha256: str) -> DeriveExecution: ...


@dataclass(frozen=True, slots=True)
class OfficialDeriveAdapter:
    artifact_root: Path
    executor: MonthlyDeriveExecutor
    additional_artifact_roots: tuple[Path, ...] = ()
    stage: str = "DERIVE"
    adapter_id: str = "aistock.monthly.derive.official"
    adapter_version: str = OFFICIAL_ADAPTER_VERSION
    contract_sha256: str = _contract_digest("DERIVE")

    def execute(self, context: ProducerContext) -> MonthlyStageResult:
        started = time.monotonic()
        manifest_sha = _manifest_identity(context)
        result = self.executor.execute(context, dataset_manifest_sha256=manifest_sha)
        if not result.assets or len({item.asset_id for item in result.assets}) != len(result.assets):
            raise OfficialMonthlyAdapterError("derived assets must be non-empty and uniquely named")
        inputs = tuple(
            _artifact(_adapter_roots(self), path, label="derive input")
            for path in result.input_artifacts
        )
        asset_artifacts = tuple(
            _artifact(_adapter_roots(self), item.path, label="derived asset")
            for item in result.assets
        )
        root = _stage_root(self.artifact_root, context)
        rows = []
        for item, artifact in zip(result.assets, asset_artifacts, strict=True):
            ref = _content_ref(_adapter_roots(self), artifact.path, label="derived asset")
            rows.append(
                {
                    "asset_id": item.asset_id,
                    "path": ref["id"],
                    "sha256": ref["sha256"],
                    "size": ref["size"],
                    "schema_version": item.schema_version,
                }
            )
        registry_path = _write_canonical_exclusive(
            root / "derived-asset-registry.json",
            {
                "schema_version": DERIVED_ASSET_REGISTRY_SCHEMA,
                "source_dataset_manifest_sha256": manifest_sha,
                "assets": rows,
            },
        )
        registry_artifact = _artifact(_adapter_roots(self), registry_path, label="derived registry")
        outputs = (*asset_artifacts, registry_artifact)
        asset_refs = [
            _content_ref(_adapter_roots(self), item.path, label="derived asset")
            for item in result.assets
        ]
        registry_ref = _content_ref(_adapter_roots(self), registry_path, label="derived registry")
        return MonthlyStageResult(
            scope={
                "dataset_manifest_sha256": manifest_sha,
                "source_dataset_manifest_sha256": manifest_sha,
                "hmm_fit_count": 0,
                "training_started": False,
                "derived_assets": asset_refs,
                "derived_asset_registry_ref": registry_ref,
                "derived_asset_registry_sha256": registry_ref["sha256"],
            },
            input_artifacts=inputs,
            output_artifacts=outputs,
            counts=_counts(started=started, outputs=outputs, inputs=inputs, workload=result.workload),
        )


@dataclass(frozen=True, slots=True)
class LocalValidationExecution:
    pool_gap_counts: Mapping[str, int]
    dataset_identity_complete: bool
    consumer_contracts: tuple[Path, ...]
    source_readiness: tuple[Path, ...]
    component_validations: tuple[Path, ...]
    lineage_path: Path
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()


class MonthlyLocalValidationExecutor(Protocol):
    def execute(
        self, context: ProducerContext, *, dataset_manifest_sha256: str
    ) -> LocalValidationExecution: ...


@dataclass(frozen=True, slots=True)
class OfficialLocalValidateAdapter:
    artifact_root: Path
    executor: MonthlyLocalValidationExecutor
    additional_artifact_roots: tuple[Path, ...] = ()
    stage: str = "LOCAL_VALIDATE"
    adapter_id: str = "aistock.monthly.local_validate.official"
    adapter_version: str = OFFICIAL_ADAPTER_VERSION
    contract_sha256: str = _contract_digest("LOCAL_VALIDATE")

    def execute(self, context: ProducerContext) -> MonthlyStageResult:
        started = time.monotonic()
        manifest_sha = _manifest_identity(context)
        result = self.executor.execute(context, dataset_manifest_sha256=manifest_sha)
        pool_names = {"stock_universe", "csi300", "csi500", "csi1000", "star50", "star100"}
        if set(result.pool_gap_counts) != pool_names or any(
            type(value) is not int or value != 0 for value in result.pool_gap_counts.values()
        ):
            raise OfficialMonthlyAdapterError("local validation did not close all six pools")
        if result.dataset_identity_complete is not True:
            raise OfficialMonthlyAdapterError("local dataset identity is incomplete")
        build_scope = _prior_scope(context, "BUILD")
        derive_scope = _prior_scope(context, "DERIVE")
        manifest_ref = build_scope.get("dataset_manifest_ref")
        derived_refs = derive_scope.get("derived_assets")
        if not isinstance(manifest_ref, Mapping) or not isinstance(derived_refs, list) or not derived_refs:
            raise OfficialMonthlyAdapterError("local validation inputs are incomplete")

        def refs(paths: Sequence[Path], label: str) -> list[dict[str, Any]]:
            if not paths:
                raise OfficialMonthlyAdapterError(f"{label} evidence is empty")
            return [_content_ref(_adapter_roots(self), path, label=label) for path in paths]

        closure: dict[str, Any] = {
            "schema_version": RELEASE_CLOSURE_SCHEMA,
            "dataset_manifest_ref": dict(manifest_ref),
            "derived_asset_refs": [dict(item) for item in derived_refs],
            "consumer_contract_refs": refs(result.consumer_contracts, "consumer contract"),
            "source_readiness_refs": refs(result.source_readiness, "source readiness"),
            "component_validation_refs": refs(
                result.component_validations, "component validation"
            ),
            "lineage_ref": _content_ref(
                _adapter_roots(self), result.lineage_path, label="release lineage"
            ),
        }
        closure["canonical_sha256"] = hashlib.sha256(canonical_json_bytes(closure)).hexdigest()
        root = _stage_root(self.artifact_root, context)
        closure_path = _write_canonical_exclusive(root / "release-closure.json", closure)
        inputs = tuple(
            _artifact(_adapter_roots(self), path, label="local validation input")
            for path in result.input_artifacts
        )
        output_paths = (
            *result.consumer_contracts,
            *result.source_readiness,
            *result.component_validations,
            result.lineage_path,
            closure_path,
        )
        outputs = tuple(
            _artifact(_adapter_roots(self), path, label="local validation output")
            for path in output_paths
        )
        closure_ref = _content_ref(_adapter_roots(self), closure_path, label="release closure")
        return MonthlyStageResult(
            scope={
                "dataset_manifest_sha256": manifest_sha,
                "pool_gap_counts": dict(result.pool_gap_counts),
                "dataset_identity_complete": True,
                "release_closure": closure,
                "release_closure_ref": closure_ref,
                "release_closure_file_sha256": closure_ref["sha256"],
            },
            input_artifacts=inputs,
            output_artifacts=outputs,
            counts=_counts(started=started, outputs=outputs, inputs=inputs, workload=result.workload),
        )


@dataclass(frozen=True, slots=True)
class NodeDeployment:
    node_id: str
    candidate_root: str
    manifest_sha256: str
    relative_files: tuple[Path, ...]
    deployment_receipt: Path


@dataclass(frozen=True, slots=True)
class DeployExecution:
    nodes: tuple[NodeDeployment, ...]
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()


class MonthlyDeployExecutor(Protocol):
    def execute(self, context: ProducerContext, *, dataset_manifest_sha256: str) -> DeployExecution: ...


@dataclass(frozen=True, slots=True)
class OfficialDeployAdapter:
    artifact_root: Path
    executor: MonthlyDeployExecutor
    additional_artifact_roots: tuple[Path, ...] = ()
    stage: str = "DEPLOY"
    adapter_id: str = "aistock.monthly.deploy.official"
    adapter_version: str = OFFICIAL_ADAPTER_VERSION
    contract_sha256: str = _contract_digest("DEPLOY")

    def execute(self, context: ProducerContext) -> MonthlyStageResult:
        started = time.monotonic()
        manifest_sha = _manifest_identity(context)
        result = self.executor.execute(context, dataset_manifest_sha256=manifest_sha)
        by_node = {item.node_id: item for item in result.nodes}
        if len(by_node) != len(result.nodes) or set(by_node) != set(REQUIRED_NODES):
            raise OfficialMonthlyAdapterError("deployment did not cover the exact node registry")
        if {item.manifest_sha256 for item in result.nodes} != {manifest_sha}:
            raise OfficialMonthlyAdapterError("deployment node manifest identities differ")
        local_scope = _prior_scope(context, "LOCAL_VALIDATE")
        closure_ref = local_scope.get("release_closure_ref")
        build_scope = _prior_scope(context, "BUILD")
        manifest_ref = build_scope.get("dataset_manifest_ref")
        if not isinstance(closure_ref, Mapping) or not isinstance(manifest_ref, Mapping):
            raise OfficialMonthlyAdapterError("deployment input identities are incomplete")
        root = _stage_root(self.artifact_root, context)
        registrations: dict[str, Mapping[str, Any]] = {}
        registration_paths: list[Path] = []
        for node_id in REQUIRED_NODES:
            item = by_node[node_id]
            file_refs = [
                _content_ref(_adapter_roots(self), path, label=f"{node_id} deployed file")
                for path in item.relative_files
            ]
            if not file_refs:
                raise OfficialMonthlyAdapterError(f"{node_id} deployment file set is empty")
            receipt_ref = _content_ref(
                _adapter_roots(self),
                item.deployment_receipt,
                label=f"{node_id} deployment receipt",
            )
            registration = {
                "schema_version": NODE_REGISTRATION_SCHEMA,
                "node_id": node_id,
                "release_id": context.plan.get("release_id"),
                "dataset_manifest_ref": dict(manifest_ref),
                "closure_ref": dict(closure_ref),
                "candidate_root": item.candidate_root,
                "relative_file_refs": file_refs,
                "deployment_receipt_ref": receipt_ref,
            }
            path = _write_canonical_exclusive(root / f"{node_id}-registration.json", registration)
            registrations[node_id] = registration
            registration_paths.append(path)
        inputs = tuple(
            _artifact(_adapter_roots(self), path, label="deployment input")
            for path in result.input_artifacts
        )
        output_paths = (
            *(item.deployment_receipt for item in result.nodes),
            *registration_paths,
        )
        outputs = tuple(
            _artifact(_adapter_roots(self), path, label="deployment output")
            for path in output_paths
        )
        return MonthlyStageResult(
            scope={
                "dataset_manifest_sha256": manifest_sha,
                "nodes": list(REQUIRED_NODES),
                "node_manifest_sha256": {node: manifest_sha for node in REQUIRED_NODES},
                "node_registrations": registrations,
                "node_registration_refs": {
                    node: _content_ref(
                        _adapter_roots(self),
                        registration_paths[index],
                        label=f"{node} node registration",
                    )
                    for index, node in enumerate(REQUIRED_NODES)
                },
            },
            input_artifacts=inputs,
            output_artifacts=outputs,
            counts=_counts(started=started, outputs=outputs, inputs=inputs, workload=result.workload),
        )


@dataclass(frozen=True, slots=True)
class ConsumerReadback:
    consumer_id: str
    node_id: str
    binding_path: Path
    required_window: Mapping[str, Any]
    resolved_component_paths: tuple[Path, ...]
    derived_asset_paths: tuple[Path, ...]
    coverage_counts: Mapping[str, int]
    adapter_version: str
    result_path: Path


@dataclass(frozen=True, slots=True)
class ConsumerValidationExecution:
    readbacks: tuple[ConsumerReadback, ...]
    input_artifacts: tuple[Path, ...] = ()
    workload: StageWorkload = StageWorkload()


class MonthlyConsumerValidationExecutor(Protocol):
    def execute(
        self, context: ProducerContext, *, dataset_manifest_sha256: str
    ) -> ConsumerValidationExecution: ...


@dataclass(frozen=True, slots=True)
class OfficialConsumerValidateAdapter:
    artifact_root: Path
    executor: MonthlyConsumerValidationExecutor
    additional_artifact_roots: tuple[Path, ...] = ()
    stage: str = "CONSUMER_VALIDATE"
    adapter_id: str = "aistock.monthly.consumer_validate.official"
    adapter_version: str = OFFICIAL_ADAPTER_VERSION
    contract_sha256: str = _contract_digest("CONSUMER_VALIDATE")

    def execute(self, context: ProducerContext) -> MonthlyStageResult:
        started = time.monotonic()
        manifest_sha = _manifest_identity(context)
        result = self.executor.execute(context, dataset_manifest_sha256=manifest_sha)
        by_name = {item.consumer_id: item for item in result.readbacks}
        if len(by_name) != len(result.readbacks) or set(by_name) != set(REQUIRED_CONSUMERS):
            raise OfficialMonthlyAdapterError("consumer validation coverage differs")
        root = _stage_root(self.artifact_root, context)
        readbacks: dict[str, Mapping[str, Any]] = {}
        readback_paths: list[Path] = []
        for name in REQUIRED_CONSUMERS:
            item = by_name[name]
            if not item.node_id.strip() or not item.adapter_version.strip():
                raise OfficialMonthlyAdapterError(f"consumer identity is incomplete: {name}")
            if any(type(value) is not int or value < 0 for value in item.coverage_counts.values()):
                raise OfficialMonthlyAdapterError(f"consumer coverage counts are invalid: {name}")
            readback = {
                "schema_version": CONSUMER_READBACK_SCHEMA,
                "consumer_id": name,
                "node_id": item.node_id,
                "binding_ref": _content_ref(
                    _adapter_roots(self), item.binding_path, label=f"{name} binding"
                ),
                "required_window": dict(item.required_window),
                "resolved_component_refs": [
                    _content_ref(_adapter_roots(self), path, label=f"{name} component")
                    for path in item.resolved_component_paths
                ],
                "derived_asset_refs": [
                    _content_ref(_adapter_roots(self), path, label=f"{name} derived asset")
                    for path in item.derived_asset_paths
                ],
                "coverage_counts": dict(item.coverage_counts),
                "command_or_adapter_version": item.adapter_version,
                "result_ref": _content_ref(
                    _adapter_roots(self), item.result_path, label=f"{name} result"
                ),
                "side_effect_flags": {
                    "outcomes_read": False,
                    "training_started": False,
                    "experiment_started": False,
                    "runtime_action_performed": False,
                },
                "dataset_manifest_sha256": manifest_sha,
            }
            if not readback["resolved_component_refs"] or not readback["derived_asset_refs"]:
                raise OfficialMonthlyAdapterError(f"consumer resolved file set is empty: {name}")
            path = _write_canonical_exclusive(root / f"{name}-readback.json", readback)
            readbacks[name] = readback
            readback_paths.append(path)
        inputs = tuple(
            _artifact(_adapter_roots(self), path, label="consumer validation input")
            for path in result.input_artifacts
        )
        outputs = tuple(
            _artifact(_adapter_roots(self), path, label="consumer readback")
            for path in readback_paths
        )
        return MonthlyStageResult(
            scope={
                "dataset_manifest_sha256": manifest_sha,
                "consumers": list(REQUIRED_CONSUMERS),
                "consumer_readbacks": readbacks,
                "consumer_readback_refs": {
                    name: _content_ref(
                        _adapter_roots(self),
                        readback_paths[index],
                        label=f"{name} consumer readback",
                    )
                    for index, name in enumerate(REQUIRED_CONSUMERS)
                },
                "outcomes_read": False,
                "training_started": False,
                "experiment_started": False,
                "runtime_action_performed": False,
            },
            input_artifacts=inputs,
            output_artifacts=outputs,
            counts=_counts(started=started, outputs=outputs, inputs=inputs, workload=result.workload),
        )


def build_official_monthly_registry(
    *,
    source: AuditedMonthlySourceProducer,
    build: OfficialBuildAdapter,
    derive: OfficialDeriveAdapter,
    local_validate: OfficialLocalValidateAdapter,
    deploy: OfficialDeployAdapter,
    consumer_validate: OfficialConsumerValidateAdapter,
) -> OfficialMonthlyProducerRegistry:
    """Compose the exact production stage set without configurable commands."""

    from .monthly_stage_adapter import CodeOwnedMonthlyStageProducer

    if not isinstance(source, AuditedMonthlySourceProducer):
        raise OfficialMonthlyAdapterError(
            "official SOURCE must use the audited snapshot producer"
        )

    return OfficialMonthlyProducerRegistry(
        source=source,
        build=CodeOwnedMonthlyStageProducer(build),
        derive=CodeOwnedMonthlyStageProducer(derive),
        local_validate=CodeOwnedMonthlyStageProducer(local_validate),
        deploy=CodeOwnedMonthlyStageProducer(deploy),
        consumer_validate=CodeOwnedMonthlyStageProducer(consumer_validate),
    )


__all__: Sequence[str] = (
    "BuildExecution",
    "ConsumerReadback",
    "ConsumerValidationExecution",
    "DeployExecution",
    "DerivedAsset",
    "DeriveExecution",
    "LocalValidationExecution",
    "MonthlyBuildExecutor",
    "MonthlyConsumerValidationExecutor",
    "MonthlyDeployExecutor",
    "MonthlyDeriveExecutor",
    "MonthlyLocalValidationExecutor",
    "NodeDeployment",
    "OfficialBuildAdapter",
    "OfficialConsumerValidateAdapter",
    "OfficialDeployAdapter",
    "OfficialDeriveAdapter",
    "OfficialLocalValidateAdapter",
    "OfficialMonthlyAdapterError",
    "StageWorkload",
    "build_official_monthly_registry",
)
