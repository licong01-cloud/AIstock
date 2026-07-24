"""Canonical, content-based identity for QE multi-alpha execution.

This module intentionally performs no environment probing and never treats a
filesystem path as proof of content. Callers supply manifests captured by the
dataset/runtime owning services; this module freezes and verifies their
identity before P0-2 recovery or remote submission uses them.
"""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from backend.services.multi_alpha.durable_models import (
    DurableContractError,
    canonical_json,
    sha256_identity,
)
from backend.services.model_store import ModelStoreService
from backend.services.multi_alpha.combine_backtest import (
    RUNTIME_EXTERNAL_DATA_LINK_NAMES,
    CombineBacktestRequest,
    is_runtime_external_data_link,
)
from backend.services.multi_alpha.durable_plan import PLANNER_VERSION
from backend.services.multi_alpha.remote_dispatch import get_compute_node_info
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceClient,
    QEWorkspaceDatasetIdentity,
    QEWorkspaceExecutionEnvironment,
)


EXECUTION_IDENTITY_SCHEMA_VERSION = "multi_alpha_execution_identity_v1"

_DATASET_FIELDS = (
    "deployment_snapshot_id",
    "dataset_manifest_sha256",
    "cutoff_trade_date",
    "qlib_calendar_sha256",
    "qlib_instruments_sha256",
    "st_pit_snapshot_id",
    "st_pit_manifest_sha256",
    "resolved_node_id",
    "resolved_data_root_uri",
)
_RUNTIME_FIELDS = (
    "qlib_runtime_template_sha256",
    "conda_environment_lock_sha256",
    "execution_environment_snapshot_id",
    "execution_environment_manifest_sha256",
    "executor_code_commit",
    "executor_file_set_sha256",
    "backtest_config_sha256",
)
_MATERIALIZER_FIELDS = (
    "aistock_commit",
    "planner_version",
    "combiner_file_sha256",
    "panel_builder_file_sha256",
    "materializer_file_set_sha256",
)
_BUSINESS_FORMULA_FIELDS = (
    "formula_version",
    "assembler_file_sha256",
    "delta_formula_sha256",
)
_PREDICTION_SOURCE_FIELDS = ("leg_id", "seed_run_id", "artifact_uri", "artifact_sha256")
_IDENTITY_EVIDENCE_SCHEMA_VERSION = "multi_alpha_execution_identity_evidence_v1"

EnvironmentLoader = Callable[[str], QEWorkspaceExecutionEnvironment]
DatasetLoader = Callable[[str, str | None], QEWorkspaceDatasetIdentity]


@dataclass(frozen=True)
class ExecutionIdentity:
    payload: Mapping[str, Any]
    identity_hash: str
    identity_source: str

    def __post_init__(self) -> None:
        if self.identity_hash != sha256_identity(dict(self.payload)):
            raise DurableContractError(
                "execution identity hash does not match canonical payload",
                reason_code="multi_alpha_execution_identity_hash_mismatch",
                context={"expected": sha256_identity(dict(self.payload)), "actual": self.identity_hash},
            )


def build_execution_identity(
    *,
    dataset: Mapping[str, Any],
    prediction_sources: Sequence[Mapping[str, Any]],
    runtime: Mapping[str, Any],
    materializer: Mapping[str, Any],
    business_formula: Mapping[str, Any],
    identity_source: str = "runtime_manifest",
) -> ExecutionIdentity:
    """Build a canonical P0-2 execution identity from content manifests.

    All required evidence is explicit. A missing field is reported through a
    structured contract error rather than silently falling back to a current
    path, runtime default, or guessed configuration.
    """

    _require_mapping_fields(dataset, _DATASET_FIELDS, section="dataset")
    _require_mapping_fields(runtime, _RUNTIME_FIELDS, section="runtime")
    _require_mapping_fields(materializer, _MATERIALIZER_FIELDS, section="materializer")
    _require_mapping_fields(business_formula, _BUSINESS_FORMULA_FIELDS, section="business_formula")
    if not isinstance(prediction_sources, Sequence) or isinstance(prediction_sources, (str, bytes)):
        raise DurableContractError(
            "prediction_sources must be an array of source objects",
            reason_code="multi_alpha_execution_identity_invalid",
        )
    if not prediction_sources:
        raise DurableContractError(
            "prediction_sources must not be empty",
            reason_code="multi_alpha_execution_identity_invalid",
        )

    normalized_sources: list[dict[str, str]] = []
    seen_leg_seed: set[tuple[str, str]] = set()
    for raw_source in prediction_sources:
        _require_mapping_fields(raw_source, _PREDICTION_SOURCE_FIELDS, section="prediction_source")
        normalized_source = {field: _required_text(raw_source[field], field=f"prediction_source.{field}") for field in _PREDICTION_SOURCE_FIELDS}
        leg_seed = (normalized_source["leg_id"], normalized_source["seed_run_id"])
        if leg_seed in seen_leg_seed:
            raise DurableContractError(
                "prediction source identity contains duplicate leg/seed pair",
                reason_code="multi_alpha_execution_identity_invalid",
                context={"leg_id": leg_seed[0], "seed_run_id": leg_seed[1]},
            )
        seen_leg_seed.add(leg_seed)
        normalized_sources.append(normalized_source)

    if not isinstance(identity_source, str) or not identity_source.strip():
        raise DurableContractError(
            "identity_source must be a non-empty string",
            reason_code="multi_alpha_execution_identity_invalid",
        )
    payload = {
        "schema_version": EXECUTION_IDENTITY_SCHEMA_VERSION,
        "identity_source": identity_source,
        "dataset": _normalize_mapping(dataset, _DATASET_FIELDS, section="dataset"),
        "prediction_sources": sorted(
            normalized_sources,
            key=lambda item: (item["leg_id"], item["seed_run_id"], item["artifact_sha256"]),
        ),
        "runtime": _normalize_mapping(runtime, _RUNTIME_FIELDS, section="runtime"),
        "materializer": _normalize_mapping(materializer, _MATERIALIZER_FIELDS, section="materializer"),
        "business_formula": _normalize_mapping(
            business_formula,
            _BUSINESS_FORMULA_FIELDS,
            section="business_formula",
        ),
    }
    return ExecutionIdentity(
        payload=payload,
        identity_hash=sha256_identity(payload),
        identity_source=identity_source,
    )


def validate_execution_identity(
    *,
    payload: Mapping[str, Any],
    identity_hash: str,
) -> ExecutionIdentity:
    """Validate a persisted identity without reinterpreting its source paths."""

    if not isinstance(payload, Mapping):
        raise DurableContractError(
            "execution identity payload must be an object",
            reason_code="multi_alpha_execution_identity_invalid",
        )
    expected_hash = sha256_identity(dict(payload))
    if identity_hash != expected_hash:
        raise DurableContractError(
            "persisted execution identity hash mismatch",
            reason_code="multi_alpha_execution_identity_hash_mismatch",
            context={"expected": expected_hash, "actual": identity_hash},
        )
    if payload.get("schema_version") != EXECUTION_IDENTITY_SCHEMA_VERSION:
        raise DurableContractError(
            "unsupported execution identity schema version",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"schema_version": payload.get("schema_version")},
        )
    identity_source = _required_text(payload.get("identity_source"), field="identity_source")
    # Rebuild through the same normalizer so persisted input cannot omit a
    # required content identity field while retaining a syntactically valid hash.
    rebuilt = build_execution_identity(
        dataset=_as_mapping(payload.get("dataset"), field="dataset"),
        prediction_sources=_as_sequence(payload.get("prediction_sources"), field="prediction_sources"),
        runtime=_as_mapping(payload.get("runtime"), field="runtime"),
        materializer=_as_mapping(payload.get("materializer"), field="materializer"),
        business_formula=_as_mapping(payload.get("business_formula"), field="business_formula"),
        identity_source=identity_source,
    )
    if rebuilt.identity_hash != identity_hash or canonical_json(rebuilt.payload) != canonical_json(dict(payload)):
        raise DurableContractError(
            "execution identity canonical reconstruction mismatch",
            reason_code="multi_alpha_execution_identity_hash_mismatch",
            context={"expected": rebuilt.identity_hash, "actual": identity_hash},
        )
    return rebuilt


def legacy_execution_identity_evidence(
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Describe historical identity evidence without inventing missing facts.

    The caller can show this report to research users and acquire the missing
    manifest. It never classifies a research direction as rejected.
    """

    if payload is None:
        return {
            "identity_source": "legacy_missing",
            "complete": False,
            "reason_code": "legacy_execution_identity_incomplete",
            "missing": ["execution_identity_payload"],
            "acquisition_suggestions": [
                "locate durable child/attempt manifests",
                "locate immutable dataset and runtime deployment manifests",
                "run a separately identified new QE research run if historical contents cannot be reconstructed",
            ],
        }
    try:
        identity_hash = sha256_identity(dict(payload))
        rebuilt = validate_execution_identity(payload=payload, identity_hash=identity_hash)
    except DurableContractError as exc:
        return {
            "identity_source": str(payload.get("identity_source") or "historical_manifest_reconstruction"),
            "complete": False,
            "reason_code": "legacy_execution_identity_incomplete",
            "missing": list(exc.context.get("missing", [])) or [str(exc)],
            "acquisition_suggestions": [
                "locate the missing content manifest rather than reusing a mutable path",
                "retain historical results for analysis with explicit evidence limitations",
            ],
        }
    return {
        "identity_source": rebuilt.identity_source,
        "complete": True,
        "reason_code": None,
        "missing": [],
        "identity_hash": rebuilt.identity_hash,
        "acquisition_suggestions": [],
    }


@dataclass(frozen=True)
class ExecutionIdentityResolution:
    """The exact identity when available, otherwise durable evidence of its gap.

    Identity incompleteness is not a research-admission result.  The normal QE
    run remains valid for analysis; only an exact recovery mode later exposes
    which frozen content evidence still needs to be acquired.
    """

    identity: ExecutionIdentity | None
    evidence: Mapping[str, Any]

    @property
    def complete(self) -> bool:
        return self.identity is not None

    def input_manifest_fragment(self) -> dict[str, Any]:
        return {
            "execution_identity": dict(self.identity.payload) if self.identity is not None else None,
            "execution_identity_hash": self.identity.identity_hash if self.identity is not None else None,
            "execution_identity_evidence": dict(self.evidence),
        }


class DurableExecutionIdentityResolver:
    """Capture verifiable P0-2 content identity before durable child planning.

    Every source is either content-addressed (published dataset manifest,
    prediction blob, runtime template, owning-node environment manifest, or
    source file hash) or recorded as explicit incomplete evidence.  There is no
    path-only substitute and no implicit switch to a different recovery mode.
    """

    def __init__(
        self,
        *,
        model_store: ModelStoreService | None = None,
        environment_loader: EnvironmentLoader | None = None,
        dataset_loader: DatasetLoader | None = None,
        node_info_resolver: Callable[[str], Any] = get_compute_node_info,
        source_root: str | Path | None = None,
    ) -> None:
        self._model_store = model_store or ModelStoreService()
        self._environment_loader = environment_loader or _load_environment_from_workspace
        self._dataset_loader = dataset_loader or _load_dataset_from_workspace
        self._node_info_resolver = node_info_resolver
        self._source_root = Path(source_root or Path(__file__).resolve().parents[3])

    def resolve(
        self,
        *,
        request: CombineBacktestRequest,
        node_id: str,
    ) -> ExecutionIdentityResolution:
        missing: list[str] = []
        suggestions: list[str] = []
        observations: dict[str, Any] = {"node_id": str(node_id)}

        environment = self._resolve_environment(
            node_id=node_id,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        dataset = self._resolve_dataset(
            node_id=node_id,
            request=request,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        prediction_sources = self._resolve_prediction_sources(
            request=request,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        runtime = self._resolve_runtime(
            request=request,
            environment=environment,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        materializer = self._resolve_materializer(
            request=request,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        business_formula = self._resolve_business_formula(
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )

        if missing:
            evidence = {
                "schema_version": _IDENTITY_EVIDENCE_SCHEMA_VERSION,
                "complete": False,
                "reason_code": "multi_alpha_execution_identity_incomplete",
                "missing": sorted(set(missing)),
                "acquisition_suggestions": _dedupe_text(suggestions),
                "observations": observations,
            }
            return ExecutionIdentityResolution(identity=None, evidence=evidence)

        assert dataset is not None
        assert prediction_sources is not None
        assert runtime is not None
        assert materializer is not None
        assert business_formula is not None
        try:
            identity = build_execution_identity(
                dataset=dataset,
                prediction_sources=prediction_sources,
                runtime=runtime,
                materializer=materializer,
                business_formula=business_formula,
                identity_source="captured_before_durable_child_planning",
            )
        except DurableContractError as exc:
            # Every field above came from an owned content source.  A malformed
            # source is still an explicit evidence problem, never a silent
            # replacement with mutable local defaults.
            evidence = {
                "schema_version": _IDENTITY_EVIDENCE_SCHEMA_VERSION,
                "complete": False,
                "reason_code": str(exc.reason_code),
                "missing": list(exc.context.get("missing", [])) or ["canonical_execution_identity"],
                "acquisition_suggestions": [
                    "repair the reported immutable identity source and capture a new manifest",
                    "retain the QE research result with the recorded identity limitation",
                ],
                "observations": observations,
            }
            return ExecutionIdentityResolution(identity=None, evidence=evidence)
        evidence = {
            "schema_version": _IDENTITY_EVIDENCE_SCHEMA_VERSION,
            "complete": True,
            "reason_code": None,
            "missing": [],
            "acquisition_suggestions": [],
            "identity_hash": identity.identity_hash,
            "identity_source": identity.identity_source,
            "observations": observations,
        }
        return ExecutionIdentityResolution(identity=identity, evidence=evidence)

    def resolve_materializer_identity(
        self,
        *,
        request: CombineBacktestRequest,
    ) -> dict[str, str]:
        """Return the exact local materializer identity used by recovery.

        This is the same implementation used by full execution-identity
        capture.  Recovery wiring calls it independently so a temporarily
        unavailable dataset/runtime endpoint cannot be confused with missing
        local rematerialization code.
        """

        missing: list[str] = []
        suggestions: list[str] = []
        observations: dict[str, Any] = {}
        materializer = self._resolve_materializer(
            request=request,
            missing=missing,
            suggestions=suggestions,
            observations=observations,
        )
        if materializer is None:
            raise DurableContractError(
                "current QE recovery materializer identity is incomplete",
                reason_code="rematerialize_recovery_code_identity_missing",
                context={
                    "missing": missing,
                    "acquisition_suggestions": suggestions,
                    "observations": observations,
                },
            )
        return materializer

    def _resolve_environment(
        self,
        *,
        node_id: str,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> QEWorkspaceExecutionEnvironment | None:
        try:
            environment = self._environment_loader(node_id)
        except Exception as exc:
            missing.extend(
                [
                    "runtime.execution_environment_snapshot_id",
                    "runtime.execution_environment_manifest_sha256",
                    "runtime.executor_file_set_sha256",
                ]
            )
            suggestions.append(
                "make the QE owning-service execution-environment manifest endpoint available on the selected node",
            )
            observations["environment_error"] = _exception_observation(exc)
            return None
        observations["execution_environment_snapshot_id"] = environment.execution_environment_snapshot_id
        observations["execution_environment_manifest_sha256"] = environment.execution_environment_manifest_sha256
        return environment

    def _resolve_dataset(
        self,
        *,
        node_id: str,
        request: CombineBacktestRequest,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> dict[str, str] | None:
        data_root_uri: str | None = None
        try:
            node = self._node_info_resolver(node_id)
            data_root_uri = str(
                request.backtest_config.get("remote_qlib_data_path")
                or getattr(node, "qlib_data_path", None)
                or ""
            ).strip() or None
        except Exception as exc:
            observations["node_resolution_error"] = _exception_observation(exc)
        try:
            report = self._dataset_loader(node_id, data_root_uri)
        except Exception as exc:
            missing.extend([f"dataset.{field}" for field in _DATASET_FIELDS])
            suggestions.append(
                "make the selected QE node dataset-identity endpoint available and publish the immutable dataset manifest",
            )
            observations["dataset_error"] = _exception_observation(exc)
            return None
        if not report.complete or report.dataset is None:
            missing.extend([f"dataset.{field}" for field in report.missing] or ["dataset.execution_identity"])
            suggestions.extend(report.acquisition_suggestions)
            observations["dataset_evidence"] = {
                "reason_code": report.reason_code,
                "missing": list(report.missing),
                "detail": report.detail,
            }
            return None
        dataset = dict(report.dataset)
        if dataset.get("resolved_node_id") != str(node_id):
            missing.append("dataset.resolved_node_id")
            suggestions.append("publish/retrieve the dataset identity from the actual selected QE node")
            observations["dataset_node_mismatch"] = {
                "expected": str(node_id),
                "actual": dataset.get("resolved_node_id"),
            }
            return None
        observations["dataset_manifest_sha256"] = dataset.get("dataset_manifest_sha256")
        return dataset

    def _resolve_prediction_sources(
        self,
        *,
        request: CombineBacktestRequest,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> list[dict[str, str]] | None:
        sources: list[dict[str, str]] = []
        errors: list[dict[str, Any]] = []
        for leg in request.roster:
            for seed_run_id in leg.seed_run_ids:
                try:
                    pointer = self._model_store.get_pointer(run_id=seed_run_id)
                    path = self._model_store.prediction_path(run_id=seed_run_id)
                    artifact_uri = str(pointer.get("mlflow_artifact_uri") or path.as_uri())
                    sources.append(
                        {
                            "leg_id": str(leg.leg_id),
                            "seed_run_id": str(seed_run_id),
                            "artifact_uri": artifact_uri,
                            "artifact_sha256": _sha256_file(path),
                        }
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "leg_id": str(leg.leg_id),
                            "seed_run_id": str(seed_run_id),
                            "error": _exception_observation(exc),
                        }
                    )
        if errors:
            missing.append("prediction_sources.exact_prediction_artifact")
            suggestions.append(
                "restore or re-ingest the exact source prediction artifact and its content hash into the model store",
            )
            observations["prediction_source_errors"] = errors
            return None
        observations["prediction_source_count"] = len(sources)
        return sources

    def _resolve_runtime(
        self,
        *,
        request: CombineBacktestRequest,
        environment: QEWorkspaceExecutionEnvironment | None,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> dict[str, str] | None:
        runtime_missing_before = len(missing)
        config = dict(request.backtest_config)
        environment_manifest = dict(environment.manifest) if environment is not None else {}
        declared_runtime = environment_manifest.get("declared_runtime_identity")
        declared_runtime = dict(declared_runtime) if isinstance(declared_runtime, Mapping) else {}
        runtime_template_hash = _configured_or_file_hash(
            config,
            hash_key="qlib_runtime_template_sha256",
            path_key="runtime_template_dir",
        )
        if runtime_template_hash is None:
            runtime_template_hash = _optional_sha256(
                declared_runtime.get("qlib_runtime_template_sha256"),
            )
        conda_lock_hash = _configured_or_file_hash(
            config,
            hash_key="conda_environment_lock_sha256",
            path_key="conda_environment_lock_path",
        )
        if conda_lock_hash is None:
            conda_lock_hash = _optional_sha256(
                declared_runtime.get("conda_environment_lock_sha256"),
            )
        executor_code_commit = str(
            config.get("executor_code_commit")
            or declared_runtime.get("executor_code_commit")
            or ""
        ).strip()
        executor_file_set_hash = str(environment_manifest.get("executor_file_set_sha256") or "").strip()
        if runtime_template_hash is None:
            missing.append("runtime.qlib_runtime_template_sha256")
            suggestions.append("configure a content hash for the QE qrun runtime template or a readable runtime_template_dir")
        if conda_lock_hash is None:
            missing.append("runtime.conda_environment_lock_sha256")
            suggestions.append("configure the expected conda environment lock hash for the QE deployment")
        if environment is None:
            # The environment loader already records the detail; keep each
            # required runtime component visible in the same evidence object.
            missing.extend(
                [
                    "runtime.execution_environment_snapshot_id",
                    "runtime.execution_environment_manifest_sha256",
                    "runtime.executor_file_set_sha256",
                ]
            )
        if not executor_code_commit:
            missing.append("runtime.executor_code_commit")
            suggestions.append("set the owning QE deployment executor_code_commit as an immutable deployment value")
        if not executor_file_set_hash:
            missing.append("runtime.executor_file_set_sha256")
            suggestions.append("repair the owning QE execution-environment manifest executor file set")
        if len(missing) > runtime_missing_before:
            return None
        assert environment is not None
        assert runtime_template_hash is not None
        assert conda_lock_hash is not None
        runtime = {
            "qlib_runtime_template_sha256": runtime_template_hash,
            "conda_environment_lock_sha256": conda_lock_hash,
            "execution_environment_snapshot_id": environment.execution_environment_snapshot_id,
            "execution_environment_manifest_sha256": environment.execution_environment_manifest_sha256,
            "executor_code_commit": executor_code_commit,
            "executor_file_set_sha256": executor_file_set_hash,
            "backtest_config_sha256": sha256_identity(_backtest_config_identity_payload(config)),
        }
        observations["runtime"] = {
            "execution_environment_snapshot_id": runtime["execution_environment_snapshot_id"],
            "execution_environment_manifest_sha256": runtime["execution_environment_manifest_sha256"],
            "backtest_config_sha256": runtime["backtest_config_sha256"],
        }
        return runtime

    def _resolve_materializer(
        self,
        *,
        request: CombineBacktestRequest,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> dict[str, str] | None:
        materializer_missing_before = len(missing)
        config = dict(request.backtest_config)
        aistock_commit = str(config.get("aistock_commit") or _git_commit(self._source_root) or "").strip()
        file_paths = {
            "combiner_file_sha256": self._source_root / "backend/services/multi_alpha/combine_backtest.py",
            "panel_builder_file_sha256": self._source_root / "backend/services/multi_alpha/panels.py",
            "durable_plan_file_sha256": self._source_root / "backend/services/multi_alpha/durable_plan.py",
            "durable_execution_adapter_file_sha256": self._source_root / "backend/services/multi_alpha/durable_execution_adapter.py",
        }
        hashes: dict[str, str] = {}
        for field, path in file_paths.items():
            try:
                hashes[field] = _sha256_file(path)
            except Exception as exc:
                missing.append(f"materializer.{field}")
                suggestions.append("deploy the complete AIstock multi-alpha materializer source set before exact recovery")
                observations.setdefault("materializer_errors", []).append(
                    {"field": field, "error": _exception_observation(exc)},
                )
        if not aistock_commit:
            missing.append("materializer.aistock_commit")
            suggestions.append("record the immutable AIstock source commit in backtest_config.aistock_commit")
        if len(missing) > materializer_missing_before:
            return None
        materializer_file_set_sha256 = sha256_identity(
            {field: hashes[field] for field in sorted(hashes)},
        )
        materializer = {
            "aistock_commit": aistock_commit,
            "planner_version": PLANNER_VERSION,
            "combiner_file_sha256": hashes["combiner_file_sha256"],
            "panel_builder_file_sha256": hashes["panel_builder_file_sha256"],
            "materializer_file_set_sha256": materializer_file_set_sha256,
        }
        observations["materializer"] = {
            "aistock_commit": aistock_commit,
            "materializer_file_set_sha256": materializer_file_set_sha256,
        }
        return materializer

    def _resolve_business_formula(
        self,
        *,
        missing: list[str],
        suggestions: list[str],
        observations: dict[str, Any],
    ) -> dict[str, str] | None:
        combiner_path = self._source_root / "backend/services/multi_alpha/combine_backtest.py"
        try:
            assembler_hash = _sha256_file(combiner_path)
        except Exception as exc:
            missing.extend(["business_formula.assembler_file_sha256", "business_formula.delta_formula_sha256"])
            suggestions.append("deploy the complete multi-alpha business formula source before exact recovery")
            observations["business_formula_error"] = _exception_observation(exc)
            return None
        formula_version = "multi_alpha_combine_formula_v1"
        delta_hash = sha256_identity(
            {
                "formula_version": formula_version,
                "assembler_file_sha256": assembler_hash,
                "derived_metrics": "baseline_scheme_loo_marginal_delta_v1",
            },
        )
        return {
            "formula_version": formula_version,
            "assembler_file_sha256": assembler_hash,
            "delta_formula_sha256": delta_hash,
        }


def _load_environment_from_workspace(node_id: str) -> QEWorkspaceExecutionEnvironment:
    async def _load() -> QEWorkspaceExecutionEnvironment:
        client = QEWorkspaceClient.for_node(node_id)
        try:
            return await client.get_execution_environment()
        finally:
            await client.close()

    return _run_async(_load())


def _load_dataset_from_workspace(node_id: str, data_root_uri: str | None) -> QEWorkspaceDatasetIdentity:
    async def _load() -> QEWorkspaceDatasetIdentity:
        client = QEWorkspaceClient.for_node(node_id)
        try:
            return await client.get_dataset_identity(node_id=node_id, data_root_uri=data_root_uri)
        finally:
            await client.close()

    return _run_async(_load())


def _run_async(awaitable: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    # The durable submission route is synchronous.  If an embedding invokes it
    # from an event-loop thread, do not create a nested loop or silently skip the
    # owning-node check; callers receive explicit evidence through the resolver.
    if hasattr(awaitable, "close"):
        awaitable.close()
    raise RuntimeError("QE execution identity lookup requires a synchronous durable submission worker")


def _configured_or_file_hash(config: Mapping[str, Any], *, hash_key: str, path_key: str) -> str | None:
    configured = str(config.get(hash_key) or "").strip().lower()
    if configured:
        _require_identity_sha256(configured, field=hash_key)
        return configured
    raw_path = str(config.get(path_key) or "").strip()
    if not raw_path:
        return None
    excluded_names = RUNTIME_EXTERNAL_DATA_LINK_NAMES if path_key == "runtime_template_dir" else frozenset()
    return _sha256_tree(Path(raw_path), external_data_names=excluded_names)


def _optional_sha256(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    _require_identity_sha256(normalized, field="configured_identity_hash")
    return normalized


def _require_identity_sha256(value: str, *, field: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise DurableContractError(
            f"{field} must be a lowercase SHA-256 digest",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"field": field, "value": value},
        )


def _backtest_config_identity_payload(config: Mapping[str, Any]) -> dict[str, Any]:
    excluded = {
        "execution_identity",
        "execution_identity_hash",
        "execution_identity_evidence",
        "__multi_alpha_request_snapshot__",
    }
    return {str(key): value for key, value in config.items() if str(key) not in excluded}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_tree(root: Path, *, external_data_names: frozenset[str] = frozenset()) -> str:
    resolved = root.resolve(strict=True)
    if not resolved.is_dir():
        return _sha256_file(resolved)
    rows: list[dict[str, str]] = []
    external_data_bindings: list[str] = []
    for path in sorted(resolved.rglob("*")):
        relative = path.relative_to(resolved)
        if len(relative.parts) == 1 and path.name in external_data_names:
            try:
                external_data_link = is_runtime_external_data_link(path)
            except OSError as exc:
                raise DurableContractError(
                    "runtime template identity cannot inspect an external QE data link",
                    reason_code="multi_alpha_execution_identity_invalid",
                    context={"path": str(path), "error_type": type(exc).__name__, "message": str(exc)},
                ) from exc
            if external_data_link:
                external_data_bindings.append(relative.as_posix())
                continue
        try:
            is_file = path.is_file()
        except OSError as exc:
            raise DurableContractError(
                "runtime template identity cannot inspect a filesystem entry",
                reason_code="multi_alpha_execution_identity_invalid",
                context={"path": str(path), "error_type": type(exc).__name__, "message": str(exc)},
            ) from exc
        if not is_file:
            continue
        if path.is_symlink():
            raise DurableContractError(
                "runtime template identity does not permit symlinked files",
                reason_code="multi_alpha_execution_identity_invalid",
                context={"path": str(path)},
            )
        rows.append({"path": relative.as_posix(), "sha256": _sha256_file(path)})
    if not rows:
        raise DurableContractError(
            "runtime template identity cannot hash an empty directory",
            reason_code="multi_alpha_execution_identity_incomplete",
            context={"path": str(resolved)},
        )
    identity_payload: dict[str, Any] = {"root_kind": "directory", "files": rows}
    if external_data_bindings:
        identity_payload["external_data_bindings"] = external_data_bindings
    return sha256_identity(identity_payload)


def _git_commit(root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = completed.stdout.strip().lower()
    return value if len(value) == 40 and all(char in "0123456789abcdef" for char in value) else None


def _exception_observation(exc: BaseException) -> dict[str, str]:
    return {"type": type(exc).__name__, "message": str(exc)}


def _dedupe_text(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in result:
            result.append(normalized)
    return result


def _normalize_mapping(value: Mapping[str, Any], fields: Sequence[str], *, section: str) -> dict[str, str]:
    return {field: _required_text(value[field], field=f"{section}.{field}") for field in fields}


def _require_mapping_fields(value: Mapping[str, Any], fields: Sequence[str], *, section: str) -> None:
    if not isinstance(value, Mapping):
        raise DurableContractError(
            f"{section} must be an object",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"section": section},
        )
    missing = [field for field in fields if field not in value or value[field] in (None, "")]
    if missing:
        raise DurableContractError(
            f"{section} is missing required content identity fields",
            reason_code="multi_alpha_execution_identity_incomplete",
            context={"section": section, "missing": missing},
        )


def _required_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DurableContractError(
            "execution identity field must be a non-empty string",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"field": field, "value": value},
        )
    return value.strip()


def _as_mapping(value: Any, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DurableContractError(
            "execution identity section must be an object",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"field": field},
        )
    return value


def _as_sequence(value: Any, *, field: str) -> Sequence[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or any(
        not isinstance(item, Mapping) for item in value
    ):
        raise DurableContractError(
            "execution identity prediction sources must be an array of objects",
            reason_code="multi_alpha_execution_identity_invalid",
            context={"field": field},
        )
    return value
