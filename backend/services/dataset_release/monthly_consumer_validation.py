"""Manifest-bound orchestration for required monthly consumer probes."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Any, Callable, Mapping, Protocol, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_official_adapters import (
    ConsumerReadback,
    ConsumerValidationExecution,
    StageWorkload,
)
from .monthly_unified import (
    CONSUMER_VALIDATION_BINDING_SCHEMA,
    REQUIRED_CONSUMERS,
    REQUIRED_NODES,
)
from .monthly_worker import ProducerContext
from .profile_contract import (
    ACTIVE_PROFILE_SCHEMA_V4,
    ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS,
)


CONSUMER_PROBE_RESULT_SCHEMA = "aistock_monthly_consumer_probe_result_v1"
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_SIDE_EFFECT_FLAGS = (
    "outcomes_read",
    "training_started",
    "experiment_started",
    "runtime_action_performed",
)


class MonthlyConsumerValidationError(RuntimeError):
    """Consumer validation evidence is incomplete or crosses release identity."""


@dataclass(frozen=True, slots=True)
class ConsumerProbeRequest:
    consumer_id: str
    node_id: str
    dataset_manifest_sha256: str
    binding_path: Path
    profile_path: Path
    controller_candidate_root: Path
    node_candidate_root: str
    resolved_component_paths: tuple[Path, ...]
    derived_asset_paths: tuple[Path, ...]


class MonthlyConsumerProbe(Protocol):
    """One business-owned, read-only consumer smoke implementation."""

    probe_id: str
    probe_version: str

    def run(self, request: ConsumerProbeRequest) -> Path: ...


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_canonical(path: Path, *, label: str) -> dict[str, Any]:
    try:
        resolved = path.resolve(strict=True)
        raw = resolved.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlyConsumerValidationError(f"{label} is unreadable") from exc
    if _is_link(path) or not resolved.is_file():
        raise MonthlyConsumerValidationError(f"{label} is not a plain file")
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise MonthlyConsumerValidationError(f"{label} is not canonical JSON")
    return value


def _write_canonical(path: Path, value: Mapping[str, Any]) -> Path:
    payload = canonical_json_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        if _is_link(path) or not path.is_file() or path.read_bytes() != payload:
            raise MonthlyConsumerValidationError(
                f"consumer validation output differs: {path.name}"
            ) from exc
    return path


def _plain_under(root: Path, relative: str, *, label: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise MonthlyConsumerValidationError(f"{label} path is invalid")
    current = root
    for part in path.parts:
        current /= part
        if _is_link(current):
            raise MonthlyConsumerValidationError(f"{label} traverses a link")
    try:
        resolved = (root / path).resolve(strict=True)
    except OSError as exc:
        raise MonthlyConsumerValidationError(f"{label} is unavailable") from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise MonthlyConsumerValidationError(f"{label} escaped the candidate")
    return resolved


def _content_ref(path: Path) -> dict[str, Any]:
    return {
        "path": path.name,
        "sha256": _sha256(path),
        "size": path.stat().st_size,
    }


def _is_content_ref(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"id", "sha256", "size"}:
        return False
    ref_id = value.get("id")
    digest = value.get("sha256")
    size = value.get("size")
    return bool(
        isinstance(ref_id, str)
        and ref_id
        and not Path(ref_id).is_absolute()
        and ".." not in Path(ref_id).parts
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
        and type(size) is int
        and size >= 0
    )


def _require_prior_scope(
    context: ProducerContext,
    stage: str,
) -> Mapping[str, Any]:
    receipt = context.prior_receipts.get(stage)
    scope = receipt.get("scope") if isinstance(receipt, Mapping) else None
    if not isinstance(scope, Mapping):
        raise MonthlyConsumerValidationError(f"{stage} scope is unavailable")
    return scope


def _profile_binding_defaults() -> tuple[
    Callable[[Path], Any],
    Callable[..., Mapping[str, Any]],
]:
    from backend.services.quantevolver.qe_active_dataset_profile import (
        load_qe_profile,
        resolve_active_dataset_consumer_binding,
        validate_controller_snapshot,
    )

    def load(path: Path) -> Any:
        profile = load_qe_profile(path)
        validate_controller_snapshot(profile)
        return profile

    return load, resolve_active_dataset_consumer_binding


def _profile_raw(profile: Any) -> Mapping[str, Any]:
    raw = getattr(profile, "raw", None)
    if not isinstance(raw, Mapping):
        raise MonthlyConsumerValidationError("profile loader returned no raw profile")
    return raw


def _pinned_candidate_paths(
    *,
    root: Path,
    contract: Mapping[str, Any],
    logical_components: Sequence[str],
    verified: dict[str, tuple[str, int, Path]],
) -> tuple[Path, ...]:
    refs = contract.get("component_refs")
    if not isinstance(refs, Mapping) or set(refs) != set(logical_components):
        raise MonthlyConsumerValidationError("consumer contract component set differs")
    paths: list[Path] = []
    observed: set[str] = set()
    for component in logical_components:
        pins = refs[component]
        if not isinstance(pins, list) or not pins:
            raise MonthlyConsumerValidationError(
                f"consumer component is empty: {component}"
            )
        for pin in pins:
            if not isinstance(pin, Mapping) or set(pin) != {"path", "sha256", "size"}:
                raise MonthlyConsumerValidationError("consumer component pin is invalid")
            relative = str(pin["path"])
            path = _plain_under(root, relative, label="consumer component")
            cached = verified.get(relative)
            identity = (str(pin["sha256"]), int(pin["size"]), path)
            if cached is not None and cached != identity:
                raise MonthlyConsumerValidationError(
                    f"consumer component pin conflicts: {relative}"
                )
            if cached is None and (
                path.stat().st_size != pin["size"] or _sha256(path) != pin["sha256"]
            ):
                raise MonthlyConsumerValidationError(
                    f"consumer component bytes differ: {relative}"
                )
            verified[relative] = identity
            if relative not in observed:
                observed.add(relative)
                paths.append(path)
    return tuple(paths)


def _probe_result(
    path: Path,
    *,
    request: ConsumerProbeRequest,
    probe: MonthlyConsumerProbe,
) -> tuple[dict[str, Any], dict[str, int], dict[str, Any]]:
    value = _read_canonical(path, label=f"{request.consumer_id} probe result")
    expected_fields = {
        "schema_version",
        "status",
        "consumer_id",
        "node_id",
        "dataset_manifest_sha256",
        "binding_sha256",
        "required_window",
        "coverage_counts",
        "adapter",
        "evidence_refs",
        "side_effect_flags",
    }
    coverage = value.get("coverage_counts")
    window = value.get("required_window")
    flags = value.get("side_effect_flags")
    adapter = value.get("adapter")
    if (
        set(value) != expected_fields
        or value.get("schema_version") != CONSUMER_PROBE_RESULT_SCHEMA
        or value.get("status") != "PASS"
        or value.get("consumer_id") != request.consumer_id
        or value.get("node_id") != request.node_id
        or value.get("dataset_manifest_sha256")
        != request.dataset_manifest_sha256
        or value.get("binding_sha256") != _sha256(request.binding_path)
        or not isinstance(window, Mapping)
        or not window
        or not isinstance(coverage, Mapping)
        or coverage.get("unresolved_count") != 0
        or any(type(item) is not int or item < 0 for item in coverage.values())
        or not isinstance(flags, Mapping)
        or set(flags) != set(_SIDE_EFFECT_FLAGS)
        or any(flags[name] is not False for name in _SIDE_EFFECT_FLAGS)
        or not isinstance(adapter, Mapping)
        or adapter
        != {"id": str(probe.probe_id), "version": str(probe.probe_version)}
        or not isinstance(value.get("evidence_refs"), list)
        or not value["evidence_refs"]
        or any(not _is_content_ref(item) for item in value["evidence_refs"])
    ):
        raise MonthlyConsumerValidationError(
            f"consumer probe result differs: {request.consumer_id}"
        )
    return value, dict(coverage), dict(window)


@dataclass(frozen=True, slots=True)
class RegisteredMonthlyConsumerValidationExecutor:
    """Run an exact code-owned consumer registry against deployed immutable roots."""

    artifact_root: Path
    probes: Mapping[str, MonthlyConsumerProbe]
    consumer_nodes: Mapping[str, str]
    profile_loader: Callable[[Path], Any] | None = None
    binding_resolver: Callable[..., Mapping[str, Any]] | None = None

    def __post_init__(self) -> None:
        if set(self.probes) != set(REQUIRED_CONSUMERS):
            raise ValueError("consumer probe registry must cover exact required consumers")
        if set(self.consumer_nodes) != set(REQUIRED_CONSUMERS) or any(
            node not in {"wsl2-5080", "rdagent-node1"}
            for node in self.consumer_nodes.values()
        ) or set(self.consumer_nodes.values()) != {"wsl2-5080", "rdagent-node1"}:
            raise ValueError("consumer node registry must cover WSL and node1 bindings")
        identities = [
            (str(probe.probe_id).strip(), str(probe.probe_version).strip())
            for probe in self.probes.values()
        ]
        if any(not probe_id or not version for probe_id, version in identities):
            raise ValueError("consumer probe identity is incomplete")
        root = self.artifact_root.resolve(strict=True)
        if _is_link(self.artifact_root) or not root.is_dir():
            raise ValueError("consumer validation artifact root is unavailable")

    def execute(
        self,
        context: ProducerContext,
        *,
        dataset_manifest_sha256: str,
    ) -> ConsumerValidationExecution:
        if context.stage != "CONSUMER_VALIDATE":
            raise MonthlyConsumerValidationError(
                "consumer validator received another stage"
            )
        ensure_sha256(dataset_manifest_sha256, field="dataset_manifest_sha256")
        candidate = Path(str(context.plan.get("candidate_root") or ""))
        try:
            root = candidate.resolve(strict=True)
        except OSError as exc:
            raise MonthlyConsumerValidationError("candidate root is unavailable") from exc
        if _is_link(candidate) or not root.is_dir():
            raise MonthlyConsumerValidationError("candidate root is linked or invalid")

        local_scope = _require_prior_scope(context, "LOCAL_VALIDATE")
        deploy_scope = _require_prior_scope(context, "DEPLOY")
        if (
            local_scope.get("dataset_manifest_sha256") != dataset_manifest_sha256
            or deploy_scope.get("dataset_manifest_sha256")
            != dataset_manifest_sha256
        ):
            raise MonthlyConsumerValidationError("prior stage manifest identity differs")
        registrations = deploy_scope.get("node_registrations")
        registration_refs = deploy_scope.get("node_registration_refs")
        if not isinstance(registrations, Mapping) or set(registrations) != set(
            REQUIRED_NODES
        ) or not isinstance(registration_refs, Mapping) or set(
            registration_refs
        ) != set(REQUIRED_NODES):
            raise MonthlyConsumerValidationError("node registrations are incomplete")

        profile_path = Path(str(context.plan.get("profile_candidate") or ""))
        profile_ref = local_scope.get("profile_candidate_ref")
        try:
            resolved_profile = profile_path.resolve(strict=True)
        except OSError as exc:
            raise MonthlyConsumerValidationError(
                "profile candidate is unavailable"
            ) from exc
        if (
            not profile_path.is_absolute()
            or _is_link(profile_path)
            or not resolved_profile.is_file()
            or not isinstance(profile_ref, Mapping)
            or not _is_content_ref(profile_ref)
            or _sha256(resolved_profile) != profile_ref.get("sha256")
            or resolved_profile.stat().st_size != profile_ref.get("size")
        ):
            raise MonthlyConsumerValidationError("profile candidate identity differs")
        default_loader, default_resolver = _profile_binding_defaults()
        loader = self.profile_loader or default_loader
        resolver = self.binding_resolver or default_resolver
        profile = loader(profile_path)
        profile_raw = _profile_raw(profile)
        components = profile_raw.get("components")
        profile_nodes = profile_raw.get("node_bindings")
        profile_consumers = profile_raw.get("consumers")
        if (
            profile_raw.get("schema_version") != ACTIVE_PROFILE_SCHEMA_V4
            or not isinstance(components, Mapping)
            or components.get("dataset_manifest_sha256")
            != dataset_manifest_sha256
            or not isinstance(profile_nodes, Mapping)
            or set(profile_nodes) != {"wsl2-5080", "rdagent-node1"}
            or not isinstance(profile_consumers, Mapping)
            or not set(REQUIRED_CONSUMERS).issubset(profile_consumers)
        ):
            raise MonthlyConsumerValidationError("profile release identity differs")
        for consumer_id in REQUIRED_CONSUMERS:
            raw_consumer = profile_consumers[consumer_id]
            if (
                not isinstance(raw_consumer, Mapping)
                or raw_consumer.get("required_components")
                != sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer_id])
            ):
                raise MonthlyConsumerValidationError(
                    f"profile consumer contract differs: {consumer_id}"
                )
        for node_id, raw in profile_nodes.items():
            registration = registrations[node_id]
            if (
                not isinstance(raw, Mapping)
                or not isinstance(registration, Mapping)
                or not _is_content_ref(registration_refs[node_id])
                or raw.get("candidate_root") != registration.get("candidate_root")
            ):
                raise MonthlyConsumerValidationError(
                    f"profile node registration differs: {node_id}"
                )

        attempt_root = (
            self.artifact_root.resolve(strict=True)
            / "monthly"
            / context.operation_id
            / "consumer-validation"
            / f"attempt-{context.attempt}"
        )
        if attempt_root.exists():
            if _is_link(attempt_root) or not attempt_root.is_dir():
                raise MonthlyConsumerValidationError(
                    "consumer validation attempt root is invalid"
                )
        else:
            attempt_root.mkdir(parents=True, exist_ok=False)

        readbacks: list[ConsumerReadback] = []
        inputs: list[Path] = [profile_path]
        verified_components: dict[str, tuple[str, int, Path]] = {}
        computed = 0
        for consumer_id in REQUIRED_CONSUMERS:
            probe = self.probes[consumer_id]
            node_id = self.consumer_nodes[consumer_id]
            contract_path = _plain_under(
                root,
                f"provenance/consumer-contract-{consumer_id}.json",
                label="consumer contract",
            )
            contract = _read_canonical(contract_path, label="consumer contract")
            required = sorted(ACTIVE_PROFILE_V4_CONSUMER_REQUIREMENTS[consumer_id])
            if (
                contract.get("consumer_id") != consumer_id
                or contract.get("dataset_manifest_sha256")
                != dataset_manifest_sha256
                or contract.get("required_components") != required
                or contract.get("status") != "READY_FOR_NODE_READBACK"
            ):
                raise MonthlyConsumerValidationError(
                    f"consumer contract identity differs: {consumer_id}"
                )
            resolved_paths = _pinned_candidate_paths(
                root=root,
                contract=contract,
                logical_components=required,
                verified=verified_components,
            )
            derived_paths = tuple(
                path
                for path in resolved_paths
                if path.is_relative_to(root / "derived")
            )
            component_paths = tuple(
                path for path in resolved_paths if path not in derived_paths
            )
            if "derived_assets" in required and not derived_paths:
                raise MonthlyConsumerValidationError(
                    f"derived assets are unavailable: {consumer_id}"
                )
            component_file_refs = [
                {
                    "id": path.relative_to(root).as_posix(),
                    "sha256": verified_components[
                        path.relative_to(root).as_posix()
                    ][0],
                    "size": verified_components[path.relative_to(root).as_posix()][1],
                }
                for path in component_paths
            ]
            derived_asset_refs = [
                {
                    "id": path.relative_to(root).as_posix(),
                    "sha256": verified_components[
                        path.relative_to(root).as_posix()
                    ][0],
                    "size": verified_components[path.relative_to(root).as_posix()][1],
                }
                for path in derived_paths
            ]
            resolved_binding = dict(
                resolver(
                    consumer_id=consumer_id,
                    node_id=node_id,
                    profile=profile,
                )
            )
            if (
                resolved_binding.get("consumer_id") != consumer_id
                or resolved_binding.get("node_id") != node_id
                or resolved_binding.get("dataset_manifest_sha256")
                != dataset_manifest_sha256
                or resolved_binding.get("required_components") != required
                or resolved_binding.get("resolved_once") is not True
                or resolved_binding.get("legacy_fallback") is not False
            ):
                raise MonthlyConsumerValidationError(
                    f"resolved consumer binding differs: {consumer_id}"
                )
            binding_path = _write_canonical(
                attempt_root / f"{consumer_id}-binding.json",
                {
                    "schema_version": CONSUMER_VALIDATION_BINDING_SCHEMA,
                    "dataset_manifest_sha256": dataset_manifest_sha256,
                    "consumer_id": consumer_id,
                    "node_id": node_id,
                    "profile_ref": _content_ref(profile_path),
                    "contract_ref": _content_ref(contract_path),
                    "node_registration_ref": dict(
                        registration_refs[node_id]
                    ),
                    "resolved_component_refs": component_file_refs,
                    "derived_asset_refs": derived_asset_refs,
                    "resolved_binding": resolved_binding,
                },
            )
            request = ConsumerProbeRequest(
                consumer_id=consumer_id,
                node_id=node_id,
                dataset_manifest_sha256=dataset_manifest_sha256,
                binding_path=binding_path,
                profile_path=profile_path,
                controller_candidate_root=root,
                node_candidate_root=str(profile_nodes[node_id]["candidate_root"]),
                resolved_component_paths=component_paths,
                derived_asset_paths=derived_paths,
            )
            result_path = probe.run(request)
            _result, coverage, window = _probe_result(
                result_path,
                request=request,
                probe=probe,
            )
            readbacks.append(
                ConsumerReadback(
                    consumer_id=consumer_id,
                    node_id=node_id,
                    binding_path=binding_path,
                    required_window=window,
                    resolved_component_paths=component_paths,
                    derived_asset_paths=derived_paths,
                    coverage_counts=coverage,
                    adapter_version=f"{probe.probe_id}@{probe.probe_version}",
                    result_path=result_path,
                )
            )
            inputs.extend((contract_path, result_path))
            computed += 1
        return ConsumerValidationExecution(
            readbacks=tuple(readbacks),
            input_artifacts=tuple(inputs),
            workload=StageWorkload(computed_rows=computed, bytes_transferred=0),
        )


__all__: Sequence[str] = (
    "CONSUMER_PROBE_RESULT_SCHEMA",
    "CONSUMER_VALIDATION_BINDING_SCHEMA",
    "ConsumerProbeRequest",
    "MonthlyConsumerProbe",
    "MonthlyConsumerValidationError",
    "RegisteredMonthlyConsumerValidationExecutor",
)
