from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .canonical import digest_named_fields, ensure_sha256, sha256_hex
from .contracts import ValidationCompatibility
from .errors import IdentityConflictError


FINGERPRINT_SCHEMA_VERSION = "dataset_release_component_fingerprints_v1"


class FingerprintChange(str, Enum):
    NONE = "none"
    RESOURCE_POLICY_ONLY = "resource_policy_only"
    VALIDATOR_STRENGTHENING_COMPATIBLE = "validator_strengthening_compatible"
    SOURCE_INPUT_CHANGED = "source_input_changed"
    PRODUCER_OR_ARTIFACT_INCOMPATIBLE = "producer_or_artifact_incompatible"
    SEMANTIC_CONTRACT_CHANGED = "semantic_contract_changed"


@dataclass(frozen=True)
class ProducerDependency:
    path: str
    sha256: str
    symbols: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path.replace("\\", "/"),
            "sha256": ensure_sha256(self.sha256, field=f"dependency:{self.path}"),
            "symbols": list(self.symbols),
        }


def fingerprint_dependency_files(
    root: Path,
    paths: Sequence[str | Path],
    *,
    dirty_paths: Iterable[str | Path] = (),
    symbols: Mapping[str, Sequence[str]] | None = None,
) -> tuple[ProducerDependency, ...]:
    root = root.resolve()
    normalized_dirty = {str(Path(value)).replace("\\", "/").casefold().lstrip("./") for value in dirty_paths}
    dependencies: list[ProducerDependency] = []
    for value in sorted({str(Path(path)).replace("\\", "/") for path in paths}):
        path = (root / value).resolve()
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as exc:
            raise IdentityConflictError(f"producer dependency escapes root: {value}") from exc
        if not path.is_file():
            raise IdentityConflictError(f"producer dependency missing: {relative}")
        relative_key = relative.casefold()
        if relative_key in normalized_dirty:
            raise IdentityConflictError(
                f"dirty path intersects producer dependency: {relative}",
                code="DATASET_RELEASE_DIRTY_PRODUCER_DEPENDENCY",
            )
        dependencies.append(
            ProducerDependency(
                path=relative,
                sha256=sha256_hex(path.read_bytes()),
                symbols=tuple((symbols or {}).get(relative, ())),
            )
        )
    if not dependencies:
        raise IdentityConflictError("producer dependency manifest cannot be empty")
    return tuple(dependencies)


def producer_fingerprint(component: str, dependencies: Sequence[ProducerDependency]) -> str:
    return digest_named_fields(
        "dataset_release_producer_fingerprint_v1",
        {
            "component": component,
            "dependencies": [item.as_dict() for item in dependencies],
        },
    )


@dataclass(frozen=True)
class ComponentFingerprints:
    semantic_fingerprint: str
    source_input_digest: str
    producer_fingerprint: str
    artifact_fingerprint: str
    validation_fingerprint: str
    resource_policy_digest: str

    def __post_init__(self) -> None:
        for name, value in self.as_dict().items():
            ensure_sha256(value, field=name)

    def as_dict(self) -> dict[str, str]:
        return {
            "semantic_fingerprint": self.semantic_fingerprint,
            "source_input_digest": self.source_input_digest,
            "producer_fingerprint": self.producer_fingerprint,
            "artifact_fingerprint": self.artifact_fingerprint,
            "validation_fingerprint": self.validation_fingerprint,
            "resource_policy_digest": self.resource_policy_digest,
        }

    @property
    def data_identity_digest(self) -> str:
        """Data bytes identity intentionally excludes validator and resource policy."""

        return digest_named_fields(
            "dataset_release_component_data_identity_v1",
            {
                "semantic_fingerprint": self.semantic_fingerprint,
                "source_input_digest": self.source_input_digest,
                "producer_fingerprint": self.producer_fingerprint,
                "artifact_fingerprint": self.artifact_fingerprint,
            },
        )

    @property
    def full_digest(self) -> str:
        return digest_named_fields(FINGERPRINT_SCHEMA_VERSION, self.as_dict())


def fingerprint_payload(schema: str, payload: Mapping[str, Any]) -> str:
    return digest_named_fields(schema, payload)


def changed_layers(
    previous: ComponentFingerprints,
    current: ComponentFingerprints,
) -> tuple[str, ...]:
    return tuple(name for name in previous.as_dict() if previous.as_dict()[name] != current.as_dict()[name])


def classify_fingerprint_change(
    previous: ComponentFingerprints,
    current: ComponentFingerprints,
    *,
    validation_compatibility: ValidationCompatibility = ValidationCompatibility.UNCHANGED,
) -> FingerprintChange:
    changed = set(changed_layers(previous, current))
    if not changed:
        return FingerprintChange.NONE
    if changed == {"resource_policy_digest"}:
        return FingerprintChange.RESOURCE_POLICY_ONLY
    # Physical tuning is recorded but never masks or escalates a data/validation change.
    changed.discard("resource_policy_digest")
    if "semantic_fingerprint" in changed:
        return FingerprintChange.SEMANTIC_CONTRACT_CHANGED
    if "source_input_digest" in changed:
        return FingerprintChange.SOURCE_INPUT_CHANGED
    if changed.intersection({"producer_fingerprint", "artifact_fingerprint"}):
        return FingerprintChange.PRODUCER_OR_ARTIFACT_INCOMPATIBLE
    if changed == {"validation_fingerprint"}:
        if validation_compatibility is ValidationCompatibility.VALIDATOR_STRENGTHENING_COMPATIBLE:
            return FingerprintChange.VALIDATOR_STRENGTHENING_COMPATIBLE
        if validation_compatibility is ValidationCompatibility.READER_OR_ARTIFACT_INCOMPATIBLE:
            return FingerprintChange.PRODUCER_OR_ARTIFACT_INCOMPATIBLE
        if validation_compatibility is ValidationCompatibility.SEMANTIC_CONTRACT_CHANGED:
            return FingerprintChange.SEMANTIC_CONTRACT_CHANGED
        raise IdentityConflictError("validation fingerprint changed without an explicit compatibility classification")
    raise IdentityConflictError(f"unsupported fingerprint change combination: {sorted(changed)}")
