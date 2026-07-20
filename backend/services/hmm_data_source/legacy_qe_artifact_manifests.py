"""Immutable integrity receipts for approved pre-manifest QE workspaces."""

from __future__ import annotations

import re
from dataclasses import dataclass


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


@dataclass(frozen=True, slots=True)
class LegacyQEArtifactReceipt:
    artifact_name: str
    workspace_path: str
    schema_version: str
    sha256: str
    size_bytes: int
    row_count: int
    quality_status: str = "ok"

    def __post_init__(self) -> None:
        if self.artifact_name not in {"pred.pkl", "label.pkl"}:
            raise ValueError("legacy QE receipt may only describe pred.pkl or label.pkl")
        if not self.workspace_path.endswith(f"/artifacts/{self.artifact_name}"):
            raise ValueError("legacy QE artifact path does not match artifact_name")
        if not self.schema_version or not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError("legacy QE artifact integrity metadata is invalid")
        if self.size_bytes < 1 or self.row_count < 1 or self.quality_status != "ok":
            raise ValueError("legacy QE artifact receipt is not usable")


@dataclass(frozen=True, slots=True)
class LegacyQERecorderEvidence:
    workspace_path: str
    sha256: str
    size_bytes: int
    terminal_status: str

    def __post_init__(self) -> None:
        if (
            self.workspace_path != "run.log"
            or not _SHA256_RE.fullmatch(self.sha256)
            or self.size_bytes < 1
            or self.terminal_status != "FINISHED"
        ):
            raise ValueError("legacy QE recorder evidence is invalid")


@dataclass(frozen=True, slots=True)
class LegacyQEArtifactManifest:
    base_loop_ref: str
    logical_experiment_id: str
    recorder_experiment_id: str
    recorder_id: str
    recorder_evidence: LegacyQERecorderEvidence
    artifacts: tuple[LegacyQEArtifactReceipt, ...]

    def __post_init__(self) -> None:
        if self.base_loop_ref.count("/") != 1 or not self.logical_experiment_id:
            raise ValueError("legacy QE manifest identity is invalid")
        if not _SAFE_ID_RE.fullmatch(self.recorder_experiment_id) or not _SAFE_ID_RE.fullmatch(
            self.recorder_id
        ):
            raise ValueError("legacy QE recorder identity is invalid")
        if len(self.artifacts) != 2 or {item.artifact_name for item in self.artifacts} != {
            "pred.pkl",
            "label.pkl",
        }:
            raise ValueError("legacy QE manifest requires exactly pred.pkl and label.pkl")
        expected_prefix = f"mlruns/{self.recorder_experiment_id}/{self.recorder_id}/artifacts/"
        if any(not item.workspace_path.startswith(expected_prefix) for item in self.artifacts):
            raise ValueError("legacy QE artifact path does not match recorder identity")

    def artifact(self, artifact_name: str) -> LegacyQEArtifactReceipt:
        matches = [item for item in self.artifacts if item.artifact_name == artifact_name]
        if len(matches) != 1:
            raise ValueError(f"legacy QE artifact receipt is not unique: {artifact_name}")
        return matches[0]


LEGACY_QE_ARTIFACT_MANIFESTS: tuple[LegacyQEArtifactManifest, ...] = (
    LegacyQEArtifactManifest(
        base_loop_ref="qe_20260502_131502_9b54/Loop1",
        logical_experiment_id="qe_20260502_131502_9b54_L1",
        recorder_experiment_id="308973027052385728",
        recorder_id="5c85da5785e9495b85c36d5b6f6e97b9",
        recorder_evidence=LegacyQERecorderEvidence(
            workspace_path="run.log",
            sha256="315fe648c87ebedaee0715ac318889e9919bfe6106cffe434b5b30114985b31e",
            size_bytes=140227,
            terminal_status="FINISHED",
        ),
        artifacts=(
            LegacyQEArtifactReceipt(
                artifact_name="pred.pkl",
                workspace_path=(
                    "mlruns/308973027052385728/5c85da5785e9495b85c36d5b6f6e97b9/"
                    "artifacts/pred.pkl"
                ),
                schema_version="legacy_qe_dataframe_pickle_v1",
                sha256="24ca37fc573f57b0c1759501af7b0b17e4cf02c8fbf97144e49c73696a694da6",
                size_bytes=24615874,
                row_count=2045269,
            ),
            LegacyQEArtifactReceipt(
                artifact_name="label.pkl",
                workspace_path=(
                    "mlruns/308973027052385728/5c85da5785e9495b85c36d5b6f6e97b9/"
                    "artifacts/label.pkl"
                ),
                schema_version="legacy_qe_dataframe_pickle_v1",
                sha256="cf258ca77dd03f512e1587ac7a3a72903e431f6ba6400a53c47b92d832a55ec6",
                size_bytes=16434799,
                row_count=2045269,
            ),
        ),
    ),
)


def find_legacy_qe_artifact_manifest(base_loop_ref: str) -> LegacyQEArtifactManifest | None:
    matches = [item for item in LEGACY_QE_ARTIFACT_MANIFESTS if item.base_loop_ref == base_loop_ref]
    if len(matches) > 1:
        raise ValueError(f"legacy QE manifest is ambiguous: {base_loop_ref}")
    return matches[0] if matches else None
