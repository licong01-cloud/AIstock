"""Build the immutable, importable F-014 evaluator deployment bundle."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from backend.services.quantevolver.long_trend_evaluation_contract import (
    EVALUATOR_VERSION,
    QELongTrendError,
    QELongTrendReason,
    canonical_sha256,
)

BUNDLE_SCHEMA_VERSION = "qe_long_trend_bundle_v1"
BUNDLE_MANIFEST_NAME = "bundle_manifest.json"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

BUNDLE_SOURCE_PATHS: tuple[str, ...] = (
    "backend/__init__.py",
    "backend/services/__init__.py",
    "backend/services/quantevolver/__init__.py",
    "backend/services/quantevolver/long_trend_evaluation_contract.py",
    "backend/services/quantevolver/long_trend_data_reader.py",
    "backend/services/quantevolver/long_trend_evaluation.py",
    "backend/services/quantevolver/qe_dataset_contract.py",
    "backend/services/quantevolver/long_trend_pickle_parser_entry.py",
    "backend/services/quantevolver/long_trend_worker_entry.py",
)


@dataclass(frozen=True)
class QELongTrendEvaluatorBundle:
    schema_version: str
    bundle_sha256: str
    evaluator_source_sha256: str
    execution_environment_snapshot_id: str
    execution_environment_manifest_sha256: str
    manifest: dict[str, Any]
    files: dict[str, str]

    def request_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_sha256": self.bundle_sha256,
            "evaluator_source_sha256": self.evaluator_source_sha256,
            "execution_environment_snapshot_id": self.execution_environment_snapshot_id,
            "execution_environment_manifest_sha256": self.execution_environment_manifest_sha256,
            "manifest": dict(self.manifest),
            "files": dict(self.files),
        }


def build_long_trend_evaluator_bundle(
    *,
    repo_root: str | Path,
    execution_environment: Mapping[str, Any],
) -> QELongTrendEvaluatorBundle:
    root = Path(repo_root).resolve()
    snapshot_id = str(execution_environment.get("execution_environment_snapshot_id") or "").strip()
    environment_sha = str(
        execution_environment.get("execution_environment_manifest_sha256") or ""
    ).strip().lower()
    environment_manifest = execution_environment.get("manifest")
    if not snapshot_id or not _SHA256_RE.fullmatch(environment_sha) or not isinstance(environment_manifest, Mapping):
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_ENVIRONMENT_MISMATCH,
            "QE node execution environment identity is incomplete",
        )

    files: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    for relative_path in BUNDLE_SOURCE_PATHS:
        _validate_bundle_relative_path(relative_path)
        path = (root / relative_path).resolve()
        if root not in path.parents or not path.is_file() or path.is_symlink():
            raise QELongTrendError(
                QELongTrendReason.BUNDLE_INVALID,
                f"bundle source is missing, linked, or outside repository: {relative_path}",
            )
        payload = path.read_bytes()
        try:
            source = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise QELongTrendError(
                QELongTrendReason.BUNDLE_INVALID,
                f"bundle source is not UTF-8: {relative_path}",
            ) from exc
        digest = hashlib.sha256(payload).hexdigest()
        files[relative_path] = source
        rows.append({"relative_path": relative_path, "sha256": digest, "size_bytes": len(payload)})

    evaluator_rows = [
        row
        for row in rows
        if Path(str(row["relative_path"])).name
        in {
            "long_trend_evaluation_contract.py",
            "long_trend_data_reader.py",
            "long_trend_evaluation.py",
            "qe_dataset_contract.py",
            "long_trend_pickle_parser_entry.py",
            "long_trend_worker_entry.py",
        }
    ]
    evaluator_source_sha = canonical_sha256({"files": evaluator_rows})
    python_manifest = environment_manifest.get("python")
    if not isinstance(python_manifest, Mapping):
        raise QELongTrendError(
            QELongTrendReason.EXECUTION_ENVIRONMENT_MISMATCH,
            "QE node execution environment is missing Python ABI identity",
        )
    manifest: dict[str, Any] = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "evaluator_version": EVALUATOR_VERSION,
        "evaluator_source_sha256": evaluator_source_sha,
        "execution_environment_snapshot_id": snapshot_id,
        "execution_environment_manifest_sha256": environment_sha,
        "python_abi": {
            "implementation": python_manifest.get("implementation"),
            "version": python_manifest.get("version"),
            "cache_tag": python_manifest.get("cache_tag"),
        },
        "files": rows,
    }
    bundle_sha = canonical_sha256(manifest)
    manifest["bundle_sha256"] = bundle_sha
    files[BUNDLE_MANIFEST_NAME] = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return QELongTrendEvaluatorBundle(
        schema_version=BUNDLE_SCHEMA_VERSION,
        bundle_sha256=bundle_sha,
        evaluator_source_sha256=evaluator_source_sha,
        execution_environment_snapshot_id=snapshot_id,
        execution_environment_manifest_sha256=environment_sha,
        manifest=manifest,
        files=files,
    )


def _validate_bundle_relative_path(value: str) -> None:
    path = PurePosixPath(str(value or ""))
    if not value or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise QELongTrendError(QELongTrendReason.BUNDLE_INVALID, f"invalid bundle path: {value!r}")

