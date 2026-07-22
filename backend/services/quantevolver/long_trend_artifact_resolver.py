"""Exact Recorder and catalog resolver shared by normal and historical F-014."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence

from backend.services.quantevolver.long_trend_evaluation_contract import (
    QELongTrendError,
    QELongTrendReason,
    canonical_sha256,
)

CATALOG_SCHEMA_VERSIONS = frozenset({"hmm_qe_asset_catalog_v1", "qe_workspace_catalog_v1"})
ORDER_PARSER_CONTRACTS = frozenset({"qe_order_evidence_v1", "qe_trade_evidence_v1"})


@dataclass(frozen=True)
class RecorderArtifactInventory:
    task_id: str
    loop_id: str
    experiment_id: str
    recorder_id: str
    artifact_prefix: str
    backtest_freq: str
    catalog_completeness: str
    artifacts: dict[str, dict[str, Any] | None]
    warnings: tuple[str, ...]
    input_manifest_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "loop_id": self.loop_id,
            "experiment_id": self.experiment_id,
            "recorder_id": self.recorder_id,
            "artifact_prefix": self.artifact_prefix,
            "backtest_freq": self.backtest_freq,
            "catalog_completeness": self.catalog_completeness,
            "artifacts": self.artifacts,
            "warnings": list(self.warnings),
            "input_manifest_sha256": self.input_manifest_sha256,
        }


def build_recorder_artifact_prefix(recorder_ref: Mapping[str, Any]) -> tuple[str, str, str]:
    experiment_id = _safe_component(recorder_ref.get("experiment_id"), "experiment_id")
    recorder_id = _safe_component(recorder_ref.get("recorder_id"), "recorder_id")
    return experiment_id, recorder_id, f"mlruns/{experiment_id}/{recorder_id}/artifacts"


def resolve_long_trend_recorder_artifacts(
    *,
    task_id: str,
    loop_id: str,
    recorder_ref: Mapping[str, Any],
    catalog: Mapping[str, Any],
    backtest_freq: str,
) -> RecorderArtifactInventory:
    normalized_task = _safe_component(task_id, "task_id")
    normalized_loop = _safe_component(loop_id, "loop_id")
    frequency = _safe_component(backtest_freq, "backtest_freq")
    experiment_id, recorder_id, prefix = build_recorder_artifact_prefix(recorder_ref)
    if str(catalog.get("task_id") or "") != normalized_task:
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, "catalog task identity mismatch")
    catalog_loop = str(catalog.get("loop_name") or catalog.get("loop_id") or "")
    if catalog_loop != normalized_loop:
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, "catalog Loop identity mismatch")
    schema_version = str(catalog.get("schema_version") or "")
    if schema_version not in CATALOG_SCHEMA_VERSIONS:
        raise QELongTrendError(
            QELongTrendReason.NON_QE_SOURCE_REJECTED,
            f"unsupported workspace catalog schema: {schema_version!r}",
        )
    completeness = str(catalog.get("catalog_completeness") or "").lower()
    if completeness not in {"complete", "partial"}:
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, "catalog completeness is invalid")
    files = catalog.get("files")
    if not isinstance(files, Sequence) or isinstance(files, (str, bytes, bytearray)):
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, "catalog files must be an array")
    entries: dict[str, dict[str, Any]] = {}
    for item in files:
        if not isinstance(item, Mapping):
            raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, "catalog file entry is invalid")
        path = str(item.get("relative_path") or "")
        _safe_relative_path(path)
        if path in entries:
            raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, f"duplicate catalog path: {path}")
        entries[path] = dict(item)

    candidates: dict[str, tuple[str, ...]] = {
        "prediction": (f"{prefix}/pred.pkl",),
        "label": (f"{prefix}/label.pkl", f"{prefix}/sig_analysis/label.pkl"),
        "params": (f"{prefix}/params.pkl", f"{prefix}/params_pkl"),
        "portfolio_report": (f"{prefix}/portfolio_analysis/report_normal_1day.pkl",),
        "positions": (f"{prefix}/portfolio_analysis/positions_normal_1day.pkl",),
        "indicator_summary": (f"{prefix}/portfolio_analysis/indicators_normal_{frequency}.pkl",),
        "indicator_object": (f"{prefix}/portfolio_analysis/indicators_normal_{frequency}_obj.pkl",),
    }
    artifacts: dict[str, dict[str, Any] | None] = {
        name: _first_entry(entries, paths) for name, paths in candidates.items()
    }
    for path, item in entries.items():
        if not path.startswith(prefix + "/"):
            continue
        parser_contract = str(item.get("parser_contract") or "")
        if parser_contract in ORDER_PARSER_CONTRACTS:
            artifact_name = "orders" if parser_contract == "qe_order_evidence_v1" else "trades"
            if artifacts.get(artifact_name) is not None:
                raise QELongTrendError(
                    QELongTrendReason.NON_QE_SOURCE_REJECTED,
                    f"multiple authoritative {artifact_name} artifacts in catalog",
                )
            artifacts[artifact_name] = dict(item)

    warnings = [str(item) for item in (catalog.get("warnings") or [])]
    if completeness == "partial":
        warnings.append(QELongTrendReason.WORKSPACE_CATALOG_PARTIAL.value)
    summary = artifacts["indicator_summary"]
    obj = artifacts["indicator_object"]
    other_frequencies = sorted(
        path
        for path in entries
        if path.startswith(f"{prefix}/portfolio_analysis/indicators_normal_")
        and path not in candidates["indicator_summary"] + candidates["indicator_object"]
    )
    if summary is None and obj is None and other_frequencies:
        raise QELongTrendError(
            QELongTrendReason.INDICATOR_FREQUENCY_CONFLICT,
            "frozen backtest frequency does not match Recorder indicator paths",
            context={"backtest_freq": frequency, "observed_paths": other_frequencies[:20]},
        )
    if obj is None:
        warnings.append(QELongTrendReason.INDICATOR_OBJECT_MISSING.value)

    manifest = {
        name: (
            {
                "relative_path": item.get("relative_path"),
                "sha256": item.get("sha256"),
                "size_bytes": item.get("size_bytes"),
                "parser_contract": item.get("parser_contract"),
            }
            if item is not None
            else {"type": "explicit_null", "field": name}
        )
        for name, item in sorted(artifacts.items())
    }
    return RecorderArtifactInventory(
        task_id=normalized_task,
        loop_id=normalized_loop,
        experiment_id=experiment_id,
        recorder_id=recorder_id,
        artifact_prefix=prefix,
        backtest_freq=frequency,
        catalog_completeness=completeness,
        artifacts=artifacts,
        warnings=tuple(sorted(set(warnings))),
        input_manifest_sha256=canonical_sha256(manifest),
    )


def _first_entry(entries: Mapping[str, dict[str, Any]], paths: Sequence[str]) -> dict[str, Any] | None:
    for path in paths:
        if path in entries:
            return dict(entries[path])
    return None


def _safe_component(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text or "/" in text or "\\" in text or text in {".", ".."}:
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, f"invalid {field_name}")
    return text


def _safe_relative_path(value: str) -> None:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise QELongTrendError(QELongTrendReason.NON_QE_SOURCE_REJECTED, f"invalid catalog path: {value!r}")

