"""Phase 0 input provenance adapter for replayable Phase 1 evaluations."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

from .evaluator import DateCoveragePlan
from .models import CandidateRecord, canonical_json_sha256

SOURCE_MANIFEST_VERSION = "hmm_evaluation_source_manifest_v1"


def build_source_manifest(
    *,
    base_loop_ref: str,
    predictions: pd.DataFrame,
    labels: pd.DataFrame,
    artifact_source_info: Mapping[str, Mapping[str, Any]],
    candidate: CandidateRecord,
    date_plan: DateCoveragePlan,
    label_horizon_days: int,
    market_forward_return: Mapping[str, Any],
    warnings: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build a secret/path-free manifest from already verified Phase 0 reads."""

    task_id, loop_name = _parse_loop_ref(base_loop_ref)
    artifacts = [
        _artifact_entry(name, artifact_source_info.get(name), len(frame))
        for name, frame in (("pred.pkl", predictions), ("label.pkl", labels))
    ]
    symbols = sorted({str(item).strip() for item in predictions["symbol"] if str(item).strip()})
    prediction_dates = _date_range(predictions)
    label_dates = _date_range(labels)
    candidate_manifest = candidate.artifact_manifest
    warning_list = [dict(item) for item in warnings]
    degraded = date_plan.degraded or bool(warning_list)
    manifest = {
        "schema_version": SOURCE_MANIFEST_VERSION,
        "base_loop_ref": base_loop_ref,
        "task_id": task_id,
        "loop_name": loop_name,
        "artifacts": artifacts,
        "prediction_date_range": prediction_dates,
        "label_date_range": label_dates,
        "label_horizon_days": label_horizon_days,
        "universe": {
            "type": "prediction_artifact_all",
            "symbol_count": len(symbols),
            "universe_hash": canonical_json_sha256(symbols),
        },
        "candidate": {
            "candidate_id": candidate.candidate_id,
            "candidate_manifest_hash": candidate.manifest_hash,
            "artifact_sha256": candidate_manifest.artifact_sha256,
            "artifact_uri": candidate_manifest.artifact_uri,
            "algorithm_version": candidate_manifest.algorithm_version,
        },
        "date_coverage": date_plan.as_evidence(),
        "market_forward_return": dict(market_forward_return),
        "evidence_quality": "degraded" if degraded else "complete",
        "warnings": warning_list,
    }
    _assert_no_absolute_paths(manifest)
    return manifest


def _artifact_entry(
    artifact_name: str,
    source_info: Mapping[str, Any] | None,
    actual_row_count: int,
) -> dict[str, Any]:
    if source_info is None:
        raise ValueError(f"source information is missing for {artifact_name}")
    source = str(source_info.get("source") or "").strip()
    sha256 = str(source_info.get("sha256") or "").strip().lower()
    uri = str(source_info.get("uri") or "").strip()
    size_bytes = source_info.get("size_bytes")
    declared_rows = source_info.get("row_count")
    if not source or not uri or len(sha256) != 64 or size_bytes is None:
        raise ValueError(f"source information is incomplete for {artifact_name}")
    if declared_rows is not None and int(declared_rows) != actual_row_count:
        raise ValueError(
            f"source row count mismatch for {artifact_name}: declared={declared_rows}, actual={actual_row_count}"
        )
    return {
        "artifact_name": artifact_name,
        "source": source,
        "uri": uri,
        "sha256": sha256,
        "size_bytes": int(size_bytes),
        "row_count": actual_row_count,
        "trust_level": str(source_info.get("trust_level") or "trusted_computational_input"),
        "zero_copy": bool(source_info.get("zero_copy", False)),
        "fallback": bool(source_info.get("fallback", False)),
        "schema_version": source_info.get("remote_schema_version"),
    }


def _parse_loop_ref(base_loop_ref: str) -> tuple[str, str]:
    parts = str(base_loop_ref or "").split("/")
    if len(parts) != 2 or any(not part or part in {".", ".."} for part in parts):
        raise ValueError("base_loop_ref must use '<task_id>/LoopN'")
    return parts[0], parts[1]


def _date_range(frame: pd.DataFrame) -> dict[str, str]:
    dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
    return {"start": min(dates).isoformat(), "end": max(dates).isoformat()}


def _assert_no_absolute_paths(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key).lower() in {"path", "cache_path", "absolute_path"}:
                raise ValueError("source manifest cannot contain filesystem paths")
            _assert_no_absolute_paths(nested)
        return
    if isinstance(value, (list, tuple)):
        for nested in value:
            _assert_no_absolute_paths(nested)
        return
    if isinstance(value, str):
        if len(value) >= 3 and value[1:3] in {":/", ":\\"}:
            raise ValueError("source manifest cannot contain absolute Windows paths")
