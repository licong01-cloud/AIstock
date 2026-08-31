"""Fixed external worker entry for one immutable QE long-trend evaluation."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping

from backend.services.quantevolver.long_trend_data_reader import (
    QELongTrendDatasetReader,
    inspect_qe_snapshot_identity,
    verify_outcome_snapshot_extension,
)
from backend.services.quantevolver.long_trend_evaluation import (
    ExecutionEvidenceBundle,
    QELongTrendEvaluationEngine,
)
from backend.services.quantevolver.long_trend_evaluation_contract import (
    EVALUATOR_VERSION,
    QELongTrendEvaluationContext,
    canonical_sha256,
    get_long_trend_profile,
)

WORKER_REQUEST_SCHEMA = "qe_long_trend_worker_request_v1"
TERMINAL_SCHEMA = "qe_long_trend_worker_terminal_v1"
COMPACT_SCHEMA = "qe_long_trend_worker_compact_v1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    try:
        request = _read_json(Path(args.request).resolve())
        terminal = run_worker(request=request, output_dir=output_dir, started=started)
        _atomic_json(output_dir / "worker_terminal_receipt.json", terminal)
        return 0
    except Exception as exc:
        failure = _failure_terminal(args, exc, started)
        compact = {
            "schema_version": COMPACT_SCHEMA,
            "receipt_stage": "worker_terminal",
            "evaluation_id": failure["evaluation_id"],
            "worker_terminal_sha256": {"type": "explicit_null", "field": "worker_terminal_sha256"},
            "family_status": failure["family_status"],
            "headline_metrics": {},
            "data_action_plan": failure["data_action_plan"],
            "platform_delivery_status": failure["platform_delivery_status"],
            "artifact_manifest_uri": {"type": "explicit_null", "field": "artifact_manifest_uri"},
            "artifact_manifest_sha256": {"type": "explicit_null", "field": "artifact_manifest_sha256"},
        }
        compact_path = output_dir / "worker_compact_receipt.json"
        _atomic_json(compact_path, compact)
        failure["artifacts"]["worker_compact_receipt"] = _file_metadata(compact_path)
        _atomic_json(output_dir / "worker_terminal_receipt.json", failure)
        return 2


def run_worker(*, request: Mapping[str, Any], output_dir: Path, started: float) -> dict[str, Any]:
    if str(request.get("schema_version") or "") != WORKER_REQUEST_SCHEMA:
        raise ValueError("unsupported worker request schema")
    evaluation_id = str(request.get("evaluation_id") or "")
    if not evaluation_id.startswith("qelt_"):
        raise ValueError("invalid worker evaluation identity")
    if str(request.get("evaluator_version") or "") != EVALUATOR_VERSION:
        raise ValueError("worker evaluator version mismatch")
    capability = _runtime_capability()
    parser_receipt = _run_parser(request, output_dir)
    normalized = _load_normalized_artifacts(parser_receipt, output_dir / "normalized")

    feature_root = Path(str(request.get("feature_data_root") or "")).resolve()
    outcome_root = Path(str(request.get("outcome_data_root") or "")).resolve()
    feature_workspace_root = Path(str(request.get("feature_workspace_root") or "")).resolve()
    outcome_workspace_root = Path(str(request.get("outcome_workspace_root") or "")).resolve()
    feature_identity = inspect_qe_snapshot_identity(feature_root)
    outcome_identity = inspect_qe_snapshot_identity(outcome_root)
    _require_snapshot_identity(request.get("feature_snapshot"), feature_identity, "feature")
    _require_snapshot_identity(request.get("outcome_snapshot"), outcome_identity, "outcome")

    feature_reader = QELongTrendDatasetReader(
        factor_data_dir=feature_root,
        qe_workspace_root=feature_workspace_root,
        qe_dataset_contract_id=str(request["qe_dataset_contract_id"]),
        snapshot_identity=feature_identity,
    )
    outcome_reader = QELongTrendDatasetReader(
        factor_data_dir=outcome_root,
        qe_workspace_root=outcome_workspace_root,
        qe_dataset_contract_id=str(request["qe_dataset_contract_id"]),
        snapshot_identity=outcome_identity,
    )
    feature_prices = feature_reader.load_prices(
        start_date=feature_identity.start_date,
        end_date=feature_identity.end_date,
    )
    outcome_frames = outcome_reader.load(
        start_date=outcome_identity.start_date,
        end_date=str(request["evaluation_asof"]),
        include_sector=True,
    )
    parity = verify_outcome_snapshot_extension(
        feature_identity=feature_identity,
        outcome_identity=outcome_identity,
        feature_prices=feature_prices,
        outcome_prices=outcome_frames.prices,
    )
    del feature_prices
    context = QELongTrendEvaluationContext(
        run_id=str(request["run_id"]),
        evaluator_source_sha256=str(request["evaluator_source_sha256"]),
        feature_snapshot=feature_identity,
        outcome_snapshot=outcome_identity,
        overlap_receipt=parity,
        input_artifact_hashes=dict(request.get("input_artifact_hashes") or {}),
        execution_environment_manifest_sha256=str(request["execution_environment_manifest_sha256"]),
    )
    evidence = ExecutionEvidenceBundle(
        indicator=normalized.get("indicator_object"),
        trades=normalized.get("trades"),
        orders=normalized.get("orders"),
    )
    result = QELongTrendEvaluationEngine(get_long_trend_profile(str(request["profile_id"]))).evaluate(
        context=context,
        predictions=normalized.get("prediction"),
        prices=outcome_frames.prices,
        sectors=outcome_frames.sectors,
        labels=normalized.get("label"),
        label_horizon=request.get("label_horizon"),
        positions=normalized.get("positions"),
        portfolio_report=normalized.get("portfolio_report"),
        execution_evidence=evidence,
        strategy_topk=request.get("strategy_topk"),
    )
    del outcome_frames, normalized
    artifacts: dict[str, dict[str, Any]] = {}
    family = {name: status.as_dict() for name, status in result.family_status.items()}
    if family["signal_path"]["status"] in {"COMPUTED", "COMPUTED_WITH_LIMITATIONS"}:
        artifacts["signal_observations"] = _write_parquet(
            output_dir / "signal_observations.parquet", result.signal_observations
        )
    if family["position_episode"]["status"] in {"COMPUTED", "COMPUTED_WITH_LIMITATIONS"}:
        artifacts["holding_episodes"] = _write_parquet(
            output_dir / "holding_episodes.parquet", result.holding_episodes
        )

    task_status = "succeeded"
    if any(item["status"] != "COMPUTED" for item in family.values()):
        task_status = "partial"
    compact = {
        "schema_version": COMPACT_SCHEMA,
        "receipt_stage": "worker_terminal",
        "evaluation_id": evaluation_id,
        "worker_terminal_sha256": {"type": "explicit_null", "field": "worker_terminal_sha256"},
        "family_status": family,
        "headline_metrics": _headline_metrics(result.metrics),
        "data_action_plan": _data_actions(family),
        "platform_delivery_status": {"worker": task_status, "cas": "awaiting_collect"},
        "artifact_manifest_uri": {"type": "explicit_null", "field": "artifact_manifest_uri"},
        "artifact_manifest_sha256": {"type": "explicit_null", "field": "artifact_manifest_sha256"},
    }
    compact_path = output_dir / "worker_compact_receipt.json"
    _atomic_json(compact_path, compact)
    artifacts["worker_compact_receipt"] = _file_metadata(compact_path)
    terminal = {
        "schema_version": TERMINAL_SCHEMA,
        "evaluation_id": evaluation_id,
        "job_id": request.get("job_id"),
        "attempt_id": request.get("attempt_id"),
        "request_sha": request.get("request_sha"),
        "node_id": request.get("node_id"),
        "status": task_status,
        "profile_id": request.get("profile_id"),
        "profile_sha256": request.get("profile_sha256"),
        "evaluator_version": request.get("evaluator_version"),
        "evaluator_source_sha256": request.get("evaluator_source_sha256"),
        "bundle_sha256": request.get("bundle_sha256"),
        "execution_environment_snapshot_id": request.get("execution_environment_snapshot_id"),
        "execution_environment_manifest_sha256": request.get("execution_environment_manifest_sha256"),
        "input_manifest_sha256": request.get("input_manifest_sha256"),
        "evaluation_asof": request.get("evaluation_asof"),
        "family_status": family,
        "metrics": result.metrics,
        "data_action_plan": _data_actions(family),
        "parser_artifact_statuses": dict(parser_receipt.get("artifacts") or {}),
        "parser_receipt_sha256": canonical_sha256(parser_receipt),
        "runtime_capability": capability,
        "platform_delivery_status": {"worker": task_status, "cas": "awaiting_collect"},
        "artifacts": artifacts,
        "stats": _resource_stats(started, result),
    }
    return terminal


def _run_parser(request: Mapping[str, Any], output_dir: Path) -> dict[str, Any]:
    normalized_dir = output_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    parser_request = {
        "schema_version": "qe_long_trend_parser_request_v1",
        "evaluation_id": request["evaluation_id"],
        "allowed_root": request["loop_root"],
        "inputs": dict(request.get("artifact_paths") or {}),
    }
    parser_request_path = output_dir / "parser_request.json"
    _atomic_json(parser_request_path, parser_request)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(("PG", "DATABASE_", "POSTGRES_", "QE_RESOURCE_", "AISTOCK_PREDICTION_STORE_"))
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "backend.services.quantevolver.long_trend_pickle_parser_entry",
            "--request",
            str(parser_request_path),
            "--output-dir",
            str(normalized_dir),
        ],
        cwd=str(output_dir),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=int(request.get("parser_timeout_seconds") or 300),
        check=False,
    )
    receipt = _read_json(normalized_dir / "parser_receipt.json")
    if completed.returncode != 0 or receipt.get("status") == "failed":
        raise RuntimeError(
            "QELT_PICKLE_PARSER_FAILED: "
            + json.dumps(
                {
                    "returncode": completed.returncode,
                    "receipt": receipt,
                    "stderr": completed.stderr[-4000:],
                },
                ensure_ascii=False,
            )
        )
    return receipt


def _load_normalized_artifacts(receipt: Mapping[str, Any], normalized_dir: Path) -> dict[str, Any]:
    import pandas as pd

    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("parser receipt artifacts are invalid")
    loaded: dict[str, Any] = {}
    for name, item in artifacts.items():
        if not isinstance(item, Mapping) or item.get("status") != "parsed":
            continue
        path = (normalized_dir / str(item["relative_path"])).resolve()
        path.relative_to(normalized_dir.resolve())
        digest, size = _sha256_file(path)
        if digest != item.get("sha256") or size != int(item.get("size_bytes") or -1):
            raise ValueError(f"normalized parser artifact hash mismatch: {name}")
        loaded[str(name)] = pd.read_parquet(path)
    return loaded


def _runtime_capability() -> dict[str, Any]:
    imports = {}
    for import_name, distribution in (
        ("numpy", "numpy"),
        ("pandas", "pandas"),
        ("tables", "tables"),
        ("pyarrow", "pyarrow"),
    ):
        try:
            __import__(import_name)
            version = importlib.metadata.version(distribution)
        except Exception as exc:
            raise RuntimeError(
                f"QELT_NODE_CAPABILITY_UNAVAILABLE: {import_name}: {type(exc).__name__}: {exc}"
            ) from exc
        imports[import_name] = version
    return {"schema_version": "qe_long_trend_runtime_capability_v1", "imports": imports}


def _require_snapshot_identity(expected_value: Any, actual: Any, label: str) -> None:
    if not isinstance(expected_value, Mapping):
        raise ValueError(f"{label} snapshot identity is missing")
    expected = dict(expected_value)
    expected_lineage = expected.get("lineage_parent_ids")
    if not isinstance(expected_lineage, (list, tuple)):
        raise ValueError(f"{label} snapshot lineage_parent_ids must be an array")
    expected["lineage_parent_ids"] = tuple(expected_lineage)
    actual_dict = asdict(actual)
    if expected != actual_dict:
        raise ValueError(
            f"QELT_EXECUTION_ENVIRONMENT_MISMATCH: {label} snapshot identity differs: "
            f"expected={expected} actual={actual_dict}"
        )


def _headline_metrics(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    names = {"capture_ratio", "episode_capture", "time_to_hit", "mfe", "mae"}
    selected: dict[str, Any] = {}
    for row in metrics:
        metric_name = str(row.get("metric_name") or "")
        if any(token in metric_name for token in names):
            selected[f"{metric_name}:{row.get('slice_name')}:{row.get('horizon')}"] = row.get("value")
    return selected


def _data_actions(family: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family_name, status in family.items():
        for item in status.get("data_actions") or []:
            if isinstance(item, Mapping):
                rows.append({"family": family_name, **dict(item)})
    return rows


def _resource_stats(started: float, result: Any) -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "duration_seconds": max(0.0, time.monotonic() - started),
        "process_rss_peak_bytes": int(usage.ru_maxrss) * (1024 if sys.platform != "darwin" else 1),
        "signal_rows": int(len(result.signal_observations)),
        "episode_rows": int(len(result.holding_episodes)),
        "metric_rows": int(len(result.metrics)),
    }


def _write_parquet(path: Path, frame: Any) -> dict[str, Any]:
    frame.to_parquet(path, index=True)
    metadata = _file_metadata(path)
    metadata["row_count"] = int(len(frame))
    metadata["columns"] = [str(value) for value in frame.columns]
    return metadata


def _file_metadata(path: Path) -> dict[str, Any]:
    digest, size = _sha256_file(path)
    return {"relative_path": path.name, "sha256": digest, "size_bytes": size}


def _failure_terminal(args: Any, exc: Exception, started: float) -> dict[str, Any]:
    evaluation_id = "unknown"
    request_read_error: dict[str, str] | None = None
    try:
        request = _read_json(Path(args.request).resolve())
        evaluation_id = str(request.get("evaluation_id") or "unknown")
    except Exception as request_exc:
        request = {}
        request_read_error = {
            "error_type": type(request_exc).__name__,
            "message": str(request_exc),
        }
    reason_code = _reason_code(exc)
    families = {
        name: {
            "status": "NOT_COMPUTABLE",
            "available_inputs": [],
            "missing_inputs": ["worker_completed_inputs"],
            "coverage": {},
            "limitations": [str(exc)],
            "supporting_artifacts": [],
            "reason_codes": [reason_code],
            "data_actions": [],
        }
        for name in (
            "signal_path", "position_episode", "portfolio_result",
            "order_fill", "execution_cause", "sector_regime",
        )
    }
    return {
        "schema_version": TERMINAL_SCHEMA,
        "evaluation_id": evaluation_id,
        "job_id": request.get("job_id"),
        "attempt_id": request.get("attempt_id"),
        "request_sha": request.get("request_sha"),
        "status": "failed",
        "reason_code": reason_code,
        "reason_json": {
            "error_type": type(exc).__name__,
            "message": str(exc),
            "request_read_error": request_read_error,
        },
        "family_status": families,
        "metrics": [],
        "data_action_plan": [],
        "platform_delivery_status": {"worker": "failed", "cas": "awaiting_repair"},
        "artifacts": {},
        "stats": {"duration_seconds": max(0.0, time.monotonic() - started)},
    }


def _reason_code(exc: Exception) -> str:
    text = str(exc)
    for code in (
        "QELT_PICKLE_PARSER_FAILED",
        "QELT_NODE_CAPABILITY_UNAVAILABLE",
        "QELT_EXECUTION_ENVIRONMENT_MISMATCH",
    ):
        if code in text:
            return code
    return "QELT_WORKER_FAILED"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                dict(payload),
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
                default=_json_default,
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _json_default(value: Any) -> Any:
    import numpy as np
    import pandas as pd

    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    raise TypeError(f"unsupported long-trend receipt JSON value: {type(value).__name__}")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


if __name__ == "__main__":
    raise SystemExit(main())
