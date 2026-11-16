"""Read a selected baseline's sealed results without executing an experiment."""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any

from .models import sha256_json


def baseline_only_requested(source: Mapping[str, Any]) -> bool:
    from backend.services.multi_alpha.combine_backtest import REQUEST_SNAPSHOT_KEY

    config = source.get("run", {}).get("backtest_config_json") or {}
    snapshot = config.get(REQUEST_SNAPSHOT_KEY) or {}
    return (snapshot.get("prediction_task_selection") or {}).get("baseline_only") is True


def load_selected_baseline_evidence(
    source: Mapping[str, Any], *, adapter_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Validate existing remote-result bytes; never mutate the frozen manifests."""
    from backend.services.multi_alpha.combine_backtest import parse_request, is_baseline_only
    from backend.services.multi_alpha.combine_backtest import REQUEST_SNAPSHOT_KEY
    from backend.services.multi_alpha.durable_execution_adapter import QEWorkspacePredBacktestAdapter

    run = source["run"]
    request = parse_request(run["backtest_config_json"][REQUEST_SNAPSHOT_KEY])
    if not is_baseline_only(request) or run.get("status") != "succeeded":
        raise ValueError("baseline_archive_requires_successful_explicit_baseline_only")
    children = list(source.get("children") or ())
    if len(children) != 1:
        raise ValueError("baseline_archive_requires_exact_single_child")
    child = children[0]
    if (child.get("run_id") != run["id"] or child.get("child_kind") != "baseline"
            or child.get("child_key") != f"baseline:{request.baseline_leg_id}"
            or child.get("status") != "succeeded"):
        raise ValueError("baseline_archive_child_identity_mismatch")
    attempts = [a for a in source.get("attempts") or ()
                if a.get("attempt_id") == child.get("selected_attempt_id")]
    if len(attempts) != 1:
        raise ValueError("baseline_archive_selected_attempt_missing_or_ambiguous")
    attempt = attempts[0]
    if (attempt.get("child_id") != child["child_id"] or attempt.get("run_id") != run["id"]
            or attempt.get("status") != "succeeded"
            or attempt.get("execution_kind", "remote_execution") != "remote_execution"):
        raise ValueError("baseline_archive_selected_remote_attempt_mismatch")
    manifest = attempt.get("result_manifest_json") or {}
    if (not manifest or sha256_json(manifest) != attempt.get("result_manifest_hash")
            or sha256_json({k: v for k, v in manifest.items() if k != "manifest_hash"})
            != manifest.get("manifest_hash")):
        raise ValueError("baseline_archive_result_manifest_hash_mismatch")
    for key, value in {"run_id": run["id"], "child_id": child["child_id"],
                       "attempt_id": attempt["attempt_id"], "node_id": attempt.get("node_id"),
                       "qe_task_id": attempt.get("qe_task_id"),
                       "qe_loop_id": attempt.get("qe_loop_id")}.items():
        if not value or manifest.get(key) != value:
            raise ValueError(f"baseline_archive_result_identity_mismatch:{key}")
    adapter = (adapter_factory or QEWorkspacePredBacktestAdapter)()
    artifacts = adapter.load_published_artifacts(
        run_id=run["id"], child_id=child["child_id"], attempt_id=attempt["attempt_id"],
    )
    if (artifacts.artifact_manifest.get("manifest_hash") != manifest.get("artifact_manifest_hash")
            and manifest.get("submission_artifact_manifest_hash")
            != artifacts.artifact_manifest.get("manifest_hash")):
        raise ValueError("baseline_archive_published_manifest_mismatch")
    local_manifest = json.loads((artifacts.workspace / "result_manifest.json").read_text(encoding="utf-8"))
    if local_manifest != manifest:
        raise ValueError("baseline_archive_local_manifest_mismatch")
    if manifest.get("raw_result_file") != "qlib_results_enhanced.json":
        raise ValueError("baseline_archive_result_filename_mismatch")
    raw = json.loads((artifacts.workspace / manifest["raw_result_file"]).read_text(encoding="utf-8"))
    if sha256_json(raw) != manifest.get("raw_result_hash"):
        raise ValueError("baseline_archive_raw_result_hash_mismatch")
    window = manifest.get("result_window") or {}
    prediction_window = manifest.get("prediction_window") or {}
    count = window.get("trading_date_count")
    if (window.get("effective_start") != request.oos_start
            or window.get("effective_end") != request.oos_end
            or not isinstance(count, int) or isinstance(count, bool) or count <= 0
            or any(window.get(k) != prediction_window.get(k)
                   for k in ("effective_start", "effective_end", "trading_date_count"))):
        raise ValueError("baseline_archive_frozen_window_mismatch")
    metrics = dict(adapter.load_collected_metrics(artifacts))
    metrics.update(prediction_store_manifest=manifest.get("prediction_store_manifest"),
                   pred_persisted=manifest.get("prediction_store_manifest") is not None)
    if sha256_json(metrics) != manifest.get("metrics_hash"):
        raise ValueError("baseline_archive_metrics_hash_mismatch")
    return {
        "schema_version": "qe_selected_baseline_result_evidence_v1",
        "run_id": run["id"], "child_id": child["child_id"], "attempt_id": attempt["attempt_id"],
        "result_manifest_hash": attempt["result_manifest_hash"],
        "raw_result_hash": manifest["raw_result_hash"], "metrics_hash": manifest["metrics_hash"],
        "result_window": window, "metrics": metrics,
        "new_fit_predict_backtest": 0,
    }


def require_archive_preview(report: Mapping[str, Any], run_id: str) -> None:
    """HTTP success alone does not mean an archive preview is eligible."""
    if report.get("run_id") != run_id:
        raise ValueError("archive_preview_run_mismatch")
    if report.get("error") or report.get("skipped_reason"):
        raise ValueError(f"archive_preview_blocked:{report.get('skipped_reason') or report.get('error')}")
    if report.get("dry_run") is not True or report.get("written") is not False:
        raise ValueError("archive_preview_contract_invalid")
