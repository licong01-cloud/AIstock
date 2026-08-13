from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows can import the WSL-only pipeline for diagnostics/tests.
    _resource = None

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import (
    FrozenAdvisoryPolicyDatasetRequestV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.policy_cpcv import build_policy_cpcv_paths
from backend.services.advisory_model_first.policy_dataset_bundle import publish_policy_dataset_bundle
from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels
from backend.services.advisory_model_first.policy_rank_source import build_policy_rankings
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_suspend_rows,
    load_trading_calendar,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio


class PolicyDatasetProgress:
    def __init__(self, *, limit_bytes: int) -> None:
        self.limit_bytes = limit_bytes
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def stage(self, name: str, started: float, **details: Any) -> None:
        peak = _peak_rss_bytes()
        receipt = {
            "stage": name,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": peak,
            **details,
        }
        self.stages.append(receipt)
        print(json.dumps(receipt, ensure_ascii=True, sort_keys=True), flush=True)
        if peak > self.limit_bytes:
            raise AdvisoryModelFirstError(
                "policy dataset build exceeded the approved RSS limit",
                reason_code="ADVISORY_POLICY_DATASET_MEMORY_LIMIT_EXCEEDED",
                context={"stage": name, "peak_rss_bytes": peak, "limit_bytes": self.limit_bytes},
            )

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_policy_dataset_resource_report_v1",
            "peak_rss_bytes": _peak_rss_bytes(),
            "limit_bytes": self.limit_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def run_policy_dataset_pipeline(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    environment = _verify_environment(request)
    progress = PolicyDatasetProgress(limit_bytes=request.resource_max_rss_bytes)

    started = time.monotonic()
    source = ExactPredictionSource(request.prediction_store_root)
    actual = source.describe_all(request.representative_seed_run_ids.values())
    mismatches = [
        run_id
        for run_id, descriptor in request.prediction_artifacts.items()
        if run_id not in actual or actual[run_id].model_dump(mode="json") != descriptor.model_dump(mode="json")
    ]
    if mismatches:
        raise AdvisoryModelFirstError(
            "policy dataset Prediction Store artifacts changed after request freeze",
            reason_code="ADVISORY_MODEL_PREDICTION_HASH_MISMATCH",
            context={"run_ids": mismatches},
        )
    leg_frames = {
        leg_id: source.load_scores(run_id, verify_artifact=False)
        for leg_id, run_id in request.representative_seed_run_ids.items()
    }
    date_sets = [
        set(pd.DatetimeIndex(frame["trade_date"]).normalize())
        for frame in leg_frames.values()
    ]
    common_dates = sorted(set.intersection(*date_sets))
    candidate_decisions = pd.DatetimeIndex(
        [
            value
            for value in common_dates
            if pd.Timestamp(request.decision_date_start) <= value <= pd.Timestamp(request.decision_date_end)
        ]
    )
    context_decisions = pd.DatetimeIndex(
        [
            value
            for value in common_dates
            if pd.Timestamp(request.decision_date_start) <= value < pd.Timestamp(request.data_cutoff)
        ]
    )
    if candidate_decisions.empty or context_decisions.empty:
        raise AdvisoryModelFirstError(
            "policy dataset representative legs have no common decision dates",
            reason_code="ADVISORY_POLICY_RANK_INCOMPLETE",
        )
    progress.stage(
        "input_identity",
        started,
        candidate_decision_date_count=len(candidate_decisions),
        rank_context_date_count=len(context_decisions),
        leg_count=len(leg_frames),
    )

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    calendar = load_trading_calendar(request.decision_date_start, request.data_cutoff)
    identity = {
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
    }
    rank_result = build_policy_rankings(
        leg_frames=leg_frames,
        terminal_weights=request.terminal_weights,
        decision_dates=context_decisions,
        trading_calendar=calendar,
        identity=identity,
    )
    rank_result.rankings["is_candidate_decision"] = rank_result.rankings[
        "decision_as_of_trade_date"
    ].isin(candidate_decisions)
    del leg_frames
    gc.collect()
    progress.stage(
        "top40_rankings",
        started,
        row_count=len(rank_result.rankings),
        candidate_date_count=len(candidate_decisions),
        context_date_count=len(context_decisions),
    )

    started = time.monotonic()
    symbols = sorted(rank_result.rankings["instrument"].unique())
    daily = load_qlib_daily(
        symbols,
        start=request.decision_date_start,
        end=request.data_cutoff,
    )
    benchmark = load_qlib_daily(
        [request.cost_policy.benchmark_instrument],
        start=request.decision_date_start,
        end=request.data_cutoff,
        fields=("$open",),
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=request.decision_date_start,
        end=request.data_cutoff,
        instruments=symbols,
    )
    progress.stage(
        "file_market_projection",
        started,
        symbol_count=len(symbols),
        daily_row_count=len(daily),
        suspend_row_count=len(suspend),
    )

    policy = transition_policy_from_payload(request.shadow_policy)
    request_identity = {
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        **identity,
    }
    started = time.monotonic()
    label_result = build_policy_episode_labels(
        rankings=rank_result.rankings,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost_policy=request.cost_policy,
        request_identity=request_identity,
        candidate_decision_dates=candidate_decisions,
    )
    progress.stage(
        "candidate_policy_labels",
        started,
        row_count=len(label_result.labels),
        matured_count=int((label_result.labels["label_status"] == "MATURED").sum()),
    )

    started = time.monotonic()
    shadow = replay_shadow_portfolio(
        rankings=rank_result.rankings,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost_policy=request.cost_policy,
        request_id=request.request_id,
        candidate_decision_dates=candidate_decisions,
    )
    progress.stage("shadow_portfolio", started, day_count=len(shadow.daily), episode_count=len(shadow.episodes))

    started = time.monotonic()
    cpcv = build_policy_cpcv_paths(
        label_result.labels,
        split_policy=request.split_policy,
        trading_calendar=calendar,
        request_sha256=request.request_sha256,
    )
    pbo = {
        "schema_version": "advisory_policy_pbo_receipt_v1",
        "status": "NOT_COMPUTABLE",
        "reason_code": "NOT_COMPUTABLE_NO_TRIAL_RESULTS",
        "method": "advisory_block_score_cscv_pbo_v1",
        "trial_count": 0,
        "group_count": request.split_policy.group_count,
        "partition_count": 0,
        "pbo": None,
        "partitions": [],
    }
    progress.stage("cpcv_pbo", started, path_count=len(cpcv.paths), pbo_status=pbo["status"])

    started = time.monotonic()
    source_receipt = {
        "schema_version": "advisory_policy_dataset_source_receipt_v1",
        "environment": environment,
        "qlib_daily_fields": [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "factor",
            "up_limit_price",
            "down_limit_price",
            "prev_close",
            "limit_up",
            "limit_down",
        ],
        "prediction_artifacts": {
            key: value.model_dump(mode="json") for key, value in sorted(actual.items())
        },
        "candidate_decision_date_count": len(candidate_decisions),
        "candidate_decision_date_start": candidate_decisions[0].date().isoformat(),
        "candidate_decision_date_end": candidate_decisions[-1].date().isoformat(),
        "rank_context_date_count": len(context_decisions),
        "rank_context_date_end": context_decisions[-1].date().isoformat(),
        "data_cutoff": request.data_cutoff,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_policy_dataset_bundle(
        request=request,
        rankings=rank_result.rankings,
        labels=label_result.labels,
        label_coverage=label_result.coverage.to_dict("records"),
        shadow_daily=shadow.daily,
        shadow_episodes=shadow.episodes,
        shadow_metrics=shadow.metrics,
        cpcv_payload={
            "schema_version": "advisory_policy_cpcv_paths_v1",
            "split_policy": request.split_policy.model_dump(mode="json"),
            "paths": list(cpcv.paths),
            "block_by_date": cpcv.block_by_date,
        },
        pbo_receipt=pbo,
        source_schema_receipt=source_receipt,
        resource_report=resource,
    )
    progress.stage("bundle_publish", started, bundle_id=bundle_id, bundle_path=str(bundle_path))
    return {
        "status": "BUILT",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "policy_dataset_bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "label_status_counts": label_result.labels["label_status"].value_counts().sort_index().to_dict(),
        "cpcv_ready_path_count": sum(path.get("status") == "READY" for path in cpcv.paths),
        "pbo_status": pbo["status"],
        "shadow_metrics": shadow.metrics,
        "resource_report": progress.report(),
    }


def _verify_environment(request: FrozenAdvisoryPolicyDatasetRequestV1) -> dict[str, Any]:
    if os.name == "nt" or "microsoft" not in platform.release().lower():
        raise AdvisoryModelFirstError(
            "policy dataset build must run inside WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        raise AdvisoryModelFirstError(
            "policy dataset build requires the rdagent-gpu Conda environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"conda_env": conda_env or None},
        )
    repository_root = Path(request.repository_root).resolve()
    git_command = ["git"]
    git_pointer = repository_root / ".git"
    if git_pointer.is_file():
        pointer = git_pointer.read_text(encoding="utf-8").strip()
        if not pointer.startswith("gitdir: "):
            raise AdvisoryModelFirstError(
                "policy dataset worktree has an invalid .git pointer",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            )
        raw_git_dir = pointer.removeprefix("gitdir: ").strip()
        try:
            translated = subprocess.run(
                ["wslpath", "-u", raw_git_dir],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise AdvisoryModelFirstError(
                "policy dataset WSL could not translate the worktree git directory",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            ) from exc
        git_command.append(f"--git-dir={translated}")
    try:
        commit = subprocess.run(
            [*git_command, "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise AdvisoryModelFirstError(
            "policy dataset repository identity cannot be read",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
        ) from exc
    if commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "policy dataset repository commit differs from the frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"expected": request.repository_commit, "actual": commit},
        )
    return {
        "platform_release": platform.release(),
        "conda_env": conda_env,
        "repository_commit": commit,
        "python": platform.python_version(),
        "pandas": importlib.metadata.version("pandas"),
    }


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024
