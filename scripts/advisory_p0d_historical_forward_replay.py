#!/usr/bin/env python3
"""Run P0-D over immutable Historical Range days with a virtual maturity clock."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence

from dotenv import load_dotenv


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402
from backend.services.advisory_historical_range.artifact_store import (  # noqa: E402
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.composition import (  # noqa: E402
    explicit_historical_range_connection_factory,
)
from backend.services.advisory_historical_range.fullstack_comparison import (  # noqa: E402
    HistoricalComparisonArtifactRefV1,
    HistoricalComparisonArtifactStore,
)
from backend.services.advisory_historical_range.model_challenger import (  # noqa: E402
    HistoricalMetaLabelChallenger,
    HistoricalMetaLabelChallengerArtifactV1,
)
from backend.services.advisory_historical_range.models import (  # noqa: E402
    HistoricalRangeArtifactRefV1,
    HistoricalRangeCandidateArtifactPayloadV2,
)
from backend.services.advisory_historical_range.query_repository import (  # noqa: E402
    PostgresHistoricalRangeQueryRepository,
)
from backend.services.advisory_model_first.historical_forward_replay import (  # noqa: E402
    HistoricalForwardReplayArtifactStore,
    HistoricalForwardReplayDayV1,
    HistoricalForwardEvaluationMarketSource,
    HistoricalForwardReplayPriorityV1,
    HistoricalForwardReplayRankV1,
    HistoricalForwardReplayRequestV1,
    WINDOW_CONSUMED_OR_UNKNOWN,
    WINDOW_UNCONSUMED,
    build_historical_forward_replay,
)
from backend.services.advisory_model_first.model_binding_resolution import (  # noqa: E402
    AdvisoryModelBindingResolver,
)
from backend.services.advisory_model_first.model_inference import (  # noqa: E402
    validate_meta_label_bundle_runtime,
)
from backend.services.advisory_model_first.policy_contracts import (  # noqa: E402
    FrozenAdvisoryPolicyDatasetRequestV1,
)
from backend.services.advisory_model_first.policy_dataset_bundle import (  # noqa: E402
    load_policy_dataset_bundle,
)
from backend.services.advisory_historical_range.wsl_model_scorer import (  # noqa: E402
    WslMetaLabelFeatureMatrixScorer,
    load_deferred_exact_meta_label_runtime_bundle,
)
from backend.services.strategy_package.runtime_variant import (  # noqa: E402
    canonical_json_sha256,
)


DEFAULT_OUTPUT_ROOT = Path(
    "F:/Dev/AIstock_model_artifacts/advisory_p0d_historical_forward_replay"
)


def main() -> None:
    parser = _parser()
    args = parser.parse_args()

    load_dotenv(args.env_file, override=False)
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    replay_store = HistoricalForwardReplayArtifactStore(root=output_root)
    if args.command == "report":
        if not args.artifact_hash:
            parser.error("--artifact-hash is required for report")
        artifact = replay_store.load(artifact_hash=args.artifact_hash)
        print(_render_report(artifact.model_dump(mode="json")))
        return
    _require_args(
        parser,
        args,
        "parent_range_run_id",
        "historical_artifact_root",
        "model_root",
        "program_id",
        "binding_version_id",
    )
    if args.command == "run":
        _require_args(parser, args, "window_usage")
    inputs = _load_inputs(
        parent_range_run_id=args.parent_range_run_id,
        historical_artifact_root=Path(args.historical_artifact_root).resolve(),
        model_root=Path(args.model_root).resolve(),
        program_id=args.program_id,
        binding_version_id=args.binding_version_id,
        decision_start=(
            date.fromisoformat(args.decision_start) if args.decision_start else None
        ),
        decision_end=(
            date.fromisoformat(args.decision_end) if args.decision_end else None
        ),
    )
    if args.command == "validate-inputs":
        print(
            json.dumps(
                {
                    "status": "READY",
                    "parent_range_run_id": args.parent_range_run_id,
                    "context_day_count": len(inputs["parents"]),
                    "decision_day_count": len(inputs["decision_parents"]),
                    "decision_start": inputs["decision_parents"][0][
                        "parent"
                    ].decision_trade_date.isoformat(),
                    "decision_end": inputs["decision_parents"][-1][
                        "parent"
                    ].decision_trade_date.isoformat(),
                    "replay_as_of_trade_date": inputs["targets"][-1].isoformat(),
                    "maturity_horizon_trade_days": inputs[
                        "maturity_horizon_trade_days"
                    ],
                    "model_training_data_cutoff": inputs[
                        "model_training_data_cutoff"
                    ].isoformat(),
                    "bundle_id": inputs["resolution"].bundle_id,
                    "model_descriptor_sha256": inputs["resolution"].descriptor_sha256,
                },
                sort_keys=True,
            )
        )
        return

    artifact = _run(
        inputs=inputs,
        output_root=output_root,
        parent_range_run_id=args.parent_range_run_id,
        model_root=Path(args.model_root).resolve(),
        program_id=args.program_id,
        binding_version_id=args.binding_version_id,
        window_usage=args.window_usage,
    )
    artifact_path = replay_store.publish(artifact)
    report_path = output_root / f"report_{artifact.artifact_hash}.md"
    _write_atomic(report_path, _render_report(artifact.model_dump(mode="json")) + "\n")
    print(
        json.dumps(
            {
                "status": "COMPLETED",
                "artifact_hash": artifact.artifact_hash,
                "artifact_path": str(artifact_path),
                "report_path": str(report_path),
                "evidence_classification": artifact.evidence_classification,
                "metrics": artifact.metrics,
                "baseline_metrics": artifact.baseline_metrics,
                "comparison_metrics": artifact.comparison_metrics,
            },
            sort_keys=True,
        )
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("validate-inputs", "run", "report"))
    parser.add_argument(
        "--env-file",
        default=os.environ.get("AISTOCK_ENV_FILE", str(REPOSITORY_ROOT / ".env")),
    )
    parser.add_argument("--parent-range-run-id")
    parser.add_argument("--historical-artifact-root")
    parser.add_argument("--model-root")
    parser.add_argument("--program-id")
    parser.add_argument("--binding-version-id")
    parser.add_argument(
        "--window-usage",
        choices=(WINDOW_UNCONSUMED, WINDOW_CONSUMED_OR_UNKNOWN),
    )
    parser.add_argument("--decision-start")
    parser.add_argument("--decision-end")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--artifact-hash")
    return parser


def _require_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace, *names: str
) -> None:
    missing = [
        f"--{name.replace('_', '-')}" for name in names if not getattr(args, name)
    ]
    if missing:
        parser.error(
            f"missing required arguments for {args.command}: {', '.join(missing)}"
        )


def _load_inputs(
    *,
    parent_range_run_id: str,
    historical_artifact_root: Path,
    model_root: Path,
    program_id: str,
    binding_version_id: str,
    decision_start: date | None,
    decision_end: date | None,
) -> dict[str, Any]:
    query = PostgresHistoricalRangeQueryRepository(
        conn_factory=explicit_historical_range_connection_factory()
    )
    run = query.get_run(parent_range_run_id)
    if str(run.get("status")) != "COMPLETED":
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_PARENT_NOT_READY")
    page = query.list_days(range_run_id=parent_range_run_id, limit=500)
    days = sorted(
        page["items"], key=lambda item: (int(item["ordinal"]), str(item["day_run_id"]))
    )
    if page["page"]["has_more"] or len(days) < 2:
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_PARENT_DAY_SET_INCOMPLETE")
    store = HistoricalRangeArtifactStore(root=historical_artifact_root)
    loaded: list[dict[str, Any]] = []
    for day in days:
        ref = HistoricalRangeArtifactRefV1.model_validate(day["candidate_artifact_ref"])
        parent = HistoricalRangeCandidateArtifactPayloadV2.model_validate(
            store.load(ref).payload
        )
        projected_trade_date = date.fromisoformat(str(day["decision_trade_date"]))
        if (
            parent.range_run_id != parent_range_run_id
            or parent.decision_trade_date != projected_trade_date
        ):
            raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_PARENT_IDENTITY_MISMATCH")
        loaded.append({"parent": parent, "ref": ref})
    targets = _target_dates([item["parent"].decision_trade_date for item in loaded])

    first = loaded[0]["parent"]
    resolution = AdvisoryModelBindingResolver().resolve(
        model_root=model_root,
        program=SimpleNamespace(program_id=program_id, package_ids=(first.package_id,)),
        active_binding={
            "binding_version_id": binding_version_id,
            "package_ids": [first.package_id],
        },
        selection_run=SimpleNamespace(
            manifest_sha256_by_package={first.package_id: first.manifest_sha256}
        ),
    )
    bundle = load_deferred_exact_meta_label_runtime_bundle(
        model_root=model_root,
        bundle_id=resolution.bundle_id,
        bundle_manifest_sha256=resolution.bundle_manifest_sha256,
    )
    validate_meta_label_bundle_runtime(bundle, resolution=resolution)
    policy_dataset_bundle_id = str(bundle["manifest"]["policy_dataset_bundle_id"])
    policy_dataset_root = model_root / "policy_datasets" / policy_dataset_bundle_id
    policy_manifest = load_policy_dataset_bundle(
        policy_dataset_root,
        expected_bundle_id=policy_dataset_bundle_id,
    )
    policy_request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
        (policy_dataset_root / "request.json").read_text(encoding="utf-8")
    )
    if policy_request.request_sha256 != policy_manifest.get("request_sha256"):
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_POLICY_REQUEST_MISMATCH")
    horizon = int(bundle["shadow_policy_maturity_horizon_days"])
    context, decisions, context_targets = _select_window(
        loaded,
        targets,
        maturity_horizon_trade_days=horizon,
        decision_start=decision_start,
        decision_end=decision_end,
    )
    return {
        "parents": context,
        "decision_parents": decisions,
        "targets": context_targets,
        "resolution": resolution,
        "bundle": bundle,
        "model_training_data_cutoff": date.fromisoformat(policy_request.data_cutoff),
        "maturity_horizon_trade_days": horizon,
    }


def _select_window(
    parents: Sequence[dict[str, Any]],
    targets: Sequence[date],
    *,
    maturity_horizon_trade_days: int,
    decision_start: date | None,
    decision_end: date | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[date]]:
    if len(parents) != len(targets):
        raise ValueError("historical replay parent and target counts differ")
    dates = [item["parent"].decision_trade_date for item in parents]
    start_index = dates.index(decision_start) if decision_start is not None else 0
    context = list(parents[start_index:])
    context_targets = list(targets[start_index:])
    maximum_decision_count = len(context) - maturity_horizon_trade_days
    if maximum_decision_count < 1:
        raise ValueError(
            "historical replay has no mature decision after reserving the context tail"
        )
    if decision_end is None:
        decision_count = maximum_decision_count
    else:
        local_dates = [item["parent"].decision_trade_date for item in context]
        decision_count = local_dates.index(decision_end) + 1
        if decision_count > maximum_decision_count:
            raise ValueError(
                "requested decision end does not leave a complete maturity tail"
            )
    return context, context[:decision_count], context_targets


def _run(
    *,
    inputs: dict[str, Any],
    output_root: Path,
    parent_range_run_id: str,
    model_root: Path,
    program_id: str,
    binding_version_id: str,
    window_usage: str,
):
    implementation_sha256 = _implementation_sha256()
    scoring_implementation_sha256 = _scoring_implementation_sha256()
    resolution = inputs["resolution"]
    base_identity = {
        "schema_version": "advisory_p0d_historical_forward_state_identity_v1",
        "parent_range_run_id": parent_range_run_id,
        "program_id": program_id,
        "binding_version_id": binding_version_id,
        "model_descriptor_sha256": resolution.descriptor_sha256,
        "bundle_id": resolution.bundle_id,
        "decision_dates": [
            item["parent"].decision_trade_date.isoformat()
            for item in inputs["decision_parents"]
        ],
        "context_dates": [
            item["parent"].decision_trade_date.isoformat() for item in inputs["parents"]
        ],
        "scoring_implementation_sha256": scoring_implementation_sha256,
    }
    state_id = canonical_json_sha256(base_identity)
    state_path = output_root / f"state_{state_id}.json"
    state = (
        _read_json(state_path)
        if state_path.exists()
        else {**base_identity, "state_id": state_id, "days": {}}
    )
    if {key: state.get(key) for key in base_identity} != base_identity:
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_EXACT_RETRY_CONFLICT")
    comparison_store = HistoricalComparisonArtifactStore(
        root=output_root / "day-artifacts"
    )
    challenger = HistoricalMetaLabelChallenger(
        bundle_loader=load_deferred_exact_meta_label_runtime_bundle,
        scorer=WslMetaLabelFeatureMatrixScorer(repo_root=REPOSITORY_ROOT),
    )
    scored_by_date: dict[date, HistoricalMetaLabelChallengerArtifactV1] = {}
    target_by_decision = {
        item["parent"].decision_trade_date: target
        for item, target in zip(inputs["parents"], inputs["targets"], strict=True)
    }
    for item in inputs["decision_parents"]:
        parent = item["parent"]
        trade_date = parent.decision_trade_date
        existing = state["days"].get(trade_date.isoformat())
        if existing and existing.get("status") == "COMPLETE":
            ref = HistoricalComparisonArtifactRefV1(**existing["artifact_ref"])
            artifact = comparison_store.load_meta_label_challenger(ref)
            if (
                artifact.parent_candidate_artifact_hash
                != item["ref"].semantic_content_hash
            ):
                raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_EXACT_RETRY_CONFLICT")
            scored_by_date[trade_date] = artifact
            continue
        started = time.monotonic()
        artifact = challenger.score_day(
            parent=parent,
            parent_candidate_artifact_hash=item["ref"].semantic_content_hash,
            target_trade_date=target_by_decision[trade_date],
            model_root=str(model_root),
            program_id=program_id,
            binding_version_id=binding_version_id,
            producer_implementation_sha256=scoring_implementation_sha256,
        )
        ref, stored_artifact = _publish_and_reload_challenger(
            comparison_store, artifact
        )
        state["days"][trade_date.isoformat()] = {
            "status": "COMPLETE",
            "artifact_ref": ref.__dict__,
            "parent_candidate_artifact_hash": item["ref"].semantic_content_hash,
            "duration_seconds": round(time.monotonic() - started, 3),
        }
        _write_atomic(state_path, _json_text(state) + "\n")
        # Use the persisted contract as the canonical cross-process form. This
        # keeps a cold run and an exact retry identical even when a scorer emits
        # numpy or Decimal scalar subclasses before JSON normalization.
        scored_by_date[trade_date] = stored_artifact
        print(
            json.dumps(
                {
                    "trade_date": trade_date.isoformat(),
                    "status": "COMPLETE",
                    "artifact_hash": artifact.artifact_hash,
                    "duration_seconds": state["days"][trade_date.isoformat()][
                        "duration_seconds"
                    ],
                },
                sort_keys=True,
            ),
            flush=True,
        )

    context_days: list[HistoricalForwardReplayDayV1] = []
    all_symbols: set[str] = set()
    for item, target in zip(inputs["parents"], inputs["targets"], strict=True):
        parent = item["parent"]
        rankings = _rankings_from_parent(parent)
        all_symbols.update(value.symbol for value in rankings)
        scored = scored_by_date.get(parent.decision_trade_date)
        priorities = (
            tuple(
                HistoricalForwardReplayPriorityV1(
                    symbol=value.symbol,
                    entry_priority_rank=value.entry_priority_rank,
                    take_probability=value.take_probability,
                    skip_probability=value.skip_probability,
                    advisory_model_confidence=value.advisory_model_confidence,
                )
                for value in scored.candidates
            )
            if scored is not None
            else ()
        )
        context_days.append(
            HistoricalForwardReplayDayV1(
                decision_as_of_trade_date=parent.decision_trade_date,
                target_trade_date=target,
                parent_candidate_artifact_hash=item["ref"].semantic_content_hash,
                rankings=rankings,
                entry_priorities=priorities,
            )
        )
    first_scored = scored_by_date[
        inputs["decision_parents"][0]["parent"].decision_trade_date
    ]
    benchmark = str(first_scored.cost_policy.get("benchmark_instrument") or "")
    market = HistoricalForwardEvaluationMarketSource().load(
        symbols=sorted(all_symbols),
        benchmark_instrument=benchmark,
        start_trade_date=context_days[0].target_trade_date,
        end_trade_date=context_days[-1].target_trade_date,
    )
    request_payload = {
        "request_id": f"advhreplay_{state_id[:24]}",
        "parent_range_run_id": parent_range_run_id,
        "program_id": program_id,
        "package_id": first_scored.package_id,
        "model_descriptor_sha256": first_scored.model_descriptor_sha256,
        "bundle_id": first_scored.bundle_id,
        "bundle_manifest_sha256": first_scored.bundle_manifest_sha256,
        "shadow_policy": first_scored.shadow_policy,
        "shadow_policy_sha256": first_scored.shadow_policy_sha256,
        "cost_policy": first_scored.cost_policy,
        "cost_policy_sha256": first_scored.cost_policy_sha256,
        "model_training_data_cutoff_trade_date": inputs["model_training_data_cutoff"],
        "window_usage": window_usage,
        "replay_as_of_trade_date": context_days[-1].target_trade_date,
        "maturity_horizon_trade_days": inputs["maturity_horizon_trade_days"],
        "market_input_sha256": market.input_sha256,
        "implementation_sha256": implementation_sha256,
        "context_days": tuple(context_days),
    }
    request = HistoricalForwardReplayRequestV1.model_validate(request_payload)
    result = build_historical_forward_replay(request=request, market=market)
    state["status"] = "COMPLETED"
    state["request_sha256"] = request.request_sha256
    state["artifact_hash"] = result.artifact_hash
    _write_atomic(state_path, _json_text(state) + "\n")
    return result


def _publish_and_reload_challenger(
    store: HistoricalComparisonArtifactStore,
    artifact: HistoricalMetaLabelChallengerArtifactV1,
) -> tuple[HistoricalComparisonArtifactRefV1, HistoricalMetaLabelChallengerArtifactV1]:
    ref = store.publish_meta_label_challenger(artifact)
    stored_artifact = store.load_meta_label_challenger(ref)
    if stored_artifact.artifact_hash != artifact.artifact_hash:
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_ARTIFACT_READBACK_MISMATCH")
    return ref, stored_artifact


def _rankings_from_parent(
    parent: HistoricalRangeCandidateArtifactPayloadV2,
) -> tuple[HistoricalForwardReplayRankV1, ...]:
    selected = sorted(
        (
            fact
            for fact in parent.candidates
            if fact.membership_status == "INCLUDED"
            and fact.selection_effective_rank is not None
            and fact.selection_effective_rank <= 40
        ),
        key=lambda fact: (int(fact.selection_effective_rank or 0), fact.symbol),
    )
    if len(selected) != 40 or [
        int(item.selection_effective_rank or 0) for item in selected
    ] != list(range(1, 41)):
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_TOP40_INCOMPLETE")
    return tuple(
        HistoricalForwardReplayRankV1(
            symbol=item.symbol,
            selection_effective_rank=int(item.selection_effective_rank),
            combined_score=float(item.selection_effective_score),
        )
        for item in selected
    )


def _target_dates(decisions: Sequence[date]) -> list[date]:
    if len(decisions) < 2 or sorted(set(decisions)) != list(decisions):
        raise ValueError("historical replay decisions must be unique and ordered")
    targets = list(decisions[1:])
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT MIN(cal_date) FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date > %s""",
                (decisions[-1],),
            )
            row = cur.fetchone()
        conn.rollback()
    if not row or row[0] is None:
        raise RuntimeError("ADVISORY_HISTORICAL_FORWARD_NEXT_TRADE_DATE_UNAVAILABLE")
    targets.append(row[0])
    return targets


def _implementation_sha256() -> str:
    paths = (
        REPOSITORY_ROOT
        / "backend/services/advisory_model_first/historical_forward_replay.py",
        REPOSITORY_ROOT
        / "backend/services/advisory_historical_range/model_challenger.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/model_inference.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/meta_label_bundle.py",
        REPOSITORY_ROOT
        / "backend/services/advisory_model_first/shadow_portfolio_policy.py",
        Path(__file__).resolve(),
    )
    return canonical_json_sha256(
        {
            str(path.relative_to(REPOSITORY_ROOT))
            .replace("\\", "/"): hashlib.sha256(path.read_bytes())
            .hexdigest()
            for path in paths
        }
    )


def _scoring_implementation_sha256() -> str:
    paths = (
        REPOSITORY_ROOT
        / "backend/services/advisory_historical_range/model_challenger.py",
        REPOSITORY_ROOT
        / "backend/services/advisory_historical_range/wsl_model_scorer.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/model_inference.py",
        REPOSITORY_ROOT / "backend/services/advisory_model_first/meta_label_bundle.py",
        REPOSITORY_ROOT / "scripts/wsl/advisory_historical_model_predict.py",
    )
    return canonical_json_sha256(
        {
            str(path.relative_to(REPOSITORY_ROOT))
            .replace("\\", "/"): hashlib.sha256(path.read_bytes())
            .hexdigest()
            for path in paths
        }
    )


def _render_report(payload: dict[str, Any]) -> str:
    metrics = payload["metrics"]
    baseline = payload["baseline_metrics"]
    comparison = payload["comparison_metrics"]
    lines = [
        "# Advisory P0-D 历史虚拟前向结果",
        "",
        f"- 证据分类：`{payload['evidence_classification']}`",
        f"- 分类说明：{payload['evidence_reason']}",
        f"- 冻结训练数据截止：{payload['model_training_data_cutoff_trade_date']}",
        f"- 决策窗口：{payload['decision_start_trade_date']} 至 {payload['decision_end_trade_date']}",
        f"- maturity tail：{payload['maturity_horizon_trade_days']} 个交易日",
        f"- 回放 watermark：{payload['replay_as_of_trade_date']}",
        f"- 决策 observation：{payload['decision_observation_count']}",
        f"- rank-context 天数：{payload['context_day_count']}",
        f"- resolved / unresolved：{payload['resolved_observation_count']} / {payload['unresolved_observation_count']}",
        f"- coverage：{payload['coverage']:.2%}",
        f"- 完成 episode：{metrics.get('exited_episode_count')}",
        f"- episode 胜率：{_percent(metrics.get('completed_episode_hit_rate'))}",
        f"- 平均 episode 净收益：{_bps(metrics.get('mean_completed_episode_net_return_bps'))}",
        f"- 平均日净收益：{_bps(metrics.get('mean_daily_net_return_bps'))}",
        f"- 平均日超额收益：{_bps(metrics.get('mean_daily_net_excess_return_bps'))}",
        f"- 最大回撤：{_percent(metrics.get('maximum_drawdown'))}",
        f"- 平均换手：{_percent(metrics.get('mean_turnover_fraction'))}",
        "",
        "## Selection Top5 同策略基线",
        "",
        f"- 完成 episode：{baseline.get('exited_episode_count')}",
        f"- episode 胜率：{_percent(baseline.get('completed_episode_hit_rate'))}",
        f"- 平均 episode 净收益：{_bps(baseline.get('mean_completed_episode_net_return_bps'))}",
        f"- 累计净收益：{_percent(baseline.get('cumulative_net_return'))}",
        f"- 最大回撤：{_percent(baseline.get('maximum_drawdown'))}",
        "",
        "## P0-D 相对基线",
        "",
        f"- 配对交易日：{comparison.get('paired_day_count')}",
        f"- 平均日净收益 lift：{_bps(comparison.get('mean_daily_net_return_lift_bps'))}",
        f"- episode 胜率 lift：{_percent(comparison.get('completed_episode_hit_rate_lift'))}",
        f"- 平均 episode 净收益 lift：{_bps(comparison.get('mean_completed_episode_net_return_lift_bps'))}",
        f"- 累计净收益 lift：{_percent(comparison.get('cumulative_net_return_lift'))}",
        "",
        "> 本结果来自历史虚拟前向，不是自然 future OOS，不写入生产 forward 事实。",
    ]
    return "\n".join(lines)


def _percent(value: Any) -> str:
    return "N/A" if value is None else f"{float(value):.2%}"


def _bps(value: Any) -> str:
    return "N/A" if value is None else f"{float(value):.2f} bps"


def _json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("historical replay state is not a JSON object")
    return payload


def _write_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
