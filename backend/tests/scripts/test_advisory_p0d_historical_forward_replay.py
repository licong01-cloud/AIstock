from __future__ import annotations

import importlib.util
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT_PATH = Path("scripts/advisory_p0d_historical_forward_replay.py")
SPEC = importlib.util.spec_from_file_location(
    "advisory_p0d_historical_forward_replay", SCRIPT_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _parents(count: int) -> tuple[list[dict], list[date]]:
    start = date(2026, 5, 15)
    parents = [
        {"parent": SimpleNamespace(decision_trade_date=start + timedelta(days=index))}
        for index in range(count)
    ]
    targets = [start + timedelta(days=index + 1) for index in range(count)]
    return parents, targets


def test_report_parser_does_not_require_source_inputs() -> None:
    args = MODULE._parser().parse_args(
        ["report", "--output-root", "F:/artifacts", "--artifact-hash", "a" * 64]
    )

    assert args.command == "report"
    assert args.parent_range_run_id is None
    assert not hasattr(args, "model_training_candidate_end")


def test_select_window_reserves_complete_maturity_tail() -> None:
    parents, targets = _parents(44)

    context, decisions, selected_targets = MODULE._select_window(
        parents,
        targets,
        maturity_horizon_trade_days=20,
        decision_start=None,
        decision_end=None,
    )

    assert len(context) == 44
    assert len(decisions) == 24
    assert len(selected_targets) == 44
    assert (
        decisions[-1]["parent"].decision_trade_date
        == parents[23]["parent"].decision_trade_date
    )


def test_select_window_rejects_decision_end_inside_maturity_tail() -> None:
    parents, targets = _parents(44)

    with pytest.raises(ValueError, match="complete maturity tail"):
        MODULE._select_window(
            parents,
            targets,
            maturity_horizon_trade_days=20,
            decision_start=None,
            decision_end=parents[24]["parent"].decision_trade_date,
        )


def test_render_report_distinguishes_historical_evidence_and_matched_baseline() -> None:
    report = MODULE._render_report(
        {
            "evidence_classification": "HISTORICAL_OUT_OF_TIME",
            "evidence_reason": "frozen before the replay window",
            "model_training_data_cutoff_trade_date": "2026-03-10",
            "decision_start_trade_date": "2026-05-15",
            "decision_end_trade_date": "2026-06-17",
            "maturity_horizon_trade_days": 20,
            "replay_as_of_trade_date": "2026-07-17",
            "decision_observation_count": 24,
            "context_day_count": 44,
            "resolved_observation_count": 24,
            "unresolved_observation_count": 0,
            "coverage": 1.0,
            "metrics": {
                "exited_episode_count": 30,
                "completed_episode_hit_rate": 0.36,
                "mean_completed_episode_net_return_bps": -352.0,
                "mean_daily_net_return_bps": -69.0,
                "mean_daily_net_excess_return_bps": -72.0,
                "maximum_drawdown": -0.22,
                "mean_turnover_fraction": 0.4,
            },
            "baseline_metrics": {
                "exited_episode_count": 26,
                "completed_episode_hit_rate": 0.27,
                "mean_completed_episode_net_return_bps": -345.0,
                "cumulative_net_return": -0.17,
                "maximum_drawdown": -0.17,
            },
            "comparison_metrics": {
                "paired_day_count": 30,
                "mean_daily_net_return_lift_bps": -10.0,
                "completed_episode_hit_rate_lift": 0.09,
                "mean_completed_episode_net_return_lift_bps": -7.0,
                "cumulative_net_return_lift": -0.025,
            },
        }
    )

    assert "HISTORICAL_OUT_OF_TIME" in report
    assert "Selection Top5 同策略基线" in report
    assert "P0-D 相对基线" in report
    assert "2026-03-10" in report
    assert "2026-05-15 至 2026-06-17" in report
    assert "2026-07-17" in report
    assert "不是自然 future OOS" in report


def test_publish_path_returns_persisted_canonical_artifact() -> None:
    generated = SimpleNamespace(artifact_hash="a" * 64)
    persisted = SimpleNamespace(artifact_hash="a" * 64)
    ref = SimpleNamespace(artifact_hash="a" * 64)

    class _Store:
        def publish_meta_label_challenger(self, artifact):
            assert artifact is generated
            return ref

        def load_meta_label_challenger(self, artifact_ref):
            assert artifact_ref is ref
            return persisted

    returned_ref, returned_artifact = MODULE._publish_and_reload_challenger(
        _Store(), generated
    )

    assert returned_ref is ref
    assert returned_artifact is persisted
