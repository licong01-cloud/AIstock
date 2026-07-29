from __future__ import annotations

import pandas as pd

from backend.services.quantevolver.sector_risk_overlay_evaluation import (
    evaluate_sector_risk_overlay,
)


def _action(date, action_type, *, generated=True):
    return {
        "trade_date": date,
        "instrument": "000001.SZ",
        "action_type": action_type,
        "risk_state": "CRITICAL" if action_type != "REENTRY_BUY" else "NORMAL",
        "order_generated": generated,
        "reason": "fixture",
        "policy_hash": "policy-v1",
    }


def _metric(result, key):
    return next(item for item in result.metrics if item["metric_key"] == key)


def test_overlay_evaluator_aligns_warning_exit_reentry_and_f014_capture() -> None:
    actions = [
        _action("2026-01-05", "DE_RISK_SELL"),
        _action("2026-01-08", "EXIT"),
        _action("2026-01-12", "REENTRY_BUY"),
    ]
    episodes = pd.DataFrame(
        [
            {
                "instrument": "000001.SZ",
                "exit_signal_date": "2026-01-08",
                "post_exit_signal_mae": -0.12,
                "post_exit_mfe": 0.06,
                "false_early_exit": False,
                "episode_capture_ratio": 0.72,
                "extended_capture_ratio": 0.63,
            }
        ]
    )
    report = pd.DataFrame(
        {
            "report_date": pd.bdate_range("2026-01-05", "2026-01-12"),
            "cost": [0.001] * 6,
            "turnover": [0.10] * 6,
        }
    )

    result = evaluate_sector_risk_overlay(
        actions,
        holding_episodes=episodes,
        portfolio_report=report,
    )

    warning = _metric(result, "warning_lead_summary")["value_json"]
    assert warning["lead_days"]["median"] == 3.0
    assert warning["lead_at_least_n_days"]["3"] == 1
    exit_effect = _metric(result, "exit_effect_summary")["value_json"]
    assert exit_effect["avoided_drawdown"]["mean"] == 0.12
    assert exit_effect["episode_capture_ratio"]["mean"] == 0.72
    reentry = _metric(result, "reentry_delay_summary")["value_json"]
    assert reentry["trading_day_delay"]["median"] == 2.0
    assert result.summary["local_limitation_metric_count"] == 0


def test_missing_episode_and_report_families_are_local_not_global() -> None:
    result = evaluate_sector_risk_overlay(
        [_action("2026-01-05", "ENTRY_BLOCK", generated=False)],
        holding_episodes=pd.DataFrame(),
        portfolio_report=None,
    )

    assert _metric(result, "action_summary")["quality_flag"] == "ok"
    assert _metric(result, "warning_lead_summary")["quality_flag"] == "missing_local_evidence"
    assert _metric(result, "cost_turnover_summary")["quality_flag"] == "computed_with_local_limitations"
    assert result.summary["metric_count"] == 5
