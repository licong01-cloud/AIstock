"""Tests for compact QE summary payload projection."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from backend.services.quantevolver.payload_summary import (
    compact_config_summary,
    compact_enhanced_metric_summary,
    compact_experiment_row,
    compact_loop_row,
    compact_metric_summary,
    derive_position_summary_from_enhanced_metrics,
)
from backend.services.quantevolver.qe_evolution_agents import EvolutionAgents


def test_compact_config_summary_preserves_strategy_tail_fields() -> None:
    summary = compact_config_summary(
        {
            "model_id": "LGBModel",
            "strategy_id": "TopkDropoutStrategy",
            "strategy_params": {
                "topk": 50,
                "hold_thresh": 10,
                "unfilled_handler": "TAIL_SUBSTITUTE",
                "unfilled_backup_depth": 15,
                "unfilled_handler_params": {"backup_depth": 15},
                "candidate_symbols": ["too-large-to-return"],
            },
            "factor_list": ["alpha_a", "alpha_b"],
        }
    )

    assert summary["strategy_params"]["hold_thresh"] == 10
    assert summary["strategy_params"]["unfilled_handler"] == "TAIL_SUBSTITUTE"
    assert summary["strategy_params"]["unfilled_backup_depth"] == 15
    assert summary["unfilled_handler_params"]["backup_depth"] == 15
    assert "candidate_symbols" not in summary["strategy_params"]
    assert "factor_list" not in summary


def test_compact_metric_summary_drops_large_enhanced_payloads() -> None:
    metrics = {
        "IC": 0.0412,
        "ICIR": 1.23,
        "enhanced_metrics": {
            "rank_ic_series": [0.1, 0.2],
            "return_curves": {"cumulative_excess_no_cost": [1, 2, 3]},
            "stock_trades": {"SH600000": [{"profit": 1.2}]},
            "training_diagnostics": {"train_loss": [0.3, 0.2], "val_loss": [0.4, 0.25]},
        },
    }

    summary = compact_metric_summary(metrics)

    assert summary["ic"] == 0.0412
    assert summary["icir"] == 1.23
    assert summary["train_loss_final"] == 0.2
    assert summary["val_loss_final"] == 0.25
    assert "enhanced_metrics" not in summary
    assert "stock_trades" not in summary
    assert "return_curves" not in summary
    assert "rank_ic_series" not in summary


def test_compact_metric_summary_keeps_enhanced_scalars_only() -> None:
    metrics = {
        "IC": 0.0412,
        "enhanced_metrics": {
            "absolute_returns": {
                "cagr": 0.42,
                "sharpe": 2.1,
                "max_drawdown": -0.13,
                "final_cash": 1000.0,
                "final_stock_value": 9000.0,
                "final_total_value": 10000.0,
            },
            "position_summary": {
                "position_count_avg": 48.5,
                "position_count_max": 55,
            },
            "return_curves": {"dates": ["2026-01-01"], "cumulative_portfolio": [1.1]},
            "stock_trades": {"000001.SZ": [{"date": "2026-01-01", "type": "buy"}]},
        },
    }

    summary = compact_metric_summary(metrics)
    enhanced = summary["enhanced_metrics"]

    assert enhanced["absolute_returns"]["cagr"] == 0.42
    assert enhanced["absolute_returns"]["sharpe"] == 2.1
    assert enhanced["position_summary"]["position_count_avg"] == 48.5
    assert enhanced["position_summary"]["final_cash"] == 1000.0
    assert enhanced["position_summary"]["final_stock_value"] == 9000.0
    assert "return_curves" not in enhanced
    assert "stock_trades" not in enhanced


def test_compact_enhanced_summary_derives_position_counts_from_stock_trades() -> None:
    metrics = {
        "enhanced_metrics": {
            "absolute_returns": {
                "cagr": 0.35,
                "sharpe": 1.9,
                "max_drawdown": -0.11,
                "final_cash": 500.0,
                "final_stock_value": 9500.0,
                "final_total_value": 10000.0,
            },
            "return_curves": {"dates": ["2026-01-01", "2026-01-02", "2026-01-03"]},
            "stock_trades": {
                "000001.SZ": [
                    {"date": "2026-01-01", "type": "buy"},
                    {"date": "2026-01-03", "type": "sell"},
                ],
                "000002.SZ": [{"date": "2026-01-02", "type": "buy"}],
            },
        }
    }

    enhanced = compact_enhanced_metric_summary(metrics)
    position = enhanced["position_summary"]

    assert enhanced["absolute_returns"]["cagr"] == 0.35
    assert position["position_count_min"] == 1
    assert position["position_count_avg"] == 4 / 3
    assert position["position_count_max"] == 2
    assert position["final_stock_count"] == 1
    assert position["final_cash"] == 500.0
    assert position["final_stock_value"] == 9500.0


def test_derive_position_summary_from_enhanced_metrics_is_compact() -> None:
    summary = derive_position_summary_from_enhanced_metrics(
        {
            "absolute_returns": {"final_cash": 100.0, "final_total_value": 1000.0},
            "stock_trades": {
                "AAA": [{"date": "2026-01-01", "side": "buy"}],
                "BBB": [{"date": "2026-01-02", "side": "buy"}],
            },
        }
    )

    assert summary["position_count_max"] == 2
    assert summary["final_cash_ratio"] == 0.1


def test_compact_loop_row_projects_factors_and_scalar_metrics_only() -> None:
    row = {
        "loop_id": "task_1_Loop2",
        "task_id": "task_1",
        "loop_index": 2,
        "action_type": "mutate",
        "is_sota": True,
        "status": "completed",
        "config_json": {"factor_list": ["alpha_a", "alpha_b"], "model_id": "LGB", "label_horizon": 5},
        "metrics_json": {"Rank IC": 0.0389, "enhanced_metrics": {"stock_trades": {"x": []}}},
        "agent_analysis": {"trace": "large"},
    }

    compact = compact_loop_row(row)

    assert compact["loop_index"] == 2
    assert compact["rank_ic"] == 0.0389
    assert compact["factors"] == ["alpha_a", "alpha_b"]
    assert compact["factor_count"] == 2
    assert compact["config_summary"]["model_id"] == "LGB"
    assert compact["config_summary"]["label_horizon"] == 5
    assert "factor_list" not in compact["config_summary"]
    assert "factors" not in compact["config_summary"]
    assert "config_json" not in compact
    assert "metrics_json" not in compact
    assert "agent_analysis" not in compact
    assert "stock_trades" not in compact["metrics_summary"]


def test_compact_loop_row_accepts_sql_projected_summary_fields() -> None:
    row = {
        "loop_id": "task_1_Loop3",
        "task_id": "task_1",
        "loop_index": 3,
        "factor_list": ["alpha_x", "alpha_y"],
        "model_id": "LGB",
        "strategy_id": "topk",
        "ic": "0.031",
        "annualized_return": "0.12",
    }

    compact = compact_loop_row(row)

    assert compact["factors"] == ["alpha_x", "alpha_y"]
    assert compact["factor_count"] == 2
    assert compact["config_summary"]["model_id"] == "LGB"
    assert compact["config_summary"]["strategy_id"] == "topk"
    assert compact["ic"] == 0.031
    assert compact["annualized_return"] == 0.12
    assert "config_json" not in compact
    assert "metrics_json" not in compact


def test_compact_loop_row_sota_summary_is_null_tolerant_without_topk() -> None:
    row = {
        "loop_id": "task_1_Loop4",
        "task_id": "task_1",
        "loop_index": 4,
        "metrics_json": {
            "IC": 0.01,
            "enhanced_metrics": {
                "absolute_returns": {
                    "cagr": 0.60,
                    "max_drawdown": -0.12,
                }
            },
        },
    }

    compact = compact_loop_row(row)
    sota = compact["sota_metric_summary"]

    assert compact["cagr"] == 0.60
    assert compact["max_drawdown"] == -0.12
    assert compact["calmar"] == pytest.approx(5.0)
    assert compact["metrics_summary"]["calmar"] == pytest.approx(5.0)
    assert "topk_return_20" not in compact
    assert "topk_return_20" not in compact["metrics_summary"]
    assert sota["primary"] == {"cagr": 0.60, "max_drawdown": -0.12, "calmar": pytest.approx(5.0)}
    assert sota["topk"] == {}
    assert sota["topk_present"] is False
    assert sota["topk_status"] == "not_present"
    assert sota["topk_policy"] == "present_only_not_zero_fallback"
    assert sota["signal_diagnostics"] == {"ic": 0.01}
    assert sota["signal_policy"] == "diagnostic_only_not_primary"


def test_compact_loop_row_sota_summary_includes_topk_only_when_present() -> None:
    row = {
        "loop_id": "task_1_Loop5",
        "task_id": "task_1",
        "loop_index": 5,
        "metrics_json": {
            "enhanced_metrics": {
                "absolute_returns": {
                    "cagr": 0.72,
                    "max_drawdown": -0.18,
                },
                "prediction_diagnostics": {
                    "topk_quality_status": "ok",
                    "topk_return_20": 0.012,
                    "topk_return_50": 0.007,
                    "topk_hit_rate_20": 0.61,
                    "topk_decay": 0.005,
                    "within_portfolio_rankic": 0.08,
                },
            },
        },
    }

    compact = compact_loop_row(row)
    sota = compact["sota_metric_summary"]

    assert compact["calmar"] == pytest.approx(4.0)
    assert compact["topk_return_20"] == 0.012
    assert compact["topk_hit_rate_20"] == 0.61
    assert compact["topk_decay"] == 0.005
    assert sota["topk_present"] is True
    assert sota["topk_status"] == "ok"
    assert sota["topk"]["topk_return_20"] == 0.012
    assert sota["topk"]["topk_return_50"] == 0.007
    assert sota["topk"]["topk_hit_rate_20"] == 0.61
    assert sota["topk"]["topk_decay"] == 0.005
    assert sota["topk"]["within_portfolio_rankic"] == 0.08


def test_evaluate_sota_prompt_uses_cagr_mdd_topk_null_tolerant_rubric() -> None:
    prompt_source = Path("backend/register_evolution_v2_prompts.py").read_text(encoding="utf-8")

    assert "CAGR/annualized_return" in prompt_source
    assert "MDD/max_drawdown" in prompt_source
    assert "Top-K 缺失、为 null 或未提供时，不报错、不降级、不按 0 处理" in prompt_source
    assert "IC/RankIC/ICIR 仅作信号广度诊断" in prompt_source
    assert "IC 提升 > 0.002" not in prompt_source


def test_run_evaluator_baseline_does_not_require_ic_or_topk() -> None:
    evaluator = EvolutionAgents()

    result = asyncio.run(
        evaluator.run_evaluator(
            {
                "annualized_return": 0.62,
                "max_drawdown": -0.18,
                "IC": None,
                "topk_return_20": None,
            },
            None,
        )
    )

    assert result["is_sota"] is True
    assert result["method"] == "baseline"
    assert "AnnRet/CAGR" in result["reason"]


def test_compact_experiment_row_uses_existing_scalar_columns_without_result_metrics() -> None:
    row = {
        "experiment_id": "qe_1",
        "experiment_name": "demo",
        "status": "completed",
        "factor_names": ["alpha_a"],
        "ic": 0.02,
        "rank_ic": 0.03,
        "result_metrics": None,
    }

    compact = compact_experiment_row(row)

    assert compact["experiment_id"] == "qe_1"
    assert compact["ic"] == 0.02
    assert compact["rank_ic"] == 0.03
    assert compact["metrics_summary"] == {"ic": 0.02, "rank_ic": 0.03}
    assert "result_metrics" not in compact

