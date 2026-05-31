"""Tests for compact QE summary payload projection."""

from __future__ import annotations

from backend.services.quantevolver.payload_summary import (
    compact_enhanced_metric_summary,
    compact_experiment_row,
    compact_loop_row,
    compact_metric_summary,
    derive_position_summary_from_enhanced_metrics,
)


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

