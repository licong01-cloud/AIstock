"""Compact QE payload helpers for MCP/API summary views.

The normal QE UI can request full JSONB payloads, but agent-facing MCP tools
need small scalar summaries by default.  These helpers centralize the lossy
projection so routers and services do not each invent slightly different metric
aliases.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

SCALAR_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "ic": ("ic", "IC"),
    "icir": ("icir", "ICIR"),
    "rank_ic": ("rank_ic", "Rank_IC", "Rank IC"),
    "rank_icir": ("rank_icir", "Rank_ICIR", "Rank ICIR"),
    "annualized_return": (
        "annualized_return",
        "excess_return_with_cost_annualized",
        "1day.excess_return_with_cost.annualized_return",
    ),
    "max_drawdown": (
        "max_drawdown",
        "excess_return_with_cost_max_drawdown",
        "1day.excess_return_with_cost.max_drawdown",
    ),
    "information_ratio": (
        "information_ratio",
        "sharpe",
        "excess_return_with_cost_IR",
        "1day.excess_return_with_cost.information_ratio",
    ),
    "annualized_return_no_cost": (
        "annualized_return_no_cost",
        "excess_return_without_cost_annualized",
        "1day.excess_return_without_cost.annualized_return",
    ),
    "max_drawdown_no_cost": (
        "max_drawdown_no_cost",
        "excess_return_without_cost_max_drawdown",
        "1day.excess_return_without_cost.max_drawdown",
    ),
    "information_ratio_no_cost": (
        "information_ratio_no_cost",
        "sharpe_no_cost",
        "excess_return_without_cost_IR",
        "1day.excess_return_without_cost.information_ratio",
    ),
    "daily_win_rate": ("daily_win_rate",),
    "weekly_win_rate": ("weekly_win_rate",),
    "stock_win_rate": ("stock_win_rate",),
    "total_trades": ("total_trades",),
    "winning_trades": ("winning_trades",),
    "losing_trades": ("losing_trades",),
    "train_loss_final": ("train_loss_final", "final_train_loss"),
    "val_loss_final": ("val_loss_final", "final_val_loss", "final_valid_loss"),
    "overfit_ratio": ("overfit_ratio",),
    "best_epoch": ("best_epoch",),
}

SUMMARY_CONFIG_KEYS = (
    "model_id",
    "strategy_id",
    "label_horizon",
    "execution_algo",
    "node_id",
    "backtest_only",
)


def parse_jsonish(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def first_number(*values: Any) -> float | int | None:
    for value in values:
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return None


def _last_number(value: Any) -> float | int | None:
    if isinstance(value, (list, tuple)):
        for item in reversed(value):
            num = first_number(item)
            if num is not None:
                return num
    return first_number(value)


def _mapping(value: Any) -> dict[str, Any]:
    parsed = parse_jsonish(value)
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _containers(metrics: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    if not isinstance(metrics, Mapping):
        return []
    containers: list[Mapping[str, Any]] = [metrics]
    for key in ("summary", "daily_win_stats", "stock_trade_stats", "training_diagnostics"):
        child = metrics.get(key)
        if isinstance(child, Mapping):
            containers.append(child)
    enhanced = metrics.get("enhanced_metrics")
    if isinstance(enhanced, Mapping):
        containers.append(enhanced)
        for key in ("summary", "daily_win_stats", "stock_trade_stats", "training_diagnostics"):
            child = enhanced.get(key)
            if isinstance(child, Mapping):
                containers.append(child)
    return containers


def _first_by_alias(containers: list[Mapping[str, Any]], aliases: tuple[str, ...]) -> Any:
    for container in containers:
        for alias in aliases:
            value = container.get(alias)
            if value is not None:
                return value
    return None


def compact_metric_summary(metrics: Any, *, row: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return scalar metrics only; never include time series or trade rows."""
    parsed = _mapping(metrics)
    containers = _containers(parsed)
    row_map = row if isinstance(row, Mapping) else {}
    summary: dict[str, Any] = {}
    for canonical, aliases in SCALAR_METRIC_ALIASES.items():
        value = first_number(row_map.get(canonical), _first_by_alias(containers, aliases))
        if value is not None:
            summary[canonical] = value

    for container in containers:
        training = container.get("training_diagnostics")
        if isinstance(training, Mapping):
            train_final = _last_number(training.get("train_loss") or training.get("train_losses"))
            val_final = _last_number(training.get("val_loss") or training.get("valid_loss") or training.get("val_losses"))
            if train_final is not None and "train_loss_final" not in summary:
                summary["train_loss_final"] = train_final
            if val_final is not None and "val_loss_final" not in summary:
                summary["val_loss_final"] = val_final
            break
    return summary


def factors_from_config(config: Any) -> list[Any]:
    cfg = _mapping(config)
    factors = cfg.get("factor_list") or cfg.get("factor_names") or cfg.get("factors") or []
    if isinstance(factors, str):
        parsed = parse_jsonish(factors)
        factors = parsed if isinstance(parsed, list) else [factors]
    return list(factors) if isinstance(factors, list) else []


def compact_config_summary(config: Any) -> dict[str, Any]:
    cfg = _mapping(config)
    summary = {key: cfg[key] for key in SUMMARY_CONFIG_KEYS if key in cfg and cfg[key] not in (None, [], {})}
    model_params = cfg.get("model_params")
    if isinstance(model_params, Mapping):
        for key in ("label_horizon", "random_seed", "execution_algo"):
            if key in model_params and key not in summary:
                summary[key] = model_params[key]
    runtime_flags = cfg.get("runtime_flags")
    if isinstance(runtime_flags, Mapping):
        for key in ("random_seed", "seed"):
            if key in runtime_flags and key not in summary:
                summary[key] = runtime_flags[key]
    return summary


def compact_experiment_row(row: Mapping[str, Any], *, include_config_summary: bool = False) -> dict[str, Any]:
    base_keys = (
        "experiment_id",
        "experiment_name",
        "status",
        "model_id",
        "strategy_id",
        "qe_task_id",
        "qe_loop_id",
        "loop_index",
        "parent_experiment_id",
        "is_evolution_loop",
        "alpha_mode",
        "parent_multi_alpha_id",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
    )
    item = {key: row.get(key) for key in base_keys if key in row}
    if "factor_count" in row:
        item["factor_count"] = row["factor_count"]
    else:
        factor_names = row.get("factor_names")
        item["factor_count"] = len(factor_names) if isinstance(factor_names, (list, tuple)) else 0
    metrics = compact_metric_summary(row.get("result_metrics"), row=row)
    item.update(metrics)
    if metrics:
        item["metrics_summary"] = metrics
    if include_config_summary:
        config_summary = compact_config_summary(row.get("custom_params"))
        if config_summary:
            item["custom_params_summary"] = config_summary
    return item


def compact_loop_row(row: Mapping[str, Any]) -> dict[str, Any]:
    base_keys = (
        "loop_id",
        "task_id",
        "loop_index",
        "action_type",
        "is_sota",
        "status",
        "node_id",
        "experiment_id",
        "created_at",
        "updated_at",
    )
    item = {key: row.get(key) for key in base_keys if key in row}
    config_source = row.get("config_json") if row.get("config_json") is not None else row
    config_summary = compact_config_summary(config_source)
    metrics = compact_metric_summary(row.get("metrics_json"), row=row)
    factors = factors_from_config(config_source)
    item.update(metrics)
    if factors:
        item["factors"] = factors
        item["factor_count"] = len(factors)
    if config_summary:
        item["config_summary"] = config_summary
    item["metrics_summary"] = metrics
    return item


def compact_task_row(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "task_id",
        "task_name",
        "target_desc",
        "max_loops",
        "current_loop",
        "status",
        "base_experiment_id",
        "task_type",
        "source_type",
        "node_id",
        "label_horizon",
        "strategy_id",
        "execution_algo",
        "strategy_evo_execution_mode",
        "created_at",
        "updated_at",
    )
    item = {key: row.get(key) for key in keys if key in row}
    item["hmm_enabled"] = _has_hmm_marker(row.get("strategy_params")) or _has_hmm_marker(row.get("strategy_evo_config"))
    return item


def _has_hmm_marker(value: Any) -> bool:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return "hmm" in value.lower()
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key).lower()
            if key_text in {"enable_sector_hmm", "hmm_enabled", "enable_hmm", "use_hmm"}:
                if _truthy_hmm_value(nested):
                    return True
                continue
            if _has_hmm_marker(nested):
                return True
            if "hmm" in key_text and nested not in (None, "", False):
                return True
    if isinstance(value, list):
        return any(_has_hmm_marker(item) for item in value)
    return False


def _truthy_hmm_value(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)
