"""Compact QE payload helpers for MCP/API summary views.

The normal QE UI can request full JSONB payloads, but agent-facing MCP tools
need small scalar summaries by default.  These helpers centralize the lossy
projection so routers and services do not each invent slightly different metric
aliases.
"""

from __future__ import annotations

import json
import math
from typing import Any, Mapping

from .qe_active_dataset_profile import is_pure_star50_universe
from .qe_run_registry import qe_registration_summary

SCALAR_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "ic": ("ic", "IC"),
    "icir": ("icir", "ICIR"),
    "rank_ic": ("rank_ic", "Rank_IC", "Rank IC"),
    "rank_icir": ("rank_icir", "Rank_ICIR", "Rank ICIR"),
    "cagr": (
        "cagr",
        "CAGR",
        "cagr_absolute",
        "annualized_return_absolute",
        "absolute_returns.cagr",
        "enhanced_metrics.absolute_returns.cagr",
    ),
    "sharpe": (
        "sharpe",
        "sharpe_absolute",
        "absolute_returns.sharpe",
        "enhanced_metrics.absolute_returns.sharpe",
    ),
    "annualized_return": (
        "annualized_return",
        "annualized_return_with_cost",
        "excess_return_with_cost_annualized",
        "1day.excess_return_with_cost.annualized_return",
    ),
    "max_drawdown": (
        "max_drawdown",
        "max_drawdown_absolute",
        "excess_return_with_cost_max_drawdown",
        "1day.excess_return_with_cost.max_drawdown",
        "absolute_returns.max_drawdown",
        "enhanced_metrics.absolute_returns.max_drawdown",
    ),
    "information_ratio": (
        "information_ratio",
        "excess_return_with_cost_IR",
        "1day.excess_return_with_cost.information_ratio",
    ),
    "benchmark_annualized_return": (
        "benchmark_annualized_return",
        "benchmark_annual_return",
        "benchmark_return_annualized",
        "enhanced_metrics.benchmark_returns.annualized_return",
    ),
    "calmar": (
        "calmar",
        "calmar_ratio",
        "calmar_absolute",
        "absolute_returns.calmar",
        "absolute_returns.calmar_ratio",
        "enhanced_metrics.absolute_returns.calmar",
        "enhanced_metrics.absolute_returns.calmar_ratio",
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
    "topk_return_20": (
        "topk_return_20",
        "topk_return@20",
        "topk_return_at_20",
        "prediction_diagnostics.topk_return_20",
        "enhanced_metrics.prediction_diagnostics.topk_return_20",
    ),
    "topk_return_50": (
        "topk_return_50",
        "topk_return@50",
        "topk_return_at_50",
        "prediction_diagnostics.topk_return_50",
        "enhanced_metrics.prediction_diagnostics.topk_return_50",
    ),
    "topk_hit_rate_20": (
        "topk_hit_rate_20",
        "topk_hit_rate@20",
        "topk_hit_rate_at_20",
        "prediction_diagnostics.topk_hit_rate_20",
        "enhanced_metrics.prediction_diagnostics.topk_hit_rate_20",
    ),
    "topk_hit_rate_50": (
        "topk_hit_rate_50",
        "topk_hit_rate@50",
        "topk_hit_rate_at_50",
        "prediction_diagnostics.topk_hit_rate_50",
        "enhanced_metrics.prediction_diagnostics.topk_hit_rate_50",
    ),
    "topk_decay": (
        "topk_decay",
        "topk_return_decay",
        "prediction_diagnostics.topk_decay",
        "enhanced_metrics.prediction_diagnostics.topk_decay",
    ),
    "within_portfolio_rankic": (
        "within_portfolio_rankic",
        "within_rankic",
        "prediction_diagnostics.within_portfolio_rankic",
        "enhanced_metrics.prediction_diagnostics.within_portfolio_rankic",
    ),
    "topk_dispersion_20": (
        "topk_dispersion_20",
        "topk_dispersion@20",
        "prediction_diagnostics.topk_dispersion_20",
        "enhanced_metrics.prediction_diagnostics.topk_dispersion_20",
    ),
    "topk_dispersion_50": (
        "topk_dispersion_50",
        "topk_dispersion@50",
        "prediction_diagnostics.topk_dispersion_50",
        "enhanced_metrics.prediction_diagnostics.topk_dispersion_50",
    ),
}

PRIMARY_SOTA_METRIC_KEYS = ("cagr", "annualized_return", "max_drawdown", "calmar")
TOPK_SOTA_METRIC_KEYS = ("topk_return_20", "topk_hit_rate_20", "topk_decay")
TOPK_QUALITY_METRIC_KEYS = (
    "topk_return_20",
    "topk_return_50",
    "topk_hit_rate_20",
    "topk_hit_rate_50",
    "topk_decay",
    "within_portfolio_rankic",
    "topk_dispersion_20",
    "topk_dispersion_50",
)
SIGNAL_DIAGNOSTIC_METRIC_KEYS = ("ic", "rank_ic", "icir", "rank_icir")
TOPK_STATUS_ALIASES = (
    "topk_quality_status",
    "prediction_diagnostics.topk_quality_status",
    "enhanced_metrics.prediction_diagnostics.topk_quality_status",
)

SUMMARY_CONFIG_KEYS = (
    "model_id",
    "strategy_id",
    "label_horizon",
    "execution_algo",
    "node_id",
    "backtest_only",
)

COMPACT_STRATEGY_CONFIG_KEYS = (
    "topk",
    "n_drop",
    "hold_thresh",
    "risk_degree",
    "initial_cash",
    "label_horizon",
    "unfilled_handler",
    "unfilled_backup_depth",
    "unfilled_trigger_minute",
    "filter_suspended_on_signal",
    "suspend_filter_strict",
    "stock_pool",
    "sector_blacklist_enabled",
    "blacklist_enabled",
)

COMPACT_UNFILLED_PARAM_KEYS = (
    "backup_depth",
    "unfilled_backup_depth",
    "unfilled_trigger_minute",
)

COMPACT_EXECUTION_PARAM_KEYS = (
    "unfilled_handler",
    "unfilled_backup_depth",
    "unfilled_trigger_minute",
    "max_single_order_value",
    "max_weight",
    "max_position_ratio",
)

COMPACT_ABSOLUTE_RETURN_KEYS: dict[str, tuple[str, ...]] = {
    "cagr": ("cagr", "cagr_absolute", "annualized_return_absolute"),
    "sharpe": ("sharpe", "sharpe_absolute"),
    "max_drawdown": ("max_drawdown", "max_drawdown_absolute"),
    "calmar": ("calmar", "calmar_ratio", "calmar_absolute"),
    "total_return": ("total_return", "absolute_total_return"),
    "annualized_volatility": ("annualized_volatility", "volatility"),
    "avg_cash_ratio": ("avg_cash_ratio", "average_cash_ratio"),
    "initial_capital": ("initial_capital", "initial_cash"),
    "final_cash": ("final_cash", "final_cash_amount", "ending_cash", "end_cash"),
    "final_stock_value": (
        "final_stock_value",
        "final_stock_market_value",
        "ending_stock_market_value",
        "end_stock_market_value",
    ),
    "final_total_value": ("final_total_value", "final_account_value", "final_total_account", "final_account"),
    "final_stock_count": ("final_stock_count", "final_position_count", "end_position_count"),
    "n_trading_days": ("n_trading_days", "trading_days"),
}

COMPACT_TRADE_DIAGNOSTIC_KEYS: dict[str, tuple[str, ...]] = {
    "avg_turnover": ("avg_turnover", "average_turnover"),
    "annualized_turnover": ("annualized_turnover",),
    "daily_trade_count_avg": ("daily_trade_count_avg", "avg_daily_trade_count"),
    "total_turnover": ("total_turnover",),
}

COMPACT_POSITION_KEYS: dict[str, tuple[str, ...]] = {
    "position_count_min": ("position_count_min", "min_position_count", "holding_count_min", "min_holding_count"),
    "position_count_avg": (
        "position_count_avg",
        "avg_position_count",
        "holding_count_avg",
        "avg_holding_count",
        "average_holding_count",
    ),
    "position_count_max": ("position_count_max", "max_position_count", "holding_count_max", "max_holding_count"),
    "position_count_p95": ("position_count_p95", "p95_position_count", "holding_count_p95", "p95_holding_count"),
    "final_stock_count": ("final_stock_count", "final_position_count", "end_position_count"),
    "final_cash": ("final_cash", "final_cash_amount", "ending_cash", "end_cash"),
    "final_stock_value": (
        "final_stock_value",
        "final_stock_market_value",
        "ending_stock_market_value",
        "end_stock_market_value",
    ),
    "final_total_value": ("final_total_value", "final_account_value", "final_total_account", "final_account"),
    "final_cash_ratio": ("final_cash_ratio",),
}


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


def _first_number_from_sources(sources: list[Mapping[str, Any]], aliases: tuple[str, ...]) -> float | int | None:
    return first_number(_first_by_alias(sources, aliases))


def _compact_numeric_fields(
    sources: list[Mapping[str, Any]],
    aliases_by_key: Mapping[str, tuple[str, ...]],
) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, aliases in aliases_by_key.items():
        value = _first_number_from_sources(sources, aliases)
        if value is not None:
            compact[key] = value
    return compact


def _compact_scalar_fields(sources: list[Mapping[str, Any]], keys: tuple[str, ...]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in keys:
            if key in compact:
                continue
            value = source.get(key)
            if value is None or value == "":
                continue
            if isinstance(value, (str, int, float, bool)):
                compact[key] = value
    return compact


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
            if "." in alias and alias not in container:
                current: Any = container
                for part in alias.split("."):
                    if not isinstance(current, Mapping) or part not in current:
                        current = None
                        break
                    current = current[part]
                if current is not None:
                    return current
            value = container.get(alias)
            if value is not None:
                return value
    return None


def _apply_calmar_if_available(summary: dict[str, Any]) -> None:
    """Derive Calmar only from present return/MDD values; missing inputs stay absent."""
    if summary.get("calmar") is not None:
        return
    annual_return = first_number(summary.get("cagr"), summary.get("annualized_return"))
    max_drawdown = first_number(summary.get("max_drawdown"))
    if annual_return is None or max_drawdown is None:
        return
    drawdown_abs = abs(max_drawdown)
    if drawdown_abs == 0:
        return
    summary["calmar"] = annual_return / drawdown_abs


def _topk_status(metrics: Any, summary: Mapping[str, Any]) -> str:
    containers = _containers(_mapping(metrics))
    status = _first_by_alias(containers, TOPK_STATUS_ALIASES)
    if isinstance(status, (str, int, float, bool)) and str(status).strip():
        return str(status)
    return "present" if any(summary.get(key) is not None for key in TOPK_QUALITY_METRIC_KEYS) else "not_present"


def compact_sota_metric_summary(metrics: Mapping[str, Any], *, raw_metrics: Any = None) -> dict[str, Any]:
    primary = {key: metrics[key] for key in PRIMARY_SOTA_METRIC_KEYS if metrics.get(key) is not None}
    topk = {key: metrics[key] for key in TOPK_QUALITY_METRIC_KEYS if metrics.get(key) is not None}
    diagnostics = {key: metrics[key] for key in SIGNAL_DIAGNOSTIC_METRIC_KEYS if metrics.get(key) is not None}
    topk_present = any(topk.get(key) is not None for key in TOPK_SOTA_METRIC_KEYS)
    return {
        "primary": primary,
        "topk": topk,
        "topk_present": topk_present,
        "topk_status": _topk_status(raw_metrics, metrics),
        "topk_policy": "present_only_not_zero_fallback",
        "signal_diagnostics": diagnostics,
        "signal_policy": "diagnostic_only_not_primary",
    }


def _trade_action(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text in {"buy", "b", "open", "long", "entry"} or "buy" in text:
        return "buy"
    if text in {"sell", "s", "close", "exit"} or "sell" in text:
        return "sell"
    return None


def _curve_dates(enhanced: Mapping[str, Any]) -> list[str]:
    curves = enhanced.get("return_curves")
    if not isinstance(curves, Mapping):
        return []
    dates = curves.get("dates")
    if not isinstance(dates, list):
        return []
    return [str(item)[:10] for item in dates if item not in (None, "")]


def _derive_position_counts_from_stock_trades(enhanced: Mapping[str, Any]) -> dict[str, Any]:
    stock_trades = enhanced.get("stock_trades")
    if not isinstance(stock_trades, Mapping):
        return {}

    events: dict[str, list[tuple[str, str]]] = {}
    for symbol, trades in stock_trades.items():
        if not isinstance(trades, list):
            continue
        symbol_text = str(symbol)
        for trade in trades:
            if not isinstance(trade, Mapping):
                continue
            trade_date = trade.get("date") or trade.get("datetime") or trade.get("trade_date")
            action = _trade_action(trade.get("type") or trade.get("side") or trade.get("action"))
            if not trade_date or not action:
                continue
            events.setdefault(str(trade_date)[:10], []).append((action, symbol_text))
    if not events:
        return {}

    ordered_dates = _curve_dates(enhanced) or sorted(events)
    active: set[str] = set()
    counts: list[int] = []
    for date in sorted(set(ordered_dates).union(events)):
        for action, symbol in events.get(date, []):
            if action == "buy":
                active.add(symbol)
            elif action == "sell":
                active.discard(symbol)
        if date in ordered_dates or date in events:
            counts.append(len(active))

    if not counts:
        return {}
    sorted_counts = sorted(counts)
    p95_index = min(len(sorted_counts) - 1, max(0, math.ceil(len(sorted_counts) * 0.95) - 1))
    return {
        "position_count_min": min(counts),
        "position_count_avg": sum(counts) / len(counts),
        "position_count_max": max(counts),
        "position_count_p95": sorted_counts[p95_index],
        "final_stock_count": counts[-1],
    }


def derive_position_summary_from_enhanced_metrics(enhanced_metrics: Any) -> dict[str, Any]:
    """Build a compact position summary without returning per-stock trade payloads."""
    enhanced = _mapping(enhanced_metrics)
    if not enhanced:
        return {}

    absolute = _mapping(enhanced.get("absolute_returns"))
    existing_sources = [
        _mapping(enhanced.get("position_summary")),
        _mapping(enhanced.get("holding_audit")),
        _mapping(enhanced.get("position_diagnostics")),
        absolute,
        enhanced,
    ]
    summary = _compact_numeric_fields(existing_sources, COMPACT_POSITION_KEYS)

    if summary.get("position_count_avg") is None or summary.get("position_count_max") is None:
        derived = _derive_position_counts_from_stock_trades(enhanced)
        for key, value in derived.items():
            summary.setdefault(key, value)

    final_total = summary.get("final_total_value")
    final_cash = summary.get("final_cash")
    if summary.get("final_cash_ratio") is None and final_total not in (None, 0) and final_cash is not None:
        summary["final_cash_ratio"] = final_cash / final_total

    return summary


def compact_enhanced_metric_summary(metrics: Any) -> dict[str, Any]:
    """Return only enhanced scalar summaries needed by UI comparison views."""
    parsed = _mapping(metrics)
    enhanced = _mapping(parsed.get("enhanced_metrics"))
    if not enhanced:
        return {}

    summary_sources = [_mapping(enhanced.get("summary")), _mapping(parsed.get("summary"))]
    absolute_sources = [_mapping(enhanced.get("absolute_returns")), *summary_sources]
    trade_sources = [_mapping(enhanced.get("trade_diagnostics")), _mapping(parsed.get("trade_diagnostics"))]

    compact: dict[str, Any] = {}
    absolute = _compact_numeric_fields(absolute_sources, COMPACT_ABSOLUTE_RETURN_KEYS)
    if absolute:
        compact["absolute_returns"] = absolute

    position_summary = derive_position_summary_from_enhanced_metrics(enhanced)
    if position_summary:
        compact["position_summary"] = position_summary

    trade = _compact_numeric_fields(trade_sources, COMPACT_TRADE_DIAGNOSTIC_KEYS)
    if trade:
        compact["trade_diagnostics"] = trade

    return compact


def compact_metric_summary(metrics: Any, *, row: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return scalar metrics only; never include time series or trade rows."""
    parsed = _mapping(metrics)
    containers = _containers(parsed)
    row_map = row if isinstance(row, Mapping) else {}
    summary: dict[str, Any] = {}
    enhanced_summary = compact_enhanced_metric_summary(parsed)
    absolute = _mapping(enhanced_summary.get("absolute_returns"))
    if not absolute and row_map.get("absolute_metrics_present") is True:
        absolute = {
            key: row_map.get(key)
            for key in ("cagr", "sharpe", "max_drawdown", "calmar")
            if row_map.get(key) is not None
        }

    # New QE runs persist one authoritative absolute NAV summary.  When it is
    # present, never fill a missing absolute field from Qlib's benchmark-relative
    # columns; doing so previously produced mixed-source Calmar and mislabeled IR
    # as Sharpe.
    if absolute:
        for key in ("cagr", "sharpe", "max_drawdown", "calmar"):
            value = first_number(absolute.get(key))
            if value is not None:
                summary[key] = value
        _apply_calmar_if_available(summary)

    for canonical, aliases in SCALAR_METRIC_ALIASES.items():
        if absolute and canonical in {"cagr", "sharpe", "max_drawdown", "calmar"}:
            continue
        value = first_number(row_map.get(canonical), _first_by_alias(containers, aliases))
        if value is not None:
            summary[canonical] = value
    if not absolute:
        _apply_calmar_if_available(summary)

    absolute_missing = [
        key
        for key in ("cagr", "sharpe", "max_drawdown", "calmar")
        if summary.get(key) is None
    ]
    summary["metric_contract"] = {
        "absolute_source": (
            "enhanced_metrics.absolute_returns"
            if absolute
            else "legacy_summary_unverified"
        ),
        "active_source": (
            "benchmark_relative_with_cost"
            if summary.get("annualized_return") is not None
            or summary.get("information_ratio") is not None
            else "not_available"
        ),
        "information_ratio_is_sharpe": False,
        "absolute_missing": absolute_missing,
    }

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
    if enhanced_summary:
        summary["enhanced_metrics"] = enhanced_summary
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
    model_params = _mapping(cfg.get("model_params"))
    strategy_params = _mapping(cfg.get("strategy_params"))
    custom_params = _mapping(cfg.get("custom_params"))
    execution_algo_params = _mapping(cfg.get("execution_algo_params"))
    unfilled_handler_params = _mapping(cfg.get("unfilled_handler_params"))

    if isinstance(model_params, Mapping):
        for key in ("label_horizon", "random_seed", "execution_algo"):
            if key in model_params and key not in summary:
                summary[key] = model_params[key]
    runtime_flags = _mapping(cfg.get("runtime_flags"))
    if isinstance(runtime_flags, Mapping):
        for key in ("random_seed", "seed"):
            if key in runtime_flags and key not in summary:
                summary[key] = runtime_flags[key]
    config_sources = [cfg, strategy_params, model_params, custom_params]
    for key in ("hold_thresh", "unfilled_handler", "unfilled_backup_depth", "unfilled_trigger_minute", "stock_pool"):
        if key not in summary:
            compact = _compact_scalar_fields(config_sources, (key,))
            if key in compact:
                summary[key] = compact[key]

    strategy_summary = _compact_scalar_fields(
        [strategy_params, model_params, custom_params, cfg],
        COMPACT_STRATEGY_CONFIG_KEYS,
    )
    if strategy_summary:
        summary["strategy_params"] = strategy_summary

    nested_unfilled_params = _mapping(strategy_params.get("unfilled_handler_params"))
    unfilled_summary = _compact_scalar_fields(
        [unfilled_handler_params, nested_unfilled_params, strategy_params, model_params, custom_params, execution_algo_params],
        COMPACT_UNFILLED_PARAM_KEYS,
    )
    if unfilled_summary:
        summary["unfilled_handler_params"] = unfilled_summary

    nested_execution_params = _mapping(strategy_params.get("execution_algo_params"))
    execution_summary = _compact_scalar_fields(
        [execution_algo_params, nested_execution_params, strategy_params, model_params, custom_params],
        COMPACT_EXECUTION_PARAM_KEYS,
    )
    if execution_summary:
        summary["execution_algo_params"] = execution_summary
    metadata_sources = [cfg, model_params, custom_params]
    active_dataset = next(
        (
            candidate
            for source in metadata_sources
            if (candidate := _mapping(source.get("_qe_active_dataset_summary")))
        ),
        {},
    )
    direct_binding = next(
        (
            candidate
            for source in metadata_sources
            if (candidate := _mapping(source.get("_qe_direct_v2_dataset_binding")))
        ),
        {},
    )
    selection = _mapping(direct_binding.get("selection_pins")) or _mapping(
        cfg.get("universe_selection")
    )
    if active_dataset:
        summary["dataset"] = {
            key: active_dataset[key]
            for key in ("generation", "release_id", "cutoff", "defaults")
            if key in active_dataset
        }
    if selection:
        summary["universe"] = {
            "mode": selection.get("mode") or "stock_universe",
            "pool_ids": list(selection.get("pool_ids") or []),
            "label": selection.get("instrument_name") or selection.get("stock_pool"),
        }
    strategy_topk = first_number(strategy_summary.get("topk"))
    if is_pure_star50_universe(
        universe_selection=selection or None,
        stock_pool=summary.get("stock_pool") or cfg.get("stock_pool"),
    ):
        protocol_eligible = strategy_topk == 20
        summary.setdefault("universe", {})["star50_top20_eligible"] = protocol_eligible
        if strategy_topk is not None and not protocol_eligible:
            summary["universe"]["protocol_status"] = "historical_star50_non_top20"
    return summary


def _explicit_bool(sources: list[Mapping[str, Any]], aliases: tuple[str, ...]) -> bool | None:
    value = _first_by_alias(sources, aliases)
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return None


def _policy_effect(*, enabled: bool | None, count: float | int | None) -> tuple[bool | None, str]:
    if enabled is False:
        return False, "disabled"
    if enabled is None:
        return None, "enablement_not_recorded"
    if count is None:
        return None, "effect_evidence_not_recorded"
    return count > 0, "action_observed" if count > 0 else "enabled_no_action"


def compact_policy_summary(config: Any, metrics: Any) -> dict[str, Any]:
    """Project requested/enabled/effective policy truth without inferring use."""

    cfg = _mapping(config)
    config_sources = [
        cfg,
        _mapping(cfg.get("custom_params")),
        _mapping(cfg.get("strategy_params")),
        _mapping(cfg.get("model_params")),
        _mapping(cfg.get("runtime_profile")),
    ]
    metric_sources = _containers(_mapping(metrics))

    hmm_enabled = _explicit_bool(
        config_sources,
        ("enable_sector_hmm", "hmm_enabled", "hmm.enabled"),
    )
    hmm_trigger_count = _first_number_from_sources(
        metric_sources,
        (
            "hmm_trigger_count",
            "hmm_action_count",
            "sector_hmm_trigger_count",
            "policy_diagnostics.hmm_trigger_count",
            "enhanced_metrics.policy_diagnostics.hmm_trigger_count",
        ),
    )
    hmm_effective, hmm_reason = _policy_effect(
        enabled=hmm_enabled,
        count=hmm_trigger_count,
    )

    blacklist_enabled = _explicit_bool(
        config_sources,
        (
            "sector_blacklist_enabled",
            "blacklist_enabled",
            "sector_blacklist.enabled",
        ),
    )
    if blacklist_enabled is None:
        blacklist = _first_by_alias(config_sources, ("sector_blacklist",))
        if isinstance(blacklist, (list, tuple, set)):
            blacklist_enabled = bool(blacklist)
    blacklist_action_count = _first_number_from_sources(
        metric_sources,
        (
            "blacklist_excluded_count",
            "sector_blacklist_excluded_count",
            "blacklist_match_count",
            "policy_diagnostics.blacklist_excluded_count",
            "enhanced_metrics.policy_diagnostics.blacklist_excluded_count",
        ),
    )
    blacklist_effective, blacklist_reason = _policy_effect(
        enabled=blacklist_enabled,
        count=blacklist_action_count,
    )

    return {
        "hmm": {
            "requested": hmm_enabled,
            "enabled": hmm_enabled,
            "effective": hmm_effective,
            "trigger_count": hmm_trigger_count,
            "effective_reason": hmm_reason,
        },
        "sector_blacklist": {
            "requested": blacklist_enabled,
            "enabled": blacklist_enabled,
            "effective": blacklist_effective,
            "action_count": blacklist_action_count,
            "effective_reason": blacklist_reason,
        },
    }


def compact_experiment_row(row: Mapping[str, Any], *, include_config_summary: bool = False) -> dict[str, Any]:
    base_keys = (
        "experiment_id",
        "experiment_name",
        "status",
        "canonical_status",
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
    custom_params = _mapping(row.get("custom_params"))
    registration = qe_registration_summary(custom_params.get("_qe_run_registration"))
    if registration:
        item["registration_summary"] = registration
    artifact_retention = _mapping(custom_params.get("_qe_artifact_retention"))
    if artifact_retention:
        item["artifact_retention"] = artifact_retention
    if row.get("progress_summary"):
        item["progress_summary"] = row.get("progress_summary")
    if include_config_summary:
        config_summary = compact_config_summary(custom_params)
        if config_summary:
            item["custom_params_summary"] = config_summary
    status = str(row.get("status") or "").lower()
    startable = (
        status in {"created", "pending"}
        and not row.get("is_evolution_loop")
        and not row.get("qe_task_id")
        and not row.get("qe_loop_id")
        and not row.get("started_at")
        and not row.get("completed_at")
    )
    if startable:
        item["start_reason"] = "single experiment has not been submitted"
    else:
        item["start_reason"] = f"not startable: status={row.get('status') or 'unknown'}"
    item["startable"] = startable
    item["editable"] = startable and (row.get("alpha_mode") or "single") == "single"
    item["resume_allowed"] = False
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
    raw_metrics = row.get("metrics_json") if row.get("metrics_json") is not None else row
    metrics = compact_metric_summary(row.get("metrics_json"), row=row)
    factors = factors_from_config(config_source)
    item.update(metrics)
    item["sota_metric_summary"] = compact_sota_metric_summary(metrics, raw_metrics=raw_metrics)
    if factors:
        item["factors"] = factors
        item["factor_count"] = len(factors)
    if config_summary:
        item["config_summary"] = config_summary
    item["policy_summary"] = compact_policy_summary(config_source, raw_metrics)
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
        "long_trend_profile_id",
        "strategy_id",
        "execution_algo",
        "strategy_evo_execution_mode",
        "editable",
        "startable",
        "resume_allowed",
        "start_reason",
        "submitted_loop_count",
        "created_at",
        "updated_at",
    )
    item = {key: row.get(key) for key in keys if key in row}
    hmm_values = _collect_explicit_hmm_values(
        [row.get("strategy_params"), row.get("strategy_evo_config")]
    )
    if not hmm_values:
        item["hmm_enabled"] = None
        item["hmm_status"] = "unknown"
    elif all(hmm_values):
        item["hmm_enabled"] = True
        item["hmm_status"] = "enabled"
    elif not any(hmm_values):
        item["hmm_enabled"] = False
        item["hmm_status"] = "disabled"
    else:
        item["hmm_enabled"] = None
        item["hmm_status"] = "mixed"
    return item


def _collect_explicit_hmm_values(values: list[Any]) -> list[bool]:
    collected: list[bool] = []
    for value in values:
        _collect_explicit_hmm_value(value, collected)
    return collected


def _collect_explicit_hmm_value(value: Any, collected: list[bool]) -> None:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = str(key).lower()
            if key_text in {"enable_sector_hmm", "hmm_enabled", "enable_hmm", "use_hmm"}:
                parsed = _explicit_bool([{"value": nested}], ("value",))
                if parsed is not None:
                    collected.append(parsed)
                continue
            _collect_explicit_hmm_value(nested, collected)
    if isinstance(value, list):
        for item in value:
            _collect_explicit_hmm_value(item, collected)
