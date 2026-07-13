"""Read-only Paper Trading v2 monitoring MCP wrappers."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools

SPECS = (
    ToolSpec("paper_v2_monitoring_get_trading_day_defaults", "GET", "/trading-days/defaults", query_defaults={"lookback_trading_days": 10, "as_of_date": None, "require_minute_data": True}),
    ToolSpec("paper_v2_monitoring_list_portfolios", "GET", "/portfolios", query_defaults={"limit": 20, "page": None, "page_size": None, "status": None, "search": None, "sort_by": "created_at", "sort_dir": "desc"}, limit_caps={"limit": 100, "page_size": 100}),
    ToolSpec("paper_v2_monitoring_running_summary", "GET", "/running-summary", query_defaults={"limit": 20, "page": 1, "page_size": None, "snapshot_limit": 30, "position_limit": 8, "status": None, "search": None, "sort_by": "latest_run_time", "sort_dir": "desc"}, limit_caps={"limit": 500, "page_size": 50, "snapshot_limit": 240, "position_limit": 100}),
    ToolSpec("paper_v2_monitoring_get_portfolio", "GET", "/portfolios/{portfolio_id}", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_get_auto_run_status", "GET", "/portfolios/{portfolio_id}/auto-run/status", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_check_day_readiness", "POST", "/portfolios/{portfolio_id}/readiness", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_list_execution_policies", "GET", "/portfolios/{portfolio_id}/execution-policies", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_list_execution_policy_activations", "GET", "/portfolios/{portfolio_id}/execution-policy-activations", path_params=("portfolio_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_list_runtime_profiles", "GET", "/portfolios/{portfolio_id}/runtime-profiles", path_params=("portfolio_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_list_runtime_profile_versions", "GET", "/portfolios/{portfolio_id}/runtime-profiles/{profile_id}/versions", path_params=("portfolio_id", "profile_id"), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_list_runtime_config_activations", "GET", "/portfolios/{portfolio_id}/runtime-config-activations", path_params=("portfolio_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_list_config_change_audit", "GET", "/portfolios/{portfolio_id}/config-change-audit", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 200}),
    ToolSpec("paper_v2_monitoring_list_live_approvals", "GET", "/portfolios/{portfolio_id}/live-approvals", path_params=("portfolio_id",), query_defaults={"package_id": None, "limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_list_sessions", "GET", "/portfolios/{portfolio_id}/sessions", path_params=("portfolio_id",), query_defaults={"limit": 20}, limit_caps={"limit": 100}),
    ToolSpec("paper_v2_monitoring_get_live_dashboard", "GET", "/portfolios/{portfolio_id}/live-dashboard", path_params=("portfolio_id",), query_defaults={"trade_date": None, "event_limit": 50}, limit_caps={"event_limit": 500}),
    ToolSpec("paper_v2_monitoring_list_intraday_snapshots", "GET", "/portfolios/{portfolio_id}/intraday-snapshots", path_params=("portfolio_id",), query_defaults={"trade_date": None, "limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_get_minute_execution", "GET", "/portfolios/{portfolio_id}/minute-execution", path_params=("portfolio_id",), query_defaults={"trade_date": None, "symbol": None, "limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_get_execution_quality", "GET", "/portfolios/{portfolio_id}/execution-quality", path_params=("portfolio_id",), query_defaults={"trade_date": None, "run_id": None, "limit": 20, "scan_limit": 500}, limit_caps={"limit": 200, "scan_limit": 2000}),
    ToolSpec("paper_v2_monitoring_get_session_capabilities", "GET", "/portfolios/{portfolio_id}/session-capabilities", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_get_session", "GET", "/sessions/{session_id}", path_params=("session_id",)),
    ToolSpec("paper_v2_monitoring_get_session_progress", "GET", "/sessions/{session_id}/progress", path_params=("session_id",), query_defaults={"event_limit": 50}, limit_caps={"event_limit": 100}),
    ToolSpec("paper_v2_monitoring_get_scheduler_status", "GET", "/session-scheduler/status"),
    ToolSpec("paper_v2_monitoring_get_scheduler_bootstrap_status", "GET", "/session-scheduler/bootstrap-status"),
    ToolSpec("paper_v2_monitoring_list_orders", "GET", "/portfolios/{portfolio_id}/orders", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_fills", "GET", "/portfolios/{portfolio_id}/fills", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_cash_ledger", "GET", "/portfolios/{portfolio_id}/cash-ledger", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_positions", "GET", "/portfolios/{portfolio_id}/positions", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_daily_snapshots", "GET", "/portfolios/{portfolio_id}/daily-snapshots", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_get_performance_report", "GET", "/portfolios/{portfolio_id}/performance-report", path_params=("portfolio_id",)),
    ToolSpec("paper_v2_monitoring_list_runs", "GET", "/portfolios/{portfolio_id}/runs", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_run_events", "GET", "/portfolios/{portfolio_id}/run-events", path_params=("portfolio_id",), query_defaults={"run_id": None, "limit": 50}, limit_caps={"limit": 500}),
    ToolSpec("paper_v2_monitoring_list_errors", "GET", "/portfolios/{portfolio_id}/errors", path_params=("portfolio_id",), query_defaults={"limit": 50}, limit_caps={"limit": 500}),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(registry, module_name="paper_v2_monitoring", client_prefix="paper-v2", specs=SPECS)
