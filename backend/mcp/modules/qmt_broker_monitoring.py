"""Read-only MiniQMT broker monitoring MCP wrappers."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools

SPECS = (
    ToolSpec("qmt_broker_monitoring_get_status", "GET", "/status"),
    ToolSpec("qmt_broker_monitoring_get_account", "GET", "/account"),
    ToolSpec("qmt_broker_monitoring_list_positions", "GET", "/positions"),
    ToolSpec("qmt_broker_monitoring_get_snapshot", "GET", "/snapshot"),
    ToolSpec("qmt_broker_monitoring_list_orders", "GET", "/orders", query_defaults={"cancelable_only": False}),
    ToolSpec("qmt_broker_monitoring_list_trades", "GET", "/trades"),
    ToolSpec("qmt_broker_monitoring_get_monitor_config", "GET", "/monitor/config"),
    ToolSpec("qmt_broker_monitoring_get_monitor_summary", "GET", "/monitor/summary"),
    ToolSpec("qmt_broker_monitoring_list_monitor_strategies", "GET", "/monitor/strategies"),
    ToolSpec("qmt_broker_monitoring_get_monitor_strategy_summary", "GET", "/monitor/strategy/{strategy_id}/summary", path_params=("strategy_id",)),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(registry, module_name="qmt_broker_monitoring", client_prefix="qmt", specs=SPECS)
