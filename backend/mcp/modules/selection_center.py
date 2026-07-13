"""Selection Center MCP wrappers over /api/v1/selection-center."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools

RUN_SELECTION_CONFIRM = "RUN_SELECTION_CENTER_SELECTION"
DELETE_SELECTION_RUN_CONFIRM = "DELETE_SELECTION_CENTER_RUN"
ADD_SELECTION_TO_WATCHLIST_CONFIRM = "ADD_SELECTION_RUN_TO_WATCHLIST"

SPECS = (
    ToolSpec("selection_center_list_selectable_packages", "GET", "/selectable-packages", query_defaults={"limit": 50}, limit_caps={"limit": 200}),
    ToolSpec("selection_center_get_industry_tree", "GET", "/industry-tree"),
    ToolSpec("selection_center_list_runs", "GET", "/runs", query_defaults={"limit": 20, "page": None, "page_size": None}, limit_caps={"limit": 100, "page_size": 100}),
    ToolSpec("selection_center_get_pit_cutoff", "GET", "/pit-cutoff", query_defaults={"trade_date": None, "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE", "cutoff_date": None}),
    ToolSpec("selection_center_get_run", "GET", "/runs/{run_id}", path_params=("run_id",)),
    ToolSpec("selection_center_get_fusion_diagnostics", "GET", "/runs/{run_id}/fusion-diagnostics", path_params=("run_id",)),
    ToolSpec("selection_center_get_aggregate_results", "GET", "/runs/{run_id}/aggregate-results", path_params=("run_id",)),
    ToolSpec("selection_center_get_excluded_results", "GET", "/runs/{run_id}/excluded-results", path_params=("run_id",)),
    ToolSpec("selection_center_list_paper_links", "GET", "/runs/{run_id}/paper-portfolio-links", path_params=("run_id",)),
    ToolSpec("selection_center_advisory_preview", "POST", "/advisory/multi-package-review/preview"),
    ToolSpec("selection_center_advisory_quality_report", "POST", "/advisory/quality-report"),
    ToolSpec("selection_center_run_confirmed", "POST", "/runs", confirm_token=RUN_SELECTION_CONFIRM),
    ToolSpec("selection_center_delete_run_confirmed", "DELETE", "/runs/{run_id}", path_params=("run_id",), confirm_token=DELETE_SELECTION_RUN_CONFIRM),
    ToolSpec("selection_center_bulk_delete_runs_confirmed", "POST", "/runs/bulk-delete", confirm_token=DELETE_SELECTION_RUN_CONFIRM, body_updates={"confirm_delete": True}),
    ToolSpec("selection_center_add_to_watchlist_confirmed", "POST", "/runs/{run_id}/add-to-watchlist", path_params=("run_id",), confirm_token=ADD_SELECTION_TO_WATCHLIST_CONFIRM),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(registry, module_name="selection_center", client_prefix="selection-center", specs=SPECS)
