"""Read-only successor LocalSIM and Simulation Runtime monitoring tools."""

from __future__ import annotations

from ._gateway_specs import ToolSpec, register_spec_tools


SPECS = (
    ToolSpec("simulation_runtime_monitoring_cutover_readiness", "GET", "/localsim/cutover-readiness"),
    ToolSpec(
        "simulation_runtime_monitoring_list_accounts",
        "GET",
        "/localsim/accounts",
        query_defaults={"package_id": None, "status": None, "cursor": None, "limit": 20},
        limit_caps={"limit": 200},
    ),
    ToolSpec(
        "simulation_runtime_monitoring_get_account",
        "GET",
        "/localsim/accounts/{account_id}",
        path_params=("account_id",),
    ),
    ToolSpec(
        "simulation_runtime_monitoring_list_releases",
        "GET",
        "/localsim/accounts/{account_id}/releases",
        path_params=("account_id",),
        query_defaults={"limit": 20},
        limit_caps={"limit": 200},
    ),
    ToolSpec(
        "simulation_runtime_monitoring_list_bindings",
        "GET",
        "/localsim/accounts/{account_id}/bindings",
        path_params=("account_id",),
        query_defaults={"limit": 20},
        limit_caps={"limit": 200},
    ),
    ToolSpec(
        "simulation_runtime_monitoring_list_runs",
        "GET",
        "/localsim/accounts/{account_id}/runs",
        path_params=("account_id",),
        query_defaults={"trade_date_from": None, "trade_date_to": None, "status": None, "limit": 50},
        limit_caps={"limit": 500},
    ),
    ToolSpec(
        "simulation_runtime_monitoring_get_ledger",
        "GET",
        "/localsim/accounts/{account_id}/ledger",
        path_params=("account_id",),
    ),
    ToolSpec(
        "simulation_runtime_monitoring_get_performance",
        "GET",
        "/localsim/accounts/{account_id}/performance",
        path_params=("account_id",),
    ),
    ToolSpec(
        "simulation_runtime_monitoring_list_replays",
        "GET",
        "/localsim/replays",
        query_defaults={"simulation_account_id": None, "status": None, "cursor": None, "limit": 20},
        limit_caps={"limit": 200},
    ),
    ToolSpec(
        "simulation_runtime_monitoring_get_replay",
        "GET",
        "/localsim/replays/{replay_job_id}",
        path_params=("replay_job_id",),
    ),
    ToolSpec("simulation_runtime_monitoring_scheduler_status", "GET", "/scheduler/status"),
    ToolSpec("simulation_runtime_monitoring_scheduler_verification", "GET", "/scheduler/verification-status"),
)

TOOL_NAMES = tuple(spec.name for spec in SPECS)
TOOL_COUNT = len(TOOL_NAMES)


def register(registry) -> None:
    register_spec_tools(
        registry,
        module_name="simulation_runtime_monitoring",
        client_prefix="simulation-runtime",
        specs=SPECS,
    )
