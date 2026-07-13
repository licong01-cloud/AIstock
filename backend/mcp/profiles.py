"""Profile resolution for the unified AIstock MCP gateway."""

from __future__ import annotations

GATEWAY_MODULES = {
    "catalog",
    "research",
    "research_assistant",
    "local_data",
    "qlib_export",
    "factor_library",
    "factor_metrics",
    "factor_correlation",
    "model_registry",
    "strategy_governance",
    "strategy_packages",
    "execution_policy",
    "selection_center",
    "advisory",
    "paper_v2_monitoring",
    "qmt_broker_monitoring",
    "external_research",
    "stock_analysis",
    "validation",
    "qe_experiment",
    "qe_archive",
}

SCRIPT_BACKED_SERVERS: set[str] = set()

INITIAL_PROFILES: dict[str, list[str]] = {
    "lite": ["catalog"],
    "research": ["research"],
    "research_assistant": ["catalog", "research_assistant", "stock_analysis", "external_research"],
    "assistant": ["catalog", "research_assistant", "stock_analysis", "external_research"],
    "research_with_assistant": ["catalog", "research", "research_assistant"],
    "local_data": ["local_data"],
    "data": ["local_data"],
    "qlib_data": ["qlib_export"],
    "backtest_data": ["qlib_export"],
    "data_full": ["local_data", "qlib_export"],
    "assistant_with_local_data": ["catalog", "research_assistant", "local_data"],
    "research_with_assistant_local_data": ["catalog", "research", "research_assistant", "local_data"],
    "factor_library": ["factor_library"],
    "factor_metrics": ["factor_metrics"],
    "factor_correlation": ["factor_correlation"],
    "factor": ["factor_library", "factor_metrics", "factor_correlation"],
    "model_registry": ["model_registry"],
    "strategy_governance": ["strategy_governance"],
    "strategy_package_ops": ["strategy_packages"],
    "execution_policy": ["execution_policy"],
    "selection_advisory": ["selection_center", "advisory"],
    "paper_v2_monitor": ["paper_v2_monitoring", "qmt_broker_monitoring"],
    "paper_v2_ops": ["strategy_packages", "selection_center", "advisory", "paper_v2_monitoring", "qmt_broker_monitoring"],
    "paper_v2_stable": ["strategy_packages", "selection_center", "advisory", "paper_v2_monitoring", "qmt_broker_monitoring"],
    "external_research": ["external_research"],
    "stock_analysis": ["stock_analysis", "external_research"],
    "assistant_stock_analysis": ["catalog", "research_assistant", "stock_analysis", "external_research"],
    "validation": ["validation"],
    "qe": ["qe_experiment", "qe_archive", "model_registry"],
    "factor_research": ["factor_library", "factor_metrics", "factor_correlation"],
    "strategy_ops": ["strategy_governance", "execution_policy"],
    "trading_ops": ["strategy_governance", "execution_policy"],
    "research_full": [
        "catalog",
        "research",
        "research_assistant",
        "local_data",
        "qlib_export",
        "factor_library",
        "factor_metrics",
        "factor_correlation",
        "model_registry",
        "strategy_governance",
        "strategy_packages",
        "execution_policy",
        "selection_center",
        "advisory",
        "paper_v2_monitoring",
        "qmt_broker_monitoring",
        "external_research",
        "stock_analysis",
    ],
    "full": [
        "catalog",
        "research",
        "research_assistant",
        "local_data",
        "qlib_export",
        "factor_library",
        "factor_metrics",
        "factor_correlation",
        "model_registry",
        "strategy_governance",
        "strategy_packages",
        "execution_policy",
        "selection_center",
        "advisory",
        "paper_v2_monitoring",
        "qmt_broker_monitoring",
        "external_research",
        "stock_analysis",
        "validation",
        "qe_experiment",
        "qe_archive",
    ],
}

# Backward-compatible name used by existing tests; it now means all gateway-backed modules.
_PHASE0_5_MODULES = set(GATEWAY_MODULES)


def parse_modules(value: str | list[str] | tuple[str, ...] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        modules = [item.strip() for item in value.split(",") if item.strip()]
    else:
        modules = [str(item).strip() for item in value if str(item).strip()]
    return modules


def resolve_modules(
    *,
    profile: str | None = "research",
    modules: str | list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    """Resolve gateway-backed modules for a profile or explicit module list."""

    explicit_modules = parse_modules(modules)
    if explicit_modules is not None:
        if profile not in {None, "", "research", "lite"}:
            raise ValueError("Specify either --profile or --modules, not both")
        unknown = [name for name in explicit_modules if name not in GATEWAY_MODULES]
        if unknown:
            raise ValueError(
                f"Modules {unknown!r} are not gateway-backed modules; "
                f"allowed modules: {sorted(GATEWAY_MODULES)!r}. "
                f"Script-backed MCP servers remain registered separately: {sorted(SCRIPT_BACKED_SERVERS)!r}."
            )
        if not explicit_modules:
            raise ValueError("At least one gateway module is required")
        return explicit_modules

    selected = "research" if profile in {None, ""} else profile
    if selected in INITIAL_PROFILES:
        return list(INITIAL_PROFILES[selected])
    raise ValueError(
        f"Unknown MCP profile {selected!r}; allowed profiles: {sorted(INITIAL_PROFILES)!r}. "
        f"Script-backed MCP servers are configured separately: {sorted(SCRIPT_BACKED_SERVERS)!r}."
    )
