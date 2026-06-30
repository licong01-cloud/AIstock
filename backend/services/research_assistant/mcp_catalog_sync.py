"""Research Assistant MCP catalog synchronization source.

The Research Assistant adapter derives its MCP catalog from the unified gateway
manifest. The database tables remain a runtime cache/overlay for status and
schema details, but tool identity and risk metadata come from TOOL_MANIFEST.
"""

from __future__ import annotations

import json
import re
from hashlib import sha1
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from backend.mcp.profiles import resolve_modules
from backend.mcp.tool_manifest import TOOL_MANIFEST, TOOL_MANIFEST_BY_NAME, ToolManifestEntry
from backend.services.research_assistant.domain_ontology import McpDomain, all_domain_specs

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_CONFIG_PATH = REPO_ROOT / ".mcp.json"


@dataclass(frozen=True)
class GatewayServerCatalog:
    server_key_to_profile: dict[str, str]
    server_key_to_modules: dict[str, tuple[str, ...]]
    module_to_server_key: dict[str, str]
    server_order: tuple[str, ...]
    legacy_aliases: dict[str, str]


SERVER_DISPLAY_METADATA: dict[str, dict[str, Any]] = {
    "aistock-gateway-lite": {
        "title": "AIstock Gateway Lite MCP",
        "display_name_zh": "Gateway 轻量目录",
        "business_aliases_zh": ["MCP目录", "工具检索", "预检"],
        "domain": "mcp_capability",
        "summary_zh": "Unified gateway catalog, profile, health and preflight tools",
    },
    "research-assistant": {
        "title": "Research Assistant MCP",
        "display_name_zh": "智能助理",
        "business_aliases_zh": ["研究助理", "助手工具目录", "MCP能力目录"],
        "domain": "mcp_capability",
        "summary_zh": "Assistant task, prompt, memory, tool catalog and preflight orchestration",
    },
    "aistock-research": {
        "title": "Research Pipeline MCP",
        "display_name_zh": "研究流水线",
        "business_aliases_zh": ["研究管线", "实验流水线", "Research Pipeline"],
        "domain": "research_pipeline",
        "summary_zh": "Research pipeline experiments, stages, artifact refs and backtest records",
    },
    "aistock-local-data": {
        "title": "Local Data Management MCP",
        "display_name_zh": "本地数据管理",
        "business_aliases_zh": ["本地数据", "数据同步", "数据集健康"],
        "domain": "local_data",
        "summary_zh": "Local market-data readiness, sync, schedules, jobs and repair plans",
    },
    "aistock-qlib-data": {
        "title": "Qlib Backtest Data MCP",
        "display_name_zh": "Qlib Backtest Data",
        "business_aliases_zh": ["Qlib data", "backtest dataset", "dataset export", "H5/Bin data"],
        "domain": "qlib_data",
        "summary_zh": "Qlib H5/Bin dataset snapshots, quality checks, previews and candidate export plans",
    },
    "aistock-validation": {
        "title": "Validation / Issue MCP",
        "display_name_zh": "验证与Issue",
        "business_aliases_zh": ["问题单", "BUG流程", "验证中心"],
        "domain": "validation_issue",
        "summary_zh": "Validation Center, BUG JSON and GitHub issue list/search/create/sync",
    },
    "aistock-qe": {
        "title": "QE Gateway MCP",
        "display_name_zh": "QE实验与数仓",
        "business_aliases_zh": ["量化实验", "QE数仓", "模型库", "自定义进化"],
        "domain": "qe_gateway",
        "summary_zh": "QE experiment, QE archive warehouse, model registry and controlled run management",
    },
    "aistock-factor": {
        "title": "Factor Gateway MCP",
        "display_name_zh": "因子能力",
        "business_aliases_zh": ["因子库", "因子独立指标", "因子相关性"],
        "domain": "factor_gateway",
        "summary_zh": "Factor catalog, metrics and correlation analysis profiles",
    },
    "aistock-trading-ops": {
        "title": "Trading Ops Gateway MCP",
        "display_name_zh": "策略与执行治理",
        "business_aliases_zh": ["策略库", "执行策略", "Paper readiness"],
        "domain": "trading_ops",
        "summary_zh": "StrategyPackage governance and execution policy library",
    },
    "aistock-paper-v2-monitor": {
        "title": "Paper v2 Monitor MCP",
        "display_name_zh": "模拟盘监控",
        "business_aliases_zh": ["Paper v2监控", "MiniQMT监控", "持仓交易监控", "盈亏监控"],
        "domain": "paper_v2_monitoring",
        "summary_zh": "Read-only Paper Trading v2 and MiniQMT status, positions, trades, PnL and runtime monitoring",
    },
    "aistock-paper-v2-stable": {
        "title": "Paper v2 Stable MCP",
        "display_name_zh": "模拟盘稳定能力",
        "business_aliases_zh": ["策略包管理", "选股中心", "荐股中心", "模拟盘稳定域"],
        "domain": "paper_v2_stable",
        "summary_zh": "Stable StrategyPackage, Selection Center, Advisory and read-only Paper v2 monitoring capabilities",
    },
    "aistock-external-research": {
        "title": "External Research MCP",
        "display_name_zh": "External Research",
        "business_aliases_zh": ["external search", "web search", "paper search", "academic search"],
        "domain": "external_research",
        "summary_zh": "Evidence-first external web, paper and fetch/extract retrieval",
    },
    "aistock-stock-analysis": {
        "title": "Stock Analysis MCP",
        "display_name_zh": "个股分析",
        "business_aliases_zh": ["个股证据卡", "行情", "财务", "资金流", "技术指标"],
        "domain": "stock_analysis",
        "summary_zh": "Read-only individual stock evidence cards from deterministic market data and external research",
    },
}

LEGACY_SERVER_ALIASES: dict[str, str] = {
    "aistock-qe-experiment": "aistock-qe",
    "aistock-qe-archive": "aistock-qe",
    "aistock-model-registry": "aistock-qe",
    "aistock-factor-library": "aistock-factor",
    "aistock-factor-metrics": "aistock-factor",
    "aistock-factor-correlation": "aistock-factor",
    "aistock-strategy-governance": "aistock-trading-ops",
    "aistock-execution-policy": "aistock-trading-ops",
}

REQUIRED_INPUTS_BY_TOOL: dict[str, list[str]] = {
    "assistant_create_task": ["title"],
    "assistant_create_memory_candidate": ["memory_type", "subject_key", "title"],
    "qe_template_create": ["template_kind", "title", "config_json"],
    "qe_template_validate": ["template_id"],
    "qe_template_materialize_confirmed": ["template_id", "confirm_template"],
    "qe_template_run_confirmed": ["template_id", "confirm_run"],
    "qe_template_create_and_run_confirmed": ["template_kind", "title", "config_json", "confirm_direct_run"],
    "local_data_get_dataset_status": ["dataset"],
    "local_data_apply_repair_confirmed": ["plan_id", "confirmation_text"],
    "stock_analysis_get_quote": ["symbol"],
    "stock_analysis_get_kline": ["symbol"],
    "stock_analysis_get_financials": ["symbol"],
    "stock_analysis_get_quarterly": ["symbol"],
    "stock_analysis_get_margin_financing": ["symbol"],
    "stock_analysis_get_fund_flow": ["symbol"],
    "stock_analysis_get_technicals": ["symbol"],
}

PREFLIGHT_CHECKS_BY_TOOL: dict[str, list[str]] = {
    "qe_template_create": ["schema", "fixed_seed", "draft_only"],
    "qe_template_validate": ["template_exists", "schema", "diff_summary"],
    "qe_template_materialize_confirmed": ["stock_pool", "node_health", "cost", "approval"],
    "qe_template_run_confirmed": ["materialized_template", "cost_guard", "node_health", "approval"],
    "qe_template_create_and_run_confirmed": ["schema", "fixed_seed", "approval", "cost_guard", "node_health"],
    "local_data_apply_repair_confirmed": ["confirmation_text", "plan_id", "facade", "approval"],
}

CONFIRMATIONS_BY_TOOL = {
    "local_data_apply_repair_confirmed": ["APPROVE_RESEARCH_ASSISTANT_ACTION"],
    "qe_template_create": ["CONFIRM_QE_DRAFT"],
    "qe_template_validate": ["CONFIRM_QE_VALIDATE"],
    "qe_template_materialize_confirmed": ["CONFIRM_QE_MATERIALIZE", "MATERIALIZE_QE_TEMPLATE"],
    "qe_template_run_confirmed": ["CONFIRM_QE_RUN", "QE_EXPERIMENT_RUN"],
    "qe_template_create_and_run_confirmed": ["QE_TEMPLATE_CREATE_AND_RUN"],
    "factor_library_register_confirmed": ["REGISTER_FACTOR"],
    "factor_library_deprecate_confirmed": ["DEPRECATE_FACTOR"],
    "factor_metrics_submit_confirmed": ["SUBMIT_FACTOR_METRICS"],
    "factor_corr_submit_confirmed": ["SUBMIT_FACTOR_CORRELATION"],
    "model_registry_register_confirmed": ["REGISTER_MODEL"],
    "model_registry_deprecate_confirmed": ["DEPRECATE_MODEL"],
    "strategy_governance_promote_confirmed": ["PROMOTE_STRATEGY"],
    "strategy_governance_retire_confirmed": ["RETIRE_STRATEGY"],
    "execution_policy_bind_confirmed": ["BIND_EXECUTION_POLICY"],
    "execution_policy_retire_confirmed": ["RETIRE_EXECUTION_POLICY"],
}


def _load_mcp_config(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"MCP config not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    servers = data.get("mcpServers")
    if not isinstance(servers, dict) or not servers:
        raise ValueError(f"MCP config {path} must contain non-empty mcpServers")
    return data


def _arg_value(args: list[str], prefix: str) -> str | None:
    for index, arg in enumerate(args):
        if arg.startswith(prefix + "="):
            return arg.split("=", 1)[1]
        if arg == prefix and index + 1 < len(args):
            return args[index + 1]
    return None


def _manifest_modules_for_profile(profile: str | None) -> tuple[str, ...]:
    selected = "research" if profile in {None, ""} else str(profile)
    modules: dict[str, None] = {}
    for entry in TOOL_MANIFEST:
        if selected in entry.profile_tags:
            modules.setdefault(entry.module, None)
    return tuple(modules)


def _resolve_gateway_modules_for_profile(profile: str | None, modules_arg: str | None) -> tuple[str, ...] | None:
    try:
        return tuple(resolve_modules(profile=profile, modules=modules_arg))
    except ValueError:
        if modules_arg is not None:
            raise
        modules = _manifest_modules_for_profile(profile)
        return modules or None


def derive_gateway_server_catalog(config_path: Path | None = None) -> GatewayServerCatalog:
    path = MCP_CONFIG_PATH if config_path is None else config_path
    config = _load_mcp_config(path)
    server_key_to_profile: dict[str, str] = {}
    server_key_to_modules: dict[str, tuple[str, ...]] = {}
    server_order: list[str] = []
    candidates: dict[str, list[str]] = {}

    for server_key, spec in config["mcpServers"].items():
        args = [str(item) for item in (spec.get("args") or [])]
        if not any(arg.replace("\\", "/").endswith("scripts/aistock_mcp_gateway.py") for arg in args):
            continue
        profile = _arg_value(args, "--profile") or "research"
        modules_arg = _arg_value(args, "--modules")
        modules = _resolve_gateway_modules_for_profile(profile, modules_arg)
        if modules is None:
            continue
        if not modules:
            raise ValueError(f"Gateway server {server_key} resolved no modules")
        server_key_to_profile[str(server_key)] = profile
        server_key_to_modules[str(server_key)] = modules
        server_order.append(str(server_key))
        for module in modules:
            candidates.setdefault(module, []).append(str(server_key))

    if not server_key_to_modules:
        raise ValueError(f"MCP config {path} does not register scripts/aistock_mcp_gateway.py")

    module_to_server_key: dict[str, str] = {}
    for module in sorted({entry.module for entry in TOOL_MANIFEST}):
        module_candidates = candidates.get(module) or []
        if not module_candidates:
            raise ValueError(f"No .mcp.json gateway server exposes module {module!r}")
        single_module = [key for key in module_candidates if server_key_to_modules[key] == (module,)]
        selected = single_module[0] if single_module else module_candidates[0]
        module_to_server_key[module] = selected

    aliases = {legacy: canonical for legacy, canonical in LEGACY_SERVER_ALIASES.items() if canonical in server_key_to_modules}
    return GatewayServerCatalog(
        server_key_to_profile=server_key_to_profile,
        server_key_to_modules=server_key_to_modules,
        module_to_server_key=module_to_server_key,
        server_order=tuple(server_order),
        legacy_aliases=aliases,
    )


def gateway_catalog() -> GatewayServerCatalog:
    return derive_gateway_server_catalog()


def canonicalize_server_key(server_key: str, catalog: GatewayServerCatalog | None = None) -> str:
    selected = gateway_catalog() if catalog is None else catalog
    key = str(server_key or "").strip()
    if not key:
        raise KeyError("MCP server_key is required")
    if key in selected.server_key_to_modules:
        return key
    if key in selected.legacy_aliases:
        return selected.legacy_aliases[key]
    raise KeyError(f"unknown MCP server_key: {server_key}")


def server_key_for_module(module: str, catalog: GatewayServerCatalog | None = None) -> str:
    selected = gateway_catalog() if catalog is None else catalog
    key = selected.module_to_server_key.get(module)
    if not key:
        raise KeyError(f"no canonical server_key for MCP module: {module}")
    return key


def profile_for_server_key(server_key: str, catalog: GatewayServerCatalog | None = None) -> str:
    selected = gateway_catalog() if catalog is None else catalog
    canonical = canonicalize_server_key(server_key, selected)
    return selected.server_key_to_profile[canonical]


def _server_def(server_key: str, catalog: GatewayServerCatalog | None = None) -> dict[str, Any]:
    selected = gateway_catalog() if catalog is None else catalog
    canonical = canonicalize_server_key(server_key, selected)
    metadata = SERVER_DISPLAY_METADATA.get(canonical, {})
    modules = selected.server_key_to_modules[canonical]
    return {
        "server_key": canonical,
        "title": metadata.get("title") or canonical,
        "domain": metadata.get("domain") or modules[0],
        "modules": modules,
        "profile": selected.server_key_to_profile[canonical],
        "display_name_zh": metadata.get("display_name_zh") or canonical,
        "business_aliases_zh": list(metadata.get("business_aliases_zh") or []),
        "summary_zh": metadata.get("summary_zh") or "Unified gateway MCP server",
        "legacy_aliases": sorted(alias for alias, target in selected.legacy_aliases.items() if target == canonical),
    }


def enrich_mcp_server_record(server: dict[str, Any]) -> dict[str, Any]:
    """Overlay canonical display metadata on runtime server rows."""

    item = dict(server)
    selected = gateway_catalog()
    server_key = str(item.get("server_key") or "")
    try:
        canonical = _server_def(server_key, selected)
    except KeyError:
        canonical = {"server_key": server_key, "title": server_key, "domain": "unknown", "modules": (), "profile": "unknown", "summary_zh": ""}
    health = dict(item.get("health_json") if isinstance(item.get("health_json"), dict) else {})
    for key in ("display_name_zh", "business_aliases_zh", "summary_zh", "domain", "modules", "profile", "legacy_aliases"):
        if not health.get(key) and canonical.get(key):
            health[key] = canonical[key]
    if "module" not in health and len(canonical.get("modules") or ()) == 1:
        health["module"] = list(canonical["modules"])[0]
    if "summary_first_payload" not in health:
        health["summary_first_payload"] = True
    item["server_key"] = canonical.get("server_key") or server_key
    item.setdefault("title", canonical.get("title") or server_key)
    item["health_json"] = health
    return item


def _ra_risk_for(entry: ToolManifestEntry) -> str:
    if entry.risk_level in {"read_only", "catalog"}:
        return "low"
    if entry.risk_level == "write_confirmed":
        return "production_sensitive"
    if entry.risk_level in {"long_running", "production_adjacent", "external_network"}:
        return "high"
    raise ValueError(f"unsupported manifest risk_level for {entry.tool_name}: {entry.risk_level}")


def _ra_side_effect_for(entry: ToolManifestEntry) -> str:
    if entry.risk_level in {"read_only", "catalog"}:
        return "read_only"
    if entry.risk_level == "long_running":
        return "high_cost_compute"
    if entry.risk_level == "write_confirmed":
        return "production_sensitive"
    if entry.tool_name == "external_research_save_evidence":
        return "draft_only"
    if entry.risk_level == "external_network":
        return "draft_only"
    if entry.risk_level == "production_adjacent":
        return "write_nonprod"
    raise ValueError(f"unsupported manifest risk_level for {entry.tool_name}: {entry.risk_level}")


RA_TOOL_METADATA_OVERRIDES: dict[str, dict[str, Any]] = {}


def _required_confirmations_for(entry: ToolManifestEntry) -> list[str]:
    if entry.assistant_usable == "direct_or_catalog" and entry.risk_level in {"read_only", "catalog"}:
        return []
    return list(CONFIRMATIONS_BY_TOOL.get(entry.tool_name, []))


def _input_schema_for(tool_name: str) -> dict[str, Any]:
    required_inputs = list(REQUIRED_INPUTS_BY_TOOL.get(tool_name, []))
    properties: dict[str, Any] = {
        "request": {"type": "string", "description": "Original user request for audit context."},
        "query": {"type": "string", "description": "Search or natural-language query."},
        "q": {"type": "string", "description": "Short search query alias."},
        "search": {"type": "string", "description": "Catalog or text search filter."},
        "locale": {"type": "string", "description": "Locale such as zh-CN or en-US."},
        "provider": {"type": "string", "description": "Optional provider selector."},
        "url": {"type": "string", "description": "URL for fetch/extract tools."},
        "max_chars": {"type": "integer", "minimum": 1, "maximum": 12000},
        "status": {"type": "string", "description": "Business status filter, for example running/completed/created/failed."},
        "state": {"type": "string", "description": "State filter alias when a tool uses state instead of status."},
        "symbol": {"type": "string", "description": "Stock symbol or ts_code, for example 000688.SZ."},
        "ts_code": {"type": "string", "description": "Tushare stock code."},
        "stock_code": {"type": "string", "description": "Stock code alias."},
        "dataset": {"type": "string", "description": "Local-data dataset key."},
        "analysis_date": {"type": "string", "description": "Analysis date in YYYY-MM-DD format."},
        "trade_date": {"type": "string", "description": "Trading date in YYYY-MM-DD or YYYYMMDD format."},
        "report_period": {"type": "string", "description": "Financial report period."},
        "period": {"type": "string", "description": "Lookback period such as 1m, 3m, 1y."},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        "offset": {"type": "integer", "minimum": 0, "maximum": 10000},
        "order_by": {"type": "string", "description": "Sort metric such as cagr, calmar, updated_at, created_at."},
        "active_only": {"type": "boolean", "description": "Return only active/running records when supported."},
        "model_type": {"type": "string"},
        "experiment_id": {"type": "string"},
        "task_id": {"type": "string"},
        "qe_task_id": {"type": "string"},
        "qe_loop_id": {"type": "string"},
        "loop_id": {"type": "string"},
        "loop_index": {"type": "integer"},
        "run_id": {"type": "string"},
        "factor_name": {"type": "string"},
        "model_id": {"type": "string"},
        "package_id": {"type": "string"},
        "algo_code": {"type": "string"},
        "method": {"type": "string"},
        "min_abs_corr": {"type": "number"},
        "min_icir": {"type": "number"},
        "min_ir": {"type": "number"},
        "min_runs": {"type": "integer", "minimum": 1},
        "qe_selectable": {"type": "boolean"},
        "plan_id": {"type": "string"},
        "confirmation_text": {"type": "string"},
        "title": {"type": "string"},
        "template_kind": {"type": "string"},
        "config_json": {"type": "object"},
        "template_id": {"type": "string"},
        "confirm_template": {"type": "string"},
        "confirm_run": {"type": "string"},
        "confirm_direct_run": {"type": "string"},
        "memory_type": {"type": "string"},
        "subject_key": {"type": "string"},
        "problem_statement": {"type": "string"},
        "bug_id": {"type": "string"},
        "issue_number": {"type": "integer"},
        "candidate_id": {"type": "string"},
        "mode": {"type": "string"},
    }
    for required in required_inputs:
        properties.setdefault(str(required), {"type": "string"})
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required_inputs:
        schema["required"] = required_inputs
    return schema


def mcp_tool_function_name(tool_name: str) -> str:
    """Return a provider-safe function name for a manifest MCP tool."""

    safe = re.sub(r"[^A-Za-z0-9_]", "_", str(tool_name or "")).strip("_") or "mcp_tool"
    if not re.match(r"^[A-Za-z_]", safe):
        safe = f"tool_{safe}"
    if len(safe) <= 64:
        return safe
    digest = sha1(safe.encode("utf-8")).hexdigest()[:8]
    return f"{safe[:55]}_{digest}"


def function_calling_tools_for_mcp(tools: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """Build LiteLLM/OpenAI function specs plus a local name-to-MCP registry."""

    specs: list[dict[str, Any]] = []
    registry: dict[str, dict[str, Any]] = {}
    for tool in tools:
        if str(tool.get("status") or "") not in {"enabled", "ready", "approved"}:
            continue
        server_key = str(tool.get("server_key") or "").strip()
        tool_name = str(tool.get("tool_name") or "").strip()
        if not server_key or not tool_name:
            continue
        function_name = mcp_tool_function_name(tool_name)
        schema = tool.get("input_schema_json") if isinstance(tool.get("input_schema_json"), dict) else {"type": "object"}
        description = str(tool.get("description") or tool.get("title") or tool_name).strip()
        risk = str(tool.get("risk_level") or "medium")
        side_effect = str(tool.get("side_effect_level") or "read_only")
        specs.append(
            {
                "type": "function",
                "function": {
                    "name": function_name,
                    "description": (
                        f"{description} MCP route={server_key}/{tool_name}; "
                        f"risk={risk}; side_effect={side_effect}. "
                        "Use only parameters declared in the JSON schema."
                    )[:1024],
                    "parameters": schema,
                },
            }
        )
        registry[function_name] = {"server_key": server_key, "tool_name": tool_name}
    return specs, registry


def manifest_entry_to_mcp_tool(entry: ToolManifestEntry, overlay: Mapping[str, Any] | None = None, catalog: GatewayServerCatalog | None = None) -> dict[str, Any]:
    selected = gateway_catalog() if catalog is None else catalog
    server_key = server_key_for_module(entry.module, selected)
    server = _server_def(server_key, selected)
    risk = _ra_risk_for(entry)
    side_effect = _ra_side_effect_for(entry)
    requires_approval = entry.assistant_usable == "preflight_required" or entry.requires_confirmation or side_effect != "read_only"
    override = RA_TOOL_METADATA_OVERRIDES.get(entry.tool_name)
    if override:
        risk = str(override.get("risk_level", risk))
        side_effect = str(override.get("side_effect_level", side_effect))
        if "requires_approval" in override:
            requires_approval = bool(override["requires_approval"])
    status = "enabled"
    if overlay and str(overlay.get("status") or "") in {"disabled", "blocked", "deprecated"}:
        status = str(overlay["status"])
    required_confirmations = _required_confirmations_for(entry)
    checks = PREFLIGHT_CHECKS_BY_TOOL.get(entry.tool_name)
    if checks is None:
        checks = ["catalog", "manifest_risk", "approval" if requires_approval else "read_only_boundary", "payload_budget"]
    schema = _input_schema_for(entry.tool_name)
    gateway_manifest = {
        "tool_name": entry.tool_name,
        "module": entry.module,
        "profile_tags": list(entry.profile_tags),
        "risk_level": entry.risk_level,
        "assistant_usable": entry.assistant_usable,
        "requires_confirmation": entry.requires_confirmation,
        "backend_endpoint": entry.backend_endpoint,
        "migration_state": entry.migration_state,
        "response_budget": entry.response_budget,
    }
    return {
        "server_key": server_key,
        "tool_name": entry.tool_name,
        "title": entry.tool_name.replace("_", " "),
        "description": f"{server.get('summary_zh') or server_key}: {entry.tool_name}",
        "risk_level": risk,
        "side_effect_level": side_effect,
        "requires_approval": requires_approval,
        "input_schema_json": schema,
        "output_schema_json": {"type": "object"},
        "preflight_schema_json": {
            "checks": checks,
            "gateway_manifest": gateway_manifest,
            "manifest": gateway_manifest,
        },
        "required_confirmations": required_confirmations,
        "status": status,
        "module": entry.module,
        "profile_tags": list(entry.profile_tags),
        "profile": profile_for_server_key(server_key, selected),
        "manifest_risk_level": entry.risk_level,
        "assistant_usable": entry.assistant_usable,
        "requires_confirmation": entry.requires_confirmation,
        "backend_endpoint": entry.backend_endpoint,
        "migration_state": entry.migration_state,
        "response_budget": entry.response_budget,
        "catalog_source": "gateway_manifest_derived_catalog",
        "legacy_server_aliases": sorted(alias for alias, target in selected.legacy_aliases.items() if target == server_key),
    }


def default_mcp_servers() -> list[dict[str, Any]]:
    selected = gateway_catalog()
    records: list[dict[str, Any]] = []
    for server_key in selected.server_order:
        canonical = _server_def(server_key, selected)
        modules = list(canonical["modules"])
        health = {
            "mode": "loopback",
            "module": modules[0] if len(modules) == 1 else None,
            "modules": modules,
            "profile": canonical["profile"],
            "domain": canonical["domain"],
            "display_name_zh": canonical["display_name_zh"],
            "business_aliases_zh": canonical["business_aliases_zh"],
            "summary_zh": canonical["summary_zh"],
            "legacy_aliases": canonical["legacy_aliases"],
            "summary_first_payload": True,
            "catalog_source": "gateway_manifest_derived_catalog",
        }
        if server_key == "aistock-local-data":
            health["capability_key"] = "local_data_management"
        records.append({"server_key": server_key, "title": canonical["title"], "status": "ready", "health_json": health})
    return records


def default_mcp_tools() -> list[dict[str, Any]]:
    selected = gateway_catalog()
    return [manifest_entry_to_mcp_tool(entry, catalog=selected) for entry in TOOL_MANIFEST]


def workflow_capabilities() -> list[dict[str, Any]]:
    selected = gateway_catalog()
    capabilities: list[dict[str, Any]] = []
    for spec in all_domain_specs():
        if spec.domain == McpDomain.GENERAL:
            continue
        refs = []
        for tool in [*spec.read_tools, *spec.plan_tools, *spec.confirmed_tools]:
            entry = TOOL_MANIFEST_BY_NAME.get(tool)
            if entry is None:
                raise KeyError(f"workflow capability references unknown MCP tool: {tool}")
            refs.append({"server_key": server_key_for_module(entry.module, selected), "tool_name": tool})
        if not refs:
            entry = TOOL_MANIFEST_BY_NAME.get(spec.default_tool)
            if entry is None:
                raise KeyError(f"workflow capability default tool is unknown: {spec.default_tool}")
            refs = [{"server_key": server_key_for_module(entry.module, selected), "tool_name": spec.default_tool}]
        capabilities.append(
            {
                "capability_key": f"{spec.domain.value}.mcp_orchestration",
                "capability_type": "mcp_tool",
                "title": spec.summary_zh,
                "natural_language_triggers": list(spec.synonyms),
                "description_for_llm": f"Use audited MCP tools for {spec.summary_zh}; keep summary-first payloads and follow {spec.risk_policy}.",
                "risk_level": "medium" if spec.plan_tools else "low",
                "side_effect_level": "draft_only" if spec.plan_tools else "read_only",
                "required_confirmations": [],
                "mcp_tool_refs": refs,
                "skill_refs": [],
                "workflow_pack_ref": None,
                "status": "approved",
                "source_ref": "backend/services/research_assistant/mcp_catalog_sync.py#manifest-derived",
            }
        )
    return capabilities


def load_catalog() -> dict[str, Any]:
    servers = default_mcp_servers()
    tools = default_mcp_tools()
    return {
        "servers": servers,
        "tools": tools,
        "workflow_capabilities": workflow_capabilities(),
        "server_count": len(servers),
        "tool_count": len(tools),
        "catalog_source": "gateway_manifest_derived_catalog",
    }
