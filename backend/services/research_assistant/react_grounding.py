"""Provider-only ReAct grounding core for Research Assistant.

The module is domain-neutral: it receives an audited tool catalog, a model
callable, and an MCP provider adapter from the host service. It never imports
AIstock service, repository, database, or domain modules.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol
from urllib.parse import urlparse


logger = logging.getLogger("aistock.research_assistant.react_grounding")


@dataclass(frozen=True)
class ReactGroundingConfig:
    max_tool_iterations: int
    evidence_required: bool = True
    user_message: str = ""
    token_budget: int | None = None
    placeholder_patterns: tuple[str, ...] = (r"\bXX\b", r"\bX%\b", r"approxX", r"about X")
    forbidden_answer_markers: tuple[str, ...] = ()
    future_answer_terms: tuple[str, ...] = ("未来", "预测", "预判", "趋势", "会涨", "会跌", "上涨", "下跌", "forecast", "predict", "outlook")
    future_required_terms: tuple[str, ...] = ("驱动", "情景", "风险", "driver", "scenario", "risk")
    future_directional_markers: tuple[str, ...] = ("一定会上涨", "将上涨", "会持续上涨", "必然上涨", "一定会下跌", "将下跌", "会持续下跌", "必然下跌", "sure to rise", "will rise", "will fall")

    def __post_init__(self) -> None:
        if self.max_tool_iterations <= 0:
            raise ValueError("max_tool_iterations must be positive")


@dataclass(frozen=True)
class McpToolCall:
    server_key: str
    tool_name: str
    payload_json: dict[str, Any] = field(default_factory=dict)
    stable_call_id: str = ""
    reason: str = ""
    risk_level: str | None = None
    side_effect_level: str | None = None

    def sorted_key(self) -> tuple[str, str, str]:
        return (self.server_key, self.tool_name, self.stable_call_id or "")


@dataclass(frozen=True)
class ToolCatalogEntry:
    server_key: str
    tool_name: str
    status: str
    risk_level: str = "medium"
    side_effect_level: str = "read_only"
    requires_approval: bool = False
    required_confirmations: tuple[str, ...] = ()


@dataclass(frozen=True)
class ToolGateDecision:
    allowed: bool
    action: str
    reason: str
    catalog_entry: ToolCatalogEntry | None = None
    requires_approval: bool = False
    risk_level: str = "medium"
    side_effect_level: str = "read_only"


@dataclass
class McpToolResult:
    server_key: str
    tool_name: str
    status: str
    payload_json: dict[str, Any] = field(default_factory=dict)
    source_refs: list[str] = field(default_factory=list)
    as_of: str | None = None
    artifact_refs: list[Any] = field(default_factory=list)
    summary: str = ""
    observation: str = ""
    tool_event_id: str | None = None
    action_proposal_id: str | None = None
    preflight: dict[str, Any] = field(default_factory=dict)
    error_json: dict[str, Any] = field(default_factory=dict)
    executed: bool = False
    blocked_reason: str | None = None
    stable_call_id: str = ""
    side_effect_level: str = "read_only"

    def sorted_key(self) -> tuple[str, str, str]:
        return (self.server_key, self.tool_name, self.stable_call_id or "")


@dataclass(frozen=True)
class ModelTurn:
    content: str
    provider: str
    model: str
    duration_ms: int
    usage: dict[str, Any] = field(default_factory=dict)
    tool_calls: list[McpToolCall] = field(default_factory=list)


@dataclass(frozen=True)
class EvidenceGuardDecision:
    allowed: bool
    text: str
    reason: str
    source_count: int
    as_of_count: int


@dataclass
class ReactGroundingResult:
    final_text: str
    messages: list[dict[str, Any]]
    tool_calls: list[McpToolCall]
    tool_results: list[McpToolResult]
    trace_steps: list[dict[str, Any]]
    evidence_guard: EvidenceGuardDecision
    iterations: int
    stopped_reason: str
    model_turns: list[ModelTurn] = field(default_factory=list)


class McpProvider(Protocol):
    def execute_read_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        ...

    def preflight_confirmation_only(self, call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
        ...


ModelComplete = Callable[[list[dict[str, Any]]], ModelTurn]
ToolResultCompactor = Callable[[McpToolResult], McpToolResult]


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)
_TOOL_CHOICE_RE = re.compile(r"<assistant_tool_choice\b[^>]*>(.*?)</assistant_tool_choice>", re.IGNORECASE | re.DOTALL)
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])\d+(?:\.\d+)?\s*(?:%|days?|items?)?")
PROGRAM_ERROR_REASON_CODES = {
    "capability_not_found",
    "tool_not_in_audited_catalog",
    "tool_execution_error",
    "data_source_unavailable",
    "tool_result_compaction_error",
}
READ_ONLY_PARTIAL_EVIDENCE_REASON_CODES = {
    "stock_depth_required_evidence_missing",
    "max_tool_iterations_exhausted",
}
SUCCESS_STATUSES = {"succeeded", "success", "ok"}
EXTERNAL_RESEARCH_SERVER_KEY = "aistock-external-research"
EXTERNAL_RESEARCH_WEB_TOOL = "external_research_search_web"
EXTERNAL_RESEARCH_STUB_HOSTS = ("example.org",)
EXTERNAL_RESEARCH_STUB_MARKERS = (
    "deterministic_offline",
    "offline_extract_provider",
    "offline_external_research",
    "offline_stub",
    "example_web_index",
    "external_research_summary_adapter",
    "summary_adapter",
)
GRAPH_CONTEXT_RESULT_KEY = ("research-assistant", "graph_context")
EVIDENCE_GUARD_RESULT_KEY = ("evidence_guard", "compose_with_evidence_guard")
INFORMATION_QUERY_TERMS = (
    "what",
    "which",
    "who",
    "where",
    "when",
    "how",
    "search",
    "find",
    "latest",
    "recent",
    "news",
    "information",
    "overview",
    "analysis",
    "trend",
    "industry",
    "\u4ec0\u4e48",
    "\u54ea\u4e9b",
    "\u8c01",
    "\u4f55\u65f6",
    "\u600e\u4e48",
    "\u5982\u4f55",
    "\u662f\u5426",
    "\u67e5\u8be2",
    "\u641c\u7d22",
    "\u6700\u65b0",
    "\u8fd1\u671f",
    "\u65b0\u95fb",
    "\u4fe1\u606f",
    "\u8d44\u6599",
    "\u60c5\u51b5",
    "\u57fa\u672c\u9762",
    "\u884c\u4e1a",
    "\u8d70\u52bf",
    "\u8d8b\u52bf",
    "\u5206\u6790",
)
STOCK_DEPTH_QUERY_TERMS = (
    "stock depth",
    "all-round",
    "comprehensive",
    "future trend",
    "recent trend",
    "\u4e09\u7ef4",
    "\u7efc\u5408",
    "\u5168\u65b9\u4f4d",
    "\u6df1\u5ea6",
)
STOCK_DEPTH_DIMENSION_TERMS = (
    "limit down",
    "fundamental",
    "fundamentals",
    "\u8dcc\u505c",
    "\u57fa\u672c\u9762",
    "\u57fa\u672c\u60c5\u51b5",
    "\u8fd1\u671f\u8d70\u52bf",
    "\u672a\u6765\u8d8b\u52bf",
    "\u884c\u4e1a\u5730\u4f4d",
    "\u8d44\u91d1",
    "\u8d22\u52a1",
    "\u6280\u672f",
    "fund flow",
    "financial",
    "technical",
    "industry",
)
STOCK_DEPTH_REQUIRED_CATEGORIES = ("market", "history", "fund_flow", "fundamental")
STOCK_DEPTH_MIN_REQUIRED_CATEGORIES = 4
STOCK_DEPTH_MIN_TOOL_EXECUTIONS = 8
STOCK_ANALYSIS_SERVER_KEY = "aistock-stock-analysis"
FACTUAL_LIST_QUERY_TERMS = (
    "leaderboard",
    "ranking",
    "rank",
    "top",
    "topn",
    "top n",
    "list",
    "table",
    "respectively",
    "cagr",
    "annualized",
    "\u6392\u540d",
    "\u699c",
    "\u5217\u8868",
    "\u6e05\u5355",
    "\u8868\u683c",
    "\u5206\u522b",
    "\u5404\u81ea",
    "\u5e74\u5316",
    "\u6536\u76ca",
)
FACTUAL_LOOKUP_QUERY_TERMS = (
    "what are",
    "which",
    "model",
    "\u6a21\u578b",
    "\u591a\u5c11",
    "\u54ea\u4e9b",
    "\u662f\u4ec0\u4e48",
)
JUDGEMENT_SYNTHESIS_QUERY_TERMS = (
    "analysis",
    "analyze",
    "synthesize",
    "comprehensive",
    "all-round",
    "trend",
    "future",
    "outlook",
    "why",
    "how",
    "\u5206\u6790",
    "\u7efc\u5408",
    "\u5168\u65b9\u4f4d",
    "\u600e\u4e48\u6837",
    "\u600e\u4e48\u770b",
    "\u4e3a\u4ec0\u4e48",
    "\u539f\u56e0",
    "\u8d70\u52bf",
    "\u8d8b\u52bf",
    "\u672a\u6765",
)
UNVERIFIED_VALUE_TERMS = ("not_verified", "not verified", "unverified", "\u672a\u9a8c\u8bc1")
UNVERIFIED_RISK_TERMS = (
    "risk",
    "risky",
    "do not treat",
    "not real",
    "not production",
    "backtest",
    "\u98ce\u9669",
    "\u52ff\u5f53",
    "\u4e0d\u80fd\u5f53",
    "\u672a\u9a8c\u8bc1\u56de\u6d4b",
    "\u771f\u5b9e\u6536\u76ca",
)


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = text.strip()
    candidates = [raw]
    candidates.extend(match.group(1).strip() for match in _JSON_FENCE_RE.finditer(text))
    for match in _TOOL_CHOICE_RE.finditer(text):
        candidates.append(match.group(1).strip())
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def extract_structured_tool_calls(text: str) -> list[McpToolCall]:
    payload = _parse_json_object(text)
    if not isinstance(payload, dict):
        return []
    calls_raw = payload.get("tool_calls")
    if calls_raw is None and (payload.get("server_key") or payload.get("tool_name") or payload.get("tool")):
        calls_raw = [payload]
    if not isinstance(calls_raw, list):
        return []
    calls: list[McpToolCall] = []
    for index, raw in enumerate(calls_raw):
        if not isinstance(raw, dict):
            continue
        server_key = str(raw.get("server_key") or raw.get("server") or "").strip()
        tool_name = str(raw.get("tool_name") or raw.get("tool") or "").strip()
        if not server_key or not tool_name:
            continue
        payload_json = raw.get("payload_json") or raw.get("payload") or raw.get("args") or {}
        if not isinstance(payload_json, dict):
            payload_json = {"value": payload_json}
        stable_call_id = str(raw.get("stable_call_id") or raw.get("call_id") or f"call_{index:03d}")
        calls.append(
            McpToolCall(
                server_key=server_key,
                tool_name=tool_name,
                payload_json=payload_json,
                stable_call_id=stable_call_id,
                reason=str(raw.get("reason") or ""),
                risk_level=str(raw.get("risk_level")) if raw.get("risk_level") is not None else None,
                side_effect_level=str(raw.get("side_effect_level")) if raw.get("side_effect_level") is not None else None,
            )
        )
    return sorted(calls, key=lambda call: call.sorted_key())


def assert_tool_in_catalog(call: McpToolCall, catalog_entries: list[ToolCatalogEntry]) -> ToolGateDecision:
    entry = next((item for item in catalog_entries if item.server_key == call.server_key and item.tool_name == call.tool_name), None)
    if entry is None:
        return ToolGateDecision(False, "reject_catalog", "tool_not_in_audited_catalog")
    if entry.status not in {"enabled", "ready", "approved"}:
        return ToolGateDecision(
            False,
            "reject_catalog",
            f"tool_status_{entry.status}",
            catalog_entry=entry,
            risk_level=call.risk_level or entry.risk_level or "medium",
            side_effect_level=call.side_effect_level or entry.side_effect_level or "read_only",
        )
    risk_level = call.risk_level or entry.risk_level or "medium"
    side_effect_level = call.side_effect_level or entry.side_effect_level or "read_only"
    requires_approval = bool(entry.requires_approval) or risk_level in {"high", "production_sensitive"} or side_effect_level in {
        "write_nonprod",
        "high_cost_compute",
        "production_sensitive",
        "draft_only",
    }
    if risk_level == "low" and side_effect_level == "read_only" and not entry.requires_approval:
        return ToolGateDecision(True, "execute_read_only", "low_risk_read_only", entry, False, risk_level, side_effect_level)
    return ToolGateDecision(True, "preflight_confirmation_only", "requires_preflight_confirmation", entry, requires_approval, risk_level, side_effect_level)


def _compact_payload(payload: dict[str, Any], *, max_chars: int = 1400) -> dict[str, Any]:
    allowed = {
        "source",
        "domain",
        "total",
        "limit",
        "offset",
        "summary_first",
        "response_mode",
        "server_key",
        "tool_name",
        "next_step",
        "items",
        "artifact_refs",
        "omitted_sections",
        "detail_tool",
        "evidence_policy",
        "query",
        "locale",
        "provider",
        "source_refs",
        "as_of",
        "reason_codes",
        "warnings",
        "status",
        "status_counts",
        "group_counts",
        "status_groups",
        "health_summary",
        "trade_date",
        "evidence_card",
        "sections",
        "fundamentals",
        "symbol",
        "dataset",
        "item",
    }
    compact = {key: value for key, value in payload.items() if key in allowed}
    if isinstance(compact.get("items"), list):
        compact["items"] = compact["items"][:3]
    rendered = json.dumps(compact, ensure_ascii=False, sort_keys=True)
    if len(rendered) > max_chars:
        compact = {
            "summary_first": payload.get("summary_first", True),
            "source": payload.get("source"),
            "total": payload.get("total"),
            "response_mode": payload.get("response_mode"),
            "as_of": payload.get("as_of"),
            "trade_date": payload.get("trade_date"),
            "status_counts": payload.get("status_counts"),
            "group_counts": payload.get("group_counts"),
            "health_summary": payload.get("health_summary"),
            "sections": (payload.get("sections") or [])[:8] if isinstance(payload.get("sections"), list) else payload.get("sections"),
            "artifact_refs": (payload.get("artifact_refs") or [])[:3] if isinstance(payload.get("artifact_refs"), list) else [],
            "omitted_sections": payload.get("omitted_sections") or [],
        }
        for key in ("domain", "server_key", "tool_name", "detail_tool", "evidence_policy", "query", "locale", "provider"):
            if key in payload:
                compact[key] = payload.get(key)
        if isinstance(payload.get("items"), list):
            compact["items"] = payload["items"][:3]
        if isinstance(payload.get("status_groups"), dict):
            compact["status_groups"] = {
                str(key): value[:5] if isinstance(value, list) else value
                for key, value in payload["status_groups"].items()
            }
    return compact


def _normalize_status(value: str | None) -> str:
    return str(value or "").strip().lower()


def _is_success_result(result: McpToolResult) -> bool:
    return _normalize_status(result.status) in SUCCESS_STATUSES and bool(result.executed)


def _payload_list_count(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    return len(value) if isinstance(value, list) else 0


def _payload_declares_empty_result(payload: dict[str, Any]) -> bool:
    try:
        if "total" in payload and int(payload.get("total") or 0) == 0:
            return True
    except (TypeError, ValueError):
        pass
    if "items" in payload and isinstance(payload.get("items"), list) and not payload["items"]:
        if _payload_list_count(payload, "sections") == 0 and not isinstance(payload.get("item"), dict):
            return True
    return False


def _iter_string_values(value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(value, dict):
        for item in value.values():
            values.extend(_iter_string_values(item))
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            values.extend(_iter_string_values(item))
    elif value is not None:
        text = str(value).strip()
        if text:
            values.append(text)
    return values


def _hostname_is_stub(hostname: str | None) -> bool:
    host = str(hostname or "").strip().lower()
    return any(host == stub_host or host.endswith(f".{stub_host}") for stub_host in EXTERNAL_RESEARCH_STUB_HOSTS)


def _text_contains_stub_host(text: str) -> bool:
    try:
        parsed = urlparse(text)
    except ValueError:
        parsed = urlparse("")
    if _hostname_is_stub(parsed.hostname):
        return True
    if "://" not in text:
        try:
            parsed = urlparse(f"//{text}")
        except ValueError:
            parsed = urlparse("")
        if _hostname_is_stub(parsed.hostname):
            return True
    lowered = text.lower()
    return any(re.search(rf"(^|[^a-z0-9-]){re.escape(stub_host)}([^a-z0-9-]|$)", lowered) for stub_host in EXTERNAL_RESEARCH_STUB_HOSTS)


def _external_research_result_is_stub(result: McpToolResult) -> bool:
    if not _is_external_research_result(result):
        return False
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    values = _iter_string_values(payload)
    values.extend(str(item) for item in result.source_refs if str(item or "").strip())
    values.extend(str(item) for item in (result.summary, result.observation) if str(item or "").strip())
    for value in values:
        lowered = value.lower()
        if any(marker in lowered for marker in EXTERNAL_RESEARCH_STUB_MARKERS):
            return True
        if _text_contains_stub_host(value):
            return True
    return False


def _result_has_evidence_items(result: McpToolResult) -> bool:
    if not _is_success_result(result):
        return False
    if _external_research_result_is_stub(result):
        return False
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    if _payload_declares_empty_result(payload):
        return False
    if _payload_list_count(payload, "items") > 0:
        return True
    if _payload_list_count(payload, "sections") > 0:
        return True
    if isinstance(payload.get("item"), dict) and payload["item"]:
        return True
    if payload.get("graph_context") or payload.get("response_mode") == "graph_context":
        return True
    if result.source_refs and result.as_of and (result.summary or result.observation):
        return True
    return False


def _is_empty_success_result(result: McpToolResult) -> bool:
    if not _is_success_result(result):
        return False
    return not _result_has_evidence_items(result)


def _is_business_source_result(result: McpToolResult) -> bool:
    return (result.server_key, result.tool_name) not in {GRAPH_CONTEXT_RESULT_KEY, EVIDENCE_GUARD_RESULT_KEY}


def _is_external_research_result(result: McpToolResult) -> bool:
    return result.server_key == EXTERNAL_RESEARCH_SERVER_KEY and result.tool_name == EXTERNAL_RESEARCH_WEB_TOOL


def _has_business_evidence(collected_results: list[McpToolResult]) -> bool:
    return any(_is_business_source_result(result) and _result_has_evidence_items(result) for result in collected_results)


def _is_stock_depth_query(config: ReactGroundingConfig) -> bool:
    text = str(config.user_message or "")
    lower = text.lower()
    has_depth_focus = "stock depth" in lower or "\u6df1\u5ea6" in text
    dimension_hits = {term for term in STOCK_DEPTH_DIMENSION_TERMS if term.lower() in lower}
    has_limit_down_triplet = (
        ("limit down" in lower or "\u8dcc\u505c" in text)
        and ("future" in lower or "\u672a\u6765" in text)
        and ("fundamental" in lower or "\u57fa\u672c\u9762" in text or "\u57fa\u672c\u60c5\u51b5" in text)
    )
    has_stock_depth_phrase = "stock depth" in lower and ("fundamental" in lower or "future" in lower)
    return (has_depth_focus and len(dimension_hits) >= 2) or has_limit_down_triplet or has_stock_depth_phrase


def _stock_depth_category_for_dataset(dataset: str) -> str | None:
    normalized = dataset.strip().lower()
    if normalized == "quote":
        return "market"
    if normalized == "kline":
        return "history"
    if normalized == "fund_flow":
        return "fund_flow"
    if normalized in {"financials", "quarterly", "fundamentals"}:
        return "fundamental"
    if normalized == "technicals":
        return "technicals"
    if normalized == "margin_financing":
        return "margin"
    return None


def _stock_depth_section_has_evidence(section: dict[str, Any]) -> bool:
    status = _normalize_status(str(section.get("status") or "ok"))
    if status in {"blocked", "degraded", "failed", "failure", "error"}:
        return False
    if isinstance(section.get("items"), list) and section["items"]:
        return True
    try:
        if int(section.get("total") or 0) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return bool(section.get("source_refs") and section.get("as_of"))


def _stock_depth_section_has_real_external_evidence(section: dict[str, Any]) -> bool:
    source_blob = json.dumps(
        {
            "source": section.get("source"),
            "source_refs": section.get("source_refs"),
            "items": section.get("items"),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    lowered = source_blob.lower()
    if not ("external_research" in lowered or "external-research" in lowered):
        return False
    if any(marker in lowered for marker in EXTERNAL_RESEARCH_STUB_MARKERS):
        return False
    return not _text_contains_stub_host(source_blob)


def _stock_depth_evidence_coverage(collected_results: list[McpToolResult]) -> dict[str, Any]:
    categories: set[str] = set()
    tools: set[str] = set()
    evidence_units: set[str] = set()
    external_research = False
    for result in collected_results:
        if not _is_success_result(result):
            continue
        if result.server_key == EXTERNAL_RESEARCH_SERVER_KEY:
            tools.add(f"{result.server_key}/{result.tool_name}")
        elif result.server_key == STOCK_ANALYSIS_SERVER_KEY:
            tools.add(f"{result.server_key}/{result.tool_name}")
        if not _result_has_evidence_items(result):
            continue
        if result.server_key == EXTERNAL_RESEARCH_SERVER_KEY:
            external_research = True
        payload = result.payload_json if isinstance(result.payload_json, dict) else {}
        sections = _payload_section_dicts(payload)
        if not sections and payload.get("dataset"):
            sections = [payload]
        for section in sections:
            if not _stock_depth_section_has_evidence(section):
                continue
            dataset = str(section.get("dataset") or "")
            category = _stock_depth_category_for_dataset(dataset)
            if category:
                categories.add(category)
                evidence_units.add(f"{result.server_key}/{result.tool_name}:{dataset}")
            if _stock_depth_section_has_real_external_evidence(section):
                external_research = True
        if payload.get("response_mode") == "stock_analysis_evidence_card":
            tools.add(f"{result.server_key}/{result.tool_name}")
    missing = [category for category in STOCK_DEPTH_REQUIRED_CATEGORIES if category not in categories]
    return {
        "categories": sorted(categories),
        "missing_categories": missing,
        "external_research": external_research,
        "tool_count": max(len(tools), len(evidence_units)),
        "tools": sorted(tools),
    }


def _passes_stock_depth_required_evidence(config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> bool:
    if not _is_stock_depth_query(config):
        return True
    coverage = _stock_depth_evidence_coverage(collected_results)
    return (
        len(set(coverage["categories"]) & set(STOCK_DEPTH_REQUIRED_CATEGORIES)) >= STOCK_DEPTH_MIN_REQUIRED_CATEGORIES
        and not coverage["missing_categories"]
        and bool(coverage["external_research"])
        and int(coverage["tool_count"]) >= STOCK_DEPTH_MIN_TOOL_EXECUTIONS
    )


def _render_stock_depth_missing_evidence_reply(config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> str:
    del config
    coverage = _stock_depth_evidence_coverage(collected_results)
    missing = list(coverage["missing_categories"])
    if not coverage["external_research"]:
        missing.append("external_research")
    if int(coverage["tool_count"]) < STOCK_DEPTH_MIN_TOOL_EXECUTIONS:
        missing.append(f"tool_count>={STOCK_DEPTH_MIN_TOOL_EXECUTIONS}")
    attempted = ", ".join(coverage["tools"]) if coverage["tools"] else "none"
    covered = ", ".join(coverage["categories"]) if coverage["categories"] else "none"
    return (
        "\u8bc1\u636e\u4e0d\u8db3\uff1a\u4e2a\u80a1\u6df1\u5ea6\u5206\u6790\u5fc5\u987b\u8986\u76d6"
        "\u884c\u60c5\u3001>=60\u4ea4\u6613\u65e5\u5386\u53f2K\u7ebf\u3001\u8d44\u91d1\u6d41\u3001\u57fa\u672c\u9762\u5e76\u81f3\u5c11\u4e00\u6b21\u8054\u7f51\u8bc1\u636e\uff1b"
        f"\u5df2\u8986\u76d6={covered}\uff1b\u7f3a\u5931={', '.join(missing)}\uff1b"
        f"\u5df2\u5c1d\u8bd5\u5de5\u5177={attempted}\u3002reason_code=stock_depth_required_evidence_missing"
    )


def _side_effect_level(value: str | None) -> str:
    return str(value or "read_only")


def _result_side_effect_level(result: McpToolResult) -> str:
    return _side_effect_level(getattr(result, "side_effect_level", None))


def _call_side_effect_level(call: McpToolCall) -> str:
    return _side_effect_level(call.side_effect_level)


def _is_read_only_result(result: McpToolResult) -> bool:
    return _result_side_effect_level(result) == "read_only"


def _all_attempted_actions_read_only(collected_calls: list[McpToolCall], collected_results: list[McpToolResult]) -> bool:
    return all(_call_side_effect_level(call) == "read_only" for call in collected_calls) and all(
        _result_side_effect_level(result) == "read_only" for result in collected_results
    )


def _read_only_business_evidence_results(collected_results: list[McpToolResult]) -> list[McpToolResult]:
    return [
        result
        for result in collected_results
        if _is_read_only_result(result)
        and _is_business_source_result(result)
        and _result_has_evidence_items(result)
    ]


def _read_only_gap_notes(reason: str, config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> list[str]:
    del config
    notes: list[str] = []
    if reason == "stock_depth_required_evidence_missing":
        coverage = _stock_depth_evidence_coverage(collected_results)
        for category in coverage["missing_categories"]:
            notes.append(f"missing stock-depth category: {category}")
        if not coverage["external_research"]:
            notes.append("missing external_research evidence")
        if int(coverage["tool_count"]) < STOCK_DEPTH_MIN_TOOL_EXECUTIONS:
            notes.append("missing required read-only tool breadth")
    if reason == "max_tool_iterations_exhausted":
        notes.append("model did not produce a final grounded synthesis before the tool loop stopped")
    for result in collected_results:
        if not _is_read_only_result(result) or not _is_business_source_result(result):
            continue
        if _is_empty_success_result(result):
            notes.append(f"{result.server_key}/{result.tool_name} returned no evidence items")
        elif _normalize_status(result.status) not in SUCCESS_STATUSES and result.blocked_reason:
            notes.append(f"{result.server_key}/{result.tool_name} ended with status={result.status} reason={result.blocked_reason}")
    return _dedupe_preserve_order(notes) or ["read-only evidence coverage is incomplete"]


def _short_evidence_summary(result: McpToolResult) -> str:
    summary = str(result.summary or result.observation or "").strip()
    if not summary:
        payload = result.payload_json if isinstance(result.payload_json, dict) else {}
        response_mode = str(payload.get("response_mode") or "").strip()
        dataset = str(payload.get("dataset") or "").strip()
        mode = response_mode or dataset or "evidence returned"
        summary = f"{mode}"
    summary = " ".join(summary.split())
    return summary[:220] + ("..." if len(summary) > 220 else "")


def _render_read_only_partial_evidence_reply(
    *,
    candidate_text: str,
    original_reason: str,
    collected_calls: list[McpToolCall],
    collected_results: list[McpToolResult],
    config: ReactGroundingConfig,
) -> str:
    candidate = strip_internal_chain(candidate_text)
    candidate_lower = candidate.lower()
    lines: list[str] = []
    if candidate and "insufficient evidence" not in candidate_lower and "max tool iterations reached" not in candidate_lower:
        lines.append(candidate)
        lines.append("")
        lines.append("Read-only partial evidence note:")
    else:
        lines.append("Read-only partial evidence note:")
    lines.append("I can answer only the parts backed by collected read-only evidence; missing dimensions are explicit below.")
    lines.append("For future-looking parts, drivers/scenarios/risks are incomplete; I do not predict direction.")
    lines.append("Available read-only evidence:")
    for result in _read_only_business_evidence_results(collected_results)[:6]:
        pairs = _citation_pairs_for_result(result)
        pair = pairs[0] if pairs else {
            "source": (result.source_refs[0] if result.source_refs else "unknown"),
            "as_of": str(result.as_of or "unknown"),
        }
        lines.append(
            f"- {result.server_key}/{result.tool_name}: {_short_evidence_summary(result)}; "
            f"source={pair['source']} as_of={pair['as_of']}"
        )
    lines.append("Missing / not covered:")
    lines.extend(f"- {note}" for note in _read_only_gap_notes(original_reason, config, collected_results)[:8])
    if _has_unverified_evidence(collected_results):
        lines.append("- unverified backtest or experimental data is risky; do not treat it as real returns.")
    attempted = ", ".join(_attempted_source_names(collected_calls, collected_results)) or "none"
    lines.append(f"Attempted sources: {attempted}")
    lines.append(f"reason_code=read_only_partial_evidence_degraded; original_reason={original_reason}")
    return "\n".join(lines).strip()


def _degraded_reply_preserves_redlines(
    text: str,
    *,
    config: ReactGroundingConfig,
    collected_results: list[McpToolResult],
) -> bool:
    known_sources = _known_source_values(collected_results)
    known_as_of = _known_as_of_values(collected_results)
    inline_source = _has_inline_source(text, known_sources)
    inline_as_of = _has_inline_as_of(text, known_as_of)
    if _contains_placeholder(text, config.placeholder_patterns):
        return False
    if _contains_forbidden_marker(text, config.forbidden_answer_markers):
        return False
    if config.evidence_required and collected_results and not (inline_source and inline_as_of):
        return False
    if config.evidence_required and _has_numeric_fact(text) and not (inline_source and inline_as_of):
        return False
    if not _passes_factual_list_row_citations(text, config, known_sources, known_as_of):
        return False
    if not _passes_future_answer_discipline(text, config):
        return False
    if _has_unverified_evidence(collected_results) and not _passes_unverified_risk_labels(text):
        return False
    return True


def _read_only_partial_evidence_degradation(
    *,
    guard: EvidenceGuardDecision,
    candidate_text: str,
    collected_calls: list[McpToolCall],
    collected_results: list[McpToolResult],
    config: ReactGroundingConfig,
) -> EvidenceGuardDecision | None:
    if guard.allowed or guard.reason not in READ_ONLY_PARTIAL_EVIDENCE_REASON_CODES:
        return None
    if _program_error_results(collected_results):
        return None
    if not _all_attempted_actions_read_only(collected_calls, collected_results):
        return None
    if not _read_only_business_evidence_results(collected_results):
        return None
    text = _render_read_only_partial_evidence_reply(
        candidate_text=candidate_text,
        original_reason=guard.reason,
        collected_calls=collected_calls,
        collected_results=collected_results,
        config=config,
    )
    if not _degraded_reply_preserves_redlines(text, config=config, collected_results=collected_results):
        return None
    return EvidenceGuardDecision(True, text, "read_only_partial_evidence_degraded", guard.source_count, guard.as_of_count)


def _has_empty_mcp_business_result(collected_results: list[McpToolResult]) -> bool:
    return any(
        _is_business_source_result(result)
        and not _is_external_research_result(result)
        and _is_empty_success_result(result)
        for result in collected_results
    )


def _has_empty_external_research_result(collected_results: list[McpToolResult]) -> bool:
    return any(_is_external_research_result(result) and _is_empty_success_result(result) for result in collected_results)


def _has_external_research_call_or_result(calls: list[McpToolCall], results: list[McpToolResult]) -> bool:
    return any(call.server_key == EXTERNAL_RESEARCH_SERVER_KEY and call.tool_name == EXTERNAL_RESEARCH_WEB_TOOL for call in calls) or any(
        _is_external_research_result(result) for result in results
    )


def _is_information_query(config: ReactGroundingConfig) -> bool:
    return _contains_any(config.user_message, INFORMATION_QUERY_TERMS)


def _external_research_web_fallback_call(config: ReactGroundingConfig) -> McpToolCall:
    query = config.user_message.strip() or "external research"
    return McpToolCall(
        server_key=EXTERNAL_RESEARCH_SERVER_KEY,
        tool_name=EXTERNAL_RESEARCH_WEB_TOOL,
        payload_json={"query": query, "locale": "zh-CN", "limit": 3},
        stable_call_id="fallback:aistock-external-research:external_research_search_web",
        reason="deterministic_external_research_after_empty_mcp",
        risk_level="low",
        side_effect_level="read_only",
    )


def _should_force_external_research(
    *,
    config: ReactGroundingConfig,
    collected_calls: list[McpToolCall],
    collected_results: list[McpToolResult],
) -> bool:
    if not _is_information_query(config):
        return False
    if _has_business_evidence(collected_results):
        return False
    if not _has_empty_mcp_business_result(collected_results):
        return False
    return not _has_external_research_call_or_result(collected_calls, collected_results)


def _attempted_source_names(collected_calls: list[McpToolCall], collected_results: list[McpToolResult]) -> list[str]:
    attempted = [f"{call.server_key}/{call.tool_name}" for call in collected_calls if call.server_key and call.tool_name]
    attempted.extend(f"{result.server_key}/{result.tool_name}" for result in collected_results if result.server_key and result.tool_name)
    return _dedupe_preserve_order(attempted)


def _render_no_data_source_reply(
    *,
    collected_calls: list[McpToolCall],
    collected_results: list[McpToolResult],
    config: ReactGroundingConfig,
) -> str:
    attempted = _attempted_source_names(collected_calls, collected_results)
    attempted_text = ", ".join(attempted) if attempted else "none"
    query = config.user_message.strip() or "the requested information"
    return (
        "没有对应数据源：已按信息查询兜底流程尝试已路由 MCP 只读工具和 external_research，"
        f"但没有返回可用于回答 `{query}` 的证据。"
        f"尝试过的来源：{attempted_text}。reason_code=no_data_source_after_mcp_and_external_research"
    )


def _no_data_source_guard(
    *,
    collected_calls: list[McpToolCall],
    collected_results: list[McpToolResult],
    config: ReactGroundingConfig,
) -> EvidenceGuardDecision:
    return EvidenceGuardDecision(
        False,
        _render_no_data_source_reply(collected_calls=collected_calls, collected_results=collected_results, config=config),
        "no_data_source_after_mcp_and_external_research",
        sum(1 for item in collected_results if _result_has_evidence_items(item) and item.source_refs),
        sum(1 for item in collected_results if _result_has_evidence_items(item) and item.as_of),
    )


def _classify_exception_reason(exc: BaseException, call: McpToolCall) -> str:
    message = str(exc)
    lowered = message.lower()
    if isinstance(exc, KeyError) and "approved capability not found for tool" in lowered:
        return "capability_not_found"
    unavailable_terms = (
        "connection refused",
        "connection reset",
        "timed out",
        "timeout",
        "unavailable",
        "offline",
        "database is locked",
        "could not connect",
    )
    if call.server_key == "aistock-local-data" and any(term in lowered for term in unavailable_terms):
        return "data_source_unavailable"
    return "tool_execution_error"


def exception_result(call: McpToolCall, exc: BaseException, *, stage: str) -> McpToolResult:
    reason_code = _classify_exception_reason(exc, call)
    error = {
        "reason_code": reason_code,
        "code": reason_code,
        "stage": stage,
        "server_key": call.server_key,
        "tool_name": call.tool_name,
        "exception_type": type(exc).__name__,
        "message": str(exc),
    }
    return McpToolResult(
        server_key=call.server_key,
        tool_name=call.tool_name,
        status="failed",
        summary=_render_program_error_summary(error),
        error_json=error,
        executed=False,
        blocked_reason=reason_code,
        stable_call_id=call.stable_call_id,
        side_effect_level=_call_side_effect_level(call),
    )


def _program_error_results(results: list[McpToolResult]) -> list[McpToolResult]:
    program_errors: list[McpToolResult] = []
    for index, result in enumerate(results):
        error = result.error_json if isinstance(result.error_json, dict) else {}
        reason_code = str(error.get("reason_code") or error.get("code") or result.blocked_reason or "")
        later_success = any(
            item.executed and item.status in {"succeeded", "success", "ok"}
            for item in results[index + 1 :]
        )
        if later_success and error.get("recoverable_catalog_rejection"):
            continue
        if result.status in {"failed", "rejected"} and reason_code in PROGRAM_ERROR_REASON_CODES:
            program_errors.append(result)
    return program_errors


def _render_program_error_summary(error: dict[str, Any]) -> str:
    reason_code = str(error.get("reason_code") or error.get("code") or "tool_execution_error")
    route = f"{error.get('server_key')}/{error.get('tool_name')}"
    exc_type = str(error.get("exception_type") or error.get("error_type") or "Error")
    message = str(error.get("message") or error.get("human_reason") or error.get("error_summary") or "")
    catalog_reason = str(error.get("catalog_reason") or "")
    catalog_fragment = f"catalog_reason={catalog_reason}; " if catalog_reason else ""
    return (
        "工具调用失败："
        f"reason_code={reason_code}; {catalog_fragment}tool={route}; exception_type={exc_type}; "
        f"error_summary={message}"
    ).strip()


def _render_program_error_reply(results: list[McpToolResult]) -> str:
    lines = ["本轮无法完成工具取证，因为检测到真实工具/能力错误（不是 evidence insufficient）："]
    for result in results[:3]:
        error = result.error_json if isinstance(result.error_json, dict) else {}
        if not error:
            error = {
                "reason_code": result.blocked_reason or "tool_execution_error",
                "server_key": result.server_key,
                "tool_name": result.tool_name,
                "message": result.summary,
            }
        lines.append(f"- {_render_program_error_summary(error)}")
    if len(results) > 3:
        lines.append(f"- 另有 {len(results) - 3} 个工具错误，详见本轮诊断卡。")
    return "\n".join(lines)


def _text_reports_program_error(text: str, results: list[McpToolResult]) -> bool:
    lowered = text.lower()
    if "reason_code" not in lowered:
        return False
    for result in results:
        error = result.error_json if isinstance(result.error_json, dict) else {}
        reason_code = str(error.get("reason_code") or error.get("code") or result.blocked_reason or "")
        if reason_code and reason_code.lower() in lowered:
            return True
    return False


TERMINAL_SUMMARY_RESPONSE_MODES = {"local_data_daily_sync_status", "stock_analysis_evidence_card"}


def tool_result_message(result: McpToolResult) -> dict[str, Any]:
    content = {
        "type": "TOOL_RESULT",
        "server_key": result.server_key,
        "tool_name": result.tool_name,
        "status": result.status,
        "side_effect_level": result.side_effect_level,
        "summary": result.summary,
        "source_refs": result.source_refs[:8],
        "as_of": result.as_of,
        "artifact_refs": result.artifact_refs[:8],
        "tool_event_id": result.tool_event_id,
        "action_proposal_id": result.action_proposal_id,
        "executed": result.executed,
        "blocked_reason": result.blocked_reason,
        "payload": _compact_payload(result.payload_json) if isinstance(result.payload_json, dict) else {},
    }
    citation_options = _evidence_citation_inventory([result])
    if citation_options:
        content["citation_options"] = citation_options[:8]
    if result.preflight:
        content["preflight"] = _compact_payload(result.preflight, max_chars=900)
    if result.error_json:
        content["error"] = _compact_payload(result.error_json, max_chars=500)
    # This ReAct loop uses JSON tool choices, not provider-native tool calls.
    # Provider-native `role=tool` messages require a matching `tool_call_id`;
    # send observations as ordinary user-visible context instead.
    return {"role": "user", "content": json.dumps(content, ensure_ascii=False, sort_keys=True)}


def _contains_placeholder(text: str, patterns: tuple[str, ...]) -> str | None:
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return pattern
    return None


def _contains_forbidden_marker(text: str, markers: tuple[str, ...]) -> str | None:
    lowered = text.lower()
    for marker in markers:
        marker_text = str(marker or "").strip()
        if marker_text and marker_text.lower() in lowered:
            return marker_text
    return None


def _has_numeric_fact(text: str) -> bool:
    return _NUMBER_RE.search(text) is not None


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _payload_section_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sections = payload.get("sections") if isinstance(payload.get("sections"), list) else []
    return [item for item in sections if isinstance(item, dict)]


def _payload_source_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("source", "evidence_ref", "url"):
        if payload.get(key):
            values.append(str(payload[key]))
    for key in ("source_refs", "evidence_sources", "evidence_refs"):
        items = payload.get(key) if isinstance(payload.get(key), list) else []
        values.extend(str(item) for item in items if str(item or "").strip())
    if payload.get("graph_context") or payload.get("response_mode") == "graph_context":
        values.append("graph_context")
    return _dedupe_preserve_order(values)


def _iter_nested_dicts(value: Any) -> list[dict[str, Any]]:
    nested: list[dict[str, Any]] = []
    if isinstance(value, dict):
        nested.append(value)
        for item in value.values():
            nested.extend(_iter_nested_dicts(item))
    elif isinstance(value, list):
        for item in value:
            nested.extend(_iter_nested_dicts(item))
    return nested


def _payload_as_of_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("as_of", "trade_date", "analysis_date", "report_period", "date", "indicator_date"):
        if payload.get(key):
            values.append(str(payload[key]))
    if payload.get("graph_context") or payload.get("response_mode") == "graph_context":
        values.append(str(payload.get("as_of") or "LIVE"))
    return _dedupe_preserve_order(values)


def _known_source_values(collected_results: list[McpToolResult]) -> set[str]:
    values: list[str] = []
    for item in collected_results:
        values.extend(str(source) for source in item.source_refs if str(source or "").strip())
        payload = item.payload_json if isinstance(item.payload_json, dict) else {}
        values.extend(_payload_source_values(payload))
        for section in _payload_section_dicts(payload):
            values.extend(_payload_source_values(section))
        for nested in _iter_nested_dicts(payload):
            values.extend(_payload_source_values(nested))
    return set(_dedupe_preserve_order(values))


def _known_as_of_values(collected_results: list[McpToolResult]) -> set[str]:
    values: list[str] = []
    for item in collected_results:
        if item.as_of:
            values.append(str(item.as_of))
        payload = item.payload_json if isinstance(item.payload_json, dict) else {}
        values.extend(_payload_as_of_values(payload))
        for section in _payload_section_dicts(payload):
            values.extend(_payload_as_of_values(section))
        for nested in _iter_nested_dicts(payload):
            values.extend(_payload_as_of_values(nested))
    return set(_dedupe_preserve_order(values))


def _has_inline_source(text: str, known_sources: set[str]) -> bool:
    lower = text.lower()
    return any(source and source.lower() in lower for source in known_sources)


def _has_inline_as_of(text: str, known_as_of: set[str]) -> bool:
    lower = text.lower()
    return any(value and value.lower() in lower for value in known_as_of)


def _citation_pairs_for_result(result: McpToolResult) -> list[dict[str, str]]:
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    payload_dates = _payload_as_of_values(payload)
    result_dates = _dedupe_preserve_order([str(result.as_of or ""), *payload_dates])
    pairs: list[dict[str, str]] = []
    if payload.get("graph_context") or payload.get("response_mode") == "graph_context":
        graph_date = str(payload.get("as_of") or result.as_of or "LIVE")
        pairs.append({"server_key": result.server_key, "tool_name": result.tool_name, "source": "graph_context", "as_of": graph_date})
    for source in _dedupe_preserve_order([*result.source_refs, *_payload_source_values(payload)]):
        for as_of in result_dates[:1]:
            pairs.append({"server_key": result.server_key, "tool_name": result.tool_name, "source": source, "as_of": as_of})
    for section in _payload_section_dicts(payload):
        section_dates = _payload_as_of_values(section) or result_dates
        section_sources = _payload_source_values(section)
        dataset = str(section.get("dataset") or "").strip()
        for source in section_sources:
            for as_of in section_dates[:1]:
                item = {"server_key": result.server_key, "tool_name": result.tool_name, "source": source, "as_of": as_of}
                if dataset:
                    item["dataset"] = dataset
                pairs.append(item)
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pair in pairs:
        key = (pair["source"], pair["as_of"])
        if not pair["source"] or not pair["as_of"] or key in seen:
            continue
        seen.add(key)
        deduped.append(pair)
    return deduped


def _evidence_citation_inventory(collected_results: list[McpToolResult]) -> list[dict[str, str]]:
    inventory: list[dict[str, str]] = []
    for result in collected_results:
        inventory.extend(_citation_pairs_for_result(result))
    return inventory


def _evidence_citation_suffix(collected_results: list[McpToolResult]) -> str | None:
    citation_pairs = _evidence_citation_inventory(collected_results)
    if citation_pairs:
        fragments = [f"来源 {pair['source']}，截至 {pair['as_of']}" for pair in citation_pairs[:4]]
        return "；".join(fragments) + "。"
    return None


def _append_missing_evidence_citation(text: str, collected_results: list[McpToolResult]) -> str | None:
    suffix = _evidence_citation_suffix(collected_results)
    if not suffix:
        return None
    stripped = text.rstrip()
    if suffix in stripped:
        return stripped
    separator = "" if stripped.endswith(("。", ".", "！", "!", "？", "?")) else "。"
    return f"{stripped}{separator}{suffix}"


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(term.lower() in lowered for term in terms)


def _is_factual_list_query(config: ReactGroundingConfig) -> bool:
    question = str(config.user_message or "")
    lowered = question.lower()
    if re.search(r"\btop\s*\d+\b", lowered) or re.search(r"\btop\s*n\b", lowered):
        return True
    if re.search(r"\u524d\s*\d+\s*(?:\u4f4d|\u540d|\u4e2a|\u6761|loop|run)?", question):
        return True
    has_list_cue = _contains_any(question, FACTUAL_LIST_QUERY_TERMS)
    has_lookup_cue = _contains_any(question, FACTUAL_LOOKUP_QUERY_TERMS)
    if has_list_cue and has_lookup_cue:
        return True
    if _contains_any(question, JUDGEMENT_SYNTHESIS_QUERY_TERMS):
        return False
    return has_list_cue


def _is_future_question(config: ReactGroundingConfig) -> bool:
    return _contains_any(config.user_message, config.future_answer_terms)


def _passes_future_answer_discipline(text: str, config: ReactGroundingConfig) -> bool:
    if not _is_future_question(config):
        return True
    lowered = text.lower()
    answer_mentions_future = _contains_any(
        text,
        ("未来", "预测", "预判", "会涨", "会跌", "上涨", "下跌", "forecast", "predict", "outlook", *config.future_directional_markers),
    )
    if not answer_mentions_future:
        return True
    if any(marker.lower() in lowered for marker in config.future_directional_markers):
        return False
    matched = sum(1 for term in config.future_required_terms if term.lower() in lowered)
    no_prediction_boundary = any(term in lowered for term in ("不预测", "不做方向预测", "不构成投资建议", "not predict", "not investment advice"))
    return matched >= 3 and no_prediction_boundary


def _requires_synthesis_answer(config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> bool:
    if _is_factual_list_query(config):
        return False
    executed_tool_keys = {
        (item.server_key, item.tool_name)
        for item in collected_results
        if item.executed and (item.server_key, item.tool_name) != ("research-assistant", "graph_context")
    }
    if len(executed_tool_keys) >= 2 and not any(item.status in {"failed", "rejected"} for item in collected_results):
        return True
    return _contains_any(config.user_message, ("综合", "关系", "怎么用", "如何用", "怎么利用", "路径", "链路", "synthesize", "multi-source"))


def _looks_like_source_listing(text: str, collected_results: list[McpToolResult]) -> bool:
    executed_tool_keys = {
        (item.server_key, item.tool_name)
        for item in collected_results
        if item.executed and (item.server_key, item.tool_name) != ("research-assistant", "graph_context")
    }
    if len(executed_tool_keys) < 2:
        return False
    lowered = text.lower()
    tool_mentions = sum(1 for item in collected_results if item.tool_name and item.tool_name.lower() in lowered)
    section_markers = sum(1 for marker in ("来源1", "来源2", "来源 1", "来源 2", "source 1", "source 2", "工具1", "工具2", "tool 1", "tool 2", "第一项", "第二项") if marker in lowered)
    synthesis_terms = ("bottom-line", "结论", "综合", "意味着", "优先", "路径", "下一步", "判断")
    return (tool_mentions >= 2 or section_markers >= 2) and not any(term in lowered for term in synthesis_terms)


def _passes_multi_source_synthesis(text: str, config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> bool:
    if not _requires_synthesis_answer(config, collected_results):
        return True
    if _looks_like_source_listing(text, collected_results):
        return False
    lowered = text.lower()
    if any(term in lowered for term in ("bottom-line", "结论", "综合", "意味着", "路径", "下一步", "判断", "可以先", "优先")):
        return True
    return bool(_evidence_citation_inventory(collected_results))


def _factual_list_data_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith(("source:", "sources:", "as_of:", "as of:", "\u6765\u6e90", "\u622a\u81f3")):
            continue
        if set(line.replace("|", "").replace(" ", "").replace(":", "")) <= {"-"}:
            continue
        table_or_list = "|" in line or re.match(r"^(?:[-*]|\d+[\).、])\s+", line) is not None
        if table_or_list and _has_numeric_fact(line):
            lines.append(line)
    return lines


def _passes_factual_list_row_citations(text: str, config: ReactGroundingConfig, known_sources: set[str], known_as_of: set[str]) -> bool:
    if not _is_factual_list_query(config):
        return True
    data_lines = _factual_list_data_lines(text)
    if not data_lines:
        return True
    return all(_has_inline_source(line, known_sources) and _has_inline_as_of(line, known_as_of) for line in data_lines)


def _result_contains_unverified_value(result: McpToolResult) -> bool:
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    values = _iter_string_values(payload)
    values.extend(str(item) for item in (result.summary, result.observation) if str(item or "").strip())
    lowered = " ".join(values).lower()
    return any(term in lowered for term in UNVERIFIED_VALUE_TERMS)


def _has_unverified_evidence(collected_results: list[McpToolResult]) -> bool:
    return any(_is_success_result(result) and _result_contains_unverified_value(result) for result in collected_results)


def _mentions_unverified_risk(text: str) -> bool:
    lowered = text.lower()
    has_unverified_marker = any(term in lowered for term in UNVERIFIED_VALUE_TERMS)
    has_risk_marker = any(term in lowered for term in UNVERIFIED_RISK_TERMS)
    return has_unverified_marker and has_risk_marker


def _passes_unverified_risk_labels(text: str) -> bool:
    if not _mentions_unverified_risk(text):
        return False
    unverified_lines = [line.strip() for line in text.splitlines() if any(term in line.lower() for term in UNVERIFIED_VALUE_TERMS)]
    if not unverified_lines:
        return True
    return all(any(term in line.lower() for term in UNVERIFIED_RISK_TERMS) for line in unverified_lines)


def _status_counts_from_results(collected_results: list[McpToolResult]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for result in collected_results:
        payload = result.payload_json if isinstance(result.payload_json, dict) else {}
        counts = payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {}
        for key, value in counts.items():
            try:
                totals[str(key).lower()] = totals.get(str(key).lower(), 0) + int(value or 0)
            except (TypeError, ValueError):
                continue
    return totals


def _matches_running_focus(text: str, config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> bool:
    question = config.user_message
    if not _contains_any(question, ("running", "正在运行", "还在运行", "在运行", "运行中", "还在跑")):
        return True
    counts = _status_counts_from_results(collected_results)
    if not counts:
        return True
    running_count = counts.get("running", 0)
    if running_count == 0:
        return _contains_any(text, ("无正在运行", "没有正在运行", "暂无正在运行", "无运行中", "no running", "none are running", "0 running"))
    return _contains_any(text, ("running", "正在运行", "运行中"))


def _matches_completed_focus(text: str, config: ReactGroundingConfig, collected_results: list[McpToolResult]) -> bool:
    question = config.user_message
    if not _contains_any(question, ("completed", "已完成", "完成几个", "完成了几个", "完成数量", "多少完成")):
        return True
    counts = _status_counts_from_results(collected_results)
    if not counts or "completed" not in counts:
        return True
    return str(counts["completed"]) in text and _contains_any(text, ("completed", "已完成", "完成"))


def compose_with_evidence_guard(answer_text: str, collected_results: list[McpToolResult], config: ReactGroundingConfig) -> EvidenceGuardDecision:
    text = strip_internal_chain(answer_text).strip()
    known_sources = _known_source_values(collected_results)
    known_as_of = _known_as_of_values(collected_results)
    source_count = len(known_sources)
    as_of_count = len(known_as_of)
    program_errors = _program_error_results(collected_results)
    placeholder = _contains_placeholder(text, config.placeholder_patterns)
    if placeholder:
        decision = EvidenceGuardDecision(False, "Insufficient evidence: placeholder tokens are not allowed in factual answers.", f"placeholder_blocked:{placeholder}", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    forbidden_marker = _contains_forbidden_marker(text, config.forbidden_answer_markers)
    if forbidden_marker:
        decision = EvidenceGuardDecision(False, "Insufficient evidence: final answer matched a retired business template marker.", f"forbidden_answer_marker:{forbidden_marker}", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    has_numbers = _has_numeric_fact(text)
    inline_source = _has_inline_source(text, known_sources)
    inline_as_of = _has_inline_as_of(text, known_as_of)
    if config.evidence_required and collected_results and not (inline_source and inline_as_of):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: tool-grounded answers require inline source/as_of.", "missing_inline_tool_evidence", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _passes_factual_list_row_citations(text, config, known_sources, known_as_of):
        decision = EvidenceGuardDecision(
            False,
            "Insufficient evidence: factual ranking/list rows require inline source/as_of.",
            "factual_list_row_evidence_missing",
            source_count,
            as_of_count,
        )
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and has_numbers and not (inline_source and inline_as_of):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: numeric facts require inline source/as_of.", "unsourced_numeric_fact", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _matches_running_focus(text, config, collected_results):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: answer did not address the requested running-status focus.", "question_focus_mismatch", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _matches_completed_focus(text, config, collected_results):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: answer did not address the requested completed-status focus.", "question_focus_mismatch", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _passes_future_answer_discipline(text, config):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: future-looking answers require drivers, scenarios, risks, and no directional prediction.", "future_answer_boundary_missing", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and _has_unverified_evidence(collected_results) and not _passes_unverified_risk_labels(text):
        decision = EvidenceGuardDecision(
            False,
            "Insufficient evidence: unverified backtest or experimental data must carry an explicit risk label.",
            "unverified_evidence_risk_label_missing",
            source_count,
            as_of_count,
        )
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _passes_multi_source_synthesis(text, config, collected_results):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: multi-source answers must synthesize a judgement instead of listing tool outputs.", "multi_source_synthesis_missing", source_count, as_of_count)
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and not _passes_stock_depth_required_evidence(config, collected_results):
        decision = EvidenceGuardDecision(
            False,
            _render_stock_depth_missing_evidence_reply(config, collected_results),
            "stock_depth_required_evidence_missing",
            source_count,
            as_of_count,
        )
        if program_errors and _text_reports_program_error(text, program_errors):
            return EvidenceGuardDecision(True, text, "explicit_tool_error", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    if config.evidence_required and collected_results and _contains_forbidden_marker(text, config.forbidden_answer_markers):
        decision = EvidenceGuardDecision(False, "Insufficient evidence: regenerated answer still matched a retired business template marker.", "post_guard_forbidden_answer_marker", source_count, as_of_count)
        if program_errors:
            return EvidenceGuardDecision(False, _render_program_error_reply(program_errors), "explicit_tool_error", source_count, as_of_count)
        return decision
    return EvidenceGuardDecision(True, text, "ok", source_count, as_of_count)


def strip_internal_chain(text: str) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        lowered = stripped.lower()
        if lowered.startswith(("thought:", "observation:", "reflexion:", "reflection:")):
            continue
        if "context pack" in lowered:
            continue
        if lowered.startswith("final answer:"):
            stripped = stripped.split(":", 1)[1].strip()
        lines.append(stripped)
    return "\n".join(line for line in lines if line).strip()


def rejection_result(call: McpToolCall, decision: ToolGateDecision) -> McpToolResult:
    if decision.reason == "tool_not_in_audited_catalog":
        error = {
            "reason_code": "capability_not_found",
            "code": "capability_not_found",
            "catalog_reason": "tool_not_in_audited_catalog",
            "stage": "catalog_gate",
            "server_key": call.server_key,
            "tool_name": call.tool_name,
            "exception_type": "KeyError",
            "message": f"approved capability not found for tool: {call.server_key}/{call.tool_name}; catalog_reason=tool_not_in_audited_catalog",
            "recoverable_catalog_rejection": True,
        }
        return McpToolResult(
            server_key=call.server_key,
            tool_name=call.tool_name,
            status="rejected",
            summary=_render_program_error_summary(error),
            error_json=error,
            executed=False,
            blocked_reason="capability_not_found",
            stable_call_id=call.stable_call_id,
            side_effect_level=_side_effect_level(call.side_effect_level or decision.side_effect_level),
        )
    return McpToolResult(
        server_key=call.server_key,
        tool_name=call.tool_name,
        status="rejected",
        summary=decision.reason,
        error_json={"code": decision.action, "reason_code": decision.reason, "reason": decision.reason},
        executed=False,
        blocked_reason=decision.reason,
        stable_call_id=call.stable_call_id,
        side_effect_level=_side_effect_level(call.side_effect_level or decision.side_effect_level),
    )


def _evidence_summary_fallback_text(result: McpToolResult) -> str:
    source = result.source_refs[0] if result.source_refs else "unknown"
    return (
        f"Tool-grounded summary for {result.server_key}/{result.tool_name}; "
        f"source={source} as_of={result.as_of} summary-first read-only route={result.server_key}/{result.tool_name}. "
        f"{result.summary}"
    ).strip()


def _is_terminal_summary_result(result: McpToolResult) -> bool:
    if _normalize_status(result.status) not in SUCCESS_STATUSES or not result.executed:
        return False
    if not result.source_refs or not result.as_of:
        return False
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    response_mode = str(payload.get("response_mode") or "")
    if response_mode in TERMINAL_SUMMARY_RESPONSE_MODES:
        return True
    if result.server_key == EXTERNAL_RESEARCH_SERVER_KEY:
        return bool(_result_has_evidence_items(result))
    return False


def _has_terminal_summary_evidence(collected_results: list[McpToolResult]) -> bool:
    return any(_is_terminal_summary_result(result) for result in collected_results)


def _retry_directive(results: list[McpToolResult]) -> dict[str, Any]:
    compact = [
        {
            "server_key": item.server_key,
            "tool_name": item.tool_name,
            "status": item.status,
            "reason": item.blocked_reason or item.summary or item.error_json.get("code"),
        }
        for item in results
        if item.status in {"failed", "rejected", "preflight_blocked", "preflight_required", "approval_required"}
    ]
    return {
        "role": "system",
        "content": json.dumps(
            {
                "type": "REACT_RETRY_DIRECTIVE",
                "instruction": "Previous tool attempt failed or needs different evidence. Choose another audited read-only tool or answer with evidence insufficiency.",
                "tool_results": compact,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    }


def _evidence_guard_retry_directive(guard: EvidenceGuardDecision, failed_answer: str, collected_results: list[McpToolResult]) -> dict[str, Any]:
    return {
        "role": "system",
        "content": json.dumps(
            {
                "type": "REACT_EVIDENCE_GUARD_REPAIR_DIRECTIVE",
                "reason_code": guard.reason,
                "failed_answer": failed_answer[:1800],
                "instruction": (
                    "Regenerate the final answer using only the existing TOOL_RESULT evidence. "
                    "Cite exact citation_options.source and citation_options.as_of strings; "
                    "for multi-section evidence cite the relevant section source/as_of token. "
                    "If reason_code is future_answer_boundary_missing, use drivers, scenarios, risks, and say you do not predict direction. "
                    "If reason_code is multi_source_synthesis_missing, give a bottom-line synthesis before details instead of listing tools. "
                    "If reason_code is factual_list_row_evidence_missing, add the exact source/as_of token to every factual list or table row. If reason_code is unverified_evidence_risk_label_missing, label every not_verified row as unverified backtest or experimental data and warn not to treat it as real returns. "
                    "Do not invent placeholders, dates, sources, or new facts. "
                    "If the evidence is genuinely insufficient, say so with the reason."
                ),
                "citation_options": _evidence_citation_inventory(collected_results)[:12],
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    }


def run_react_grounding_loop(
    *,
    messages: list[dict[str, Any]],
    model_complete: ModelComplete,
    mcp_provider: McpProvider,
    catalog_entries: list[ToolCatalogEntry],
    config: ReactGroundingConfig,
    seeded_tool_calls: list[McpToolCall] | None = None,
    initial_tool_results: list[McpToolResult] | None = None,
    fallback_tool_calls: Callable[[], list[McpToolCall]] | None = None,
    tool_result_compactor: ToolResultCompactor | None = None,
) -> ReactGroundingResult:
    working_messages = [dict(item) for item in messages]
    trace_steps: list[dict[str, Any]] = []
    collected_calls: list[McpToolCall] = []
    collected_results: list[McpToolResult] = []
    model_turns: list[ModelTurn] = []
    for result in list(initial_tool_results or []):
        collected_results.append(result)
        working_messages.append(tool_result_message(result))
    if collected_results:
        trace_steps.append({"iteration": 0, "preloaded_tool_result_count": len(collected_results)})
    pending_seeded = list(seeded_tool_calls or [])
    final_text = ""
    stopped_reason = "max_iterations_exhausted"
    last_guard: EvidenceGuardDecision | None = None

    for iteration in range(1, config.max_tool_iterations + 1):
        if config.token_budget is not None:
            rendered_messages = json.dumps(working_messages, ensure_ascii=False, sort_keys=True)
            if len(rendered_messages) // 4 > config.token_budget:
                stopped_reason = "token_budget_exhausted"
                break
        if pending_seeded:
            calls = pending_seeded
            pending_seeded = []
            turn = ModelTurn(content=json.dumps({"tool_calls": [call.__dict__ for call in calls]}, ensure_ascii=False), provider="route_seed", model="route_seed", duration_ms=0, usage={})
        else:
            turn = model_complete(working_messages)
            calls = list(turn.tool_calls or extract_structured_tool_calls(turn.content))
        model_turns.append(turn)
        trace_steps.append({"iteration": iteration, "model": turn.model, "tool_call_count": len(calls), "provider": turn.provider})
        if not calls:
            if _should_force_external_research(config=config, collected_calls=collected_calls, collected_results=collected_results):
                calls = [_external_research_web_fallback_call(config)]
                trace_steps.append(
                    {
                        "iteration": iteration,
                        "fallback": "external_research_after_empty_mcp",
                        "fallback_tool_call_count": 1,
                        "reason": "empty_mcp_result_for_information_query",
                    }
                )
            else:
                if (
                    _has_empty_external_research_result(collected_results)
                    and _is_information_query(config)
                    and not _has_business_evidence(collected_results)
                    and _has_empty_mcp_business_result(collected_results)
                ):
                    guard = _no_data_source_guard(collected_calls=collected_calls, collected_results=collected_results, config=config)
                    trace_steps.append({"iteration": iteration, "evidence_guard_allowed": guard.allowed, "evidence_guard_reason": guard.reason})
                    return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, iteration, "no_data_source", model_turns)
                calls = []

        if not calls:
            guard = compose_with_evidence_guard(turn.content, collected_results, config)
            last_guard = guard
            trace_steps.append({"iteration": iteration, "evidence_guard_allowed": guard.allowed, "evidence_guard_reason": guard.reason})
            citation_failure = guard.reason in {
                "missing_inline_tool_evidence",
                "unsourced_numeric_fact",
                "factual_list_row_evidence_missing",
                "future_answer_boundary_missing",
                "multi_source_synthesis_missing",
                "unverified_evidence_risk_label_missing",
            }
            if not guard.allowed and guard.reason == "stock_depth_required_evidence_missing":
                degraded_guard = _read_only_partial_evidence_degradation(
                    guard=guard,
                    candidate_text=turn.content,
                    collected_calls=collected_calls,
                    collected_results=collected_results,
                    config=config,
                )
                if degraded_guard is not None:
                    trace_steps.append(
                        {
                            "iteration": iteration,
                            "degradation": "read_only_partial_evidence",
                            "original_reason": guard.reason,
                        }
                    )
                    return ReactGroundingResult(
                        degraded_guard.text,
                        working_messages,
                        collected_calls,
                        collected_results,
                        trace_steps,
                        degraded_guard,
                        iteration,
                        "read_only_partial_evidence_degraded",
                        model_turns,
                    )
                stopped_reason = "stock_depth_required_evidence_missing"
                return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, iteration, stopped_reason, model_turns)
            if not guard.allowed and citation_failure and iteration < config.max_tool_iterations and _evidence_citation_inventory(collected_results):
                collected_results.append(
                    McpToolResult(
                        server_key="evidence_guard",
                        tool_name="compose_with_evidence_guard",
                        status="failed",
                        summary=guard.reason,
                        error_json={"code": guard.reason},
                        executed=False,
                        stable_call_id=f"guard_{iteration}",
                        side_effect_level="read_only",
                    )
                )
                working_messages.append(_evidence_guard_retry_directive(guard, strip_internal_chain(turn.content), collected_results))
                trace_steps.append({"iteration": iteration, "repair": "regenerate_with_evidence_citation_options", "reason": guard.reason})
                continue
            if not guard.allowed and citation_failure and _has_terminal_summary_evidence(collected_results):
                cited_text = _append_missing_evidence_citation(strip_internal_chain(turn.content), collected_results)
                if cited_text:
                    repaired_guard = compose_with_evidence_guard(cited_text, collected_results, config)
                    last_guard = repaired_guard
                    if repaired_guard.allowed:
                        final_text = repaired_guard.text
                        stopped_reason = "final_answer"
                        trace_steps.append({"iteration": iteration, "repair": "append_tool_evidence_citation_after_regeneration"})
                        return ReactGroundingResult(final_text, working_messages, collected_calls, collected_results, trace_steps, repaired_guard, iteration, stopped_reason, model_turns)
            if guard.allowed:
                final_text = guard.text
                stopped_reason = "final_answer"
                return ReactGroundingResult(final_text, working_messages, collected_calls, collected_results, trace_steps, guard, iteration, stopped_reason, model_turns)
            program_errors = _program_error_results(collected_results)
            if program_errors:
                explicit = _render_program_error_reply(program_errors)
                error_guard = EvidenceGuardDecision(False, explicit, "explicit_tool_error", sum(1 for item in collected_results if item.source_refs), sum(1 for item in collected_results if item.as_of))
                stopped_reason = "tool_error"
                return ReactGroundingResult(explicit, working_messages, collected_calls, collected_results, trace_steps, error_guard, iteration, stopped_reason, model_turns)
            fallback_calls = list(fallback_tool_calls() if fallback_tool_calls else [])
            already_called = {(call.server_key, call.tool_name) for call in collected_calls}
            fallback_calls = [call for call in fallback_calls if (call.server_key, call.tool_name) not in already_called]
            if fallback_calls:
                calls = fallback_calls
                trace_steps.append({"iteration": iteration, "fallback_tool_call_count": len(calls), "reason": guard.reason})
            elif _should_force_external_research(config=config, collected_calls=collected_calls, collected_results=collected_results):
                calls = [_external_research_web_fallback_call(config)]
                trace_steps.append(
                    {
                        "iteration": iteration,
                        "fallback": "external_research_after_empty_mcp",
                        "fallback_tool_call_count": 1,
                        "reason": "empty_mcp_result_for_information_query",
                    }
                )
            else:
                collected_results.append(
                    McpToolResult(
                        server_key="evidence_guard",
                        tool_name="compose_with_evidence_guard",
                        status="failed",
                        summary=guard.reason,
                        error_json={"code": guard.reason},
                        executed=False,
                        stable_call_id=f"guard_{iteration}",
                        side_effect_level="read_only",
                    )
                )
                working_messages.append(_retry_directive(collected_results[-1:]))
                continue

        iteration_results: list[McpToolResult] = []
        for call in calls:
            collected_calls.append(call)
            decision: ToolGateDecision | None = None
            try:
                decision = assert_tool_in_catalog(call, catalog_entries)
                if not decision.allowed:
                    result = rejection_result(call, decision)
                elif decision.action == "execute_read_only":
                    result = mcp_provider.execute_read_only(call, decision)
                elif decision.action == "preflight_confirmation_only":
                    result = mcp_provider.preflight_confirmation_only(call, decision)
                else:
                    result = rejection_result(call, decision)
            except Exception as exc:  # noqa: BLE001 - one tool failure must not abort the whole chat turn.
                logger.exception(
                    "research assistant ReAct tool failed: tool=%s/%s stage=tool_dispatch",
                    call.server_key,
                    call.tool_name,
                )
                result = exception_result(call, exc, stage="tool_dispatch")
            if tool_result_compactor is not None:
                try:
                    result = tool_result_compactor(result)
                except Exception as exc:  # noqa: BLE001 - report compaction failures explicitly instead of hiding them.
                    logger.exception(
                        "research assistant ReAct tool result compaction failed: tool=%s/%s",
                        call.server_key,
                        call.tool_name,
                    )
                    result = exception_result(call, exc, stage="tool_result_compaction")
                    result.error_json["reason_code"] = "tool_result_compaction_error"
                    result.error_json["code"] = "tool_result_compaction_error"
                    result.blocked_reason = "tool_result_compaction_error"
                    result.summary = _render_program_error_summary(result.error_json)
            result.stable_call_id = call.stable_call_id
            result.side_effect_level = _side_effect_level(
                (decision.side_effect_level if decision is not None else None)
                or call.side_effect_level
                or getattr(result, "side_effect_level", None)
            )
            iteration_results.append(result)
        if _is_stock_depth_query(config):
            iteration_results.sort(
                key=lambda item: (
                    0
                    if item.server_key == STOCK_ANALYSIS_SERVER_KEY
                    else 1
                    if item.server_key == EXTERNAL_RESEARCH_SERVER_KEY
                    else 2,
                    item.tool_name,
                    item.stable_call_id or "",
                )
            )
        else:
            iteration_results.sort(key=lambda item: item.sorted_key())
        for result in iteration_results:
            collected_results.append(result)
            working_messages.append(tool_result_message(result))
        if (
            iteration < config.max_tool_iterations
            and _should_force_external_research(config=config, collected_calls=collected_calls, collected_results=collected_results)
        ):
            pending_seeded = [_external_research_web_fallback_call(config)]
            trace_steps.append(
                {
                    "iteration": iteration,
                    "fallback": "external_research_after_empty_mcp",
                    "fallback_tool_call_count": 1,
                    "reason": "empty_mcp_result_for_information_query",
                }
            )
            continue
        if (
            _has_empty_external_research_result(iteration_results)
            and _is_information_query(config)
            and not _has_business_evidence(collected_results)
            and _has_empty_mcp_business_result(collected_results)
        ):
            guard = _no_data_source_guard(collected_calls=collected_calls, collected_results=collected_results, config=config)
            trace_steps.append({"iteration": iteration, "evidence_guard_allowed": guard.allowed, "evidence_guard_reason": guard.reason})
            return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, iteration, "no_data_source", model_turns)
        if any(item.status in {"failed", "rejected"} for item in iteration_results):
            working_messages.append(_retry_directive(iteration_results))

    program_errors = _program_error_results(collected_results)
    if final_text:
        guard = compose_with_evidence_guard(final_text, collected_results, config)
        if program_errors and (not guard.allowed or "insufficient evidence" in guard.text.lower()):
            explicit = _render_program_error_reply(program_errors)
            guard = EvidenceGuardDecision(False, explicit, "explicit_tool_error", sum(1 for item in collected_results if item.source_refs), sum(1 for item in collected_results if item.as_of))
            stopped_reason = "tool_error"
    elif program_errors:
        explicit = _render_program_error_reply(program_errors)
        guard = EvidenceGuardDecision(False, explicit, "explicit_tool_error", sum(1 for item in collected_results if item.source_refs), sum(1 for item in collected_results if item.as_of))
        stopped_reason = "tool_error"
    elif last_guard is not None:
        guard = last_guard
    else:
        guard = EvidenceGuardDecision(False, "Insufficient evidence: max tool iterations reached without reliable evidence.", "max_tool_iterations_exhausted", sum(1 for item in collected_results if item.source_refs), sum(1 for item in collected_results if item.as_of))
    if not guard.allowed and guard.reason in READ_ONLY_PARTIAL_EVIDENCE_REASON_CODES:
        degraded_guard = _read_only_partial_evidence_degradation(
            guard=guard,
            candidate_text=final_text,
            collected_calls=collected_calls,
            collected_results=collected_results,
            config=config,
        )
        if degraded_guard is not None:
            trace_steps.append(
                {
                    "iteration": config.max_tool_iterations,
                    "degradation": "read_only_partial_evidence",
                    "original_reason": guard.reason,
                }
            )
            guard = degraded_guard
            stopped_reason = "read_only_partial_evidence_degraded"
    return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, config.max_tool_iterations, stopped_reason, model_turns)
