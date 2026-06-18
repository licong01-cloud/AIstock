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


logger = logging.getLogger("aistock.research_assistant.react_grounding")


@dataclass(frozen=True)
class ReactGroundingConfig:
    max_tool_iterations: int
    evidence_required: bool = True
    user_message: str = ""
    token_budget: int | None = None
    placeholder_patterns: tuple[str, ...] = (r"\bXX\b", r"\bX%\b", r"approxX", r"about X")
    forbidden_answer_markers: tuple[str, ...] = ()

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
        return ToolGateDecision(False, "reject_catalog", f"tool_status_{entry.status}", catalog_entry=entry)
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
    return _dedupe_preserve_order(values)


def _payload_as_of_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("as_of", "trade_date", "analysis_date", "report_period", "date", "indicator_date"):
        if payload.get(key):
            values.append(str(payload[key]))
    return _dedupe_preserve_order(values)


def _known_source_values(collected_results: list[McpToolResult]) -> set[str]:
    values: list[str] = []
    for item in collected_results:
        values.extend(str(source) for source in item.source_refs if str(source or "").strip())
        payload = item.payload_json if isinstance(item.payload_json, dict) else {}
        values.extend(_payload_source_values(payload))
        for section in _payload_section_dicts(payload):
            values.extend(_payload_source_values(section))
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
    )


def _evidence_summary_fallback_text(result: McpToolResult) -> str:
    source = result.source_refs[0] if result.source_refs else "unknown"
    return (
        f"Tool-grounded summary for {result.server_key}/{result.tool_name}; "
        f"source={source} as_of={result.as_of} summary-first read-only route={result.server_key}/{result.tool_name}. "
        f"{result.summary}"
    ).strip()


def _is_terminal_summary_result(result: McpToolResult) -> bool:
    if result.status not in {"succeeded", "success", "ok"} or not result.executed:
        return False
    if not result.source_refs or not result.as_of:
        return False
    payload = result.payload_json if isinstance(result.payload_json, dict) else {}
    return str(payload.get("response_mode") or "") in TERMINAL_SUMMARY_RESPONSE_MODES


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
    fallback_tool_calls: Callable[[], list[McpToolCall]] | None = None,
    tool_result_compactor: ToolResultCompactor | None = None,
) -> ReactGroundingResult:
    working_messages = [dict(item) for item in messages]
    trace_steps: list[dict[str, Any]] = []
    collected_calls: list[McpToolCall] = []
    collected_results: list[McpToolResult] = []
    model_turns: list[ModelTurn] = []
    pending_seeded = sorted(seeded_tool_calls or [], key=lambda call: call.sorted_key())
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
            calls = sorted((turn.tool_calls or extract_structured_tool_calls(turn.content)), key=lambda call: call.sorted_key())
        model_turns.append(turn)
        trace_steps.append({"iteration": iteration, "model": turn.model, "tool_call_count": len(calls), "provider": turn.provider})
        if not calls:
            guard = compose_with_evidence_guard(turn.content, collected_results, config)
            last_guard = guard
            trace_steps.append({"iteration": iteration, "evidence_guard_allowed": guard.allowed, "evidence_guard_reason": guard.reason})
            citation_failure = guard.reason in {"missing_inline_tool_evidence", "unsourced_numeric_fact"}
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
            fallback_calls = sorted((fallback_tool_calls() if fallback_tool_calls else []), key=lambda call: call.sorted_key())
            already_called = {(call.server_key, call.tool_name) for call in collected_calls}
            fallback_calls = [call for call in fallback_calls if (call.server_key, call.tool_name) not in already_called]
            if fallback_calls:
                calls = fallback_calls
                trace_steps.append({"iteration": iteration, "fallback_tool_call_count": len(calls), "reason": guard.reason})
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
                    )
                )
                working_messages.append(_retry_directive(collected_results[-1:]))
                continue

        iteration_results: list[McpToolResult] = []
        for call in sorted(calls, key=lambda item: item.sorted_key()):
            collected_calls.append(call)
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
            iteration_results.append(result)
        iteration_results.sort(key=lambda item: item.sorted_key())
        for result in iteration_results:
            collected_results.append(result)
            working_messages.append(tool_result_message(result))
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
    return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, config.max_tool_iterations, stopped_reason, model_turns)
