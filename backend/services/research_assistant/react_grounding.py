"""Provider-only ReAct grounding core for Research Assistant.

The module is domain-neutral: it receives an audited tool catalog, a model
callable, and an MCP provider adapter from the host service. It never imports
AIstock service, repository, database, or domain modules.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


@dataclass(frozen=True)
class ReactGroundingConfig:
    max_tool_iterations: int
    evidence_required: bool = True
    placeholder_patterns: tuple[str, ...] = (r"\bXX\b", r"\bX%\b", r"approxX", r"about X")

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


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)
_TOOL_CHOICE_RE = re.compile(r"<assistant_tool_choice\b[^>]*>(.*?)</assistant_tool_choice>", re.IGNORECASE | re.DOTALL)
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9_])\d+(?:\.\d+)?\s*(?:%|days?|items?)?")


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
            "artifact_refs": (payload.get("artifact_refs") or [])[:3] if isinstance(payload.get("artifact_refs"), list) else [],
            "omitted_sections": payload.get("omitted_sections") or [],
        }
    return compact


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
    if result.preflight:
        content["preflight"] = _compact_payload(result.preflight, max_chars=900)
    if result.error_json:
        content["error"] = _compact_payload(result.error_json, max_chars=500)
    return {"role": "tool", "content": json.dumps(content, ensure_ascii=False, sort_keys=True)}


def _contains_placeholder(text: str, patterns: tuple[str, ...]) -> str | None:
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return pattern
    return None


def _has_numeric_fact(text: str) -> bool:
    return _NUMBER_RE.search(text) is not None


def compose_with_evidence_guard(answer_text: str, collected_results: list[McpToolResult], config: ReactGroundingConfig) -> EvidenceGuardDecision:
    text = strip_internal_chain(answer_text).strip()
    source_count = sum(1 for item in collected_results if item.source_refs)
    as_of_count = sum(1 for item in collected_results if item.as_of)
    placeholder = _contains_placeholder(text, config.placeholder_patterns)
    if placeholder:
        return EvidenceGuardDecision(False, "Insufficient evidence: placeholder tokens are not allowed in factual answers.", f"placeholder_blocked:{placeholder}", source_count, as_of_count)
    has_numbers = _has_numeric_fact(text)
    inline_source = bool(re.search(r"source\s*=|source_refs?", text, flags=re.IGNORECASE))
    inline_as_of = bool(re.search(r"as_of\s*=|trade_date\s*=|report_period\s*=", text, flags=re.IGNORECASE))
    if config.evidence_required and collected_results and not (inline_source and inline_as_of):
        return EvidenceGuardDecision(False, "Insufficient evidence: tool-grounded answers require inline source/as_of.", "missing_inline_tool_evidence", source_count, as_of_count)
    if config.evidence_required and has_numbers and not (inline_source and inline_as_of):
        return EvidenceGuardDecision(False, "Insufficient evidence: numeric facts require inline source/as_of.", "unsourced_numeric_fact", source_count, as_of_count)
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
    return McpToolResult(
        server_key=call.server_key,
        tool_name=call.tool_name,
        status="rejected",
        summary=decision.reason,
        error_json={"code": decision.action, "reason": decision.reason},
        executed=False,
        blocked_reason=decision.reason,
        stable_call_id=call.stable_call_id,
    )


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


def run_react_grounding_loop(
    *,
    messages: list[dict[str, Any]],
    model_complete: ModelComplete,
    mcp_provider: McpProvider,
    catalog_entries: list[ToolCatalogEntry],
    config: ReactGroundingConfig,
    seeded_tool_calls: list[McpToolCall] | None = None,
    fallback_tool_calls: Callable[[], list[McpToolCall]] | None = None,
) -> ReactGroundingResult:
    working_messages = [dict(item) for item in messages]
    trace_steps: list[dict[str, Any]] = []
    collected_calls: list[McpToolCall] = []
    collected_results: list[McpToolResult] = []
    model_turns: list[ModelTurn] = []
    pending_seeded = sorted(seeded_tool_calls or [], key=lambda call: call.sorted_key())
    final_text = ""
    stopped_reason = "max_iterations_exhausted"

    for iteration in range(1, config.max_tool_iterations + 1):
        if pending_seeded:
            calls = pending_seeded
            pending_seeded = []
            turn = ModelTurn(content=json.dumps({"tool_calls": [call.__dict__ for call in calls]}, ensure_ascii=False), provider="route_seed", model="route_seed", duration_ms=0, usage={})
        else:
            turn = model_complete(working_messages)
            calls = extract_structured_tool_calls(turn.content)
        model_turns.append(turn)
        trace_steps.append({"iteration": iteration, "model": turn.model, "tool_call_count": len(calls), "provider": turn.provider})
        if not calls:
            guard = compose_with_evidence_guard(turn.content, collected_results, config)
            if guard.allowed:
                final_text = guard.text
                stopped_reason = "final_answer"
                return ReactGroundingResult(final_text, working_messages, collected_calls, collected_results, trace_steps, guard, iteration, stopped_reason, model_turns)
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
            decision = assert_tool_in_catalog(call, catalog_entries)
            if not decision.allowed:
                result = rejection_result(call, decision)
            elif decision.action == "execute_read_only":
                result = mcp_provider.execute_read_only(call, decision)
            elif decision.action == "preflight_confirmation_only":
                result = mcp_provider.preflight_confirmation_only(call, decision)
            else:
                result = rejection_result(call, decision)
            result.stable_call_id = call.stable_call_id
            iteration_results.append(result)
        iteration_results.sort(key=lambda item: item.sorted_key())
        for result in iteration_results:
            collected_results.append(result)
            working_messages.append(tool_result_message(result))
        if any(item.status in {"failed", "rejected"} for item in iteration_results):
            working_messages.append(_retry_directive(iteration_results))

    if final_text:
        guard = compose_with_evidence_guard(final_text, collected_results, config)
    else:
        sourced = next((item for item in collected_results if item.source_refs and item.as_of), None)
        if sourced is not None:
            fallback_text = (
                f"Tool-grounded summary for {sourced.server_key}/{sourced.tool_name}; "
                f"source={sourced.source_refs[0]} as_of={sourced.as_of} summary-first read-only route={sourced.server_key}/{sourced.tool_name}. "
                f"{sourced.summary}"
            ).strip()
            guard = compose_with_evidence_guard(fallback_text, collected_results, config)
            stopped_reason = "evidence_summary_fallback"
        else:
            guard = EvidenceGuardDecision(False, "Insufficient evidence: max tool iterations reached without reliable evidence.", "max_tool_iterations_exhausted", sum(1 for item in collected_results if item.source_refs), sum(1 for item in collected_results if item.as_of))
    return ReactGroundingResult(guard.text, working_messages, collected_calls, collected_results, trace_steps, guard, config.max_tool_iterations, stopped_reason, model_turns)
