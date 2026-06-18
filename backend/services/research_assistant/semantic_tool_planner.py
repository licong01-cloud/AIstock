"""LLM-backed semantic MCP tool planner for Research Assistant.

This module intentionally avoids routing by user-message keyword lists. It asks
the configured model to interpret the request against the audited MCP catalog
and returns a validated structured route frame for the service to execute.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Protocol

from .domain_ontology import DOMAIN_SPECS, McpDomain, spec_for_domain


class ToolPlanningLlmClient(Protocol):
    def complete_tool_plan(
        self,
        *,
        messages: list[dict[str, str]],
        model_profile: dict[str, Any],
        temperature: float,
        max_tokens: int,
    ) -> Any:
        ...


@dataclass(frozen=True)
class SemanticToolPlan:
    status: str
    domain: str = "general"
    server_key: str | None = None
    tool_name: str | None = None
    tool_args: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    reason: str = ""
    requires_clarification: bool = False
    clarification_questions: tuple[str, ...] = ()
    raw_response: str = ""

    def to_route(self) -> dict[str, Any]:
        route: dict[str, Any] = {
            "domain": self.domain,
            "server_key": self.server_key,
            "tool_name": self.tool_name,
            "reason": self.reason,
            "confidence": self.confidence,
            "planner_source": "llm_semantic_tool_planner",
            "semantic_status": self.status,
            "matched_terms": [],
        }
        if self.requires_clarification:
            route.update(
                {
                    "policy": "semantic_clarification_required",
                    "requires_clarification": True,
                    "clarification_questions": list(self.clarification_questions),
                    "side_effect": "none",
                }
            )
            return route
        if self.status == "no_tool":
            route.update(
                {
                    "domain": McpDomain.GENERAL.value,
                    "server_key": None,
                    "tool_name": None,
                    "policy": "semantic_no_tool",
                    "side_effect": "none",
                }
            )
            return route
        if self.tool_args:
            route["tool_args"] = dict(self.tool_args)
            if "limit" in self.tool_args:
                route["limit"] = self.tool_args["limit"]
        if self.domain != McpDomain.GENERAL.value:
            try:
                spec = spec_for_domain(self.domain)
            except ValueError:
                spec = None
            if spec is not None:
                route["intent_value"] = spec.intent_value
                route["policy"] = spec.risk_policy
                route["read_tools"] = list(spec.read_tools)
                route["plan_tools"] = list(spec.plan_tools)
                route["confirmed_tools"] = list(spec.confirmed_tools)
                if self.tool_name in spec.plan_tools:
                    route["side_effect"] = "plan_or_preflight"
                elif self.tool_name in spec.confirmed_tools:
                    route["side_effect"] = "confirmed_action"
                else:
                    route["side_effect"] = "read_only"
        return route


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.IGNORECASE | re.DOTALL)


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = text.strip()
    candidates = [raw]
    candidates.extend(match.group(1).strip() for match in _JSON_FENCE_RE.finditer(text))
    for candidate in candidates:
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def _domain_for_tool(tool_name: str) -> str | None:
    for spec in DOMAIN_SPECS.values():
        if tool_name in {*spec.read_tools, *spec.plan_tools, *spec.confirmed_tools}:
            return spec.domain.value
    return None


def _clamp_confidence(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, numeric))


def _compact_tool_catalog(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for tool in tools:
        if str(tool.get("status") or "") not in {"enabled", "ready", "approved"}:
            continue
        input_schema = tool.get("input_schema_json") if isinstance(tool.get("input_schema_json"), dict) else {"type": "object"}
        compact.append(
            {
                "server_key": str(tool.get("server_key") or ""),
                "tool_name": str(tool.get("tool_name") or ""),
                "module": str(tool.get("module") or ""),
                "title": str(tool.get("title") or tool.get("tool_name") or ""),
                "description": str(tool.get("description") or ""),
                "risk_level": str(tool.get("risk_level") or ""),
                "side_effect_level": str(tool.get("side_effect_level") or ""),
                "requires_approval": bool(tool.get("requires_approval")),
                "input_schema": input_schema,
            }
        )
    return [item for item in compact if item["server_key"] and item["tool_name"]]


class SemanticToolPlanner:
    """Ask the model to select an MCP route from the audited tool catalog."""

    def __init__(self, llm_client: Any) -> None:
        self.llm_client = llm_client

    def available(self) -> bool:
        return callable(getattr(self.llm_client, "complete_tool_plan", None))

    def plan(
        self,
        *,
        user_message: str,
        model_profile: dict[str, Any],
        tool_catalog: list[dict[str, Any]],
        max_tokens: int = 900,
    ) -> SemanticToolPlan | None:
        complete_tool_plan = getattr(self.llm_client, "complete_tool_plan", None)
        if not callable(complete_tool_plan):
            return None
        compact_catalog = _compact_tool_catalog(tool_catalog)
        if not compact_catalog:
            return None
        messages = self._messages(user_message=user_message, tool_catalog=compact_catalog)
        result = complete_tool_plan(
            messages=messages,
            model_profile=model_profile,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        raw = str(getattr(result, "content", result) or "").strip()
        payload = _parse_json_object(raw)
        if payload is None:
            return None
        return self._validated_plan(payload, raw_response=raw, tool_catalog=compact_catalog)

    @staticmethod
    def _messages(*, user_message: str, tool_catalog: list[dict[str, Any]]) -> list[dict[str, str]]:
        business_domains = [
            {
                "domain": spec.domain.value,
                "intent_value": spec.intent_value,
                "server_key": spec.server_key,
                "summary": spec.summary_zh,
                "read_tools": list(spec.read_tools),
                "plan_tools": list(spec.plan_tools),
                "confirmed_tools": list(spec.confirmed_tools),
            }
            for spec in DOMAIN_SPECS.values()
            if spec.domain != McpDomain.GENERAL
        ]
        system_payload = {
            "instruction": (
                "You are the Research Assistant semantic MCP planner. Interpret the user's real intent, "
                "choose one audited MCP tool only when the requested outcome is clear, and ask a concise "
                "clarifying question when the comparison object, metric, or action boundary is underspecified. "
                "Do not use keyword matching or synonym lists; reason from the business meaning and the tool catalog."
            ),
            "response_schema": {
                "status": "tool_plan | clarification | no_tool",
                "domain": "domain string such as qe_warehouse or local_data",
                "server_key": "audited MCP server key when status is tool_plan",
                "tool_name": "audited MCP tool name when status is tool_plan",
                "tool_args": "object with safe read-only filters from input_schema, such as status, state, symbol, ts_code, analysis_date, trade_date, period, limit, offset, or order_by",
                "confidence": "0.0-1.0",
                "reason": "short business reason",
                "clarification_questions": "array of concise questions when status is clarification",
            },
            "planning_rules": [
                "Never invent a tool; pick only from audited_tools.",
                "Never choose confirmed/write tools for an unclear or read-only question.",
                "When the user asks for a comparison but the metric is not explicit enough to select a tool argument safely, return clarification.",
                "Prefer a read-only summary or analytics tool for factual questions that can be answered from existing warehouse/catalog data.",
                "When the user asks for running/completed/created/failed records, include status/state filters only when the selected tool schema supports them; otherwise rely on final synthesis to filter the returned evidence.",
                "For individual-stock evidence-card requests, include the stock code or symbol in tool_args.symbol and prefer a read-only stock_analysis tool.",
                "Keep the final response as a single JSON object and no prose.",
            ],
            "business_domains": business_domains,
            "audited_tools": tool_catalog,
        }
        return [
            {"role": "system", "content": json.dumps(system_payload, ensure_ascii=False, sort_keys=True)},
            {"role": "user", "content": user_message},
        ]

    @staticmethod
    def _validated_plan(
        payload: dict[str, Any],
        *,
        raw_response: str,
        tool_catalog: list[dict[str, Any]],
    ) -> SemanticToolPlan | None:
        status = str(payload.get("status") or "").strip().lower()
        requires_clarification = status == "clarification" or bool(payload.get("requires_clarification"))
        reason = str(payload.get("reason") or "").strip()
        confidence = _clamp_confidence(payload.get("confidence"))
        if requires_clarification:
            questions_raw = payload.get("clarification_questions") or payload.get("questions") or []
            if not isinstance(questions_raw, list):
                questions_raw = [questions_raw]
            questions = tuple(str(item).strip() for item in questions_raw if str(item).strip())
            if not questions:
                questions = ("Which metric should I use for this comparison?",)
            domain = str(payload.get("domain") or McpDomain.GENERAL.value).strip() or McpDomain.GENERAL.value
            return SemanticToolPlan(
                status="clarification",
                domain=domain,
                confidence=confidence or 0.7,
                reason=reason or "The request needs one more business choice before selecting an MCP tool.",
                requires_clarification=True,
                clarification_questions=questions,
                raw_response=raw_response,
            )
        if status == "no_tool":
            return SemanticToolPlan(
                status="no_tool",
                domain=McpDomain.GENERAL.value,
                confidence=confidence,
                reason=reason or "The request does not need an MCP tool.",
                raw_response=raw_response,
            )
        if status not in {"tool_plan", "tool", "route"}:
            return None
        tool_name = str(payload.get("tool_name") or payload.get("tool") or "").strip()
        server_key = str(payload.get("server_key") or payload.get("server") or "").strip()
        if not tool_name:
            return None
        tool = next(
            (
                item
                for item in tool_catalog
                if item["tool_name"] == tool_name and (not server_key or item["server_key"] == server_key)
            ),
            None,
        )
        if tool is None:
            return None
        server_key = str(tool["server_key"])
        domain = str(payload.get("domain") or _domain_for_tool(tool_name) or McpDomain.GENERAL.value).strip()
        try:
            McpDomain(domain)
        except ValueError:
            domain = _domain_for_tool(tool_name) or McpDomain.GENERAL.value
        tool_args = payload.get("tool_args") or payload.get("args") or payload.get("parameters") or {}
        if not isinstance(tool_args, dict):
            tool_args = {"value": tool_args}
        return SemanticToolPlan(
            status="tool_plan",
            domain=domain,
            server_key=server_key,
            tool_name=tool_name,
            tool_args=dict(tool_args),
            confidence=confidence,
            reason=reason or f"LLM selected audited MCP tool {server_key}/{tool_name}.",
            raw_response=raw_response,
        )
