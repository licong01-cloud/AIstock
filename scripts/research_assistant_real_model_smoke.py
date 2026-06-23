#!/usr/bin/env python
"""Real-model smoke for Research Assistant A1/A2 post-merge checks.

The smoke deliberately uses an in-memory Research Assistant repository so it
does not connect to production DB, seed production, apply DDL, or start any
service. It calls the configured DeepSeek endpoint only when
DEEPSEEK_API_KEY is present in the process environment.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
import json
import os
from pathlib import Path
import re
import sys
import traceback
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


SCHEMA_VERSION = "aistock_research_assistant_real_model_smoke_v1"
BUG_ID = "BUG-496"
RELATED_BUG_IDS = ["BUG-436", "BUG-496"]
SKIP_EXIT_CODE = 77
FAIL_EXIT_CODE = 1
PASS_EXIT_CODE = 0
DEFAULT_OUTPUT = ROOT / "tmp" / "validation" / "research_assistant" / "real_model_smoke.json"

NO_DATA_PHRASE = "没有对应数据源 / 无法获取该数据"
A2_REQUIRED_TOOL_REFS = {
    ("aistock-external-research", "external_research_search_web"),
    ("aistock-external-research", "external_research_fetch_extract"),
    ("aistock-stock-analysis", "stock_analysis_get_quote"),
    ("aistock-stock-analysis", "stock_analysis_get_financials"),
}
FORBIDDEN_CRASH_MARKERS = (
    "chat_turn_unexpected_error",
    "runtime_config_invalid_",
    "capability_not_found",
    "KeyError",
)
PROGRAM_ERROR_MARKERS = (
    "tool_execution_error",
    "data_source_unavailable",
    "chat_turn_unexpected_error",
    "capability_not_found",
)
B2_FORBIDDEN_TEMPLATE_MARKERS = (
    "已完成查询",
    "工具1",
    "工具 1",
    "工具2",
    "工具 2",
    "source 1",
    "source 2",
    "每个数据源",
    "单工具",
    "server_key",
    "tool_name",
    "mcp_execution_result",
    "summary_first",
)
BOTTOM_LINE_MARKERS = (
    "bottom-line",
    "bottom line",
    "结论",
    "先说结论",
    "核心判断",
    "一句话",
    "底线",
)
FUTURE_REQUIRED_TERMS = ("驱动", "情景", "风险")
FUTURE_DISCLAIMER_TERMS = ("不预测", "不做方向预测", "不是投资建议", "不构成投资建议", "仅供参考")
FUTURE_DIRECTIONAL_PATTERNS = (
    "会上涨",
    "会下跌",
    "将上涨",
    "将下跌",
    "必涨",
    "必跌",
    "看涨",
    "看跌",
    "可以买入",
    "可以卖出",
    "应买入",
    "应卖出",
    "应该买入",
    "应该卖出",
    "目标价",
    "涨到",
    "跌到",
    "上涨概率",
    "下跌概率",
    "go up",
    "go down",
    "bullish",
    "bearish",
    "price target",
)
B2_QE_REQUIRED_TOOLS = {
    ("aistock-qe", "qe_archive_query_promotion_candidates"),
    ("aistock-trading-ops", "strategy_governance_list_packages"),
    ("aistock-trading-ops", "strategy_governance_get_paper_readiness"),
}
B2_STOCK_REQUIRED_TOOLS = {
    ("aistock-stock-analysis", "stock_analysis_get_quote"),
    ("aistock-external-research", "external_research_search_web"),
}
B2_QE_REQUIRED_TERMS = ("QE", "graph_context", "LIVE")
B2_QE_STRATEGY_TERMS = ("策略包", "Strategy Package", "strategy package")
B2_QE_PAPER_TERMS = ("Paper v2", "paper_v2", "PaperV2")
B2_STOCK_SYNTHESIS_TERMS = ("综合", "核心判断", "判断", "意味着", "同时", "但", "风险", "Bottom-line")
B2_ASSERTION_MANIFEST = [
    "QE成果怎么利用 -> graph_context QE->Strategy Package->Paper v2 + multi-tool synthesis + bottom-line",
    "individual stock all-round question -> stock_analysis + external_research multi-tool synthesis + bottom-line",
    "future-looking question -> drivers/scenarios/risks/disclaimer, no directional prediction",
    "same seeded data with two different questions -> distinct question-specific answers, not one template",
    "write action -> approval card/preflight gate; no automatic write execution",
]


class SmokeFailure(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = details or {}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _preview(text: Any, limit: int = 900) -> str:
    rendered = str(text or "").strip()
    return rendered if len(rendered) <= limit else rendered[:limit] + "...<truncated>"


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except TypeError:
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_json_safe(item) for item in value]
        return str(value)


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "bug_id": BUG_ID,
        "related_bug_ids": RELATED_BUG_IDS,
        "acceptance_source": "B2/#1504 design killer assertions",
        "assertion_manifest": B2_ASSERTION_MANIFEST,
        "status": "running",
        "fake_pass": False,
        "started_at": _utc_now(),
        "safety": {
            "started_services": False,
            "production_db_touched": False,
            "ddl_executed": False,
            "production_seed_executed": False,
            "trading_or_order_write_executed": False,
            "repository_mode": "in_memory_test_only",
        },
        "checks": [],
    }


def _normalize_for_match(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "").lower())


def _normalize_for_similarity(text: Any) -> str:
    lowered = str(text or "").lower()
    lowered = re.sub(r"\d{4}-\d{2}-\d{2}(?:t\d{2}:\d{2}:\d{2}(?:\+\d{2}:\d{2}|z)?)?", "<date>", lowered)
    lowered = re.sub(r"\d+(?:\.\d+)?%?", "<num>", lowered)
    return re.sub(r"\s+", "", lowered)


def _starts_with_bottom_line(text: str) -> bool:
    head = str(text or "").strip()[:180].lower()
    return any(marker in head for marker in BOTTOM_LINE_MARKERS)


def _contains_all(text: str, terms: tuple[str, ...] | list[str]) -> bool:
    normalized = _normalize_for_match(text)
    return all(_normalize_for_match(term) in normalized for term in terms)


def _contains_any(text: str, terms: tuple[str, ...] | list[str]) -> bool:
    normalized = _normalize_for_match(text)
    return any(_normalize_for_match(term) in normalized for term in terms)


def _assert_text_contains(text: str, terms: tuple[str, ...] | list[str], *, reason_code: str, label: str) -> None:
    missing = [term for term in terms if _normalize_for_match(term) not in _normalize_for_match(text)]
    if missing:
        raise SmokeFailure(
            reason_code,
            f"{label} missing expected terms: {missing}",
            details={"missing_terms": missing, "assistant_text_preview": _preview(text)},
        )


def _assert_text_excludes(text: str, terms: tuple[str, ...] | list[str], *, reason_code: str, label: str) -> None:
    found = [term for term in terms if _normalize_for_match(term) in _normalize_for_match(text)]
    if found:
        raise SmokeFailure(
            reason_code,
            f"{label} included forbidden markers: {found}",
            details={"forbidden_markers": found, "assistant_text_preview": _preview(text)},
        )


def _directional_prediction_hits(text: str) -> list[str]:
    normalized = _normalize_for_match(text)
    hits: list[str] = []
    negation_cues = (
        "不",
        "不能",
        "不会",
        "无法",
        "不要",
        "不做",
        "不给",
        "不能给",
        "无法给",
        "不构成",
        "禁止",
        "避免",
        "拒绝",
        "非",
    )
    for pattern in FUTURE_DIRECTIONAL_PATTERNS:
        needle = _normalize_for_match(pattern)
        start = 0
        while True:
            index = normalized.find(needle, start)
            if index < 0:
                break
            prefix = normalized[max(0, index - 12) : index]
            if not any(cue in prefix for cue in negation_cues):
                hits.append(pattern)
                break
            start = index + len(needle)
    return hits


def _assert_no_directional_predictions(text: str, *, reason_code: str, label: str) -> None:
    found = _directional_prediction_hits(text)
    if found:
        raise SmokeFailure(
            reason_code,
            f"{label} included directional prediction markers: {found}",
            details={"directional_markers": found, "assistant_text_preview": _preview(text)},
        )


def _assert_bottom_line(text: str, *, reason_code: str, label: str) -> None:
    if not _starts_with_bottom_line(text):
        raise SmokeFailure(
            reason_code,
            f"{label} did not start with a bottom-line style synthesis.",
            details={"assistant_text_preview": _preview(text, limit=500)},
        )


def _assert_tool_refs_present(
    actual: set[tuple[str, str]],
    required: set[tuple[str, str]],
    *,
    reason_code: str,
    label: str,
) -> None:
    missing = sorted(required - actual)
    if missing:
        raise SmokeFailure(
            reason_code,
            f"{label} missing required tool executions: {missing}",
            details={"missing": [f"{server}/{tool}" for server, tool in missing], "actual": [f"{server}/{tool}" for server, tool in sorted(actual)]},
        )


def _assert_any_text_contains(
    text: str,
    terms: tuple[str, ...] | list[str],
    *,
    reason_code: str,
    label: str,
) -> None:
    if not _contains_any(text, terms):
        raise SmokeFailure(
            reason_code,
            f"{label} missing any expected terms: {list(terms)}",
            details={"expected_any_terms": list(terms), "assistant_text_preview": _preview(text)},
        )


def _assert_future_direction_boundary(text: str, *, label: str) -> None:
    _assert_text_contains(
        text,
        FUTURE_REQUIRED_TERMS,
        reason_code="b2_future_required_terms_missing",
        label=label,
    )
    _assert_any_text_contains(
        text,
        FUTURE_DISCLAIMER_TERMS,
        reason_code="b2_future_disclaimer_missing",
        label=label,
    )
    _assert_no_directional_predictions(
        text,
        reason_code="b2_future_directional_prediction_present",
        label=label,
    )


def _react_executed_tool_refs(result: dict[str, Any]) -> set[tuple[str, str]]:
    cards = _cards(result)
    refs: set[tuple[str, str]] = set()
    react = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
    for item in react.get("executed_tools") or []:
        if not isinstance(item, dict):
            continue
        server = str(item.get("server_key") or "")
        tool = str(item.get("tool_name") or "")
        if server and tool:
            refs.add((server, tool))
    execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
    server = str(execution.get("server_key") or "")
    tool = str(execution.get("tool_name") or "")
    if server and tool and execution.get("auto_executed"):
        refs.add((server, tool))
    return refs


def _service_tool_event_refs(service: Any) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()
    try:
        events = service.list_records("mcp_tool_events", limit=200)["items"]
    except Exception:
        events = []
    for event in events:
        if not isinstance(event, dict) or str(event.get("status") or "") != "succeeded":
            continue
        server = str(event.get("server_key") or "")
        tool = str(event.get("tool_name") or "")
        if server and tool:
            refs.add((server, tool))
    return refs


def _result_summary(result: dict[str, Any]) -> dict[str, Any]:
    cards = _cards(result)
    react = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
    route = cards.get("mcp_route_decision") if isinstance(cards.get("mcp_route_decision"), dict) else {}
    execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
    return {
        "assistant_text_preview": _preview(_assistant_text(result)),
        "route": {
            "server_key": route.get("server_key"),
            "tool_name": route.get("tool_name"),
            "graph_first": route.get("graph_first"),
            "agentic_route_policy": route.get("agentic_route_policy"),
            "route_candidates": [
                {
                    "server_key": item.get("server_key"),
                    "tool_name": item.get("tool_name"),
                    "side_effect": item.get("side_effect"),
                    "candidate_reason": item.get("candidate_reason"),
                }
                for item in (route.get("route_candidates") or [])[:8]
                if isinstance(item, dict)
            ],
        },
        "react_grounding": {
            "tool_call_count": react.get("tool_call_count"),
            "tool_result_count": react.get("tool_result_count"),
            "stopped_reason": react.get("stopped_reason"),
            "evidence_guard": react.get("evidence_guard"),
            "executed_tools": react.get("executed_tools"),
        },
        "mcp_execution_result": {
            "server_key": execution.get("server_key"),
            "tool_name": execution.get("tool_name"),
            "status": execution.get("status"),
            "auto_executed": execution.get("auto_executed"),
            "executed": execution.get("executed"),
            "approval_id": execution.get("approval_id"),
            "required_confirmation_text": execution.get("required_confirmation_text"),
        },
    }


def _require_deepseek_env(report: dict[str, Any]) -> bool:
    key = str(os.getenv("DEEPSEEK_API_KEY") or "").strip().strip("\"'")
    base_url = str(os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").strip()
    report["llm_config"] = {
        "provider": "deepseek",
        "credential_source": "env:DEEPSEEK_API_KEY" if key else "missing",
        "api_base_source": "env:DEEPSEEK_BASE_URL" if os.getenv("DEEPSEEK_BASE_URL") else "default:https://api.deepseek.com",
        "base_url": base_url,
        "has_api_key": bool(key),
        "db_config_lookup_allowed": False,
    }
    return bool(key)


def _require_litellm_available() -> None:
    try:
        import litellm  # noqa: F401
    except Exception as exc:  # noqa: BLE001 - dependency failure must be loud and specific.
        raise SmokeFailure(
            "litellm_missing",
            "litellm is not importable; real-model smoke cannot call DeepSeek.",
            details={"exception_type": type(exc).__name__, "message": str(exc)},
        ) from exc


@dataclass
class RecordingLlmClient:
    """Records actual tool exposure while delegating calls to the real RA LLM client."""

    inner: Any
    calls: list[dict[str, Any]]

    def complete(self, **kwargs: Any) -> Any:
        messages = kwargs.get("messages") if isinstance(kwargs.get("messages"), list) else []
        content_blob = "\n".join(
            str(message.get("content", ""))
            for message in messages
            if isinstance(message, dict)
        )
        registry = kwargs.get("tool_registry") if isinstance(kwargs.get("tool_registry"), dict) else {}
        tool_pairs = sorted(
            {
                (str(item.get("server_key")), str(item.get("tool_name")))
                for item in registry.values()
                if isinstance(item, dict)
            }
        )
        meta: dict[str, Any] = {
            "message_count": len(messages),
            "tool_spec_count": len(kwargs.get("tools") or []),
            "tool_registry_count": len(registry),
            "tool_registry_pairs": [f"{server}/{tool}" for server, tool in tool_pairs],
            "contains_graph_context": "graph_context" in content_blob,
            "contains_qe_strategy_paper_path": all(term in content_blob for term in ("module.qe", "module.strategy_package", "module.paper_v2")),
            "contains_qe_strategy_paper_chain": all(term in content_blob for term in ("module.qe", "promotes_to", "module.strategy_package", "enabled_for", "module.paper_v2")),
            "contains_agentic_synthesis_directive": "AGENTIC_REPLY_SYNTHESIS_DIRECTIVE" in content_blob,
        }
        self.calls.append(meta)
        try:
            result = self.inner.complete(**kwargs)
        except Exception as exc:
            meta["status"] = "failed"
            meta["exception_type"] = type(exc).__name__
            meta["message"] = str(exc)
            raise
        meta["status"] = "succeeded"
        meta["provider"] = getattr(result, "provider", None)
        meta["model"] = getattr(result, "model", None)
        meta["content_preview"] = _preview(getattr(result, "content", ""))
        meta["native_tool_calls"] = [
            f"{call.server_key}/{call.tool_name}"
            for call in (getattr(result, "tool_calls", None) or [])
        ]
        return result


def _make_recording_llm_client() -> RecordingLlmClient:
    from backend.services.research_assistant.service import ResearchAssistantLlmClient

    return RecordingLlmClient(inner=ResearchAssistantLlmClient(), calls=[])


def _new_service(llm_client: Any) -> Any:
    from backend.services.research_assistant.repository import InMemoryResearchAssistantRepository
    from backend.services.research_assistant.service import ResearchAssistantService

    service = ResearchAssistantService(
        repository=InMemoryResearchAssistantRepository(),
        llm_client=llm_client,
    )
    service.seed_catalogs()
    return service


def _analysis_mode(service: Any, message: str) -> Any:
    return service._decide_dialogue_mode(  # noqa: SLF001 - smoke asserts RA runtime internals intentionally.
        message,
        dialogue_intent=service._classify_dialogue_intent(message),  # noqa: SLF001
        phase="analysis",
        allow_execute=False,
        risk_level="medium",
        override="analysis",
    )


def _tool_sets(service: Any, message: str) -> dict[str, Any]:
    mode_decision = _analysis_mode(service, message)
    _specs, registry = service._agentic_function_tools(mode_decision)  # noqa: SLF001
    executable_entries = service._react_tool_catalog_entries(capability_backed_only=True)  # noqa: SLF001
    function_registry = {
        (str(item["server_key"]), str(item["tool_name"]))
        for item in registry.values()
        if isinstance(item, dict)
    }
    executable = {(entry.server_key, entry.tool_name) for entry in executable_entries}
    read_only_executable = {
        (entry.server_key, entry.tool_name)
        for entry in executable_entries
        if entry.side_effect_level == "read_only"
    }
    return {
        "function_registry": sorted(f"{server}/{tool}" for server, tool in function_registry),
        "executable": sorted(f"{server}/{tool}" for server, tool in executable),
        "read_only_executable": sorted(f"{server}/{tool}" for server, tool in read_only_executable),
        "function_registry_equals_read_only_executable": function_registry == read_only_executable,
        "required_refs_present": sorted(
            f"{server}/{tool}"
            for server, tool in A2_REQUIRED_TOOL_REFS
            if (server, tool) in function_registry and (server, tool) in executable
        ),
    }


def _assert_a2_tool_sets(tool_sets: dict[str, Any]) -> None:
    registry = {
        tuple(item.split("/", 1))
        for item in tool_sets["function_registry"]
        if isinstance(item, str) and "/" in item
    }
    executable = {
        tuple(item.split("/", 1))
        for item in tool_sets["executable"]
        if isinstance(item, str) and "/" in item
    }
    missing_registry = sorted(A2_REQUIRED_TOOL_REFS - registry)
    missing_executable = sorted(A2_REQUIRED_TOOL_REFS - executable)
    if missing_registry or missing_executable:
        raise SmokeFailure(
            "a2_required_tool_refs_missing",
            "A2 required external_research/stock_analysis refs are not both provided and executable.",
            details={
                "missing_from_function_registry": [f"{server}/{tool}" for server, tool in missing_registry],
                "missing_from_executable": [f"{server}/{tool}" for server, tool in missing_executable],
            },
        )
    if not tool_sets["function_registry_equals_read_only_executable"]:
        raise SmokeFailure(
            "a2_function_registry_not_equal_executable_read_only_set",
            "Agent offered read-only tool registry does not equal the capability-backed executable read-only set.",
            details={
                "function_registry_count": len(tool_sets["function_registry"]),
                "read_only_executable_count": len(tool_sets["read_only_executable"]),
            },
        )


def _cards(result: dict[str, Any]) -> dict[str, Any]:
    cards = result.get("cards") if isinstance(result.get("cards"), dict) else {}
    return cards


def _assistant_text(result: dict[str, Any]) -> str:
    assistant = result.get("assistant_message") if isinstance(result.get("assistant_message"), dict) else {}
    return str(assistant.get("content_text") or "")


def _assert_no_forbidden_markers(payload: Any, *, markers: tuple[str, ...], reason_code: str) -> None:
    rendered = json.dumps(_json_safe(payload), ensure_ascii=False, sort_keys=True)
    found = [marker for marker in markers if marker in rendered]
    if found:
        raise SmokeFailure(
            reason_code,
            f"Forbidden error markers appeared in smoke output: {found}",
            details={"forbidden_markers": found},
        )


def _provided_pairs_from_calls(calls: list[dict[str, Any]]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for call in calls:
        for item in call.get("tool_registry_pairs") or []:
            text = str(item)
            if "/" in text:
                pairs.add(tuple(text.split("/", 1)))  # type: ignore[arg-type]
    return pairs


def _contains_native_call(calls: list[dict[str, Any]], ref: tuple[str, str]) -> bool:
    needle = f"{ref[0]}/{ref[1]}"
    return any(needle in {str(item) for item in (call.get("native_tool_calls") or [])} for call in calls)


def _llm_call_with_flags(calls: list[dict[str, Any]], *flags: str) -> bool:
    return any(all(bool(call.get(flag)) for flag in flags) for call in calls)


def _seed_module_graph(service: Any) -> None:
    from backend.services.research_assistant.models import GraphEntityCreate, GraphRelationCreate

    qe = service.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.qe",
            title="QE",
            summary="Quant evolution module produces validated research candidates.",
            source_refs=["real-model-smoke://module/qe"],
            approval_status="approved",
        )
    )
    strategy_package = service.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.strategy_package",
            title="Strategy Package",
            summary="Strategy Package receives promoted QE outputs and makes them governable.",
            source_refs=["real-model-smoke://module/strategy-package"],
            approval_status="approved",
        )
    )
    paper_v2 = service.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.paper_v2",
            title="Paper v2",
            summary="Paper v2 runs simulation and paper validation for approved strategy packages.",
            source_refs=["real-model-smoke://module/paper-v2"],
            approval_status="approved",
        )
    )
    service.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=qe["entity_id"],
            target_entity_id=strategy_package["entity_id"],
            relation_type="promotes_to",
            evidence_refs=["real-model-smoke://graph/qe-promotes-strategy-package"],
            approval_status="approved",
        )
    )
    service.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=strategy_package["entity_id"],
            target_entity_id=paper_v2["entity_id"],
            relation_type="enabled_for",
            evidence_refs=["real-model-smoke://graph/strategy-package-enabled-for-paper-v2"],
            approval_status="approved",
        )
    )


class SeededQeArchiveRepository:
    def get_archive_summary(self) -> dict[str, Any]:
        return {
            "run_count": 7,
            "pending_outbox_count": 0,
            "latest_archived_at": "2026-06-17T09:00:00+08:00",
            "research_valid_counts": {"true": 7, "false": 0},
        }

    def list_outbox_events(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []

    def list_runs(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return [{"run_id": "run_qe_promoted", "status": "archived", "model_type": "CatBoost", "cagr": 0.18}]

    def get_analytics_view_status(self) -> list[dict[str, Any]]:
        return [{"logical_name": "promotion_candidates", "available": True, "row_count": 1}]

    def query_run_leaderboard(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return [{"run_id": "run_qe_promoted", "experiment_id": "exp_qe_promoted", "model_type": "CatBoost", "cagr": 0.18}]

    def query_seed_robustness(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []

    def query_factor_performance(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []

    def query_model_hyperparam_seed_perf(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []

    def query_overfit_flags(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []

    def query_promotion_candidates(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return [
            {
                "factor_set_hash": "fs_qe_promoted",
                "model_type": "CatBoost",
                "label_horizon": "20d",
                "topk": 20,
                "run_count": 6,
                "distinct_seed_count": 5,
                "cagr_mean": 0.18,
                "sharpe_mean": 1.2,
                "passes_gate": True,
                "latest_completed_at": "2026-06-17T09:00:00+08:00",
            }
        ]

    def query_evolution_lineage(self, **kwargs: Any) -> list[dict[str, Any]]:
        del kwargs
        return []


def _stock_envelope(*, dataset: str, symbol: str, summary: str, source: str = "stock_analysis_real_model_smoke") -> dict[str, Any]:
    return {
        "domain": f"stock_analysis.{dataset}",
        "symbol": symbol,
        "items": [{"dataset": dataset, "summary": summary, "symbol": symbol}],
        "total": 1,
        "source": source,
        "source_refs": [f"{source}:{dataset}:{symbol}"],
        "dataset": dataset,
        "as_of": "2026-06-17",
        "status": "ok",
        "summary": summary,
        "reason_codes": [],
        "warnings": [],
    }


class SeededStockFacade:
    def get_stock_quote_evidence(self, symbol: str) -> dict[str, Any]:
        return _stock_envelope(dataset="quote", symbol=symbol, summary=f"{symbol} quote shows liquidity but no standalone direction signal.")

    def get_stock_kline_evidence(self, symbol: str, period: str = "1y", analysis_date: str | None = None) -> dict[str, Any]:
        del period, analysis_date
        return _stock_envelope(dataset="kline", symbol=symbol, summary=f"{symbol} kline needs trend confirmation and risk controls.")

    def get_stock_financials_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _stock_envelope(dataset="financials", symbol=symbol, summary=f"{symbol} financials should be checked against revenue quality and margins.")

    def get_stock_quarterly_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _stock_envelope(dataset="quarterly", symbol=symbol, summary=f"{symbol} quarterly data highlights the need to compare recent and trailing periods.")

    def get_stock_margin_financing_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _stock_envelope(dataset="margin_financing", symbol=symbol, summary=f"{symbol} margin data is a sentiment and crowding input.")

    def get_stock_fund_flow_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _stock_envelope(dataset="fund_flow", symbol=symbol, summary=f"{symbol} fund-flow evidence must be cross-checked with price and volume.")

    def get_stock_technicals_evidence(self, symbol: str, period: str = "1y", analysis_date: str | None = None) -> dict[str, Any]:
        del period, analysis_date
        return _stock_envelope(dataset="technicals", symbol=symbol, summary=f"{symbol} technicals are useful for scenarios, not a directional prediction.")


class SeededExternalResult:
    def __init__(self, *, query: str) -> None:
        self.query = query
        self.url = f"https://example.org/ra-smoke/{sha_like(query)}"

    def compact(self, max_preview_chars: int = 800) -> dict[str, Any]:
        del max_preview_chars
        return {
            "title": "External business background",
            "summary": "External background adds industry and company context, but remains evidence input rather than final advice.",
            "url": self.url,
            "source": "external_research_real_model_smoke",
            "as_of": "2026-06-17",
            "evidence_ref": f"external-research-smoke:{sha_like(self.query)}",
        }


class SeededExternalExtract:
    def __init__(self, *, url: str) -> None:
        self.url = url

    def compact(self, max_preview_chars: int = 800) -> dict[str, Any]:
        del max_preview_chars
        return {
            "title": "External extract",
            "summary": "The extract reinforces that fundamentals should be weighed with market and risk evidence.",
            "url": self.url,
            "source": "external_research_real_model_smoke",
            "as_of": "2026-06-17",
            "evidence_ref": f"external-extract-smoke:{sha_like(self.url)}",
        }


def sha_like(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


class SeededExternalResearchProvider:
    provider_key = "ra_real_model_smoke_seeded_external_research"

    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list[Any]:
        del locale, limit
        return [SeededExternalResult(query=query)]

    def search_papers(self, query: str, *, provider: str | None = None, limit: int = 10) -> list[Any]:
        del provider, limit
        return [SeededExternalResult(query=query)]

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> Any:
        del max_chars
        return SeededExternalExtract(url=url)


def _call_summary(service: Any, result: dict[str, Any], llm: RecordingLlmClient) -> dict[str, Any]:
    return {
        **_result_summary(result),
        "service_tool_events": [f"{server}/{tool}" for server, tool in sorted(_service_tool_event_refs(service))],
        "react_executed_tools": [f"{server}/{tool}" for server, tool in sorted(_react_executed_tool_refs(result))],
        "llm_call_count": len(llm.calls),
        "llm_calls": llm.calls,
    }


def _chat_with_seeded_data(
    message: str,
    *,
    dialogue_mode_override: str = "analysis",
    allow_execute: bool = False,
    seed_graph: bool = False,
) -> tuple[Any, RecordingLlmClient, dict[str, Any]]:
    from backend.services.research_assistant.models import ChatTurnRequest

    llm = _make_recording_llm_client()
    service = _new_service(llm)
    service.qe_archive_repository_factory = SeededQeArchiveRepository
    service.stock_analysis_facade_factory = SeededStockFacade
    service.external_research_provider_factory = SeededExternalResearchProvider
    if seed_graph:
        _seed_module_graph(service)
    result = service.chat_turn(
        ChatTurnRequest(
            message=message,
            dialogue_mode_override=dialogue_mode_override,
            allow_execute=allow_execute,
        )
    )
    return service, llm, result


def run_b2_qe_usage_smoke(message: str) -> dict[str, Any]:
    service, llm, result = _chat_with_seeded_data(message, seed_graph=True)
    text = _assistant_text(result)
    executed_tools = _service_tool_event_refs(service) | _react_executed_tool_refs(result)

    _assert_tool_refs_present(
        executed_tools,
        B2_QE_REQUIRED_TOOLS,
        reason_code="b2_qe_required_tools_missing",
        label="B2 QE usage smoke",
    )
    if not _llm_call_with_flags(llm.calls, "contains_graph_context", "contains_qe_strategy_paper_chain"):
        raise SmokeFailure(
            "b2_qe_graph_context_missing_from_llm",
            "B2 QE usage smoke did not send QE->Strategy Package->Paper v2 graph_context to the chat LLM.",
            details={"llm_calls": llm.calls},
        )
    _assert_bottom_line(text, reason_code="b2_qe_bottom_line_missing", label="B2 QE usage smoke")
    _assert_text_contains(
        text,
        B2_QE_REQUIRED_TERMS,
        reason_code="b2_qe_expected_terms_missing",
        label="B2 QE usage smoke",
    )
    _assert_any_text_contains(
        text,
        B2_QE_STRATEGY_TERMS,
        reason_code="b2_qe_strategy_package_missing",
        label="B2 QE usage smoke",
    )
    _assert_any_text_contains(
        text,
        B2_QE_PAPER_TERMS,
        reason_code="b2_qe_paper_v2_missing",
        label="B2 QE usage smoke",
    )
    _assert_text_excludes(
        text,
        B2_FORBIDDEN_TEMPLATE_MARKERS,
        reason_code="b2_qe_template_marker_present",
        label="B2 QE usage smoke",
    )
    _assert_no_forbidden_markers(result, markers=FORBIDDEN_CRASH_MARKERS, reason_code="b2_qe_forbidden_error_marker")
    return {
        "name": "B2 QE usage graph + multi-tool synthesis",
        "status": "passed",
        "message": message,
        **_call_summary(service, result, llm),
    }


def run_b2_stock_synthesis_smoke(message: str) -> dict[str, Any]:
    service, llm, result = _chat_with_seeded_data(message)
    text = _assistant_text(result)
    executed_tools = _service_tool_event_refs(service) | _react_executed_tool_refs(result)

    _assert_tool_refs_present(
        executed_tools,
        B2_STOCK_REQUIRED_TOOLS,
        reason_code="b2_stock_required_tools_missing",
        label="B2 stock synthesis smoke",
    )
    _assert_bottom_line(text, reason_code="b2_stock_bottom_line_missing", label="B2 stock synthesis smoke")
    _assert_any_text_contains(
        text,
        B2_STOCK_SYNTHESIS_TERMS,
        reason_code="b2_stock_synthesis_terms_missing",
        label="B2 stock synthesis smoke",
    )
    _assert_text_excludes(
        text,
        B2_FORBIDDEN_TEMPLATE_MARKERS,
        reason_code="b2_stock_template_marker_present",
        label="B2 stock synthesis smoke",
    )
    _assert_no_forbidden_markers(result, markers=FORBIDDEN_CRASH_MARKERS, reason_code="b2_stock_forbidden_error_marker")
    return {
        "name": "B2 stock multi-source synthesis",
        "status": "passed",
        "message": message,
        **_call_summary(service, result, llm),
    }


def run_b2_future_boundary_smoke(message: str) -> dict[str, Any]:
    service, llm, result = _chat_with_seeded_data(message)
    text = _assistant_text(result)

    _assert_bottom_line(text, reason_code="b2_future_bottom_line_missing", label="B2 future boundary smoke")
    _assert_future_direction_boundary(text, label="B2 future boundary smoke")
    _assert_text_excludes(
        text,
        B2_FORBIDDEN_TEMPLATE_MARKERS,
        reason_code="b2_future_template_marker_present",
        label="B2 future boundary smoke",
    )
    _assert_no_forbidden_markers(result, markers=FORBIDDEN_CRASH_MARKERS, reason_code="b2_future_forbidden_error_marker")
    return {
        "name": "B2 future boundary no-direction smoke",
        "status": "passed",
        "message": message,
        **_call_summary(service, result, llm),
    }


def run_b2_question_specificity_smoke(first_message: str, second_message: str) -> dict[str, Any]:
    _first_service, first_llm, first_result = _chat_with_seeded_data(first_message)
    _second_service, second_llm, second_result = _chat_with_seeded_data(second_message)
    first_text = _assistant_text(first_result)
    second_text = _assistant_text(second_result)
    first_norm = _normalize_for_similarity(first_text)
    second_norm = _normalize_for_similarity(second_text)
    similarity = SequenceMatcher(None, first_norm, second_norm).ratio()

    if similarity >= 0.86:
        raise SmokeFailure(
            "b2_question_specificity_answers_too_similar",
            "B2 question-specificity smoke produced near-template-identical answers for two different questions over the same seeded data.",
            details={
                "similarity_ratio": similarity,
                "first_answer_preview": _preview(first_text),
                "second_answer_preview": _preview(second_text),
            },
        )
    _assert_any_text_contains(
        first_text,
        ("风险", "回撤", "crowding", "拥挤", "下行"),
        reason_code="b2_specificity_first_focus_missing",
        label="B2 question-specificity risk question",
    )
    _assert_any_text_contains(
        second_text,
        ("驱动", "情景", "增长", "基本面", "资金"),
        reason_code="b2_specificity_second_focus_missing",
        label="B2 question-specificity driver question",
    )
    _assert_text_excludes(
        first_text + "\n" + second_text,
        B2_FORBIDDEN_TEMPLATE_MARKERS,
        reason_code="b2_specificity_template_marker_present",
        label="B2 question-specificity smoke",
    )
    return {
        "name": "B2 same-data different-question specificity",
        "status": "passed",
        "messages": [first_message, second_message],
        "similarity_ratio": similarity,
        "first": {
            "assistant_text_preview": _preview(first_text),
            "llm_call_count": len(first_llm.calls),
            "llm_calls": first_llm.calls,
        },
        "second": {
            "assistant_text_preview": _preview(second_text),
            "llm_call_count": len(second_llm.calls),
            "llm_calls": second_llm.calls,
        },
    }


def run_b2_write_approval_smoke(message: str) -> dict[str, Any]:
    service, llm, result = _chat_with_seeded_data(message, dialogue_mode_override="planning", allow_execute=False)
    cards = _cards(result)
    proposals = cards.get("action_proposals") if isinstance(cards.get("action_proposals"), list) else []
    execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
    approval_cards = [
        proposal
        for proposal in proposals
        if isinstance(proposal, dict) and (proposal.get("approval_required") is True or str(proposal.get("status") or "") == "approval_required")
    ]

    if not approval_cards:
        raise SmokeFailure(
            "b2_write_approval_card_missing",
            "B2 write-action smoke did not produce an approval-required action card.",
            details={"cards": cards, "llm_calls": llm.calls},
        )
    if execution.get("executed") is not False or execution.get("auto_executed") is not False:
        raise SmokeFailure(
            "b2_write_action_auto_executed",
            "B2 write-action smoke unexpectedly executed or auto-executed a write action.",
            details={"mcp_execution_result": execution, "action_proposals": proposals},
        )
    latest = approval_cards[-1]
    if not (latest.get("approval_id") and latest.get("required_confirmation_text")):
        raise SmokeFailure(
            "b2_write_approval_metadata_missing",
            "B2 write-action approval card lacks approval_id or required_confirmation_text.",
            details={"approval_card": latest, "mcp_execution_result": execution},
        )
    return {
        "name": "B2 write action approval gate",
        "status": "passed",
        "message": message,
        **_call_summary(service, result, llm),
    }


def run_a2_smoke(message: str) -> dict[str, Any]:
    from backend.services.research_assistant.models import ChatTurnRequest

    llm = _make_recording_llm_client()
    service = _new_service(llm)
    tool_sets = _tool_sets(service, message)
    _assert_a2_tool_sets(tool_sets)

    result = service.chat_turn(ChatTurnRequest(message=message, dialogue_mode_override="analysis"))
    text = _assistant_text(result)
    cards = _cards(result)
    execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
    react = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}
    provided_pairs = _provided_pairs_from_calls(llm.calls)

    if not A2_REQUIRED_TOOL_REFS <= provided_pairs:
        raise SmokeFailure(
            "a2_real_model_call_missing_required_tool_refs",
            "DeepSeek chat turn was not actually offered all required A2 tool refs.",
            details={
                "missing": [f"{server}/{tool}" for server, tool in sorted(A2_REQUIRED_TOOL_REFS - provided_pairs)],
                "provided_count": len(provided_pairs),
            },
        )
    if execution.get("tool_name") != "external_research_search_web" or execution.get("status") != "succeeded":
        raise SmokeFailure(
            "a2_search_web_not_executed",
            "A2 smoke did not execute external_research_search_web successfully.",
            details={"mcp_execution_result": execution, "react_grounding": react},
        )
    _assert_no_forbidden_markers(result, markers=FORBIDDEN_CRASH_MARKERS, reason_code="a2_forbidden_error_marker")
    return {
        "name": "A2 external_research + stock_analysis availability",
        "status": "passed",
        "message": message,
        "tool_sets": tool_sets,
        "llm_call_count": len(llm.calls),
        "llm_calls": llm.calls,
        "executed_tool": f"{execution.get('server_key')}/{execution.get('tool_name')}",
        "execution_status": execution.get("status"),
        "react_grounding": react,
        "assistant_text_preview": _preview(text),
    }


def _missing_stock_envelope(*, dataset: str, symbol: str, label: str) -> dict[str, Any]:
    reason_code = f"stock_{dataset}_facade_missing"
    as_of = _utc_now()
    return {
        "domain": f"stock_analysis.{dataset}",
        "symbol": symbol,
        "items": [],
        "total": 0,
        "source": "ra_real_model_smoke_no_data_fixture",
        "source_refs": [],
        "dataset": dataset,
        "as_of": as_of,
        "status": "degraded",
        "summary": f"{NO_DATA_PHRASE}：缺少 {label} 的 RA stock_analysis 只读数据源。",
        "reason_codes": [reason_code],
        "warnings": [
            {
                "reason_code": reason_code,
                "warning": f"{NO_DATA_PHRASE}：缺少 {label} 的 RA stock_analysis 只读数据源。",
                "dataset": dataset,
            }
        ],
    }


class NoDataStockFacade:
    def get_stock_quote_evidence(self, symbol: str) -> dict[str, Any]:
        return _missing_stock_envelope(dataset="quote", symbol=symbol, label="行情")

    def get_stock_kline_evidence(self, symbol: str, period: str = "1y", analysis_date: str | None = None) -> dict[str, Any]:
        del period, analysis_date
        return _missing_stock_envelope(dataset="kline", symbol=symbol, label="K线")

    def get_stock_financials_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _missing_stock_envelope(dataset="financials", symbol=symbol, label="财务摘要")

    def get_stock_quarterly_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _missing_stock_envelope(dataset="quarterly", symbol=symbol, label="季度财务")

    def get_stock_margin_financing_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _missing_stock_envelope(dataset="margin_financing", symbol=symbol, label="融资融券")

    def get_stock_fund_flow_evidence(self, symbol: str, analysis_date: str | None = None) -> dict[str, Any]:
        del analysis_date
        return _missing_stock_envelope(dataset="fund_flow", symbol=symbol, label="资金流")

    def get_stock_technicals_evidence(self, symbol: str, period: str = "1y", analysis_date: str | None = None) -> dict[str, Any]:
        del period, analysis_date
        return _missing_stock_envelope(dataset="technicals", symbol=symbol, label="技术指标")


class EmptyExternalResearchProvider:
    provider_key = "ra_real_model_smoke_empty_external_research"

    def search_web(self, query: str, *, locale: str = "zh-CN", limit: int = 10) -> list[Any]:
        del query, locale, limit
        return []

    def search_papers(self, query: str, *, provider: str | None = None, limit: int = 10) -> list[Any]:
        del query, provider, limit
        return []

    def fetch_extract(self, url: str, *, max_chars: int = 2000) -> Any:
        raise RuntimeError(f"{NO_DATA_PHRASE}: no external URL was available for fetch_extract; url={url}; max_chars={max_chars}")


def run_a1_smoke(message: str) -> dict[str, Any]:
    from backend.services.research_assistant.models import ChatTurnRequest

    llm = _make_recording_llm_client()
    service = _new_service(llm)
    service.stock_analysis_facade_factory = NoDataStockFacade
    service.external_research_provider_factory = EmptyExternalResearchProvider

    result = service.chat_turn(ChatTurnRequest(message=message, dialogue_mode_override="analysis"))
    text = _assistant_text(result)
    cards = _cards(result)
    execution = cards.get("mcp_execution_result") if isinstance(cards.get("mcp_execution_result"), dict) else {}
    react = cards.get("react_grounding") if isinstance(cards.get("react_grounding"), dict) else {}

    if execution.get("status") != "succeeded" or str(execution.get("tool_name") or "").startswith("stock_analysis_") is False:
        raise SmokeFailure(
            "a1_no_data_stock_tool_not_executed",
            "A1 smoke did not execute a stock_analysis read-only tool in the constructed no-data-source scenario.",
            details={"mcp_execution_result": execution, "react_grounding": react},
        )
    if NO_DATA_PHRASE not in text:
        raise SmokeFailure(
            "a1_no_data_source_phrase_missing",
            f"A1 final answer did not explicitly include: {NO_DATA_PHRASE}",
            details={"assistant_text_preview": _preview(text), "mcp_execution_result": execution},
        )
    required_missing_classes = ("行情", "财务", "资金流", "技术", "external_research")
    missing_classes = [item for item in required_missing_classes if item not in text]
    if missing_classes:
        raise SmokeFailure(
            "a1_missing_data_classes_not_named",
            "A1 final answer did not name the expected missing data classes.",
            details={"missing_classes": missing_classes, "assistant_text_preview": _preview(text)},
        )
    _assert_no_forbidden_markers(result, markers=PROGRAM_ERROR_MARKERS, reason_code="a1_program_error_masked_as_insufficient")
    if "Insufficient evidence" in text:
        raise SmokeFailure(
            "a1_english_insufficient_evidence_instead_of_no_data_source",
            "A1 final answer used generic insufficient-evidence wording instead of explicit no-data-source disclosure.",
            details={"assistant_text_preview": _preview(text)},
        )
    return {
        "name": "A1 no-data-source disclosure",
        "status": "passed",
        "message": message,
        "llm_call_count": len(llm.calls),
        "llm_calls": llm.calls,
        "executed_tool": f"{execution.get('server_key')}/{execution.get('tool_name')}",
        "execution_status": execution.get("status"),
        "react_grounding": react,
        "assistant_text_preview": _preview(text),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run real DeepSeek Research Assistant smoke for A1/A2 checks and B2 killer assertions."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="JSON report path.")
    parser.add_argument("--b2-qe-message", default="QE成果怎么利用")
    parser.add_argument(
        "--b2-stock-message",
        default=(
            "请综合 000001.SZ 的行情、财务、资金流和 web search 背景，给一个全方位判断。"
            "需要同时使用 stock_analysis_get_quote 和 external_research_search_web 两个只读工具，"
            "先给 Bottom-line，再合成一个论点，不要逐个罗列工具。"
        ),
    )
    parser.add_argument(
        "--b2-future-message",
        default=(
            "请综合 000001.SZ 的个股证据判断未来一个月的关键边界："
            "只给驱动、情景、风险和免责声明，明确不做方向预测。"
        ),
    )
    parser.add_argument(
        "--b2-specificity-first-message",
        default=(
            "基于同一组 000001.SZ 证据，重点回答风险和回撤约束是什么；"
            "先给 Bottom-line，再说明风险，不要模板化罗列。"
        ),
    )
    parser.add_argument(
        "--b2-specificity-second-message",
        default=(
            "基于同一组 000001.SZ 证据，重点回答主要驱动和可能情景是什么；"
            "先给 Bottom-line，再说明驱动，不要模板化罗列。"
        ),
    )
    parser.add_argument(
        "--b2-write-message",
        default=(
            "请创建一个 QE template draft，不要真正执行或物化。"
            "如果需要工具，请对 qe_template_create 发起工具调用，必须走审批卡。"
        ),
    )
    parser.add_argument(
        "--a2-message",
        default=(
            "国城矿业基本情况、近期走势、未来趋势怎样？"
            "请直接通过可用 tool_call 调用 external_research_search_web 做只读联网检索；"
            "如果需要工具调用，请使用可用工具，不要编造。"
        ),
    )
    parser.add_argument(
        "--a1-message",
        default=(
            "请用 RA 现有 stock_analysis MCP 只读工具检查 000688 的行情、财务、资金流、技术指标和联网基本面。"
            "这是 smoke 构造的无数据源场景；如果工具结果显示缺数据，最终回答必须明确写出："
            "没有对应数据源 / 无法获取该数据，并指出缺哪些类数据；不要把真实工具错误伪装成 insufficient。"
            "最终回答还必须逐项写出这些缺失类别词：行情、财务、资金流、技术、external_research。"
            "请先调用 stock_analysis_get_quote，symbol=000688。"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(list(argv or sys.argv[1:]))
    report = _base_report()
    report["output_path"] = str(args.output)

    if not _require_deepseek_env(report):
        report.update(
            {
                "status": "skipped",
                "reason_code": "deepseek_api_key_missing",
                "message": (
                    "DEEPSEEK_API_KEY is missing from the process environment; "
                    "real-model smoke loud-skipped without DB lookup or fake pass."
                ),
                "completed_at": _utc_now(),
            }
        )
        _write_report(args.output, report)
        print(
            "research-assistant real-model smoke: SKIPPED "
            "reason_code=deepseek_api_key_missing; missing=DEEPSEEK_API_KEY; "
            "db_lookup=false; fake_pass=false"
        )
        return SKIP_EXIT_CODE

    try:
        _require_litellm_available()
        report["assertion_manifest"] = B2_ASSERTION_MANIFEST
        report["checks"].append(run_b2_qe_usage_smoke(args.b2_qe_message))
        report["checks"].append(run_b2_stock_synthesis_smoke(args.b2_stock_message))
        report["checks"].append(run_b2_future_boundary_smoke(args.b2_future_message))
        report["checks"].append(
            run_b2_question_specificity_smoke(
                args.b2_specificity_first_message,
                args.b2_specificity_second_message,
            )
        )
        report["checks"].append(run_b2_write_approval_smoke(args.b2_write_message))
        report["checks"].append(run_a2_smoke(args.a2_message))
        report["checks"].append(run_a1_smoke(args.a1_message))
    except SmokeFailure as exc:
        report.update(
            {
                "status": "failed",
                "reason_code": exc.reason_code,
                "message": str(exc),
                "details": exc.details,
                "completed_at": _utc_now(),
            }
        )
        _write_report(args.output, report)
        print(
            "research-assistant real-model smoke: FAILED "
            f"reason_code={exc.reason_code}; message={exc}",
            file=sys.stderr,
        )
        return FAIL_EXIT_CODE
    except Exception as exc:  # noqa: BLE001 - smoke must report unexpected infrastructure failures loudly.
        report.update(
            {
                "status": "failed",
                "reason_code": "real_model_smoke_unexpected_error",
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "traceback_tail": traceback.format_exc().splitlines()[-20:],
                "completed_at": _utc_now(),
            }
        )
        _write_report(args.output, report)
        print(
            "research-assistant real-model smoke: FAILED "
            f"reason_code=real_model_smoke_unexpected_error; exception_type={type(exc).__name__}; message={exc}",
            file=sys.stderr,
        )
        return FAIL_EXIT_CODE

    report.update(
        {
            "status": "passed",
            "reason_code": "ok",
            "completed_at": _utc_now(),
        }
    )
    _write_report(args.output, report)
    print(
        "research-assistant real-model smoke: PASSED "
        f"checks={len(report['checks'])}; output={args.output}"
    )
    return PASS_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
