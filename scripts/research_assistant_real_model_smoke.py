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
import json
import os
from pathlib import Path
import sys
import traceback
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


SCHEMA_VERSION = "aistock_research_assistant_real_model_smoke_v1"
BUG_ID = "BUG-436"
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
        "status": "running",
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
        registry = kwargs.get("tool_registry") if isinstance(kwargs.get("tool_registry"), dict) else {}
        tool_pairs = sorted(
            {
                (str(item.get("server_key")), str(item.get("tool_name")))
                for item in registry.values()
                if isinstance(item, dict)
            }
        )
        meta: dict[str, Any] = {
            "message_count": len(kwargs.get("messages") or []),
            "tool_spec_count": len(kwargs.get("tools") or []),
            "tool_registry_count": len(registry),
            "tool_registry_pairs": [f"{server}/{tool}" for server, tool in tool_pairs],
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
        description="Run real DeepSeek Research Assistant smoke for A1 no-data-source disclosure and A2 web-search availability."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="JSON report path.")
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
