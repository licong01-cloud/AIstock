from __future__ import annotations

import copy
import json
import logging

import pytest
import yaml

from backend.mcp.tool_manifest import TOOL_MANIFEST
from backend.services.research_assistant.models import (
    ApprovalCreate,
    ChatTurnRequest,
    ConversationCreate,
    ContextPackBuildRequest,
    EvolutionPathCreate,
    ExternalAgentEventCreate,
    ExternalAgentSessionCreate,
    GraphEntityCreate,
    GraphRelationCreate,
    IssueCandidateCreate,
    IssueCandidateGithubSyncRequest,
    McpPreflightRequest,
    MemoryCreate,
    ModelRouteRequest,
    PromptBundleBuildRequest,
    SkillUsageCreate,
    TaskCreate,
    TaskEventCreate,
    TraceEventCreate,
    WorkbenchDryRunExecuteRequest,
)
from backend.services.research_assistant.repository import DatabaseResearchAssistantRepository, InMemoryResearchAssistantRepository, TABLES, _adapt_json
from backend.services.research_assistant.service import (
    ASSISTANT_APPROVAL_CONFIRM,
    DialogueIntent,
    DialogueMode,
    IssueCandidateFactSource,
    LlmCallResult,
    ModeDecision,
    ResearchAssistantCatalogNotReadyError,
    ResearchAssistantRuntimeConfigInvalidError,
    ResearchAssistantService,
    STOCK_DEPTH_EXTERNAL_TOOL_NAMES,
    STOCK_DEPTH_HISTORY_PERIOD,
    STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS,
    STOCK_DEPTH_MIN_TOOL_EXECUTIONS,
    STOCK_DEPTH_REQUIRED_TOOL_REFS,
    STOCK_DEPTH_STOCK_TOOL_NAMES,
    _calculate_litellm_cost,
    _normalize_litellm_usage,
)
from backend.services.research_assistant.runtime_config import (
    RuntimeConfigCapabilityValidationError,
    load_runtime_config,
    validate_runtime_config_payload,
)
from backend.services.research_assistant.react_grounding import EvidenceGuardDecision, McpToolCall, ModelTurn, ReactGroundingResult


class DialogueAwareFakeLlmClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        business_answer = _agentic_business_answer(kwargs.get("messages"))
        if business_answer is not None:
            return LlmCallResult(
                content=business_answer,
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={"prompt_tokens": 100, "completion_tokens": 40},
            )
        messages = kwargs.get("messages", [])
        current_message = str(messages[-1].get("content", "")) if isinstance(messages, list) and messages else ""  # type: ignore[union-attr]
        if "是否可以" in current_message or "能生成 QE 实验和诊断 bug" in current_message:
            content = "可以。我能生成 QE 实验草案、校验模板并在确认后进入 MCP preflight；也能诊断 bug，分析报错、日志、Trace、实验记录和配置差异。"
        elif "诊断" in current_message and ("报错" in current_message or "bug" in current_message):
            content = "可以诊断。请提供报错文本、任务 ID、实验 ID、页面路径或复现步骤中的任意一种，我会先做只读根因分析。"
        else:
            content = "已收到明确的 QE 实验草案任务。我会先整理目标、股票池、时间窗、成本和风险边界；不默认固定迭代数量。"
        return LlmCallResult(
            content=content,
            provider="fake",
            model="fake-primary",
            duration_ms=12,
            usage={"prompt_tokens": 100, "completion_tokens": 40},
        )


FakeLlmClient = DialogueAwareFakeLlmClient


def _tool_payloads_from_messages(messages: object) -> list[dict[str, object]]:
    payloads: list[dict[str, object]] = []
    if not isinstance(messages, list):
        return payloads
    for message in messages:
        if not isinstance(message, dict):
            continue
        try:
            parsed = json.loads(str(message.get("content") or ""))
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and parsed.get("type") == "TOOL_RESULT" and isinstance(parsed.get("payload"), dict):
            payloads.append(parsed["payload"])
    return payloads


def _request_text_from_messages(messages: object) -> str:
    if not isinstance(messages, list):
        return ""
    for message in reversed(messages):
        if not isinstance(message, dict) or message.get("role") != "user":
            continue
        content = str(message.get("content") or "")
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and parsed.get("type") in {"TOOL_RESULT", "REACT_RETRY_DIRECTIVE"}:
            continue
        if content.strip():
            return content
    return ""


def _agentic_business_answer(messages: object) -> str | None:
    payloads = _tool_payloads_from_messages(messages)
    if not payloads:
        return None
    payload = payloads[-1]
    request_text = _request_text_from_messages(messages)
    response_mode = str(payload.get("response_mode") or "")
    source = str(payload.get("source") or "mcp_tool_event")
    as_of = str(payload.get("as_of") or payload.get("trade_date") or "2026-06-17")
    if response_mode == "qe_experiment_status_summary":
        counts = payload.get("status_counts") if isinstance(payload.get("status_counts"), dict) else {}
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        running = int(counts.get("running") or 0)
        created = int(counts.get("created") or 0)
        completed = int(counts.get("completed") or 0)
        failed = int(counts.get("failed") or 0)
        if any(term in request_text for term in ("还在运行", "还在跑", "running")):
            if running == 0:
                return f"目前无正在运行的 QE 实验；{created} created、{completed} completed。来源 {source}，截至 {as_of}。"
            names = "、".join(str(item.get("experiment_name") or item.get("task_name") or item.get("experiment_id") or item.get("task_id")) for item in items if isinstance(item, dict) and str(item.get("status") or "") == "running")
            return f"目前有 {running} 个 QE 实验正在运行：{names}。来源 {source}，截至 {as_of}。"
        names = "、".join(str(item.get("experiment_name") or item.get("task_name") or item.get("experiment_id") or item.get("task_id")) for item in items[:3] if isinstance(item, dict))
        return f"当前 QE 实验证据显示 completed={completed}、running={running}、failed={failed}；代表记录包括 {names}。来源 {source}，截至 {as_of}。"
    if response_mode == "qe_warehouse_business_summary":
        summary_kind = str(payload.get("summary_kind") or "")
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        health = payload.get("health_summary") if isinstance(payload.get("health_summary"), dict) else {}
        if (summary_kind == "query_run_leaderboard" or any("cagr" in item for item in items if isinstance(item, dict))) and items and isinstance(items[0], dict):
            best = items[0]
            return (
                f"按 CAGR 口径看，{best.get('experiment_id')} / {best.get('run_id')} 当前最好："
                f"CAGR={float(best.get('cagr') or 0) * 100:.2f}%，模型={best.get('model_type')}，"
                f"Sharpe={best.get('sharpe')}，IR={best.get('information_ratio')}。来源 {source}，截至 {as_of}。"
            )
        if health:
            return (
                f"QE 数仓当前可读：run_count={health.get('run_count')}、pending_outbox={health.get('pending_outbox_count')}、"
                f"research_valid={health.get('research_valid_counts')}。来源 {source}，截至 {as_of}。"
            )
        return f"QE 数仓返回 {len(items)} 条只读记录。来源 {source}，截至 {as_of}。"
    if response_mode == "local_data_daily_sync_status" or payload.get("local_data_daily_status"):
        counts = payload.get("group_counts") if isinstance(payload.get("group_counts"), dict) else {}
        groups = payload.get("status_groups") if isinstance(payload.get("status_groups"), dict) else {}
        success_names = "、".join(str(item.get("dataset")) for item in (groups.get("success") or []) if isinstance(item, dict))
        failed_names = "、".join(str(item.get("dataset")) for item in (groups.get("failed") or []) if isinstance(item, dict))
        running_names = "、".join(str(item.get("dataset")) for item in (groups.get("running") or []) if isinstance(item, dict))
        blocked_names = "、".join(str(item.get("dataset")) for item in (groups.get("blocked") or []) if isinstance(item, dict))
        return (
            f"今天本地数据同步结果：success={counts.get('success', 0)}、failed={counts.get('failed', 0)}、"
            f"running={counts.get('running', 0)}、blocked={counts.get('blocked', 0)}；"
            f"已完成 {success_names or '无'}；失败 {failed_names or '无'}；运行中 {running_names or '无'}；阻断 {blocked_names or '无'}。"
            f"来源 {source}，截至 {as_of}。"
        )
    if response_mode == "stock_analysis_evidence_card":
        symbol = str(payload.get("symbol") or "")
        sections = payload.get("sections") if isinstance(payload.get("sections"), list) else []
        datasets = [str(section.get("dataset")) for section in sections if isinstance(section, dict)]
        return (
            f"{symbol} 的全方位分析已覆盖基本情况、近期走势和未来趋势线索；证据维度包括 {', '.join(datasets)}。"
            f"可用证据支持行情、财务、资金流、技术面与联网基本面一起阅读，但不构成买卖建议。来源 {source}，截至 {as_of}。"
        )
    return None


class AgenticBusinessSynthesisFakeLlmClient(FakeLlmClient):
    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        content = _agentic_business_answer(kwargs.get("messages")) or "需要先读取审计工具结果再回答。"
        return LlmCallResult(
            content=content,
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


class B2AgenticSynthesisFakeLlmClient(FakeLlmClient):
    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        payloads = _tool_payloads_from_messages(kwargs.get("messages"))
        modes = {str(payload.get("response_mode") or "") for payload in payloads}
        if {"graph_context", "qe_warehouse_business_summary", "summary"} <= modes:
            content = (
                "Bottom-line：QE 成果的利用路径是先把通过门禁的候选沉淀成策略包，再进入 Paper v2 做模拟盘验证；"
                "当前已有 QE promotes_to 策略包、策略包 enabled_for Paper v2 的链路。"
                "图谱来源 graph_context，截至 LIVE；QE 候选来源 qe_archive_read_adapter，截至 "
                f"{_payload_as_of(payloads, 'qe_warehouse_business_summary')}；策略治理只读结果用于核对清单和 readiness。"
                "下一步可以先看 promotion candidates 是否 passes_gate，再核对策略包清单与 Paper v2 readiness，避免把候选直接当成已上线。"
            )
        elif "stock_analysis_evidence_card" in modes:
            content = (
                "Bottom-line：这次更适合做证据约束的多维观察，而不是给方向预测；"
                "驱动看行情和资金流，情景看成交量/价格组合变化，风险是单日资金扰动和样本窗口不足，不预测涨跌方向，不构成投资建议。"
                f"来源 stock_analysis_summary_adapter，截至 {_payload_as_of(payloads, 'stock_analysis_evidence_card')}。"
            )
        else:
            content = _agentic_business_answer(kwargs.get("messages")) or "需要先读取审计工具结果后再回答。"
        return LlmCallResult(
            content=content,
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def _payload_as_of(payloads: list[dict[str, object]], response_mode: str) -> str:
    for payload in payloads:
        if str(payload.get("response_mode") or "") == response_mode:
            return str(payload.get("as_of") or payload.get("trade_date") or "2026-06-17")
    return "2026-06-17"


class SemanticPlanningFakeLlmClient(DialogueAwareFakeLlmClient):
    def __init__(self, plan: dict[str, object]) -> None:
        super().__init__()
        self.plan = dict(plan)
        self.plan_calls: list[dict[str, object]] = []

    def complete_tool_plan(self, **kwargs: object) -> LlmCallResult:
        self.plan_calls.append(kwargs)
        return LlmCallResult(
            content=json.dumps(self.plan, ensure_ascii=False),
            provider="fake",
            model="fake-semantic-planner",
            duration_ms=1,
            usage={"prompt_tokens": 12, "completion_tokens": 8},
        )

    def complete(self, **kwargs: object) -> LlmCallResult:
        answer = _agentic_business_answer(kwargs.get("messages"))
        if answer is not None:
            self.calls.append(kwargs)
            return LlmCallResult(
                content=answer,
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )
        return super().complete(**kwargs)


class PromptTooLongOnceLlmClient(FakeLlmClient):
    def __init__(self) -> None:
        super().__init__()
        self.failed_once = False

    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        contents = "\n".join(str(message.get("content", "")) for message in kwargs.get("messages", []))  # type: ignore[union-attr]
        is_main_call = "Context Pack 摘要" in contents
        is_recovery_call = "上一次模型调用因为上下文过长" in contents
        if is_main_call and not is_recovery_call and not self.failed_once:
            self.failed_once = True
            raise RuntimeError("prompt_too_long")
        return LlmCallResult(
            content="已在自动压缩后继续本轮回答，不需要用户重复背景。",
            provider="fake",
            model="fake-primary",
            duration_ms=13,
            usage={"prompt_tokens": 90, "completion_tokens": 35},
        )


class MainPromptTooLongLlmClient(FakeLlmClient):
    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        contents = "\n".join(str(message.get("content", "")) for message in kwargs.get("messages", []))  # type: ignore[union-attr]
        if "Context Pack 摘要" in contents:
            raise RuntimeError("context_length_exceeded")
        return LlmCallResult(
            content="结构化摘要和关键事实提取成功。",
            provider="fake",
            model="fake-primary",
            duration_ms=11,
            usage={"prompt_tokens": 80, "completion_tokens": 30},
        )


def _service() -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository())
    svc.seed_catalogs()
    return svc


def _chat_service(fake: FakeLlmClient | None = None) -> ResearchAssistantService:
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake or FakeLlmClient())
    svc.seed_catalogs()
    return svc


def test_issue_candidate_fact_source_initializes_pipeline_center_lazily() -> None:
    calls: list[str] = []

    class FakePipelineCenter:
        def issue_candidates(self, **kwargs: object) -> dict[str, object]:
            return {"items": [], "total": 0, "page": kwargs.get("page", 1), "page_size": kwargs.get("page_size", 20), "has_more": False}

        def issue_candidate_summary(self, **_kwargs: object) -> dict[str, object]:
            return {"candidate_count": 0, "by_status": {}, "data_state": "complete"}

    def factory() -> FakePipelineCenter:
        calls.append("created")
        return FakePipelineCenter()

    source = IssueCandidateFactSource(pipeline_center_factory=factory)
    assert calls == []
    source.issue_candidates(search="target", page=1, page_size=5)
    assert calls == ["created"]
    source.issue_candidate_summary()
    assert calls == ["created"]


def _write_runtime_config_fixture(tmp_path, mutator) -> object:
    payload = copy.deepcopy(load_runtime_config().config)
    mutator(payload)
    path = tmp_path / "runtime_context.yaml"
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return path


def _reload_runtime_config_fixture(svc: ResearchAssistantService, tmp_path, mutator) -> object:
    path = _write_runtime_config_fixture(tmp_path, mutator)
    svc.reload_declarative_config(runtime_config_path=path)
    return path


def _runtime_config_payload_with_mutated_capability(capability_key: str, field: str, value: object) -> dict[str, object]:
    payload: dict[str, object] = copy.deepcopy(load_runtime_config().config)
    planner = payload["planner"]
    assert isinstance(planner, dict)
    capabilities = planner["workflow_capabilities"]
    assert isinstance(capabilities, list)
    for capability in capabilities:
        if isinstance(capability, dict) and capability.get("capability_key") == capability_key:
            capability[field] = value
            return payload
    raise AssertionError(f"capability not found in runtime config fixture: {capability_key}")


@pytest.mark.parametrize(
    ("bad_value", "expected_actual_type"),
    [
        ("[]", "str"),
        ({"server_key": "research-assistant", "tool_name": "assistant_list_mcp_tools"}, "dict"),
    ],
)
def test_runtime_config_rejects_capability_mcp_tool_refs_non_list(bad_value: object, expected_actual_type: str) -> None:
    payload = _runtime_config_payload_with_mutated_capability("skill_library.reuse", "mcp_tool_refs", bad_value)

    with pytest.raises(RuntimeConfigCapabilityValidationError) as exc_info:
        validate_runtime_config_payload(payload, "unit-test-runtime-config")

    message = str(exc_info.value)
    assert "planner.workflow_capabilities" in message
    assert "capability_key=skill_library.reuse" in message
    assert "field=mcp_tool_refs" in message
    assert f"actual_type={expected_actual_type}" in message


def test_runtime_config_rejects_capability_mcp_tool_refs_non_object_entry() -> None:
    payload = _runtime_config_payload_with_mutated_capability("skill_library.reuse", "mcp_tool_refs", ["not-an-object"])

    with pytest.raises(RuntimeConfigCapabilityValidationError) as exc_info:
        validate_runtime_config_payload(payload, "unit-test-runtime-config")

    message = str(exc_info.value)
    assert "planner.workflow_capabilities" in message
    assert "capability_key=skill_library.reuse" in message
    assert "field=mcp_tool_refs" in message
    assert "mcp_tool_refs[0]" in message
    assert "actual_type=str" in message


class FakeQeExperimentService:
    def list_experiments(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        return {
            "ok": True,
            "total": 3,
            "items": [
                {
                    "experiment_id": "exp_completed",
                    "experiment_name": "alpha baseline",
                    "status": "completed",
                    "model_id": "lgbm_v1",
                    "qe_task_id": "task-custom-1",
                    "qe_loop_id": "loop-1",
                    "loop_index": 1,
                    "ic": 0.031,
                    "rank_ic": 0.044,
                    "information_ratio": 1.21,
                    "updated_at": "2026-06-13T09:30:00+08:00",
                },
                {
                    "experiment_id": "exp_running",
                    "experiment_name": "alpha live loop",
                    "status": "running",
                    "model_id": "catboost_v2",
                    "qe_task_id": "task-custom-2",
                    "loop_index": 2,
                    "rank_ic": 0.052,
                },
                {
                    "experiment_id": "exp_failed",
                    "experiment_name": "alpha failed",
                    "status": "failed",
                    "model_id": "xgb_v1",
                },
            ],
        }


class FakeQeExperimentZeroRunningService:
    def list_experiments(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        return {
            "ok": True,
            "total": 20,
            "items": [
                {"experiment_id": "created-1", "experiment_name": "created draft 1", "status": "created"},
                {"experiment_id": "created-2", "experiment_name": "created draft 2", "status": "created"},
                *[
                    {
                        "experiment_id": f"completed-{index:02d}",
                        "experiment_name": f"completed loop {index:02d}",
                        "status": "completed",
                    }
                    for index in range(1, 19)
                ],
            ],
        }


class FakeQeCustomEvoService:
    async def get_all_tasks(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return [
            {
                "task_id": "task-custom-1",
                "task_name": "custom evo alpha",
                "status": "running",
                "current_loop": 2,
                "max_loops": 5,
                "node_id": "qe-node-a",
                "loop_status_counts": {"completed": 1, "running": 1},
                "updated_at": "2026-06-13T10:00:00+08:00",
            },
            {
                "task_id": "task-custom-2",
                "task_name": "custom evo beta",
                "status": "completed",
                "current_loop": 5,
                "max_loops": 5,
                "loop_status_counts": {"completed": 5},
            },
        ]


class FakeQeArchiveRepository:
    last_leaderboard_kwargs: dict[str, object] = {}

    def get_archive_summary(self) -> dict[str, object]:
        return {
            "run_count": 7,
            "pending_outbox_count": 1,
            "latest_archived_at": "2026-06-13T09:00:00+08:00",
            "skip_count": 2,
            "manual_only_count": 0,
            "outbox_status_counts": {"pending": 1, "succeeded": 8},
            "archive_job_status_counts": {"succeeded": 7},
            "ingest_history_status_counts": {"archived": 7},
            "backfill_run_status_counts": {"completed": 1},
            "research_valid_counts": {"true": 6, "false": 1},
        }

    def list_outbox_events(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return [{"event_id": "evt-1", "event_type": "run", "status": "pending", "retry_count": 0}]

    def list_runs(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return [{"run_id": "run-1", "status": "archived", "model_type": "LightGBM", "cagr": 0.12}]

    def get_analytics_view_status(self) -> list[dict[str, object]]:
        return [{"logical_name": "leaderboard", "view_name": "qe_run_leaderboard", "available": True, "row_count": 5}]

    def query_run_leaderboard(self, **kwargs: object) -> list[dict[str, object]]:
        FakeQeArchiveRepository.last_leaderboard_kwargs = dict(kwargs)
        return [
            {
                "run_id": "run-best",
                "task_id": "task-custom-1",
                "experiment_id": "exp_completed",
                "model_type": "CatBoost",
                "cagr": 0.22,
                "sharpe": 1.4,
                "information_ratio": 1.1,
                "icir": 0.8,
                "completed_at": "2026-06-13T09:00:00+08:00",
            }
        ]

    def query_seed_robustness(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return []

    def query_factor_performance(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return []

    def query_model_hyperparam_seed_perf(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return []

    def query_overfit_flags(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return []

    def query_promotion_candidates(self, **kwargs: object) -> list[dict[str, object]]:
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
                "latest_completed_at": "2026-06-13T09:00:00+08:00",
            }
        ]

    def query_evolution_lineage(self, **kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return []


class FakeLocalDataDailyStatusService:
    def get_preset_daily_status(self) -> dict[str, object]:
        return {
            "operation": "local_data_get_preset_daily_status",
            "risk_level": "read_only",
            "data": {
                "items": {
                    "daily_basic": {"status": "success", "created_at": "2026-06-12T09:00:00+08:00", "finished_at": "2026-06-12T09:02:00+08:00"},
                    "stock_moneyflow_ts": {"status": "failed", "created_at": "2026-06-12T09:03:00+08:00", "finished_at": "2026-06-12T09:04:00+08:00"},
                    "kline_weekly": {"status": "running", "created_at": "2026-06-12T09:05:00+08:00"},
                }
            },
            "trace": {"generated_at": "2026-06-12T01:06:00+00:00"},
        }

    def get_preset_stats(self) -> dict[str, object]:
        return {
            "operation": "local_data_get_preset_stats",
            "risk_level": "read_only",
            "data": {
                "items": [
                    {"dataset": "daily_basic"},
                    {"dataset": "stock_moneyflow_ts"},
                    {"dataset": "kline_weekly"},
                    {"dataset": "anns_metadata"},
                ]
            },
        }

    def list_jobs(self, *, limit: int = 50, active_only: bool = False) -> dict[str, object]:
        del limit, active_only
        return {
            "operation": "local_data_list_jobs",
            "risk_level": "read_only",
            "data": {"items": [{"job_id": "job-live-1", "status": "queued", "summary": {"dataset": "sector_data"}, "created_at": "2026-06-12T09:06:00+08:00"}]},
        }

    def list_sync_targets(self, *, limit: int = 100) -> dict[str, object]:
        del limit
        return {
            "operation": "local_data_list_sync_targets",
            "risk_level": "read_only",
            "data": {
                "items": [
                    {
                        "dataset": "stock_moneyflow_ts",
                        "target_status": "retry",
                        "last_error_message": "upstream timeout",
                    },
                    {
                        "dataset": "anns_metadata",
                        "target_status": "final_blocked",
                        "last_error_message": "schema mismatch",
                    },
                ]
            },
        }


class FailingLocalDataDailyStatusService(FakeLocalDataDailyStatusService):
    def get_preset_daily_status(self) -> dict[str, object]:
        raise ConnectionError("local data facade offline: 127.0.0.1:8001 refused")


def test_catalog_readiness_blocks_chat_until_seeded() -> None:
    fake = FakeLlmClient()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)

    health = svc.health()
    assert health["status"] == "catalog_not_ready"
    assert health["catalog_readiness"]["ready"] is False
    assert "skills" in health["catalog_readiness"]["missing_catalogs"]

    with pytest.raises(ResearchAssistantCatalogNotReadyError) as excinfo:
        svc.chat_turn(ChatTurnRequest(message="帮我设计一个 QE 实验草案，先不要执行。"))
    assert excinfo.value.readiness["operator_action"] == "POST /api/v1/research-assistant/catalogs/seed"
    assert fake.calls == []

    with pytest.raises(ResearchAssistantCatalogNotReadyError):
        svc.build_prompt_bundle(PromptBundleBuildRequest(user_message="QE 实验草案", phase="planning"))

    seed_result = svc.seed_catalogs()
    assert seed_result["seeded"]["prompt_nodes"] == 0
    assert seed_result["seeded"]["skills"] >= 1
    assert svc.health()["status"] == "ok"
    assert svc.catalog_readiness()["ready"] is True


def test_seed_catalogs_leaves_retired_db_activation_projection_untouched() -> None:
    repository = InMemoryResearchAssistantRepository()
    svc = ResearchAssistantService(repository=repository)
    repository.create_record(
        "runtime_config_activations",
        {
            "activation_id": "runtime_config_activation_old_active",
            "config_key": "research_assistant.runtime_context",
            "config_version": "0.0.0",
            "environment": svc.environment,
            "source_id": "runtime_config_source_old",
            "config_json": {},
            "status": "active",
        },
    )
    repository.create_record(
        "prompt_activations",
        {
            "activation_id": "prompt_activation_old_active",
            "assistant_key": "research_assistant",
            "environment": svc.environment,
            "pack_key": "old.prompt.pack",
            "pack_version": "0.0.0",
            "source_id": "prompt_source_old",
            "version_refs": [],
            "bundle_signature": "old",
            "status": "active",
        },
    )

    svc.seed_catalogs()
    svc.seed_catalogs()

    assert repository.get_record("runtime_config_activations", "runtime_config_activation_old_active")["status"] == "active"
    assert repository.get_record("prompt_activations", "prompt_activation_old_active")["status"] == "active"
    prompt_actives = svc.list_records(
        "prompt_activations",
        filters={"assistant_key": "research_assistant", "environment": svc.environment, "status": "active"},
        limit=10,
    )
    runtime_actives = svc.list_records(
        "runtime_config_activations",
        filters={"config_key": "research_assistant.runtime_context", "environment": svc.environment, "status": "active"},
        limit=10,
    )
    assert prompt_actives["declarative_authority"] == "yaml_memory"
    assert runtime_actives["declarative_authority"] == "yaml_memory"
    assert prompt_actives["items"][0]["activation_id"] != "prompt_activation_old_active"
    assert runtime_actives["items"][0]["activation_id"] != "runtime_config_activation_old_active"
    assert svc.catalog_readiness()["ready"] is True


def test_service_runs_phase1_task_memory_context_approval_issue_flow() -> None:
    svc = _service()

    task = svc.create_task(TaskCreate(title="QE 实验规划", idempotency_key="idem-1"))
    assert task["status"] == "planned"
    assert svc.create_task(TaskCreate(title="duplicate", idempotency_key="idem-1"))["task_id"] == task["task_id"]

    event = svc.add_task_event(task["task_id"], TaskEventCreate(event_type="mcp_started", message="preflight"))
    assert event["event_type"] == "mcp_started"
    assert svc.get_task(task["task_id"])["task"]["status"] == "running"

    memory = svc.create_memory(
        MemoryCreate(
            memory_type="core",
            subject_key="assistant.memory",
            title="长期记忆原则",
            content_text="Memory Ledger 是事实源。",
            evidence_refs=["docs/architecture/aistock_research_agent_console_design_20260520.md"],
        )
    )
    assert memory["approval_status"] == "draft"
    memory_approval = svc.create_approval(
        ApprovalCreate(
            task_id=task["task_id"],
            approval_type="memory.approve",
            plan_digest="digest-memory-approve",
            summary="approve assistant.memory",
            required_confirmation_text=ASSISTANT_APPROVAL_CONFIRM,
        )
    )
    with pytest.raises(ValueError, match="requires approval_id"):
        svc.update_memory_status(memory["memory_id"], "approved", approved_by="pytest")
    svc.update_memory_status(
        memory["memory_id"],
        "approved",
        approved_by="pytest",
        approval_id=memory_approval["approval_id"],
        confirmation_text=ASSISTANT_APPROVAL_CONFIRM,
    )

    pack = svc.build_context_pack(ContextPackBuildRequest(task_id=task["task_id"], token_budget=4000))
    assert pack["pack_summary"].startswith("Context Pack:")
    assert memory["memory_id"] in pack["core_memory_refs"]
    access_log = svc.list_records("memory_access_log", filters={"task_id": task["task_id"]})
    assert any(item["memory_id"] == memory["memory_id"] for item in access_log["items"])

    approval = svc.create_approval(
        ApprovalCreate(
            task_id=task["task_id"],
            approval_type="mcp.high_risk",
            plan_digest="digest-123456",
            summary="高风险 MCP 调用",
            required_confirmation_text=ASSISTANT_APPROVAL_CONFIRM,
        )
    )
    with pytest.raises(ValueError, match="confirmation_text"):
        svc.decide_approval(approval["approval_id"], action="approve", confirmation_text="WRONG")
    approved = svc.decide_approval(approval["approval_id"], action="approve", confirmation_text=ASSISTANT_APPROVAL_CONFIRM)
    assert approved["status"] == "approved"

    issue = svc.create_issue_candidate(
        IssueCandidateCreate(title="Candidate defect", severity="P1", problem_statement="must use standard workflow")
    )
    assert issue["status"] == "retired"
    assert issue["standard_workflow_required"] is True
    assert issue["storage_performed"] is False
    assert issue["direct_github_create_performed"] is False
    assert issue["draft_storage_authoritative"] is False
    assert "mcp_github_issue_sync_bug" in issue["recommended_tools"]


def test_retired_issue_candidate_tool_is_not_exposed_in_mcp_catalog() -> None:
    svc = _service()

    assert svc.repository.find_one("mcp_tools", {"server_key": "research-assistant", "tool_name": "assistant_create_issue_candidate"}) is None
    with pytest.raises(KeyError, match="MCP tool not registered in gateway manifest"):
        svc.preflight_mcp_tool(
            McpPreflightRequest(
                server_key="research-assistant",
                tool_name="assistant_create_issue_candidate",
                payload_json={"title": "P1", "problem_statement": "problem"},
            )
        )


def test_model_route_and_temp_memory_are_explicit() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="日志分析"))

    route = svc.route_model(ModelRouteRequest(role="cheap_worker", risk_level="low", token_estimate=1000))
    assert route["route_status"] == "fallback_selected"
    assert route["fallback_reason"]
    assert route["model_profile"]["status"] == "enabled"
    assert route["temp_memory_only_for_low_cost"] is False

    temp = svc.create_temp_memory({"task_id": task["task_id"], "content_text": "低价模型阶段性反馈"})
    assert temp["content_text"] == "低价模型阶段性反馈"
    with pytest.raises(ValueError, match="task_id or stream_id"):
        svc.create_temp_memory({"content_text": "missing scope"})


def test_production_repository_is_default_and_no_silent_in_memory_fallback() -> None:
    svc = ResearchAssistantService()
    assert isinstance(svc.repository, DatabaseResearchAssistantRepository)

    db_repo = DatabaseResearchAssistantRepository(connection_provider=lambda: (_ for _ in ()).throw(RuntimeError("db unavailable")))
    svc = ResearchAssistantService(repository=db_repo)
    with pytest.raises(RuntimeError, match="db unavailable"):
        svc.health()



def test_graph_skill_external_trace_and_workbench_contracts_are_replayable() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="phase1 backend contract"))

    source = svc.create_graph_entity(GraphEntityCreate(entity_type="module", entity_key="qe", title="QE", source_refs=["doc#qe"]))
    target = svc.create_graph_entity(GraphEntityCreate(entity_type="finding", entity_key="gap", title="Gap", source_refs=["doc#gap"]))
    relation = svc.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=source["entity_id"],
            target_entity_id=target["entity_id"],
            relation_type="has_gap",
            evidence_refs=["matrix#17.2"],
        )
    )
    assert relation["evidence_refs"] == ["matrix#17.2"]
    assert svc.get_graph_entity(source["entity_id"])["outgoing_relations"][0]["relation_id"] == relation["relation_id"]
    with pytest.raises(ValueError, match="evidence_refs"):
        svc.create_graph_relation({"source_entity_id": source["entity_id"], "target_entity_id": target["entity_id"], "relation_type": "missing"})

    path = svc.create_evolution_path(
        EvolutionPathCreate(
            stream_id="research-console",
            objective="close phase1 gaps",
            current_best_entity_id=target["entity_id"],
            decision_notes="backend worker",
            evidence_refs=["matrix#graph"],
        )
    )
    assert path["path_id"].startswith("evopath_")

    disabled = svc.set_skill_enabled("qe-evolution-diagnostics", enabled=False)
    assert disabled["status"] == "blocked"
    with pytest.raises(ValueError, match="not enabled"):
        svc.create_skill_usage_event(SkillUsageCreate(skill_key="qe-evolution-diagnostics", task_id=task["task_id"]))
    svc.set_skill_enabled("qe-evolution-diagnostics", enabled=True)
    skill_event = svc.create_skill_usage_event(
        SkillUsageCreate(skill_key="qe-evolution-diagnostics", task_id=task["task_id"], input_summary_json={"experiment_id": "exp"})
    )
    assert skill_event["skill_id"] == "skill_qe-evolution-diagnostics"
    failed_skill_event = svc.create_skill_usage_event(
        SkillUsageCreate(skill_key="qe-evolution-diagnostics", task_id=task["task_id"], status="failed", error_message="boom")
    )
    assert failed_skill_event["status"] == "failed"
    assert svc.get_task(task["task_id"])["task"]["status"] == "triage_required"

    session = svc.create_external_agent_session(
        ExternalAgentSessionCreate(agent_type="codex", agent_name="backend-worker", bound_task_id=task["task_id"], auth_scope={"can_write_evidence": True})
    )
    with pytest.raises(ValueError, match="requires evidence_refs"):
        svc.create_external_agent_event(ExternalAgentEventCreate(session_id=session["session_id"], event_type="context_pack_written"))
    external_event = svc.create_external_agent_event(
        ExternalAgentEventCreate(
            session_id=session["session_id"],
            event_type="context_pack_written",
            payload_json={"context_pack_id": "ctx_x"},
            evidence_refs=["ctx_x"],
        )
    )
    assert external_event["external_event_id"].startswith("extev_")

    trace = svc.create_trace_event(TraceEventCreate(task_id=task["task_id"], event_type="llm_call", component="model_router", status="ok", cost_json={"usd": 0.01}))
    assert trace["trace_id"].startswith("trace_")

    dry_run = svc.dry_run_execute_tool(
        WorkbenchDryRunExecuteRequest(task_id=task["task_id"], server_key="research-assistant", tool_name="assistant_create_task", payload_json={"title": "x"})
    )
    assert dry_run["dry_run"] is True
    assert dry_run["tool_result"]["executed"] is False
    assert dry_run["deep_link"].startswith("/research-assistant/workbench")
    assert svc.list_records("mcp_tool_events", filters={"task_id": task["task_id"]})["total"] >= 1


def _seed_qe_module_graph(svc: ResearchAssistantService) -> None:
    qe = svc.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.qe",
            title="QE",
            summary="Quant evolution module.",
            source_refs=["test://module/qe"],
            approval_status="approved",
        )
    )
    strategy_package = svc.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.strategy_package",
            title="Strategy Package",
            summary="Strategy package module promoted by QE.",
            source_refs=["test://module/strategy-package"],
            approval_status="approved",
        )
    )
    paper_v2 = svc.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.paper_v2",
            title="Paper v2",
            summary="Paper v2 simulation trading module enabled by Strategy Package.",
            source_refs=["test://module/paper-v2"],
            approval_status="approved",
        )
    )
    svc.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=qe["entity_id"],
            target_entity_id=strategy_package["entity_id"],
            relation_type="promotes_to",
            evidence_refs=["test://graph/qe-promotes-strategy-package"],
            approval_status="approved",
        )
    )
    svc.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=strategy_package["entity_id"],
            target_entity_id=paper_v2["entity_id"],
            relation_type="enabled_for",
            evidence_refs=["test://graph/strategy-package-enabled-for-paper-v2"],
            approval_status="approved",
        )
    )


def _seed_chinese_module_graph(svc: ResearchAssistantService) -> None:
    validation = svc.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.validation",
            title="验证",
            summary="Validation module.",
            source_refs=["test://module/validation"],
            approval_status="approved",
        )
    )
    strategy_packages = svc.create_graph_entity(
        GraphEntityCreate(
            entity_type="module",
            entity_key="module.strategy_packages",
            title="策略包",
            summary="Strategy packages module.",
            source_refs=["test://module/strategy-packages"],
            approval_status="approved",
        )
    )
    svc.create_graph_relation(
        GraphRelationCreate(
            source_entity_id=validation["entity_id"],
            target_entity_id=strategy_packages["entity_id"],
            relation_type="governs",
            evidence_refs=["test://graph/validation-governs-strategy-packages"],
            approval_status="approved",
        )
    )


def test_user_message_module_key_seed_expands_matching_module_subgraph() -> None:
    svc = _service()
    _seed_qe_module_graph(svc)

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="Explain the QE module path before drafting a plan.",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    graph_context = pack["pack_json"]["graph_context"]
    assert graph_context["seed_entity_keys"] == ["module.qe"]
    assert pack["graph_relation_refs"]
    assert graph_context["relation_refs"][0]["relation_type"] == "promotes_to"
    assert graph_context["relation_refs"][0]["source_entity_key"] == "module.qe"
    assert graph_context["relation_refs"][0]["neighbor_entity_key"] == "module.strategy_package"


def test_qe_usage_question_expands_qe_strategy_package_paper_v2_graph_chain() -> None:
    svc = _service()
    _seed_qe_module_graph(svc)

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="QE成果怎么利用",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    graph_context = pack["pack_json"]["graph_context"]
    relation_refs = graph_context["relation_refs"]
    assert graph_context["seed_entity_keys"] == ["module.qe"]
    assert {item["relation_type"] for item in relation_refs} >= {"promotes_to", "enabled_for"}
    assert any(item["source_entity_key"] == "module.strategy_package" and item["target_entity_key"] == "module.paper_v2" for item in relation_refs)
    assert graph_context["source"] == "graph_context"
    assert graph_context["as_of"] == "LIVE"


def test_user_message_cjk_module_title_substring_seeds_module_and_expands_neighbor() -> None:
    svc = _service()
    _seed_chinese_module_graph(svc)

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="策略包和验证什么关系",
            dialogue_intent="analysis",
            token_budget=4000,
        )
    )

    graph_context = pack["pack_json"]["graph_context"]
    assert graph_context["seed_entity_keys"] == ["module.strategy_packages"]
    assert graph_context["relation_refs"][0]["relation_type"] == "governs"
    assert graph_context["relation_refs"][0]["source_entity_key"] == "module.validation"
    assert graph_context["relation_refs"][0]["target_entity_key"] == "module.strategy_packages"
    assert graph_context["relation_refs"][0]["neighbor_entity_key"] == "module.validation"


def test_user_message_cjk_common_two_char_title_does_not_substring_seed() -> None:
    svc = _service()
    _seed_chinese_module_graph(svc)

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="我想验证一下这个想法",
            dialogue_intent="dialogue",
            token_budget=4000,
        )
    )

    graph_context = pack["pack_json"]["graph_context"]
    assert graph_context["seed_entity_keys"] == []
    assert graph_context["relation_refs"] == []


def test_user_message_module_key_seed_does_not_fire_without_module_term() -> None:
    svc = _service()
    _seed_qe_module_graph(svc)

    pack = svc.build_context_pack(
        ContextPackBuildRequest(
            user_message="Please answer concisely with evidence.",
            dialogue_intent="dialogue",
            token_budget=4000,
        )
    )

    graph_context = pack["pack_json"]["graph_context"]
    assert graph_context["seed_entity_keys"] == []
    assert graph_context["relation_refs"] == []


def test_graph_entity_keys_for_memory_items_preserves_memory_seed_behavior() -> None:
    svc = _service()

    keys = svc._graph_entity_keys_for_memory_items(
        [
            {
                "subject_key": "project.module.alpha_module",
                "content_json": {"entity_keys": ["module.alpha_module"]},
                "content_text": "alpha module context",
            }
        ],
        user_message="alpha module analysis",
    )

    assert keys == ["module.alpha_module"]


def test_chat_turn_injects_user_seeded_module_graph_into_llm_context() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)
    _seed_qe_module_graph(svc)

    result = svc.chat_turn(ChatTurnRequest(message="Explain QE module dependencies.", dialogue_mode_override="analysis"))

    context_calls = [
        call
        for call in fake.calls
        if "Context Pack Evidence Manifest" in " ".join(str(message.get("content")) for message in call.get("messages", []) if isinstance(message, dict))
    ]
    assert context_calls
    messages = context_calls[0]["messages"]
    assert isinstance(messages, list)
    llm_context = " ".join(str(message.get("content")) for message in messages if isinstance(message, dict))
    assert "Context Pack Evidence Manifest" in llm_context
    assert "module.qe" in llm_context
    assert "module.strategy_package" in llm_context
    assert "promotes_to" in llm_context
    assert "2 graph relations" in result["context_pack"]["pack_summary"]


def test_b2_route_candidates_seed_multi_tool_qe_strategy_paper_path() -> None:
    svc = _chat_service_with_qe_fakes(B2AgenticSynthesisFakeLlmClient())
    _seed_qe_module_graph(svc)

    result = svc.chat_turn(ChatTurnRequest(message="QE成果怎么利用", dialogue_mode_override="analysis"))

    route = result["cards"]["mcp_route_decision"]
    candidate_tools = [candidate["tool_name"] for candidate in route["route_candidates"]]
    assert candidate_tools[:3] == [
        "qe_archive_query_promotion_candidates",
        "strategy_governance_list_packages",
        "strategy_governance_get_paper_readiness",
    ]
    assert route["graph_first"] is True
    react = result["cards"]["react_grounding"]
    assert react["tool_call_count"] >= 3
    assert react["tool_result_count"] >= 4
    text = result["assistant_message"]["content_text"]
    assert "Bottom-line" in text
    assert "graph_context" in text
    assert "LIVE" in text
    assert "qe_archive_read_adapter" in text
    assert "Paper v2" in text
    assert "已完成查询" not in text


def test_b2_non_module_question_does_not_seed_graph_or_graph_result() -> None:
    svc = _chat_service_with_qe_fakes(B2AgenticSynthesisFakeLlmClient())
    _seed_qe_module_graph(svc)

    result = svc.chat_turn(ChatTurnRequest(message="请简要说明今天应该怎么阅读市场新闻", dialogue_mode_override="analysis"))

    assert "0 graph relations" in result["context_pack"]["pack_summary"]
    react = result["cards"]["react_grounding"]
    assert react["tool_result_count"] == 0


def test_candidate_issue_no_storage_response_is_stable_without_repository_row() -> None:
    svc = _service()
    first = svc.create_issue_candidate(IssueCandidateCreate(title="Duplicate Gate", problem_statement="same", dedupe_key="dedupe-x"))
    second = svc.create_issue_candidate(IssueCandidateCreate(title="Duplicate Gate", problem_statement="same", dedupe_key="dedupe-x"))

    assert second["candidate_id"] == first["candidate_id"] == "dedupe-x"
    assert first["storage_performed"] is False
    assert second["storage_performed"] is False
    assert "issue_candidates" not in TABLES


def test_candidate_issue_github_sync_gate_never_creates_github_issue() -> None:
    svc = _service()
    issue = svc.create_issue_candidate(IssueCandidateCreate(title="GitHub gate", problem_statement="must not create directly"))

    dry_run = svc.github_sync_issue_candidate(issue["candidate_id"], IssueCandidateGithubSyncRequest(mode="dry_run", requested_by="pytest"))
    assert dry_run["github_sync_status"] == "blocked"
    assert dry_run["github_sync_json"]["reason"] == "ra_github_sync_retired_use_standard_workflow"
    assert dry_run["github_sync_json"]["direct_github_create_performed"] is False
    assert dry_run["storage_performed"] is False
    assert "mcp_github_issue_sync_bug" in dry_run["github_sync_json"]["recommended_tools"]

    formal_without_approval = svc.github_sync_issue_candidate(issue["candidate_id"], IssueCandidateGithubSyncRequest(mode="formal"))
    assert formal_without_approval["github_sync_status"] == "blocked"
    assert formal_without_approval["github_sync_json"]["direct_github_create_performed"] is False
    assert formal_without_approval["storage_performed"] is False
    assert formal_without_approval["draft_storage_authoritative"] is False
    assert formal_without_approval["github_issue_url"] is None

def test_prompt_tree_capability_inquiry_does_not_trigger_qe_workflow() -> None:
    svc = _chat_service()

    bundle = svc.build_prompt_bundle(
        PromptBundleBuildRequest(
            user_message="目前助手是否可以生成 QE 实验和诊断 bug？",
            phase="planning",
            model_profile_id="model_deepseek_v4_pro_primary",
        )
    )

    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert {"root.assistant", "mode.dialogue"} <= keys
    assert "intent.planning" not in keys
    assert "domain.qe_experiment" not in keys
    assert "workflow.qe_draft_then_approval" not in keys
    assert "tool_guard.mcp_qe" not in keys
    assert "governance.no_silent_action" not in keys
    assert bundle["selection_trace_json"]["algorithm"] == "mode_routed_prompt_tree_v1"
    assert bundle["selection_trace_json"]["dialogue_intent"] == "capability_inquiry"
    assert bundle["selection_trace_json"]["dialogue_mode"] == "dialogue"
    assert bundle["selection_trace_json"]["mode_decision"]["allowed_tool_side_effect"] == "none"
    assert bundle["activation_id"]
    assert bundle["version_refs"]
    assert bundle["selection_trace_json"]["prompt_activation_id"] == bundle["activation_id"]
    assert bundle["cache_path"]


def test_prompt_tree_explicit_qe_draft_selects_qe_without_tool_guard() -> None:
    svc = _chat_service()

    bundle = svc.build_prompt_bundle(
        PromptBundleBuildRequest(
            user_message="帮我设计一个 QE 实验草案，先不要执行。",
            phase="planning",
            model_profile_id="model_deepseek_v4_pro_primary",
        )
    )

    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert {"root.assistant", "mode.planning", "intent.planning", "domain.qe_experiment", "workflow.qe_draft_then_approval"} <= keys
    assert "tool_guard.mcp_qe" not in keys
    assert "governance.no_silent_action" not in keys
    assert bundle["selection_trace_json"]["algorithm"] == "mode_routed_prompt_tree_v1"
    assert bundle["selection_trace_json"]["dialogue_intent"] == "experiment_draft_request"
    assert bundle["selection_trace_json"]["dialogue_mode"] == "planning"


def test_prompt_tree_qe_validate_selects_tool_guard() -> None:
    svc = _chat_service()

    bundle = svc.build_prompt_bundle(PromptBundleBuildRequest(user_message="请验证 QE template。", phase="planning"))

    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert {"mode.preflight", "domain.qe_experiment", "workflow.qe_draft_then_approval", "tool_guard.mcp_qe"} <= keys
    assert bundle["selection_trace_json"]["dialogue_intent"] == "experiment_validation_request"
    assert bundle["selection_trace_json"]["dialogue_mode"] == "preflight"


def test_prompt_tree_ambiguous_task_does_not_start_qe_workflow() -> None:
    svc = _chat_service()

    bundle = svc.build_prompt_bundle(PromptBundleBuildRequest(user_message="请处理一下。", phase="planning"))

    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert {"root.assistant", "mode.analysis"} <= keys
    assert "intent.planning" not in keys
    assert "domain.qe_experiment" not in keys
    assert "workflow.qe_draft_then_approval" not in keys
    assert "tool_guard.mcp_qe" not in keys
    assert bundle["selection_trace_json"]["dialogue_intent"] == "ambiguous_request"
    assert bundle["selection_trace_json"]["dialogue_mode"] == "analysis"



def test_local_data_management_catalog_prompt_and_cards() -> None:
    svc = _chat_service()
    message = "local data sync health check and repair plan before execute"

    capability = svc.repository.find_one("skills", {"skill_key": "local_data_management"})
    assert capability is not None
    assert capability["skill_type"] == "assistant_capability"
    assert capability["entrypoint_ref"] == "aistock-local-data"
    assert "aistock-local-data/local_data_plan_repair" in capability["required_mcp_tools"]

    server = svc.repository.find_one("mcp_servers", {"server_key": "aistock-local-data"})
    assert server is not None
    assert server["health_json"]["capability_key"] == "local_data_management"

    tools = svc.list_records("mcp_tools", filters={"server_key": "aistock-local-data"}, limit=20)["items"]
    tool_names = {tool["tool_name"] for tool in tools}
    assert {"local_data_health_overview", "local_data_get_dataset_status", "local_data_list_sync_targets", "local_data_plan_repair", "local_data_apply_repair_confirmed"} <= tool_names
    apply_tool = svc.repository.find_one("mcp_tools", {"server_key": "aistock-local-data", "tool_name": "local_data_apply_repair_confirmed"})
    assert apply_tool["requires_approval"] is True
    assert apply_tool["required_confirmations"] == [ASSISTANT_APPROVAL_CONFIRM]

    workflow_capability = svc._workflow_capability_by_key("local_data.plan_repair")
    assert workflow_capability is not None
    assert workflow_capability["status"] == "approved"

    bundle = svc.build_prompt_bundle(PromptBundleBuildRequest(user_message=message, phase="planning"))
    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert {"prompt.local_data_management", "workflow.local_data_check_repair", "tool_guard.mcp_local_data"} <= keys
    assert "domain.qe_experiment" not in keys
    assert bundle["selection_trace_json"]["dialogue_intent"] == "local_data_management_request"
    assert bundle["selection_trace_json"]["dialogue_mode"] == "planning"

    result = svc.chat_turn(ChatTurnRequest(message=message))
    assert result["cards"]["intent_type"] == "local_data_management_request"
    assert result["mode_decision"]["mode"] == "planning"
    assert "aistock-local-data" in result["cards"]["capability_summary"]["mcp"]
    assert "local_data_plan_repair" in result["cards"]["capability_summary"]["mcp_tools"]
    assert result["cards"]["safety"]["local_data_read_only_before_confirmation"] is True
    assert result["cards"]["safety"]["no_data_job_before_confirmation"] is True
    assert result["cards"]["local_data_management"]["mcp_server"] == "aistock-local-data"
    assert result["cards"]["action_proposals"][0]["status"] == "read_only"
    assert result["cards"]["action_proposals"][-1]["status"] == "waiting_confirmation"


def test_specific_mcp_domains_are_not_overridden_by_local_data_fallback() -> None:
    svc = _chat_service()
    prompt_only_cases = {
        "strategy package paper readiness": ("strategy_governance_request", "domain.strategy_governance"),
        "sync BUG-120 GitHub issue": ("validation_issue_request", "domain.validation_issue"),
    }
    for message, (expected_intent, expected_prompt) in prompt_only_cases.items():
        bundle = svc.build_prompt_bundle(PromptBundleBuildRequest(user_message=message, phase="planning"))
        keys = {node["prompt_key"] for node in bundle["node_refs"]}
        assert bundle["selection_trace_json"]["dialogue_intent"] == expected_intent
        assert expected_prompt in keys
        assert "prompt.local_data_management" not in keys

    bug_158_cases = {
        "因子库有哪些因子？只要概要列表，不要全量详情。": ("factor_library_request", "domain.factor_library", "aistock-factor"),
        "查看因子独立指标计算能力概要。": ("factor_metrics_request", "domain.factor_metrics", "aistock-factor"),
        "查看因子相关性计算能力概要。": ("factor_correlation_request", "domain.factor_correlation", "aistock-factor"),
        "查看模型库概要。": ("model_registry_request", "domain.model_registry", "aistock-qe"),
        "查看策略库概要。": ("strategy_governance_request", "domain.strategy_governance", "aistock-trading-ops"),
        "查看执行策略库概要。": ("execution_policy_request", "domain.execution_policy", "aistock-trading-ops"),
    }
    for message, (expected_intent, expected_prompt, expected_server) in bug_158_cases.items():
        bundle = svc.build_prompt_bundle(PromptBundleBuildRequest(user_message=message, phase="planning"))
        keys = {node["prompt_key"] for node in bundle["node_refs"]}
        assert bundle["selection_trace_json"]["dialogue_intent"] == expected_intent
        assert expected_prompt in keys
        assert "prompt.local_data_management" not in keys

        result = svc.chat_turn(ChatTurnRequest(message=message))
        assert result["mode_decision"]["intent_type"] == expected_intent
        assert result["cards"].get("local_data_management") is None
        assert result["cards"]["mcp_route_decision"]["server_key"] == expected_server
        assert "local_data" not in result["cards"]["capability_summary"].get("route", "")


def test_bug_160_utf8_business_overviews_keep_specific_mcp_cards() -> None:
    svc = _chat_service()
    cases = {
        "\u56e0\u5b50\u5e93\u6709\u54ea\u4e9b\u56e0\u5b50\uff1f\u53ea\u8981\u6982\u8981\u5217\u8868\uff0c\u4e0d\u8981\u5168\u91cf\u8be6\u60c5\u3002": ("factor_library_request", "aistock-factor", "factor_library_list"),
        "\u67e5\u770b\u56e0\u5b50\u72ec\u7acb\u6307\u6807\u8ba1\u7b97\u80fd\u529b\u6982\u8981\u3002": ("factor_metrics_request", "aistock-factor", "factor_metrics_plan"),
        "\u67e5\u770b\u56e0\u5b50\u76f8\u5173\u6027\u8ba1\u7b97\u80fd\u529b\u6982\u8981\u3002": ("factor_correlation_request", "aistock-factor", "factor_corr_plan"),
        "\u67e5\u770b\u6a21\u578b\u5e93\u6982\u8981\u3002": ("model_registry_request", "aistock-qe", "model_registry_list"),
        "\u67e5\u770b\u7b56\u7565\u5e93\u6982\u8981\u3002": ("strategy_governance_request", "aistock-trading-ops", "strategy_governance_list_packages"),
        "\u67e5\u770b\u6267\u884c\u7b56\u7565\u5e93\u6982\u8981\u3002": ("execution_policy_request", "aistock-trading-ops", "execution_policy_list_algos"),
    }
    for message, (intent, server, tool) in cases.items():
        result = svc.chat_turn(ChatTurnRequest(message=message, allow_execute=False))
        route = result["cards"]["mcp_route_decision"]
        assert result["mode_decision"]["intent_type"] == intent
        assert route["server_key"] == server
        assert route["tool_name"] == tool
        assert route["summary_first"] is True
        assert result["cards"].get("local_data_management") is None
        assert result["cards"]["capability_summary"]["route"] == f"{server}/{tool}"


def test_bug117_prompt_and_health_do_not_expose_undeveloped_capability_bans() -> None:
    svc = _chat_service()

    prompt_nodes = svc.list_records("prompt_nodes", filters={"status": "enabled"}, limit=200)["items"]
    assert prompt_nodes
    for node in prompt_nodes:
        prompt_text = str(node["prompt_text"])
        for phrase in ["禁止控制鼠标键盘", "禁止写代码", "mouse_keyboard_control", "code_write"]:
            assert phrase not in prompt_text

    health = svc.health()
    serialized = str(health)
    assert "mouse_keyboard_control" not in serialized
    assert "code_write" not in serialized
    assert health["implemented_capabilities"]["mcp_api_preflight"] is True
    assert health["governance_boundaries"]["formal_github_issue_requires_approval"] is True
    runtime_code = health["runtime_code"]
    assert runtime_code["schema_version"] == "aistock_research_assistant_runtime_code_visibility_v1"
    assert runtime_code["runtime_loaded_at"]
    assert runtime_code["runtime_loaded_git_commit_short"]
    assert runtime_code["current_repo_git_commit_short"]
    assert isinstance(runtime_code["loaded_source_matches_disk"], bool)
    assert isinstance(runtime_code["restart_required_to_activate_main"], bool)


def test_research_assistant_active_prompt_and_runtime_have_no_default_qe_loop_count() -> None:
    svc = _chat_service()

    prompt_text = "\n".join(str(item["prompt_text"]) for item in svc.list_records("prompt_nodes", limit=100)["items"])
    runtime_text = str(svc.active_runtime_config())

    forbidden = ["QE " + "10 loop", "10" + " 个 loop", "生成 " + "10 个 loop", "10 个 loop" + " 的目标"]
    for phrase in forbidden:
        assert phrase not in prompt_text
        assert phrase not in runtime_text


def test_runtime_config_declares_api_list_limit_for_each_catalog() -> None:
    svc = _service()

    limits = svc.active_runtime_config()["query_limits"]
    missing = sorted(f"api_list_{kind}" for kind in TABLES if f"api_list_{kind}" not in limits)

    assert missing == []


def test_bad_declarative_runtime_config_mcp_tool_refs_returns_specific_config_error(tmp_path) -> None:
    def mutate(config: dict[str, object]) -> None:
        planner = config["planner"]
        assert isinstance(planner, dict)
        capabilities = planner["workflow_capabilities"]
        assert isinstance(capabilities, list)
        for capability in capabilities:
            assert isinstance(capability, dict)
            if capability["capability_key"] == "skill_library.reuse":
                capability["mcp_tool_refs"] = "[]"
                break
        else:
            raise AssertionError("skill_library.reuse capability missing from runtime config")

    runtime_path = _write_runtime_config_fixture(tmp_path, mutate)

    with pytest.raises(ResearchAssistantRuntimeConfigInvalidError) as exc_info:
        ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), runtime_config_path=runtime_path)

    error = exc_info.value.error_payload
    text = str(error)
    assert error["reason_code"] == "declarative_config_invalid_capability_mcp_tool_refs"
    assert error["stage"] == "declarative_config_load"
    assert error["config_key"] == "research_assistant.runtime_context"
    assert error["capability_key"] == "skill_library.reuse"
    assert error["field"] == "mcp_tool_refs"
    assert error["actual_type"] == "str"
    assert "operator_action=fix configs/research_assistant/runtime_context.yaml" in error["message"]
    assert "chat_turn_unexpected_error" not in text


def test_bad_declarative_prompt_pack_returns_specific_config_error(tmp_path) -> None:
    pack_path = tmp_path / "pack.yaml"
    pack_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "aistock_prompt_pack_v1",
                "pack_key": "research_assistant.main",
                "pack_version": "test-bad",
                "nodes": [],
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ResearchAssistantRuntimeConfigInvalidError) as exc_info:
        ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), prompt_pack_path=pack_path)

    error = exc_info.value.error_payload
    assert error["reason_code"] == "declarative_config_invalid_prompt_pack"
    assert error["stage"] == "declarative_config_load"
    assert error["field"] == "prompt_pack"
    assert str(error["source_path"]).endswith("pack.yaml")
    assert "operator_action=fix YAML and restart/reload Research Assistant" in error["message"]


def test_declarative_config_uses_yaml_authority_without_db_projection_after_seed() -> None:
    svc = _chat_service()
    memory_by_key = {item["capability_key"]: item for item in svc._workflow_capabilities()}
    assert svc.repository.list_records("capabilities", limit=svc.configured_limit("api_list_capabilities"))["total"] == 0

    listed = svc.list_records("capabilities", limit=svc.configured_limit("api_list_capabilities"))
    listed_by_key = {item["capability_key"]: item for item in listed["items"]}

    assert listed["declarative_authority"] == "yaml_memory"
    assert set(listed_by_key) == set(memory_by_key)
    for key, memory_item in memory_by_key.items():
        listed_item = listed_by_key[key]
        for field in ("capability_key", "mcp_tool_refs", "skill_refs", "risk_level", "side_effect_level", "status"):
            assert listed_item[field] == memory_item[field]


def test_declarative_prompt_pack_uses_yaml_authority_and_db_pollution_is_ignored() -> None:
    svc = _chat_service()
    memory_by_key = {item["prompt_key"]: item for item in svc.declarative_config.prompt_node_list()}
    assert svc.repository.list_records("prompt_nodes", limit=svc.configured_limit("prompt_nodes_active"))["total"] == 0

    listed_before = svc.list_records("prompt_nodes", limit=svc.configured_limit("prompt_nodes_active"))
    listed_by_key = {item["prompt_key"]: item for item in listed_before["items"]}
    assert listed_before["declarative_authority"] == "yaml_memory"
    assert set(listed_by_key) == set(memory_by_key)

    root = dict(memory_by_key["root.assistant"])
    root["prompt_text"] = "DB projection polluted"
    root["checksum"] = "dirty-db-prompt-checksum"
    svc.repository.create_record("prompt_nodes", root)

    assert svc._prompt_text("root.assistant") == memory_by_key["root.assistant"]["prompt_text"]
    assert svc._prompt_text_for_key("root.assistant") == memory_by_key["root.assistant"]["prompt_text"]
    listed = svc.list_records("prompt_nodes", filters={"prompt_key": "root.assistant"})
    assert listed["declarative_authority"] == "yaml_memory"
    assert listed["items"][0]["prompt_text"] == memory_by_key["root.assistant"]["prompt_text"]
    assert svc.repository.find_one("prompt_nodes", {"prompt_key": "root.assistant"})["prompt_text"] == "DB projection polluted"


def test_db_runtime_config_projection_mutation_is_not_declarative_read_authority() -> None:
    svc = _chat_service()
    activation_id = svc.active_runtime_config_activation()["activation_id"]
    assert svc.repository.get_record("runtime_config_activations", activation_id) is None
    active = svc.active_runtime_config_activation()
    config = copy.deepcopy(active["config_json"])
    for capability in config["planner"]["workflow_capabilities"]:
        if capability["capability_key"] == "skill_library.reuse":
            capability["mcp_tool_refs"] = "[]"
            break
    else:
        raise AssertionError("skill_library.reuse capability missing from projected runtime config")
    dirty = dict(active)
    dirty["config_json"] = config
    svc.repository.create_record("runtime_config_activations", dirty)

    result = svc.chat_turn(ChatTurnRequest(message="stock analysis question", dialogue_mode_override="analysis"))

    text = result["assistant_message"]["content_text"]
    assert "runtime_config_invalid_capability_mcp_tool_refs" not in text
    assert "declarative_config_invalid_capability_mcp_tool_refs" not in text
    assert "chat_turn_unexpected_error" not in text
    assert "error_card" not in result["cards"]
    listed = svc.list_records("runtime_config_activations", filters={"status": "active"})
    assert listed["declarative_authority"] == "yaml_memory"
    assert listed["items"][0]["config_json"]["planner"]["workflow_capabilities"] != config["planner"]["workflow_capabilities"]


def test_active_runtime_config_accepts_empty_mcp_tool_refs_list() -> None:
    svc = _chat_service()
    capabilities = svc._workflow_capabilities()

    reuse = next(item for item in capabilities if item["capability_key"] == "skill_library.reuse")

    assert reuse["mcp_tool_refs"] == []


def test_bug_439_empty_registry_mcp_tool_refs_shape_is_ignored_by_declarative_reads(caplog: pytest.LogCaptureFixture) -> None:
    svc = _chat_service()
    capability = svc._workflow_capability_by_key("skill_library.reuse")
    assert capability is not None
    capability["capability_id"] = "cap_skill_library_reuse_dirty_empty"
    capability["mcp_tool_refs"] = {}
    svc.repository.create_record("capabilities", capability)

    caplog.set_level(logging.WARNING)
    result = svc.chat_turn(ChatTurnRequest(message="stock analysis question", dialogue_mode_override="analysis"))

    text = result["assistant_message"]["content_text"]
    unchanged = svc.repository.find_one("capabilities", {"capability_key": "skill_library.reuse"})
    assert unchanged is not None
    assert unchanged["mcp_tool_refs"] == {}
    assert "chat_turn_unexpected_error" not in text
    assert result["cards"].get("error_card", {}).get("reason_code") != "chat_turn_unexpected_error"
    assert "capability registry empty ref shape repaired" not in caplog.text


def test_bug_439_non_empty_registry_mcp_tool_refs_dict_is_ignored_by_declarative_reads() -> None:
    svc = _chat_service()
    capability = svc._workflow_capability_by_key("skill_library.reuse")
    assert capability is not None
    capability["capability_id"] = "cap_skill_library_reuse_dirty_non_empty"
    dirty_refs = {"server_key": "research-assistant", "tool_name": "assistant_list_mcp_tools"}
    capability["mcp_tool_refs"] = dirty_refs
    svc.repository.create_record("capabilities", capability)

    result = svc.chat_turn(ChatTurnRequest(message="stock analysis question", dialogue_mode_override="analysis"))

    text = result["assistant_message"]["content_text"]
    unchanged = svc.repository.find_one("capabilities", {"capability_key": "skill_library.reuse"})
    assert unchanged is not None
    assert unchanged["mcp_tool_refs"] == dirty_refs
    assert "capability_registry_invalid_mcp_tool_refs" not in text
    assert "chat_turn_unexpected_error" not in text
    assert "error_card" not in result["cards"]
    listed = svc.list_records("capabilities", filters={"capability_key": "skill_library.reuse"})
    assert listed["declarative_authority"] == "yaml_memory"
    assert listed["items"][0]["mcp_tool_refs"] == []


def test_bug_439_repository_json_adapter_preserves_empty_lists() -> None:
    assert str(_adapt_json([])) == "'[]'"
    assert str(_adapt_json({})) == "'{}'"


def test_bug_439_sync_is_retired_noop_and_does_not_repair_dirty_registry_refs() -> None:
    svc = _chat_service()
    capability = svc._workflow_capability_by_key("skill_library.reuse")
    assert capability is not None
    capability["capability_id"] = "cap_skill_library_reuse_dirty_sync"
    capability["mcp_tool_refs"] = {}
    svc.repository.create_record("capabilities", capability)

    dry_run = svc.sync_capabilities({"apply": False, "requested_by": "bug_439_unit"})
    reuse_diff = next(item for item in dry_run["diff"] if item["capability_key"] == "skill_library.reuse")

    assert dry_run["db_projection_retired"] is True
    assert reuse_diff["change"] == "retired_db_projection"
    assert reuse_diff["reason"] == "yaml_memory_authority_no_db_write"

    applied = svc.sync_capabilities({"apply": True, "requested_by": "bug_439_unit"})
    repaired = svc.repository.find_one("capabilities", {"capability_key": "skill_library.reuse"})
    assert applied["applied_count"] == 0
    assert applied["db_projection_retired"] is True
    assert repaired is not None
    assert repaired["mcp_tool_refs"] == {}


def test_bug_439_sync_ignores_non_empty_non_list_registry_refs_after_db_projection_retired() -> None:
    svc = _chat_service()
    capability = svc._workflow_capability_by_key("skill_library.reuse")
    assert capability is not None
    capability["capability_id"] = "cap_skill_library_reuse_dirty_sync_non_empty"
    capability["mcp_tool_refs"] = {"server_key": "research-assistant", "tool_name": "assistant_list_mcp_tools"}
    svc.repository.create_record("capabilities", capability)

    result = svc.sync_capabilities({"apply": True, "requested_by": "bug_439_unit"})
    unchanged = svc.repository.find_one("capabilities", {"capability_key": "skill_library.reuse"})

    assert result["db_projection_retired"] is True
    assert result["applied_count"] == 0
    assert unchanged is not None
    assert unchanged["mcp_tool_refs"] == {"server_key": "research-assistant", "tool_name": "assistant_list_mcp_tools"}


def test_bug_439_catalog_ready_path_does_not_make_db_projection_authoritative() -> None:
    svc = _chat_service()
    capability = svc._workflow_capability_by_key("skill_library.reuse")
    assert capability is not None
    capability["capability_id"] = "cap_skill_library_reuse_dirty_catalog_ready"
    capability["mcp_tool_refs"] = {}
    svc.repository.create_record("capabilities", capability)

    readiness = svc.ensure_catalog_ready()

    unchanged = svc.repository.find_one("capabilities", {"capability_key": "skill_library.reuse"})
    assert readiness["ready"] is True
    assert unchanged is not None
    assert unchanged["mcp_tool_refs"] == {}


def test_dirty_db_declarative_rows_do_not_change_yaml_backed_behavior() -> None:
    svc = _chat_service()
    baseline_capability_key = svc._capability_key_for_tool(
        McpToolCall(server_key="aistock-stock-analysis", tool_name="stock_analysis_get_quote", payload_json={})
    )
    baseline_prompt = svc._prompt_text("root.assistant")
    baseline_runtime_activation = svc.active_runtime_config_activation()["activation_id"]

    dirty_capability = dict(svc._workflow_capability_by_key("stock_analysis.mcp_orchestration") or {})
    dirty_capability["capability_id"] = "cap_stock_analysis_dirty_db_projection"
    dirty_capability["mcp_tool_refs"] = []
    dirty_capability["checksum"] = "dirty-db-capability-checksum"
    svc.repository.create_record("capabilities", dirty_capability)

    dirty_prompt = dict(svc.declarative_config.prompt_node("root.assistant") or {})
    dirty_prompt["prompt_text"] = "Dirty DB prompt projection must not be read"
    dirty_prompt["checksum"] = "dirty-db-prompt-checksum"
    svc.repository.create_record("prompt_nodes", dirty_prompt)

    dirty_runtime = dict(svc.active_runtime_config_activation())
    dirty_runtime["config_json"] = {"planner": {"workflow_capabilities": []}}
    svc.repository.create_record("runtime_config_activations", dirty_runtime)

    assert svc._capability_key_for_tool(
        McpToolCall(server_key="aistock-stock-analysis", tool_name="stock_analysis_get_quote", payload_json={})
    ) == baseline_capability_key
    assert svc._prompt_text("root.assistant") == baseline_prompt
    assert svc.active_runtime_config_activation()["activation_id"] == baseline_runtime_activation
    assert svc.list_records("capabilities", filters={"capability_key": "stock_analysis.mcp_orchestration"})["items"][0]["mcp_tool_refs"]
    assert svc.list_records("prompt_nodes", filters={"prompt_key": "root.assistant"})["items"][0]["prompt_text"] == baseline_prompt
    assert svc.list_records("runtime_config_activations", filters={"status": "active"})["declarative_authority"] == "yaml_memory"


def test_runtime_config_reload_updates_former_db_bypass_capability_reads(tmp_path) -> None:
    svc = _chat_service()
    route_call = McpToolCall(server_key="research-assistant", tool_name="assistant_create_memory_candidate", payload_json={})
    assert svc._capability_key_for_tool(route_call) == "memory.write_candidate"

    def mutate(config: dict[str, object]) -> None:
        for capability in config["planner"]["workflow_capabilities"]:
            if capability["capability_key"] == "memory.write_candidate":
                capability["mcp_tool_refs"] = [
                    ref
                    for ref in capability["mcp_tool_refs"]
                    if ref["tool_name"] != "assistant_create_memory_candidate"
                ]
                capability["title"] = "Memory write reload test"
                return
        raise AssertionError("memory.write_candidate missing from runtime config")

    _reload_runtime_config_fixture(svc, tmp_path, mutate)

    reloaded = svc._workflow_capability_by_key("memory.write_candidate")
    assert reloaded is not None
    assert reloaded["title"] == "Memory write reload test"
    assert not svc._capability_has_tool_ref(reloaded, "research-assistant", "assistant_create_memory_candidate")
    assert ("research-assistant", "assistant_create_memory_candidate") not in svc._approved_capability_mcp_tool_refs()
    with pytest.raises(KeyError, match="approved capability not found"):
        svc._capability_key_for_tool(route_call)


def test_runtime_config_controls_api_page_defaults_and_max(tmp_path) -> None:
    svc = _service()

    def mutate(config: dict[str, object]) -> None:
        query_limits = dict(config["query_limits"])
        query_limits["api_list_skills"] = 2
        query_limits["api_list_max_page_size"] = 3
        query_limits["router_mcp_servers"] = 1
        config["query_limits"] = query_limits

    _reload_runtime_config_fixture(svc, tmp_path, mutate)

    skills = svc.list_records("skills")
    assert skills["page_size"] == 2
    assert len(skills["items"]) == 2

    mcp_servers = svc.list_records("mcp_servers", limit_key="router_mcp_servers")
    assert mcp_servers["page_size"] == 1

    with pytest.raises(ValueError, match="api_list_max_page_size"):
        svc.list_records("skills", limit=4)
    with pytest.raises(ValueError, match="limit must be positive"):
        svc.list_records("skills", limit=0)


def test_context_pack_token_budget_max_is_runtime_config_driven(tmp_path) -> None:
    svc = _service()

    def mutate(config: dict[str, object]) -> None:
        query_limits = dict(config["query_limits"])
        query_limits["context_pack_max_token_budget"] = 9
        config["query_limits"] = query_limits

    _reload_runtime_config_fixture(svc, tmp_path, mutate)
    task = svc.create_task(TaskCreate(title="context pack budget gate"))

    with pytest.raises(ValueError, match="context_pack_max_token_budget"):
        svc.build_context_pack(ContextPackBuildRequest(task_id=task["task_id"], token_budget=10))

    pack = svc.build_context_pack(ContextPackBuildRequest(task_id=task["task_id"], token_budget=9))
    assert pack["token_budget"] == 9


def test_chat_turn_capability_inquiry_answers_without_workflow_noise() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="目前助手是否可以生成 QE 实验和诊断 bug？"))

    assert len(fake.calls) == 1
    assert result["assistant_message"]["content_text"].startswith("可以。我能生成 QE 实验草案")
    assert "诊断 bug" in result["assistant_message"]["content_text"]
    assert "请先确认" not in result["assistant_message"]["content_text"]
    assert "materialize/run" not in result["assistant_message"]["content_text"]
    assert result["cards"]["intent_type"] == "capability_inquiry"
    assert "plan_card" not in result["cards"]
    assert "clarification_card" not in result["cards"]
    assert result["cards"]["ui_display"]["show_plan_card"] is False
    assert result["cards"]["ui_display"]["show_context_health_badge"] is False
    assert result["cards"]["action_proposals"] == []
    capability_keys = {item["capability_key"] for item in result["cards"]["capability_cards"]}
    assert {"qe.create_experiment_draft", "qe.validate_template", "qe.run_experiment"} <= capability_keys
    assert result["cards"]["status_rail"][2] == {"label": "回答", "status": "done"}
    assert result["trace"]["status"] == "ok"
    assert result["context_health"]["show_badge"] is False
    assert result["mode_decision"]["mode"] == "dialogue"
    assert result["mode_decision"]["allowed_tool_side_effect"] == "none"
    assert result["prompt_bundle"]["selection_trace_json"]["algorithm"] == "mode_routed_prompt_tree_v1"
    assert result["prompt_bundle"]["selection_trace_json"]["dialogue_intent"] == "capability_inquiry"
    assert result["prompt_bundle"]["selection_trace_json"]["dialogue_mode"] == "dialogue"
    event_types = {event["event_type"] for event in result["task_events"]}
    assert {"chat_received", "prompt_bundle_built", "context_pack_built", "llm_started", "llm_done"} <= event_types
    assert "action_proposed" not in event_types


def test_chat_turn_mcp_tool_inquiry_uses_runtime_catalog_not_generic_tool_claims() -> None:
    class GenericToolHallucinationLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="I can use MCP tools for reading files, writing files, editing files, Git operations, HTTP requests, and no direct warehouse tool.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = GenericToolHallucinationLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="What MCP tools are available?"))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    assert isinstance(messages, list)
    catalog_context = "\n".join(str(item.get("content", "")) for item in messages if isinstance(item, dict))
    assert "Runtime MCP catalog snapshot" in catalog_context
    assert "assistant_create_task" in catalog_context
    assert "qe_template_create" in catalog_context
    assert "mcp_github_issue_create" in catalog_context

    text = result["assistant_message"]["content_text"]
    assert "assistant create task" in text
    assert "qe_template_create" in catalog_context
    assert "mcp_github_issue_create" in catalog_context
    assert "assistant_create_issue_candidate" not in text
    assert "qe_template_create" not in text
    assert "mcp_github_issue_create" not in text
    assert "reading files" not in text
    assert "writing files" not in text
    assert "HTTP requests" not in text
    assert "no direct warehouse tool" not in text
    catalog = result["cards"]["runtime_mcp_catalog"]
    assert catalog["source"] == "gateway_manifest_derived_catalog"
    assert catalog["manifest_tool_count"] == len(TOOL_MANIFEST)
    assert catalog["tool_count"] == len(TOOL_MANIFEST)
    assert result["mode_decision"]["intent_type"] == "capability_inquiry"
    assert result["cards"]["action_proposals"] == []


def test_chat_turn_chinese_factor_library_request_does_not_surface_mock_counts() -> None:
    class FactorHallucinationLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="因子库目前有 10 个已注册因子：alpha_001、alpha_002 等。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(FactorHallucinationLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="帮我看看因子库有哪些可用因子"))

    text = result["assistant_message"]["content_text"]
    assert "10 个已注册因子" not in text
    assert "factor library list" in text
    assert "summary-first" not in text
    assert "Route decision" not in text
    assert result["mode_decision"]["intent_type"] == "factor_library_request"
    assert result["cards"]["mcp_route_decision"]["domain"] == "factor_library"
    assert result["cards"]["mcp_route_decision"]["summary_first"] is True
    keys = set(result["prompt_bundle"]["selected_prompt_keys"])
    assert "domain.factor_library" in keys


def test_chat_turn_auto_executes_read_only_mcp_summary_cards() -> None:
    class FactorHallucinationLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="There are exactly 999 factors: fake_alpha.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(FactorHallucinationLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="List available factor library entries as a compact summary."))

    text = result["assistant_message"]["content_text"]
    assert "999 factors" not in text
    assert "factor library list" in text
    _assert_no_mcp_process_markers(text)
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["status"] == "succeeded"
    assert execution["route"] == "aistock-factor/factor_library_list"
    assert execution["summary_first"] is True
    assert execution["response_summary"]["returned_count"] >= 1
    assert result["cards"]["mcp_tool_event"]["transport"] == "research_assistant_catalog_summary_adapter"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["summary_first"] is True
    assert summary["response_mode"] == "summary"
    assert summary["artifact_refs"]
    forbidden = {"metrics_json", "config_json", "raw_payload", "matrix", "logs", "rows", "model_weights", "training_curves"}

    def walk(value: object) -> None:
        if isinstance(value, dict):
            assert not (set(value) & forbidden)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(summary)
    assert any(event["event_type"] == "mcp_done" for event in result["task_events"])


def test_bug_359_generic_mcp_reply_renderer_hides_process_markers_across_domains() -> None:
    class DiagnosticLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content=(
                    "已通过只读工具完成 MCP summary-first 查询；Route decision：x/y；"
                    "server_key=x tool_name=y source=research_assistant_catalog_summary_adapter as_of=2026-06-13"
                ),
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    cases = [
        ("因子库有哪些因子？只要概要列表。", "factor library list"),
        ("模型库有什么模型？给我概要。", "model registry list"),
        ("策略库目前有哪些策略？", "strategy governance list packages"),
        ("执行策略库有什么 minute algo？", "execution policy list algos"),
        ("检索 A 股多因子稳定性的论文线索。", "external evidence candidate"),
    ]
    for message, expected_business_text in cases:
        svc = _chat_service(DiagnosticLlmClient())
        result = svc.chat_turn(ChatTurnRequest(message=message))
        text = result["assistant_message"]["content_text"]
        _assert_no_mcp_process_markers(text)
        assert expected_business_text in text
        assert "已完成" in text
        assert "本轮只进行查询" in text


def test_bug_359_generic_preflight_reply_replaces_insufficient_evidence_for_non_readonly_tools() -> None:
    class InsufficientEvidenceLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="Insufficient evidence: max tool iterations reached without reliable evidence.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    cases = [
        "repair stock_daily trade_date local data plan",
        "把这个策略晋升到 paper v2",
        "查看因子相关性计算能力概要",
    ]
    for message in cases:
        svc = _chat_service(InsufficientEvidenceLlmClient())
        result = svc.chat_turn(ChatTurnRequest(message=message))
        text = result["assistant_message"]["content_text"]
        _assert_no_mcp_process_markers(text)
        assert "Insufficient evidence" not in text
        assert "安全边界" in text
        assert "本轮未执行" in text


BUG_356_FORBIDDEN_REPLY_MARKERS = (
    "summary-first",
    "Route decision",
    "artifact_ref",
    "payload budget",
    "Evidence: source=",
    "research_assistant_catalog_summary_adapter",
    "local_data_health_overview",
    "\u6211\u53ea\u5c55\u793a\u6982\u8981",
)


BUG_357_FORBIDDEN_REPLY_MARKERS = (
    "summary-first",
    "summary_first",
    "Route decision",
    "route decision",
    "artifact_ref",
    "payload budget",
    "Evidence: source=",
    "source=",
    "as_of=",
    "research_assistant_catalog_summary_adapter",
    "summary_adapter",
    "server_key",
    "tool_name",
    "\u6211\u53ea\u5c55\u793a\u6982\u8981",
    "Insufficient evidence",
    "max tool iterations",
    "已汇总",
    "状态汇总：",
)


def _chat_service_with_qe_fakes(fake: FakeLlmClient | None = None) -> ResearchAssistantService:
    svc = _chat_service(fake or AgenticBusinessSynthesisFakeLlmClient())
    svc.qe_experiment_service_factory = FakeQeExperimentService
    svc.qe_custom_evo_service_factory = FakeQeCustomEvoService
    svc.qe_archive_repository_factory = FakeQeArchiveRepository
    return svc


def _assert_bug_357_no_diagnostic_reply(result: dict[str, object]) -> str:
    text = result["assistant_message"]["content_text"]  # type: ignore[index]
    for marker in BUG_357_FORBIDDEN_REPLY_MARKERS:
        assert marker not in text
    return text


def _assert_no_mcp_process_markers(text: str) -> None:
    for marker in BUG_357_FORBIDDEN_REPLY_MARKERS + BUG_356_FORBIDDEN_REPLY_MARKERS:
        assert marker not in text


def _assert_bug_356_business_local_data_reply(result: dict[str, object]) -> None:
    text = result["assistant_message"]["content_text"]  # type: ignore[index]
    assert "success=1" in text
    assert "failed=1" in text
    assert "running=2" in text
    assert "blocked=1" in text
    assert "stock_moneyflow_ts" in text
    assert "来源 local_data_facade_read_adapter" in text
    for marker in BUG_356_FORBIDDEN_REPLY_MARKERS:
        assert marker not in text

    cards = result["cards"]  # type: ignore[index]
    route = cards["mcp_route_decision"]  # type: ignore[index]
    assert route["tool_name"] == "local_data_get_preset_daily_status"
    execution = cards["mcp_execution_result"]  # type: ignore[index]
    assert execution["auto_executed"] is True
    assert execution["tool_name"] == "local_data_get_preset_daily_status"
    summary = cards["mcp_summary_result"]  # type: ignore[index]
    assert summary["response_mode"] == "local_data_daily_sync_status"
    assert summary["source"] == "local_data_facade_read_adapter"



def test_bug_357_qe_experiment_list_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()

    result = svc.chat_turn(ChatTurnRequest(message="\u76ee\u524d\u6700\u8fd1\u7684 QE \u5b9e\u9a8c\u6709\u54ea\u4e9b\uff1f\u7ed9\u6211\u4e00\u4e2a\u5217\u8868\u548c\u72b6\u6001\u6c47\u603b"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "当前 QE 实验证据显示" in text
    assert "completed=1" in text
    assert "running=1" in text
    assert "failed=1" in text
    assert "alpha baseline" in text
    assert "来源 qe_experiment_read_adapter" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_experiment"
    assert route["tool_name"] == "qe_experiment_list"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_experiment_status_summary"
    assert summary["source"] == "qe_experiment_read_adapter"
    assert summary["summary_kind"] == "qe_experiments"
    assert result["cards"]["react_grounding"]["stopped_reason"] == "final_answer"


def test_bug_357_qe_custom_evo_progress_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()

    result = svc.chat_turn(ChatTurnRequest(message="custom_evo \u4efb\u52a1\u6700\u65b0\u8fdb\u5ea6\u600e\u4e48\u6837\uff1f\u7ed9\u6211\u72b6\u6001\u6c47\u603b"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "当前 QE 实验证据显示" in text
    assert "custom evo alpha" in text
    assert "completed=1" in text
    assert "running=1" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_experiment"
    assert route["tool_name"] == "qe_experiment_list"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_experiment_status_summary"
    assert summary["summary_kind"] == "custom_evo_tasks"
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True


def test_bug_357_qe_warehouse_health_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()

    result = svc.chat_turn(ChatTurnRequest(message="QE \u6570\u4ed3\u73b0\u5728\u662f\u5426\u6b63\u5e38\uff1f\u7ed9\u6211\u5065\u5eb7\u72b6\u6001\u548c\u5165\u4ed3\u6c47\u603b"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "QE 数仓当前可读" in text
    assert "run_count=7" in text
    assert "pending_outbox=1" in text
    assert "research_valid" in text
    assert "来源 qe_archive_read_adapter" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_warehouse"
    assert route["tool_name"] == "qe_archive_health"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_warehouse_business_summary"
    assert summary["source"] == "qe_archive_read_adapter"


def test_bug_357_qe_warehouse_leaderboard_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()
    FakeQeArchiveRepository.last_leaderboard_kwargs = {}

    result = svc.chat_turn(ChatTurnRequest(message="\u67e5\u770b QE run leaderboard\uff0c\u544a\u8bc9\u6211\u6700\u597d\u7684\u6a21\u578b\u548c\u5173\u952e\u6307\u6807"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "按 CAGR 口径看" in text
    assert "run-best" in text
    assert "CatBoost" in text
    assert "22.00%" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_warehouse"
    assert route["tool_name"] == "qe_archive_query_run_leaderboard"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_warehouse_business_summary"
    assert summary["summary_kind"] == "query_run_leaderboard"
    assert FakeQeArchiveRepository.last_leaderboard_kwargs["order_by"] == "cagr"


def test_bug_376_qe_archive_best_return_question_gets_human_leaderboard_answer() -> None:
    svc = _chat_service_with_qe_fakes()
    FakeQeArchiveRepository.last_leaderboard_kwargs = {}

    result = svc.chat_turn(ChatTurnRequest(message="目前进入数仓的 QE 实验，回测效果最好的收益是多少？是哪个实验？"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "按 CAGR 口径看" in text
    assert "exp_completed" in text
    assert "CAGR=22.00%" in text
    assert "run-best" in text
    assert "模型=CatBoost" in text
    assert "run_count=7" not in text
    assert "pending_outbox=1" not in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_warehouse"
    assert route["tool_name"] == "qe_archive_query_run_leaderboard"
    assert route["side_effect"] == "read_only"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["summary_kind"] == "query_run_leaderboard"
    assert FakeQeArchiveRepository.last_leaderboard_kwargs["order_by"] == "cagr"


def test_bug_379_semantic_planner_overrides_legacy_route_without_new_phrase_rules() -> None:
    fake = SemanticPlanningFakeLlmClient(
        {
            "status": "tool_plan",
            "domain": "qe_warehouse",
            "server_key": "aistock-qe",
            "tool_name": "qe_archive_query_run_leaderboard",
            "tool_args": {"order_by": "cagr", "limit": 1},
            "confidence": 0.91,
            "reason": "The user asks for the best archived QE result by return, so a run leaderboard query is the relevant read-only evidence.",
        }
    )
    svc = _chat_service_with_qe_fakes(fake)
    FakeQeArchiveRepository.last_leaderboard_kwargs = {}

    result = svc.chat_turn(ChatTurnRequest(message="帮我从 QE archive 中找一个按 CAGR 口径的冠军样本，并说出它的数值。"))

    route = result["cards"]["mcp_route_decision"]
    assert route["planner_source"] == "llm_semantic_tool_planner"
    assert route["tool_name"] == "qe_archive_query_run_leaderboard"
    assert route["tool_args"]["order_by"] == "cagr"
    assert route["limit"] == 1
    assert FakeQeArchiveRepository.last_leaderboard_kwargs["order_by"] == "cagr"
    assert FakeQeArchiveRepository.last_leaderboard_kwargs["limit"] == 1
    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "CAGR=22.00%" in text
    assert "run_count=7" not in text
    assert len(fake.plan_calls) == 1
    planner_context = "\n".join(str(message.get("content", "")) for message in fake.plan_calls[0]["messages"])  # type: ignore[index]
    assert "Do not use keyword matching or synonym lists" in planner_context
    assert "audited_tools" in planner_context


def test_bug_379_semantic_planner_asks_metric_clarification_for_underspecified_best() -> None:
    fake = SemanticPlanningFakeLlmClient(
        {
            "status": "clarification",
            "domain": "qe_warehouse",
            "confidence": 0.82,
            "reason": "The user asks for the best QE experiment but does not state whether best means return, risk-adjusted return, stability, or latest status.",
            "clarification_questions": ["你希望按收益、Sharpe/IR、稳定性，还是最新状态来判断“最好”？"],
        }
    )
    svc = _chat_service_with_qe_fakes(fake)
    FakeQeArchiveRepository.last_leaderboard_kwargs = {}

    result = svc.chat_turn(ChatTurnRequest(message="QE 实验里哪个最好？"))

    route = result["cards"]["mcp_route_decision"]
    assert route["planner_source"] == "llm_semantic_tool_planner"
    assert route["requires_clarification"] is True
    assert "mcp_execution_result" not in result["cards"]
    assert FakeQeArchiveRepository.last_leaderboard_kwargs == {}
    text = result["assistant_message"]["content_text"]
    assert "需要先确认比较口径" in text
    assert "收益、Sharpe/IR、稳定性" in text
    assert "QE 实验状态" not in text


def test_bug_357_qe_draft_does_not_auto_execute_or_surface_insufficient_evidence() -> None:
    class InsufficientEvidenceDraftLlm(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="Insufficient evidence: max tool iterations reached without reliable evidence.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service_with_qe_fakes(InsufficientEvidenceDraftLlm())

    result = svc.chat_turn(ChatTurnRequest(message="\u5e2e\u6211\u8bbe\u8ba1\u4e00\u4e2a QE \u5b9e\u9a8c\u8349\u6848\uff0c\u5148\u4e0d\u8981\u6267\u884c\u3002"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "\u5df2\u6536\u5230 QE \u5b9e\u9a8c\u8349\u6848\u9700\u6c42" in text
    assert "\u672c\u8f6e\u53ea\u751f\u6210\u65b9\u6848" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["tool_name"] == "qe_template_create"
    assert route["side_effect"] == "plan_or_preflight"
    assert "mcp_execution_result" not in result["cards"] or result["cards"]["mcp_execution_result"].get("auto_executed") is not True


def test_bug_343_chat_turn_renders_local_data_daily_status_groups() -> None:
    class LocalDataDailyLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Catalog route summary only.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(LocalDataDailyLlmClient())
    svc.local_data_service_factory = FakeLocalDataDailyStatusService

    result = svc.chat_turn(
        ChatTurnRequest(
            message="\u68c0\u67e5\u5f53\u524d\u672c\u5730\u6570\u636e\u540c\u6b65\u4efb\u52a1\u8fd0\u884c\u60c5\u51b5\uff0c\u4eca\u5929\u6570\u636e\u54ea\u4e9b\u5b8c\u6210\u4e86\u540c\u6b65"
        )
    )

    text = result["assistant_message"]["content_text"]
    assert "Route decision" not in text
    assert "research_assistant_catalog_summary_adapter" not in text
    assert "local_data_health_overview" not in text
    assert "success=1" in text
    assert "failed=1" in text
    assert "running=2" in text
    assert "blocked=1" in text
    assert "stock_moneyflow_ts" in text
    assert "来源 local_data_facade_read_adapter" in text

    route = result["cards"]["mcp_route_decision"]
    assert route["tool_name"] == "local_data_get_preset_daily_status"
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["tool_name"] == "local_data_get_preset_daily_status"
    assert result["cards"]["mcp_tool_event"]["transport"] == "local_data_facade_read_adapter"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["source"] == "local_data_facade_read_adapter"
    assert summary["local_data_daily_status"] is True
    assert summary["group_counts"]["success"] == 1
    assert summary["group_counts"]["failed"] == 1
    assert summary["group_counts"]["running"] == 2
    assert summary["group_counts"]["blocked"] == 1



def test_bug_412_local_data_agentic_tools_recover_from_manifest_only_read_tool() -> None:
    class LocalDataNativeToolLlmClient(FakeLlmClient):
        def __init__(self) -> None:
            super().__init__()
            self.offered_tool_pairs: list[set[tuple[str, str]]] = []
            self.tool_call_attempt = 0

        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            registry = kwargs.get("tool_registry") if isinstance(kwargs.get("tool_registry"), dict) else {}
            if registry:
                offered = {
                    (str(mapping.get("server_key")), str(mapping.get("tool_name")))
                    for mapping in registry.values()
                    if isinstance(mapping, dict)
                }
                self.offered_tool_pairs.append(offered)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            if self.tool_call_attempt == 0:
                self.tool_call_attempt += 1
                return LlmCallResult(
                    content="",
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                    tool_calls=[
                        McpToolCall(
                            server_key="aistock-local-data",
                            tool_name="local_data_get_unack_alert_count",
                            payload_json={},
                            stable_call_id="native-uncovered-local-data-alert-count",
                            reason="native_function_call:local_data_get_unack_alert_count",
                        )
                    ],
                )
            if self.tool_call_attempt == 1:
                self.tool_call_attempt += 1
                return LlmCallResult(
                    content="",
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                    tool_calls=[
                        McpToolCall(
                            server_key="aistock-local-data",
                            tool_name="local_data_get_preset_daily_status",
                            payload_json={"trade_date": "2026-06-17"},
                            stable_call_id="native-local-data-daily-status",
                            reason="native_function_call:local_data_get_preset_daily_status",
                        )
                    ],
                )
            return LlmCallResult(
                content="Insufficient evidence: expected local-data tool result first.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = LocalDataNativeToolLlmClient()
    svc = _chat_service(fake)
    svc.local_data_service_factory = FakeLocalDataDailyStatusService

    result = svc.chat_turn(ChatTurnRequest(message="\u6628\u5929\u7684\u672c\u5730\u6570\u636e\u540c\u6b65\u4efb\u52a1\u662f\u5426\u8fd0\u884c\u6b63\u5e38\uff1f"))

    offered = set().union(*fake.offered_tool_pairs)
    assert ("aistock-local-data", "local_data_get_unack_alert_count") in offered
    assert ("aistock-local-data", "local_data_get_preset_daily_status") in offered
    text = result["assistant_message"]["content_text"]
    assert "approved capability not found" not in text
    assert "Insufficient evidence" not in text
    assert "success=1" in text
    react_results = result["cards"]["react_grounding"]
    assert react_results["tool_call_count"] >= 2
    assert react_results["evidence_guard"]["allowed"] is True
    executed_pairs = {(item["server_key"], item["tool_name"]) for item in react_results["executed_tools"]}
    assert ("aistock-local-data", "local_data_get_unack_alert_count") in executed_pairs
    assert ("aistock-local-data", "local_data_get_preset_daily_status") in executed_pairs
    assert react_results["tool_errors"] == []
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["tool_name"] == "local_data_get_preset_daily_status"
    assert result["cards"]["mcp_summary_result"]["response_mode"] == "local_data_daily_sync_status"


def test_bug_356_generic_local_data_sync_summary_returns_dataset_status_list_without_diagnostics() -> None:
    class LocalDataSummaryLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Diagnostic route text that must not become the final reply.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(LocalDataSummaryLlmClient())
    svc.local_data_service_factory = FakeLocalDataDailyStatusService

    result = svc.chat_turn(
        ChatTurnRequest(
            message="\u68c0\u67e5\u672c\u5730\u6570\u636e\u540c\u6b65\u60c5\u51b5\uff0c\u5e76\u6c47\u603b\u6570\u636e\u7ed9\u6211"
        )
    )

    _assert_bug_356_business_local_data_reply(result)


def test_bug_356_each_dataset_sync_detail_returns_all_dataset_statuses_without_diagnostics() -> None:
    class LocalDataDetailLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Tool-grounded summary for local_data_get_dataset_status; source=preflight as_of=2026-06-13.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(LocalDataDetailLlmClient())
    svc.local_data_service_factory = FakeLocalDataDailyStatusService

    result = svc.chat_turn(
        ChatTurnRequest(
            message="\u7ed9\u6211\u8be6\u60c5\u4ecb\u7ecd\uff0c\u6bcf\u4e2a\u6570\u636e\u96c6\u7684\u540c\u6b65\u60c5\u51b5"
        )
    )

    _assert_bug_356_business_local_data_reply(result)
    assert "local_data_get_dataset_status" not in result["assistant_message"]["content_text"]


def test_bug_346_local_data_daily_status_stops_after_seeded_summary() -> None:
    class LocalDataSynthesisLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Unsourced placeholder answer that must not become the final reply.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = LocalDataSynthesisLlmClient()
    svc = _chat_service(fake)
    svc.local_data_service_factory = FakeLocalDataDailyStatusService

    result = svc.chat_turn(
        ChatTurnRequest(
            message="\u68c0\u67e5\u5f53\u524d\u672c\u5730\u6570\u636e\u540c\u6b65\u4efb\u52a1\u8fd0\u884c\u60c5\u51b5\u5417\uff0c\u4eca\u5929\u6570\u636e\u54ea\u4e9b\u5b8c\u6210\u4e86\u540c\u6b65\uff0c\u7ed9\u6211\u4e00\u4e2a\u6c47\u603b\u4fe1\u606f"
        )
    )

    text = result["assistant_message"]["content_text"]
    assert "Insufficient evidence" not in text
    assert "Unsourced placeholder" not in text
    assert "daily_basic" in text
    assert "stock_moneyflow_ts" in text
    assert "今天本地数据同步结果" in text
    assert len(fake.calls) >= 2
    react = result["cards"]["react_grounding"]
    assert react["stopped_reason"] == "final_answer"
    assert react["iterations"] >= 2
    assert react["evidence_guard"]["reason"] == "ok"
    assert result["cards"]["mcp_summary_result"]["response_mode"] == "local_data_daily_sync_status"


def test_bug_404_non_catalog_tool_reports_capability_not_found_without_crashing() -> None:
    class UncoveredToolLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                return LlmCallResult(
                    content="",
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                    tool_calls=[
                        McpToolCall(
                            server_key="aistock-local-data",
                            tool_name="local_data_unknown_manifest_tool",
                            payload_json={},
                            stable_call_id="call_uncovered_local_data",
                        )
                    ],
                )
            return LlmCallResult(
                content="Insufficient evidence: max tool iterations reached without reliable evidence.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = UncoveredToolLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(
        ChatTurnRequest(
            message="昨天的本地数据同步任务是否运行正常？",
            dialogue_mode_override="analysis",
        )
    )

    text = result["assistant_message"]["content_text"]
    assert "reason_code=capability_not_found" in text
    assert "aistock-local-data/local_data_unknown_manifest_tool" in text
    assert "approved capability not found for tool" in text
    assert "Insufficient evidence" not in text
    assert result["cards"]["mcp_execution_result"]["status"] == "failed"
    assert result["cards"]["mcp_execution_result"]["error"]["reason_code"] == "capability_not_found"
    assert result["cards"]["mcp_execution_result"]["error"]["catalog_reason"] == "tool_not_in_audited_catalog"
    assert result["cards"]["react_grounding"]["tool_errors"][0]["reason_code"] == "capability_not_found"
    assert result["cards"]["react_grounding"]["tool_errors"][0]["terminal_program_error"] is True
    assert any(event["event_type"] == "mcp_failed" for event in result["task_events"])


def test_bug_404_data_source_unavailable_is_explicit_not_insufficient() -> None:
    class LocalDataUnavailableLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            answer = _agentic_business_answer(kwargs.get("messages"))
            if answer is not None:
                return LlmCallResult(
                    content=answer,
                    provider="fake",
                    model="fake-primary",
                    duration_ms=1,
                    usage={},
                )
            return LlmCallResult(
                content="Fallback text that must not hide tool failures.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(LocalDataUnavailableLlmClient())
    svc.local_data_service_factory = FailingLocalDataDailyStatusService

    result = svc.chat_turn(
        ChatTurnRequest(
            message="检查当前本地数据同步任务运行情况，今天数据哪些完成了同步",
            dialogue_mode_override="analysis",
        )
    )

    text = result["assistant_message"]["content_text"]
    assert "reason_code=data_source_unavailable" in text
    assert "local data facade offline: 127.0.0.1:8001 refused" in text
    assert "aistock-local-data/local_data_get_preset_daily_status" in text
    assert "Insufficient evidence" not in text
    assert "没有对应数据源 / 无法获取该数据" not in text
    assert result["cards"]["mcp_execution_result"]["error"]["reason_code"] == "data_source_unavailable"
    assert result["cards"]["react_grounding"]["tool_errors"][0]["reason_code"] == "data_source_unavailable"
    assert any(event["event_type"] == "mcp_failed" for event in result["task_events"])


def test_bug_434_business_modes_keep_agentic_synthesis_and_dead_renderers_removed() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = ModeDecision(
        mode=DialogueMode.PLANNING,
        intent_type=DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST,
        confidence=0.96,
        mode_reason="explicit_task_request",
        requires_tool=False,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )
    removed_renderers = (
        "_render_local_data_daily_status_reply",
        "_render_qe_experiment_status_reply",
        "_render_qe_warehouse_business_reply",
        "_render_stock_analysis_evidence_card_reply",
        "_render_qe_run_leaderboard_reply",
    )
    for name in removed_renderers:
        assert not hasattr(ResearchAssistantService, name)

    cases = [
        (
            "local_data",
            {"local_data_daily_status": True, "response_mode": "local_data_daily_sync_status"},
            "model synthesized local data answer; source local_data_facade_read_adapter as_of 2026-06-19.",
        ),
        (
            "qe_experiment",
            {"response_mode": "qe_experiment_status_summary"},
            "model synthesized QE experiment answer; running=0 created=2 completed=18; source qe_experiment_read_adapter as_of 2026-06-19.",
        ),
        (
            "qe_warehouse",
            {"response_mode": "qe_warehouse_business_summary"},
            "model synthesized QE warehouse answer; run_count=7 pending_outbox=1; source qe_archive_read_adapter as_of 2026-06-19.",
        ),
        (
            "stock",
            {"response_mode": "stock_analysis_evidence_card"},
            "model synthesized stock answer for 000688 using quote fund-flow fundamentals; source stock_analysis_read_adapter as_of 2026-06-19.",
        ),
    ]
    forbidden_template_markers = (
        "已完成",
        "汇总：共",
        "本轮只进行查询",
        "mcp_execution_result",
        "response_mode",
    )

    for domain, summary, model_answer in cases:
        cards = {
            "mcp_execution_result": {
                "auto_executed": True,
                "status": "succeeded",
                "server_key": "aistock-test",
                "tool_name": f"{domain}_read_tool",
                "route": f"aistock-test/{domain}_read_tool",
            },
            "mcp_summary_result": summary,
            "react_grounding": {"stopped_reason": "final_answer"},
        }

        text = svc._compose_assistant_reply("business question", model_answer, cards, mode_decision)

        assert text == model_answer
        for marker in forbidden_template_markers:
            assert marker not in text


def test_bug_404_clarification_follow_up_always_returns_non_empty_reply() -> None:
    class ClarificationThenAnswerLlmClient(FakeLlmClient):
        def __init__(self) -> None:
            super().__init__()
            self.plan_calls: list[dict[str, object]] = []

        def complete_tool_plan(self, **kwargs: object) -> LlmCallResult:
            self.plan_calls.append(kwargs)
            plan = (
                {
                    "status": "clarification",
                    "domain": "stock_analysis",
                    "confidence": 0.82,
                    "reason": "Need to confirm the analysis dimension.",
                    "clarification_questions": ["请确认要基本面、走势、资金面还是全方位分析？"],
                }
                if len(self.plan_calls) == 1
                else {
                    "status": "no_tool",
                    "domain": "stock_analysis",
                    "confidence": 0.7,
                    "reason": "User clarified that a comprehensive answer is needed.",
                }
            )
            return LlmCallResult(
                content=json.dumps(plan, ensure_ascii=False),
                provider="fake",
                model="fake-semantic-planner",
                duration_ms=1,
                usage={},
            )

        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="已收到全方位分析要求；当前测试环境未接入实时行情，因此先给出可继续取证的明确答复。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = ClarificationThenAnswerLlmClient()
    svc = _chat_service(fake)

    first = svc.chat_turn(ChatTurnRequest(message="国城矿业的基本情况近期走势未来趋势怎样"))
    follow_up = svc.chat_turn(
        ChatTurnRequest(
            message="给我全方位的分析",
            conversation_id=first["conversation"]["conversation_id"],
        )
    )

    assert first["assistant_message"]["content_text"].strip()
    assert follow_up["assistant_message"]["content_text"].strip()
    assert "全方位分析要求" in follow_up["assistant_message"]["content_text"]


def test_bug_404_unexpected_chat_turn_error_returns_explicit_message() -> None:
    class ExplodingLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            raise RuntimeError("model gateway exploded")

    svc = _chat_service(ExplodingLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="请分析这个问题", dialogue_mode_override="analysis"))

    text = result["assistant_message"]["content_text"]
    assert "reason_code=chat_turn_unexpected_error" in text
    assert "RuntimeError" in text
    assert "model gateway exploded" in text
    assert text.strip()
    assert result["cards"]["error_card"]["reason_code"] == "chat_turn_unexpected_error"
    assert result["cards"]["mcp_execution_result"]["error"]["reason_code"] == "chat_turn_unexpected_error"


def test_bug_403_same_qe_tool_result_keeps_model_question_specific_answers() -> None:
    svc = _chat_service_with_qe_fakes()
    summary = {
        "response_mode": "qe_experiment_status_summary",
        "source": "qe_experiment_read_adapter",
        "as_of": "2026-06-17T12:00:00+00:00",
        "status_counts": {"created": 2, "completed": 18, "running": 0},
        "items": [
            {"experiment_id": "loop9", "experiment_name": "Loop 9 baseline", "status": "completed", "model_type": "LightGBM", "cagr": 0.12},
            {"experiment_id": "loop12", "experiment_name": "Loop 12 contender", "status": "completed", "model_type": "CatBoost", "cagr": 0.18},
        ],
    }
    cards = {
        "mcp_execution_result": {"auto_executed": True, "status": "succeeded", "server_key": "aistock-qe", "tool_name": "qe_experiment_list"},
        "mcp_summary_result": summary,
        "react_grounding": {"stopped_reason": "max_iterations_exhausted"},
    }
    mode_decision = ModeDecision(
        mode=DialogueMode.PLANNING,
        intent_type=DialogueIntent.EXPERIMENT_DRAFT_REQUEST,
        confidence=0.95,
        mode_reason="explicit_task_request",
        requires_tool=False,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )

    questions = [
        "哪些实验还在运行？",
        "完成了几个？",
        "最优 loop 用什么模型？",
        "对比 loop9 与 loop12。",
    ]
    model_answers = [
        "目前无正在运行的 QE 实验；2 个 created、18 个 completed。来源 qe_experiment_read_adapter，截至 2026-06-17T12:00:00+00:00。",
        "完成数量是 18 个；另有 2 个 created、0 个 running。来源 qe_experiment_read_adapter，截至 2026-06-17T12:00:00+00:00。",
        "当前最优 loop 是 loop12，模型是 CatBoost，cagr=0.18。来源 qe_experiment_read_adapter，截至 2026-06-17T12:00:00+00:00。",
        "loop9 是 LightGBM 且 cagr=0.12；loop12 是 CatBoost 且 cagr=0.18。来源 qe_experiment_read_adapter，截至 2026-06-17T12:00:00+00:00。",
    ]
    answers = [
        svc._compose_assistant_reply(question, model_answer, cards, mode_decision)
        for question, model_answer in zip(questions, model_answers, strict=True)
    ]

    assert "无正在运行" in answers[0]
    assert "2 个 created、18 个 completed" in answers[0]
    assert "18" in answers[1]
    assert "CatBoost" in answers[2]
    assert "loop9" in answers[3] and "loop12" in answers[3]
    assert answers == model_answers
    assert len(set(answers)) == len(answers)
    for answer in answers:
        assert "已汇总" not in answer
        assert "状态汇总：" not in answer
        assert "来源 qe_experiment_read_adapter" in answer


def test_bug_403_running_status_chat_path_answers_none_without_row_dump() -> None:
    svc = _chat_service(AgenticBusinessSynthesisFakeLlmClient())
    svc.qe_experiment_service_factory = FakeQeExperimentZeroRunningService
    svc.qe_custom_evo_service_factory = FakeQeCustomEvoService
    svc.qe_archive_repository_factory = FakeQeArchiveRepository

    result = svc.chat_turn(ChatTurnRequest(message="哪些 QE 实验还在运行"))

    text = result["assistant_message"]["content_text"]
    assert "无正在运行" in text
    assert "2 created、18 completed" in text
    assert "created-1" not in text
    assert "completed-18" not in text
    assert "mcp_execution_result" not in text
    assert "raw_payload" not in text
    route = result["cards"]["mcp_route_decision"]
    assert route["tool_name"] == "qe_experiment_list"
    assert route["side_effect"] == "read_only"
    assert result["cards"]["mcp_execution_result"]["auto_executed"] is True
    assert result["cards"]["mcp_summary_result"]["status_counts"] == {"created": 2, "completed": 18}
    assert result["cards"]["react_grounding"]["evidence_guard"]["reason"] == "ok"


def test_bug_403_business_summary_fails_closed_when_synthesis_is_unavailable() -> None:
    svc = _chat_service(FakeLlmClient())
    summary = {
        "local_data_daily_status": True,
        "response_mode": "local_data_daily_sync_status",
        "source": "local_data_facade_read_adapter",
        "trade_date": "2026-06-12",
        "as_of": "2026-06-12T01:06:00+00:00",
        "evidence_sources": ["local_data_get_preset_daily_status"],
        "group_counts": {"success": 1, "failed": 0, "not_synced": 0, "running": 0, "blocked": 0},
        "status_groups": {
            "success": [{"dataset": "daily_basic", "status": "success", "finished_at": "2026-06-12T09:02:00+08:00"}],
            "failed": [],
            "not_synced": [],
            "running": [],
            "blocked": [],
        },
    }
    cards = {
        "mcp_execution_result": {
            "auto_executed": True,
            "status": "succeeded",
            "server_key": "aistock-local-data",
            "tool_name": "local_data_get_preset_daily_status",
            "route": "aistock-local-data/local_data_get_preset_daily_status",
        },
        "mcp_summary_result": summary,
        "react_grounding": {"stopped_reason": "max_iterations_exhausted"},
    }
    mode_decision = ModeDecision(
        mode=DialogueMode.PLANNING,
        intent_type=DialogueIntent.LOCAL_DATA_MANAGEMENT_REQUEST,
        confidence=0.96,
        mode_reason="explicit_task_request",
        requires_tool=False,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )

    text = svc._compose_assistant_reply(
        "????????",
        "Insufficient evidence: max tool iterations reached without reliable evidence.",
        cards,
        mode_decision,
    )

    assert "Insufficient evidence" in text
    assert "success=1" not in text
    assert "local_data_get_preset_daily_status" not in text


def test_bug_352_local_data_daily_status_ignores_stale_db_capability_projection() -> None:
    svc = _chat_service(FakeLlmClient())
    svc.local_data_service_factory = FakeLocalDataDailyStatusService
    capability = svc._workflow_capability_by_key("local_data.mcp_orchestration")
    assert capability is not None
    capability["capability_id"] = "cap_local_data_mcp_orchestration_stale_db_projection"
    capability["mcp_tool_refs"] = [
        ref
        for ref in capability["mcp_tool_refs"]
        if ref["tool_name"] != "local_data_get_preset_daily_status"
    ]
    capability["checksum"] = "stale-local-data-capability-cache"
    svc.repository.create_record("capabilities", capability)

    stale = svc.repository.find_one("capabilities", {"capability_key": "local_data.mcp_orchestration"})
    assert stale is not None
    assert all(ref["tool_name"] != "local_data_get_preset_daily_status" for ref in stale["mcp_tool_refs"])

    result = svc.chat_turn(
        ChatTurnRequest(
            message="\u68c0\u67e5\u5f53\u524d\u672c\u5730\u6570\u636e\u540c\u6b65\u4efb\u52a1\u8fd0\u884c\u60c5\u51b5\u5417\uff0c\u4eca\u5929\u6570\u636e\u54ea\u4e9b\u5b8c\u6210\u4e86\u540c\u6b65\uff0c\u7ed9\u6211\u4e00\u4e2a\u6c47\u603b\u4fe1\u606f"
        )
    )

    text = result["assistant_message"]["content_text"]
    assert "approved capability not found" not in text
    assert "Insufficient evidence" not in text
    assert "daily_basic" in text
    assert result["cards"]["mcp_summary_result"]["response_mode"] == "local_data_daily_sync_status"
    unchanged = svc.repository.find_one("capabilities", {"capability_key": "local_data.mcp_orchestration"})
    assert unchanged is not None
    assert all(ref["tool_name"] != "local_data_get_preset_daily_status" for ref in unchanged["mcp_tool_refs"])


def test_bug_161_chat_turn_public_response_is_compact_and_hides_unrelated_prompt_nodes() -> None:
    class FactorOverviewLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="There are exactly 999 factors: fake_alpha.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(FactorOverviewLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="因子库有哪些因子？只要概要列表，不要全量详情。"))
    body = json.dumps(result, ensure_ascii=False)

    assert "prompt.local_data_management" not in body
    assert "workflow.local_data_check_repair" not in body
    assert "tool_guard.mcp_local_data" not in body
    assert "node_refs" not in result["prompt_bundle"]
    assert "selected_prompt_keys" in result["prompt_bundle"]
    assert "cards" not in result["assistant_message"]["content_json"]
    assert "payload_json" not in body
    assert len(body.encode("utf-8")) < 20000
    assert result["cards"]["mcp_route_decision"]["server_key"] == "aistock-factor"
    assert result["cards"]["mcp_route_decision"]["tool_name"] == "factor_library_list"
    assert result["cards"]["mcp_summary_result"]["items_truncated"] >= 0


def test_chat_turn_includes_runtime_code_visibility_card() -> None:
    svc = _chat_service(FakeLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="What MCP tools are available?"))

    runtime_code = result["cards"]["runtime_code"]
    assert runtime_code["schema_version"] == "aistock_research_assistant_runtime_code_visibility_v1"
    assert runtime_code["runtime_loaded_at"]
    assert runtime_code["runtime_loaded_git_commit_short"]
    assert runtime_code["current_repo_git_commit_short"]
    assert runtime_code["operator_message"]
    assert "runtime_code" in result["cards"]
    assert "cards" not in result["assistant_message"]["content_json"]


def test_chat_turn_chinese_factor_library_overview_auto_executes_summary_list() -> None:
    class FactorOverviewLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="因子库概要会展示全部因子明细和原始 payload。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(FactorOverviewLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="查看因子库概要"))

    text = result["assistant_message"]["content_text"]
    assert "全部因子明细" not in text
    assert "raw payload" not in text.lower()
    execution = result["cards"]["mcp_execution_result"]
    assert execution["auto_executed"] is True
    assert execution["status"] == "succeeded"
    assert execution["route"] == "aistock-factor/factor_library_list"
    route = result["cards"]["mcp_route_decision"]
    assert route["tool_name"] == "factor_library_list"
    assert route["auto_execute"]["eligible"] is True
    summary = result["cards"]["mcp_summary_result"]
    assert summary["summary_first"] is True
    assert summary["response_mode"] == "summary"
    assert result["cards"]["mcp_result_cards"]


def test_chat_turn_chinese_execution_policy_catalog_uses_read_only_list() -> None:
    class ExecutionPolicyHallucinationLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="我可以直接校验这个策略是否适合某个算法。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(ExecutionPolicyHallucinationLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="执行策略库里有什么 minute algo？"))

    text = result["assistant_message"]["content_text"]
    assert "execution policy list algos" in text
    assert "execution_policy_validate_for_strategy" not in text
    assert "Route decision" not in text
    assert result["mode_decision"]["intent_type"] == "execution_policy_request"
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "execution_policy"
    assert route["tool_name"] == "execution_policy_list_algos"
    assert route["side_effect"] == "read_only"
    assert route["preflight_required"] is False
    assert route["summary_first"] is True
    keys = set(result["prompt_bundle"]["selected_prompt_keys"])
    assert "domain.execution_policy" in keys


def test_chat_turn_tool_choice_markup_is_replaced_with_route_card_text() -> None:
    class ToolChoiceMarkupLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="<assistant_tool_choice>{\"tool\":\"mcp_github_issue_sync_bug\"}</assistant_tool_choice>",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(ToolChoiceMarkupLlmClient())

    result = svc.chat_turn(ChatTurnRequest(message="同步 BUG-120 GitHub issue 状态"))

    text = result["assistant_message"]["content_text"]
    assert "<assistant_tool_choice>" not in text
    assert "</assistant_tool_choice>" not in text
    assert "aistock-validation/mcp_github_issue_sync_bug" not in text
    assert "Route decision" not in text
    assert "summary-first" not in text
    assert result["mode_decision"]["intent_type"] == "validation_issue_request"
    assert result["cards"]["mcp_route_decision"]["server_key"] == "aistock-validation"
    assert result["cards"]["mcp_route_decision"]["tool_name"] == "mcp_github_issue_sync_bug"
    assert result["cards"]["mcp_route_decision"]["confirmation_required"] is True
    assert result["cards"]["mcp_route_decision"]["auto_execute"]["eligible"] is False
    assert result["cards"]["mcp_route_decision"]["auto_execute"]["reason"] == "route_not_read_only"
    assert "mcp_execution_result" not in result["cards"]
    assert svc.repository.list_records("mcp_tool_events", limit=100)["total"] == 0


def test_chat_turn_bug_diagnosis_request_is_first_class_intent() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="请帮我诊断这个报错是什么原因，只做分析。"))

    assert result["assistant_message"]["content_text"].startswith("可以诊断")
    assert result["cards"]["intent_type"] == "bug_diagnosis_request"
    assert result["mode_decision"]["mode"] == "analysis"
    assert "plan_card" not in result["cards"]
    assert result["cards"]["action_proposals"] == []
    assert result["cards"]["clarification_card"]["questions"]
    assert result["context_health"]["show_badge"] is False
    assert result["cards"]["status_rail"][2] == {"label": "等待诊断证据", "status": "current"}
    assert result["prompt_bundle"]["selection_trace_json"]["dialogue_intent"] == "bug_diagnosis_request"
    keys = set(result["prompt_bundle"]["selected_prompt_keys"])
    assert "domain.qe_experiment" not in keys
    assert "workflow.qe_draft_then_approval" not in keys


def test_chat_turn_ambiguous_request_needs_minimal_clarification_only() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="请处理一下。"))

    assert result["cards"]["intent_type"] == "ambiguous_request"
    assert result["mode_decision"]["mode"] == "analysis"
    assert "plan_card" not in result["cards"]
    assert result["cards"]["clarification_card"]["questions"]
    assert result["cards"]["action_proposals"] == []
    keys = set(result["prompt_bundle"]["selected_prompt_keys"])
    assert "domain.qe_experiment" not in keys
    assert "workflow.qe_draft_then_approval" not in keys


def test_mode_router_m0_matrix_keeps_keywords_from_starting_workflow() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    cases = [
        ("你能做什么？", "capability_inquiry", "dialogue"),
        ("通用能力", "capability_inquiry", "dialogue"),
        ("QE 实验和 bug 诊断能力目前是什么状态？", "capability_inquiry", "dialogue"),
        ("请展开验证矩阵和 Trace 证据", "audit_request", "audit"),
    ]
    for message, expected_intent, expected_mode in cases:
        result = svc.chat_turn(ChatTurnRequest(message=message))
        assert result["mode_decision"]["intent_type"] == expected_intent
        assert result["mode_decision"]["mode"] == expected_mode
        if expected_mode == "dialogue":
            assert result["cards"]["action_proposals"] == []
            assert "plan_card" not in result["cards"]
            keys = set(result["prompt_bundle"]["selected_prompt_keys"])
            assert "workflow.qe_draft_then_approval" not in keys
            assert "tool_guard.mcp_qe" not in keys
            assert "Context Pack" not in result["assistant_message"]["content_text"]


def test_dialogue_main_reply_pollution_guard_removes_planning_scaffolding() -> None:
    class NoisyLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="可以回答。\n目标：自动创建任务。\n风险级别：高。\nContext Pack: 0 memories\n这部分应保留。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(NoisyLlmClient())
    result = svc.chat_turn(ChatTurnRequest(message="你能做什么？"))

    text = result["assistant_message"]["content_text"]
    assert "可以回答" in text
    assert "这部分应保留" in text
    assert "目标：" not in text
    assert "风险级别：" not in text
    assert "Context Pack" not in text


def test_mode_router_is_runtime_config_driven() -> None:
    svc = _chat_service()
    config = svc.active_runtime_config()

    assert "dialogue_modes" in config
    assert "mode_router" in config
    assert set(config["dialogue_modes"]["modes"]) >= {"dialogue", "analysis", "planning", "preflight", "execution", "audit", "recovery"}
    assert config["dialogue_modes"]["modes"]["dialogue"]["allowed_tool_side_effect"] == "none"
    assert config["dialogue_modes"]["modes"]["dialogue"]["show_plan_card"] is False
    assert config["dialogue_modes"]["modes"]["planning"]["show_plan_card"] is True
    assert "只做分析" in config["mode_router"]["user_overrides"]["analysis_only_patterns"]



def test_default_chat_completion_budget_uses_provider_max_for_long_answers() -> None:
    svc = _chat_service()
    config = svc.active_runtime_config()

    plan = svc.context_budget_planner.plan(
        model_profile={"capabilities_json": {"context_window_tokens": 1_048_576}},
        runtime_config=config,
        current_user_message="国城矿业跌停原因、未来走势和基本面三维综合分析",
    )

    assert config["model_context"]["fallback_context_window_tokens"] == 1_048_576
    assert config["budget"]["response"]["max_tokens"] == 384000
    assert plan.llm_max_tokens == 384000
    assert plan.response_reserved_tokens == 384000


def test_llm_usage_normalizes_dict_and_object_usage_without_prompt_text() -> None:
    class UsageObject:
        prompt_tokens = 7
        completion_tokens = 5
        total_tokens = 12
        completion_tokens_details = {"reasoning_tokens": 2}

    dict_usage = _normalize_litellm_usage(
        {"prompt_tokens": 10, "completion_tokens": 4, "prompt_tokens_details": {"cached_tokens": 3}},
        litellm_module=None,
        model_id="fake/model",
        messages=[{"role": "user", "content": "private prompt"}],
        tools=None,
        content="private response",
    )
    object_usage = _normalize_litellm_usage(
        UsageObject(),
        litellm_module=None,
        model_id="fake/model",
        messages=[],
        tools=None,
        content="",
    )

    assert dict_usage["usage_source"] == "provider_reported"
    assert dict_usage["total_tokens"] == 14
    assert dict_usage["cache_read_input_tokens"] == 3
    assert object_usage["usage_source"] == "litellm_usage_object"
    assert object_usage["reasoning_tokens"] == 2
    assert "private prompt" not in json.dumps(dict_usage, ensure_ascii=False)


def test_llm_usage_missing_provider_usage_is_explicitly_estimated_or_unavailable() -> None:
    class FakeLiteLlm:
        def token_counter(self, **kwargs: object) -> int:
            if kwargs.get("messages") is not None:
                return 11
            return 3

        def cost_per_token(self, **_kwargs: object) -> tuple[float, float]:
            return (0.001, 0.002)

    estimated = _normalize_litellm_usage(
        None,
        litellm_module=FakeLiteLlm(),
        model_id="fake/model",
        messages=[{"role": "user", "content": "hello"}],
        tools=[],
        content="answer",
    )
    cost = _calculate_litellm_cost(
        litellm_module=FakeLiteLlm(),
        response=None,
        model_id="fake/model",
        normalized_usage=estimated,
        duration_ms=9,
    )
    unavailable = _normalize_litellm_usage(
        None,
        litellm_module=None,
        model_id="fake/model",
        messages=[],
        tools=None,
        content="",
    )

    assert estimated["usage_status"] == "estimated"
    assert estimated["usage_reason_code"] == "provider_usage_missing"
    assert estimated["prompt_tokens"] == 11
    assert estimated["completion_tokens"] == 3
    assert cost["cost_status"] == "estimated"
    assert cost["total_cost_usd"] == 0.003
    assert unavailable["usage_status"] == "unavailable"
    assert unavailable["usage_reason_code"] == "provider_usage_missing_litellm_unavailable"


def test_chat_turn_writes_llm_usage_ledger_and_trace_cost_summary() -> None:
    class UsageLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="source=fake_model as_of=2026-06-26 已完成回答。",
                provider="fake",
                model="fake-primary",
                duration_ms=7,
                usage={
                    "prompt_tokens": 21,
                    "completion_tokens": 9,
                    "total_tokens": 30,
                    "usage_status": "recorded",
                    "usage_source": "provider_reported",
                    "total_cost_usd": "0.0003000000",
                    "cost_status": "recorded",
                    "cost_source": "litellm_model_cost",
                    "currency": "USD",
                },
                usage_event={
                    "prompt_tokens": 21,
                    "completion_tokens": 9,
                    "total_tokens": 30,
                    "usage_source": "provider_reported",
                    "usage_status": "recorded",
                    "prompt_tokens_estimated": False,
                    "completion_tokens_estimated": False,
                    "usage_raw_json": {"prompt_tokens": 21, "completion_tokens": 9, "total_tokens": 30},
                    "prompt_cost_usd": "0.0002100000",
                    "completion_cost_usd": "0.0000900000",
                    "total_cost_usd": "0.0003000000",
                    "currency": "USD",
                    "cost_source": "litellm_model_cost",
                    "cost_status": "recorded",
                    "pricing_snapshot_json": {"model": "fake-primary"},
                    "request_meta_json": {"message_count": 1, "prompt_text_retained": False},
                    "response_meta_json": {"content_chars": 34, "prompt_text_retained": False},
                },
            )

    svc = _chat_service(UsageLlmClient())
    result = svc.chat_turn(ChatTurnRequest(message="请回答一个普通问题", dialogue_mode_override="dialogue"))
    trace = svc.repository.get_record("trace_events", result["trace"]["trace_id"])
    assert trace is not None
    usage_page = svc.list_llm_usage_events(trace_id=trace["trace_id"])
    usage_row = usage_page["items"][0]
    summary = trace["cost_json"]["usage_summary"]

    assert usage_page["total"] == 1
    assert usage_row["trace_id"] == trace["trace_id"]
    assert usage_row["task_id"] == result["task"]["task_id"]
    assert usage_row["conversation_id"] == result["conversation"]["conversation_id"]
    assert usage_row["prompt_tokens"] == 21
    assert usage_row["completion_tokens"] == 9
    assert usage_row["total_tokens"] == 30
    assert usage_row["request_meta_json"]["prompt_text_retained"] is False
    assert "请回答一个普通问题" not in json.dumps(usage_row, ensure_ascii=False)
    assert summary["call_count"] == 1
    assert summary["total_tokens"] == 30
    assert trace["cost_json"]["usage_event_refs"] == [f"assistant_llm_usage_events:{usage_row['usage_event_id']}"]
    assert trace["cost_json"]["source_of_truth"] == "assistant_llm_usage_events"


def test_missing_fake_usage_is_recorded_as_explicit_unavailable_not_empty() -> None:
    class MissingUsageLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            return LlmCallResult(
                content="source=fake_model as_of=2026-06-26 已完成回答。",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    svc = _chat_service(MissingUsageLlmClient())
    result = svc.chat_turn(ChatTurnRequest(message="请回答一个普通问题", dialogue_mode_override="dialogue"))
    usage = svc.list_llm_usage_events(trace_id=result["trace"]["trace_id"])["items"][0]

    assert usage["usage_status"] == "unavailable"
    assert usage["usage_reason_code"] == "llm_result_usage_missing"
    assert usage["cost_status"] == "unavailable"
    assert usage["cost_reason_code"] == "cost_not_calculated_for_injected_llm_result"
    assert usage["usage_raw_json"]["reason_code"] == "llm_result_usage_missing"


def test_llm_usage_summary_aggregates_multiple_react_model_turns() -> None:
    svc = _chat_service(FakeLlmClient())
    trace = svc.create_trace_event(TraceEventCreate(task_id="rat_usage", event_type="llm_call", component="pytest", status="ok"))
    turn_a = McpToolCall(server_key="aistock-qe", tool_name="read", payload_json={}, stable_call_id="a")

    react_result = ReactGroundingResult(
        final_text="done",
        messages=[],
        tool_calls=[turn_a],
        tool_results=[],
        trace_steps=[],
        evidence_guard=EvidenceGuardDecision(True, "done", "ok", 1, 1),
        iterations=2,
        stopped_reason="model_finished",
        model_turns=[
            ModelTurn(content="first", provider="fake", model="fake-primary", duration_ms=2, usage={"prompt_tokens": 10, "completion_tokens": 5, "usage_status": "recorded", "usage_source": "provider_reported"}),
            ModelTurn(content="second", provider="fake", model="fake-primary", duration_ms=3, usage={"prompt_tokens": 20, "completion_tokens": 7, "usage_status": "estimated", "usage_source": "litellm_token_counter_estimated", "prompt_tokens_estimated": True}),
        ],
    )

    cost_json = svc._record_llm_usage_events_for_trace(
        trace=trace,
        task_id="rat_usage",
        conversation_id="conv_usage",
        model_profile_id="model_primary_reasoner",
        react_result=react_result,
    )

    assert cost_json["usage_summary"]["call_count"] == 2
    assert cost_json["usage_summary"]["prompt_tokens"] == 30
    assert cost_json["usage_summary"]["completion_tokens"] == 12
    assert cost_json["usage_summary"]["estimated_usage_event_count"] == 1
    assert len(cost_json["usage_event_refs"]) == 2



def test_llm_usage_report_aggregates_chart_ready_hour_buckets_and_statuses() -> None:
    svc = _chat_service(FakeLlmClient())
    rows = [
        {
            "usage_event_id": "llmu_report_a",
            "trace_id": "trace_report_a",
            "task_id": "task_report",
            "conversation_id": "conv_report",
            "call_group_id": "task_report",
            "call_index": 1,
            "phase": "initial_chat",
            "component": "research_assistant.llm",
            "provider": "deepseek",
            "model": "deepseek-chat",
            "prompt_tokens": 100,
            "completion_tokens": 30,
            "total_tokens": 130,
            "usage_source": "provider_reported",
            "usage_status": "recorded",
            "total_cost_usd": "0.0013000000",
            "cost_status": "recorded",
            "cost_source": "litellm_model_cost",
            "request_meta_json": {"prompt_text_retained": False},
            "completed_at": "2026-06-27T09:15:00+08:00",
        },
        {
            "usage_event_id": "llmu_report_b",
            "trace_id": "trace_report_b",
            "task_id": "task_report",
            "conversation_id": "conv_report",
            "call_group_id": "task_report",
            "call_index": 2,
            "phase": "react_iteration",
            "component": "research_assistant.llm",
            "provider": "deepseek",
            "model": "deepseek-reasoner",
            "prompt_tokens": 50,
            "completion_tokens": 20,
            "total_tokens": 70,
            "usage_source": "litellm_token_counter_estimated",
            "usage_status": "estimated",
            "prompt_tokens_estimated": True,
            "cost_status": "unavailable",
            "cost_source": "unavailable",
            "cost_reason_code": "model_pricing_unavailable",
            "request_meta_json": {"prompt_text_retained": False},
            "completed_at": "2026-06-27T09:45:00+08:00",
        },
    ]
    for row in rows:
        svc.repository.create_record("llm_usage_events", row)
    svc.repository.create_record(
        "trace_events",
        {
            "trace_id": "trace_report_a",
            "task_id": "task_report",
            "event_type": "llm_call",
            "component": "research_assistant.llm",
            "status": "ok",
            "cost_json": {"usage_summary": {"total_tokens": 999999, "total_cost_usd": "999.0000000000"}},
        },
    )

    report = svc.llm_usage_report(
        conversation_id="conv_report",
        date_from="2026-06-27T00:00:00+08:00",
        date_to="2026-06-27T23:59:59+08:00",
        granularity="hour",
        timezone_name="Asia/Shanghai",
    )

    assert report["source_of_truth"] == "assistant_llm_usage_events"
    assert report["prompt_text_retained"] is False
    assert report["filters"]["timezone"] == "Asia/Shanghai"
    assert report["summary"]["call_count"] == 2
    assert report["summary"]["total_tokens"] == 200
    assert report["summary"]["usage_status"] == "mixed"
    assert report["summary"]["cost_status"] == "mixed"
    assert report["status_breakdown"]["usage"]["recorded"] == 1
    assert report["status_breakdown"]["usage"]["estimated"] == 1
    assert report["status_breakdown"]["cost"]["recorded"] == 1
    assert report["status_breakdown"]["cost"]["unavailable"] == 1
    assert {item["model"] for item in report["model_breakdown"]} == {"deepseek-chat", "deepseek-reasoner"}
    assert "999999" not in json.dumps(report, ensure_ascii=False)
    assert all("private prompt" not in json.dumps(bucket, ensure_ascii=False) for bucket in report["time_series"])


def test_llm_usage_report_compacts_long_tail_models_into_other_without_dropping_tokens() -> None:
    svc = _chat_service(FakeLlmClient())
    for index in range(4):
        svc.repository.create_record(
            "llm_usage_events",
            {
                "usage_event_id": f"llmu_tail_{index}",
                "trace_id": f"trace_tail_{index}",
                "task_id": "task_tail",
                "conversation_id": "conv_tail",
                "call_group_id": "task_tail",
                "call_index": index + 1,
                "phase": "react_iteration",
                "component": "research_assistant.llm",
                "provider": "provider",
                "model": f"model-{index}",
                "prompt_tokens": 10 * (index + 1),
                "completion_tokens": 5,
                "total_tokens": 10 * (index + 1) + 5,
                "usage_source": "provider_reported",
                "usage_status": "recorded",
                "total_cost_usd": "0.0001000000",
                "cost_status": "recorded",
                "cost_source": "litellm_model_cost",
                "completed_at": "2026-06-27T10:00:00+08:00",
            },
        )

    report = svc.llm_usage_report(conversation_id="conv_tail", granularity="day", timezone_name="Asia/Shanghai", limit_models=3)

    assert len(report["model_breakdown"]) == 3
    assert report["model_breakdown"][-1]["model"] == "other"
    assert sum(int(item["total_tokens"]) for item in report["model_breakdown"]) == report["summary"]["total_tokens"]
    assert any(bucket["model"] == "other" for bucket in report["time_series"])


def test_llm_usage_report_rejects_invalid_timezone_loudly() -> None:
    svc = _chat_service(FakeLlmClient())
    with pytest.raises(ValueError, match="invalid_timezone"):
        svc.llm_usage_report(timezone_name="Invalid/Timezone")

def test_chat_turn_preserves_complete_long_raw_api_response() -> None:
    class LongAnswerLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            long_text = (
                "国城矿业三维分析：跌停原因需要同时看行情、资金和消息面。"
                "基本面部分关注收入质量、利润率、资产负债和行业地位。"
                "未来走势部分只讨论驱动、情景和风险，不给方向预测。"
            ) * 30 + "收尾结论：以上分析完整结束。"
            return LlmCallResult(
                content=long_text,
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = LongAnswerLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="请直接回答国城矿业跌停原因、未来走势和基本面，不调用工具。", dialogue_mode_override="dialogue"))
    text = result["assistant_message"]["content_text"]

    assert fake.calls[0]["max_tokens"] == 384000
    assert "跌停原因" in text
    assert "基本面" in text
    assert "未来走势" in text
    assert text.endswith("收尾结论：以上分析完整结束。")
    assert len(text) > 1800


def test_chat_turn_reports_provider_length_stop_loudly() -> None:
    class LengthStoppedLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            raise RuntimeError("llm_completion_truncated: provider returned finish_reason=length")

    fake = LengthStoppedLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="请综合分析国城矿业跌停原因、未来趋势和基本面", dialogue_mode_override="dialogue"))
    text = result["assistant_message"]["content_text"]

    assert fake.calls[0]["max_tokens"] == 384000
    assert "reason_code=llm_completion_truncated" in text
    assert "stage=llm_completion" in text
    assert "llm_completion_truncated" in text
    assert result["cards"]["error_card"]["error"]["message"].startswith("llm_completion_truncated")


def test_bug_509_stock_depth_offered_tools_equal_full_executable_data_surface() -> None:
    svc = _chat_service()
    mode_decision = ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.STOCK_ANALYSIS_REQUEST,
        confidence=0.95,
        mode_reason="stock_depth",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )

    _specs, registry = svc._agentic_function_tools(mode_decision)
    offered = {
        (str(item["server_key"]), str(item["tool_name"]))
        for item in registry.values()
        if isinstance(item, dict)
    }
    executable = {
        (entry.server_key, entry.tool_name)
        for entry in svc._react_tool_catalog_entries(mode_decision=mode_decision)
        if entry.side_effect_level == "read_only"
    }
    all_executable = {
        (entry.server_key, entry.tool_name)
        for entry in svc._react_tool_catalog_entries(mode_decision=mode_decision)
    }
    manifest_read_only = {
        (str(tool["server_key"]), str(tool["tool_name"]))
        for tool in svc._manifest_mcp_catalog_records()
        if str(tool.get("side_effect_level") or "read_only") == "read_only"
    }

    assert set(STOCK_DEPTH_REQUIRED_TOOL_REFS) <= offered
    assert set(STOCK_DEPTH_REQUIRED_TOOL_REFS) <= executable
    assert executable == manifest_read_only
    assert offered == all_executable


def test_bug_527_read_only_domain_does_not_require_per_capability_seed() -> None:
    class SurfaceService(ResearchAssistantService):
        def _manifest_mcp_catalog_records(self) -> list[dict[str, object]]:
            base = {
                "server_key": "aistock-local-data",
                "status": "enabled",
                "risk_level": "low",
                "requires_approval": False,
                "input_schema_json": {"type": "object"},
            }
            return [
                {**base, "tool_name": "read_backed", "side_effect_level": "read_only"},
                {**base, "tool_name": "read_manifest_only", "side_effect_level": "read_only"},
                {
                    **base,
                    "tool_name": "write_backed",
                    "risk_level": "production_sensitive",
                    "side_effect_level": "production_sensitive",
                    "requires_approval": True,
                },
                {
                    **base,
                    "tool_name": "write_manifest_only",
                    "risk_level": "production_sensitive",
                    "side_effect_level": "production_sensitive",
                    "requires_approval": True,
                },
            ]

        def _approved_capability_mcp_tool_refs(self) -> set[tuple[str, str]]:
            return {("aistock-local-data", "read_backed"), ("aistock-local-data", "write_backed")}

    svc = SurfaceService(repository=InMemoryResearchAssistantRepository())
    read_only_mode = ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.AMBIGUOUS_REQUEST,
        confidence=0.95,
        mode_reason="read_only_surface",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )
    _specs, read_only_registry = svc._agentic_function_tools(read_only_mode)
    offered_read_only = {(str(item["server_key"]), str(item["tool_name"])) for item in read_only_registry.values()}
    callable_read_only = {
        (entry.server_key, entry.tool_name)
        for entry in svc._react_tool_catalog_entries(mode_decision=read_only_mode)
        if entry.side_effect_level == "read_only"
    }

    assert callable_read_only == {
        ("aistock-local-data", "read_backed"),
        ("aistock-local-data", "read_manifest_only"),
    }
    assert offered_read_only == {
        ("aistock-local-data", "read_backed"),
        ("aistock-local-data", "read_manifest_only"),
        ("aistock-local-data", "write_backed"),
    }
    analysis_registry = {(entry.server_key, entry.tool_name) for entry in svc._react_tool_catalog_entries(mode_decision=read_only_mode)}
    assert ("aistock-local-data", "write_backed") in analysis_registry
    assert ("aistock-local-data", "write_manifest_only") not in analysis_registry

    preflight_mode = ModeDecision(
        mode=DialogueMode.PREFLIGHT,
        intent_type=DialogueIntent.AMBIGUOUS_REQUEST,
        confidence=0.95,
        mode_reason="preflight_surface",
        requires_tool=True,
        allowed_tool_side_effect="preflight",
        requires_user_confirmation=True,
        requires_approval=True,
        visible_audit_default=True,
    )
    _specs, preflight_registry = svc._agentic_function_tools(preflight_mode)
    offered_preflight = {(str(item["server_key"]), str(item["tool_name"])) for item in preflight_registry.values()}

    assert ("aistock-local-data", "read_manifest_only") in offered_preflight
    assert ("aistock-local-data", "write_backed") in offered_preflight
    assert ("aistock-local-data", "write_manifest_only") not in offered_preflight


def test_bug_509_stock_depth_route_seeds_all_stock_reads_plus_web_and_60_day_kline() -> None:
    svc = _chat_service()
    message = "stock depth all-round fundamental future trend analysis for 000688"
    route = svc._with_agentic_route_candidates(
        message,
        {
            "domain": "stock_analysis",
            "server_key": "aistock-stock-analysis",
            "tool_name": "stock_analysis_get_quote",
            "side_effect": "read_only",
            "confidence": 0.95,
        },
    )
    mode_decision = ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.STOCK_ANALYSIS_REQUEST,
        confidence=0.95,
        mode_reason="stock_depth",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )

    seeds = svc._seeded_react_tool_calls({"mcp_route_decision": route}, mode_decision)
    seed_pairs = {(seed.server_key, seed.tool_name) for seed in seeds}
    expected_seed_pairs = {
        *{("aistock-stock-analysis", tool_name) for tool_name in STOCK_DEPTH_STOCK_TOOL_NAMES},
        ("aistock-external-research", "external_research_search_web"),
    }
    stock_seed_payloads = [seed.payload_json for seed in seeds if seed.server_key == "aistock-stock-analysis"]
    react_config = svc._react_grounding_config(svc.active_runtime_config(), user_message=message)
    react_messages = svc._react_messages_for_agentic_synthesis(
        [{"role": "user", "content": message}],
        user_message=message,
        route_seeds=seeds,
        route_candidates=route["route_candidates"],
        graph_context={},
    )
    directive = json.loads(str(react_messages[-1]["content"]))

    assert route["agentic_route_policy"]["allow_multi_tool"] is True
    assert expected_seed_pairs <= seed_pairs
    assert len(seed_pairs) >= 8
    assert all(payload.get("symbol") == "000688" for payload in stock_seed_payloads)
    assert all(payload.get("period") == STOCK_DEPTH_HISTORY_PERIOD for payload in stock_seed_payloads)
    assert all(payload.get("min_trading_days") == STOCK_DEPTH_MIN_HISTORY_TRADING_DAYS for payload in stock_seed_payloads)
    assert len(seed_pairs) >= STOCK_DEPTH_MIN_TOOL_EXECUTIONS
    assert react_config.max_tool_iterations >= 10
    stock_policy = directive["individual_stock_depth_analysis_policy"]
    assert set(stock_policy["required_stock_tools"]) == set(STOCK_DEPTH_STOCK_TOOL_NAMES)
    assert set(stock_policy["required_external_tools"]) == set(STOCK_DEPTH_EXTERNAL_TOOL_NAMES)
    assert stock_policy["minimum_kline_trading_days"] == 60


def test_chat_turn_explicit_qe_draft_builds_cards_and_blocks_execution() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="帮我设计一个 QE 实验草案，先不要执行。"))

    assert len(fake.calls) == 1
    assert result["assistant_message"]["content_text"].startswith("已收到明确的 QE 实验草案任务")
    assert result["cards"]["intent_type"] == "experiment_draft_request"
    assert result["cards"]["plan_card"]["title"] == "QE 实验草案准备"
    plan_text = "\n".join(result["cards"]["plan_card"]["steps"])
    assert "股票池" in plan_text
    assert "template draft" in plan_text
    assert result["cards"]["clarification_card"]["questions"]
    assert "如需要固定迭代数量" in "\n".join(result["cards"]["clarification_card"]["questions"])
    capability_keys = {item["capability_key"] for item in result["cards"]["capability_cards"]}
    assert {"qe.create_experiment_draft", "qe.validate_template", "qe.run_experiment"} <= capability_keys
    assert result["cards"]["missing_capability_keys"] == []
    assert result["cards"]["status_rail"][3] == {"label": "等待确认", "status": "current"}
    assert result["cards"]["safety"]["no_materialize_before_confirmation"] is True
    assert result["trace"]["status"] == "ok"
    assert result["mode_decision"]["mode"] == "planning"
    assert result["prompt_bundle"]["selection_trace_json"]["algorithm"] == "mode_routed_prompt_tree_v1"
    assert result["prompt_bundle"]["selection_trace_json"]["dialogue_intent"] == "experiment_draft_request"
    assert result["prompt_bundle"]["selection_trace_json"]["dialogue_mode"] == "planning"
    assert result["prompt_bundle"]["activation_id"]
    assert result["context_pack"]["pack_summary"].startswith("Context Pack:")
    assert fake.calls[0]["temperature"] == svc.active_runtime_config()["compaction"]["worker"]["temperature"]
    assert fake.calls[0]["max_tokens"] == svc.active_runtime_config()["budget"]["response"]["max_tokens"]
    assert result["context_health"]["show_badge"] is True
    assert result["cards"]["context_health"]["config_driven"] is True
    traces = svc.list_records("context_assembly_traces", filters={"conversation_id": result["conversation"]["conversation_id"]})
    assert traces["total"] == 1
    assert traces["items"][0]["runtime_config_activation_id"]
    event_types = {event["event_type"] for event in result["task_events"]}
    assert {"chat_received", "prompt_bundle_built", "context_pack_built", "llm_started", "llm_done", "action_proposed"} <= event_types

    blocked = svc.chat_turn(ChatTurnRequest(message="确认执行 QE materialize", allow_execute=True))
    assert blocked["mode_decision"]["mode"] == "execution"
    assert blocked["mode_decision"]["requires_approval"] is True


class _ChatApprovalToolCallLlm(FakeLlmClient):
    def __init__(self, tool_calls: list[McpToolCall], *, first_content: str = "Need approval gate.") -> None:
        super().__init__()
        self._tool_calls = tool_calls
        self._first_content = first_content

    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            return LlmCallResult(
                content=self._first_content,
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
                tool_calls=[copy.deepcopy(call) for call in self._tool_calls],
            )
        return LlmCallResult(
            content="Insufficient evidence: approval is required before execution.",
            provider="fake",
            model="fake-primary",
            duration_ms=1,
            usage={},
        )


def _qe_template_create_call(*, stable_call_id: str = "qe_template_create", title: str = "chat approval draft") -> McpToolCall:
    return McpToolCall(
        server_key="aistock-qe",
        tool_name="qe_template_create",
        payload_json={
            "template_kind": "custom_evo",
            "title": title,
            "config_json": {
                "loops": [{"factor_keys": ["alpha001"], "model_id": "lightgbm"}],
                "stock_pool": "fixed_pool",
                "backtest_window": {"start": "2024-01-01", "end": "2024-12-31"},
            },
        },
        stable_call_id=stable_call_id,
    )


def _qe_template_materialize_call() -> McpToolCall:
    return McpToolCall(
        server_key="aistock-qe",
        tool_name="qe_template_materialize_confirmed",
        payload_json={"template_id": "qet_existing", "confirm_template": "MATERIALIZE_QE_TEMPLATE"},
        stable_call_id="qe_template_materialize",
    )


def _chat_approval_proposal(
    tool_calls: list[McpToolCall] | None = None,
    *,
    first_content: str = "Need approval gate.",
) -> tuple[ResearchAssistantService, dict[str, object], dict[str, object]]:
    svc = _chat_service(_ChatApprovalToolCallLlm(tool_calls or [_qe_template_create_call()], first_content=first_content))
    result = svc.chat_turn(ChatTurnRequest(message="create qe template draft", dialogue_mode_override="planning"))
    proposal = result["cards"]["action_proposals"][-1]  # type: ignore[index]
    return svc, result, proposal


def test_chat_turn_preflight_card_includes_approval_id_and_exact_confirmation_token() -> None:
    svc, result, proposal = _chat_approval_proposal(first_content="Agent says CONFIRM_QE_DRAFT but must not self-approve.")

    approval_id = proposal["approval_id"]
    approval = svc.repository.get_record("approvals", approval_id)
    assert approval["status"] == "pending"
    assert proposal["required_confirmation_text"] == "CONFIRM_QE_DRAFT"
    assert result["cards"]["mcp_execution_result"]["approval_id"] == approval_id
    assert result["cards"]["mcp_execution_result"]["required_confirmation_text"] == "CONFIRM_QE_DRAFT"
    assert "审批 ID" in result["assistant_message"]["content_text"]
    assert "CONFIRM_QE_DRAFT" in result["assistant_message"]["content_text"]
    assert result["cards"]["mcp_execution_result"]["executed"] is False


def test_chat_turn_confirm_approval_id_and_exact_token_consumes_and_executes() -> None:
    svc, result, proposal = _chat_approval_proposal()
    approval_id = str(proposal["approval_id"])
    conversation_id = str(result["conversation"]["conversation_id"])

    confirmed = svc.chat_turn(
        ChatTurnRequest(
            message="confirm approval",
            conversation_id=conversation_id,
            confirm_approval_id=approval_id,
            confirmation_text="CONFIRM_QE_DRAFT",
        )
    )

    assert svc.repository.get_record("approvals", approval_id)["status"] == "approved"
    assert confirmed["cards"]["approval_confirmation"]["status"] == "executed"
    assert confirmed["cards"]["approval_confirmation"]["approval_id"] == approval_id
    assert confirmed["cards"]["approval_confirmation"]["confirmation_source"] == "explicit_request_field"
    assert confirmed["cards"]["mcp_execution_result"]["triggered_by_approval"] is True
    assert confirmed["cards"]["mcp_execution_result"]["executed"] is True
    assert confirmed["cards"]["mcp_execution_result"]["auto_executed"] is False


def test_chat_turn_wrong_token_rejects_and_keeps_approval_pending() -> None:
    svc, result, proposal = _chat_approval_proposal()
    approval_id = str(proposal["approval_id"])

    rejected = svc.chat_turn(
        ChatTurnRequest(
            message="confirm approval",
            conversation_id=str(result["conversation"]["conversation_id"]),
            confirm_approval_id=approval_id,
            confirmation_text="WRONG_TOKEN",
        )
    )

    assert svc.repository.get_record("approvals", approval_id)["status"] == "pending"
    assert rejected["cards"]["approval_confirmation"]["status"] == "blocked"
    assert rejected["cards"]["approval_confirmation"]["reason_code"] == "approval_confirmation_text_mismatch"
    assert rejected["cards"]["mcp_execution_result"]["executed"] is False


def test_chat_turn_cross_conversation_approval_is_rejected() -> None:
    svc, _result, proposal = _chat_approval_proposal()
    approval_id = str(proposal["approval_id"])
    other_conversation = svc.create_conversation(ConversationCreate(title="other conversation"))

    rejected = svc.chat_turn(
        ChatTurnRequest(
            message="confirm approval",
            conversation_id=other_conversation["conversation_id"],
            confirm_approval_id=approval_id,
            confirmation_text="CONFIRM_QE_DRAFT",
        )
    )

    assert svc.repository.get_record("approvals", approval_id)["status"] == "pending"
    assert rejected["cards"]["approval_confirmation"]["reason_code"] == "approval_confirmation_cross_conversation"
    assert rejected["cards"]["mcp_execution_result"]["executed"] is False


def test_chat_turn_natural_language_affirmation_maps_only_single_pending_l1_approval() -> None:
    svc, result, proposal = _chat_approval_proposal()
    approval_id = str(proposal["approval_id"])

    confirmed = svc.chat_turn(ChatTurnRequest(message="同意执行这个审批", conversation_id=str(result["conversation"]["conversation_id"])))

    assert svc.repository.get_record("approvals", approval_id)["status"] == "approved"
    assert confirmed["cards"]["approval_confirmation"]["confirmation_source"] == "user_natural_language_affirmation"
    assert confirmed["cards"]["mcp_execution_result"]["executed"] is True


def test_chat_turn_natural_language_affirmation_rejects_multiple_pending_approvals() -> None:
    svc, result, _proposal = _chat_approval_proposal(
        [
            _qe_template_create_call(stable_call_id="create_a", title="draft A"),
            _qe_template_create_call(stable_call_id="create_b", title="draft B"),
        ]
    )

    rejected = svc.chat_turn(ChatTurnRequest(message="同意执行", conversation_id=str(result["conversation"]["conversation_id"])))

    pending = svc.list_records("approvals", filters={"status": "pending"}, limit=10)["items"]
    assert len(pending) == 2
    assert rejected["cards"]["approval_confirmation"]["reason_code"] == "approval_confirmation_ambiguous_pending_approval"
    assert rejected["cards"]["mcp_execution_result"]["executed"] is False


def test_chat_turn_l2_naked_affirmation_requires_explicit_confirmation_token() -> None:
    svc, result, proposal = _chat_approval_proposal([_qe_template_materialize_call()])
    approval_id = str(proposal["approval_id"])

    rejected = svc.chat_turn(ChatTurnRequest(message="同意执行", conversation_id=str(result["conversation"]["conversation_id"])))

    assert svc.repository.get_record("approvals", approval_id)["status"] == "pending"
    assert proposal["required_confirmation_text"] == "CONFIRM_QE_MATERIALIZE"
    assert rejected["cards"]["approval_confirmation"]["reason_code"] == "approval_confirmation_l2_requires_explicit_token"
    assert rejected["cards"]["mcp_execution_result"]["executed"] is False


def test_chat_turn_agent_tool_output_cannot_self_approve_without_user_token() -> None:
    svc, _result, proposal = _chat_approval_proposal(first_content="I approve this myself with CONFIRM_QE_DRAFT.")
    approval_id = str(proposal["approval_id"])

    approval = svc.repository.get_record("approvals", approval_id)
    action = svc.repository.get_record("action_proposals", str(proposal["action_proposal_id"]))

    assert approval["status"] == "pending"
    assert action["status"] == "approval_required"
    assert action.get("approval_id") == approval_id


def test_chat_turn_prior_messages_injected_into_llm_context() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    conv_id = "conv_test_001"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "test conversation",
        "user_id": "default",
        "status": "active",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_001",
        "conversation_id": conv_id,
        "role": "user",
        "content_text": "第一轮用户消息：帮我分析因子覆盖率。",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_002",
        "conversation_id": conv_id,
        "role": "assistant",
        "content_text": "第一轮助手回复：好的，我来分析因子覆盖率。",
    })

    result = svc.chat_turn(ChatTurnRequest(
        message="继续上一轮的讨论，补充更多细节。",
        conversation_id=conv_id,
    ))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    assert isinstance(messages, list)
    roles = [str(m["role"]) for m in messages]
    assert roles[0] == "system"
    assert roles[-1] == "user"
    assert messages[-1]["content"] == "继续上一轮的讨论，补充更多细节。"

    all_content = " ".join(str(m["content"]) for m in messages)
    assert "因子覆盖率" in all_content
    assert result["conversation"]["conversation_id"] == conv_id


def test_new_conversation_has_no_prior_messages() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    svc.chat_turn(ChatTurnRequest(message="帮我设计一个 QE 实验草案，先不要执行。"))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    assert isinstance(messages, list)
    non_system = [m for m in messages if m["role"] != "system"]
    assert len(non_system) == 1
    assert "Internal Context Pack" in " ".join(str(m["content"]) for m in messages if m["role"] == "system")
    assert non_system[0]["role"] == "user"
    assert non_system[0]["content"] == "帮我设计一个 QE 实验草案，先不要执行。"


def test_chat_history_includes_all_roles() -> None:
    """system and tool messages are preserved — they carry context the LLM needs."""
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    conv_id = "conv_test_002"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "role preservation test",
        "user_id": "default",
        "status": "active",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_010",
        "conversation_id": conv_id,
        "role": "system",
        "content_text": "系统提示：当前阶段是 planning。",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_011",
        "conversation_id": conv_id,
        "role": "user",
        "content_text": "用户消息：开始分析。",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_012",
        "conversation_id": conv_id,
        "role": "tool",
        "content_text": '{"tool_result": "raw json here"}',
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_013",
        "conversation_id": conv_id,
        "role": "assistant",
        "content_text": "助手回复：分析结果如下。",
    })

    svc.chat_turn(ChatTurnRequest(
        message="第二轮用户消息",
        conversation_id=conv_id,
    ))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    all_content = " ".join(str(m["content"]) for m in messages)
    assert "用户消息：开始分析" in all_content
    assert "助手回复：分析结果如下" in all_content
    assert "系统提示：当前阶段是 planning" in all_content
    assert "raw json here" in all_content


def test_chat_history_preserves_full_message_content() -> None:
    """Messages must not be truncated — full content must reach the LLM."""
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    conv_id = "conv_test_003"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "no truncation test",
        "user_id": "default",
        "status": "active",
    })
    long_content = "长消息" + "X" * 2000
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_020",
        "conversation_id": conv_id,
        "role": "assistant",
        "content_text": long_content,
    })

    svc.chat_turn(ChatTurnRequest(
        message="继续",
        conversation_id=conv_id,
    ))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    all_content = " ".join(str(m["content"]) for m in messages)
    assert long_content in all_content


def test_chat_history_token_budget_drops_oldest_first() -> None:
    """When budget is exceeded, oldest messages are dropped, not truncated."""
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    conv_id = "conv_test_004"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "token budget test",
        "user_id": "default",
        "status": "active",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_030",
        "conversation_id": conv_id,
        "role": "user",
        "content_text": "最早的消息",
    })
    svc.repository.create_record("conversation_messages", {
        "message_id": "msg_031",
        "conversation_id": conv_id,
        "role": "assistant",
        "content_text": "最新的消息",
    })

    svc.chat_turn(ChatTurnRequest(
        message="当前消息",
        conversation_id=conv_id,
    ))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    all_content = " ".join(str(m["content"]) for m in messages)
    assert "最新的消息" in all_content
    assert "当前消息" in all_content


def test_long_chat_auto_compacts_with_key_facts_and_fresh_tail(tmp_path) -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    def mutate(config: dict[str, object]) -> None:
        config["model_context"]["fallback_context_window_tokens"] = 10000
        config["model_context"]["safety_buffer"]["ratio"] = 0.01
        config["model_context"]["safety_buffer"]["min_tokens"] = 1
        config["budget"]["response"]["reserved_ratio"] = 0.01
        config["budget"]["response"]["min_reserved_tokens"] = 1
        config["budget"]["response"]["max_tokens"] = 64
        config["budget"]["context_pack"]["min_tokens"] = 1
        config["history_fetch"]["page_size"] = 10
        config["history_fetch"]["max_pages"] = 2
        config["fresh_tail"]["min_messages"] = 1
        config["compaction"]["trigger"]["min_turns_before_compaction"] = 1
        config["compaction"]["trigger"]["min_messages_before_compaction"] = 2
        config["compaction"]["trigger"]["proactive_utilization_ratio"] = 0.05
        config["compaction"]["trigger"]["mandatory_utilization_ratio"] = 0.10
        config["compaction"]["worker"]["max_output_ratio"] = 0.10

    _reload_runtime_config_fixture(svc, tmp_path, mutate)
    config = svc.active_runtime_config()

    conv_id = "conv_compact_001"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "compact test",
        "user_id": "default",
        "status": "active",
    })
    for idx in range(4):
        svc.repository.create_record("conversation_messages", {
            "message_id": f"msg_compact_{idx}",
            "conversation_id": conv_id,
            "role": "user" if idx % 2 == 0 else "assistant",
            "content_text": f"历史消息 {idx}: " + ("重要参数A " * 80),
        })

    svc.chat_turn(ChatTurnRequest(message="继续刚才的重要参数A", conversation_id=conv_id))

    assert len(fake.calls) == 3
    assert fake.calls[0]["max_tokens"] == int((10000 - 100 - 100) * config["compaction"]["worker"]["max_output_ratio"])
    segments = svc.list_records("context_segments", filters={"conversation_id": conv_id, "status": "active"})
    facts = svc.list_records("context_key_facts", filters={"conversation_id": conv_id, "status": "active"})
    traces = svc.list_records("context_assembly_traces", filters={"conversation_id": conv_id})
    assert segments["total"] == 1
    assert facts["total"] == 1
    assert facts["items"][0]["fact_type"] == "key_fact_block"
    assert facts["items"][0]["fact_json"]["prompt_key"] == "context.compaction.key_fact_extraction"
    assert traces["total"] == 1
    final_messages = fake.calls[-1]["messages"]
    final_content = " ".join(str(m["content"]) for m in final_messages)
    assert "重要参数A" in final_content
    assert "3:" in final_content


def test_reactive_context_overflow_compacts_and_retries_without_user_interruption(tmp_path) -> None:
    fake = PromptTooLongOnceLlmClient()
    svc = _chat_service(fake)

    def mutate(config: dict[str, object]) -> None:
        config["fresh_tail"]["min_messages"] = 1
        config["compaction"]["trigger"]["min_turns_before_compaction"] = 1
        config["compaction"]["trigger"]["min_messages_before_compaction"] = 2
        config["compaction"]["worker"]["max_retries"] = 2

    _reload_runtime_config_fixture(svc, tmp_path, mutate)

    conv_id = "conv_reactive_001"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "reactive compact test",
        "user_id": "default",
        "status": "active",
    })
    for idx in range(3):
        svc.repository.create_record("conversation_messages", {
            "message_id": f"msg_reactive_{idx}",
            "conversation_id": conv_id,
            "role": "user" if idx % 2 == 0 else "assistant",
            "content_text": f"待保留关键上下文 {idx}: 用户已确认参数和风险边界。",
        })

    result = svc.chat_turn(ChatTurnRequest(message="继续，不要让我重复背景", conversation_id=conv_id))

    assert fake.failed_once is True
    assert result["assistant_message"]["content_text"].startswith("已在自动压缩后继续")
    segments = svc.list_records("context_segments", filters={"conversation_id": conv_id, "status": "active"})
    facts = svc.list_records("context_key_facts", filters={"conversation_id": conv_id, "status": "active"})
    traces = svc.list_records("context_assembly_traces", filters={"conversation_id": conv_id})
    assert segments["total"] == 1
    assert facts["total"] == 1
    assert traces["total"] == 2
    assert {item["status"] for item in traces["items"]} == {"ok", "retry_after_compaction"}
    retry_messages = fake.calls[-1]["messages"]
    assert any("上一次模型调用因为上下文过长" in str(message["content"]) for message in retry_messages)


def test_model_routing_uses_runtime_config_long_context_threshold(tmp_path) -> None:
    svc = _service()

    def mutate(config: dict[str, object]) -> None:
        config["model_routing"]["long_context_trigger_tokens"] = 10

    _reload_runtime_config_fixture(svc, tmp_path, mutate)

    route = svc.route_model(ModelRouteRequest(role="primary_reasoner", risk_level="medium", token_estimate=11))

    assert route["policy"]["policy_id"] == "route_long_context_medium"
    assert route["route_status"] == "fallback_selected"
    assert route["model_profile"]["model_profile_id"] == "model_deepseek_v4_pro_primary"


def test_high_risk_reactive_overflow_fail_fast_after_configured_retries(tmp_path) -> None:
    fake = MainPromptTooLongLlmClient()
    svc = _chat_service(fake)

    def mutate(config: dict[str, object]) -> None:
        config["fresh_tail"]["min_messages"] = 1
        config["compaction"]["trigger"]["min_turns_before_compaction"] = 1
        config["compaction"]["trigger"]["min_messages_before_compaction"] = 2
        config["compaction"]["worker"]["max_retries"] = 1

    _reload_runtime_config_fixture(svc, tmp_path, mutate)

    conv_id = "conv_fail_fast_001"
    svc.repository.create_record("conversations", {
        "conversation_id": conv_id,
        "title": "fail fast compact test",
        "user_id": "default",
        "status": "active",
    })
    for idx in range(3):
        svc.repository.create_record("conversation_messages", {
            "message_id": f"msg_fail_fast_{idx}",
            "conversation_id": conv_id,
            "role": "user",
            "content_text": f"高风险上下文 {idx}: 保留审批状态和风险边界。",
        })

    with pytest.raises(RuntimeError, match="High-risk Research Assistant task stopped"):
        svc.chat_turn(ChatTurnRequest(message="高风险继续执行前检查", conversation_id=conv_id, risk_level="high"))

    segments = svc.list_records("context_segments", filters={"conversation_id": conv_id, "status": "active"})
    traces = svc.list_records("trace_events", filters={"status": "context_overflow_fail_fast"})
    assert segments["total"] == 1
    assert traces["total"] == 1


def test_read_only_partial_evidence_degraded_reply_is_returned_by_compose_path() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.STOCK_ANALYSIS_REQUEST,
        confidence=0.95,
        mode_reason="read_only_partial_evidence",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )
    degraded_text = (
        "Read-only partial evidence note: source=stock_quote:000688 as_of=2026-06-24. "
        "Missing / not covered: external_research. reason_code=read_only_partial_evidence_degraded"
    )
    cards = {
        "react_grounding": {
            "stopped_reason": "read_only_partial_evidence_degraded",
            "evidence_guard": {
                "allowed": True,
                "reason": "read_only_partial_evidence_degraded",
                "source_count": 1,
                "as_of_count": 1,
            },
            "tool_errors": [],
        }
    }

    text = svc._compose_assistant_reply("stock depth question", degraded_text, cards, mode_decision)

    assert text == degraded_text


def _bug_538_capability_inquiry_mode() -> ModeDecision:
    return ModeDecision(
        mode=DialogueMode.ANALYSIS,
        intent_type=DialogueIntent.MCP_CAPABILITY_INQUIRY,
        confidence=0.93,
        mode_reason="capability_inquiry",
        requires_tool=True,
        allowed_tool_side_effect="read_only",
        requires_user_confirmation=False,
        requires_approval=False,
        visible_audit_default=False,
    )


def _bug_538_react_grounded_business_cards() -> dict[str, object]:
    return {
        "react_grounding": {
            "schema_version": "research_assistant_react_grounding_v1",
            "tool_result_count": 1,
            "executed_tools": [
                {
                    "server_key": "aistock-stock-analysis",
                    "tool_name": "stock_analysis_get_quote",
                    "status": "succeeded",
                    "side_effect_level": "read_only",
                }
            ],
            "evidence_guard": {
                "allowed": True,
                "reason": "ok",
                "source_count": 1,
                "as_of_count": 1,
            },
            "tool_errors": [],
        }
    }


def _bug_538_catalog_cards() -> dict[str, object]:
    return {
        "runtime_mcp_catalog": {
            "source": "gateway_manifest_derived_catalog",
            "server_count": 1,
            "tool_count": 2,
            "capability_count": 1,
            "tools_by_server": {},
            "servers_by_key": {},
        }
    }


def test_bug_538_grounded_react_answer_wins_over_mcp_catalog_canned_reply() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_react_grounded_business_cards()
    cards.update(_bug_538_catalog_cards())
    grounded_answer = (
        "Grounded answer: 国城矿业已完成只读取证；source stock_quote_000688 as_of 2026-06-27. "
        "我会围绕基本面、近期走势和风险因素合成，不返回工具目录。"
    )

    text = svc._compose_assistant_reply("请用 MCP 工具分析国城矿业", grounded_answer, cards, mode_decision)

    assert text == grounded_answer
    assert "Grounded answer" in text
    assert "个业务域" not in text
    assert "个可用能力" not in text


def test_bug_538_pure_mcp_catalog_inquiry_still_returns_catalog_without_business_tools() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_catalog_cards()

    text = svc._compose_assistant_reply(
        "你有哪些 MCP 工具？",
        "I can use arbitrary file and HTTP tools.",
        cards,
        mode_decision,
    )

    assert "个业务域" in text
    assert "个可用能力" in text
    assert "arbitrary file" not in text


def test_bug_538_grounded_forbidden_marker_answer_is_cleaned_not_replaced() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_react_grounded_business_cards()
    grounded_answer = (
        "Bottom-line: 国城矿业当前回答已基于只读行情和财务证据合成；"
        "raw_payload 显示的内部载荷只用于核对，不应展示字段名。\n"
        "server_key=aistock-stock-analysis tool_name=stock_analysis_get_quote omitted_sections=[debug]\n"
        "结论：保留基本面、近期走势、风险和证据来源，不做方向预测。"
    )

    text = svc._compose_assistant_reply("请分析国城矿业的 MCP 取证结果", grounded_answer, cards, mode_decision)

    assert "Bottom-line" in text
    assert "国城矿业" in text
    assert "不做方向预测" in text
    assert "Insufficient evidence: business reply synthesis did not pass grounding guard." not in text
    for marker in ("server_key", "raw_payload", "omitted_sections"):
        assert marker not in text


def test_bug_538_forbidden_marker_cleanup_preserves_short_section_headings() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_react_grounded_business_cards()
    grounded_answer = (
        "Bottom-line: 已基于只读行情和资金流证据合成。\n"
        "## 一、跌停原因分析\n"
        "盘面出现放量下跌，需要结合公告、资金流与板块表现交叉核对。\n"
        "raw_payload=debug server_key=aistock-stock-analysis omitted_sections=[internal]\n"
        "## 收尾结论\n"
        "保留基本面、近期走势、风险和证据来源，不做方向预测。"
    )

    text = svc._compose_assistant_reply("请分析国城矿业的 MCP 取证结果", grounded_answer, cards, mode_decision)

    assert "## 一、跌停原因分析" in text
    assert "## 收尾结论" in text
    assert text.count("## ") >= 2
    assert "盘面出现放量下跌" in text
    assert "不做方向预测" in text
    assert "Insufficient evidence: business reply synthesis did not pass grounding guard." not in text
    for marker in ("server_key", "raw_payload", "omitted_sections"):
        assert marker not in text


def test_bug_538_forbidden_marker_cleanup_preserves_internal_field_guard() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_react_grounded_business_cards()

    text = svc._compose_assistant_reply(
        "请分析工具结果",
        "raw_payload: valuation evidence; server_key=aistock-stock-analysis; omitted_sections=debug",
        cards,
        mode_decision,
    )

    assert "valuation evidence" in text
    assert not ResearchAssistantService._contains_mcp_business_forbidden_marker(text)
    for marker in ("server_key", "raw_payload", "omitted_sections"):
        assert marker not in text


def test_bug_538_forbidden_marker_cleanup_falls_back_when_no_substantive_text_remains() -> None:
    svc = _chat_service(FakeLlmClient())
    mode_decision = _bug_538_capability_inquiry_mode()
    cards = _bug_538_react_grounded_business_cards()
    cards["mcp_execution_result"] = {
        "auto_executed": True,
        "status": "succeeded",
        "server_key": "aistock-stock-analysis",
        "tool_name": "stock_analysis_get_quote",
        "route": "aistock-stock-analysis/stock_analysis_get_quote",
    }
    cards["mcp_summary_result"] = {
        "response_mode": "stock_analysis_evidence_card",
        "source": "stock_analysis_read_adapter",
        "as_of": "2026-06-27",
    }

    text = svc._compose_assistant_reply(
        "请分析工具结果",
        "raw_payload\nserver_key\nomitted_sections",
        cards,
        mode_decision,
    )

    assert text == "Insufficient evidence: business reply synthesis did not pass grounding guard."
