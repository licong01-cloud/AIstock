from __future__ import annotations

import json

import pytest

from backend.services.research_assistant.models import (
    ApprovalCreate,
    ChatTurnRequest,
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
from backend.services.research_assistant.repository import DatabaseResearchAssistantRepository, InMemoryResearchAssistantRepository, TABLES
from backend.services.research_assistant.service import (
    ASSISTANT_APPROVAL_CONFIRM,
    DialogueIntent,
    DialogueMode,
    LlmCallResult,
    ModeDecision,
    ResearchAssistantCatalogNotReadyError,
    ResearchAssistantService,
)


class DialogueAwareFakeLlmClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
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
        del kwargs
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
        return []

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


def test_catalog_readiness_blocks_chat_until_seeded() -> None:
    fake = FakeLlmClient()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)

    health = svc.health()
    assert health["status"] == "catalog_not_ready"
    assert health["catalog_readiness"]["ready"] is False
    assert "prompt_nodes" in health["catalog_readiness"]["missing_catalogs"]

    with pytest.raises(ResearchAssistantCatalogNotReadyError) as excinfo:
        svc.chat_turn(ChatTurnRequest(message="帮我设计一个 QE 实验草案，先不要执行。"))
    assert excinfo.value.readiness["operator_action"] == "POST /api/v1/research-assistant/catalogs/seed"
    assert fake.calls == []

    with pytest.raises(ResearchAssistantCatalogNotReadyError):
        svc.build_prompt_bundle(PromptBundleBuildRequest(user_message="QE 实验草案", phase="planning"))

    seed_result = svc.seed_catalogs()
    assert seed_result["seeded"]["prompt_nodes"] >= 1
    assert svc.health()["status"] == "ok"
    assert svc.catalog_readiness()["ready"] is True


def test_seed_catalogs_retires_superseded_active_activations() -> None:
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

    assert repository.get_record("runtime_config_activations", "runtime_config_activation_old_active")["status"] == "retired"
    assert repository.get_record("prompt_activations", "prompt_activation_old_active")["status"] == "retired"
    prompt_actives = repository.list_records(
        "prompt_activations",
        filters={"assistant_key": "research_assistant", "environment": svc.environment, "status": "active"},
        limit=10,
    )["items"]
    runtime_actives = repository.list_records(
        "runtime_config_activations",
        filters={"config_key": "research_assistant.runtime_context", "environment": svc.environment, "status": "active"},
        limit=10,
    )["items"]
    assert len(prompt_actives) == 1
    assert len(runtime_actives) == 1
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
        IssueCandidateCreate(title="候选缺陷", severity="P1", problem_statement="只进入候选队列，不直接创建正式 GitHub Issue。")
    )
    assert issue["status"] == "needs_review"
    assert issue["github_sync_status"] == "not_requested"
    assert issue["github_sync_json"]["formal_github_issue_requires_approval"] is True


def test_preflight_high_risk_requires_approval_and_records_event() -> None:
    svc = _service()
    task = svc.create_task(TaskCreate(title="候选 issue preflight"))

    result = svc.preflight_mcp_tool(
        McpPreflightRequest(
            task_id=task["task_id"],
            server_key="research-assistant",
            tool_name="assistant_create_issue_candidate",
            payload_json={"title": "P1"},
        )
    )

    assert result["passed"] is False
    assert result["approval_required"] is True
    assert result["failed_checks"][0]["check"] == "input_schema"
    assert result["preflight_checks"] == ["dedupe_key", "evidence_refs", "draft_only", "github_formal_issue_blocked"]
    assert result["assistant_usable"] == "preflight_required"
    detail = svc.get_task(task["task_id"])
    assert any(event["event_type"] == "mcp_preflight_failed" for event in detail["events"])

    ok_result = svc.preflight_mcp_tool(
        McpPreflightRequest(
            task_id=task["task_id"],
            server_key="research-assistant",
            tool_name="assistant_create_issue_candidate",
            payload_json={"title": "P1", "problem_statement": "problem"},
        )
    )
    assert ok_result["passed"] is False
    assert ok_result["approval_required"] is True

    tool = svc.repository.find_one("mcp_tools", {"server_key": "research-assistant", "tool_name": "assistant_create_issue_candidate"})
    svc.repository.update_record("mcp_tools", tool["tool_id"], {"status": "disabled"})
    disabled_result = svc.preflight_mcp_tool(
        McpPreflightRequest(server_key="research-assistant", tool_name="assistant_create_issue_candidate", payload_json={"title": "P1", "problem_statement": "problem"})
    )
    assert disabled_result["passed"] is False
    assert disabled_result["failed_checks"][0]["check"] == "tool_status"


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


def test_candidate_issue_duplicate_does_not_hide_canonical_candidate() -> None:
    svc = _service()
    first = svc.create_issue_candidate(IssueCandidateCreate(title="Duplicate Gate", problem_statement="same"))
    second = svc.create_issue_candidate(IssueCandidateCreate(title="Duplicate Gate", problem_statement="same"))
    assert second["candidate_id"] == first["candidate_id"]
    assert second["deduplicated"] is True
    assert svc.repository.get_record("issue_candidates", first["candidate_id"])["status"] == "needs_review"


def test_candidate_issue_github_sync_gate_never_creates_github_issue() -> None:
    svc = _service()
    issue = svc.create_issue_candidate(IssueCandidateCreate(title="GitHub gate", problem_statement="must not create directly"))

    dry_run = svc.github_sync_issue_candidate(issue["candidate_id"], IssueCandidateGithubSyncRequest(mode="dry_run", requested_by="pytest"))
    assert dry_run["github_sync_status"] == "dry_run"
    assert dry_run["github_sync_json"]["direct_github_create_performed"] is False

    formal_without_approval = svc.github_sync_issue_candidate(issue["candidate_id"], IssueCandidateGithubSyncRequest(mode="formal"))
    assert formal_without_approval["github_sync_status"] == "approval_required"

    approval = svc.create_approval(
        ApprovalCreate(
            approval_type="issue.github_sync",
            plan_digest="digest-github-sync",
            summary="GitHub gate formal sync",
            required_confirmation_text=ASSISTANT_APPROVAL_CONFIRM,
        )
    )
    formal_blocked = svc.github_sync_issue_candidate(
        issue["candidate_id"],
        IssueCandidateGithubSyncRequest(mode="formal", approval_id=approval["approval_id"], confirmation_text=ASSISTANT_APPROVAL_CONFIRM),
    )
    assert formal_blocked["github_sync_status"] == "blocked"
    assert formal_blocked["github_sync_json"]["direct_github_create_performed"] is False
    assert formal_blocked["github_issue_url"] is None


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

    workflow_capability = svc.repository.find_one("capabilities", {"capability_key": "local_data.plan_repair"})
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

    prompt_nodes = svc.repository.list_records("prompt_nodes", filters={"status": "enabled"}, limit=200)["items"]
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


def test_runtime_config_controls_api_page_defaults_and_max() -> None:
    svc = _service()
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["query_limits"] = dict(config["query_limits"])
    config["query_limits"]["api_list_skills"] = 2
    config["query_limits"]["api_list_max_page_size"] = 3
    config["query_limits"]["router_mcp_servers"] = 1
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

    skills = svc.list_records("skills")
    assert skills["page_size"] == 2
    assert len(skills["items"]) == 2

    mcp_servers = svc.list_records("mcp_servers", limit_key="router_mcp_servers")
    assert mcp_servers["page_size"] == 1

    with pytest.raises(ValueError, match="api_list_max_page_size"):
        svc.list_records("skills", limit=4)
    with pytest.raises(ValueError, match="limit must be positive"):
        svc.list_records("skills", limit=0)


def test_context_pack_token_budget_max_is_runtime_config_driven() -> None:
    svc = _service()
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["query_limits"] = dict(config["query_limits"])
    config["query_limits"]["context_pack_max_token_budget"] = 9
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})
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
    assert "assistant create issue candidate" in text
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
    assert catalog["manifest_tool_count"] == 345
    assert catalog["tool_count"] == 345
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
)


def _chat_service_with_qe_fakes(fake: FakeLlmClient | None = None) -> ResearchAssistantService:
    svc = _chat_service(fake)
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
    assert "\u5df2\u540c\u6b65\u6210\u529f" in text
    assert "\u540c\u6b65\u5931\u8d25" in text
    assert "\u672a\u540c\u6b65/\u672a\u8fd0\u884c" in text
    assert "\u8fd0\u884c\u4e2d/\u6392\u961f\u4e2d" in text
    assert "\u544a\u8b66/\u963b\u65ad/\u9700\u8981\u5904\u7406" in text
    assert "daily_basic" in text
    assert "stock_moneyflow_ts" in text
    assert "anns_metadata" in text
    assert "\u672c\u8f6e\u672a\u6267\u884c\u4efb\u4f55\u540c\u6b65" in text
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
    assert "\u5df2\u6c47\u603b QE \u5b9e\u9a8c\u72b6\u6001\u5982\u4e0b" in text
    assert "completed=1" in text
    assert "running=1" in text
    assert "failed=1" in text
    assert "exp_completed" in text
    assert "alpha baseline" in text
    assert "\u672c\u8f6e\u672a\u542f\u52a8\u3001\u6267\u884c\u3001\u7269\u5316\u6216\u4fee\u6539\u4efb\u4f55 QE \u5b9e\u9a8c" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_experiment"
    assert route["tool_name"] == "qe_experiment_list"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_experiment_status_summary"
    assert summary["source"] == "qe_experiment_read_adapter"
    assert summary["summary_kind"] == "qe_experiments"
    assert result["cards"]["react_grounding"]["stopped_reason"] == "business_summary_terminal"


def test_bug_357_qe_custom_evo_progress_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()

    result = svc.chat_turn(ChatTurnRequest(message="custom_evo \u4efb\u52a1\u6700\u65b0\u8fdb\u5ea6\u600e\u4e48\u6837\uff1f\u7ed9\u6211\u72b6\u6001\u6c47\u603b"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "\u5df2\u6c47\u603b QE custom_evo \u4efb\u52a1\u8fdb\u5ea6\u5982\u4e0b" in text
    assert "custom evo alpha" in text
    assert "task-custom-1" in text
    assert "current_loop=2" in text
    assert "loops[completed=1" in text
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
    assert "QE \u6570\u4ed3\u5065\u5eb7\u6c47\u603b\u5982\u4e0b" in text
    assert "run_count=7" in text
    assert "pending_outbox=1" in text
    assert "outbox \u72b6\u6001" in text
    assert "research_valid" in text
    assert "\u672c\u8f6e\u672a\u6267\u884c backfill\u3001\u91cd\u8dd1\u3001\u5199\u5e93\u6216\u4efb\u4f55\u9ad8\u98ce\u9669 QE \u64cd\u4f5c" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_warehouse"
    assert route["tool_name"] == "qe_archive_health"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_warehouse_business_summary"
    assert summary["source"] == "qe_archive_read_adapter"


def test_bug_357_qe_warehouse_leaderboard_business_reply_without_diagnostics() -> None:
    svc = _chat_service_with_qe_fakes()

    result = svc.chat_turn(ChatTurnRequest(message="\u67e5\u770b QE run leaderboard\uff0c\u544a\u8bc9\u6211\u6700\u597d\u7684\u6a21\u578b\u548c\u5173\u952e\u6307\u6807"))

    text = _assert_bug_357_no_diagnostic_reply(result)
    assert "QE run leaderboard \u6c47\u603b\u5982\u4e0b" in text
    assert "\u5f53\u524d\u5217\u8868\u9996\u4f4d" in text
    assert "run-best" in text
    assert "CatBoost" in text
    assert "cagr=0.22" in text
    route = result["cards"]["mcp_route_decision"]
    assert route["domain"] == "qe_warehouse"
    assert route["tool_name"] == "qe_archive_query_run_leaderboard"
    summary = result["cards"]["mcp_summary_result"]
    assert summary["response_mode"] == "qe_warehouse_business_summary"
    assert summary["summary_kind"] == "query_run_leaderboard"


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
    assert "\u5df2\u540c\u6b65\u6210\u529f" in text
    assert "\u540c\u6b65\u5931\u8d25" in text
    assert "\u672a\u540c\u6b65/\u672a\u8fd0\u884c" in text
    assert "\u8fd0\u884c\u4e2d/\u6392\u961f\u4e2d" in text
    assert "\u544a\u8b66/\u963b\u65ad/\u9700\u8981\u5904\u7406" in text
    assert "daily_basic" in text
    assert "stock_moneyflow_ts" in text
    assert "anns_metadata" in text
    assert "\u672c\u8f6e\u672a\u6267\u884c\u4efb\u4f55\u540c\u6b65" in text

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



def test_bug_356_generic_local_data_sync_summary_returns_dataset_status_list_without_diagnostics() -> None:
    class LocalDataSummaryLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
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
    class LocalDataSingleShotLlmClient(FakeLlmClient):
        def complete(self, **kwargs: object) -> LlmCallResult:
            self.calls.append(kwargs)
            if len(self.calls) > 1:
                raise AssertionError("terminal local-data summary should not ask the LLM to re-compose")
            return LlmCallResult(
                content="Unsourced placeholder answer that must not become the final reply.",
                provider="fake",
                model="fake-primary",
                duration_ms=1,
                usage={},
            )

    fake = LocalDataSingleShotLlmClient()
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
    assert "\u5df2\u540c\u6b65\u6210\u529f" in text
    assert len(fake.calls) == 1
    react = result["cards"]["react_grounding"]
    assert react["stopped_reason"] == "evidence_summary_fallback"
    assert react["iterations"] == 1
    assert react["evidence_guard"]["reason"] == "ok"
    assert result["cards"]["mcp_summary_result"]["response_mode"] == "local_data_daily_sync_status"


def test_bug_346_local_data_daily_status_renderer_wins_over_react_exhaustion() -> None:
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

    assert "Insufficient evidence" not in text
    assert "daily_basic" in text
    assert "local_data_get_preset_daily_status" not in text


def test_bug_352_local_data_daily_status_refreshes_stale_capability_cache() -> None:
    svc = _chat_service(FakeLlmClient())
    svc.local_data_service_factory = FakeLocalDataDailyStatusService
    capability = svc.repository.find_one("capabilities", {"capability_key": "local_data.mcp_orchestration"})
    assert capability is not None
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
    refreshed = svc.repository.find_one("capabilities", {"capability_key": "local_data.mcp_orchestration"})
    assert refreshed is not None
    assert any(ref["tool_name"] == "local_data_get_preset_daily_status" for ref in refreshed["mcp_tool_refs"])


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


def test_long_chat_auto_compacts_with_key_facts_and_fresh_tail() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
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
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

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


def test_reactive_context_overflow_compacts_and_retries_without_user_interruption() -> None:
    fake = PromptTooLongOnceLlmClient()
    svc = _chat_service(fake)
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["fresh_tail"]["min_messages"] = 1
    config["compaction"]["trigger"]["min_turns_before_compaction"] = 1
    config["compaction"]["trigger"]["min_messages_before_compaction"] = 2
    config["compaction"]["worker"]["max_retries"] = 2
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

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


def test_model_routing_uses_runtime_config_long_context_threshold() -> None:
    svc = _service()
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["model_routing"]["long_context_trigger_tokens"] = 10
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

    route = svc.route_model(ModelRouteRequest(role="primary_reasoner", risk_level="medium", token_estimate=11))

    assert route["policy"]["policy_id"] == "route_long_context_medium"
    assert route["route_status"] == "fallback_selected"
    assert route["model_profile"]["model_profile_id"] == "model_deepseek_v4_pro_primary"


def test_high_risk_reactive_overflow_fail_fast_after_configured_retries() -> None:
    fake = MainPromptTooLongLlmClient()
    svc = _chat_service(fake)
    activation = svc.active_runtime_config_activation()
    config = dict(activation["config_json"])
    config["fresh_tail"]["min_messages"] = 1
    config["compaction"]["trigger"]["min_turns_before_compaction"] = 1
    config["compaction"]["trigger"]["min_messages_before_compaction"] = 2
    config["compaction"]["worker"]["max_retries"] = 1
    svc.repository.update_record("runtime_config_activations", activation["activation_id"], {"config_json": config})

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
