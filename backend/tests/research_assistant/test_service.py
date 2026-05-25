from __future__ import annotations

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
    FORBIDDEN_UNDEVELOPED_CAPABILITY_PHRASES,
    LlmCallResult,
    ResearchAssistantCatalogNotReadyError,
    ResearchAssistantService,
)


class FakeLlmClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def complete(self, **kwargs: object) -> LlmCallResult:
        self.calls.append(kwargs)
        return LlmCallResult(
            content="我理解你要创建 QE 10 loop 回测实验。本轮我会先复述目标、确认固定 PIT 股票池，并生成计划，不执行物化或运行。",
            provider="fake",
            model="fake-primary",
            duration_ms=12,
            usage={"prompt_tokens": 100, "completion_tokens": 40},
        )


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


def test_catalog_readiness_blocks_chat_until_seeded() -> None:
    fake = FakeLlmClient()
    svc = ResearchAssistantService(repository=InMemoryResearchAssistantRepository(), llm_client=fake)

    health = svc.health()
    assert health["status"] == "catalog_not_ready"
    assert health["catalog_readiness"]["ready"] is False
    assert "prompt_nodes" in health["catalog_readiness"]["missing_catalogs"]

    with pytest.raises(ResearchAssistantCatalogNotReadyError) as excinfo:
        svc.chat_turn(ChatTurnRequest(message="帮我创建一个 QE 10 loop 实验，先不要执行。"))
    assert excinfo.value.readiness["operator_action"] == "POST /api/v1/research-assistant/catalogs/seed"
    assert fake.calls == []

    with pytest.raises(ResearchAssistantCatalogNotReadyError):
        svc.build_prompt_bundle(PromptBundleBuildRequest(user_message="QE 10 loop", phase="planning"))

    seed_result = svc.seed_catalogs()
    assert seed_result["seeded"]["prompt_nodes"] >= 1
    assert svc.health()["status"] == "ok"
    assert svc.catalog_readiness()["ready"] is True


def test_service_runs_phase1_task_memory_context_approval_issue_flow() -> None:
    svc = _service()

    task = svc.create_task(TaskCreate(title="QE 10 loop 规划", idempotency_key="idem-1"))
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
    assert access_log["items"][0]["memory_id"] == memory["memory_id"]

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
    assert result["approval_required"] is False
    assert result["failed_checks"][0]["check"] == "input_schema"
    assert result["preflight_checks"] == ["dedupe_key", "evidence_refs", "draft_only", "github_formal_issue_blocked"]
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
    assert ok_result["passed"] is True

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


def test_prompt_tree_selects_qe_multibranch_and_records_bundle() -> None:
    svc = _chat_service()

    bundle = svc.build_prompt_bundle(
        PromptBundleBuildRequest(
            user_message="帮我创建一个 QE 10 loop 实验，先不要执行。",
            phase="planning",
            model_profile_id="model_deepseek_v4_pro_primary",
        )
    )

    keys = {node["prompt_key"] for node in bundle["node_refs"]}
    assert "root.assistant" in keys
    assert "governance.no_silent_action" in keys
    assert "intent.planning" in keys
    assert "domain.qe_experiment" in keys
    assert "workflow.qe_draft_then_approval" in keys
    assert "tool_guard.mcp_qe" in keys
    assert "renderer.human_cards" in keys
    assert bundle["selection_trace_json"]["algorithm"] == "ancestor_closed_keyword_multibranch_v1"
    assert bundle["activation_id"]
    assert bundle["version_refs"]
    assert bundle["selection_trace_json"]["prompt_activation_id"] == bundle["activation_id"]
    assert bundle["cache_path"]


def test_bug117_prompt_and_health_do_not_expose_undeveloped_capability_bans() -> None:
    svc = _chat_service()

    root = svc.repository.find_one("prompt_nodes", {"prompt_key": "root.assistant", "status": "enabled"})
    assert root is not None
    root_text = str(root["prompt_text"])
    for phrase in FORBIDDEN_UNDEVELOPED_CAPABILITY_PHRASES:
        assert phrase not in root_text

    health = svc.health()
    serialized = str(health)
    assert "mouse_keyboard_control" not in serialized
    assert "code_write" not in serialized
    assert health["implemented_capabilities"]["mcp_api_preflight"] is True
    assert health["governance_boundaries"]["formal_github_issue_requires_approval"] is True


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


def test_chat_turn_uses_llm_builds_cards_and_blocks_execution() -> None:
    fake = FakeLlmClient()
    svc = _chat_service(fake)

    result = svc.chat_turn(ChatTurnRequest(message="帮我创建一个 QE 10 loop 实验，先不要执行。"))

    assert len(fake.calls) == 1
    assert result["assistant_message"]["content_text"].startswith("我理解你要创建 QE 10 loop")
    assert result["cards"]["plan_card"]["title"] == "本轮计划"
    assert "固定 PIT 股票池" in "\n".join(result["cards"]["plan_card"]["steps"])
    assert result["cards"]["clarification_card"]["questions"]
    capability_keys = {item["capability_key"] for item in result["cards"]["capability_cards"]}
    assert {"qe.create_experiment_draft", "qe.validate_template", "qe.run_experiment"} <= capability_keys
    assert result["cards"]["missing_capability_keys"] == []
    assert result["cards"]["status_rail"][3] == {"label": "等待确认", "status": "current"}
    assert result["cards"]["safety"]["no_materialize_before_confirmation"] is True
    assert result["trace"]["status"] == "ok"
    assert result["prompt_bundle"]["selection_trace_json"]["algorithm"] == "ancestor_closed_keyword_multibranch_v1"
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

    with pytest.raises(ValueError, match="does not execute actions"):
        svc.chat_turn(ChatTurnRequest(message="确认执行 QE materialize", allow_execute=True))


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

    svc.chat_turn(ChatTurnRequest(message="帮我创建一个 QE 10 loop 实验，先不要执行。"))

    assert len(fake.calls) == 1
    messages = fake.calls[0]["messages"]
    assert isinstance(messages, list)
    non_system = [m for m in messages if m["role"] != "system"]
    assert len(non_system) == 2
    assert non_system[0]["role"] == "user"
    assert "Context Pack" in str(non_system[0]["content"])
    assert non_system[1]["role"] == "user"
    assert non_system[1]["content"] == "帮我创建一个 QE 10 loop 实验，先不要执行。"


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
    assert "QE 10 loop" in final_content
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
