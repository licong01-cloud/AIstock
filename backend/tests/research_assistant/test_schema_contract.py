from __future__ import annotations

import re

from backend.db.init_research_assistant_schema_20260521 import DDL, RESEARCH_ASSISTANT_EVENT_TYPES, RESEARCH_ASSISTANT_SCHEMA_VERSION
from backend.services.research_assistant.models import EVENT_TYPES, PROMPT_NODE_CATEGORIES
from backend.services.research_assistant.repository import TABLES
from backend.services.research_assistant.service import (
    DEFAULT_MCP_SERVERS,
    DEFAULT_MCP_TOOLS,
    DEFAULT_WORKFLOW_CAPABILITIES,
    DEFAULT_MODEL_PROFILES,
    DEFAULT_PROMPT_NODES,
    DEFAULT_ROUTING_POLICIES,
    DEFAULT_SKILLS,
)


def _table_columns(sql: str, table: str) -> set[str]:
    match = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\s*\)", sql, re.S)
    assert match, f"missing DDL for {table}"
    columns: set[str] = set()
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("CONSTRAINT "):
            continue
        columns.add(line.split()[0])
    return columns


def _assert_columns(table_columns: dict[str, set[str]], kind: str, payload: dict[str, object]) -> None:
    table = TABLES[kind]["table"]
    unknown = set(payload) - table_columns[table]
    assert not unknown, f"{kind} writes columns missing from {table}: {sorted(unknown)}"


def test_research_assistant_schema_contains_phase1_tables_and_gates() -> None:
    sql = "\n".join(DDL)

    assert RESEARCH_ASSISTANT_SCHEMA_VERSION == "research_assistant_memory_tree_v1_20260601"
    for table in {
        "research_agent_tasks",
        "agent_task_events",
        "assistant_conversations",
        "assistant_conversation_messages",
        "research_memory_items",
        "research_memory_access_log",
        "assistant_context_packs",
        "research_memory_entities",
        "research_memory_relations",
        "research_evolution_paths",
        "assistant_skill_registry",
        "assistant_skill_usage_events",
        "assistant_mcp_servers",
        "assistant_capabilities",
        "assistant_action_proposals",
        "assistant_mcp_tools",
        "assistant_mcp_tool_events",
        "assistant_approval_requests",
        "assistant_issue_candidates",
        "assistant_external_agent_sessions",
        "assistant_external_agent_events",
        "assistant_model_profiles",
        "assistant_model_routing_policies",
        "assistant_prompt_nodes",
        "assistant_prompt_sources",
        "assistant_prompt_node_versions",
        "assistant_prompt_activations",
        "assistant_prompt_activation_events",
        "assistant_prompt_bundles",
        "assistant_runtime_config_sources",
        "assistant_runtime_config_activations",
        "assistant_context_segments",
        "assistant_context_key_facts",
        "assistant_context_assembly_traces",
        "assistant_temp_memories",
        "assistant_notifications",
        "assistant_reports",
        "assistant_agenda_items",
        "assistant_validation_discovery_reports",
        "assistant_trace_events",
    }:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql

    assert "Memory Ledger" in sql
    assert "formal GitHub issue creation requires explicit approval and sync" in sql
    assert "CONSTRAINT ck_rat_status" in sql
    assert "CONSTRAINT ck_rmi_approval" in sql
    assert "CONSTRAINT ck_aar_status" in sql
    assert "CONSTRAINT ck_aic_status" in sql
    assert "requires_approval BOOLEAN NOT NULL DEFAULT FALSE" in sql
    assert "COMMENT ON TABLE assistant_capabilities" in sql
    assert "COMMENT ON TABLE assistant_action_proposals" in sql
    assert "action_proposal_id TEXT PRIMARY KEY" in sql
    assert "assistant_mcp_tool_events.result_card_json" in sql
    assert "assistant_mcp_tool_events.artifact_refs" in sql
    assert "mcp_execution_timeout" in sql
    assert "chat_received" in sql
    assert "prompt_bundle_built" in sql
    assert "llm_done" in sql
    assert set(RESEARCH_ASSISTANT_EVENT_TYPES) == EVENT_TYPES
    assert "DROP CONSTRAINT IF EXISTS ck_ate_type" in sql
    assert "ADD CONSTRAINT ck_ate_type CHECK" in sql
    category_constraints = re.findall(r"ck_apn_category CHECK \(category IN \(([^)]*)\)\)", sql)
    assert category_constraints
    for constraint in category_constraints:
        values = {item.strip().strip("'") for item in constraint.split(",")}
        assert values == PROMPT_NODE_CATEGORIES


def test_research_assistant_service_payloads_match_schema_columns() -> None:
    sql = "\n".join(DDL)
    table_columns = {meta["table"]: _table_columns(sql, meta["table"]) for meta in TABLES.values()}

    _assert_columns(
        table_columns,
        "task_events",
        {"event_id": "ratev_x", "task_id": "rat_x", "event_type": "planned", "severity": "info", "message": "created", "payload_json": {}, "evidence_refs": []},
    )
    _assert_columns(
        table_columns,
        "conversations",
        {"conversation_id": "conv_x", "user_id": "default", "title": "Chat", "status": "active", "metadata_json": {}},
    )
    _assert_columns(
        table_columns,
        "conversation_messages",
        {"message_id": "msg_x", "conversation_id": "conv_x", "role": "assistant", "content_text": "text", "content_json": {}, "task_id": "rat_x", "model_profile_id": "model", "prompt_bundle_id": "pbundle_x", "trace_id": "trace_x", "is_visible": True},
    )
    for item in DEFAULT_SKILLS:
        _assert_columns(
            table_columns,
            "skills",
            {
                "skill_id": f"skill_{item['skill_key']}",
                "version": "1.0.0",
                "skill_type": "local_codex_skill",
                "entrypoint_type": "local_skill",
                "entrypoint_ref": item["skill_key"],
                "allowed_side_effect_level": "none",
                "required_approval_level": "L1",
                "owner": "codex",
                "source_ref": f"C:/Users/lc999/.codex/skills/{item['skill_key']}/SKILL.md",
                "status": "approved",
                "checksum": "checksum",
                "required_mcp_tools": [],
                "skill_key": item["skill_key"],
                "title": item["title"],
                "description": item["description"],
                "domain": item["domain"],
                "risk_level": item["risk_level"],
                "permission_scope": item["permission_scope"],
                "tags_json": item["tags_json"],
                "input_schema_json": item["input_schema_json"],
                "output_schema_json": item["output_schema_json"],
            },
        )
    for item in DEFAULT_MCP_SERVERS:
        _assert_columns(table_columns, "mcp_servers", {"server_id": f"mcp_server_{item['server_key']}", **item})
    for item in DEFAULT_MCP_TOOLS:
        tool_id = f"mcp_tool_{item['server_key']}_{item['tool_name']}".replace("-", "_")
        _assert_columns(table_columns, "mcp_tools", {"tool_id": tool_id, "status": "enabled", **item})
    _assert_columns(
        table_columns,
        "mcp_tool_events",
        {
            "tool_event_id": "mcptev_x",
            "task_id": "rat_x",
            "server_key": "aistock-qe-experiment",
            "tool_name": "qe_template_create",
            "event_type": "execute",
            "status": "succeeded",
            "idempotency_key": "idem",
            "request_json": {},
            "response_json": {},
            "error_json": {},
            "action_proposal_id": "actprop_x",
            "approval_id": "appr_x",
            "plan_digest": "digest",
            "transport": "loopback_http",
            "timeout_ms": 60000,
            "attempt_index": 0,
            "duration_ms": 10,
            "result_card_json": {},
            "artifact_refs": [],
            "started_at": "2099-12-31T00:00:00+00:00",
            "completed_at": "2099-12-31T00:00:00+00:00",
        },
    )
    for item in DEFAULT_WORKFLOW_CAPABILITIES:
        _assert_columns(
            table_columns,
            "capabilities",
            {
                "capability_id": f"cap_{item['capability_key'].replace('.', '_').replace('-', '_')}",
                "checksum": "checksum",
                "last_synced_at": "2099-12-31T00:00:00+00:00",
                **item,
            },
        )
    _assert_columns(
        table_columns,
        "action_proposals",
        {
            "action_proposal_id": "actprop_x",
            "task_id": "rat_x",
            "conversation_id": "conv_x",
            "capability_key": "qe.create_experiment_draft",
            "proposal_type": "workflow_pack",
            "title": "QE draft",
            "summary": "draft",
            "risk_level": "medium",
            "side_effect_level": "draft_only",
            "input_json": {},
            "expected_result_json": {},
            "plan_digest": "digest",
            "prompt_bundle_signature": "sig",
            "runtime_config_activation_id": "runtime",
            "context_pack_id": "ctx_x",
            "status": "proposed",
            "approval_id": None,
            "idempotency_key": "idem",
            "expires_at": "2099-12-31T00:00:00+00:00",
            "created_by": "assistant",
        },
    )
    for item in DEFAULT_MODEL_PROFILES:
        _assert_columns(table_columns, "model_profiles", {**item, "display_name": "display"})
    for item in DEFAULT_ROUTING_POLICIES:
        payload = dict(item)
        payload["primary_profile_id"] = payload.pop("model_profile_id")
        payload["fallback_profile_id"] = "fallback"
        _assert_columns(table_columns, "routing_policies", payload)
    for item in DEFAULT_PROMPT_NODES:
        _assert_columns(
            table_columns,
            "prompt_nodes",
            {**item, "prompt_node_id": f"prompt_{item['prompt_key'].replace('.', '_')}", "checksum": "checksum"},
        )
    _assert_columns(
        table_columns,
        "prompt_bundles",
        {
            "prompt_bundle_id": "pbundle_x",
            "task_id": "rat_x",
            "conversation_id": "conv_x",
            "phase": "planning",
            "model_profile_id": "model",
            "activation_id": "prompt_activation_x",
            "version_refs": [],
            "node_refs": [],
            "selection_trace_json": {},
            "bundle_json": {},
            "bundle_text": "prompt",
            "checksum": "checksum",
            "cache_path": "var/research_assistant/prompt_cache/checksum.json",
        },
    )
    _assert_columns(
        table_columns,
        "memory_items",
        {"memory_id": "mem_x", "memory_type": "core", "namespace": "aistock", "subject_key": "assistant", "title": "Memory", "content_json": {}, "content_text": "fact", "source_type": "manual", "source_ref": "doc", "confidence": 1.0, "valid_from": None, "valid_to": "2099-12-31T00:00:00+00:00", "supersedes_id": None, "contradicts_id": None, "approval_status": "draft", "risk_level": "medium", "evidence_refs": [], "created_by": "user", "checksum": "checksum"},
    )
    _assert_columns(
        table_columns,
        "memory_access_log",
        {"access_id": "memacc_x", "memory_id": "mem_x", "task_id": "rat_x", "agent_id": "agent", "retrieval_reason": {}, "used_in_prompt": True, "payload_json": {}},
    )
    _assert_columns(
        table_columns,
        "approvals",
        {"approval_id": "appr_x", "status": "pending", "task_id": "rat_x", "approval_type": "issue.candidate", "risk_level": "high", "plan_digest": "digest-abcdef", "summary": "Issue", "required_confirmation_text": "APPROVE", "created_by": "assistant", "approval_context_json": {}},
    )
    _assert_columns(
        table_columns,
        "issue_candidates",
        {"candidate_id": "issuecand_x", "status": "needs_review", "dedupe_key": "dedupe", "github_sync_status": "not_requested", "github_sync_json": {}, "title": "Bug", "severity": "P1", "module": "research_assistant", "problem_statement": "problem", "reproduce_command": None, "evidence_refs": [], "proposed_by": "assistant"},
    )
    _assert_columns(
        table_columns,
        "temp_memories",
        {"temp_memory_id": "tmpmem_x", "task_id": "rat_x", "stream_id": None, "memory_type": "task_state", "content_json": {}, "content_text": "progress", "evidence_refs": [], "confidence": 0.5, "expires_at": "2099-12-31T00:00:00+00:00", "model_profile_id": "model", "created_by_model_profile_id": "model"},
    )
