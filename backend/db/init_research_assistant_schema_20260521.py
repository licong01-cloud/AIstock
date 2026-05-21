
"""Explicit schema bootstrap for the AIstock Research Assistant Console.

This module exposes DDL for operator-controlled migrations. Runtime services do
not auto-create these tables and do not silently degrade to in-memory state when
schema objects are missing.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

try:
    from .pg_pool import get_conn
except ImportError:  # pragma: no cover
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from backend.db.pg_pool import get_conn

RESEARCH_ASSISTANT_SCHEMA_VERSION = "research_assistant_console_v1_20260521"

BASE_DDL: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS research_agent_tasks (
        task_id TEXT PRIMARY KEY,
        stream_id TEXT,
        task_type TEXT NOT NULL,
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        risk_level TEXT NOT NULL DEFAULT 'medium',
        idempotency_key TEXT,
        plan_digest TEXT,
        input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        triage_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        assigned_model_profile_id TEXT,
        created_by TEXT NOT NULL DEFAULT 'user',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ,
        CONSTRAINT ck_rat_status CHECK (status IN ('draft','planned','approval_required','approved','running','blocked','failed','triage_required','completed','cancelled')),
        CONSTRAINT ck_rat_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT uq_rat_idempotency UNIQUE (idempotency_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rat_status_updated ON research_agent_tasks(status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rat_stream_updated ON research_agent_tasks(stream_id, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS agent_task_events (
        event_id TEXT PRIMARY KEY,
        task_id TEXT REFERENCES research_agent_tasks(task_id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'info',
        message TEXT NOT NULL,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_ate_severity CHECK (severity IN ('debug','info','warning','error','critical')),
        CONSTRAINT ck_ate_type CHECK (event_type IN ('planned','context_pack_built','mcp_preflight_started','mcp_preflight_passed','mcp_preflight_failed','mcp_started','mcp_done','mcp_failed','skill_started','skill_done','skill_failed','approval_required','approved','rejected','memory_written','report_ready','triage_required'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ate_task_created ON agent_task_events(task_id, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS research_memory_items (
        memory_id TEXT PRIMARY KEY,
        memory_type TEXT NOT NULL,
        namespace TEXT NOT NULL,
        subject_key TEXT NOT NULL,
        title TEXT NOT NULL,
        content_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        content_text TEXT NOT NULL DEFAULT '',
        source_type TEXT NOT NULL DEFAULT 'manual',
        source_ref TEXT,
        source_timestamp TIMESTAMPTZ,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        approval_status TEXT NOT NULL DEFAULT 'draft',
        risk_level TEXT NOT NULL DEFAULT 'medium',
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        valid_from TIMESTAMPTZ,
        valid_to TIMESTAMPTZ,
        supersedes_id TEXT,
        contradicts_id TEXT,
        checksum TEXT NOT NULL,
        created_by TEXT NOT NULL DEFAULT 'user',
        approved_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rmi_type CHECK (memory_type IN ('core','procedural','architecture','roadmap','task_state','experiment','episodic','external','agenda')),
        CONSTRAINT ck_rmi_approval CHECK (approval_status IN ('draft','approved','rejected','expired','superseded')),
        CONSTRAINT ck_rmi_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_rmi_confidence CHECK (confidence >= 0 AND confidence <= 1)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rmi_scope_type ON research_memory_items(namespace, memory_type, approval_status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rmi_subject ON research_memory_items(subject_key, valid_to)",
    """
    CREATE TABLE IF NOT EXISTS research_memory_access_log (
        access_id TEXT PRIMARY KEY,
        memory_id TEXT REFERENCES research_memory_items(memory_id) ON DELETE CASCADE,
        task_id TEXT,
        stream_id TEXT,
        agent_id TEXT,
        retrieval_reason JSONB NOT NULL DEFAULT '{}'::jsonb,
        used_in_prompt BOOLEAN NOT NULL DEFAULT FALSE,
        used_in_report BOOLEAN NOT NULL DEFAULT FALSE,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_context_packs (
        context_pack_id TEXT PRIMARY KEY,
        task_id TEXT,
        agent_id TEXT,
        model_profile TEXT,
        token_budget INTEGER NOT NULL,
        core_memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        procedural_memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        architecture_memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        task_state_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        experiment_memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        graph_relation_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        external_source_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        temp_memory_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        omitted_relevant_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        pack_summary TEXT NOT NULL DEFAULT '',
        pack_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        checksum TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_memory_entities (
        entity_id TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_key TEXT NOT NULL,
        title TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '',
        namespace TEXT NOT NULL DEFAULT 'aistock',
        source_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        approval_status TEXT NOT NULL DEFAULT 'draft',
        valid_from TIMESTAMPTZ,
        valid_to TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_rme_entity UNIQUE (namespace, entity_type, entity_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_memory_relations (
        relation_id TEXT PRIMARY KEY,
        source_entity_id TEXT REFERENCES research_memory_entities(entity_id) ON DELETE CASCADE,
        target_entity_id TEXT REFERENCES research_memory_entities(entity_id) ON DELETE CASCADE,
        relation_type TEXT NOT NULL,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0,
        approval_status TEXT NOT NULL DEFAULT 'draft',
        valid_from TIMESTAMPTZ,
        valid_to TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS research_evolution_paths (
        path_id TEXT PRIMARY KEY,
        stream_id TEXT NOT NULL,
        objective TEXT NOT NULL,
        current_best_entity_id TEXT,
        rejected_entities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        next_candidate_entities_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        supporting_paper_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        decision_notes TEXT NOT NULL DEFAULT '',
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_skill_registry (
        skill_id TEXT PRIMARY KEY,
        skill_key TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        version TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        domain TEXT NOT NULL,
        risk_level TEXT NOT NULL DEFAULT 'medium',
        permission_scope TEXT NOT NULL DEFAULT 'read_analysis',
        skill_type TEXT NOT NULL,
        entrypoint_type TEXT NOT NULL,
        entrypoint_ref TEXT NOT NULL,
        input_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        required_mcp_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
        allowed_side_effect_level TEXT NOT NULL DEFAULT 'none',
        required_approval_level TEXT NOT NULL DEFAULT 'L1',
        owner TEXT NOT NULL DEFAULT 'codex',
        source_ref TEXT,
        checksum TEXT NOT NULL,
        tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        status TEXT NOT NULL DEFAULT 'draft',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_asr_side_effect CHECK (allowed_side_effect_level IN ('none','read_only','draft_only','controlled_write')),
        CONSTRAINT ck_asr_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_asr_status CHECK (status IN ('draft','approved','deprecated','blocked'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_skill_usage_events (
        skill_event_id TEXT PRIMARY KEY,
        skill_id TEXT,
        skill_key TEXT NOT NULL,
        task_id TEXT,
        status TEXT NOT NULL,
        input_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        error_message TEXT,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_mcp_servers (
        server_id TEXT PRIMARY KEY,
        server_key TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        base_url TEXT,
        transport TEXT NOT NULL DEFAULT 'loopback_http',
        status TEXT NOT NULL DEFAULT 'unknown',
        health_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_checked_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_mcp_tools (
        tool_id TEXT PRIMARY KEY,
        server_key TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        risk_level TEXT NOT NULL DEFAULT 'medium',
        side_effect_level TEXT NOT NULL DEFAULT 'read_only',
        requires_approval BOOLEAN NOT NULL DEFAULT FALSE,
        input_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        preflight_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        required_confirmations JSONB NOT NULL DEFAULT '[]'::jsonb,
        status TEXT NOT NULL DEFAULT 'approved',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_amt_tool UNIQUE (server_key, tool_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_mcp_tool_events (
        tool_event_id TEXT PRIMARY KEY,
        task_id TEXT,
        server_key TEXT NOT NULL,
        tool_name TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL,
        idempotency_key TEXT,
        request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        error_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_approval_requests (
        approval_id TEXT PRIMARY KEY,
        task_id TEXT,
        approval_type TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        plan_digest TEXT NOT NULL,
        config_version_id TEXT,
        summary TEXT NOT NULL,
        required_confirmation_text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        approval_context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        approved_by TEXT,
        approval_source TEXT,
        approval_text TEXT,
        execution_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        decided_at TIMESTAMPTZ,
        approved_at TIMESTAMPTZ,
        created_by TEXT NOT NULL DEFAULT 'assistant',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_aar_status CHECK (status IN ('pending','approved','rejected','expired'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_issue_candidates (
        candidate_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        severity TEXT NOT NULL,
        module TEXT NOT NULL,
        problem_statement TEXT NOT NULL,
        reproduce_command TEXT,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        dedupe_key TEXT,
        status TEXT NOT NULL DEFAULT 'needs_review',
        github_issue_number INTEGER,
        github_issue_url TEXT,
        github_sync_status TEXT NOT NULL DEFAULT 'not_requested',
        github_sync_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        proposed_by TEXT NOT NULL DEFAULT 'assistant',
        reviewed_by TEXT,
        reviewed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_aic_status CHECK (status IN ('draft','needs_review','approved_for_github','rejected','synced_to_github','duplicate')),
        CONSTRAINT uq_aic_dedupe UNIQUE (dedupe_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_external_agent_sessions (
        session_id TEXT PRIMARY KEY,
        agent_type TEXT NOT NULL,
        agent_name TEXT NOT NULL,
        model_profile_id TEXT,
        auth_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
        bound_task_id TEXT,
        bound_stream_id TEXT,
        can_act_as_primary BOOLEAN NOT NULL DEFAULT FALSE,
        status TEXT NOT NULL DEFAULT 'active',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_external_agent_events (
        external_event_id TEXT PRIMARY KEY,
        session_id TEXT REFERENCES assistant_external_agent_sessions(session_id) ON DELETE CASCADE,
        event_type TEXT NOT NULL,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        risk_level TEXT NOT NULL DEFAULT 'medium',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_model_profiles (
        model_profile_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        model_name TEXT NOT NULL,
        role TEXT NOT NULL,
        display_name TEXT NOT NULL,
        capabilities_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        cost_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        limits_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'approved',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_model_routing_policies (
        policy_id TEXT PRIMARY KEY,
        role TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        primary_profile_id TEXT NOT NULL,
        fallback_profile_id TEXT,
        selector_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        fallback_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'approved',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_temp_memories (
        temp_memory_id TEXT PRIMARY KEY,
        task_id TEXT,
        stream_id TEXT,
        model_profile_id TEXT,
        created_by_model_profile_id TEXT,
        memory_type TEXT NOT NULL,
        content_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        content_text TEXT NOT NULL DEFAULT '',
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5,
        expires_at TIMESTAMPTZ NOT NULL,
        promoted_memory_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_notifications (
        notification_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'default',
        source_type TEXT NOT NULL,
        source_id TEXT,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        severity TEXT NOT NULL DEFAULT 'info',
        status TEXT NOT NULL DEFAULT 'unread',
        action_route TEXT,
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        read_at TIMESTAMPTZ,
        CONSTRAINT ck_an_status CHECK (status IN ('unread','read','dismissed','resolved'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_reports (
        report_id TEXT PRIMARY KEY,
        report_type TEXT NOT NULL,
        title TEXT NOT NULL,
        body_md TEXT NOT NULL,
        summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        status TEXT NOT NULL DEFAULT 'draft',
        created_by TEXT NOT NULL DEFAULT 'assistant',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_agenda_items (
        agenda_item_id TEXT PRIMARY KEY,
        namespace TEXT NOT NULL DEFAULT 'personal',
        title TEXT NOT NULL,
        due_at TIMESTAMPTZ,
        status TEXT NOT NULL DEFAULT 'open',
        priority TEXT NOT NULL DEFAULT 'normal',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_by TEXT NOT NULL DEFAULT 'user',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_validation_discovery_reports (
        discovery_report_id TEXT PRIMARY KEY,
        run_date DATE NOT NULL,
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'draft',
        summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        candidate_issue_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        validation_run_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_trace_events (
        trace_id TEXT PRIMARY KEY,
        task_id TEXT,
        event_type TEXT NOT NULL,
        component TEXT NOT NULL,
        status TEXT NOT NULL,
        duration_ms INTEGER,
        model_profile_id TEXT,
        payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        cost_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_aic_status_updated ON assistant_issue_candidates(status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_an_user_status_created ON assistant_notifications(user_id, status, created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_amt_server_tool ON assistant_mcp_tools(server_key, tool_name)",
]

TABLE_COMMENTS = {
    "research_agent_tasks": "Research Assistant task ledger with explicit status, risk, idempotency and trace boundaries.",
    "agent_task_events": "Append-only task events for replayable assistant progress and failures.",
    "research_memory_items": "Native long-term Memory Ledger; source of truth, not RAG chunks.",
    "assistant_context_packs": "Deterministic memory/context bundles used by models and external agents.",
    "research_memory_entities": "Native lightweight knowledge graph entities.",
    "research_memory_relations": "Native lightweight knowledge graph relations with evidence references.",
    "assistant_skill_registry": "Local-only skill catalog with checksum, permissions and approval metadata.",
    "assistant_mcp_tools": "MCP/API execution catalog including risk and preflight metadata.",
    "assistant_approval_requests": "Approval gate records for L2+ assistant operations.",
    "assistant_issue_candidates": "Candidate issue queue; formal GitHub issue creation requires explicit approval and sync.",
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_comment_ddl() -> list[str]:
    return [f"COMMENT ON TABLE {table} IS '{_sql_literal(comment)}'" for table, comment in TABLE_COMMENTS.items()]


DDL = BASE_DDL + _build_comment_ddl()


def iter_ddl() -> Iterable[str]:
    return tuple(DDL)


def init_research_assistant_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)
        conn.commit()


if __name__ == "__main__":
    init_research_assistant_schema()
    print(f"Research Assistant schema initialized: {RESEARCH_ASSISTANT_SCHEMA_VERSION}")
