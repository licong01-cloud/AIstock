
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

RESEARCH_ASSISTANT_SCHEMA_VERSION = "research_assistant_phase12_skill_library_v1_20260616"

RESEARCH_ASSISTANT_EVENT_TYPES: tuple[str, ...] = (
    "planned",
    "chat_received",
    "prompt_bundle_built",
    "context_pack_built",
    "context_compacted",
    "llm_started",
    "llm_done",
    "llm_failed",
    "action_proposed",
    "mcp_preflight_started",
    "mcp_preflight_passed",
    "mcp_preflight_failed",
    "mcp_execution_timeout",
    "mcp_started",
    "mcp_retry",
    "mcp_done",
    "mcp_failed",
    "skill_started",
    "skill_done",
    "skill_failed",
    "approval_required",
    "approved",
    "rejected",
    "memory_written",
    "report_ready",
    "triage_required",
)


def _check_in_values(column_name: str, values: tuple[str, ...]) -> str:
    return f"{column_name} IN ({','.join(repr(value) for value in values)})"


AGENT_TASK_EVENT_TYPE_CHECK = _check_in_values("event_type", RESEARCH_ASSISTANT_EVENT_TYPES)

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
        CONSTRAINT ck_ate_type CHECK ({event_type_check})
    )
    """.replace("{event_type_check}", AGENT_TASK_EVENT_TYPE_CHECK),
    "CREATE INDEX IF NOT EXISTS idx_ate_task_created ON agent_task_events(task_id, created_at DESC)",
    """
    ALTER TABLE agent_task_events
        DROP CONSTRAINT IF EXISTS ck_ate_type
    """,
    f"""
    ALTER TABLE agent_task_events
        ADD CONSTRAINT ck_ate_type CHECK ({AGENT_TASK_EVENT_TYPE_CHECK})
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_conversations (
        conversation_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'default',
        title TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_ac_status CHECK (status IN ('active','archived','closed'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ac_user_updated ON assistant_conversations(user_id, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS assistant_conversation_messages (
        message_id TEXT PRIMARY KEY,
        conversation_id TEXT REFERENCES assistant_conversations(conversation_id) ON DELETE CASCADE,
        role TEXT NOT NULL,
        content_text TEXT NOT NULL,
        content_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        task_id TEXT,
        model_profile_id TEXT,
        prompt_bundle_id TEXT,
        trace_id TEXT,
        is_visible BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_acm_role CHECK (role IN ('user','assistant','system','tool'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acm_conversation_created ON assistant_conversation_messages(conversation_id, created_at ASC)",
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
        tree_path TEXT,
        parent_key TEXT,
        node_type TEXT NOT NULL DEFAULT 'fact',
        scope TEXT NOT NULL DEFAULT 'project',
        importance REAL NOT NULL DEFAULT 0.5,
        last_used_at TIMESTAMPTZ,
        use_count INTEGER NOT NULL DEFAULT 0,
        auto_created BOOLEAN NOT NULL DEFAULT FALSE,
        trust_level TEXT NOT NULL DEFAULT 'user_stated',
        provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        resident BOOLEAN NOT NULL DEFAULT FALSE,
        checksum TEXT NOT NULL,
        created_by TEXT NOT NULL DEFAULT 'user',
        approved_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_rmi_type CHECK (memory_type IN ('core','procedural','architecture','roadmap','task_state','experiment','episodic','external','agenda','user_preference','directive','habit','analysis_note')),
        CONSTRAINT ck_rmi_approval CHECK (approval_status IN ('draft','approved','rejected','expired','superseded')),
        CONSTRAINT ck_rmi_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_rmi_confidence CHECK (confidence >= 0 AND confidence <= 1),
        CONSTRAINT ck_rmi_node_type CHECK (node_type IN ('branch','fact')),
        CONSTRAINT ck_rmi_scope CHECK (scope IN ('project','personal')),
        CONSTRAINT ck_rmi_importance CHECK (importance >= 0 AND importance <= 1),
        CONSTRAINT ck_rmi_trust_level CHECK (trust_level IN ('user_stated','assistant_inferred')),
        CONSTRAINT ck_rmi_use_count CHECK (use_count >= 0)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rmi_scope_type ON research_memory_items(namespace, memory_type, approval_status, updated_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rmi_subject ON research_memory_items(subject_key, valid_to)",
    "CREATE INDEX IF NOT EXISTS idx_rmi_tree ON research_memory_items(scope, tree_path, approval_status, importance DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rmi_parent ON research_memory_items(parent_key)",
    "CREATE INDEX IF NOT EXISTS idx_rmi_resident ON research_memory_items(scope, resident) WHERE resident = TRUE",
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
    CREATE TABLE IF NOT EXISTS assistant_code_context_refs (
        code_ref_id TEXT PRIMARY KEY,
        task_id TEXT,
        query_scope TEXT NOT NULL,
        manifest_json JSONB NOT NULL,
        source TEXT NOT NULL,
        provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        as_of TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_proactive_reports (
        report_id TEXT PRIMARY KEY,
        report_type TEXT NOT NULL,
        report_date DATE NOT NULL,
        summary_md TEXT NOT NULL,
        sections_json JSONB NOT NULL,
        source_refs_json JSONB NOT NULL DEFAULT '[]'::jsonb,
        status TEXT NOT NULL DEFAULT 'generated',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_apr UNIQUE (report_type, report_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_reflection_cards (
        card_id TEXT PRIMARY KEY,
        task_id TEXT,
        trigger TEXT NOT NULL,
        lesson_md TEXT NOT NULL,
        structured_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        memory_ref TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_lab_runs (
        lab_run_id TEXT PRIMARY KEY,
        target_prompt_key TEXT NOT NULL,
        optimizer TEXT NOT NULL,
        eval_set_ref TEXT NOT NULL,
        candidate_text TEXT NOT NULL,
        judge_score_json JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'candidate',
        approval_request_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_skill_library (
        skill_id TEXT PRIMARY KEY,
        skill_key TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL,
        recipe_json JSONB NOT NULL,
        success_count INTEGER NOT NULL DEFAULT 0,
        provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'draft',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
    CREATE TABLE IF NOT EXISTS assistant_capabilities (
        capability_id TEXT PRIMARY KEY,
        capability_key TEXT NOT NULL UNIQUE,
        capability_type TEXT NOT NULL,
        title TEXT NOT NULL,
        natural_language_triggers JSONB NOT NULL DEFAULT '[]'::jsonb,
        description_for_llm TEXT NOT NULL DEFAULT '',
        risk_level TEXT NOT NULL DEFAULT 'medium',
        side_effect_level TEXT NOT NULL DEFAULT 'read_only',
        required_confirmations JSONB NOT NULL DEFAULT '[]'::jsonb,
        preferred_model_role TEXT,
        input_slots JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_cards JSONB NOT NULL DEFAULT '[]'::jsonb,
        mcp_tool_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        skill_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        workflow_pack_ref TEXT,
        status TEXT NOT NULL DEFAULT 'approved',
        source_ref TEXT,
        checksum TEXT NOT NULL,
        last_synced_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_acap_type CHECK (capability_type IN ('mcp_tool','skill','workflow_pack','composite')),
        CONSTRAINT ck_acap_status CHECK (status IN ('draft','approved','disabled','deprecated','blocked')),
        CONSTRAINT ck_acap_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_acap_side_effect CHECK (side_effect_level IN ('read_only','draft_only','write_nonprod','high_cost_compute','production_sensitive'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acap_status_risk ON assistant_capabilities(status, risk_level, updated_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS assistant_action_proposals (
        action_proposal_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        conversation_id TEXT,
        capability_key TEXT NOT NULL,
        proposal_type TEXT NOT NULL,
        title TEXT NOT NULL,
        summary TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        side_effect_level TEXT NOT NULL,
        input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        expected_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        plan_digest TEXT NOT NULL,
        prompt_bundle_signature TEXT,
        runtime_config_activation_id TEXT,
        context_pack_id TEXT,
        status TEXT NOT NULL DEFAULT 'proposed',
        approval_id TEXT,
        idempotency_key TEXT NOT NULL,
        expires_at TIMESTAMPTZ,
        created_by TEXT NOT NULL DEFAULT 'assistant',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_aap_type CHECK (proposal_type IN ('workflow_step','mcp_tool','skill','workflow_pack')),
        CONSTRAINT ck_aap_status CHECK (status IN ('proposed','confirmed','preflight_passed','approval_required','approved','executing','succeeded','rejected','expired','preflight_failed','failed','cancelled')),
        CONSTRAINT ck_aap_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_aap_side_effect CHECK (side_effect_level IN ('read_only','draft_only','write_nonprod','high_cost_compute','production_sensitive')),
        CONSTRAINT uq_aap_idempotency UNIQUE (idempotency_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_aap_task_status ON assistant_action_proposals(task_id, status, updated_at DESC)",
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
        action_proposal_id TEXT,
        approval_id TEXT,
        plan_digest TEXT,
        transport TEXT,
        timeout_ms INTEGER,
        attempt_index INTEGER,
        duration_ms INTEGER,
        result_card_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        artifact_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS action_proposal_id TEXT",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS approval_id TEXT",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS plan_digest TEXT",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS transport TEXT",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS timeout_ms INTEGER",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS attempt_index INTEGER",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS duration_ms INTEGER",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS result_card_json JSONB NOT NULL DEFAULT '{}'::jsonb",
    "ALTER TABLE assistant_mcp_tool_events ADD COLUMN IF NOT EXISTS artifact_refs JSONB NOT NULL DEFAULT '[]'::jsonb",
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
    CREATE TABLE IF NOT EXISTS assistant_agent_runs (
        agent_run_id TEXT PRIMARY KEY,
        parent_task_id TEXT NOT NULL,
        agent_key TEXT NOT NULL,
        role TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        model_profile_id TEXT,
        trace_id TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_assistant_agent_runs_status CHECK (status IN ('queued','running','succeeded','failed','cancelled'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_aar_parent ON assistant_agent_runs(parent_task_id, status)",
    """
    CREATE TABLE IF NOT EXISTS qe_autonomous_evolution_runs (
        auto_run_id TEXT PRIMARY KEY,
        qe_task_id TEXT NOT NULL,
        methodology_ref TEXT,
        stop_conditions_json JSONB NOT NULL,
        budget_json JSONB NOT NULL,
        status TEXT NOT NULL,
        loops_completed INTEGER NOT NULL DEFAULT 0,
        last_verdict_json JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_qaer_status CHECK (status IN ('running','stopped_target','stopped_no_improve','stopped_budget','failed'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_qaer_task_status ON qe_autonomous_evolution_runs(qe_task_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_qaer_updated_at ON qe_autonomous_evolution_runs(updated_at)",
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
    CREATE TABLE IF NOT EXISTS assistant_prompt_nodes (
        prompt_node_id TEXT PRIMARY KEY,
        prompt_key TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        category TEXT NOT NULL,
        tree_path TEXT NOT NULL,
        parent_key TEXT,
        version TEXT NOT NULL DEFAULT '1.0.0',
        phase TEXT NOT NULL DEFAULT 'planning',
        trigger_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        prompt_text TEXT NOT NULL,
        risk_level TEXT NOT NULL DEFAULT 'medium',
        status TEXT NOT NULL DEFAULT 'enabled',
        source_ref TEXT,
        checksum TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_apn_category CHECK (category IN ('root','governance','intent','domain','workflow','tool_guard','renderer','memory','model_routing','context','mode')),
        CONSTRAINT ck_apn_phase CHECK (phase IN ('planning','preflight','execution','result','reflection')),
        CONSTRAINT ck_apn_risk CHECK (risk_level IN ('low','medium','high','production_sensitive')),
        CONSTRAINT ck_apn_status CHECK (status IN ('draft','enabled','disabled','deprecated'))
    )
    """,
    """
    ALTER TABLE assistant_prompt_nodes
        DROP CONSTRAINT IF EXISTS ck_apn_category
    """,
    """
    ALTER TABLE assistant_prompt_nodes
        ADD CONSTRAINT ck_apn_category CHECK (category IN ('root','governance','intent','domain','workflow','tool_guard','renderer','memory','model_routing','context','mode'))
    """,
    "CREATE INDEX IF NOT EXISTS idx_apn_tree_phase ON assistant_prompt_nodes(tree_path, phase, status)",
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_sources (
        source_id TEXT PRIMARY KEY,
        pack_key TEXT NOT NULL,
        pack_version TEXT NOT NULL,
        source_path TEXT NOT NULL,
        source_commit TEXT,
        source_sha256 TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'approved',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        imported_by TEXT NOT NULL DEFAULT 'system',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_aps_status CHECK (status IN ('draft','approved','rejected','deprecated'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_aps_pack_version ON assistant_prompt_sources(pack_key, pack_version, source_sha256)",
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_node_versions (
        version_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL,
        pack_key TEXT NOT NULL,
        pack_version TEXT NOT NULL,
        prompt_key TEXT NOT NULL,
        prompt_node_id TEXT,
        title TEXT NOT NULL,
        category TEXT NOT NULL,
        tree_path TEXT NOT NULL,
        parent_key TEXT,
        phase TEXT NOT NULL,
        trigger_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        prompt_text TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        source_ref TEXT NOT NULL,
        checksum TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'approved',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_apnv_status CHECK (status IN ('draft','approved','rejected','deprecated'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_apnv_prompt_checksum ON assistant_prompt_node_versions(prompt_key, checksum)",
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_activations (
        activation_id TEXT PRIMARY KEY,
        assistant_key TEXT NOT NULL,
        environment TEXT NOT NULL,
        pack_key TEXT NOT NULL,
        pack_version TEXT NOT NULL,
        source_id TEXT NOT NULL,
        version_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        bundle_signature TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        active_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        active_to TIMESTAMPTZ,
        activated_by TEXT NOT NULL DEFAULT 'system',
        activation_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_apa_status CHECK (status IN ('active','paused','retired'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_apa_active_env ON assistant_prompt_activations(assistant_key, environment, status) WHERE status = 'active'",
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_activation_events (
        event_id TEXT PRIMARY KEY,
        activation_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT 'system',
        event_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS assistant_prompt_bundles (
        prompt_bundle_id TEXT PRIMARY KEY,
        task_id TEXT,
        conversation_id TEXT,
        phase TEXT NOT NULL,
        model_profile_id TEXT,
        activation_id TEXT,
        version_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        node_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
        selection_trace_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        bundle_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        bundle_text TEXT NOT NULL,
        checksum TEXT NOT NULL,
        cache_path TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_apb_phase CHECK (phase IN ('planning','preflight','execution','result','reflection'))
    )
    """,
    "ALTER TABLE assistant_prompt_bundles ADD COLUMN IF NOT EXISTS activation_id TEXT",
    "ALTER TABLE assistant_prompt_bundles ADD COLUMN IF NOT EXISTS version_refs JSONB NOT NULL DEFAULT '[]'::jsonb",
    "CREATE INDEX IF NOT EXISTS idx_apb_task_created ON assistant_prompt_bundles(task_id, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS assistant_runtime_config_sources (
        source_id TEXT PRIMARY KEY,
        config_key TEXT NOT NULL,
        config_version TEXT NOT NULL,
        source_path TEXT NOT NULL,
        source_commit TEXT,
        source_sha256 TEXT NOT NULL,
        config_json JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'approved',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        imported_by TEXT NOT NULL DEFAULT 'system',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_arcs_status CHECK (status IN ('draft','approved','rejected','deprecated'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_arcs_config_version ON assistant_runtime_config_sources(config_key, config_version, source_sha256)",
    """
    CREATE TABLE IF NOT EXISTS assistant_runtime_config_activations (
        activation_id TEXT PRIMARY KEY,
        config_key TEXT NOT NULL,
        config_version TEXT NOT NULL,
        environment TEXT NOT NULL,
        source_id TEXT NOT NULL,
        config_json JSONB NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        active_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        active_to TIMESTAMPTZ,
        activated_by TEXT NOT NULL DEFAULT 'system',
        activation_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_arca_status CHECK (status IN ('active','paused','retired'))
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_arca_active_env ON assistant_runtime_config_activations(config_key, environment, status) WHERE status = 'active'",
    """
    CREATE TABLE IF NOT EXISTS assistant_context_segments (
        segment_id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        segment_type TEXT NOT NULL,
        summary_depth INTEGER NOT NULL DEFAULT 1,
        content_text TEXT NOT NULL,
        content_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
        source_sha256 TEXT NOT NULL,
        prompt_activation_id TEXT,
        runtime_config_activation_id TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_acs_status CHECK (status IN ('active','superseded','rejected')),
        CONSTRAINT ck_acs_type CHECK (segment_type IN ('compact_summary','summary_of_summaries','recovery_note'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acs_conversation_status ON assistant_context_segments(conversation_id, status, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS assistant_context_key_facts (
        fact_id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        segment_id TEXT,
        fact_type TEXT NOT NULL,
        fact_text TEXT NOT NULL,
        fact_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
        confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5,
        status TEXT NOT NULL DEFAULT 'active',
        metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT ck_ackf_status CHECK (status IN ('active','superseded','rejected'))
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ackf_conversation_status ON assistant_context_key_facts(conversation_id, status, created_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS assistant_context_assembly_traces (
        assembly_trace_id TEXT PRIMARY KEY,
        conversation_id TEXT,
        task_id TEXT,
        prompt_activation_id TEXT,
        runtime_config_activation_id TEXT,
        budget_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        assembly_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        source_refs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL DEFAULT 'ok',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acat_conversation_created ON assistant_context_assembly_traces(conversation_id, created_at DESC)",
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
    "assistant_conversations": "Human-facing Research Assistant conversation sessions.",
    "assistant_conversation_messages": "Visible and audit-scoped conversation messages; main UI renders only human-readable text and cards.",
    "research_memory_items": "Native long-term Memory Ledger; source of truth, not RAG chunks.",
    "assistant_context_packs": "Deterministic memory/context bundles used by models and external agents.",
    "assistant_code_context_refs": "Code-intelligence context references injected into Research Assistant Context Packs; AST deterministic, no embedding.",
    "assistant_proactive_reports": "Proactive morning and experiment reports generated from read-only evidence providers.",
    "assistant_reflection_cards": "Reflection Cards generated from task failure, correction, or low-confidence signals; writes personal.episodic memory only.",
    "assistant_prompt_lab_runs": "Prompt Lab offline GEPA/DSPy-style candidate prompt runs with LLM-as-judge scoring and human approval gate.",
    "assistant_skill_library": "Gated reusable workflow, prompt and tool recipes; draft until human approval and reused only through Action Proposal gates.",
    "research_memory_entities": "Native lightweight knowledge graph entities.",
    "research_memory_relations": "Native lightweight knowledge graph relations with evidence references.",
    "assistant_skill_registry": "Local-only skill catalog with checksum, permissions and approval metadata.",
    "assistant_capabilities": "Approved Research Assistant Capability Registry for planner-selectable MCP tools, skills and workflow packs.",
    "assistant_action_proposals": "Immutable pre-execution action proposal snapshots bound to plan digest, confirmation, preflight and approval state.",
    "assistant_mcp_tool_events": "MCP/API execution event ledger including preflight, retry, approval and result-card audit data.",
    "assistant_mcp_tools": "MCP/API execution catalog including risk and preflight metadata.",
    "assistant_approval_requests": "Approval gate records for L2+ assistant operations.",
    "assistant_validation_discovery_reports": "Non-authoritative conversation draft / explanation cache; pending Phase 2 retirement. Discovery facts come from Validation/Nightly candidate sources.",
    "assistant_issue_candidates": "Non-authoritative conversation draft / explanation cache; pending Phase 2 retirement. Formal submission must use AIstock issue workflow / Validation MCP.",
    "assistant_prompt_nodes": "Tree-structured prompt nodes with version, checksum, trigger and phase metadata.",
    "assistant_prompt_sources": "Git-backed prompt pack import records; file content remains the review source and DB stores immutable imported versions.",
    "assistant_prompt_node_versions": "Immutable prompt node versions imported from prompt pack files with checksums and source references.",
    "assistant_prompt_activations": "Environment-scoped active prompt pack selections used by chat runtime snapshots and bundle audit.",
    "assistant_prompt_activation_events": "Append-only prompt activation lifecycle events for review, rollback and audit.",
    "assistant_prompt_bundles": "Deterministic prompt bundles selected for one conversation turn; cache is derivative, not source of truth.",
    "assistant_runtime_config_sources": "Git-backed Research Assistant runtime context configuration imports with checksums.",
    "assistant_runtime_config_activations": "Environment-scoped active runtime context configuration snapshots.",
    "assistant_context_segments": "Derived compact summaries and recovery notes linked to original conversation messages.",
    "assistant_context_key_facts": "Structured key facts extracted from compacted context with source message references.",
    "assistant_context_assembly_traces": "Per-turn context assembly budget, ordering and source-reference audit trail.",
    "qe_autonomous_evolution_runs": "QE autonomous evolution loop ledger with stop conditions, budget guardrails and approval boundary reports.",
}

COLUMN_COMMENTS = {
    "research_memory_items.tree_path": "Dotted memory tree path under project.* or personal.* used for collapsed branch retrieval.",
    "research_memory_items.parent_key": "Parent memory key or branch path; root nodes keep NULL.",
    "research_memory_items.node_type": "Memory node kind: branch for structural nodes, fact for retrievable content.",
    "research_memory_items.scope": "Memory scope boundary: project for shared project memory, personal for user-specific memory.",
    "research_memory_items.importance": "Normalized 0..1 priority used with recency for deterministic memory ordering.",
    "research_memory_items.last_used_at": "Last timestamp when this memory was selected into a context pack.",
    "research_memory_items.use_count": "Number of times this memory has been selected or self-edited.",
    "research_memory_items.auto_created": "True when curator created this branch or fact without manual seed.",
    "research_memory_items.trust_level": "user_stated means the user explicitly said it; assistant_inferred means the assistant inferred it.",
    "research_memory_items.provenance_json": "Required provenance for auto-created memories, including conversation, message, turn, and source.",
    "research_memory_items.resident": "True when directive or preference memory must be injected every turn regardless of branch match.",
    "assistant_prompt_sources.source_id": "Stable prompt pack source import identifier.",
    "assistant_prompt_sources.pack_key": "Logical prompt pack key such as research_assistant.main.",
    "assistant_prompt_sources.pack_version": "Semantic prompt pack version imported from Git files.",
    "assistant_prompt_sources.source_path": "Repository-relative prompt pack manifest path.",
    "assistant_prompt_sources.source_commit": "Git commit or external source revision for the imported prompt pack when available.",
    "assistant_prompt_sources.source_sha256": "SHA256 of the prompt pack file graph used for drift detection.",
    "assistant_prompt_sources.status": "Prompt pack source lifecycle status, normally approved after validation.",
    "assistant_prompt_sources.metadata_json": "Importer metadata such as schema and validation context.",
    "assistant_prompt_sources.imported_at": "Timestamp when the prompt pack source was imported.",
    "assistant_prompt_sources.imported_by": "Actor or process that imported the prompt pack source.",
    "assistant_prompt_sources.created_at": "Row creation timestamp.",
    "assistant_prompt_sources.updated_at": "Row update timestamp.",
    "assistant_prompt_node_versions.version_id": "Immutable prompt node version identifier derived from key and checksum.",
    "assistant_prompt_node_versions.source_id": "Prompt pack source import that produced this node version.",
    "assistant_prompt_node_versions.pack_key": "Prompt pack key that owns this node version.",
    "assistant_prompt_node_versions.pack_version": "Prompt pack version that owns this node version.",
    "assistant_prompt_node_versions.prompt_key": "Logical prompt node key used by runtime selection.",
    "assistant_prompt_node_versions.prompt_node_id": "Compatibility prompt node id written to the legacy active table.",
    "assistant_prompt_node_versions.title": "Human-readable prompt node title.",
    "assistant_prompt_node_versions.category": "Prompt node category such as root, governance, context or renderer.",
    "assistant_prompt_node_versions.tree_path": "Prompt tree path used for ancestor-closed selection.",
    "assistant_prompt_node_versions.parent_key": "Parent prompt key for tree traversal.",
    "assistant_prompt_node_versions.phase": "Conversation phase where this prompt node is eligible.",
    "assistant_prompt_node_versions.trigger_json": "Structured trigger conditions imported from pack.yaml.",
    "assistant_prompt_node_versions.prompt_text": "Validated prompt text imported from Markdown source.",
    "assistant_prompt_node_versions.risk_level": "Risk level metadata attached to this prompt node.",
    "assistant_prompt_node_versions.source_ref": "Repository-relative Markdown file path for this prompt text.",
    "assistant_prompt_node_versions.checksum": "Stable checksum of prompt_key, version, prompt_text and source_ref.",
    "assistant_prompt_node_versions.status": "Version lifecycle status such as approved or rejected.",
    "assistant_prompt_node_versions.metadata_json": "Importer and validation metadata for this prompt node version.",
    "assistant_prompt_node_versions.created_at": "Row creation timestamp.",
    "assistant_prompt_node_versions.updated_at": "Row update timestamp.",
    "assistant_prompt_activations.activation_id": "Environment-scoped active prompt pack activation identifier.",
    "assistant_prompt_activations.assistant_key": "Assistant runtime key that consumes this activation.",
    "assistant_prompt_activations.environment": "Environment scope such as dev, staging or production.",
    "assistant_prompt_activations.pack_key": "Activated prompt pack key.",
    "assistant_prompt_activations.pack_version": "Activated prompt pack version.",
    "assistant_prompt_activations.source_id": "Prompt source import selected by this activation.",
    "assistant_prompt_activations.version_refs": "JSON list of immutable prompt node version refs in the active pack.",
    "assistant_prompt_activations.bundle_signature": "Checksum of active version refs used to detect activation drift.",
    "assistant_prompt_activations.status": "Activation status; only active records should be used at runtime.",
    "assistant_prompt_activations.active_from": "Timestamp from which this activation is valid.",
    "assistant_prompt_activations.active_to": "Timestamp when this activation was superseded, if any.",
    "assistant_prompt_activations.activated_by": "Actor or process that activated this prompt pack.",
    "assistant_prompt_activations.activation_metadata_json": "Activation reason, validation and source checksum metadata.",
    "assistant_prompt_activations.created_at": "Row creation timestamp.",
    "assistant_prompt_activations.updated_at": "Row update timestamp.",
    "assistant_prompt_activation_events.event_id": "Prompt activation lifecycle event identifier.",
    "assistant_prompt_activation_events.activation_id": "Prompt activation that this event describes.",
    "assistant_prompt_activation_events.event_type": "Lifecycle event type such as seed_or_refresh or rollback.",
    "assistant_prompt_activation_events.actor": "Actor or process that emitted the event.",
    "assistant_prompt_activation_events.event_json": "Structured event payload for prompt activation audit.",
    "assistant_prompt_activation_events.created_at": "Row creation timestamp.",
    "assistant_prompt_activation_events.updated_at": "Row update timestamp.",
    "assistant_prompt_bundles.activation_id": "Prompt activation used for this bundle; old requests remain traceable after rollback.",
    "assistant_prompt_bundles.version_refs": "Immutable prompt node version refs selected for this bundle.",
    "assistant_runtime_config_sources.source_id": "Stable runtime config source import identifier.",
    "assistant_runtime_config_sources.config_key": "Logical runtime config key consumed by Research Assistant.",
    "assistant_runtime_config_sources.config_version": "Semantic runtime config version imported from Git.",
    "assistant_runtime_config_sources.source_path": "Repository-relative runtime config YAML path.",
    "assistant_runtime_config_sources.source_commit": "Git commit or external source revision for this config import when available.",
    "assistant_runtime_config_sources.source_sha256": "SHA256 of normalized runtime config for drift detection.",
    "assistant_runtime_config_sources.config_json": "Validated runtime context configuration imported from Git.",
    "assistant_runtime_config_sources.status": "Runtime config source lifecycle status.",
    "assistant_runtime_config_sources.metadata_json": "Importer metadata such as schema and validation context.",
    "assistant_runtime_config_sources.imported_at": "Timestamp when the runtime config source was imported.",
    "assistant_runtime_config_sources.imported_by": "Actor or process that imported the runtime config source.",
    "assistant_runtime_config_sources.created_at": "Row creation timestamp.",
    "assistant_runtime_config_sources.updated_at": "Row update timestamp.",
    "assistant_runtime_config_activations.activation_id": "Environment-scoped active runtime config activation identifier.",
    "assistant_runtime_config_activations.config_key": "Runtime config key activated for the assistant.",
    "assistant_runtime_config_activations.config_version": "Runtime config version activated for this environment.",
    "assistant_runtime_config_activations.environment": "Environment scope such as dev, staging or production.",
    "assistant_runtime_config_activations.source_id": "Runtime config source import selected by this activation.",
    "assistant_runtime_config_activations.config_json": "Active runtime context configuration snapshot read by chat runtime.",
    "assistant_runtime_config_activations.status": "Activation status; only active records should be used at runtime.",
    "assistant_runtime_config_activations.active_from": "Timestamp from which this activation is valid.",
    "assistant_runtime_config_activations.active_to": "Timestamp when this activation was superseded, if any.",
    "assistant_runtime_config_activations.activated_by": "Actor or process that activated this runtime config.",
    "assistant_runtime_config_activations.activation_metadata_json": "Activation reason, validation and source checksum metadata.",
    "assistant_runtime_config_activations.created_at": "Row creation timestamp.",
    "assistant_runtime_config_activations.updated_at": "Row update timestamp.",
    "assistant_context_segments.segment_id": "Derived context segment identifier.",
    "assistant_context_segments.conversation_id": "Conversation whose original messages are summarized by this segment.",
    "assistant_context_segments.segment_type": "Derived segment type such as compact_summary or retrieved_raw_snippet.",
    "assistant_context_segments.summary_depth": "Compaction depth for summary-of-summaries lifecycle control.",
    "assistant_context_segments.content_text": "Human-readable compact summary or derived context text.",
    "assistant_context_segments.content_json": "Typed compact summary payload; original messages remain the source of truth.",
    "assistant_context_segments.source_message_ids": "Original message IDs covered by this derived segment.",
    "assistant_context_segments.source_sha256": "Checksum of the covered source messages used for traceability.",
    "assistant_context_segments.prompt_activation_id": "Prompt activation used to generate this derived context segment.",
    "assistant_context_segments.runtime_config_activation_id": "Runtime config activation used to generate this segment.",
    "assistant_context_segments.status": "Segment lifecycle status such as active or superseded.",
    "assistant_context_segments.metadata_json": "Task id, compaction trigger and worker policy metadata.",
    "assistant_context_segments.created_at": "Row creation timestamp.",
    "assistant_context_segments.updated_at": "Row update timestamp.",
    "assistant_context_key_facts.fact_id": "Structured key fact identifier.",
    "assistant_context_key_facts.conversation_id": "Conversation that owns this key fact.",
    "assistant_context_key_facts.segment_id": "Context segment from which this key fact was extracted.",
    "assistant_context_key_facts.fact_type": "Key fact category such as decision, approval, open_task or key_fact_block.",
    "assistant_context_key_facts.fact_text": "Human-readable key fact text used during context recovery.",
    "assistant_context_key_facts.fact_json": "Structured key fact payload including decisions, approvals or open tasks.",
    "assistant_context_key_facts.source_message_ids": "Original message IDs supporting the key fact.",
    "assistant_context_key_facts.confidence": "Model or rule confidence for this key fact.",
    "assistant_context_key_facts.status": "Key fact lifecycle status such as active or superseded.",
    "assistant_context_key_facts.metadata_json": "Task id and extraction metadata.",
    "assistant_context_key_facts.created_at": "Row creation timestamp.",
    "assistant_context_key_facts.updated_at": "Row update timestamp.",
    "assistant_context_assembly_traces.assembly_trace_id": "Per-turn context assembly trace identifier.",
    "assistant_context_assembly_traces.conversation_id": "Conversation assembled for this turn.",
    "assistant_context_assembly_traces.task_id": "Assistant task associated with this assembly trace.",
    "assistant_context_assembly_traces.prompt_activation_id": "Prompt activation selected for this turn.",
    "assistant_context_assembly_traces.runtime_config_activation_id": "Runtime config activation selected for this turn.",
    "assistant_context_assembly_traces.budget_json": "Context budget planner output for this chat turn.",
    "assistant_context_assembly_traces.assembly_json": "Actual context assembly order and selected message/segment counts.",
    "assistant_context_assembly_traces.source_refs_json": "Prompt activation, runtime config activation and context source references.",
    "assistant_context_assembly_traces.status": "Assembly status such as ok, retry_after_compaction or failed.",
    "assistant_context_assembly_traces.created_at": "Row creation timestamp.",
    "assistant_context_assembly_traces.updated_at": "Row update timestamp.",
    "qe_autonomous_evolution_runs.auto_run_id": "Stable autonomous QE run identifier.",
    "qe_autonomous_evolution_runs.qe_task_id": "QE evolution task controlled by this autonomous loop.",
    "qe_autonomous_evolution_runs.methodology_ref": "Methodology or evolution-route reference used by the autonomous loop.",
    "qe_autonomous_evolution_runs.stop_conditions_json": "JSONB stop-condition policy including target, no-improve and failure guards.",
    "qe_autonomous_evolution_runs.budget_json": "JSONB budget guard policy including max loops, elapsed time and GPU occupancy.",
    "qe_autonomous_evolution_runs.status": "Autonomous loop status: running, stopped_target, stopped_no_improve, stopped_budget, or failed.",
    "qe_autonomous_evolution_runs.loops_completed": "Number of QE loops observed by the autonomous state machine.",
    "qe_autonomous_evolution_runs.last_verdict_json": "Compact last verdict and final autonomy report; large artifacts remain referenced externally.",
    "qe_autonomous_evolution_runs.created_at": "Row creation timestamp.",
    "qe_autonomous_evolution_runs.updated_at": "Row update timestamp.",
    "assistant_code_context_refs.code_ref_id": "Stable code context reference identifier for query-scoped code intelligence.",
    "assistant_code_context_refs.task_id": "Optional Research Assistant task that owns this code context reference.",
    "assistant_code_context_refs.query_scope": "Concrete query scope resolved from user text: symbol, module, or path.",
    "assistant_code_context_refs.manifest_json": "Compact CodeGraph/Understand manifest, affected-tests summary, and context artifact refs.",
    "assistant_code_context_refs.source": "Code intelligence source such as codegraph or understand_anything.",
    "assistant_code_context_refs.provenance_json": "Required provenance including commit, file, symbol, and generated_at.",
    "assistant_code_context_refs.as_of": "Timestamp of the adapter artifact or graph snapshot used by this reference.",
    "assistant_code_context_refs.created_at": "Row creation timestamp.",
    "assistant_proactive_reports.report_id": "Stable proactive report identifier.",
    "assistant_proactive_reports.report_type": "Report type such as morning_brief or experiment_daily.",
    "assistant_proactive_reports.report_date": "Business date the proactive report covers.",
    "assistant_proactive_reports.summary_md": "Evidence-first natural-language report body.",
    "assistant_proactive_reports.sections_json": "Structured sections with facts, source_refs, reason_codes, and warnings.",
    "assistant_proactive_reports.source_refs_json": "Flattened evidence references used by the report.",
    "assistant_proactive_reports.status": "Report generation status.",
    "assistant_proactive_reports.created_at": "Row creation timestamp.",
    "assistant_reflection_cards.card_id": "Stable Reflection Card identifier.",
    "assistant_reflection_cards.task_id": "Optional Research Assistant task that produced the reflection.",
    "assistant_reflection_cards.trigger": "Reflection trigger: failure, correction, or low_confidence.",
    "assistant_reflection_cards.lesson_md": "External-safe lesson without chain-of-thought disclosure.",
    "assistant_reflection_cards.structured_json": "Structured cause, lesson, next strategy, source_refs, reason_codes, warnings, and safety flags.",
    "assistant_reflection_cards.memory_ref": "personal.episodic.* memory_id written for L1 recall.",
    "assistant_reflection_cards.created_at": "Row creation timestamp.",
    "assistant_prompt_lab_runs.lab_run_id": "Stable Prompt Lab run identifier.",
    "assistant_prompt_lab_runs.target_prompt_key": "Prompt node key targeted by the offline optimization candidate.",
    "assistant_prompt_lab_runs.optimizer": "Offline optimizer family such as gepa, dspy_mipro, or manual.",
    "assistant_prompt_lab_runs.eval_set_ref": "Historical trace evaluation-set reference used for the candidate.",
    "assistant_prompt_lab_runs.candidate_text": "Candidate prompt text generated offline; not active until approved.",
    "assistant_prompt_lab_runs.judge_score_json": "Offline LLM-as-judge or deterministic judge score, dimensions, reason_codes, warnings, and source_refs.",
    "assistant_prompt_lab_runs.status": "Prompt Lab lifecycle status: candidate, approved, or rejected.",
    "assistant_prompt_lab_runs.approval_request_id": "Pending assistant_approval_requests record required before prompt activation can change.",
    "assistant_prompt_lab_runs.created_at": "Row creation timestamp.",
    "assistant_skill_library.skill_id": "Stable Skill Library recipe identifier.",
    "assistant_skill_library.skill_key": "Unique reusable skill key derived from a successful workflow or explicit operator key.",
    "assistant_skill_library.description": "Human-readable summary of the reusable workflow, prompt, and tool recipe.",
    "assistant_skill_library.recipe_json": "Reusable workflow, prompt, tool, evidence, and risk-gate recipe; never executable without approval.",
    "assistant_skill_library.success_count": "Number of successful source workflows supporting this recipe.",
    "assistant_skill_library.provenance_json": "Source task, evidence refs, approval request, and generated_at metadata for audit replay.",
    "assistant_skill_library.status": "Skill lifecycle status: draft, approved, or deprecated.",
    "assistant_skill_library.created_at": "Row creation timestamp.",
    "assistant_capabilities.capability_id": "Stable capability registry identifier used by planner and audit replay.",
    "assistant_capabilities.capability_key": "Human and model readable capability key such as qe.create_experiment_draft.",
    "assistant_capabilities.capability_type": "Capability implementation type: mcp_tool, skill, workflow_pack or composite.",
    "assistant_capabilities.title": "Chinese operator-facing capability title.",
    "assistant_capabilities.natural_language_triggers": "JSON trigger phrases or intents used to recall this capability; source is approved catalog sync.",
    "assistant_capabilities.description_for_llm": "Chinese LLM-facing description including boundaries and non-goals.",
    "assistant_capabilities.risk_level": "Risk classification used for approval and model routing gates.",
    "assistant_capabilities.side_effect_level": "Side-effect classification controlling confirmation, preflight and execute policy.",
    "assistant_capabilities.required_confirmations": "JSON list of exact confirmation tokens required before side-effecting execution.",
    "assistant_capabilities.preferred_model_role": "Preferred model role for planning or rendering; execution remains gateway-owned.",
    "assistant_capabilities.input_slots": "JSON schema-like input slot contract including required business fields and quality rules.",
    "assistant_capabilities.output_cards": "JSON list of human-readable result card descriptors; raw JSON is debug-only.",
    "assistant_capabilities.mcp_tool_refs": "JSON list of approved MCP server/tool references backing this capability.",
    "assistant_capabilities.skill_refs": "JSON list of approved skill keys backing this capability.",
    "assistant_capabilities.workflow_pack_ref": "Workflow pack identifier when this capability represents a composed assistant workflow.",
    "assistant_capabilities.status": "Capability lifecycle state; only approved capabilities are planner-selectable for new work.",
    "assistant_capabilities.source_ref": "Catalog, manifest or design source used by capability sync.",
    "assistant_capabilities.checksum": "Checksum of normalized capability metadata used to invalidate stale approvals.",
    "assistant_capabilities.last_synced_at": "Timestamp of the latest approved capability sync that touched this row.",
    "assistant_capabilities.created_at": "Row creation timestamp.",
    "assistant_capabilities.updated_at": "Row update timestamp.",
    "assistant_action_proposals.action_proposal_id": "Stable Action Proposal identifier used for confirm, preflight, approval, execute and audit replay.",
    "assistant_action_proposals.task_id": "Research Assistant task that owns this action proposal.",
    "assistant_action_proposals.conversation_id": "Conversation where the proposal was shown to the user, if any.",
    "assistant_action_proposals.capability_key": "Approved capability selected for this proposal at creation time.",
    "assistant_action_proposals.proposal_type": "Proposal implementation type: workflow_step, mcp_tool, skill or workflow_pack.",
    "assistant_action_proposals.title": "Chinese human-readable proposal title.",
    "assistant_action_proposals.summary": "Chinese business summary shown before confirmation and approval.",
    "assistant_action_proposals.risk_level": "Risk level snapshotted from the capability when the proposal was created.",
    "assistant_action_proposals.side_effect_level": "Side-effect level snapshotted from the capability when the proposal was created.",
    "assistant_action_proposals.input_json": "Canonical proposal input JSON used for plan digest and execution; source is user-confirmed slots.",
    "assistant_action_proposals.expected_result_json": "Expected result card contract and artifact expectations for preflight and execution.",
    "assistant_action_proposals.plan_digest": "Digest covering capability checksum, input, prompt bundle, runtime config and approval requirements.",
    "assistant_action_proposals.prompt_bundle_signature": "Prompt bundle signature active when the proposal was created.",
    "assistant_action_proposals.runtime_config_activation_id": "Runtime config activation used for policy, timeout and retry resolution.",
    "assistant_action_proposals.context_pack_id": "Context pack used to plan this action proposal, if any.",
    "assistant_action_proposals.status": "Proposal state machine status from proposed through confirmed, preflight, approval and execution result.",
    "assistant_action_proposals.approval_id": "Approval gate record bound to this exact proposal digest, if required.",
    "assistant_action_proposals.idempotency_key": "Idempotency key preventing duplicate proposal or execution replay with different payload.",
    "assistant_action_proposals.expires_at": "Expiration timestamp after which confirmation, preflight or approval must be renewed.",
    "assistant_action_proposals.created_by": "Actor that created the proposal, normally assistant planner.",
    "assistant_action_proposals.created_at": "Row creation timestamp.",
    "assistant_action_proposals.updated_at": "Row update timestamp.",
    "assistant_mcp_tool_events.action_proposal_id": "Action Proposal associated with this MCP event when execution uses the gateway.",
    "assistant_mcp_tool_events.approval_id": "Approval record consumed or checked for this MCP event.",
    "assistant_mcp_tool_events.plan_digest": "Plan digest checked before executing this MCP event.",
    "assistant_mcp_tool_events.transport": "Execution transport such as loopback_http or python_module.",
    "assistant_mcp_tool_events.timeout_ms": "Resolved timeout in milliseconds from runtime config for this attempt.",
    "assistant_mcp_tool_events.attempt_index": "Zero-based attempt index used for timeout and retry audit.",
    "assistant_mcp_tool_events.duration_ms": "Execution duration in milliseconds for this attempt.",
    "assistant_mcp_tool_events.result_card_json": "Human-readable result card payload; raw response JSON is debug-only.",
    "assistant_mcp_tool_events.artifact_refs": "JSON list of durable artifact references produced or inspected by this execution.",
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_comment_ddl() -> list[str]:
    table_comments = [f"COMMENT ON TABLE {table} IS '{_sql_literal(comment)}'" for table, comment in TABLE_COMMENTS.items()]
    column_comments = [f"COMMENT ON COLUMN {column} IS '{_sql_literal(comment)}'" for column, comment in COLUMN_COMMENTS.items()]
    return table_comments + column_comments


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
