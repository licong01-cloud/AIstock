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
);
CREATE INDEX IF NOT EXISTS idx_aar_parent ON assistant_agent_runs(parent_task_id, status);
COMMENT ON TABLE assistant_agent_runs IS 'Agent Teams 主从运行记录；worker 结果 reduce 回 orchestrator';
COMMENT ON COLUMN assistant_agent_runs.agent_run_id IS 'Stable Agent Teams worker run identifier.';
COMMENT ON COLUMN assistant_agent_runs.parent_task_id IS 'Parent Research Assistant task or team task that owns this worker run.';
COMMENT ON COLUMN assistant_agent_runs.agent_key IS 'Declarative worker key from configs/research_assistant/agent_teams.yaml.';
COMMENT ON COLUMN assistant_agent_runs.role IS 'Worker role used for decomposition and reduce.';
COMMENT ON COLUMN assistant_agent_runs.status IS 'Worker run lifecycle status: queued, running, succeeded, failed, or cancelled.';
COMMENT ON COLUMN assistant_agent_runs.input_json IS 'Worker input payload including objective, prompt nodes, context refs, and tool subset.';
COMMENT ON COLUMN assistant_agent_runs.result_json IS 'Structured worker output containing summary, artifacts, evidence refs, status, and errors.';
COMMENT ON COLUMN assistant_agent_runs.model_profile_id IS 'Model profile or model role selected for this worker run.';
COMMENT ON COLUMN assistant_agent_runs.trace_id IS 'Trace event or workbench trace identifier for worker internals.';
COMMENT ON COLUMN assistant_agent_runs.created_at IS 'Row creation timestamp.';
COMMENT ON COLUMN assistant_agent_runs.updated_at IS 'Row update timestamp.';
