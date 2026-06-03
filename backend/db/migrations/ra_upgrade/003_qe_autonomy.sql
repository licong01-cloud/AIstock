CREATE TABLE IF NOT EXISTS qe_autonomous_evolution_runs (
    auto_run_id TEXT PRIMARY KEY,
    qe_task_id TEXT NOT NULL,
    methodology_ref TEXT,
    stop_conditions_json JSONB NOT NULL,
    budget_json JSONB NOT NULL,
    status TEXT NOT NULL,
    loops_completed INTEGER NOT NULL DEFAULT 0,
    last_verdict_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qaer_status CHECK (status IN ('running','stopped_target','stopped_no_improve','stopped_budget','failed'))
);
CREATE INDEX IF NOT EXISTS idx_qaer_task_status ON qe_autonomous_evolution_runs(qe_task_id, status);
CREATE INDEX IF NOT EXISTS idx_qaer_updated_at ON qe_autonomous_evolution_runs(updated_at);
COMMENT ON TABLE qe_autonomous_evolution_runs IS 'QE 自主演进主循环运行记录，含停止条件、预算守护、审批边界与最终报告';
COMMENT ON COLUMN qe_autonomous_evolution_runs.auto_run_id IS 'Stable autonomous QE run identifier.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.qe_task_id IS 'QE evolution task controlled by this autonomous loop.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.methodology_ref IS 'Methodology or evolution-route reference used by the autonomous loop.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.stop_conditions_json IS 'JSONB stop-condition policy including target, no-improve and failure guards.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.budget_json IS 'JSONB budget guard policy including max loops, elapsed time and GPU occupancy.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.status IS 'Autonomous loop status: running, stopped_target, stopped_no_improve, stopped_budget, or failed.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.loops_completed IS 'Number of QE loops observed by the autonomous state machine.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.last_verdict_json IS 'Compact last verdict and final autonomy report; large artifacts remain referenced externally.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.created_at IS 'Row creation timestamp.';
COMMENT ON COLUMN qe_autonomous_evolution_runs.updated_at IS 'Row update timestamp.';
