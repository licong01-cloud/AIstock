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
);

COMMENT ON TABLE assistant_prompt_lab_runs IS 'Prompt Lab offline GEPA/DSPy-style candidate prompt runs with LLM-as-judge scoring and human approval gate.';
COMMENT ON COLUMN assistant_prompt_lab_runs.lab_run_id IS 'Stable Prompt Lab run identifier.';
COMMENT ON COLUMN assistant_prompt_lab_runs.target_prompt_key IS 'Prompt node key targeted by the offline optimization candidate.';
COMMENT ON COLUMN assistant_prompt_lab_runs.optimizer IS 'Offline optimizer family such as gepa, dspy_mipro, or manual.';
COMMENT ON COLUMN assistant_prompt_lab_runs.eval_set_ref IS 'Historical trace evaluation-set reference used for the candidate.';
COMMENT ON COLUMN assistant_prompt_lab_runs.candidate_text IS 'Candidate prompt text generated offline; not active until approved.';
COMMENT ON COLUMN assistant_prompt_lab_runs.judge_score_json IS 'Offline LLM-as-judge or deterministic judge score, dimensions, reason_codes, warnings, and source_refs.';
COMMENT ON COLUMN assistant_prompt_lab_runs.status IS 'Prompt Lab lifecycle status: candidate, approved, or rejected.';
COMMENT ON COLUMN assistant_prompt_lab_runs.approval_request_id IS 'Pending assistant_approval_requests record required before prompt activation can change.';
COMMENT ON COLUMN assistant_prompt_lab_runs.created_at IS 'Row creation timestamp.';
