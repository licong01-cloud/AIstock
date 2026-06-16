CREATE TABLE IF NOT EXISTS assistant_code_context_refs (
    code_ref_id TEXT PRIMARY KEY,
    task_id TEXT,
    query_scope TEXT NOT NULL,
    manifest_json JSONB NOT NULL,
    source TEXT NOT NULL,
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    as_of TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE assistant_code_context_refs IS '代码智能(CodeGraph/Understand)注入 Context Pack 的轻量引用，AST确定性，无embedding';
COMMENT ON COLUMN assistant_code_context_refs.code_ref_id IS 'Stable code context reference identifier for query-scoped code intelligence.';
COMMENT ON COLUMN assistant_code_context_refs.task_id IS 'Optional Research Assistant task that owns this code context reference.';
COMMENT ON COLUMN assistant_code_context_refs.query_scope IS 'Concrete query scope resolved from user text: symbol, module, or path.';
COMMENT ON COLUMN assistant_code_context_refs.manifest_json IS 'Compact CodeGraph/Understand manifest, affected-tests summary, and context artifact refs.';
COMMENT ON COLUMN assistant_code_context_refs.source IS 'Code intelligence source such as codegraph or understand_anything.';
COMMENT ON COLUMN assistant_code_context_refs.provenance_json IS 'Required provenance including commit, file, symbol, and generated_at.';
COMMENT ON COLUMN assistant_code_context_refs.as_of IS 'Timestamp of the adapter artifact or graph snapshot used by this reference.';
COMMENT ON COLUMN assistant_code_context_refs.created_at IS 'Row creation timestamp.';
