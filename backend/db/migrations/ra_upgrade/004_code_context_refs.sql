CREATE TABLE IF NOT EXISTS assistant_code_context_refs (
    code_context_ref_id TEXT PRIMARY KEY,
    context_pack_id TEXT NOT NULL,
    task_id TEXT,
    query_text TEXT NOT NULL,
    file_path TEXT NOT NULL,
    symbol TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    summary_ref TEXT,
    detail_ref TEXT,
    edge_refs_json JSONB NOT NULL,
    affected_tests_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    manifest_json JSONB NOT NULL,
    provenance_json JSONB NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_accr_status CHECK (status IN ('ok','evidence_insufficient')),
    CONSTRAINT ck_accr_edge_refs_array CHECK (jsonb_typeof(edge_refs_json) = 'array' AND jsonb_array_length(edge_refs_json) > 0),
    CONSTRAINT ck_accr_affected_tests_array CHECK (jsonb_typeof(affected_tests_json) = 'array'),
    CONSTRAINT ck_accr_manifest_object CHECK (jsonb_typeof(manifest_json) = 'object'),
    CONSTRAINT ck_accr_provenance_object CHECK (jsonb_typeof(provenance_json) = 'object' AND provenance_json <> '{}'::jsonb),
    CONSTRAINT ck_accr_no_test_success_claim CHECK (
        affected_tests_json::text !~* '"(status|classification|result|state)"\s*:\s*"(passed|verified|ci_passed|pytest_passed|nox_passed)"'
    )
);

CREATE INDEX IF NOT EXISTS idx_accr_context_pack ON assistant_code_context_refs(context_pack_id, status);
CREATE INDEX IF NOT EXISTS idx_accr_task_file ON assistant_code_context_refs(task_id, file_path);
CREATE INDEX IF NOT EXISTS idx_accr_as_of ON assistant_code_context_refs(as_of);
CREATE INDEX IF NOT EXISTS idx_accr_manifest_gin ON assistant_code_context_refs USING GIN (manifest_json);
CREATE INDEX IF NOT EXISTS idx_accr_provenance_gin ON assistant_code_context_refs USING GIN (provenance_json);

COMMENT ON TABLE assistant_code_context_refs IS 'Research Assistant code intelligence context refs from CodeGraph/Understand adapter; compact AST-derived summaries only, with provenance and explicit as_of.';
COMMENT ON COLUMN assistant_code_context_refs.code_context_ref_id IS 'Stable row id for one code context ref linked to a context pack.';
COMMENT ON COLUMN assistant_code_context_refs.context_pack_id IS 'Research Assistant context pack id that consumed this ref; no implicit schema creation is allowed.';
COMMENT ON COLUMN assistant_code_context_refs.task_id IS 'Optional Research Assistant task id owning the context pack.';
COMMENT ON COLUMN assistant_code_context_refs.query_text IS 'Original user or orchestrator query that requested code/module context.';
COMMENT ON COLUMN assistant_code_context_refs.file_path IS 'Repository file path or repo-scope ref normalized with slash separators.';
COMMENT ON COLUMN assistant_code_context_refs.symbol IS 'Symbol or module name used for sorting and traceability.';
COMMENT ON COLUMN assistant_code_context_refs.status IS 'Ref status: ok or evidence_insufficient; insufficient refs are not treated as verified facts.';
COMMENT ON COLUMN assistant_code_context_refs.summary_ref IS 'Compact summary artifact path from the existing adapter, nullable when unavailable.';
COMMENT ON COLUMN assistant_code_context_refs.detail_ref IS 'Compact detail artifact path from the existing adapter, nullable when unavailable.';
COMMENT ON COLUMN assistant_code_context_refs.edge_refs_json IS 'JSONB array of compact file/symbol/edge refs; schema version comes from manifest_json and must not contain full source payloads.';
COMMENT ON COLUMN assistant_code_context_refs.affected_tests_json IS 'JSONB array of impacted/recommended tests only; never stores passed/verified/CI outcome claims.';
COMMENT ON COLUMN assistant_code_context_refs.manifest_json IS 'JSONB compact manifest metadata including provider, schema_version, source refs, status, and quality boundaries.';
COMMENT ON COLUMN assistant_code_context_refs.provenance_json IS 'JSONB provenance object from scripts.code_intelligence_adapter outputs; non-empty and used for evidence traceability.';
COMMENT ON COLUMN assistant_code_context_refs.as_of IS 'Timestamp explicitly sourced from adapter manifest generated_at/as_of or caller input; no default clock fallback.';
COMMENT ON COLUMN assistant_code_context_refs.created_at IS 'Row creation timestamp assigned by PostgreSQL for audit ordering, not used as code evidence as_of.';
COMMENT ON COLUMN assistant_code_context_refs.updated_at IS 'Row update timestamp assigned by PostgreSQL for audit ordering, not used as code evidence as_of.';
