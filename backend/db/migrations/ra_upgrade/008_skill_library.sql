CREATE TABLE IF NOT EXISTS assistant_skill_library (
    skill_id TEXT PRIMARY KEY,
    skill_key TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    recipe_json JSONB NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE assistant_skill_library IS 'Gated reusable workflow, prompt and tool recipes; draft until human approval and reused only through Action Proposal gates.';
COMMENT ON COLUMN assistant_skill_library.skill_id IS 'Stable Skill Library recipe identifier.';
COMMENT ON COLUMN assistant_skill_library.skill_key IS 'Unique reusable skill key derived from a successful workflow or explicit operator key.';
COMMENT ON COLUMN assistant_skill_library.description IS 'Human-readable summary of the reusable workflow, prompt, and tool recipe.';
COMMENT ON COLUMN assistant_skill_library.recipe_json IS 'Reusable workflow, prompt, tool, evidence, and risk-gate recipe; never executable without approval.';
COMMENT ON COLUMN assistant_skill_library.success_count IS 'Number of successful source workflows supporting this recipe.';
COMMENT ON COLUMN assistant_skill_library.provenance_json IS 'Source task, evidence refs, approval request, and generated_at metadata for audit replay.';
COMMENT ON COLUMN assistant_skill_library.status IS 'Skill lifecycle status: draft, approved, or deprecated.';
COMMENT ON COLUMN assistant_skill_library.created_at IS 'Row creation timestamp.';
