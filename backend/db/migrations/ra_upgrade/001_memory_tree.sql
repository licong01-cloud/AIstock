BEGIN;

ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS tree_path TEXT;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS parent_key TEXT;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS node_type TEXT NOT NULL DEFAULT 'fact';
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'project';
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS importance REAL NOT NULL DEFAULT 0.5;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS auto_created BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS trust_level TEXT NOT NULL DEFAULT 'user_stated';
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE research_memory_items ADD COLUMN IF NOT EXISTS resident BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE research_memory_items
SET
    scope = COALESCE(NULLIF(scope, ''), 'project'),
    node_type = COALESCE(NULLIF(node_type, ''), 'fact'),
    importance = COALESCE(importance, 0.5),
    use_count = COALESCE(use_count, 0),
    auto_created = COALESCE(auto_created, FALSE),
    trust_level = COALESCE(NULLIF(trust_level, ''), 'user_stated'),
    provenance_json = COALESCE(provenance_json, '{}'::jsonb),
    resident = COALESCE(resident, FALSE),
    tree_path = COALESCE(
        NULLIF(tree_path, ''),
        CASE
            WHEN subject_key LIKE 'project.%' OR subject_key LIKE 'personal.%' THEN subject_key
            WHEN memory_type IN ('user_preference', 'directive', 'habit') THEN 'personal.' || memory_type || '.' || regexp_replace(subject_key, '[^a-zA-Z0-9_.-]+', '_', 'g')
            ELSE 'project.' || memory_type || '.' || regexp_replace(subject_key, '[^a-zA-Z0-9_.-]+', '_', 'g')
        END
    )
WHERE
    scope IS NULL OR scope = ''
    OR node_type IS NULL OR node_type = ''
    OR importance IS NULL
    OR use_count IS NULL
    OR auto_created IS NULL
    OR trust_level IS NULL OR trust_level = ''
    OR provenance_json IS NULL
    OR resident IS NULL
    OR tree_path IS NULL OR tree_path = '';

ALTER TABLE research_memory_items ALTER COLUMN node_type SET DEFAULT 'fact';
ALTER TABLE research_memory_items ALTER COLUMN node_type SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN scope SET DEFAULT 'project';
ALTER TABLE research_memory_items ALTER COLUMN scope SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN importance SET DEFAULT 0.5;
ALTER TABLE research_memory_items ALTER COLUMN importance SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN use_count SET DEFAULT 0;
ALTER TABLE research_memory_items ALTER COLUMN use_count SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN auto_created SET DEFAULT FALSE;
ALTER TABLE research_memory_items ALTER COLUMN auto_created SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN trust_level SET DEFAULT 'user_stated';
ALTER TABLE research_memory_items ALTER COLUMN trust_level SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN provenance_json SET DEFAULT '{}'::jsonb;
ALTER TABLE research_memory_items ALTER COLUMN provenance_json SET NOT NULL;
ALTER TABLE research_memory_items ALTER COLUMN resident SET DEFAULT FALSE;
ALTER TABLE research_memory_items ALTER COLUMN resident SET NOT NULL;

ALTER TABLE research_memory_items DROP CONSTRAINT IF EXISTS ck_rmi_type;
ALTER TABLE research_memory_items
    ADD CONSTRAINT ck_rmi_type CHECK (
        memory_type IN (
            'core','procedural','architecture','roadmap','task_state','experiment','episodic','external','agenda',
            'user_preference','directive','habit','analysis_note'
        )
    );

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_rmi_node_type' AND conrelid = 'research_memory_items'::regclass
    ) THEN
        ALTER TABLE research_memory_items ADD CONSTRAINT ck_rmi_node_type CHECK (node_type IN ('branch','fact'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_rmi_scope' AND conrelid = 'research_memory_items'::regclass
    ) THEN
        ALTER TABLE research_memory_items ADD CONSTRAINT ck_rmi_scope CHECK (scope IN ('project','personal'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_rmi_importance' AND conrelid = 'research_memory_items'::regclass
    ) THEN
        ALTER TABLE research_memory_items ADD CONSTRAINT ck_rmi_importance CHECK (importance >= 0 AND importance <= 1);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_rmi_trust_level' AND conrelid = 'research_memory_items'::regclass
    ) THEN
        ALTER TABLE research_memory_items ADD CONSTRAINT ck_rmi_trust_level CHECK (trust_level IN ('user_stated','assistant_inferred'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_rmi_use_count' AND conrelid = 'research_memory_items'::regclass
    ) THEN
        ALTER TABLE research_memory_items ADD CONSTRAINT ck_rmi_use_count CHECK (use_count >= 0);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_rmi_tree ON research_memory_items(scope, tree_path, approval_status, importance DESC);
CREATE INDEX IF NOT EXISTS idx_rmi_parent ON research_memory_items(parent_key);
CREATE INDEX IF NOT EXISTS idx_rmi_resident ON research_memory_items(scope, resident) WHERE resident = TRUE;

COMMENT ON COLUMN research_memory_items.tree_path IS 'Dotted memory tree path under project.* or personal.* used for collapsed branch retrieval.';
COMMENT ON COLUMN research_memory_items.parent_key IS 'Parent memory key or branch path; root nodes keep NULL.';
COMMENT ON COLUMN research_memory_items.node_type IS 'Memory node kind: branch for structural nodes, fact for retrievable content.';
COMMENT ON COLUMN research_memory_items.scope IS 'Memory scope boundary: project for shared project memory, personal for user-specific memory.';
COMMENT ON COLUMN research_memory_items.importance IS 'Normalized 0..1 priority used with recency for deterministic memory ordering.';
COMMENT ON COLUMN research_memory_items.last_used_at IS 'Last timestamp when this memory was selected into a context pack.';
COMMENT ON COLUMN research_memory_items.use_count IS 'Number of times this memory has been selected or self-edited.';
COMMENT ON COLUMN research_memory_items.auto_created IS 'True when curator created this branch or fact without manual seed.';
COMMENT ON COLUMN research_memory_items.trust_level IS 'user_stated means the user explicitly said it; assistant_inferred means the assistant inferred it.';
COMMENT ON COLUMN research_memory_items.provenance_json IS 'Required provenance for auto-created memories, including conversation, message, turn, and source.';
COMMENT ON COLUMN research_memory_items.resident IS 'True when directive or preference memory must be injected every turn regardless of branch match.';

COMMIT;
