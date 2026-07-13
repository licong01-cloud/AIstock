CREATE TABLE IF NOT EXISTS assistant_reflection_cards (
    card_id TEXT PRIMARY KEY,
    task_id TEXT,
    trigger TEXT NOT NULL,
    lesson_md TEXT NOT NULL,
    structured_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE assistant_reflection_cards IS 'Reflection Cards generated from task failure, correction, or low-confidence signals; writes personal.episodic memory only.';
COMMENT ON COLUMN assistant_reflection_cards.card_id IS 'Stable Reflection Card identifier.';
COMMENT ON COLUMN assistant_reflection_cards.task_id IS 'Optional Research Assistant task that produced the reflection.';
COMMENT ON COLUMN assistant_reflection_cards.trigger IS 'Reflection trigger: failure, correction, or low_confidence.';
COMMENT ON COLUMN assistant_reflection_cards.lesson_md IS 'External-safe lesson without chain-of-thought disclosure.';
COMMENT ON COLUMN assistant_reflection_cards.structured_json IS 'Structured cause, lesson, next strategy, source_refs, reason_codes, warnings, and safety flags.';
COMMENT ON COLUMN assistant_reflection_cards.memory_ref IS 'personal.episodic.* memory_id written for L1 recall.';
COMMENT ON COLUMN assistant_reflection_cards.created_at IS 'Row creation timestamp.';
