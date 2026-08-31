BEGIN;

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.stock_universe_pit_authority_versions (
    authority_id TEXT NOT NULL CONSTRAINT ck_pit_authority_versions_authority
        CHECK (authority_id = 'aistock_equity_pit_canonical'),
    rule_version TEXT NOT NULL,
    rolling_key TEXT NOT NULL UNIQUE,
    rule_parameters_digest TEXT NOT NULL CONSTRAINT ck_pit_authority_versions_rule_digest
        CHECK (rule_parameters_digest ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CONSTRAINT ck_pit_authority_versions_status CHECK (status IN (
        'DEPLOYED_LEGACY_PENDING_MIGRATION',
        'ACTIVE_CANONICAL',
        'SESSION_PINNED_DRAINING',
        'ARCHIVED_NONCANONICAL',
        'EMERGENCY_LEGACY_ROLLBACK'
    )),
    first_candidate_bundle_digest TEXT CONSTRAINT ck_pit_authority_versions_candidate_digest CHECK (
        first_candidate_bundle_digest IS NULL OR first_candidate_bundle_digest ~ '^[0-9a-f]{64}$'
    ),
    first_release_id TEXT,
    first_release_receipt_digest TEXT CONSTRAINT ck_pit_authority_versions_release_digest CHECK (
        first_release_receipt_digest IS NULL OR first_release_receipt_digest ~ '^[0-9a-f]{64}$'
    ),
    first_source_commit TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (authority_id, rule_version)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_universe_pit_authority_version_identity
    ON market.stock_universe_pit_authority_versions (authority_id, rule_version, rolling_key);

CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_universe_pit_one_active_canonical
    ON market.stock_universe_pit_authority_versions (authority_id)
    WHERE status = 'ACTIVE_CANONICAL';

CREATE TABLE IF NOT EXISTS market.stock_universe_pit_authority_pointer (
    authority_id TEXT PRIMARY KEY CONSTRAINT ck_pit_authority_pointer_authority
        CHECK (authority_id = 'aistock_equity_pit_canonical'),
    current_rule_version TEXT NOT NULL,
    current_rolling_key TEXT NOT NULL,
    activation_generation BIGINT NOT NULL DEFAULT 0 CONSTRAINT ck_pit_authority_pointer_generation
        CHECK (activation_generation >= 0),
    activation_envelope_digest TEXT CONSTRAINT ck_pit_authority_pointer_envelope_digest CHECK (
        activation_envelope_digest IS NULL OR activation_envelope_digest ~ '^[0-9a-f]{64}$'
    ),
    expected_source_commit TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_pit_authority_pointer_version
        FOREIGN KEY (authority_id, current_rule_version, current_rolling_key)
        REFERENCES market.stock_universe_pit_authority_versions (authority_id, rule_version, rolling_key),
    CONSTRAINT ck_pit_authority_pointer_activation_evidence CHECK (
        (activation_generation = 0 AND activation_envelope_digest IS NULL AND expected_source_commit IS NULL)
        OR
        (activation_generation > 0 AND activation_envelope_digest IS NOT NULL AND expected_source_commit IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS market.stock_universe_pit_authority_events (
    event_id BIGSERIAL PRIMARY KEY,
    authority_id TEXT NOT NULL CONSTRAINT ck_pit_authority_events_authority
        CHECK (authority_id = 'aistock_equity_pit_canonical'),
    event_type TEXT NOT NULL CONSTRAINT ck_pit_authority_events_type
        CHECK (event_type IN ('PREPARE', 'ACTIVATE', 'ROLLBACK')),
    before_generation BIGINT CONSTRAINT ck_pit_authority_events_before_generation
        CHECK (before_generation IS NULL OR before_generation >= 0),
    after_generation BIGINT NOT NULL CONSTRAINT ck_pit_authority_events_after_generation
        CHECK (after_generation >= 0),
    before_rule_version TEXT,
    after_rule_version TEXT NOT NULL,
    before_rolling_key TEXT,
    after_rolling_key TEXT NOT NULL,
    candidate_bundle_digest TEXT CONSTRAINT ck_pit_authority_events_candidate_digest CHECK (
        candidate_bundle_digest IS NULL OR candidate_bundle_digest ~ '^[0-9a-f]{64}$'
    ),
    activation_envelope_digest TEXT CONSTRAINT ck_pit_authority_events_envelope_digest CHECK (
        activation_envelope_digest IS NULL OR activation_envelope_digest ~ '^[0-9a-f]{64}$'
    ),
    independent_receipt_digest TEXT CONSTRAINT ck_pit_authority_events_receipt_digest CHECK (
        independent_receipt_digest IS NULL OR independent_receipt_digest ~ '^[0-9a-f]{64}$'
    ),
    expected_source_commit TEXT,
    operator_intent TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE market.stock_universe_pit_authority_versions IS
    'Immutable identity and lifecycle status for each canonical equity PIT authority version.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.authority_id IS 'Fixed singleton equity PIT authority id.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.rule_version IS 'Immutable PIT business-rule version.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.rolling_key IS 'Unique rolling materialization key for this rule version.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.rule_parameters_digest IS 'SHA-256 of canonical JSON rule parameters.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.status IS 'Authority lifecycle state; at most one row is ACTIVE_CANONICAL.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.first_candidate_bundle_digest IS 'First independently validated candidate bundle SHA-256.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.first_release_id IS 'First immutable dataset release bound to this rule version.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.first_release_receipt_digest IS 'First release validation receipt SHA-256.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.first_source_commit IS 'Source commit sealed by the first accepted candidate.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.created_at IS 'Version registration timestamp.';
COMMENT ON COLUMN market.stock_universe_pit_authority_versions.updated_at IS 'Lifecycle status update timestamp.';

COMMENT ON TABLE market.stock_universe_pit_authority_pointer IS
    'Singleton live equity PIT authority pointer updated only by transactional CAS.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.authority_id IS 'Fixed singleton equity PIT authority id.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.current_rule_version IS 'Rule version currently admitted for live consumers.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.current_rolling_key IS 'Rolling PIT key currently admitted for live consumers.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.activation_generation IS 'Monotonic CAS generation; zero is legacy pending migration.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.activation_envelope_digest IS 'Sealed activation envelope SHA-256 for generation greater than zero.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.expected_source_commit IS 'Source commit required by the active activation envelope.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.created_at IS 'Pointer creation timestamp.';
COMMENT ON COLUMN market.stock_universe_pit_authority_pointer.updated_at IS 'Last successful pointer CAS timestamp.';

COMMENT ON TABLE market.stock_universe_pit_authority_events IS
    'Append-only audit ledger for PIT authority prepare, activate, and rollback transitions.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.event_id IS 'Monotonic audit event id.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.authority_id IS 'Fixed singleton equity PIT authority id.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.event_type IS 'PREPARE, ACTIVATE, or ROLLBACK transition type.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.before_generation IS 'Pointer generation observed before transition; null for initial prepare.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.after_generation IS 'Pointer generation after transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.before_rule_version IS 'Rule version before transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.after_rule_version IS 'Rule version after transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.before_rolling_key IS 'Rolling key before transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.after_rolling_key IS 'Rolling key after transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.candidate_bundle_digest IS 'Validated candidate bundle SHA-256 used by transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.activation_envelope_digest IS 'Sealed activation envelope SHA-256 used by transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.independent_receipt_digest IS 'Independent validation receipt SHA-256 used by transition.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.expected_source_commit IS 'Source commit sealed by transition evidence.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.operator_intent IS 'Explicit operator intent recorded for audit.';
COMMENT ON COLUMN market.stock_universe_pit_authority_events.created_at IS 'Append-only event creation timestamp.';

CREATE OR REPLACE FUNCTION market.reject_stock_universe_pit_authority_event_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $function$
BEGIN
    RAISE EXCEPTION 'PIT_AUTHORITY_EVENTS_APPEND_ONLY';
END
$function$;

DROP TRIGGER IF EXISTS trg_stock_universe_pit_authority_events_append_only
    ON market.stock_universe_pit_authority_events;
CREATE TRIGGER trg_stock_universe_pit_authority_events_append_only
    BEFORE UPDATE OR DELETE ON market.stock_universe_pit_authority_events
    FOR EACH ROW EXECUTE FUNCTION market.reject_stock_universe_pit_authority_event_mutation();

INSERT INTO market.stock_universe_pit_authority_versions (
    authority_id, rule_version, rolling_key, rule_parameters_digest, status, first_source_commit
) VALUES (
    'aistock_equity_pit_canonical',
    'st_pub_next_trade_restore_active_l_v1',
    'shsz_st_pit_active_v1',
    'a0f12e75cc799ec636ffba3fac29ca894185d08f346422c8ea8f1a4778fb038a',
    'DEPLOYED_LEGACY_PENDING_MIGRATION',
    NULL
)
ON CONFLICT (authority_id, rule_version) DO NOTHING;

INSERT INTO market.stock_universe_pit_authority_pointer (
    authority_id, current_rule_version, current_rolling_key, activation_generation
) VALUES (
    'aistock_equity_pit_canonical',
    'st_pub_next_trade_restore_active_l_v1',
    'shsz_st_pit_active_v1',
    0
)
ON CONFLICT (authority_id) DO NOTHING;

INSERT INTO market.stock_universe_pit_authority_events (
    authority_id, event_type, before_generation, after_generation,
    before_rule_version, after_rule_version, before_rolling_key, after_rolling_key,
    operator_intent
)
SELECT
    'aistock_equity_pit_canonical', 'PREPARE', NULL, 0,
    NULL, 'st_pub_next_trade_restore_active_l_v1', NULL, 'shsz_st_pit_active_v1',
    'W1 registry installation; retain legacy v1 pending migration'
WHERE NOT EXISTS (
    SELECT 1 FROM market.stock_universe_pit_authority_events
     WHERE authority_id = 'aistock_equity_pit_canonical'
       AND event_type = 'PREPARE'
       AND after_generation = 0
);

DO $verify$
DECLARE
    versions_checks INTEGER;
    pointer_checks INTEGER;
    events_checks INTEGER;
BEGIN
    IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_pointer) <> 1 THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_POINTER_CARDINALITY';
    END IF;
    IF EXISTS (SELECT 1 FROM market.stock_universe_pit_authority_versions WHERE status = 'ACTIVE_CANONICAL') THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_MUST_NOT_ACTIVATE_V2';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM market.stock_universe_pit_authority_versions
         WHERE authority_id = 'aistock_equity_pit_canonical'
           AND rule_version = 'st_pub_next_trade_restore_active_l_v1'
           AND rolling_key = 'shsz_st_pit_active_v1'
           AND rule_parameters_digest = 'a0f12e75cc799ec636ffba3fac29ca894185d08f346422c8ea8f1a4778fb038a'
           AND status = 'DEPLOYED_LEGACY_PENDING_MIGRATION'
    ) THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_LEGACY_VERSION_DRIFT';
    END IF;
    SELECT COUNT(*) INTO versions_checks
      FROM pg_constraint
     WHERE conrelid = 'market.stock_universe_pit_authority_versions'::regclass
       AND contype = 'c';
    SELECT COUNT(*) INTO pointer_checks
      FROM pg_constraint
     WHERE conrelid = 'market.stock_universe_pit_authority_pointer'::regclass
       AND contype = 'c';
    SELECT COUNT(*) INTO events_checks
      FROM pg_constraint
     WHERE conrelid = 'market.stock_universe_pit_authority_events'::regclass
       AND contype = 'c';
    IF versions_checks < 5 OR pointer_checks < 4 OR events_checks < 7 THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_REQUIRED_CHECKS_MISSING: versions=% pointer=% events=%',
            versions_checks, pointer_checks, events_checks;
    END IF;
    IF (
        SELECT COUNT(*) FROM pg_constraint
         WHERE conrelid = 'market.stock_universe_pit_authority_versions'::regclass
           AND conname = ANY(ARRAY[
               'ck_pit_authority_versions_authority',
               'ck_pit_authority_versions_rule_digest',
               'ck_pit_authority_versions_status',
               'ck_pit_authority_versions_candidate_digest',
               'ck_pit_authority_versions_release_digest'
           ])
    ) <> 5 OR (
        SELECT COUNT(*) FROM pg_constraint
         WHERE conrelid = 'market.stock_universe_pit_authority_pointer'::regclass
           AND conname = ANY(ARRAY[
               'ck_pit_authority_pointer_authority',
               'ck_pit_authority_pointer_generation',
               'ck_pit_authority_pointer_envelope_digest',
               'ck_pit_authority_pointer_activation_evidence'
           ])
    ) <> 4 OR (
        SELECT COUNT(*) FROM pg_constraint
         WHERE conrelid = 'market.stock_universe_pit_authority_events'::regclass
           AND conname = ANY(ARRAY[
               'ck_pit_authority_events_authority',
               'ck_pit_authority_events_type',
               'ck_pit_authority_events_before_generation',
               'ck_pit_authority_events_after_generation',
               'ck_pit_authority_events_candidate_digest',
               'ck_pit_authority_events_envelope_digest',
               'ck_pit_authority_events_receipt_digest'
           ])
    ) <> 7 THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_NAMED_CHECKS_MISSING';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'market.stock_universe_pit_authority_pointer'::regclass
           AND conname = 'fk_pit_authority_pointer_version'
           AND contype = 'f'
    ) THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_POINTER_FK_MISSING';
    END IF;
    IF to_regclass('market.uq_stock_universe_pit_one_active_canonical') IS NULL
       OR to_regclass('market.uq_stock_universe_pit_authority_version_identity') IS NULL THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_REQUIRED_INDEX_MISSING';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
         WHERE tgrelid = 'market.stock_universe_pit_authority_events'::regclass
           AND tgname = 'trg_stock_universe_pit_authority_events_append_only'
           AND tgenabled = 'O'
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_FORWARD_APPEND_ONLY_TRIGGER_MISSING';
    END IF;
END
$verify$;

COMMIT;
