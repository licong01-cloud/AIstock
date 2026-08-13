DO $preflight$
DECLARE
    existing_count INTEGER;
BEGIN
    IF to_regclass('market.stock_universe_pit_state') IS NULL
       OR to_regclass('market.stock_universe_pit_spans') IS NULL THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_BASE_TABLES_MISSING';
    END IF;

    SELECT COUNT(*) INTO existing_count
      FROM (VALUES
          (to_regclass('market.stock_universe_pit_authority_versions')),
          (to_regclass('market.stock_universe_pit_authority_pointer')),
          (to_regclass('market.stock_universe_pit_authority_events'))
      ) AS registry_tables(regclass_value)
     WHERE regclass_value IS NOT NULL;
    IF existing_count NOT IN (0, 3) THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_PARTIAL_REGISTRY: %/3 tables exist', existing_count;
    END IF;

    IF existing_count = 3 THEN
        IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_pointer) <> 1
           OR NOT EXISTS (
            SELECT 1 FROM market.stock_universe_pit_authority_pointer
             WHERE authority_id = 'aistock_equity_pit_canonical'
               AND current_rule_version = 'st_pub_next_trade_restore_active_l_v1'
               AND current_rolling_key = 'shsz_st_pit_active_v1'
               AND activation_generation = 0
        ) THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_EXISTING_POINTER_DRIFT';
        END IF;
        IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_versions) <> 1
           OR NOT EXISTS (
               SELECT 1 FROM market.stock_universe_pit_authority_versions
                WHERE authority_id = 'aistock_equity_pit_canonical'
                  AND rule_version = 'st_pub_next_trade_restore_active_l_v1'
                  AND rolling_key = 'shsz_st_pit_active_v1'
                  AND rule_parameters_digest = 'a0f12e75cc799ec636ffba3fac29ca894185d08f346422c8ea8f1a4778fb038a'
                  AND status = 'DEPLOYED_LEGACY_PENDING_MIGRATION'
           ) THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_EXISTING_VERSION_DRIFT';
        END IF;
        IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_events) <> 1
           OR NOT EXISTS (
               SELECT 1 FROM market.stock_universe_pit_authority_events
                WHERE authority_id = 'aistock_equity_pit_canonical'
                  AND event_type = 'PREPARE'
                  AND before_generation IS NULL
                  AND after_generation = 0
                  AND after_rule_version = 'st_pub_next_trade_restore_active_l_v1'
                  AND after_rolling_key = 'shsz_st_pit_active_v1'
           ) THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_EXISTING_EVENT_DRIFT';
        END IF;
        IF EXISTS (
            SELECT 1 FROM market.stock_universe_pit_authority_versions WHERE status = 'ACTIVE_CANONICAL'
        ) THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_V2_ALREADY_ACTIVE';
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
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_REQUIRED_CHECKS_MISSING';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
             WHERE conrelid = 'market.stock_universe_pit_authority_pointer'::regclass
               AND conname = 'fk_pit_authority_pointer_version'
               AND contype = 'f'
        ) OR to_regclass('market.uq_stock_universe_pit_one_active_canonical') IS NULL
           OR to_regclass('market.uq_stock_universe_pit_authority_version_identity') IS NULL THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_REQUIRED_IDENTITY_CONSTRAINT_MISSING';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
             WHERE tgrelid = 'market.stock_universe_pit_authority_events'::regclass
               AND tgname = 'trg_stock_universe_pit_authority_events_append_only'
               AND tgenabled = 'O'
               AND NOT tgisinternal
        ) THEN
            RAISE EXCEPTION 'PIT_AUTHORITY_PREFLIGHT_APPEND_ONLY_TRIGGER_MISSING';
        END IF;
    END IF;
END
$preflight$;
