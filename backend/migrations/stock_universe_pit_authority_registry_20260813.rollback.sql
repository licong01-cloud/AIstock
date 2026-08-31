BEGIN;

DO $rollback_guard$
BEGIN
    IF to_regclass('market.stock_universe_pit_authority_versions') IS NULL
       AND to_regclass('market.stock_universe_pit_authority_pointer') IS NULL
       AND to_regclass('market.stock_universe_pit_authority_events') IS NULL THEN
        RETURN;
    END IF;
    IF to_regclass('market.stock_universe_pit_authority_versions') IS NULL
       OR to_regclass('market.stock_universe_pit_authority_pointer') IS NULL
       OR to_regclass('market.stock_universe_pit_authority_events') IS NULL THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_ROLLBACK_PARTIAL_REGISTRY';
    END IF;
    IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_pointer) <> 1
       OR NOT EXISTS (
           SELECT 1 FROM market.stock_universe_pit_authority_pointer
            WHERE authority_id = 'aistock_equity_pit_canonical'
              AND current_rule_version = 'st_pub_next_trade_restore_active_l_v1'
              AND current_rolling_key = 'shsz_st_pit_active_v1'
              AND activation_generation = 0
              AND activation_envelope_digest IS NULL
              AND expected_source_commit IS NULL
       ) THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_ROLLBACK_REFUSED_POINTER_CHANGED';
    END IF;
    IF (SELECT COUNT(*) FROM market.stock_universe_pit_authority_versions) <> 1
       OR (SELECT COUNT(*) FROM market.stock_universe_pit_authority_events) <> 1 THEN
        RAISE EXCEPTION 'PIT_AUTHORITY_ROLLBACK_REFUSED_HISTORY_EXISTS';
    END IF;
END
$rollback_guard$;

DROP TABLE IF EXISTS market.stock_universe_pit_authority_pointer;
DROP TABLE IF EXISTS market.stock_universe_pit_authority_events;
DROP TABLE IF EXISTS market.stock_universe_pit_authority_versions;
DROP FUNCTION IF EXISTS market.reject_stock_universe_pit_authority_event_mutation();

COMMIT;
