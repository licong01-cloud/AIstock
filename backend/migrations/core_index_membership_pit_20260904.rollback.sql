BEGIN;

DO $$
BEGIN
    IF to_regclass('market.core_index_membership_pit') IS NOT NULL
       AND EXISTS (SELECT 1 FROM market.core_index_membership_pit) THEN
        RAISE EXCEPTION 'refusing rollback: market.core_index_membership_pit contains data';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS trg_validate_core_index_membership_pit
    ON market.core_index_membership_pit;
DROP TABLE IF EXISTS market.core_index_membership_pit;
DROP FUNCTION IF EXISTS market.validate_core_index_membership_pit();

COMMIT;
