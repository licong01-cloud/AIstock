BEGIN;

DO $rollback$
BEGIN
    IF to_regclass('market.dividend') IS NOT NULL
       AND EXISTS (SELECT 1 FROM market.dividend LIMIT 1) THEN
        RAISE EXCEPTION 'refusing to drop non-empty market.dividend';
    END IF;
END
$rollback$;

DROP TABLE IF EXISTS market.dividend;

COMMIT;
