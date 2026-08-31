BEGIN;

DO $preflight$
BEGIN
    IF to_regclass('market.dividend') IS NULL THEN
        RAISE EXCEPTION 'market.dividend is missing';
    END IF;

    IF EXISTS (SELECT 1 FROM market.dividend WHERE ann_date IS NULL) THEN
        RAISE EXCEPTION 'refusing rollback: market.dividend contains source rows with NULL ann_date';
    END IF;
END
$preflight$;

ALTER TABLE market.dividend
    DROP CONSTRAINT IF EXISTS market_dividend_source_identity_key;

ALTER TABLE market.dividend
    ALTER COLUMN ann_date SET NOT NULL;

DO $constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'market.dividend'::regclass
           AND contype = 'p'
    ) THEN
        ALTER TABLE market.dividend
            ADD CONSTRAINT dividend_pkey
            PRIMARY KEY (ts_code, end_date, ann_date, div_proc);
    END IF;
END
$constraint$;

COMMENT ON COLUMN market.dividend.ann_date IS
    'Plan announcement date supplied by Tushare; part of the decision-time knowledge contract';

DO $verify$
DECLARE
    primary_key_columns TEXT[];
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_attribute
         WHERE attrelid = 'market.dividend'::regclass
           AND attname = 'ann_date'
           AND attnotnull
    ) THEN
        RAISE EXCEPTION 'market.dividend.ann_date rollback did not restore NOT NULL';
    END IF;

    SELECT array_agg(att.attname ORDER BY key_columns.ordinality)
      INTO primary_key_columns
      FROM pg_constraint con
      JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
        ON TRUE
      JOIN pg_attribute att
        ON att.attrelid = con.conrelid AND att.attnum = key_columns.attnum
     WHERE con.conrelid = 'market.dividend'::regclass
       AND con.contype = 'p';

    IF primary_key_columns IS DISTINCT FROM ARRAY['ts_code', 'end_date', 'ann_date', 'div_proc']::TEXT[] THEN
        RAISE EXCEPTION 'market.dividend primary key rollback mismatch: %', primary_key_columns;
    END IF;
END
$verify$;

COMMIT;
