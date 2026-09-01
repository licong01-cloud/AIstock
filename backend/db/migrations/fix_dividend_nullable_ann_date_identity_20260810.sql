BEGIN;

DO $preflight$
BEGIN
    IF to_regclass('market.dividend') IS NULL THEN
        RAISE EXCEPTION 'market.dividend is missing; apply add_advisory_price_range_dividend_20260810.sql first';
    END IF;

    IF current_setting('server_version_num')::INTEGER < 150000 THEN
        RAISE EXCEPTION 'market.dividend nullable identity requires PostgreSQL 15 or newer';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM market.dividend
         GROUP BY ts_code, end_date, ann_date, div_proc
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'market.dividend contains duplicate source identities';
    END IF;
END
$preflight$;

ALTER TABLE market.dividend
    DROP CONSTRAINT IF EXISTS dividend_pkey;

ALTER TABLE market.dividend
    ALTER COLUMN ann_date DROP NOT NULL;

DO $constraint$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'market.dividend'::regclass
           AND conname = 'market_dividend_source_identity_key'
    ) THEN
        ALTER TABLE market.dividend
            ADD CONSTRAINT market_dividend_source_identity_key
            UNIQUE NULLS NOT DISTINCT (ts_code, end_date, ann_date, div_proc);
    END IF;
END
$constraint$;

COMMENT ON COLUMN market.dividend.ann_date IS
    'Nullable plan announcement date supplied by Tushare; NULL is preserved and never replaced with the implementation announcement date';

DO $verify$
DECLARE
    identity_columns TEXT[];
    identity_nulls_not_distinct BOOLEAN;
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_attribute
         WHERE attrelid = 'market.dividend'::regclass
           AND attname = 'ann_date'
           AND attnotnull
    ) THEN
        RAISE EXCEPTION 'market.dividend.ann_date is still NOT NULL';
    END IF;

    SELECT array_agg(att.attname ORDER BY key_columns.ordinality),
           BOOL_AND(idx.indnullsnotdistinct)
      INTO identity_columns, identity_nulls_not_distinct
      FROM pg_constraint con
      JOIN pg_index idx
        ON idx.indexrelid = con.conindid
      JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
        ON TRUE
      JOIN pg_attribute att
        ON att.attrelid = con.conrelid AND att.attnum = key_columns.attnum
     WHERE con.conrelid = 'market.dividend'::regclass
       AND con.conname = 'market_dividend_source_identity_key'
       AND con.contype = 'u';

    IF identity_columns IS DISTINCT FROM ARRAY['ts_code', 'end_date', 'ann_date', 'div_proc']::TEXT[]
       OR identity_nulls_not_distinct IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION
            'market.dividend nullable identity mismatch: columns=%, nulls_not_distinct=%',
            identity_columns,
            identity_nulls_not_distinct;
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'market.dividend'::regclass
           AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'market.dividend must not retain a primary key that rejects nullable ann_date';
    END IF;
END
$verify$;

COMMIT;
