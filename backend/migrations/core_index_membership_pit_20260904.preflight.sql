DO $$
DECLARE
    actual_columns TEXT[];
    expected_columns CONSTANT TEXT[] := ARRAY[
        'effective_from',
        'effective_to_exclusive',
        'index_code',
        'pool_id',
        'source_provider',
        'source_reference',
        'ts_code',
        'updated_at'
    ];
BEGIN
    IF to_regclass('market.stock_universe_pit_spans') IS NULL THEN
        RAISE EXCEPTION 'required table market.stock_universe_pit_spans is missing';
    END IF;
    IF to_regclass('market.kline_daily_raw') IS NULL THEN
        RAISE EXCEPTION 'required table market.kline_daily_raw is missing';
    END IF;
    IF to_regclass('market.core_index_membership_pit') IS NULL THEN
        RETURN;
    END IF;

    SELECT array_agg(column_name ORDER BY column_name)
      INTO actual_columns
      FROM information_schema.columns
     WHERE table_schema = 'market'
       AND table_name = 'core_index_membership_pit';
    IF actual_columns IS DISTINCT FROM expected_columns THEN
        RAISE EXCEPTION 'existing market.core_index_membership_pit has unexpected columns: %', actual_columns;
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'market.core_index_membership_pit'::regclass
           AND contype = 'p'
    ) THEN
        RAISE EXCEPTION 'existing market.core_index_membership_pit is missing its primary key';
    END IF;
    IF NOT EXISTS (
        SELECT 1
          FROM pg_trigger
         WHERE tgrelid = 'market.core_index_membership_pit'::regclass
           AND tgname = 'trg_validate_core_index_membership_pit'
           AND NOT tgisinternal
    ) THEN
        RAISE EXCEPTION 'existing market.core_index_membership_pit is missing its overlap trigger';
    END IF;
END
$$;
