-- Read-only prerequisite check for the additive SIM-LR-B successor schema.
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET LOCAL statement_timeout = '30s';
DO $$
DECLARE
    table_name TEXT;
    missing_binding_columns TEXT[];
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'strategy_pkg.package',
        'strategy_pkg.strategy_runtime_release',
        'paper_v2.simulation_release_binding'
    ] LOOP
        IF to_regclass(table_name) IS NULL THEN
            RAISE EXCEPTION 'SIM-LR-B preflight prerequisite table % is absent', table_name;
        END IF;
    END LOOP;
    SELECT array_agg(required.name ORDER BY required.name)
      INTO missing_binding_columns
      FROM (VALUES ('account_group_id'), ('strategy_slot_id')) AS required(name)
     WHERE NOT EXISTS (
        SELECT 1
          FROM pg_attribute
         WHERE attrelid = 'paper_v2.simulation_release_binding'::regclass
           AND attname = required.name
           AND attnum > 0
           AND NOT attisdropped
     );
    IF missing_binding_columns IS NOT NULL THEN
        RAISE EXCEPTION
            'SIM-LR-B preflight requires add_simulation_runtime_account_slots_20260604.sql; missing columns %',
            missing_binding_columns;
    END IF;
END $$;
COMMIT;
