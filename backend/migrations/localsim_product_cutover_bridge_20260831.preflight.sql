-- Read-only preflight for the SIM-LR-C neutral ledger-scope bridge.
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET LOCAL statement_timeout = '30s';
DO $$
DECLARE
    table_name TEXT;
    runtime_fk_count INTEGER;
    orphan_count BIGINT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'paper_v2.simulation_account_v1',
        'strategy_pkg.package',
        'paper_v2.portfolio',
        'paper_v2.run',
        'paper_v2.intraday_snapshots'
    ] LOOP
        IF to_regclass(table_name) IS NULL THEN
            RAISE EXCEPTION 'SIM-LR-C ledger-scope preflight prerequisite % is absent', table_name;
        END IF;
    END LOOP;
    SELECT count(*) INTO runtime_fk_count
    FROM pg_constraint
    WHERE contype = 'f'
      AND conrelid IN ('paper_v2.run'::regclass, 'paper_v2.intraday_snapshots'::regclass)
      AND confrelid IN (
          'paper_v2.portfolio'::regclass,
          COALESCE(to_regclass('paper_v2.simulation_ledger_scope_v1'), 'paper_v2.portfolio'::regclass)
      );
    IF runtime_fk_count <> 2 THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope preflight expected exactly 2 runtime FKs, found %', runtime_fk_count;
    END IF;
    IF to_regclass('paper_v2.simulation_ledger_scope_v1') IS NULL THEN
        SELECT count(*) INTO orphan_count
        FROM (
            SELECT run.portfolio_id AS ledger_scope_id FROM paper_v2.run AS run
            UNION
            SELECT snapshot.portfolio_id FROM paper_v2.intraday_snapshots AS snapshot
        ) AS referenced
        LEFT JOIN paper_v2.portfolio AS portfolio
          ON portfolio.portfolio_id = referenced.ledger_scope_id
        WHERE portfolio.portfolio_id IS NULL;
    ELSE
        EXECUTE $query$
            SELECT count(*)
            FROM (
                SELECT run.portfolio_id AS ledger_scope_id FROM paper_v2.run AS run
                UNION
                SELECT snapshot.portfolio_id FROM paper_v2.intraday_snapshots AS snapshot
            ) AS referenced
            LEFT JOIN paper_v2.simulation_ledger_scope_v1 AS scope
              ON scope.ledger_scope_id = referenced.ledger_scope_id
            WHERE scope.ledger_scope_id IS NULL
        $query$ INTO orphan_count;
    END IF;
    IF orphan_count <> 0 THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope preflight found % orphan runtime scopes', orphan_count;
    END IF;
END $$;
COMMIT;
