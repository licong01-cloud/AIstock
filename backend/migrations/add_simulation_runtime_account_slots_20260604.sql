BEGIN;

ALTER TABLE paper_v2.simulation_release_binding
    ADD COLUMN IF NOT EXISTS account_group_id TEXT,
    ADD COLUMN IF NOT EXISTS strategy_slot_id TEXT;

ALTER TABLE paper_v2.simulation_daily_run
    ADD COLUMN IF NOT EXISTS account_group_id TEXT,
    ADD COLUMN IF NOT EXISTS strategy_slot_id TEXT;

COMMENT ON COLUMN paper_v2.simulation_release_binding.account_group_id IS 'MiniQMT shared simulation account group identifier used to separate multiple strategy slots under one broker account.';
COMMENT ON COLUMN paper_v2.simulation_release_binding.strategy_slot_id IS 'Stable MiniQMT simulation strategy slot identifier used for order, fill, lot, PnL, and status attribution.';
COMMENT ON COLUMN paper_v2.simulation_daily_run.account_group_id IS 'Account group id copied from SimulationReleaseBinding at daily-run creation for restart recovery and operator status APIs.';
COMMENT ON COLUMN paper_v2.simulation_daily_run.strategy_slot_id IS 'Strategy slot id copied from SimulationReleaseBinding at daily-run creation for restart recovery and operator status APIs.';

UPDATE paper_v2.simulation_release_binding
SET
    account_group_id = COALESCE(
        NULLIF(account_group_id, ''),
        'ag_minqmt_' || regexp_replace(COALESCE(NULLIF(broker_account_id, ''), 'unassigned'), '[^A-Za-z0-9_]+', '_', 'g') || '_sim'
    ),
    strategy_slot_id = COALESCE(
        NULLIF(strategy_slot_id, ''),
        COALESCE(NULLIF(strategy_name, ''), strategy_id)
    ),
    updated_at = NOW()
WHERE broker_backend = 'minqmt_sim'
  AND (NULLIF(account_group_id, '') IS NULL OR NULLIF(strategy_slot_id, '') IS NULL);

UPDATE paper_v2.simulation_daily_run AS run
SET
    account_group_id = COALESCE(NULLIF(run.account_group_id, ''), binding.account_group_id),
    strategy_slot_id = COALESCE(NULLIF(run.strategy_slot_id, ''), binding.strategy_slot_id),
    run_payload_json = run.run_payload_json
        || jsonb_strip_nulls(
            jsonb_build_object(
                'account_group_id', COALESCE(NULLIF(run.account_group_id, ''), binding.account_group_id),
                'strategy_slot_id', COALESCE(NULLIF(run.strategy_slot_id, ''), binding.strategy_slot_id)
            )
        ),
    updated_at = NOW()
FROM paper_v2.simulation_release_binding AS binding
WHERE run.binding_id = binding.binding_id
  AND (NULLIF(run.account_group_id, '') IS NULL OR NULLIF(run.strategy_slot_id, '') IS NULL)
  AND (binding.account_group_id IS NOT NULL OR binding.strategy_slot_id IS NOT NULL);

CREATE INDEX IF NOT EXISTS idx_paper_v2_simulation_release_binding_account_slot
    ON paper_v2.simulation_release_binding(broker_backend, account_group_id, strategy_slot_id, approval_state)
    WHERE account_group_id IS NOT NULL OR strategy_slot_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_paper_v2_simulation_daily_run_account_slot_date
    ON paper_v2.simulation_daily_run(broker_backend, account_group_id, strategy_slot_id, trade_date DESC)
    WHERE account_group_id IS NOT NULL OR strategy_slot_id IS NOT NULL;

COMMIT;
