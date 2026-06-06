ALTER TABLE paper_v2.broker_account_binding
    ADD COLUMN IF NOT EXISTS account_group_id TEXT,
    ADD COLUMN IF NOT EXISTS strategy_slot_id TEXT;

COMMENT ON COLUMN paper_v2.broker_account_binding.account_group_id IS 'MiniQMT shared simulation account group id for Paper v2 account_group_slots auto-run bindings.';
COMMENT ON COLUMN paper_v2.broker_account_binding.strategy_slot_id IS 'Paper v2 strategy slot id under the MiniQMT account group; defaults to portfolio_id for N=1/N>1 compatibility.';
COMMENT ON COLUMN paper_v2.broker_account_binding.allocation_mode IS 'Capital/allocation isolation mode. account_group_slots allows multiple Paper v2 strategy slots under one MiniQMT SIM account; exclusive_account remains legacy-only.';

UPDATE paper_v2.broker_account_binding
SET
    account_group_id = COALESCE(
        NULLIF(account_group_id, ''),
        'ag_minqmt_' || COALESCE(NULLIF(btrim(regexp_replace(broker_account_id, '[^A-Za-z0-9_]+', '_', 'g'), '_'), ''), 'unassigned') || '_sim'
    ),
    strategy_slot_id = COALESCE(NULLIF(strategy_slot_id, ''), portfolio_id)
WHERE broker_backend = 'minqmt_sim'
  AND allocation_mode = 'account_group_slots'
  AND (NULLIF(account_group_id, '') IS NULL OR NULLIF(strategy_slot_id, '') IS NULL);

DROP INDEX IF EXISTS paper_v2.idx_paper_v2_broker_account_binding_active_account;

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_broker_account_binding_active_legacy_account
    ON paper_v2.broker_account_binding(broker_backend, broker_mode, broker_account_id)
    WHERE binding_status = 'ACTIVE' AND allocation_mode <> 'account_group_slots';

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_broker_account_binding_active_account_slot
    ON paper_v2.broker_account_binding(broker_backend, broker_mode, broker_account_id, account_group_id, strategy_slot_id)
    WHERE binding_status = 'ACTIVE' AND allocation_mode = 'account_group_slots';

CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_v2_broker_account_binding_active_portfolio
    ON paper_v2.broker_account_binding(portfolio_id)
    WHERE binding_status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_paper_v2_broker_account_binding_account_group
    ON paper_v2.broker_account_binding(broker_backend, broker_mode, broker_account_id, account_group_id, updated_at DESC)
    WHERE binding_status = 'ACTIVE' AND allocation_mode = 'account_group_slots';
