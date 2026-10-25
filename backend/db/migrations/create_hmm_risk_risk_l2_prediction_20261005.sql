-- HMM-owned immutable historical risk only. Apply to authorized DEV before production.
BEGIN;
SET LOCAL lock_timeout = '5s';
CREATE SCHEMA IF NOT EXISTS hmm_risk;
CREATE TABLE IF NOT EXISTS hmm_risk.risk_l2_run (
    run_id char(64) PRIMARY KEY CHECK (run_id ~ '^[0-9a-f]{64}$'),
    acceptance_hash char(64) NOT NULL CHECK (acceptance_hash = run_id),
    sealed_prediction_hash char(64) NOT NULL CHECK (sealed_prediction_hash ~ '^[0-9a-f]{64}$'),
    feature_hash char(64) NOT NULL CHECK (feature_hash ~ '^[0-9a-f]{64}$'),
    model_hash char(64) NOT NULL CHECK (model_hash ~ '^[0-9a-f]{64}$'),
    contract_hash char(64) NOT NULL CHECK (contract_hash ~ '^[0-9a-f]{64}$'),
    input_hash char(64) NOT NULL CHECK (input_hash ~ '^[0-9a-f]{64}$'),
    mapping_hash char(64) NOT NULL CHECK (mapping_hash ~ '^[0-9a-f]{64}$'),
    executor_commit char(40) NOT NULL CHECK (executor_commit ~ '^[0-9a-f]{40}$'),
    model_version text NOT NULL CHECK (model_version = 'hmm_risk_l2_absolute_drawdown_logistic_v1'),
    validation_basis text NOT NULL CHECK (validation_basis = 'HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT'),
    catalog jsonb NOT NULL CHECK (jsonb_typeof(catalog) = 'array' AND jsonb_array_length(catalog) = 131),
    dates jsonb NOT NULL CHECK (jsonb_typeof(dates) = 'array' AND jsonb_array_length(dates) = 424),
    input_identity jsonb NOT NULL CHECK (jsonb_typeof(input_identity) = 'object'),
    compact_summary jsonb NOT NULL CHECK (jsonb_typeof(compact_summary) = 'object'),
    expected_row_count integer NOT NULL CHECK (expected_row_count = 55544),
    effect_status text NOT NULL CHECK (effect_status IN ('DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED','BELOW_BINDING_RISK_MBE','EVIDENCE_INSUFFICIENT')),
    risk_l2_capability_status text NOT NULL CHECK (
      (effect_status = 'DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED' AND risk_l2_capability_status = 'RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED') OR
      (effect_status <> 'DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED' AND risk_l2_capability_status = 'NOT_AVAILABLE')),
    forward_power_status text NOT NULL CHECK (forward_power_status = 'UNAVAILABLE'),
    forward_confirmation text NOT NULL CHECK (forward_confirmation = 'NOT_STARTED'),
    advisory_status text NOT NULL CHECK (advisory_status = 'NOT_AVAILABLE'),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS hmm_risk.risk_l2_prediction (
    run_id char(64) NOT NULL REFERENCES hmm_risk.risk_l2_run(run_id) ON DELETE RESTRICT,
    trade_date date NOT NULL,
    as_of_date date NOT NULL CHECK (as_of_date < trade_date),
    sector_level text NOT NULL CHECK (sector_level = 'L2'),
    sector_code text NOT NULL CHECK (sector_code ~ '^[0-9]{6}\.SI$'),
    sector_name text CHECK (sector_name IS NULL),
    name_authority text NOT NULL CHECK (name_authority = 'CANONICAL_CODE_ONLY'),
    probability double precision,
    warning boolean,
    availability text NOT NULL,
    reason_code text,
    structural_eligible boolean NOT NULL,
    outcome_status text NOT NULL,
    event integer,
    realized_drawdown double precision,
    realized_return double precision,
    PRIMARY KEY (run_id,trade_date,sector_code),
    CHECK ((availability = 'available' AND probability IS NOT NULL AND probability >= 0 AND probability <= 1
       AND warning IS NOT NULL AND warning = (probability >= 0.20) AND reason_code IS NULL AND structural_eligible) OR
      (availability = 'unavailable' AND probability IS NULL AND warning IS NULL AND reason_code IS NOT NULL AND length(trim(reason_code)) > 0)),
    CHECK ((outcome_status = 'AVAILABLE' AND event IS NOT NULL AND event IN (0,1)
       AND realized_drawdown IS NOT NULL AND realized_drawdown >= -1 AND realized_drawdown <= 0
       AND realized_return IS NOT NULL AND realized_return > -1 AND realized_return < 'Infinity'::double precision
       AND event = CASE WHEN realized_drawdown <= -0.08 THEN 1 ELSE 0 END) OR
      (outcome_status IN ('OUTCOME_LEGAL_NA','OUTCOME_NOT_MATURE') AND event IS NULL AND realized_drawdown IS NULL AND realized_return IS NULL))
);
COMMIT;
