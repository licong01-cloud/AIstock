-- HMM-owned tables only. Source preparation is not authorization to execute.
-- Validate on existing aistock_dev before a separately authorized production run.
BEGIN;
SET LOCAL lock_timeout = '5s';

-- PostgreSQL names this multi-column inline CHECK independently of the column
-- declaration. Discover it by its exact historical contract, never by name.
DO $migration$
DECLARE
    capability_check text;
    matches integer;
    original_contract boolean;
    expected_predicate constant text :=
      '(((effect_status = ''DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED''::text) '
      || 'AND (risk_l2_capability_status = ''RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED''::text)) '
      || 'OR ((effect_status <> ''DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED''::text) '
      || 'AND (risk_l2_capability_status = ''NOT_AVAILABLE''::text)))';
BEGIN
    -- Retain the ALTER's table lock while discovering and replacing the check.
    LOCK TABLE hmm_risk.risk_l2_run IN ACCESS EXCLUSIVE MODE;
    SELECT count(*), min(c.conname::text),
           bool_and(c.convalidated AND pg_get_expr(c.conbin, c.conrelid) = expected_predicate)
      INTO matches, capability_check, original_contract
      FROM pg_constraint c
     WHERE c.conrelid = 'hmm_risk.risk_l2_run'::regclass AND c.contype = 'c'
       AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
              FROM pg_attribute a
             WHERE a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey))
           = ARRAY['effect_status', 'risk_l2_capability_status'];
    IF matches <> 1 OR original_contract IS DISTINCT FROM true THEN
        RAISE EXCEPTION 'Expected exactly one validated original HMM risk capability constraint, found %', matches;
    END IF;
    EXECUTE format('ALTER TABLE hmm_risk.risk_l2_run DROP CONSTRAINT %I', capability_check);
END
$migration$;

ALTER TABLE hmm_risk.risk_l2_run
    DROP CONSTRAINT risk_l2_run_validation_basis_check,
    DROP CONSTRAINT risk_l2_run_dates_check,
    DROP CONSTRAINT risk_l2_run_expected_row_count_check,
    DROP CONSTRAINT risk_l2_run_effect_status_check,
    ADD CONSTRAINT risk_l2_run_dates_array_v2 CHECK
      (jsonb_typeof(dates) = 'array' AND jsonb_array_length(dates) > 0),
    ADD CONSTRAINT risk_l2_run_basis_contract_v2 CHECK (
      (validation_basis = 'HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT'
       AND jsonb_array_length(dates) = 424 AND expected_row_count = 55544
       AND effect_status IN ('DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED','BELOW_BINDING_RISK_MBE','EVIDENCE_INSUFFICIENT')
       AND ((effect_status = 'DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED'
             AND risk_l2_capability_status = 'RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED')
            OR (effect_status <> 'DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED'
                AND risk_l2_capability_status = 'NOT_AVAILABLE')))
      OR
      (validation_basis = 'CAUSAL_DATE_INFERENCE_NO_OUTCOMES'
       AND expected_row_count = 131 * jsonb_array_length(dates)
       AND effect_status = 'NOT_EVALUATED_FOR_INFERENCE_DATES'
       AND risk_l2_capability_status IN ('NOT_AVAILABLE','RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED'))
    );

-- The original unnamed outcome check gets an engine-generated name. Resolve
-- just that check by its attribute and literals, refusing ambiguous schemas.
DO $migration$
DECLARE
    outcome_check text;
    matches integer;
BEGIN
    SELECT count(*), min(c.conname) INTO matches, outcome_check
      FROM pg_constraint c
      JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
     WHERE c.conrelid = 'hmm_risk.risk_l2_prediction'::regclass
       AND c.contype = 'c' AND a.attname = 'outcome_status'
       AND pg_get_constraintdef(c.oid) LIKE '%OUTCOME_LEGAL_NA%'
       AND pg_get_constraintdef(c.oid) LIKE '%OUTCOME_NOT_MATURE%'
       AND pg_get_constraintdef(c.oid) NOT LIKE '%OUTCOME_NOT_EVALUATED%';
    IF matches <> 1 THEN
        RAISE EXCEPTION 'Expected exactly one original HMM risk outcome constraint, found %', matches;
    END IF;
    EXECUTE format('ALTER TABLE hmm_risk.risk_l2_prediction DROP CONSTRAINT %I', outcome_check);
END
$migration$;

ALTER TABLE hmm_risk.risk_l2_prediction ADD CONSTRAINT risk_l2_prediction_outcome_v2 CHECK (
    (outcome_status = 'AVAILABLE' AND event IS NOT NULL AND event IN (0,1)
       AND realized_drawdown IS NOT NULL AND realized_drawdown >= -1 AND realized_drawdown <= 0
       AND realized_return IS NOT NULL AND realized_return > -1 AND realized_return < 'Infinity'::double precision
       AND event = CASE WHEN realized_drawdown <= -0.08 THEN 1 ELSE 0 END)
    OR (outcome_status IN ('OUTCOME_LEGAL_NA','OUTCOME_NOT_MATURE','OUTCOME_NOT_EVALUATED')
       AND event IS NULL AND realized_drawdown IS NULL AND realized_return IS NULL)
);
COMMIT;
