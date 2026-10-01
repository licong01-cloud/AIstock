BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_rotation_l2_prediction_v1'));

CREATE SCHEMA IF NOT EXISTS hmm_risk;

CREATE TABLE IF NOT EXISTS hmm_risk.rotation_l2_prediction (
    prediction_id UUID CONSTRAINT pk_hmm_risk_rotation_l2_prediction PRIMARY KEY,
    run_id CHAR(64) NOT NULL,
    model_hash CHAR(64) NOT NULL,
    evaluation_contract_hash CHAR(64) NOT NULL,
    input_hash CHAR(64) NOT NULL,
    mapping_hash CHAR(64) NOT NULL,
    quote_authority_hash CHAR(64) NOT NULL,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    sector_level TEXT NOT NULL,
    sector_code TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    rotation_score DOUBLE PRECISION,
    forecast_state TEXT,
    feature_contributions JSONB,
    availability TEXT NOT NULL,
    reason_code TEXT,
    structural_eligible BOOLEAN NOT NULL,
    feature_eligible BOOLEAN NOT NULL,
    outcome_status TEXT NOT NULL,
    execution_status TEXT NOT NULL,
    effect_status TEXT NOT NULL,
    research_surface_status TEXT NOT NULL,
    rotation_l2_capability_status TEXT NOT NULL,
    forward_power_status TEXT NOT NULL,
    forward_confirmation TEXT NOT NULL,
    advisory_status TEXT NOT NULL,
    validation_basis TEXT NOT NULL,
    run_summary JSONB NOT NULL,
    revision INTEGER NOT NULL,
    supersedes_prediction_id UUID CONSTRAINT fk_hmm_risk_rotation_l2_prediction_supersedes
      REFERENCES hmm_risk.rotation_l2_prediction(prediction_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_hmm_risk_rotation_l2_prediction_revision UNIQUE (run_id, trade_date, sector_code, revision),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_dates CHECK (as_of_date < trade_date),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_level CHECK (
        sector_level = 'L2' AND btrim(sector_code) <> '' AND btrim(sector_name) <> ''
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_state CHECK (forecast_state IS NULL OR forecast_state IN ('trending','neutral','fading')),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_availability CHECK (
        (availability = 'available' AND rotation_score IS NOT NULL AND forecast_state IS NOT NULL
          AND feature_contributions = jsonb_build_object('moneyflow_intensity_delta_5d_rank', rotation_score)
          AND reason_code IS NULL AND structural_eligible AND feature_eligible)
        OR
        (availability = 'unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
          AND feature_contributions IS NULL AND reason_code IS NOT NULL AND btrim(reason_code) <> ''
          AND NOT feature_eligible)
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_score CHECK (rotation_score IS NULL OR rotation_score BETWEEN -0.5 AND 0.5),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_surface CHECK (research_surface_status = 'NOT_AVAILABLE'),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_capability CHECK (
        rotation_l2_capability_status IN ('NOT_AVAILABLE','RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED')
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_forward CHECK (
        forward_power_status = 'UNAVAILABLE' AND forward_confirmation = 'NOT_STARTED'
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_advisory CHECK (advisory_status = 'NOT_AVAILABLE'),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_validation CHECK (validation_basis = 'HISTORICAL_CAUSAL_REPLAY_ZERO_FIT'),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_execution CHECK (execution_status = 'COMPLETED'),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_effect CHECK (
        effect_status IN ('DEVELOPMENT_EFFECT_QUALIFIED','BELOW_BINDING_MBE','EVIDENCE_INSUFFICIENT','NO_USABLE_PREDICTIONS')
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_status_coupling CHECK (
        (effect_status = 'DEVELOPMENT_EFFECT_QUALIFIED'
          AND rotation_l2_capability_status = 'RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED')
        OR
        (effect_status <> 'DEVELOPMENT_EFFECT_QUALIFIED' AND rotation_l2_capability_status = 'NOT_AVAILABLE')
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_outcome CHECK (
        outcome_status IN ('available','outcome_not_mature','outcome_unavailable_quote_discontinued','prediction_unavailable')
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_hashes CHECK (
        run_id ~ '^[0-9a-f]{64}$' AND model_hash ~ '^[0-9a-f]{64}$'
        AND evaluation_contract_hash ~ '^[0-9a-f]{64}$' AND input_hash ~ '^[0-9a-f]{64}$'
        AND mapping_hash ~ '^[0-9a-f]{64}$' AND quote_authority_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_revision CHECK (
        revision > 0 AND ((revision = 1 AND supersedes_prediction_id IS NULL) OR (revision > 1 AND supersedes_prediction_id IS NOT NULL))
    ),
    CONSTRAINT ck_hmm_risk_rotation_l2_prediction_run_summary CHECK (
        COALESCE(jsonb_typeof(run_summary), '') = 'object'
        AND COALESCE(run_summary->>'tail_accessed', '') = 'false'
        AND COALESCE(run_summary->>'planned_fits', '') = '0'
        AND COALESCE(run_summary->>'completed_fits', '') = '0'
        AND COALESCE(run_summary->>'acceptance_sha256', '') ~ '^[0-9a-f]{64}$'
        AND COALESCE(jsonb_typeof(run_summary->'metrics'), '') = 'object'
    )
);

CREATE INDEX IF NOT EXISTS idx_hmm_risk_rotation_l2_lookup
ON hmm_risk.rotation_l2_prediction (run_id, trade_date, sector_code, revision DESC);

COMMENT ON TABLE hmm_risk.rotation_l2_prediction IS
'Immutable SW L2 zero-fit rotation research predictions; product availability requires an external validation receipt.';

DO $$
DECLARE column_name TEXT;
DECLARE constraint_name TEXT;
BEGIN
    FOREACH column_name IN ARRAY ARRAY[
        'prediction_id','run_id','model_hash','evaluation_contract_hash','input_hash','mapping_hash',
        'quote_authority_hash','trade_date','as_of_date','sector_level','sector_code','sector_name',
        'rotation_score','forecast_state','feature_contributions','availability','reason_code','structural_eligible',
        'feature_eligible','outcome_status','execution_status','effect_status','research_surface_status',
        'rotation_l2_capability_status','forward_power_status','forward_confirmation','advisory_status',
        'validation_basis','run_summary','revision','supersedes_prediction_id','created_at'
    ] LOOP
        EXECUTE format(
            'COMMENT ON COLUMN hmm_risk.rotation_l2_prediction.%I IS %L',
            column_name,
            'rotation_l2_prediction.' || column_name || ' exact hmm_risk_rotation_l2_prediction_v1 contract'
        );
    END LOOP;
    FOREACH constraint_name IN ARRAY ARRAY[
        'pk_hmm_risk_rotation_l2_prediction','uq_hmm_risk_rotation_l2_prediction_revision',
        'fk_hmm_risk_rotation_l2_prediction_supersedes','ck_hmm_risk_rotation_l2_prediction_dates',
        'ck_hmm_risk_rotation_l2_prediction_level','ck_hmm_risk_rotation_l2_prediction_state',
        'ck_hmm_risk_rotation_l2_prediction_availability','ck_hmm_risk_rotation_l2_prediction_score',
        'ck_hmm_risk_rotation_l2_prediction_surface','ck_hmm_risk_rotation_l2_prediction_capability',
        'ck_hmm_risk_rotation_l2_prediction_forward','ck_hmm_risk_rotation_l2_prediction_advisory',
        'ck_hmm_risk_rotation_l2_prediction_validation','ck_hmm_risk_rotation_l2_prediction_execution',
        'ck_hmm_risk_rotation_l2_prediction_effect','ck_hmm_risk_rotation_l2_prediction_status_coupling',
        'ck_hmm_risk_rotation_l2_prediction_outcome','ck_hmm_risk_rotation_l2_prediction_hashes',
        'ck_hmm_risk_rotation_l2_prediction_revision','ck_hmm_risk_rotation_l2_prediction_run_summary'
    ] LOOP
        EXECUTE format(
            'COMMENT ON CONSTRAINT %I ON hmm_risk.rotation_l2_prediction IS %L',
            constraint_name,
            constraint_name || ' enforces hmm_risk_rotation_l2_prediction_v1'
        );
    END LOOP;
END $$;

COMMENT ON INDEX hmm_risk.idx_hmm_risk_rotation_l2_lookup IS
'Explicit run, date and SW L2 sector revision lookup.';

COMMIT;
