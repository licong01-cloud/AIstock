-- HMM-owned source migration only. DEV validation and target-specific DDL
-- authorization are separate steps; this file does not activate any run.
-- Requires the existing rotation_l2_prediction table. No parallel writer/table.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_rotation_l2_prediction_v1'));

ALTER TABLE hmm_risk.rotation_l2_prediction
    DROP CONSTRAINT ck_hmm_risk_rotation_l2_prediction_availability,
    DROP CONSTRAINT ck_hmm_risk_rotation_l2_prediction_validation,
    DROP CONSTRAINT ck_hmm_risk_rotation_l2_prediction_effect,
    DROP CONSTRAINT ck_hmm_risk_rotation_l2_prediction_status_coupling,
    DROP CONSTRAINT ck_hmm_risk_rotation_l2_prediction_outcome;

ALTER TABLE hmm_risk.rotation_l2_prediction
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_version CHECK (
        NOT (run_summary ? 'contract_version')
        OR COALESCE(run_summary->>'contract_version', '') = 'hmm_risk_l2_postcalibration_effect_v1'
    ),
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_availability CHECK (
        COALESCE(
            (availability = 'available' AND rotation_score IS NOT NULL AND forecast_state IS NOT NULL
             AND reason_code IS NULL AND structural_eligible AND feature_eligible AND (
                (NOT (run_summary ? 'contract_version')
                 AND feature_contributions = jsonb_build_object('moneyflow_intensity_delta_5d_rank', rotation_score))
                OR
                (run_summary->>'contract_version' = 'hmm_risk_l2_postcalibration_effect_v1'
                 AND jsonb_typeof(feature_contributions) = 'object'
                 AND feature_contributions ?& ARRAY['hard_state','frozen_utility_mean','semantic_state',
                                                    'average_rank_score','daily_rank_group','model_parameter_sha256']
                 AND feature_contributions - ARRAY['hard_state','frozen_utility_mean','semantic_state',
                                                   'average_rank_score','daily_rank_group','model_parameter_sha256'] = '{}'::jsonb
                 AND feature_contributions->'hard_state' IN ('0'::jsonb,'1'::jsonb,'2'::jsonb)
                 AND jsonb_typeof(feature_contributions->'frozen_utility_mean') = 'number'
                 AND feature_contributions->>'semantic_state' = forecast_state
                 AND feature_contributions->'average_rank_score' = to_jsonb(rotation_score)
                 AND feature_contributions->>'daily_rank_group' IN ('trending','neutral','fading')
                 AND feature_contributions->>'model_parameter_sha256' ~ '^[0-9a-f]{64}$')
             ))
            OR
            (availability = 'unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
             AND feature_contributions IS NULL AND reason_code IS NOT NULL AND btrim(reason_code) <> ''
             AND (NOT feature_eligible OR run_summary->>'contract_version' = 'hmm_risk_l2_postcalibration_effect_v1')),
            FALSE
        )
    ),
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_validation CHECK (
        (NOT (run_summary ? 'contract_version') AND validation_basis = 'HISTORICAL_CAUSAL_REPLAY_ZERO_FIT')
        OR
        (run_summary->>'contract_version' = 'hmm_risk_l2_postcalibration_effect_v1'
         AND validation_basis = 'POST_CALIBRATION_RETROSPECTIVE_DEVELOPMENT'
         AND evaluation_contract_hash = '9f8f93e997334fae45580678f2984462d392878bc8cf50904c3888b45e591121'
         AND COALESCE(run_summary->>'semantic_mapping_sha256', '') ~ '^[0-9a-f]{64}$')
    ),
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_effect CHECK (
        (NOT (run_summary ? 'contract_version')
         AND effect_status IN ('DEVELOPMENT_EFFECT_QUALIFIED','BELOW_BINDING_MBE','EVIDENCE_INSUFFICIENT','NO_USABLE_PREDICTIONS'))
        OR
        (run_summary->>'contract_version' = 'hmm_risk_l2_postcalibration_effect_v1'
         AND effect_status IN ('DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED','BELOW_BINDING_MBE','EVIDENCE_INSUFFICIENT'))
    ),
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_status_coupling CHECK (
        (effect_status IN ('DEVELOPMENT_EFFECT_QUALIFIED','DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED')
         AND rotation_l2_capability_status = 'RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED')
        OR
        (effect_status NOT IN ('DEVELOPMENT_EFFECT_QUALIFIED','DEVELOPMENT_EFFECT_REACHED_FORWARD_UNCONFIRMED')
         AND rotation_l2_capability_status = 'NOT_AVAILABLE')
    ),
    ADD CONSTRAINT ck_hmm_risk_rotation_l2_prediction_outcome CHECK (
        (NOT (run_summary ? 'contract_version')
         AND outcome_status IN ('available','outcome_not_mature','outcome_unavailable_quote_discontinued','prediction_unavailable'))
        OR
        (run_summary->>'contract_version' = 'hmm_risk_l2_postcalibration_effect_v1'
         AND outcome_status IN ('available','outcome_not_mature','outcome_legal_na','prediction_unavailable'))
    );

COMMENT ON CONSTRAINT ck_hmm_risk_rotation_l2_prediction_version ON hmm_risk.rotation_l2_prediction IS
'Explicit HMM effect version; absent version retains the unchanged delta contract. Unknown versions fail closed.';
COMMIT;

-- Rollback must first prove there are zero rows with contract_version present,
-- then restore the five exact original constraints from the source migration
-- backend/db/migrations/create_hmm_risk_rotation_l2_prediction_20260922.sql.
-- Never delete or reinterpret effect rows to force rollback.
