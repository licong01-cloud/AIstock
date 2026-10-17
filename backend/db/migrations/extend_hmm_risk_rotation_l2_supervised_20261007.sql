-- Apply only after separately authorized existing-DEV validation and target approval.
-- Preserve each installed legacy predicate; add one exact supervised-version branch.
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_rotation_l2_prediction_v1'));

DO $migration$
DECLARE
    constraint_name TEXT;
    installed_definition TEXT;
    installed_comment TEXT;
    extra_predicate TEXT;
BEGIN
    IF to_regclass('hmm_risk.rotation_l2_prediction') IS NULL THEN
        RAISE EXCEPTION 'existing HMM L2 prediction table is required';
    END IF;
    FOR constraint_name, extra_predicate IN
        SELECT * FROM (VALUES
            ('ck_hmm_risk_rotation_l2_prediction_validation', $predicate$
                validation_basis = 'HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT'
                AND run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_supervised_v1'
            $predicate$),
            ('ck_hmm_risk_rotation_l2_prediction_run_summary', $predicate$
                COALESCE(jsonb_typeof(run_summary), '') = 'object'
                AND run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_supervised_v1'
                AND COALESCE(run_summary->>'tail_accessed', '') = 'false'
                AND COALESCE(run_summary->>'planned_fits', '') = '2'
                AND COALESCE(run_summary->>'completed_fits', '') = '2'
                AND COALESCE(run_summary->>'acceptance_sha256', '') ~ '^[0-9a-f]{64}$'
                AND COALESCE(jsonb_typeof(run_summary->'metrics'), '') = 'object'
                AND COALESCE(jsonb_typeof(run_summary->'parameters'), '') = 'object'
                AND run_summary->>'model_contract_hash' = '6f7b6593b77219919bb46b8ef8f5bf2c7e2391a55de9ebf52628794b88ef5e19'
                AND run_summary->'parameters'->>'contract_hash' = run_summary->>'model_contract_hash'
                AND COALESCE(run_summary->'parameters'->>'parameter_sha256', '') ~ '^[0-9a-f]{64}$'
            $predicate$),
            ('ck_hmm_risk_rotation_l2_prediction_availability', $predicate$
                (availability = 'available' AND structural_eligible AND feature_eligible
                AND rotation_score IS NOT NULL AND forecast_state IS NOT NULL AND reason_code IS NULL
                AND run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_supervised_v1'
                AND COALESCE(jsonb_typeof(feature_contributions), '') = 'object'
                AND feature_contributions ?& ARRAY['raw_prediction', 'intercept',
                    'moneyflow_level_linear_term', 'moneyflow_delta_linear_term',
                    'average_rank_score', 'daily_rank_group', 'model_parameter_sha256']
                AND feature_contributions - ARRAY['raw_prediction', 'intercept',
                    'moneyflow_level_linear_term', 'moneyflow_delta_linear_term',
                    'average_rank_score', 'daily_rank_group', 'model_parameter_sha256'] = '{}'::jsonb
                AND jsonb_typeof(feature_contributions->'raw_prediction') = 'number'
                AND jsonb_typeof(feature_contributions->'intercept') = 'number'
                AND jsonb_typeof(feature_contributions->'moneyflow_level_linear_term') = 'number'
                AND jsonb_typeof(feature_contributions->'moneyflow_delta_linear_term') = 'number'
                AND feature_contributions->'average_rank_score' = to_jsonb(rotation_score)
                AND feature_contributions->>'daily_rank_group' = forecast_state
                AND feature_contributions->>'model_parameter_sha256' = run_summary->'parameters'->>'parameter_sha256')
                OR (run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_supervised_v1'
                    AND availability = 'unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
                    AND feature_contributions IS NULL AND reason_code IS NOT NULL AND btrim(reason_code) <> ''
                    AND NOT feature_eligible)
            $predicate$)
        ) AS changes(name, predicate)
    LOOP
        SELECT pg_get_constraintdef(c.oid), obj_description(c.oid, 'pg_constraint')
        INTO installed_definition, installed_comment
        FROM pg_constraint c
        WHERE c.conrelid = 'hmm_risk.rotation_l2_prediction'::regclass
          AND c.conname = constraint_name AND c.contype = 'c' AND c.convalidated;
        IF installed_definition IS NULL OR left(installed_definition, 6) <> 'CHECK ' THEN
            RAISE EXCEPTION 'required validated installed CHECK is absent: %', constraint_name;
        END IF;
        EXECUTE format('ALTER TABLE hmm_risk.rotation_l2_prediction DROP CONSTRAINT %I', constraint_name);
        EXECUTE format('ALTER TABLE hmm_risk.rotation_l2_prediction ADD CONSTRAINT %I CHECK (((%s) AND COALESCE(run_summary->>''contract_version'', '''') <> ''hmm_risk_rotation_l2_moneyflow_supervised_v1'') OR COALESCE((%s), FALSE))',
            constraint_name, substring(installed_definition FROM 7), extra_predicate);
        IF installed_comment IS NOT NULL THEN
            EXECUTE format('COMMENT ON CONSTRAINT %I ON hmm_risk.rotation_l2_prediction IS %L',
                constraint_name, installed_comment);
        END IF;
    END LOOP;
END;
$migration$;
COMMIT;
