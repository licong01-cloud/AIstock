-- File-only delivery. DEV validation and production application require separate authorization.
-- Keep all installed validated predicates, including the two-feature version.
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
    FOR constraint_name, extra_predicate IN SELECT * FROM (VALUES
        ('ck_hmm_risk_rotation_l2_prediction_validation', $predicate$
            validation_basis = 'HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT'
            AND run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_price_supervised_v1'
        $predicate$),
        ('ck_hmm_risk_rotation_l2_prediction_run_summary', $predicate$
            COALESCE(jsonb_typeof(run_summary), '') = 'object'
            AND run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_price_supervised_v1'
            AND COALESCE(run_summary->>'tail_accessed', '') = 'false'
            AND COALESCE(run_summary->>'planned_fits', '') = '2'
            AND COALESCE(run_summary->>'completed_fits', '') = '2'
            AND COALESCE(run_summary->>'acceptance_sha256', '') ~ '^[0-9a-f]{64}$'
            AND COALESCE(jsonb_typeof(run_summary->'metrics'), '') = 'object'
            AND COALESCE(jsonb_typeof(run_summary->'parameters'), '') = 'object'
            AND run_summary->>'model_contract_hash' = '6412997efb07059adcb22b59b9e75e2fea94e0e142ae363694c5014cf1d81e47'
            AND run_summary->'parameters'->>'contract_hash' = run_summary->>'model_contract_hash'
            AND COALESCE(run_summary->'parameters'->>'parameter_sha256', '') ~ '^[0-9a-f]{64}$'
            AND run_summary->'parameters'->'feature_names' = '["moneyflow_level_rank","moneyflow_delta_rank","relative_momentum_rank","relative_downside_rank"]'::jsonb
            AND jsonb_array_length(run_summary->'parameters'->'coefficients') = 4
            AND COALESCE(jsonb_typeof(run_summary->'reference_pins'), '') = 'object'
        $predicate$),
        ('ck_hmm_risk_rotation_l2_prediction_availability', $predicate$
            (run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_price_supervised_v1'
            AND availability = 'available' AND structural_eligible AND feature_eligible
            AND rotation_score IS NOT NULL AND forecast_state IS NOT NULL AND reason_code IS NULL
            AND COALESCE(jsonb_typeof(feature_contributions), '') = 'object'
            AND feature_contributions ?& ARRAY['raw_prediction','intercept','moneyflow_level_linear_term',
                'moneyflow_delta_linear_term','relative_momentum_linear_term','relative_downside_linear_term',
                'average_rank_score','daily_rank_group','model_parameter_sha256']
            AND feature_contributions - ARRAY['raw_prediction','intercept','moneyflow_level_linear_term',
                'moneyflow_delta_linear_term','relative_momentum_linear_term','relative_downside_linear_term',
                'average_rank_score','daily_rank_group','model_parameter_sha256'] = '{}'::jsonb
            AND jsonb_typeof(feature_contributions->'raw_prediction') = 'number'
            AND jsonb_typeof(feature_contributions->'intercept') = 'number'
            AND jsonb_typeof(feature_contributions->'moneyflow_level_linear_term') = 'number'
            AND jsonb_typeof(feature_contributions->'moneyflow_delta_linear_term') = 'number'
            AND jsonb_typeof(feature_contributions->'relative_momentum_linear_term') = 'number'
            AND jsonb_typeof(feature_contributions->'relative_downside_linear_term') = 'number'
            AND feature_contributions->'average_rank_score' = to_jsonb(rotation_score)
            AND feature_contributions->>'daily_rank_group' = forecast_state
            AND feature_contributions->>'model_parameter_sha256' = run_summary->'parameters'->>'parameter_sha256')
            OR (run_summary->>'contract_version' = 'hmm_risk_rotation_l2_moneyflow_price_supervised_v1'
                AND availability = 'unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
                AND feature_contributions IS NULL AND NOT feature_eligible
                AND reason_code IS NOT NULL AND btrim(reason_code) <> '')
        $predicate$)
    ) AS changes(name, predicate)
    LOOP
        SELECT pg_get_constraintdef(c.oid), obj_description(c.oid, 'pg_constraint')
        INTO installed_definition, installed_comment FROM pg_constraint c
        WHERE c.conrelid = 'hmm_risk.rotation_l2_prediction'::regclass
          AND c.conname = constraint_name AND c.contype = 'c' AND c.convalidated;
        IF installed_definition IS NULL OR left(installed_definition, 6) <> 'CHECK ' THEN
            RAISE EXCEPTION 'required validated installed CHECK is absent: %', constraint_name;
        END IF;
        EXECUTE format('ALTER TABLE hmm_risk.rotation_l2_prediction DROP CONSTRAINT %I', constraint_name);
        EXECUTE format('ALTER TABLE hmm_risk.rotation_l2_prediction ADD CONSTRAINT %I CHECK (((%s) AND COALESCE(run_summary->>''contract_version'', '''') <> ''hmm_risk_rotation_l2_moneyflow_price_supervised_v1'') OR COALESCE((%s), FALSE))', constraint_name, substring(installed_definition FROM 7), extra_predicate);
        IF installed_comment IS NOT NULL THEN
            EXECUTE format('COMMENT ON CONSTRAINT %I ON hmm_risk.rotation_l2_prediction IS %L', constraint_name, installed_comment);
        END IF;
    END LOOP;
END;
$migration$;
COMMIT;
