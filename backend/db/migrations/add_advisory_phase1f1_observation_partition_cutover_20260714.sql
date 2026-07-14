DO $$
DECLARE
    actual_lineage_rows BIGINT;
    actual_candidate_rows BIGINT;
    actual_legacy_months TEXT;
    expected_legacy_months TEXT := current_setting('app.phase1f1_legacy_months', true);
    expected_target_months TEXT := current_setting('app.phase1f1_target_months', true);
BEGIN
    IF current_setting('app.phase1f1_legacy_inventory_hash', true) IS NULL
       OR current_setting('app.phase1f1_lineage_row_count', true) IS NULL
       OR current_setting('app.phase1f1_candidate_row_count', true) IS NULL
       OR expected_legacy_months IS NULL
       OR expected_target_months IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_PREDECESSOR_SCHEMA_INVALID';
    END IF;

    IF (SELECT relkind FROM pg_catalog.pg_class WHERE oid = 'app.advisory_signal_observation_lineage'::regclass) <> 'r'
       OR (SELECT relkind FROM pg_catalog.pg_class WHERE oid = 'app.advisory_signal_stage_candidate'::regclass) <> 'r' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_PREDECESSOR_SCHEMA_INVALID';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM app.advisory_signal_observation_lineage lineage
          LEFT JOIN app.advisory_signal_observation_version observation_version
            ON observation_version.observation_version_id = lineage.observation_version_id
          LEFT JOIN app.advisory_signal_observation observation
            ON observation.canonical_signal_id = observation_version.canonical_signal_id
         WHERE observation.decision_as_of_trade_date IS NULL
    ) OR EXISTS (
        SELECT 1
          FROM app.advisory_signal_stage_candidate candidate
          LEFT JOIN app.advisory_signal_stage_evidence stage_evidence
            ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
          LEFT JOIN app.advisory_signal_observation_version observation_version
            ON observation_version.observation_version_id = stage_evidence.observation_version_id
          LEFT JOIN app.advisory_signal_observation observation
            ON observation.canonical_signal_id = observation_version.canonical_signal_id
         WHERE observation.decision_as_of_trade_date IS NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_PARENT_DATE_UNRESOLVED';
    END IF;

    SELECT count(*) INTO actual_lineage_rows
      FROM app.advisory_signal_observation_lineage;
    SELECT count(*) INTO actual_candidate_rows
      FROM app.advisory_signal_stage_candidate;
    SELECT COALESCE(string_agg(DISTINCT to_char(date_trunc('month', value.decision_as_of_trade_date), 'YYYY-MM-DD'), ',' ORDER BY to_char(date_trunc('month', value.decision_as_of_trade_date), 'YYYY-MM-DD')), '')
      INTO actual_legacy_months
      FROM (
          SELECT observation.decision_as_of_trade_date
            FROM app.advisory_signal_observation_lineage lineage
            JOIN app.advisory_signal_observation_version observation_version
              ON observation_version.observation_version_id = lineage.observation_version_id
            JOIN app.advisory_signal_observation observation
              ON observation.canonical_signal_id = observation_version.canonical_signal_id
          UNION
          SELECT observation.decision_as_of_trade_date
            FROM app.advisory_signal_stage_candidate candidate
            JOIN app.advisory_signal_stage_evidence stage_evidence
              ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
            JOIN app.advisory_signal_observation_version observation_version
              ON observation_version.observation_version_id = stage_evidence.observation_version_id
            JOIN app.advisory_signal_observation observation
              ON observation.canonical_signal_id = observation_version.canonical_signal_id
      ) AS value;

    IF actual_lineage_rows <> current_setting('app.phase1f1_lineage_row_count')::bigint
       OR actual_candidate_rows <> current_setting('app.phase1f1_candidate_row_count')::bigint
       OR actual_legacy_months <> expected_legacy_months THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_COPY_MISMATCH';
    END IF;

    IF EXISTS (
        WITH expected_months AS (
            SELECT unnest(
                CASE WHEN expected_target_months = '' THEN ARRAY[]::date[]
                     ELSE string_to_array(expected_target_months, ',')::date[] END
            ) AS month_start
        ), required_partition AS (
            SELECT parent_relation, child_prefix
              FROM (VALUES
                  ('advisory_signal_observation_lineage_payload'::text, 'advisory_signal_observation_lineage_payload'::text),
                  ('advisory_signal_stage_candidate_payload'::text, 'advisory_signal_stage_candidate_payload'::text),
                  ('advisory_outcome_label_payload'::text, 'advisory_outcome_label_payload'::text)
              ) AS value(parent_relation, child_prefix)
        )
        SELECT 1
          FROM expected_months
          CROSS JOIN required_partition
          LEFT JOIN pg_catalog.pg_class child
            ON child.oid = to_regclass(
                'app.' || required_partition.child_prefix || '_' || to_char(expected_months.month_start, 'YYYYMM')
            )
          LEFT JOIN pg_catalog.pg_inherits inheritance
            ON inheritance.inhrelid = child.oid
          LEFT JOIN pg_catalog.pg_class parent
            ON parent.oid = inheritance.inhparent
          LEFT JOIN pg_catalog.pg_namespace parent_schema
            ON parent_schema.oid = parent.relnamespace
         WHERE child.oid IS NULL
            OR parent_schema.nspname IS DISTINCT FROM 'app'
            OR parent.relname IS DISTINCT FROM required_partition.parent_relation
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_PARTITION_MISSING';
    END IF;

    IF EXISTS (SELECT 1 FROM app.advisory_signal_observation_lineage_identity)
       OR EXISTS (SELECT 1 FROM app.advisory_signal_observation_lineage_payload)
       OR EXISTS (SELECT 1 FROM app.advisory_signal_stage_candidate_identity)
       OR EXISTS (SELECT 1 FROM app.advisory_signal_stage_candidate_payload) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_COPY_MISMATCH';
    END IF;

    INSERT INTO app.advisory_signal_observation_lineage_identity (
        lineage_id,
        decision_as_of_trade_date,
        observation_version_id,
        phase0a_audit_id,
        admission_scope_id,
        program_id,
        binding_version_id,
        lineage_source_type,
        source_run_id,
        lineage_content_hash,
        created_at
    )
    SELECT lineage.lineage_id,
           observation.decision_as_of_trade_date,
           lineage.observation_version_id,
           lineage.phase0a_audit_id,
           lineage.admission_scope_id,
           lineage.program_id,
           lineage.binding_version_id,
           lineage.lineage_source_type,
           lineage.source_run_id,
           lineage.lineage_content_hash,
           lineage.created_at
      FROM app.advisory_signal_observation_lineage lineage
      JOIN app.advisory_signal_observation_version observation_version
        ON observation_version.observation_version_id = lineage.observation_version_id
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     ORDER BY lineage.lineage_id;

    INSERT INTO app.advisory_signal_observation_lineage_payload (
        decision_as_of_trade_date,
        lineage_id,
        canonical_signal_id,
        phase0a_audit_manifest_hash,
        handoff_readiness_hash,
        admission_scope_hash,
        audit_target_id,
        target_scope_hash,
        capability,
        stable_signal_semantics_hash,
        canonical_signal_scope_hash,
        phase0a_signal_context_hash,
        oos_interval_id,
        oos_interval_hash,
        evidence_scope,
        signal_evidence_level,
        effective_cutoff_date,
        review_run_id,
        list_version_id
    )
    SELECT observation.decision_as_of_trade_date,
           lineage.lineage_id,
           lineage.canonical_signal_id,
           lineage.phase0a_audit_manifest_hash,
           lineage.handoff_readiness_hash,
           lineage.admission_scope_hash,
           lineage.audit_target_id,
           lineage.target_scope_hash,
           lineage.capability,
           lineage.stable_signal_semantics_hash,
           lineage.canonical_signal_scope_hash,
           lineage.phase0a_signal_context_hash,
           lineage.oos_interval_id,
           lineage.oos_interval_hash,
           lineage.evidence_scope,
           lineage.signal_evidence_level,
           lineage.effective_cutoff_date,
           lineage.review_run_id,
           lineage.list_version_id
      FROM app.advisory_signal_observation_lineage lineage
      JOIN app.advisory_signal_observation_version observation_version
        ON observation_version.observation_version_id = lineage.observation_version_id
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     ORDER BY lineage.lineage_id;

    INSERT INTO app.advisory_signal_stage_candidate_identity (
        stage_evidence_id,
        symbol,
        decision_as_of_trade_date,
        registered_at
    )
    SELECT candidate.stage_evidence_id,
           candidate.symbol,
           observation.decision_as_of_trade_date,
           candidate.created_at
      FROM app.advisory_signal_stage_candidate candidate
      JOIN app.advisory_signal_stage_evidence stage_evidence
        ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
      JOIN app.advisory_signal_observation_version observation_version
        ON observation_version.observation_version_id = stage_evidence.observation_version_id
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     ORDER BY candidate.stage_evidence_id, candidate.symbol;

    INSERT INTO app.advisory_signal_stage_candidate_payload (
        decision_as_of_trade_date,
        stage_evidence_id,
        symbol,
        membership_status,
        rank,
        score_decimal,
        input_rank,
        input_score_decimal,
        exclusion_reason_code,
        component_capability,
        component_evidence_schema_version,
        component_evidence_json,
        component_evidence_hash,
        component_reason_codes,
        candidate_content_hash,
        created_at
    )
    SELECT observation.decision_as_of_trade_date,
           candidate.stage_evidence_id,
           candidate.symbol,
           candidate.membership_status,
           candidate.rank,
           candidate.score_decimal,
           candidate.input_rank,
           candidate.input_score_decimal,
           candidate.exclusion_reason_code,
           candidate.component_capability,
           candidate.component_evidence_schema_version,
           candidate.component_evidence_json,
           candidate.component_evidence_hash,
           candidate.component_reason_codes,
           candidate.candidate_content_hash,
           candidate.created_at
      FROM app.advisory_signal_stage_candidate candidate
      JOIN app.advisory_signal_stage_evidence stage_evidence
        ON stage_evidence.stage_evidence_id = candidate.stage_evidence_id
      JOIN app.advisory_signal_observation_version observation_version
        ON observation_version.observation_version_id = stage_evidence.observation_version_id
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     ORDER BY candidate.stage_evidence_id, candidate.symbol;

    IF EXISTS (
        (
            SELECT lineage_id, canonical_signal_id, observation_version_id, phase0a_audit_id,
                   phase0a_audit_manifest_hash, handoff_readiness_hash, admission_scope_id,
                   admission_scope_hash, audit_target_id, target_scope_hash, capability,
                   stable_signal_semantics_hash, canonical_signal_scope_hash, phase0a_signal_context_hash,
                   oos_interval_id, oos_interval_hash, evidence_scope, signal_evidence_level,
                   effective_cutoff_date, program_id, binding_version_id, lineage_source_type,
                   source_run_id, review_run_id, list_version_id, lineage_content_hash, created_at
              FROM app.advisory_signal_observation_lineage
            EXCEPT ALL
            SELECT identity.lineage_id, payload.canonical_signal_id, identity.observation_version_id,
                   identity.phase0a_audit_id, payload.phase0a_audit_manifest_hash,
                   payload.handoff_readiness_hash, identity.admission_scope_id,
                   payload.admission_scope_hash, payload.audit_target_id, payload.target_scope_hash,
                   payload.capability, payload.stable_signal_semantics_hash,
                   payload.canonical_signal_scope_hash, payload.phase0a_signal_context_hash,
                   payload.oos_interval_id, payload.oos_interval_hash, payload.evidence_scope,
                   payload.signal_evidence_level, payload.effective_cutoff_date, identity.program_id,
                   identity.binding_version_id, identity.lineage_source_type, identity.source_run_id,
                   payload.review_run_id, payload.list_version_id, identity.lineage_content_hash,
                   identity.created_at
              FROM app.advisory_signal_observation_lineage_identity identity
              JOIN app.advisory_signal_observation_lineage_payload payload
                ON payload.lineage_id = identity.lineage_id
               AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date
        )
        UNION ALL
        (
            SELECT identity.lineage_id, payload.canonical_signal_id, identity.observation_version_id,
                   identity.phase0a_audit_id, payload.phase0a_audit_manifest_hash,
                   payload.handoff_readiness_hash, identity.admission_scope_id,
                   payload.admission_scope_hash, payload.audit_target_id, payload.target_scope_hash,
                   payload.capability, payload.stable_signal_semantics_hash,
                   payload.canonical_signal_scope_hash, payload.phase0a_signal_context_hash,
                   payload.oos_interval_id, payload.oos_interval_hash, payload.evidence_scope,
                   payload.signal_evidence_level, payload.effective_cutoff_date, identity.program_id,
                   identity.binding_version_id, identity.lineage_source_type, identity.source_run_id,
                   payload.review_run_id, payload.list_version_id, identity.lineage_content_hash,
                   identity.created_at
              FROM app.advisory_signal_observation_lineage_identity identity
              JOIN app.advisory_signal_observation_lineage_payload payload
                ON payload.lineage_id = identity.lineage_id
               AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date
            EXCEPT ALL
            SELECT lineage_id, canonical_signal_id, observation_version_id, phase0a_audit_id,
                   phase0a_audit_manifest_hash, handoff_readiness_hash, admission_scope_id,
                   admission_scope_hash, audit_target_id, target_scope_hash, capability,
                   stable_signal_semantics_hash, canonical_signal_scope_hash, phase0a_signal_context_hash,
                   oos_interval_id, oos_interval_hash, evidence_scope, signal_evidence_level,
                   effective_cutoff_date, program_id, binding_version_id, lineage_source_type,
                   source_run_id, review_run_id, list_version_id, lineage_content_hash, created_at
              FROM app.advisory_signal_observation_lineage
        )
    ) OR EXISTS (
        (
            SELECT stage_evidence_id, symbol, membership_status, rank, score_decimal, input_rank,
                   input_score_decimal, exclusion_reason_code, component_capability,
                   component_evidence_schema_version, component_evidence_json, component_evidence_hash,
                   component_reason_codes, candidate_content_hash, created_at
              FROM app.advisory_signal_stage_candidate
            EXCEPT ALL
            SELECT identity.stage_evidence_id, identity.symbol, payload.membership_status,
                   payload.rank, payload.score_decimal, payload.input_rank, payload.input_score_decimal,
                   payload.exclusion_reason_code, payload.component_capability,
                   payload.component_evidence_schema_version, payload.component_evidence_json,
                   payload.component_evidence_hash, payload.component_reason_codes,
                   payload.candidate_content_hash, payload.created_at
              FROM app.advisory_signal_stage_candidate_identity identity
              JOIN app.advisory_signal_stage_candidate_payload payload
                ON payload.stage_evidence_id = identity.stage_evidence_id
               AND payload.symbol = identity.symbol
               AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date
        )
        UNION ALL
        (
            SELECT identity.stage_evidence_id, identity.symbol, payload.membership_status,
                   payload.rank, payload.score_decimal, payload.input_rank, payload.input_score_decimal,
                   payload.exclusion_reason_code, payload.component_capability,
                   payload.component_evidence_schema_version, payload.component_evidence_json,
                   payload.component_evidence_hash, payload.component_reason_codes,
                   payload.candidate_content_hash, payload.created_at
              FROM app.advisory_signal_stage_candidate_identity identity
              JOIN app.advisory_signal_stage_candidate_payload payload
                ON payload.stage_evidence_id = identity.stage_evidence_id
               AND payload.symbol = identity.symbol
               AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date
            EXCEPT ALL
            SELECT stage_evidence_id, symbol, membership_status, rank, score_decimal, input_rank,
                   input_score_decimal, exclusion_reason_code, component_capability,
                   component_evidence_schema_version, component_evidence_json, component_evidence_hash,
                   component_reason_codes, candidate_content_hash, created_at
              FROM app.advisory_signal_stage_candidate
        )
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_COPY_MISMATCH';
    END IF;

    IF (SELECT count(*) FROM app.advisory_signal_observation_lineage_identity) <> actual_lineage_rows
       OR (SELECT count(*) FROM app.advisory_signal_observation_lineage_payload) <> actual_lineage_rows
       OR (SELECT count(*) FROM app.advisory_signal_stage_candidate_identity) <> actual_candidate_rows
       OR (SELECT count(*) FROM app.advisory_signal_stage_candidate_payload) <> actual_candidate_rows THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_COPY_MISMATCH';
    END IF;

    DROP TABLE app.advisory_signal_stage_candidate;
    DROP TABLE app.advisory_signal_observation_lineage;

    ALTER TABLE app.advisory_signal_stage_evidence
        DROP CONSTRAINT IF EXISTS advisory_signal_stage_evidence_content_hash_key;
    CREATE INDEX IF NOT EXISTS idx_adv_p1f1_stage_evidence_content_hash
        ON app.advisory_signal_stage_evidence (content_hash);

    CREATE VIEW app.advisory_signal_observation_lineage AS
    SELECT identity.lineage_id,
           payload.canonical_signal_id,
           identity.observation_version_id,
           identity.phase0a_audit_id,
           payload.phase0a_audit_manifest_hash,
           payload.handoff_readiness_hash,
           identity.admission_scope_id,
           payload.admission_scope_hash,
           payload.audit_target_id,
           payload.target_scope_hash,
           payload.capability,
           payload.stable_signal_semantics_hash,
           payload.canonical_signal_scope_hash,
           payload.phase0a_signal_context_hash,
           payload.oos_interval_id,
           payload.oos_interval_hash,
           payload.evidence_scope,
           payload.signal_evidence_level,
           payload.effective_cutoff_date,
           identity.program_id,
           identity.binding_version_id,
           identity.lineage_source_type,
           identity.source_run_id,
           payload.review_run_id,
           payload.list_version_id,
           identity.lineage_content_hash,
           identity.created_at
      FROM app.advisory_signal_observation_lineage_identity identity
      JOIN app.advisory_signal_observation_lineage_payload payload
        ON payload.lineage_id = identity.lineage_id
       AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date;

    CREATE VIEW app.advisory_signal_stage_candidate AS
    SELECT identity.stage_evidence_id,
           identity.symbol,
           payload.membership_status,
           payload.rank,
           payload.score_decimal,
           payload.input_rank,
           payload.input_score_decimal,
           payload.exclusion_reason_code,
           payload.component_capability,
           payload.component_evidence_schema_version,
           payload.component_evidence_json,
           payload.component_evidence_hash,
           payload.component_reason_codes,
           payload.candidate_content_hash,
           payload.created_at
      FROM app.advisory_signal_stage_candidate_identity identity
      JOIN app.advisory_signal_stage_candidate_payload payload
        ON payload.stage_evidence_id = identity.stage_evidence_id
       AND payload.symbol = identity.symbol
       AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date;

    COMMENT ON VIEW app.advisory_signal_observation_lineage IS
        'Read-only Phase 1F.1 compatibility projection with the exact v1 lineage column contract.';
    COMMENT ON VIEW app.advisory_signal_stage_candidate IS
        'Read-only Phase 1F.1 compatibility projection with the exact v1 stage-candidate column contract.';
END;
$$;
