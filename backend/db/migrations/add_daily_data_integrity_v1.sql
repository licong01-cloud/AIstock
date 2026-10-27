-- Additive operator migration. Validate in the existing DEV database first.
-- No market facts, existing provider verdicts or service configuration changed.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='60s';
ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS data_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS validated_revision bigint,
    ADD COLUMN IF NOT EXISTS validation_status text NOT NULL DEFAULT 'UNVERIFIED',
    ADD COLUMN IF NOT EXISTS validation_id text,
    ADD COLUMN IF NOT EXISTS scope_version text,
    ADD COLUMN IF NOT EXISTS rule_version text;

CREATE INDEX IF NOT EXISTS idx_dataset_integrity_canonical_date
    ON market.dataset_date_refresh_audit(trade_date, dataset)
    WHERE data_source = 'canonical_db';

CREATE TABLE IF NOT EXISTS market.data_daily_integrity_revisions (
    fact_date date NOT NULL,
    contract_version text NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    daily_integrity_id text NOT NULL UNIQUE,
    as_of timestamptz NOT NULL,
    state text NOT NULL CHECK (state IN ('UNVERIFIED','IN_PROGRESS','PARTIAL','ERROR','COMPLETE')),
    all_sync_complete boolean NOT NULL,
    completion_kind text,
    observed_on_time boolean,
    content_sha256 text NOT NULL,
    summary jsonb NOT NULL,
    PRIMARY KEY (fact_date, contract_version, revision)
);
COMMENT ON TABLE market.data_daily_integrity_revisions IS
    'Rebuildable versioned fact-date integrity projection; canonical partition revisions remain authoritative. No market bar copies.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.data_revision IS
    'Canonical physical partition revision. Incremented atomically with actual sanctioned fact changes; provider rows remain observations.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.validated_revision IS
    'Revision independently validated with scope_version/rule_version; NULL or unequal means not certified.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.validation_id IS
    'Idempotent content identity of canonical validation, not a manual authorization token.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.validation_status IS
    'Typed partition verdict for the validated revision; dirty/current status must additionally compare revisions and scope/rule.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.scope_version IS
    'Versioned fact scope, independent of consumer PIT buying eligibility.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.rule_version IS
    'Actual validator contract version; a different rule does not inherit old green status.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.fact_date IS 'Date of facts, not the ingestion completion date.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.contract_version IS 'Shared aggregation contract version.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.revision IS 'Monotonic daily projection revision, incremented only for changed content.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.daily_integrity_id IS 'Immutable daily projection content identity, not an approval.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.as_of IS 'Actual projection observation timestamp; never backdated.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.state IS 'Day state derived from all formal obligations and effective partition verdicts.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.all_sync_complete IS 'All obligations complete at this projection; current reads must recheck the revision watermark.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.completion_kind IS 'ON_TIME, RECOVERED, HISTORICAL_VERIFIED or SOURCE_REVISED; NULL when incomplete.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.observed_on_time IS 'Whether timely completeness is actually proven; NULL for unknown historical timing.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.content_sha256 IS 'Canonical summary content digest for idempotent publication.';
COMMENT ON COLUMN market.data_daily_integrity_revisions.summary IS 'Small obligation, partition reference and count projection; no raw market bars.';

-- Called ONCE per changed dataset/date in the same transaction as fact DML.
-- No row trigger, full-table scan, per-security query or extra queue.
CREATE OR REPLACE FUNCTION market.mark_data_integrity_changed(
    p_dataset text, p_fact_date date, p_reason text, p_source text
) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
    v_revision bigint;
    v_hash text;
    v_due timestamptz;
BEGIN
    IF p_dataset IS NULL OR p_dataset !~ '^[a-z][a-z0-9_]*$'
       OR p_fact_date IS NULL OR NULLIF(p_reason, '') IS NULL OR NULLIF(p_source, '') IS NULL THEN
        RAISE EXCEPTION 'invalid integrity change identity';
    END IF;
    INSERT INTO market.dataset_date_refresh_audit AS a
        (dataset,trade_date,data_source,status,row_count,quality_status,data_revision,metadata)
    VALUES (p_dataset,p_fact_date,'canonical_db','failed',0,'unproven',1,
        jsonb_build_object('change_reason',p_reason,'last_writer',p_source))
    ON CONFLICT (dataset,trade_date,data_source) DO UPDATE SET
        data_revision=a.data_revision+1, validation_status='UNVERIFIED',
        status='failed', quality_status='unproven', refreshed_at=clock_timestamp(),
        metadata=a.metadata || jsonb_build_object('change_reason',p_reason,'last_writer',p_source)
    RETURNING data_revision INTO v_revision;

    -- Coalesce high-frequency raw-minute batch commits. Dirtiness is already
    -- visible above; this is ONLY a short maintenance debounce, not a consumer
    -- publication/approval gate. No full-day scan while each stock is arriving.
    v_due := clock_timestamp() + CASE WHEN p_dataset='kline_minute_raw'
        THEN interval '30 seconds' ELSE interval '0 seconds' END;

    v_hash := encode(sha256(convert_to(p_dataset || E'\n' || p_fact_date::text
        || E'\nVALIDATE_ONLY\nphysical_partition_v1','UTF8')), 'hex');
    INSERT INTO market.data_sync_targets AS t
        (target_id,dataset,data_source,target_date,target_scope,target_key_sha256,
         target_status,priority,next_retry_at,metadata)
    VALUES ('dst_' || substring(v_hash,1,24),p_dataset,'canonical_db',p_fact_date,
        '{"operation":"VALIDATE_ONLY","scope":"physical_partition_v1"}'::jsonb,v_hash,
        'pending',50,v_due,jsonb_build_object('desired_revision',v_revision))
    ON CONFLICT (dataset,data_source,target_key_sha256) DO UPDATE SET
        target_status='pending', next_retry_at=v_due, reconciled_at=NULL,
        metadata=(t.metadata - 'lease_token' - 'lease_until') || EXCLUDED.metadata,
        updated_at=clock_timestamp();
    RETURN v_revision;
END;
$$;
COMMENT ON FUNCTION market.mark_data_integrity_changed(text,date,text,text) IS
    'Atomic sanctioned-writer protocol. Call only for actual changes, inside caller transaction. Raw untracked bypass SQL is not certified.';
COMMIT;
