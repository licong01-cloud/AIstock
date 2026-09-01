-- SIM-LR-B/PR-B2 additive LocalSIM successor account, lineage, and replay schema.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, paper_v2, strategy_pkg, pg_temp;

CREATE TABLE IF NOT EXISTS paper_v2.simulation_account_v1 (
    account_id TEXT PRIMARY KEY,
    account_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL CHECK (schema_version = 'simulation_account_v1'),
    account_name TEXT NOT NULL CHECK (btrim(account_name) <> ''),
    broker_backend TEXT NOT NULL CHECK (broker_backend = 'local_sim'),
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    admission_receipt_id TEXT NOT NULL,
    initial_capital NUMERIC(20, 6) NOT NULL CHECK (initial_capital > 0),
    account_config_json JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'RETIRED')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.legacy_localsim_account_lineage_v1 (
    lineage_id TEXT PRIMARY KEY,
    lineage_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL DEFAULT 'legacy_localsim_account_lineage_v1'
        CHECK (schema_version = 'legacy_localsim_account_lineage_v1'),
    legacy_account_id TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL UNIQUE REFERENCES paper_v2.simulation_account_v1(account_id),
    release_id TEXT NOT NULL REFERENCES strategy_pkg.strategy_runtime_release(release_id),
    binding_id TEXT NOT NULL REFERENCES paper_v2.simulation_release_binding(binding_id),
    ledger_scope_id TEXT NOT NULL UNIQUE,
    economic_facts_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('PREPARED', 'ACTIVATION_PENDING_SAFE_BOUNDARY', 'ACTIVE')
    ),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.localsim_replay_job_v1 (
    replay_job_id TEXT PRIMARY KEY,
    replay_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL DEFAULT 'localsim_replay_job_v1'
        CHECK (schema_version = 'localsim_replay_job_v1'),
    simulation_account_id TEXT NOT NULL REFERENCES paper_v2.simulation_account_v1(account_id),
    release_id TEXT NOT NULL REFERENCES strategy_pkg.strategy_runtime_release(release_id),
    binding_id TEXT NOT NULL REFERENCES paper_v2.simulation_release_binding(binding_id),
    day_engine_contract_id TEXT NOT NULL DEFAULT 'simulation_daily_engine_v1',
    start_trade_date DATE NOT NULL,
    end_trade_date DATE NOT NULL,
    historical_source_id TEXT NOT NULL,
    historical_source_sha256 TEXT NOT NULL,
    calendar_snapshot_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'CREATED', 'RUNNING_HISTORICAL', 'CAUGHT_UP', 'READY_FOR_LIVE',
        'ACTIVATION_PENDING_SAFE_BOUNDARY', 'LIVE_ACTIVE',
        'FAILED_RETRYABLE', 'FAILED_TERMINAL', 'CANCELLED'
    )),
    next_trade_date DATE,
    completed_trade_date DATE,
    live_release_id TEXT REFERENCES strategy_pkg.strategy_runtime_release(release_id),
    live_binding_id TEXT REFERENCES paper_v2.simulation_release_binding(binding_id),
    activation_trade_date DATE,
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    failure_code TEXT,
    failure_context JSONB,
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_localsim_replay_day_engine_contract CHECK (
        day_engine_contract_id = 'simulation_daily_engine_v1'
    ),
    CONSTRAINT ck_localsim_replay_date_window CHECK (end_trade_date >= start_trade_date),
    CONSTRAINT ck_localsim_replay_live_pair CHECK (
        (live_release_id IS NULL AND live_binding_id IS NULL)
        OR (live_release_id IS NOT NULL AND live_binding_id IS NOT NULL)
    )
);

ALTER TABLE paper_v2.localsim_replay_job_v1
    ADD COLUMN IF NOT EXISTS day_engine_contract_id TEXT NOT NULL DEFAULT 'simulation_daily_engine_v1';
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'paper_v2.localsim_replay_job_v1'::regclass
          AND conname = 'ck_localsim_replay_day_engine_contract'
    ) THEN
        ALTER TABLE paper_v2.localsim_replay_job_v1
            ADD CONSTRAINT ck_localsim_replay_day_engine_contract
            CHECK (day_engine_contract_id = 'simulation_daily_engine_v1');
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_localsim_successor_open_binding
    ON paper_v2.simulation_release_binding (
        (binding_config_json->'metadata'->>'localsim_account_id')
    )
    WHERE broker_backend = 'local_sim'
      AND effective_to IS NULL
      AND binding_config_json->'metadata'->>'localsim_account_id' IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_localsim_replay_status_cursor
    ON paper_v2.localsim_replay_job_v1(status, next_trade_date, updated_at, replay_job_id);
CREATE INDEX IF NOT EXISTS idx_localsim_lineage_status
    ON paper_v2.legacy_localsim_account_lineage_v1(status, updated_at, lineage_id);

COMMENT ON TABLE paper_v2.simulation_account_v1 IS 'Logical LocalSIM SimulationAccountV1; lifecycle is CAS-versioned and the effective release is resolved through immutable bindings.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.account_id IS 'Content-addressed LocalSIM logical account identifier.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.account_hash IS 'Canonical hash of account_config_json and immutable account identity fields.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.schema_version IS 'Simulation account contract version; fixed to simulation_account_v1.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.account_name IS 'Immutable user-facing account name.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.broker_backend IS 'Broker backend; fixed to local_sim for this successor account contract.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.package_id IS 'Immutable StrategyPackage alpha-core identity.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.manifest_sha256 IS 'Immutable StrategyPackage manifest hash.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.admission_receipt_id IS 'Admission receipt proving the alpha core was accepted before account creation.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.initial_capital IS 'Initial account capital; mutable cash and positions remain economic-ledger facts.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.account_config_json IS 'Canonical immutable account identity payload; it contains no mutable release or policy snapshot.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.status IS 'CAS lifecycle state ACTIVE, PAUSED, or RETIRED.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.version IS 'Monotonic CAS version for lifecycle transitions.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.created_by IS 'Actor that created the account.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.created_at IS 'Account creation timestamp.';
COMMENT ON COLUMN paper_v2.simulation_account_v1.updated_at IS 'Last successful lifecycle transition timestamp.';

COMMENT ON TABLE paper_v2.legacy_localsim_account_lineage_v1 IS 'One-to-one retained legacy LocalSIM identity mapping; economic rows are referenced and never copied.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.lineage_id IS 'Content-addressed lineage identifier.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.lineage_hash IS 'Canonical hash of all immutable lineage identities and the economic-facts digest.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.schema_version IS 'Lineage contract version.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.legacy_account_id IS 'Retained old LocalSIM account or portfolio identity.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.account_id IS 'Unique SimulationAccountV1 successor identity.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.release_id IS 'Existing immutable runtime release retained by the successor.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.binding_id IS 'Existing immutable binding retained by the successor.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.ledger_scope_id IS 'Original economic ledger scope retained without row copy or reset.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.economic_facts_sha256 IS 'Canonical digest proving order, fill, cash, position, run, and outbox facts remain unchanged.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.status IS 'Prepared, safe-boundary pending, or active lineage state.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.version IS 'Monotonic CAS version for lineage activation.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.created_by IS 'Actor that prepared the retained-account mapping.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.created_at IS 'Lineage creation timestamp.';
COMMENT ON COLUMN paper_v2.legacy_localsim_account_lineage_v1.updated_at IS 'Last lineage lifecycle transition timestamp.';

COMMENT ON TABLE paper_v2.localsim_replay_job_v1 IS 'Isolated LocalSIM historical replay cursor and safe-boundary live-successor state.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.replay_job_id IS 'Content-addressed replay job identifier.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.replay_hash IS 'Canonical replay identity hash excluding mutable cursor and status.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.schema_version IS 'Replay contract version.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.simulation_account_id IS 'Dedicated replay account; current running accounts are never reused.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.release_id IS 'Historical replay runtime release.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.binding_id IS 'Dedicated historical replay binding and writer scope.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.day_engine_contract_id IS 'Unified simulation daily engine contract used by historical replay and live execution.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.start_trade_date IS 'First completed trading date in the immutable replay range.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.end_trade_date IS 'Last completed trading date in the immutable replay range.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.historical_source_id IS 'Explicit completed-day historical minute provider identity.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.historical_source_sha256 IS 'Immutable historical provider contract digest.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.calendar_snapshot_sha256 IS 'Immutable ordered trading-calendar snapshot digest.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.status IS 'Durable replay and safe-boundary live transition state.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.next_trade_date IS 'Exact next completed day to execute after restart.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.completed_trade_date IS 'Last atomically completed replay trading date.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.live_release_id IS 'Atomic live successor release created after catch-up.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.live_binding_id IS 'Atomic live successor binding created after catch-up.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.activation_trade_date IS 'First safe trading date eligible for TDX current-day execution.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.version IS 'Monotonic CAS version for replay cursor and transition updates.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.failure_code IS 'Typed retryable or terminal failure code.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.failure_context IS 'Bounded structured failure evidence; never a success fallback.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.created_by IS 'Actor that created the isolated replay.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.created_at IS 'Replay creation timestamp.';
COMMENT ON COLUMN paper_v2.localsim_replay_job_v1.updated_at IS 'Last durable cursor or state transition timestamp.';
COMMIT;

BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
DO $$
DECLARE
    missing_columns TEXT[];
    relation_name TEXT;
    required_columns TEXT[];
    foreign_key_count INTEGER;
    index_is_valid BOOLEAN;
BEGIN
    IF to_regclass('paper_v2.simulation_account_v1') IS NULL
       OR to_regclass('paper_v2.legacy_localsim_account_lineage_v1') IS NULL
       OR to_regclass('paper_v2.localsim_replay_job_v1') IS NULL THEN
        RAISE EXCEPTION 'SIM-LR-B successor schema post-commit readback is incomplete';
    END IF;
    FOR relation_name, required_columns IN
        SELECT * FROM (VALUES
            ('paper_v2.simulation_account_v1', ARRAY[
                'account_id','account_hash','schema_version','account_name','broker_backend',
                'package_id','manifest_sha256','admission_receipt_id','initial_capital',
                'account_config_json','status','version','created_by','created_at','updated_at'
            ]::TEXT[]),
            ('paper_v2.legacy_localsim_account_lineage_v1', ARRAY[
                'lineage_id','lineage_hash','schema_version','legacy_account_id','account_id',
                'release_id','binding_id','ledger_scope_id','economic_facts_sha256','status',
                'version','created_by','created_at','updated_at'
            ]::TEXT[]),
            ('paper_v2.localsim_replay_job_v1', ARRAY[
                'replay_job_id','replay_hash','schema_version','simulation_account_id',
                'release_id','binding_id','day_engine_contract_id','start_trade_date','end_trade_date','historical_source_id',
                'historical_source_sha256','calendar_snapshot_sha256','status','next_trade_date',
                'completed_trade_date','live_release_id','live_binding_id','activation_trade_date',
                'version','failure_code','failure_context','created_by','created_at','updated_at'
            ]::TEXT[])
        ) AS required(relation_name, required_columns)
    LOOP
        SELECT array_agg(required_column ORDER BY required_column)
          INTO missing_columns
          FROM unnest(required_columns) AS required_column
         WHERE NOT EXISTS (
            SELECT 1
              FROM pg_attribute
             WHERE attrelid = relation_name::regclass
               AND attname = required_column
               AND attnum > 0
               AND NOT attisdropped
         );
        IF missing_columns IS NOT NULL THEN
            RAISE EXCEPTION 'SIM-LR-B post-commit relation % is missing columns %', relation_name, missing_columns;
        END IF;
        IF obj_description(relation_name::regclass, 'pg_class') IS NULL THEN
            RAISE EXCEPTION 'SIM-LR-B post-commit relation % has no table comment', relation_name;
        END IF;
    END LOOP;
    SELECT count(*) INTO foreign_key_count
      FROM pg_constraint
     WHERE conrelid IN (
        'paper_v2.simulation_account_v1'::regclass,
        'paper_v2.legacy_localsim_account_lineage_v1'::regclass,
        'paper_v2.localsim_replay_job_v1'::regclass
     ) AND contype = 'f';
    IF foreign_key_count < 9 THEN
        RAISE EXCEPTION 'SIM-LR-B post-commit successor FK readback is incomplete: %', foreign_key_count;
    END IF;
    SELECT index_record.indisvalid AND index_record.indisunique
      INTO index_is_valid
      FROM pg_index AS index_record
     WHERE index_record.indexrelid = to_regclass('paper_v2.uq_localsim_successor_open_binding');
    IF index_is_valid IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'SIM-LR-B post-commit unique open-binding authority is invalid';
    END IF;
END $$;
COMMIT;
