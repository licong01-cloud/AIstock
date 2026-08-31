-- SIM-LR-C neutral ledger-scope bridge for the atomic LocalSIM product cutover.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, paper_v2, strategy_pkg, pg_temp;

CREATE TABLE IF NOT EXISTS paper_v2.localsim_runtime_profile_v1 (
    profile_id TEXT PRIMARY KEY,
    profile_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL DEFAULT 'localsim_runtime_profile_v1'
        CHECK (schema_version = 'localsim_runtime_profile_v1'),
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    profile_name TEXT NOT NULL CHECK (btrim(profile_name) <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RETIRED')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_v2.localsim_runtime_profile_version_v1 (
    profile_version_id TEXT PRIMARY KEY,
    profile_version_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL DEFAULT 'localsim_runtime_profile_version_v1'
        CHECK (schema_version = 'localsim_runtime_profile_version_v1'),
    profile_id TEXT NOT NULL REFERENCES paper_v2.localsim_runtime_profile_v1(profile_id),
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    version_no INTEGER NOT NULL CHECK (version_no >= 1),
    config_json JSONB NOT NULL,
    config_sha256 TEXT NOT NULL,
    daily_strategy_profile_version_id TEXT NOT NULL,
    validation_status TEXT NOT NULL CHECK (validation_status IN ('VALIDATED', 'INVALID', 'RETIRED')),
    validation_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(profile_id, version_no),
    UNIQUE(profile_id, config_sha256)
);

CREATE INDEX IF NOT EXISTS idx_localsim_runtime_profile_package
    ON paper_v2.localsim_runtime_profile_v1(package_id, manifest_sha256, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_localsim_runtime_profile_version_profile
    ON paper_v2.localsim_runtime_profile_version_v1(profile_id, version_no DESC);

CREATE TABLE IF NOT EXISTS paper_v2.simulation_ledger_scope_v1 (
    ledger_scope_id TEXT PRIMARY KEY,
    ledger_scope_hash TEXT NOT NULL UNIQUE,
    schema_version TEXT NOT NULL DEFAULT 'simulation_ledger_scope_v1'
        CHECK (schema_version = 'simulation_ledger_scope_v1'),
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('LEGACY_PORTFOLIO', 'SUCCESSOR_NATIVE')),
    source_identity TEXT NOT NULL UNIQUE,
    native_account_id TEXT UNIQUE REFERENCES paper_v2.simulation_account_v1(account_id),
    created_by TEXT NOT NULL CHECK (btrim(created_by) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_simulation_ledger_scope_v1_identity CHECK (
        (
            scope_kind = 'LEGACY_PORTFOLIO'
            AND native_account_id IS NULL
            AND ledger_scope_id = source_identity
        ) OR (
            scope_kind = 'SUCCESSOR_NATIVE'
            AND native_account_id IS NOT NULL
            AND ledger_scope_id = source_identity
            AND ledger_scope_id = native_account_id
        )
    )
);

INSERT INTO paper_v2.simulation_ledger_scope_v1 (
    ledger_scope_id, ledger_scope_hash, schema_version, scope_kind,
    source_identity, native_account_id, created_by, created_at
)
SELECT
    portfolio.portfolio_id,
    encode(
        sha256(
            convert_to(
                '{"ledger_scope_id":' || to_json(portfolio.portfolio_id)::text
                || ',"native_account_id":null'
                || ',"schema_version":"simulation_ledger_scope_v1"'
                || ',"scope_kind":"LEGACY_PORTFOLIO"'
                || ',"source_identity":' || to_json(portfolio.portfolio_id)::text || '}',
                'UTF8'
            )
        ),
        'hex'
    ),
    'simulation_ledger_scope_v1',
    'LEGACY_PORTFOLIO',
    portfolio.portfolio_id,
    NULL,
    'migration:localsim_product_cutover_bridge_20260831',
    portfolio.created_at
FROM paper_v2.portfolio AS portfolio
ON CONFLICT (ledger_scope_id) DO NOTHING;

DO $$
DECLARE
    missing_scope_count BIGINT;
    constraint_record RECORD;
BEGIN
    SELECT count(*) INTO missing_scope_count
    FROM (
        SELECT run.portfolio_id AS ledger_scope_id FROM paper_v2.run AS run
        UNION
        SELECT snapshot.portfolio_id FROM paper_v2.intraday_snapshots AS snapshot
    ) AS referenced
    LEFT JOIN paper_v2.simulation_ledger_scope_v1 AS scope
      ON scope.ledger_scope_id = referenced.ledger_scope_id
    WHERE scope.ledger_scope_id IS NULL;
    IF missing_scope_count <> 0 THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope backfill is missing % runtime-active scopes', missing_scope_count;
    END IF;

    FOR constraint_record IN
        SELECT constraint_row.conrelid::regclass AS relation_name, constraint_row.conname
        FROM pg_constraint AS constraint_row
        WHERE constraint_row.contype = 'f'
          AND constraint_row.conrelid IN (
              'paper_v2.run'::regclass,
              'paper_v2.intraday_snapshots'::regclass
          )
          AND constraint_row.confrelid = 'paper_v2.portfolio'::regclass
    LOOP
        EXECUTE format(
            'ALTER TABLE %s DROP CONSTRAINT %I',
            constraint_record.relation_name,
            constraint_record.conname
        );
    END LOOP;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'paper_v2.run'::regclass
          AND conname = 'fk_paper_v2_run_ledger_scope_v1'
    ) THEN
        ALTER TABLE paper_v2.run
            ADD CONSTRAINT fk_paper_v2_run_ledger_scope_v1
            FOREIGN KEY (portfolio_id)
            REFERENCES paper_v2.simulation_ledger_scope_v1(ledger_scope_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'paper_v2.intraday_snapshots'::regclass
          AND conname = 'fk_paper_v2_intraday_snapshots_ledger_scope_v1'
    ) THEN
        ALTER TABLE paper_v2.intraday_snapshots
            ADD CONSTRAINT fk_paper_v2_intraday_snapshots_ledger_scope_v1
            FOREIGN KEY (portfolio_id)
            REFERENCES paper_v2.simulation_ledger_scope_v1(ledger_scope_id);
    END IF;
END $$;

COMMENT ON TABLE paper_v2.simulation_ledger_scope_v1 IS 'Immutable neutral LocalSIM economic-ledger namespace; it is not an account or Paper portfolio truth.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.ledger_scope_id IS 'Economic-fact namespace used by successor runtime repositories.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.ledger_scope_hash IS 'Canonical immutable SimulationLedgerScopeV1 identity hash.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.schema_version IS 'Ledger-scope contract version fixed to simulation_ledger_scope_v1.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.scope_kind IS 'Immutable source kind LEGACY_PORTFOLIO or SUCCESSOR_NATIVE.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.source_identity IS 'Original immutable ledger namespace source identity.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.native_account_id IS 'Native successor account identity; NULL for immutable legacy scopes.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.created_by IS 'Migration or application actor that created the immutable scope.';
COMMENT ON COLUMN paper_v2.simulation_ledger_scope_v1.created_at IS 'Immutable ledger-scope creation timestamp.';

COMMENT ON TABLE paper_v2.localsim_runtime_profile_v1 IS 'Package-scoped LocalSIM mutable-configuration profile; it has no Paper portfolio or account ownership.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.profile_id IS 'Content-addressed LocalSIM runtime profile identity.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.profile_hash IS 'Canonical immutable profile identity hash.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.schema_version IS 'Profile contract version fixed to localsim_runtime_profile_v1.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.package_id IS 'StrategyPackage alpha-core owner of this runtime profile.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.manifest_sha256 IS 'Frozen StrategyPackage manifest identity.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.profile_name IS 'User-facing package-scoped runtime profile name.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.status IS 'CAS lifecycle ACTIVE or RETIRED.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.version IS 'Monotonic CAS version for append and retirement.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.created_by IS 'Actor that created the profile.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.created_at IS 'Profile creation timestamp.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_v1.updated_at IS 'Last successful append or lifecycle transition timestamp.';

COMMENT ON TABLE paper_v2.localsim_runtime_profile_version_v1 IS 'Immutable validated LocalSIM daily, HMM, risk, fee, and materialized runtime-variant configuration.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.profile_version_id IS 'Content-addressed immutable profile-version identity.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.profile_version_hash IS 'Canonical profile/package/manifest/config identity hash.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.schema_version IS 'Profile-version contract fixed to localsim_runtime_profile_version_v1.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.profile_id IS 'Owning LocalSimRuntimeProfileV1.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.package_id IS 'StrategyPackage owner repeated for fail-closed readback.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.manifest_sha256 IS 'Frozen StrategyPackage manifest identity.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.version_no IS 'Monotonic immutable ordinal within one profile.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.config_json IS 'Strict canonical daily/HMM/risk/fee/materialized-variant configuration.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.config_sha256 IS 'Canonical config_json SHA-256.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.daily_strategy_profile_version_id IS 'Deterministic daily-strategy component identity derived from config_json.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.validation_status IS 'VALIDATED, INVALID, or RETIRED; only VALIDATED may enter a release.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.validation_evidence IS 'Bounded durable reference validation evidence.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.created_by IS 'Actor that created the immutable version.';
COMMENT ON COLUMN paper_v2.localsim_runtime_profile_version_v1.created_at IS 'Profile-version creation timestamp.';
COMMIT;

BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
DO $$
DECLARE
    old_runtime_fk_count INTEGER;
    new_runtime_fk_count INTEGER;
    invalid_scope_count BIGINT;
BEGIN
    IF to_regclass('paper_v2.simulation_ledger_scope_v1') IS NULL
       OR to_regclass('paper_v2.localsim_runtime_profile_v1') IS NULL
       OR to_regclass('paper_v2.localsim_runtime_profile_version_v1') IS NULL
       OR obj_description('paper_v2.simulation_ledger_scope_v1'::regclass, 'pg_class') IS NULL THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope post-commit relation/comment readback failed';
    END IF;
    SELECT count(*) INTO old_runtime_fk_count
    FROM pg_constraint
    WHERE contype = 'f'
      AND conrelid IN ('paper_v2.run'::regclass, 'paper_v2.intraday_snapshots'::regclass)
      AND confrelid = 'paper_v2.portfolio'::regclass;
    SELECT count(*) INTO new_runtime_fk_count
    FROM pg_constraint
    WHERE contype = 'f'
      AND conrelid IN ('paper_v2.run'::regclass, 'paper_v2.intraday_snapshots'::regclass)
      AND confrelid = 'paper_v2.simulation_ledger_scope_v1'::regclass;
    IF old_runtime_fk_count <> 0 OR new_runtime_fk_count <> 2 THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope FK readback mismatch old=% new=%', old_runtime_fk_count, new_runtime_fk_count;
    END IF;
    SELECT count(*) INTO invalid_scope_count
    FROM paper_v2.simulation_ledger_scope_v1 AS scope
    WHERE length(scope.ledger_scope_hash) <> 64
       OR scope.ledger_scope_hash <> lower(scope.ledger_scope_hash)
       OR (scope.scope_kind = 'LEGACY_PORTFOLIO' AND scope.native_account_id IS NOT NULL)
       OR (scope.scope_kind = 'SUCCESSOR_NATIVE' AND scope.native_account_id IS DISTINCT FROM scope.ledger_scope_id);
    IF invalid_scope_count <> 0 THEN
        RAISE EXCEPTION 'SIM-LR-C ledger-scope identity readback found % invalid rows', invalid_scope_count;
    END IF;
END $$;
COMMIT;
