-- Phase 2 StrategyPackage protected asset ledger foundation.
CREATE SCHEMA IF NOT EXISTS strategy_pkg;

CREATE TABLE IF NOT EXISTS strategy_pkg.package_asset (
    asset_id BIGSERIAL PRIMARY KEY,
    package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id),
    asset_type TEXT NOT NULL,
    asset_ref TEXT NOT NULL,
    asset_sha256 TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE strategy_pkg.package_asset
    ADD COLUMN IF NOT EXISTS asset_role TEXT NOT NULL DEFAULT 'governed_asset',
    ADD COLUMN IF NOT EXISTS asset_size_bytes BIGINT NULL,
    ADD COLUMN IF NOT EXISTS protected_asset BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS source_uri TEXT NULL;

DO $$
BEGIN
    IF to_regclass('strategy_pkg.package_asset') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM pg_catalog.pg_constraint
           WHERE conname = 'package_asset_size_non_negative_check'
             AND conrelid = 'strategy_pkg.package_asset'::regclass
       ) THEN
        ALTER TABLE strategy_pkg.package_asset
            ADD CONSTRAINT package_asset_size_non_negative_check CHECK (asset_size_bytes IS NULL OR asset_size_bytes >= 0);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_package_asset_package_type
    ON strategy_pkg.package_asset(package_id, asset_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_package_asset_protected
    ON strategy_pkg.package_asset(package_id, protected_asset, asset_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_package_asset_package_ref
    ON strategy_pkg.package_asset(package_id, asset_type, asset_ref);

COMMENT ON TABLE strategy_pkg.package_asset IS 'StrategyPackage protected asset ledger for frozen model, factor, schema, config, validation report, and runtime evidence references; this table records immutable metadata and does not authorize cleanup or overwrite.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_id IS 'Database-generated asset ledger row id.';
COMMENT ON COLUMN strategy_pkg.package_asset.package_id IS 'Frozen StrategyPackage identifier that owns this asset reference.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_type IS 'Governed asset type such as model_weight, factor_code, factor_schema, feature_order, train_config, preprocessor, prediction_schema, execution_config, risk_policy, or validation_report.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_ref IS 'Stable asset reference or path recorded for audit; code must not rewrite the referenced asset through this ledger.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_sha256 IS 'Optional SHA256 checksum of the referenced asset when available.';
COMMENT ON COLUMN strategy_pkg.package_asset.metadata IS 'Additional JSONB metadata such as source workflow, copy plan, validation notes, and lineage.';
COMMENT ON COLUMN strategy_pkg.package_asset.created_at IS 'Database timestamp when the asset ledger row was inserted.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_role IS 'Role of the asset in the frozen package, default governed_asset.';
COMMENT ON COLUMN strategy_pkg.package_asset.asset_size_bytes IS 'Optional size in bytes of the referenced asset; must be non-negative when present.';
COMMENT ON COLUMN strategy_pkg.package_asset.protected_asset IS 'Whether governance treats this asset reference as protected from cleanup or overwrite; defaults true for StrategyPackage assets.';
COMMENT ON COLUMN strategy_pkg.package_asset.source_uri IS 'Optional original source URI/path before controlled asset registration; audit-only and not used for runtime writes.';
