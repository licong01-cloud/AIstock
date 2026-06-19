-- P1 multi-alpha base schema: strategy package prediction refs + component edges.
-- Idempotent migration only; do not apply from application code.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;

ALTER TABLE strategy_pkg.package
    ADD COLUMN IF NOT EXISTS alpha_mode TEXT NOT NULL DEFAULT 'single_alpha',
    ADD COLUMN IF NOT EXISTS signal_domain TEXT,
    ADD COLUMN IF NOT EXISTS display_name TEXT,
    ADD COLUMN IF NOT EXISTS legacy_name TEXT,
    ADD COLUMN IF NOT EXISTS data_vintage DATE,
    ADD COLUMN IF NOT EXISTS prediction_ref_uri TEXT,
    ADD COLUMN IF NOT EXISTS prediction_ref_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS model_artifact_uri TEXT,
    ADD COLUMN IF NOT EXISTS model_artifact_sha256 TEXT;

UPDATE strategy_pkg.package
SET alpha_mode = COALESCE(NULLIF(alpha_mode, ''), manifest_json->>'alpha_mode', 'single_alpha'),
    display_name = COALESCE(NULLIF(display_name, ''), package_name),
    legacy_name = COALESCE(legacy_name, package_name)
WHERE alpha_mode IS NULL
   OR alpha_mode = ''
   OR display_name IS NULL
   OR legacy_name IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_strategy_pkg_package_alpha_mode'
          AND conrelid = 'strategy_pkg.package'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT ck_strategy_pkg_package_alpha_mode
            CHECK (alpha_mode IN ('single_alpha', 'multi_alpha'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_strategy_pkg_package_prediction_ref_sha256'
          AND conrelid = 'strategy_pkg.package'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT ck_strategy_pkg_package_prediction_ref_sha256
            CHECK (prediction_ref_sha256 IS NULL OR prediction_ref_sha256 ~ '^[0-9a-f]{64}$');
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_strategy_pkg_package_model_artifact_sha256'
          AND conrelid = 'strategy_pkg.package'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT ck_strategy_pkg_package_model_artifact_sha256
            CHECK (model_artifact_sha256 IS NULL OR model_artifact_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS strategy_pkg.strategy_package_components (
    id BIGSERIAL PRIMARY KEY,
    parent_package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id) ON DELETE RESTRICT,
    child_package_id TEXT NOT NULL REFERENCES strategy_pkg.package(package_id) ON DELETE RESTRICT,
    child_manifest_sha256 TEXT NOT NULL,
    component_weight NUMERIC(18, 10) NOT NULL,
    score_normalization TEXT NOT NULL DEFAULT 'rank',
    position INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_strategy_pkg_component_no_self CHECK (parent_package_id <> child_package_id),
    CONSTRAINT ck_strategy_pkg_component_weight_positive CHECK (component_weight > 0),
    CONSTRAINT ck_strategy_pkg_component_position_positive CHECK (position > 0),
    CONSTRAINT ck_strategy_pkg_component_child_sha CHECK (child_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_strategy_pkg_component_score_norm CHECK (btrim(score_normalization) <> ''),
    CONSTRAINT uq_strategy_pkg_component_parent_child UNIQUE (parent_package_id, child_package_id),
    CONSTRAINT uq_strategy_pkg_component_parent_position UNIQUE (parent_package_id, position)
);

CREATE INDEX IF NOT EXISTS idx_strategy_pkg_components_child
    ON strategy_pkg.strategy_package_components(child_package_id);

CREATE OR REPLACE FUNCTION strategy_pkg.enforce_strategy_package_component_shape()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    parent_mode TEXT;
    parent_status TEXT;
    child_mode TEXT;
    child_status TEXT;
    actual_child_sha TEXT;
BEGIN
    SELECT alpha_mode, package_status
      INTO parent_mode, parent_status
      FROM strategy_pkg.package
     WHERE package_id = NEW.parent_package_id;

    IF parent_mode IS NULL THEN
        RAISE EXCEPTION 'multi-alpha component parent package does not exist: %', NEW.parent_package_id;
    END IF;
    IF parent_mode <> 'multi_alpha' THEN
        RAISE EXCEPTION 'multi-alpha component parent must have alpha_mode=multi_alpha: package_id=% alpha_mode=%', NEW.parent_package_id, parent_mode;
    END IF;
    IF parent_status = 'RETIRED' THEN
        RAISE EXCEPTION 'multi-alpha component parent is retired: package_id=%', NEW.parent_package_id;
    END IF;

    SELECT alpha_mode, package_status, manifest_sha256
      INTO child_mode, child_status, actual_child_sha
      FROM strategy_pkg.package
     WHERE package_id = NEW.child_package_id;

    IF child_mode IS NULL THEN
        RAISE EXCEPTION 'multi-alpha component child package does not exist: %', NEW.child_package_id;
    END IF;
    IF child_mode <> 'single_alpha' THEN
        RAISE EXCEPTION 'multi-alpha component child must have alpha_mode=single_alpha: package_id=% alpha_mode=%', NEW.child_package_id, child_mode;
    END IF;
    IF child_status = 'RETIRED' THEN
        RAISE EXCEPTION 'multi-alpha component child is retired: package_id=%', NEW.child_package_id;
    END IF;
    IF actual_child_sha <> NEW.child_manifest_sha256 THEN
        RAISE EXCEPTION 'multi-alpha component child manifest sha mismatch: child=% expected=% actual=%', NEW.child_package_id, NEW.child_manifest_sha256, actual_child_sha;
    END IF;

    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_pkg_component_shape ON strategy_pkg.strategy_package_components;
CREATE TRIGGER trg_strategy_pkg_component_shape
BEFORE INSERT OR UPDATE ON strategy_pkg.strategy_package_components
FOR EACH ROW EXECUTE FUNCTION strategy_pkg.enforce_strategy_package_component_shape();

CREATE OR REPLACE FUNCTION strategy_pkg.prevent_referenced_component_retirement()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.package_status = 'RETIRED' AND OLD.package_status IS DISTINCT FROM NEW.package_status THEN
        IF EXISTS (
            SELECT 1
            FROM strategy_pkg.strategy_package_components c
            JOIN strategy_pkg.package parent ON parent.package_id = c.parent_package_id
            WHERE c.child_package_id = NEW.package_id
              AND parent.package_status <> 'RETIRED'
        ) THEN
            RAISE EXCEPTION 'referenced single-alpha child package cannot be retired while used by active multi-alpha package: package_id=%', NEW.package_id;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_pkg_component_retire_guard ON strategy_pkg.package;
CREATE TRIGGER trg_strategy_pkg_component_retire_guard
BEFORE UPDATE OF package_status ON strategy_pkg.package
FOR EACH ROW EXECUTE FUNCTION strategy_pkg.prevent_referenced_component_retirement();

COMMENT ON COLUMN strategy_pkg.package.alpha_mode IS 'single_alpha or multi_alpha package mode; promoted from manifest JSON for filtering and relational constraints.';
COMMENT ON COLUMN strategy_pkg.package.signal_domain IS 'Human-readable signal domain or theme used by package naming and package library filtering.';
COMMENT ON COLUMN strategy_pkg.package.display_name IS 'Human-readable StrategyPackage name following the multi-alpha naming blueprint; package_id remains the machine key.';
COMMENT ON COLUMN strategy_pkg.package.legacy_name IS 'Original legacy package name or alias retained for compatibility.';
COMMENT ON COLUMN strategy_pkg.package.data_vintage IS 'Data vintage date bound to this immutable package version.';
COMMENT ON COLUMN strategy_pkg.package.prediction_ref_uri IS 'Prediction-store artifact URI for pred.pkl bound to this package.';
COMMENT ON COLUMN strategy_pkg.package.prediction_ref_sha256 IS 'Expected SHA256 digest for prediction_ref_uri; verify-on-use must fail if actual digest differs.';
COMMENT ON COLUMN strategy_pkg.package.model_artifact_uri IS 'Model artifact URI, typically params.pkl, bound to this package when available.';
COMMENT ON COLUMN strategy_pkg.package.model_artifact_sha256 IS 'Expected SHA256 digest for model_artifact_uri; verify-on-use must fail if actual digest differs.';
COMMENT ON TABLE strategy_pkg.strategy_package_components IS 'Frozen depth-1 component edges for multi-alpha StrategyPackages. DB structure is authoritative; manifest JSON is a derived snapshot.';
COMMENT ON COLUMN strategy_pkg.strategy_package_components.child_manifest_sha256 IS 'Pinned manifest SHA256 of the single-alpha child at combination creation time; child changes require a new multi-alpha package version.';
COMMENT ON COLUMN strategy_pkg.strategy_package_components.component_weight IS 'Frozen component weight used by the multi-alpha combination policy.';
COMMENT ON COLUMN strategy_pkg.strategy_package_components.score_normalization IS 'Score normalization used for this child before blending, for example rank or zscore.';
COMMENT ON COLUMN strategy_pkg.strategy_package_components.position IS 'Stable display/order position inside the parent multi-alpha package.';
