-- 2026-05-10
-- Add paper_v2.run.model_params_origin (provenance of model params: node / cache / unavailable).
-- Related: backend/services/strategy_package/live_inference.py silent fallback fix
-- (C1 audit commit 88bc89c). DEFAULT 'node' covers existing rows post-migration;
-- new INSERTs from the live inference path must explicitly set the value via
-- the PreparedInferenceWorkspace.model_params_origin channel.
-- DO NOT RUN: queued for next D4 batch by user.

ALTER TABLE paper_v2.run
    ADD COLUMN IF NOT EXISTS model_params_origin VARCHAR(16) NOT NULL DEFAULT 'node';

ALTER TABLE paper_v2.run
    DROP CONSTRAINT IF EXISTS run_model_params_origin_check;
ALTER TABLE paper_v2.run
    ADD CONSTRAINT run_model_params_origin_check
    CHECK (model_params_origin IN ('node', 'cache', 'unavailable'));

COMMENT ON COLUMN paper_v2.run.model_params_origin IS
    'Provenance of model params used for this run: node (downloaded from QE node API), cache (local fallback, requires explicit allow_cache_fallback opt-in), unavailable (run failed before params resolved)';
