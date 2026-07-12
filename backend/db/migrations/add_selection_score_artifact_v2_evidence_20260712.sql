ALTER TABLE strategy_pkg.selection_score_artifact
    ADD COLUMN IF NOT EXISTS artifact_contract_version TEXT,
    ADD COLUMN IF NOT EXISTS artifact_payload_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS artifact_input_context_hash TEXT,
    ADD COLUMN IF NOT EXISTS source_revision_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS asset_closure_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_pkg_selection_artifact_v2_payload
    ON strategy_pkg.selection_score_artifact(artifact_payload_sha256)
    WHERE artifact_payload_sha256 IS NOT NULL;

COMMENT ON COLUMN strategy_pkg.selection_score_artifact.artifact_contract_version IS
    'NULL for legacy rows; selection_score_artifact_v2 enables immutable prospective evidence semantics.';
COMMENT ON COLUMN strategy_pkg.selection_score_artifact.artifact_payload_sha256 IS
    'SHA256 hash of the v2 canonical artifact header; unique for non-legacy rows.';
COMMENT ON COLUMN strategy_pkg.selection_score_artifact.artifact_input_context_hash IS
    'SHA256 hash of raw score/cutoff/calendar input identity without Program or capture context.';
COMMENT ON COLUMN strategy_pkg.selection_score_artifact.source_revision_set_hash IS
    'SHA256 hash of source read receipts used by the v2 raw inference artifact.';
COMMENT ON COLUMN strategy_pkg.selection_score_artifact.asset_closure_hash IS
    'SHA256 hash of frozen package/model/factor asset closure used by the v2 artifact.';

COMMENT ON TABLE selection.daily_selection_evidence IS
    'Immutable broker-neutral DailySelectionEvidence. Legacy v1 remains readable; daily_selection_evidence_v2 stores prospective context, stage/source/asset lineage without a parallel table.';
COMMENT ON COLUMN selection.daily_selection_evidence.evidence_payload_json IS
    'Canonical JSON payload. v1 remains legacy-readable; v2 is a typed immutable prospective contract and must not contain its own evidence_id or artifact_hash.';
