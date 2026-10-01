BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_risk_l1_prediction_v1'));

CREATE TABLE IF NOT EXISTS hmm_risk.risk_l1_prediction (
    prediction_id UUID CONSTRAINT pk_hmm_risk_risk_l1_prediction PRIMARY KEY,
    product_bundle_id TEXT,
    trade_date DATE NOT NULL,
    as_of_date DATE NOT NULL,
    sector_level TEXT NOT NULL DEFAULT 'L1',
    sector_code TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    risk_score DOUBLE PRECISION,
    risk_percentile DOUBLE PRECISION,
    risk_level TEXT,
    predicted_warning BOOLEAN,
    feature_contributions JSONB,
    availability TEXT NOT NULL,
    reason_code TEXT,
    risk_l1_research_surface_status TEXT NOT NULL,
    risk_l1_capability_status TEXT NOT NULL,
    forward_power_status TEXT NOT NULL,
    forward_confirmation TEXT NOT NULL,
    advisory_status TEXT NOT NULL,
    validation_basis TEXT NOT NULL,
    development_precision_lift DOUBLE PRECISION,
    development_recall DOUBLE PRECISION,
    model_hash CHAR(64) NOT NULL,
    input_hash CHAR(64) NOT NULL,
    mapping_snapshot_hash CHAR(64) NOT NULL,
    tail_accessed BOOLEAN NOT NULL DEFAULT FALSE,
    revision INTEGER NOT NULL,
    supersedes_prediction_id UUID CONSTRAINT fk_hmm_risk_risk_l1_prediction_supersedes
      REFERENCES hmm_risk.risk_l1_prediction(prediction_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_hmm_risk_risk_l1_prediction_revision UNIQUE (model_hash,trade_date,sector_code,revision),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_dates CHECK (as_of_date<trade_date),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_level CHECK (
      sector_level='L1' AND btrim(sector_code)<>'' AND btrim(sector_name)<>''
      AND (product_bundle_id IS NULL OR btrim(product_bundle_id)<>'')
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_availability CHECK (
      (availability='available' AND risk_score BETWEEN 0.0 AND 1.0
        AND risk_percentile BETWEEN 0.0 AND 1.0 AND risk_level IN ('normal','watch','high')
        AND predicted_warning=(risk_level='high') AND feature_contributions IS NOT NULL
        AND jsonb_typeof(feature_contributions)='array' AND jsonb_array_length(feature_contributions)=10
        AND NOT jsonb_path_exists(feature_contributions,'$[*] ? (@.type() != "number")') AND reason_code IS NULL)
      OR (availability='unavailable' AND risk_score IS NULL AND risk_percentile IS NULL
        AND risk_level IS NULL AND predicted_warning IS NULL AND feature_contributions IS NULL
        AND reason_code IS NOT NULL AND btrim(reason_code)<>'')
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_status CHECK (
      risk_l1_research_surface_status='NOT_AVAILABLE'
      AND risk_l1_capability_status IN ('NOT_AVAILABLE','RESEARCH_RISK_WARNING_AVAILABLE_FORWARD_UNCONFIRMED','ADVISORY_RISK_WARNING_AVAILABLE')
      AND forward_power_status IN ('UNAVAILABLE','INSUFFICIENT','SUFFICIENT')
      AND forward_confirmation IN ('NOT_STARTED','PENDING_INSUFFICIENT_POWER','PENDING_INCONCLUSIVE','PASSED','FAILED')
      AND validation_basis IN ('development_causal_oof','single_date_frozen_model')
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_hashes CHECK (
      model_hash ~ '^[0-9a-f]{64}$' AND input_hash ~ '^[0-9a-f]{64}$' AND mapping_snapshot_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_revision CHECK (revision>0),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_supersedes_chain CHECK (
      (revision=1 AND supersedes_prediction_id IS NULL) OR (revision>1 AND supersedes_prediction_id IS NOT NULL)
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_development CHECK (
      (development_precision_lift IS NULL AND development_recall IS NULL)
      OR (development_precision_lift>'-Infinity'::double precision AND development_precision_lift<'Infinity'::double precision
        AND development_recall BETWEEN 0.0 AND 1.0)
    ),
    CONSTRAINT ck_hmm_risk_risk_l1_prediction_advisory CHECK (
      advisory_status='NOT_AVAILABLE' AND NOT tail_accessed AND product_bundle_id IS NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_hmm_risk_risk_l1_lookup
ON hmm_risk.risk_l1_prediction(trade_date,sector_code,revision DESC);

COMMENT ON TABLE hmm_risk.risk_l1_prediction IS
'Append-only G2-B L1 risk warning revisions; scores are uncalibrated research outputs.';
COMMENT ON INDEX hmm_risk.idx_hmm_risk_risk_l1_lookup IS
'Date and sector L1 risk revision lookup; model identity remains explicit.';

DO $$
DECLARE item TEXT;
BEGIN
  FOREACH item IN ARRAY ARRAY[
    'prediction_id','product_bundle_id','trade_date','as_of_date','sector_level','sector_code','sector_name',
    'risk_score','risk_percentile','risk_level','predicted_warning','feature_contributions','availability','reason_code',
    'risk_l1_research_surface_status','risk_l1_capability_status','forward_power_status','forward_confirmation',
    'advisory_status','validation_basis','development_precision_lift','development_recall','model_hash','input_hash',
    'mapping_snapshot_hash','tail_accessed','revision','supersedes_prediction_id','created_at'
  ] LOOP
    EXECUTE format(
      'COMMENT ON COLUMN hmm_risk.risk_l1_prediction.%I IS %L', item,
      'risk_l1_prediction.' || item || ' exact hmm_risk_risk_l1_prediction_v1 contract'
    );
  END LOOP;
  FOREACH item IN ARRAY ARRAY[
    'pk_hmm_risk_risk_l1_prediction','uq_hmm_risk_risk_l1_prediction_revision',
    'fk_hmm_risk_risk_l1_prediction_supersedes','ck_hmm_risk_risk_l1_prediction_dates',
    'ck_hmm_risk_risk_l1_prediction_level','ck_hmm_risk_risk_l1_prediction_availability',
    'ck_hmm_risk_risk_l1_prediction_status','ck_hmm_risk_risk_l1_prediction_hashes',
    'ck_hmm_risk_risk_l1_prediction_revision','ck_hmm_risk_risk_l1_prediction_supersedes_chain',
    'ck_hmm_risk_risk_l1_prediction_development','ck_hmm_risk_risk_l1_prediction_advisory'
  ] LOOP
    EXECUTE format(
      'COMMENT ON CONSTRAINT %I ON hmm_risk.risk_l1_prediction IS %L', item,
      item || ' enforces hmm_risk_risk_l1_prediction_v1'
    );
  END LOOP;
END $$;

COMMIT;
