BEGIN;

LOCK TABLE hmm_risk.rotation_l1_prediction IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
  current_definition TEXT;
  compact_definition TEXT;
BEGIN
  SELECT pg_get_constraintdef(con.oid, true)
    INTO current_definition
  FROM pg_constraint con
  JOIN pg_class cls ON cls.oid = con.conrelid
  JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
  WHERE nsp.nspname = 'hmm_risk'
    AND cls.relname = 'rotation_l1_prediction'
    AND con.conname = 'ck_hmm_risk_rotation_l1_prediction_availability';

  IF current_definition IS NULL THEN
    RAISE EXCEPTION 'hmm_risk_rotation_l1_schema_drift: availability constraint missing';
  END IF;

  compact_definition := regexp_replace(lower(current_definition), '\s+', '', 'g');
  IF compact_definition NOT LIKE '%jsonb_array_length(feature_contributions)=10%'
     AND compact_definition NOT LIKE '%jsonb_array_length(feature_contributions)=any(array[10,11])%'
     AND compact_definition NOT LIKE '%jsonb_array_length(feature_contributions)in(10,11)%' THEN
    RAISE EXCEPTION 'hmm_risk_rotation_l1_schema_drift: unexpected contribution dimensions';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM hmm_risk.rotation_l1_prediction
    WHERE availability = 'available'
      AND (
        jsonb_typeof(feature_contributions) IS DISTINCT FROM 'array'
        OR CASE
          WHEN jsonb_typeof(feature_contributions) = 'array'
          THEN jsonb_array_length(feature_contributions) NOT IN (10, 11)
          ELSE TRUE
        END
      )
  ) THEN
    RAISE EXCEPTION 'hmm_risk_rotation_l1_schema_drift: stored contribution dimensions invalid';
  END IF;
END
$$;

ALTER TABLE hmm_risk.rotation_l1_prediction
  DROP CONSTRAINT ck_hmm_risk_rotation_l1_prediction_availability;

ALTER TABLE hmm_risk.rotation_l1_prediction
  ADD CONSTRAINT ck_hmm_risk_rotation_l1_prediction_availability CHECK (
    (availability='available' AND rotation_score IS NOT NULL
      AND rotation_score>'-Infinity'::double precision AND rotation_score<'Infinity'::double precision
      AND forecast_state IS NOT NULL AND feature_contributions IS NOT NULL
      AND jsonb_typeof(feature_contributions)='array'
      AND jsonb_array_length(feature_contributions) IN (10,11)
      AND NOT jsonb_path_exists(feature_contributions,'$[*] ? (@.type() != "number")')
      AND reason_code IS NULL)
    OR (availability='unavailable' AND rotation_score IS NULL AND forecast_state IS NULL
      AND feature_contributions IS NULL AND reason_code IS NOT NULL AND btrim(reason_code)<>'')
  );

COMMENT ON CONSTRAINT ck_hmm_risk_rotation_l1_prediction_availability
  ON hmm_risk.rotation_l1_prediction IS
  'ck_hmm_risk_rotation_l1_prediction_availability enforces hmm_risk_rotation_l1_prediction_v1';

COMMIT;
