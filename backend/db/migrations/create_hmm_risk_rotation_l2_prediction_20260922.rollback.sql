BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_rotation_l2_prediction_v1'));

DO $$
BEGIN
  IF to_regclass('hmm_risk.rotation_l2_prediction') IS NOT NULL
     AND EXISTS (SELECT 1 FROM hmm_risk.rotation_l2_prediction LIMIT 1) THEN
    RAISE EXCEPTION 'refusing to drop non-empty hmm_risk.rotation_l2_prediction';
  END IF;
END $$;

DROP TABLE IF EXISTS hmm_risk.rotation_l2_prediction;
COMMIT;
