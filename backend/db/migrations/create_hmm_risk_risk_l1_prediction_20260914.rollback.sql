BEGIN;
SELECT pg_advisory_xact_lock(hashtext('hmm_risk_risk_l1_prediction_v1'));

DO $$
BEGIN
  IF to_regclass('hmm_risk.risk_l1_prediction') IS NOT NULL
     AND EXISTS (SELECT 1 FROM hmm_risk.risk_l1_prediction LIMIT 1) THEN
    RAISE EXCEPTION 'refusing to drop non-empty hmm_risk.risk_l1_prediction';
  END IF;
END $$;

DROP TABLE IF EXISTS hmm_risk.risk_l1_prediction;
COMMIT;
