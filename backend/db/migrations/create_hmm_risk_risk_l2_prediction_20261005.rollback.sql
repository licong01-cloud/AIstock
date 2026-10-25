-- No historical prediction deletion. Refuse rollback while either HMM table is populated.
BEGIN;
SET LOCAL lock_timeout = '5s';
LOCK TABLE hmm_risk.risk_l2_run, hmm_risk.risk_l2_prediction IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM hmm_risk.risk_l2_run) OR EXISTS (SELECT 1 FROM hmm_risk.risk_l2_prediction) THEN
    RAISE EXCEPTION 'HMM risk L2 rollback refused: immutable research rows must be preserved';
  END IF;
END $$;
DROP TABLE hmm_risk.risk_l2_prediction;
DROP TABLE hmm_risk.risk_l2_run;
COMMIT;
