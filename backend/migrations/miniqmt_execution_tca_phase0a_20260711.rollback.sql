-- Scratch-only rollback for MiniQMT adaptive-IS Phase 0A execution TCA.
-- Production evidence must be retained; physical rollback requires separate authorization.

DROP TABLE IF EXISTS qmt_strategy.execution_tca_result_trade_observation;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_result_mark;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_receipt_result;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_receipt_planning_subject;
DROP TABLE IF EXISTS qmt_strategy.execution_parent_tca;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_rebuild_receipt;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_mark;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_trade_conflict;
DROP TABLE IF EXISTS qmt_strategy.execution_tca_trade_observation;
DROP TABLE IF EXISTS qmt_strategy.execution_parent_benchmark;
DROP TABLE IF EXISTS qmt_strategy.execution_planning_subject;

DROP FUNCTION IF EXISTS qmt_strategy.validate_tca_subject_parent();
DROP FUNCTION IF EXISTS qmt_strategy.validate_tca_result_observation_role();
DROP FUNCTION IF EXISTS qmt_strategy.reject_execution_tca_mutation();

DROP INDEX IF EXISTS qmt_strategy.ix_tca_reconciliation_issue_run;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_reconciliation_scope;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_trade_ledger_intent_time;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_order_event_intent_time;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_order_ledger_intent_sync;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_child_parent_algo;
DROP INDEX IF EXISTS qmt_strategy.ix_tca_algo_parent_runtime;
DROP INDEX IF EXISTS paper_v2.ux_tca_simulation_run_plan;
DROP INDEX IF EXISTS paper_v2.ux_tca_execution_plan_id_hash;

ALTER TABLE qmt_strategy.trade_ledger
    DROP CONSTRAINT IF EXISTS ck_tca_trade_ledger_provenance,
    DROP COLUMN IF EXISTS canonical_trade_fact_sha256,
    DROP COLUMN IF EXISTS first_ingested_at,
    DROP COLUMN IF EXISTS first_ingest_source;
