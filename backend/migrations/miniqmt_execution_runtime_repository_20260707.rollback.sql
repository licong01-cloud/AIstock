-- Rollback for MiniQMT execution runtime production repository.
-- Controlled DDL only: execute manually through the production DDL gate.

DROP TABLE IF EXISTS qmt_strategy.execution_child_order;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_instance;
DROP TABLE IF EXISTS qmt_strategy.execution_runtime_event;
DROP TABLE IF EXISTS qmt_strategy.execution_runtime;
