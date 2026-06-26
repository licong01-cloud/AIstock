-- Roll back Research Assistant LLM usage accounting ledger.
-- Run with psql --single-transaction -v ON_ERROR_STOP=1.
-- This removes only the feature-owned ledger. It does not modify
-- assistant_trace_events or other Research Assistant state.

DROP TABLE IF EXISTS assistant_llm_usage_events;
