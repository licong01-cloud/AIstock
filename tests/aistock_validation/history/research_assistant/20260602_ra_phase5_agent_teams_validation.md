# Research Assistant Phase 5 Agent Teams Validation

- plan_key: ra_phase5_agent_teams
- branch: codex/ra-agent-teams-20260601
- validated_commit: 4a4eb15c477e8441c8c489d72ca2a59b72a09d89
- g1_central_run_id: research-assistant-agent-teams_20260601_170018_l3_ra-phase5-agent-teams_52455a77_runner-validation__ec27d8d309
- production_ddl_gate: required_pending_user_approval
- production_ddl_apply: not executed
- ddl_idempotency: real_postgres twice no-diff via local dev validation DB isolated schema
- production_8001_touched: false
- production_db_touched: false

## G1
- local: PASSED - `nox -s ra_phase5_agent_teams`, 13 passed, catalog integrity passed, ownership scan files=24 mapped=24.
- central: PASSED - return_code=0, status=passed, production_8001_touched=false.

## G2 Closure Requirements
- CR-P5-01 done - agent team runtime package exists: `backend/services/research_assistant/agent_teams/`.
- CR-P5-02 done - declarative config exists: `configs/research_assistant/agent_teams.yaml`.
- CR-P5-03 done - first-wave workers registered: qe_experiment_designer, hmm_evolution, factor_developer, local_data_doctor.
- CR-P5-04 done - orchestrator uses primary_reasoner metadata and does not execute domain tools.
- CR-P5-05 done - workers use cheap_worker metadata and single-domain goals.
- CR-P5-06 done - `assistant_agent_runs` DDL exists.
- CR-P5-07 done - real Postgres idempotency test executes DDL twice with no pg_catalog diff.
- CR-P5-08 done - table/index/column COMMENT contract tested.
- CR-P5-09 done - production DDL gate marked required_pending_user_approval; production DB not touched.
- CR-P5-10 done - repository kind `agent_runs` added.
- CR-P5-11 done - service adapter queues and finishes agent runs.
- CR-P5-12 done - each worker builds isolated context pack.
- CR-P5-13 done - worker scoped catalog filters allowed tools.
- CR-P5-14 done - out-of-scope worker tool rejected by catalog gate.
- CR-P5-15 done - high-risk/write tool path is preflight-only; execution mock not called.
- CR-P5-16 done - runtime dispatches multiple workers in parallel executor.
- CR-P5-17 done - worker failure is isolated and still reduced.
- CR-P5-18 done - reduce stable order uses task_order/agent_key.
- CR-P5-19 done - deterministic id factory in tests; agent_run_id not used for ordering.
- CR-P5-20 done - repeated same input is byte-identical.
- CR-P5-21 done - reversed completion/request order is byte-stable.
- CR-P5-22 done - assistant main bubble excludes thought/observation chain.
- CR-P5-23 done - structured result contract includes summary/artifacts/evidence_refs/status.
- CR-P5-24 done - reduce includes evidence_refs and conflict_arbitration.
- CR-P5-25 done - personal.task.* memory candidate requires provenance.
- CR-P5-26 done - no-source writeback is blocked by curator adapter.
- CR-P5-27 done - project rule/architecture/directive changes remain approval-bound by existing service approval model.
- CR-P5-28 done - Phase3 ReAct loop is reused, not forked.
- CR-P5-29 done - react catalog parameterization remains worker-scoped.
- CR-P5-30 done - Phase1/2 context data is consumed in worker prompt/context pack.
- CR-P5-31 done - config loader rejects missing/duplicated worker definitions.
- CR-P5-32 done - adding workers is config-driven, runtime has no concrete worker constants.
- CR-P5-33 done - module registry updated for ra_phase5_agent_teams.
- CR-P5-34 done - file ownership updated for runtime, DDL, config, tests, history.
- CR-P5-35 done - nox session registered and allowlisted.
- CR-P5-36 done - controlled runner plan is workspace-scoped and forbids prod_db.
- CR-P5-37 done - core no-adapter import test covers Agent Teams core.
- CR-P5-38 done - blueprint section 12 Agent Teams row updated with implementation evidence.

## Key Risk Points
- DDL safety: real Postgres twice no-diff, isolated validation schema, prod_db forbidden.
- Reduce determinism: stable task_order/agent_key sort; byte-stable tests pass.
- Worker tool isolation: out-of-scope calls rejected.
- High-risk actions: preflight-only; execution assert_not_called.
- Orchestrator no-domain-work: trace asserts `orchestrator_does_domain_work=false`.
- Isolated context: one context pack per worker.
- Worker failure isolation: failed worker reduced with successful workers.
- Clean main bubble: no thought/observation exposed.
- End-to-end: >=2 workers dispatch and reduce.

## G3 Traceability
- Blueprint section 12 Agent Teams row updated for commit `4a4eb15c` and G1-central `research-assistant-agent-teams_20260601_170018_l3_ra-phase5-agent-teams_52455a77_runner-validation__ec27d8d309`.
