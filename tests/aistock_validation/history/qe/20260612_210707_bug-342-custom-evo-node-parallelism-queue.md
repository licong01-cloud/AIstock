# QE custom_evo node_parallelism queue validation

- Module: quantevolver / qe custom_evo
- Level: L3 read-only validation with UI skipped
- Date: 2026-06-12T21:07:07+08:00
- BUG: BUG-342
- Git commit at validation start: 59a79781
- Operator: lc999

## Scope

- Changed files: backend/services/quantevolver/qe_evolution_service.py; backend/tests/unified_engine/test_qe_config_truth.py; BUG-342 JSON; QE validation history.
- Impacted flows: custom_evo retry, rerun, append, resume, selected-loop start, and normal custom_evo start admission control.
- Business goal: when multiple loops in the same experiment are retried or submitted and the same-node active count reaches node_parallelism, extra loops must remain pending/queued and automatically continue after capacity frees.
- Out of scope: production service restart, production DB writes, DDL, and UI-only acknowledgement copy.

## Commands And Results

```bash
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_slot_queues_active_retry_on_same_node backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_slot_allows_when_node_has_capacity backend/tests/unified_engine/test_qe_config_truth.py::test_retry_loop_queues_until_custom_evo_parallelism_before_executor_submit backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_queue_helper_marks_pending_then_running backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_queue_helper_waits_then_runs backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_queue_helper_does_not_resubmit_active_loop -q
# 6 passed in 7.43s

python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
# 92 passed in 10.17s

python -m ruff check backend/services/quantevolver/qe_evolution_service.py backend/tests/unified_engine/test_qe_config_truth.py
# All checks passed

python -m nox -s validation_module_registry_l0 --no-error-on-missing-interpreters
# success; 8 passed

python -m nox -s l0 --no-error-on-missing-interpreters
# success; blocking=0

QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3 --no-error-on-missing-interpreters
# success; qe_read_l3 success and qe_read_backend 14 passed in 10.38s

git diff --check
# passed
```

## Evidence Summary

- Same-node over-limit admission now returns available=false so callers can queue instead of raising QE_CUSTOM_EVO_NODE_PARALLELISM_LIMIT.
- Shared queue helper writes pending for over-limit loops, sleeps/polls, then atomically transitions a loop to running only when capacity is available.
- retry_loop calls the queue helper before executor.submit, so multiple manual retries in the same experiment serialize by node_parallelism.
- The queue helper uses RETURNING status and a terminal/active guard to avoid resubmitting a loop that became running, processing, or completed while queued.
- Required workflow gates passed: validation_module_registry_l0, l0, and qe_read_l3 with UI skipped.

## Result

- Final status: passed for BUG-342 PR validation.
- Remaining risk: no live DB/RD-Agent integration run was executed in this read-only validation; the queue transition is covered by scheduler-level unit tests plus QE read-only gates.
- Need production backend restart: no.
- Need production frontend restart: no.
- Production DB/DDL touched: no.
