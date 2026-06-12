# QE custom_evo node_parallelism retry regression validation

- Module: quantevolver / qe custom_evo
- Level: L3 read-only validation with UI skipped
- Date: 2026-06-12T18:48:07+08:00
- BUG: BUG-342
- Git commit at validation start: 77e4c52c
- Operator: lc999

## Scope

- Changed files: backend/services/quantevolver/qe_evolution_service.py; backend/tests/unified_engine/test_qe_config_truth.py; BUG-342 JSON and QE validation history.
- Impacted flows: custom_evo retry, rerun, append, resume, and selected-loop start admission control.
- Business goal: retry/rerun/start must not exceed per-node node_parallelism when another loop is already running or processing on the same node.
- Out of scope: production service restart, production DB writes, DDL, and UI-only immediate retry acknowledgement behavior.

## Commands And Results

```bash
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_slot_rejects_active_retry_on_same_node backend/tests/unified_engine/test_qe_config_truth.py::test_custom_evo_parallelism_slot_allows_when_node_has_capacity backend/tests/unified_engine/test_qe_config_truth.py::test_retry_loop_checks_custom_evo_parallelism_before_running_status_update -q
# 3 passed in 7.25s

python -m ruff check backend/services/quantevolver/qe_evolution_service.py backend/tests/unified_engine/test_qe_config_truth.py
# All checks passed

python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
# 89 passed in 7.86s

python -m nox -s validation_module_registry_l0 --no-error-on-missing-interpreters
# success; 8 passed

python -m nox -s l0 --no-error-on-missing-interpreters
# success; blocking=0

QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3 --no-error-on-missing-interpreters
# success; qe_read_l3 success and qe_read_backend 14 passed in 8.81s

git diff --check
# passed
```

## Evidence Summary

- Unit regression rejects a custom_evo retry when active loops on the same target node already equal node_parallelism.
- Unit regression allows retry/start when same-node active count is below node_parallelism.
- retry_loop source-order regression confirms slot enforcement occurs before setting a loop to running.
- qe_read_l3 ran read-only with UI skipped; backend QE read validation passed.

## Result

- Final status: passed for BUG-342 pre-PR validation after merging latest origin/main.
- Remaining risk: the existing FastAPI retry endpoint schedules retry work as a background task, so API/UI may acknowledge scheduling before the background retry fails admission; scheduler-level enforcement prevents actually starting over-limit loops.
- Need production backend restart: no.
- Need production frontend restart: no.
- Production DB/DDL touched: no.
