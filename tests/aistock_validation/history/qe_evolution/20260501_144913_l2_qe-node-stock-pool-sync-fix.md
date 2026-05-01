# L2 QE Node Stock Pool Sync Fix - 2026-05-01

## Scope

- Fix QE custom evolution / multi-node submission failure: `node rdagent-node1 missing ssh_user; cannot sync stock_pool`.
- No real QE training, backtest, RD-Agent task creation, or protected QE artifact mutation was executed.
- Production backend port 8001 and frontend service were not restarted.

## Business Risk

- False failure risk: selected remote node has valid SSH identity in `infra.compute_nodes`, but preflight node rows did not carry `ssh_user` into stock-pool sync.
- False failure risk: RD-Agent workspace API returns valid qlib paths, but the sync helper only looked at top-level DB fields.
- Silent fallback risk: deriving SSH user must be deterministic from one unambiguous `/home/<user>` path and must fail fast if ambiguous.

## Changes Verified

- `get_compute_node()` now selects `ssh_user`.
- QE node preflight merges missing workspace paths from RD-Agent `/config` response into the returned node row.
- Stock-pool sync accepts top-level node paths or nested `workspace_config` paths.
- Missing `ssh_user` is resolved only from one unambiguous Linux home user; ambiguous or absent users still fail fast.
- Dispatch node create/update and registration UI can persist `ssh_user` for future nodes.

## Commands

```text
python -m py_compile backend/services/quantevolver/node_execution.py backend/services/quantevolver/stock_pool_sync.py backend/routers/dispatch.py backend/services/dispatch_service.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
cd frontend && npm exec tsc -- --noEmit
git diff --check -- backend/services/quantevolver/node_execution.py backend/services/quantevolver/stock_pool_sync.py backend/routers/dispatch.py backend/services/dispatch_service.py frontend/src/app/rdagent/dispatch/components/NodeRegisterDialog.tsx backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py
```

## Results

```text
Check                          Result
-----------------------------  ---------------------------------------------
Python compile                 PASS
QE targeted pytest             PASS: 29 passed, 3 existing warnings
Frontend TypeScript            PASS
Whitespace diff check          PASS
Real QE execution              NOT RUN by design
Protected QE artifacts         NOT MODIFIED
Production 8001 restart        NOT DONE
```

## Guardrail Review

- No new empty-result success, default price/cash/holding, daily fallback, TWAP fallback, or fake success path was added.
- The new SSH user derivation is not silent: it logs a warning when used, validates username format, and raises on ambiguity or absence.
- `git diff` scan for added `except Exception`, `return None`, `fallback`, `silently`, `兜底`, and `pass` found only non-business test/context matches after review.

## Residual Risk

- The real remote sync path still depends on SSH/SCP reachability and WSL local filtered_pool file existence; this was not exercised because the user requested no real QE run.
- Existing Pydantic deprecation warnings in `quantevolver_evolution.py` remain unrelated to this fix.

## Additional Distributed Mutation Recheck

After rechecking rerun / append / clone distributed custom_evo paths, one dry-run bug was found and fixed:

- Partial mutation routes used to validate `node_parallelism` only against the submitted replacement/new Loop subset. In a distributed task, UI may submit the full task map such as `{node-a: 2, node-b: 3}` while rerunning only `Loop2` on `node-b`; this could raise `QE_NODE_PARALLELISM_UNKNOWN_NODE` for the non-mutated node.
- The router now resolves the full post-mutation node scope from existing Loop configs plus the replacement/new Loops, validates parallelism against that full scope, but still preflights stock_pool sync only for nodes touched by the submitted Loops.
- The service now validates the full rerun config before deleting old Loop results, preventing cleanup from happening before a config validation failure.
- Frontend rerun/append keeps existing task-level node parallelism values instead of filtering them down to only the currently visible mutation Loops.

Additional commands rerun:

```text
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/node_execution.py backend/services/quantevolver/stock_pool_sync.py backend/routers/dispatch.py backend/services/dispatch_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py
python -m pytest backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_qe_node_execution.py -q
cd frontend && npm exec tsc -- --noEmit
```

Additional result:

```text
Check                          Result
-----------------------------  ---------------------------------------------
Distributed rerun route        PASS: preserves full node_parallelism map
Distributed append route       PASS: preserves existing + new node map
Distributed clone/create route PASS: preserves per-loop nodes and clone provenance
Rerun cleanup ordering         PASS by code review: validation before cleanup
Targeted pytest                PASS: 32 passed
Frontend TypeScript            PASS
```

## Checksum Output Parsing Fix

A real submission reported a false checksum mismatch where the digest was identical but the local value included the file path from `sha256sum` output:

```text
local=<hash>  /home/lc999/data/qlib_bin/instruments/filtered_pool_20260501.txt
remote=<same hash>
```

Fix:

- Parse both local and remote `sha256sum` output in Python by taking the first whitespace-delimited token.
- Validate the parsed digest is exactly 64 lowercase hex characters.
- Remove dependence on shell `awk` output formatting for this business check.
- Keep fail-fast behavior for invalid checksum output or real digest mismatch.

Validation:

```text
python -m py_compile backend/services/quantevolver/stock_pool_sync.py backend/tests/unified_engine/test_qe_config_truth.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_node_execution.py -q
```

Result:

```text
Check                          Result
-----------------------------  ---------------------------------------------
Checksum parser regression     PASS: local full sha256sum line parses to hash
QE targeted pytest             PASS: 32 passed
Real QE execution              NOT RUN by Codex
```
