# Selection / Paper v2 ST PIT Alignment And Blockers - 2026-05-06

## Scope

This note records the current Selection Center and Paper v2 blockers found during
the 2026-05-06 scan, and analyzes whether the new ST PIT stock-pool semantics
used by QE backtests must be migrated into Selection Center and Paper v2.

Evidence from the read-only/dry-run scan is under:

- `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506/db_snapshot.json`
- `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506/asset_preflight.json`
- `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506/dry_run_blockers.json`
- `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506/cold_cache_probe.json`
- `F:/Dev/AIstock_artifacts/selection_blocker_scan_20260506/summary.md`

The scan did not restart production backend port `8001`, did not create a new
`selection.run`, and disabled bottom-layer signal DB writes during WSL inference
dry-runs.

## Current Architecture Facts

### QE Backtest

- New runnable QE configs now force `risk_policy.enabled=true` with provider
  `st_pit`, hard actions `block_buy` and `force_exit`, universe key
  `shsz_st_pit_active_v1`, and strict PIT data readiness.
- QE backtest runtime materializes `qe_event_risk_policy.json`, filters buy
  scores through the ST PIT policy, and appends explicit forced-exit sell orders
  for current holdings that leave the PIT universe when orderable.
- Completed historical experiments are not migrated in-place. StrategyPackages
  created from old experiments may not contain the new ST PIT risk contract.

### Selection Center

- Selection Center has a shared runtime risk-policy implementation backed by
  `market.stock_universe_pit_spans`.
- However, direct Selection Center runs currently normalize only
  `runtime_profile`; they do not automatically inherit the frozen QE backtest
  contract from the StrategyPackage.
- The current Paper v2 selection UI sends `runtime_profile.selection.top_k`,
  `tradability`, `industry_blacklist`, and `hmm`, but does not send
  `runtime_profile.risk_policy`. The default parsed risk policy is
  `enabled=false`.
- Therefore direct Selection Center runs can diverge from a new ST PIT QE
  backtest unless the caller explicitly passes a matching risk policy.

### Paper v2

- Paper v2 day runner, readiness, and live-session paths call
  `normalize_runtime_config_with_backtest_contract(...)` before execution.
- This injects and validates QE backtest runtime features including topK,
  suspension filtering, HMM, industry blacklist, and `risk_policy`.
- Paper v2 also has a target-position engine that ports the supported QE
  `score_weighted_topk_v1/v2` contract.
- This means Paper v2 is closer to QE contract parity than direct Selection
  Center, but it is still an independently implemented Python engine rather
  than literally the same Qlib strategy code. It needs parity tests against QE
  decisions.

## Existing Package Contract Status

The currently selectable packages scanned on 2026-05-06 are all legacy from the
ST PIT risk-contract perspective:

| Package | Source | Manifest `custom_params.risk_policy` | Manifest stock pool | Implication |
| --- | --- | --- | --- | --- |
| `pkg_1de32357724a4c5b874f2abd90f22da5` | `qe_20260502_231229_0565` / `Loop1` | missing | `filtered_pool_20260503` | Not an ST PIT risk-contract package; also blocked by live feature completeness. |
| `pkg_99142cb1440c40a7824e83902f4e7da9` | `qe_20260416_082012` | missing | `filtered_pool_20260416` | Legacy contract; can run from current cache but not ST PIT aligned. |
| `pkg_006a42323f7c4e81a468fdaad2cb16a3` | `qe_20260413_084216` | missing | `filtered_pool_20260413` | Legacy contract; blocked by feature schema mismatch. |
| `pkg_b668f8a633c44b72a5d557a2cb8970e3` | `qe_20260416_002701` | missing | `filtered_pool_20260416` | Legacy contract; can run from current cache but not ST PIT aligned. |

These packages should not be silently "upgraded" by mutating their frozen
manifests, because that would make Paper v2 no longer reproduce the original
backtest contract. They should either remain explicitly marked as legacy, or be
re-created from new ST PIT QE backtests.

## Recorded Selection Blockers

| Priority | Area | Finding | Current Effect |
| --- | --- | --- | --- |
| P0 | Production backend `8001` | The process observed during the scan had started before the pushed `qe_evolution_loop` source-resolution fix. | UI may still return `QE experiment does not exist for live inference` until `8001` is restarted. |
| P0 | `pkg_1de...` / `qe_20260502_231229_0565` | Source now resolves to real experiment `qe_20260502_231229_0565_L1`, but strict WSL inference keeps `0 / 4636` rows after feature filtering (`invalid_cell_count=13047`). | After restart, the old 404 should move to a new failure: no fully scored instruments. |
| P0 | `pkg_006...` / `qe_20260413_084216` | Model expects `63` features but the live runtime prepares `52`; strict mode refuses padding/truncation. | Package cannot be used for authoritative live selection until the true training schema is recovered or the package is retired/rebuilt. |
| P1 | `pkg_991...` and `pkg_b668...` | Existing local runtime cache can run actual WSL prediction (`1955` and `1964` score rows respectively). | Usable on this machine only while the local cache remains present. |
| P1 | `pkg_991...` and `pkg_b668...` cold cache | Empty-cache materialization fails because the QE node API returns 404 for `mlruns-params`. | New machine/cache cleanup can break these packages even though the current machine runs them. |
| P1 | HMM-enabled selection | Historical failures show `HMM coefficient artifact is missing stock sector mapping`. | HMM-off runs are unaffected; HMM-on selection still needs coefficient artifact repair/preflight. |
| P1 | Paper v2 data readiness | `suspend_d` for `2026-05-06` is successful, but `stk_limit` audit for `2026-05-06` is failed with zero rows. | Selection is not directly blocked; Paper v2 day-run/readiness for today can fail. |

## Does Selection / Paper v2 Need ST PIT Migration?

Yes, but the migration should be a contract-driven alignment, not a silent
rewrite of old StrategyPackage manifests.

Required alignment:

1. New QE backtests, StrategyPackage selection, and Paper v2 execution must all
   consume the same frozen runtime contract: `risk_policy.enabled=true`,
   `providers=['st_pit']`, `hard_actions=['block_buy','force_exit']`,
   `st_universe_key='shsz_st_pit_active_v1'`, and strict data readiness.
2. Selection Center should inherit the same StrategyPackage backtest contract
   that Paper v2 already inherits. Otherwise the operator can see a selection
   list that Paper v2 will not actually trade.
3. Paper v2 should reject or clearly label legacy packages that do not contain
   the ST PIT contract when the product mode is "ST PIT authoritative".
4. Existing legacy packages should remain reproducible under their original
   contract or be re-created from new ST PIT QE runs; they should not be
   backfilled by editing frozen manifests.
5. A parity validation should compare QE backtest decisions and Paper v2 target
   decisions for the same date, prediction scores, holdings, ST PIT spans,
   suspend/limit state, and execution policy.

## Recommended Fix Plan

### P0 - Stop More Manual Trial And Error

- Add a StrategyPackage health/preflight endpoint and UI status column:
  `source_resolves`, `runtime_assets_ready`, `model_schema_matches`,
  `strict_feature_kept_rows`, `st_pit_contract_status`, `hmm_artifact_status`,
  and `cold_cache_safe`.
- Disable or label packages that fail P0 checks before the operator clicks
  "Run selection".

### P0 - Contract Alignment

- Apply `normalize_runtime_config_with_backtest_contract(...)` in Selection
  Center single-package execution before artifact generation and signal snapshot
  loading.
- Split "engine topK" from "display topK": the runtime should score/filter with
  the QE contract topK, while the UI may display only the first N results.
- Make Selection Center persist the effective QE backtest runtime contract in
  `selection.run.runtime_config` for traceability.

### P0 - Package Repair / Rebuild

- Rebuild `qe_20260502_231229_0565` or repair its feature-completeness problem
  before using it for selection; do not fill missing values silently.
- Retire or rebuild `pkg_006...` unless the original 63-feature training schema
  can be recovered exactly.
- Re-create production StrategyPackages from new ST PIT QE backtests instead of
  attempting to mutate legacy manifests.

### P1 - Cache And Artifact Reliability

- Fix QE node `mlruns-params` download for older packages or persist a curated
  StrategyPackage model cache asset that survives cache cleanup.
- Add a cold-cache regression test so a package is not considered selectable
  merely because this workstation has stale local runtime cache.

### P1 - Paper v2 Parity Tests

- Add a decision-parity test using a small fixed score table and holdings:
  compare QE `SuspendFilterScoreWeightedTopkStrategyV1/V2` output with Paper v2
  `TargetPositionEngine` output after ST PIT, suspend, missing-close, and limit
  guards.
- Add a live/PIT oracle: a symbol outside `shsz_st_pit_active_v1` must be
  blocked for new buys in both Selection and Paper, and an existing holding
  outside the universe must produce an explicit forced-exit target in Paper.

## Answer To The Operator's Question

The large number of bugs is not because ST PIT itself is wrong. The main cause
is that previous tests validated narrower slices:

- old StrategyPackages and old selection dates;
- current-machine cache rather than cold-cache materialization;
- Selection Center UI success rather than Paper v2 backtest-contract parity;
- source-resolution only, without continuing into strict live feature
  completeness and model-schema validation;
- HMM-off happy paths rather than HMM artifact readiness;
- no pre-click package health gate.

The ST PIT rollout exposed these hidden inconsistencies because the runtime
contract became stricter. Going forward, "selectable" should mean the package
has passed the same contract and data gates that Paper v2 will use, not merely
that the package is in `SELECTION_ENABLED` status.

## Implementation Order Design

### Priority Decision

The current blockers must be handled before claiming the Selection/Paper v2
flow is fixed, but not all blockers must be repaired before implementing the
contract-alignment code.

Required first:

1. Stop manual trial-and-error by adding package health/preflight visibility.
2. Ensure the active backend process contains the `qe_evolution_loop` live
   source-resolution fix before production UI validation.
3. Keep broken packages blocked or clearly labeled before the operator can run
   them.

Can proceed in parallel or after the guard:

1. Selection Center contract alignment can be implemented while package-specific
   repairs are still pending.
2. Legacy-package cold-cache repair is required only if those legacy packages
   must remain usable.
3. HMM artifact repair is required only for HMM-enabled selection.
4. `stk_limit` same-day audit repair is required for Paper v2 day-run/readiness
   on that date, not for Selection Center signal-only runs.

### Allowed Code Patterns To Reuse

- Paper v2 already applies `normalize_runtime_config_with_backtest_contract(...)`
  in day-runner/readiness/live-session paths. Selection Center should reuse this
  helper rather than inventing a second contract merger.
- Selection Center currently enters `run_packages(...)` through
  `normalize_selection_runtime_config(...)`; this is the specific gap to close.
- Runtime risk decisions must keep using `StockRiskPolicyService.evaluate(...)`,
  `apply_to_candidates(...)`, and `forced_exit_targets(...)`; do not introduce a
  second ST filter implementation.
- Health/preflight router work should follow existing router error semantics:
  `TradingCoreError` becomes structured `detail.error_code/message/context`.

### Anti-Pattern Guards

- Do not mutate frozen StrategyPackage manifests to add missing ST PIT policy.
- Do not silently enable ST PIT for legacy packages whose backtest did not use
  it; label or rebuild them instead.
- Do not let UI display topN override the QE engine topK.
- Do not pad/truncate model features to bypass schema mismatch.
- Do not read raw QE/RD-Agent worker workspaces; use AIstock-owned runtime cache
  or explicit node/archive materialization only.
- Do not restart production backend `8001` during validation unless the user
  explicitly requests runtime activation.

### Phase 0 - Worktree And Evidence Baseline

Implementation should start from a clean worktree/branch, not dirty production
root `F:/Dev/AIstock`. Baseline checks:

- `git status --short --branch`
- current selectable package list and manifest risk-policy status
- source-resolution result for each package
- current cache and cold-cache asset status
- same-day `suspend_d` and `stk_limit` audit status for Paper v2

Verification:

- Save a single preflight evidence artifact under `F:/Dev/AIstock_artifacts`.
- No StrategyPackage manifest, model weight, HMM snapshot, Qlib dataset, or
  validated policy is modified.

### Phase 1 - Package Health And UI Gate

Add a read-only health/preflight service and expose it in the selectable package
list or a dedicated endpoint. Required checks:

- `source_resolves`
- `runtime_assets_ready`
- `model_schema_matches`
- `strict_feature_kept_rows`
- `st_pit_contract_status`
- `hmm_artifact_status`
- `cold_cache_safe`
- `paper_data_readiness`

UI behavior:

- show `RUNNABLE`, `BLOCKED`, `LEGACY_NON_ST_PIT`, `CACHE_ONLY`, or
  `PAPER_DATA_BLOCKED`;
- disable run buttons for hard P0 blockers;
- keep legacy packages selectable only in an explicitly labeled legacy mode.

Verification:

- Unit tests for each health status.
- API smoke on a non-production backend port.
- UI test that a blocked package cannot be run by clicking selection.

### Phase 2 - Selection Center Contract Alignment

Update Selection Center package execution so every package runtime config is
normalized through the frozen backtest contract before artifact generation and
snapshot loading.

Design details:

- single-package mode: always apply the package contract;
- multi-package research modes: apply each package's own contract before
  package-level ranking, and reject incompatible contracts only when a future
  executable combined contract is requested;
- persist the effective `qe_backtest_runtime_contract` in
  `selection.run.runtime_config`;
- split engine topK from display topN. The engine topK must match QE contract;
  UI/watchlist may display/export only the first N after the engine result.

Verification:

- Selection test proves ST PIT risk policy is inherited even when UI omits
  `runtime_profile.risk_policy`.
- Selection test rejects conflicting `risk_policy`, `tradability`, HMM,
  industry blacklist, or engine topK overrides.
- Regression test proves old legacy packages remain legacy instead of being
  silently upgraded.

### Phase 3 - Package Repair, Rebuild, Or Retirement

Package-specific decisions:

- `pkg_1de...` / `qe_20260502_231229_0565`: after source fix activation, repair
  or rebuild because strict live inference currently keeps zero fully scored
  instruments. Prefer recreating a StrategyPackage from a fresh post-ST-PIT QE
  backtest instead of forcing old features through the new runtime.
- `pkg_006...`: retire or rebuild unless the exact original 63-feature training
  schema can be recovered. Do not pad/truncate from 52 to 63.
- `pkg_991...` and `pkg_b668...`: either repair cold-cache materialization or
  label them `CACHE_ONLY`/legacy. Current local-cache success is not enough for
  production trust.
- HMM-on packages: block until coefficient artifacts include complete
  stock-sector mapping and date coverage.

Verification:

- one package-level health report per package;
- one successful authoritative selection dry-run for every package that remains
  enabled;
- one cold-cache materialization regression for packages marked runnable.

### Phase 4 - Paper v2 QE Parity And Data Readiness

Add a parity harness for the target-decision layer:

- fixed score table;
- current holdings including a symbol outside ST PIT;
- suspend/limit/missing-price cases;
- ScoreWeighted V1 and V2 parameter sets;
- compare QE risk-filter/forced-exit decisions with Paper v2
  `TargetPositionEngine` output.

Paper v2 operational readiness:

- readiness must surface `stk_limit` and `suspend_d` audit failures before
  running;
- a same-day run cannot start if required execution-policy data is not ready;
- forced exits remain explicit targets but execution may still fail/wait on
  suspend/limit/no-bar market states.

Verification:

- unit parity tests for V1/V2;
- Paper readiness test for missing `stk_limit`;
- day-run or replay smoke on a dev port with a package that passes health.

### Phase 5 - End-To-End Validation And Production Activation

Only after Phases 1-4 pass:

- run backend regression for strategy package, selection center, Paper v2, and
  QE risk-policy tests;
- run frontend TypeScript/build and Paper v2/Selection UI smoke;
- run read-only DB/data-quality smoke;
- write validation evidence;
- then, if the user requests production activation, restart/reload production
  `8001` and perform one production smoke.

Success definition:

- no package appears "ready" if it cannot pass its own health gate;
- Selection Center and Paper v2 consume the same frozen runtime contract;
- legacy packages are visible as legacy, not silently upgraded;
- a post-ST-PIT package can produce authoritative selection and Paper v2
  readiness/day-run decisions under the same contract.

## Implementation Progress - 2026-05-06

Implemented in branch `codex/selection-st-pit-health-20260506`:

- Phase 1 partial/primary path: added SelectionPackage health summaries to the
  selectable-package response and Paper v2 Selection UI. The UI now labels
  `RUNNABLE`, `WARN`, `BLOCKED`, and `LEGACY_NON_ST_PIT` states and disables
  non-runnable packages before run submission.
- Phase 2: direct Selection Center ST PIT authoritative runs now normalize each
  package runtime config through `normalize_runtime_config_with_backtest_contract(...)`,
  persist `package_runtime_configs` and `package_health`, and reject legacy
  non-ST-PIT packages rather than silently enabling ST PIT on old manifests.
- UI TopN is separated from engine TopK by sending `display_top_n`; the engine
  TopK remains inherited from the frozen QE contract.
- Validation evidence is recorded in
  `tests/aistock_validation/history/paper_v2_selection_center/20260506_l3_selection-center-st-pit-health-contract-alignment.md`.

Remaining after this implementation:

- Package-specific repair/rebuild/retirement remains required for `pkg_1de...`,
  `pkg_006...`, `pkg_991...`, and `pkg_b668...` according to the blocker table
  above.
- HMM-on package health still needs a coefficient sector-mapping artifact check.
- A cold-cache materialization regression is still required before any legacy
  cache-dependent package can be called production-runnable.
- Paper v2/QE decision parity tests remain the next phase before claiming full
  engine parity.
- Production activation still requires an explicit `8001` restart/reload and a
  production smoke after code is merged.
