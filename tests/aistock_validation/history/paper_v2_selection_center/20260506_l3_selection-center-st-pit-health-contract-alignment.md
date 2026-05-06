# Selection Center ST PIT Health Gate And Contract Alignment

Date: 2026-05-06
Module: Paper v2 + Selection Center + StrategyPackage
Level: L3 backend/frontend regression
Worktree: `F:/Dev/AIstock_worktrees/selection-st-pit-health-20260506`
Branch: `codex/selection-st-pit-health-20260506`

## Objective

Prevent operators from discovering Selection Center blockers by repeated manual clicks, and align direct Selection Center runs with the same frozen QE backtest contract already enforced by Paper v2.

Business rules validated:

- Selection Center exposes per-package health in the selectable package list.
- ST PIT authoritative Selection Center runs reject legacy packages whose frozen manifest lacks the ST PIT risk-policy contract.
- Direct Selection Center runs inherit the package QE backtest risk policy and engine TopK from the frozen manifest.
- UI display TopN is separated from engine TopK and cannot silently override QE strategy behavior.
- No frozen StrategyPackage manifest, model weight, HMM snapshot, Qlib dataset, validated policy, or production backend process was modified.

## Implemented Scope

- Added `backend/services/selection_center/package_health.py` for conservative package health summaries.
- `SelectionCenterService.list_selectable_packages()` now returns `selection_health` for each package.
- `SelectionCenterService.run_packages()` now prepares effective per-package runtime configs before creating a run, persists `package_runtime_configs` and `package_health`, and gates ST PIT authoritative runs through health preflight.
- Selection Center now uses `normalize_runtime_config_with_backtest_contract(...)` in ST PIT authoritative mode, so risk policy, tradability, HMM, industry blacklist, and engine TopK are inherited from the frozen QE package contract.
- Paper v2 Selection UI sends `st_pit_authoritative: true`, shows package health, disables blocked/legacy packages, blocks run submission when selected packages are not runnable, and slices only displayed rows by UI TopN.
- Paper v2 status formatting now labels/tints `RUNNABLE`, `WARN`, `LEGACY_NON_ST_PIT`, `CACHE_ONLY`, and `PAPER_DATA_BLOCKED`.
- The implementation note remains in `docs/analysis/selection_paper_st_pit_alignment_and_blockers_20260506.md`.

## Automated Validation

```powershell
python -m py_compile backend/services/selection_center/package_health.py backend/services/selection_center/service.py backend/tests/selection_center/test_runtime_selection.py
```

Result: passed.

```powershell
python -m pytest backend/tests/selection_center backend/tests/strategy_package backend/tests/paper_trading_v2 -q -p no:cacheprovider
```

Result: `142 passed in 27.89s`.

```powershell
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider
```

Result: `49 passed in 27.43s`.

```powershell
cmd /c frontend\node_modules\.bin\tsc.cmd -p frontend\tsconfig.json --noEmit --incremental false --pretty false
```

Result: passed. The worktree uses an ignored `frontend/node_modules` junction pointing to the existing dependency install under `F:/Dev/AIstock/frontend/node_modules`; no dependency files were committed.

```powershell
npm run build
```

Result: passed. Next.js built 64 app routes, including `/paper-v2/selection`.

```powershell
git diff --check
```

Result: passed with only line-ending warnings for existing Windows checkout behavior.

Guardrail scans:

```powershell
rg -n -i "(api[_-]?key|secret|password|token\s*=|bearer|private_key)" <changed task files>
rg -n -i "(silent|fallback|pad|truncate|default price|fake success|empty.*success|qe_workspace|rdagent_workspace)" <changed runtime files>
```

Result: no secret findings. The only silent-fallback guard hit is the intentional health message: `never padded or truncated by Selection Center`.

## Business Outcomes Verified

- A ST PIT contract package inherits `runtime_profile.risk_policy.enabled=true` from the frozen manifest even when the UI omits risk-policy fields.
- UI `top_k=1` becomes `display_top_n=1`; engine `runtime_profile.selection.top_k` remains the QE manifest TopK.
- A risk-policy `block_buy` decision removes the blocked symbol and records `risk_policy_block_buy` in exclusions.
- A legacy non-ST-PIT package is rejected in ST PIT authoritative mode before selection execution.
- Selectable package responses include `selection_health`, enabling the UI to show and disable blocked or legacy packages.
- Frontend production build proves the new Paper v2 Selection UI compiles and routes correctly.

## Asset Safety

- No production FastAPI backend on port `8001` was restarted or touched.
- No StrategyPackage frozen manifests, model weights, HMM coefficient snapshots, QE/RD-Agent worker workspaces, Qlib datasets, validated execution policies, or Paper ledgers were modified.
- Validation used only code/tests/frontend build outputs in the development worktree. Ignored `.next/` and `node_modules/` build/dependency artifacts are not part of the patch.

## Remaining Blockers / Not Yet Claimed Fixed

- Existing selectable packages scanned earlier on 2026-05-06 are legacy from the ST PIT risk-contract perspective; they should be rebuilt from new ST PIT QE backtests or kept visibly legacy.
- `qe_20260502_231229_0565` / `pkg_1de...` still needs feature-completeness repair or rebuild; the prior dry-run kept 0 fully scored rows.
- `pkg_006...` still needs exact 63-feature schema recovery or retirement/rebuild; no padding/truncation is allowed.
- `pkg_991...` and `pkg_b668...` still need cold-cache materialization repair if they must remain runnable beyond this workstation cache.
- HMM-on Selection still needs coefficient artifact sector-mapping coverage preflight/repair.
- Paper v2 current-day run/readiness can still be blocked if same-day `stk_limit` audit has failed.
- Production activation still requires explicitly restarting/reloading backend `8001` and running a production smoke; this validation did not activate production.
