# Paper v2 QE Runtime Contract Enforcement

Date: 2026-05-05
Module: Paper v2 + Selection Center + StrategyPackage
Level: L3 backend regression

## Objective

Close the P0 consistency gap found in `20260505_l2_paper-v2-qe-strategy-consistency-audit.md`.

Business rule validated:

- Paper v2 must execute the same QE backtest runtime contract frozen in the StrategyPackage.
- Paper v2 cannot activate or select a minute execution policy that differs from `manifest.minute_execution_policy`.
- Paper v2 runtime profile values that affect strategy behavior, including TopK, HMM, industry blacklist, suspend filtering, and risk-policy enablement, must match the QE contract.
- Missing QE-configured runtime features must be populated from the frozen manifest rather than silently disabled.
- No production backend port `8001` was restarted or used for validation.

## Implemented Scope

- Added `backend/services/strategy_package/backtest_contract.py` to normalize the frozen QE package contract and enforce it in Paper v2.
- Added Paper target construction for QE `score_weighted_topk_v1/v2`, including score-weighted sizing, dynamic n-drop, V2 ghost sells, hold-threshold checks, sell target tracing, and current-price requirements for retained holdings.
- Day runner, readiness, live session, session creation/capability checks, portfolio creation, execution-policy listing, execution-policy activation, runtime-profile creation, runtime-profile versioning, and runtime-profile activation now reject mismatches.
- Runtime config normalization now applies QE HMM / blacklist / suspend / risk contract values before Paper execution.
- StrategyPackage execution policy paper-enable now rejects policies that do not match the frozen manifest execution policy.

## Automated Validation

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/selection_center/risk_policy.py backend/services/selection_center/runtime_profile.py backend/services/selection_center/service.py backend/services/selection_center/models.py backend/services/strategy_package/backtest_contract.py backend/services/strategy_package/runtime.py backend/services/strategy_package/service.py backend/services/paper_trading_v2/service.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/readiness.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/session.py
```

Result: passed.

```powershell
pytest backend/tests/strategy_package/test_backtest_contract.py -q -p no:cacheprovider
```

Result: `3 passed in 0.49s`.

```powershell
pytest backend/tests/paper_trading_v2 backend/tests/strategy_package/test_manifest_v1.py -q -p no:cacheprovider
```

Result: `70 passed in 1.14s`.

```powershell
pytest backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
```

Result before rebase: `138 passed in 18.49s`.

```powershell
pytest backend/tests/selection_center/test_risk_policy.py backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

Result before rebase: `183 passed in 20.30s`.

Re-run after rebasing/cherry-picking onto `origin/main`:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
# TDX_DB_PASSWORD was configured in the local environment for DB-backed tests.
pytest backend/tests/selection_center/test_risk_policy.py backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

Result: `180 passed in 18.87s`.

```powershell
conda run -n AIstock python -m nox -s paper_v2_backend
```

Result before rebase: successful, `138 passed in 14.53s`.

Re-run after rebasing/cherry-picking onto `origin/main`:

```powershell
conda run -n AIstock python -m nox -s paper_v2_backend
```

Result: successful, `137 passed in 23.61s`.

```powershell
conda run -n AIstock python -m nox -s paper_v2_data_quality
```

Result: successful. Paper v2 / Selection Center schema, dataset-refresh audit freshness, package readiness, selection traceability, and Paper run traceability gates passed. The smoke reported a pre-existing non-strict legacy ledger consistency warning and did not fail the gate.

```powershell
conda run -n AIstock python -m nox -s l0
```

Result before rebase: successful. Non-blocking existing guardrail findings were reported for raw-JSON UI and baseline script/complexity checks; blocking count was 0.

Re-run after rebasing/cherry-picking onto `origin/main`:

```powershell
python scripts/aistock_guardrail_scan.py --baseline --output-json tmp/validation/guardrails/baseline_20260504.json
conda run -n AIstock python -m nox -s l0
```

Result: successful. The clean push worktree needed a local guardrail baseline file first; after generating it, L0 reported only baseline/non-blocking findings and blocking count was 0.

## Business Outcomes Verified

- Requested Paper execution policy that differs from the QE manifest is rejected.
- Matching requested execution policy can create a portfolio and per-date activation can be used.
- Paper execution-policy listing marks mismatched policies as not enterable.
- Runtime TopK mismatch is rejected.
- QE-configured HMM runtime is populated into Paper runtime profile with snapshot, preset, and coefficient path.
- QE-configured industry blacklist and suspend filtering are populated into Paper runtime profile and conflicts are rejected.
- QE-configured risk policy can force existing-position exits, and risk forced exits override strategy sell metadata.
- Paper target generation no longer uses artifact equal-weight targets when a StrategyPackage manifest is available; it uses the QE ScoreWeightedTopk contract.
- Retained existing positions require real current prices; no avg-cost/default-price fallback is used for retained targets.

## Asset Safety

- No StrategyPackage manifests, validated execution policies, QE workspaces, model weights, HMM snapshots, selection artifacts, or Paper v2 ledger/run assets were modified by validation.
- No production FastAPI backend on `8001` was restarted.
- No database migration or data backfill was run.

## Residual Risks

- The Paper adapter implements the current supported QE ScoreWeighted V1/V2 semantics; future QE strategy families must be added to `backtest_contract.py` before Paper can run them.
- Current persisted Paper portfolios that already carry mismatched execution policies or runtime profiles will now fail fast until they are recreated from a package whose frozen contract matches the intended runtime.
- Stock-pool freeze remains out of scope until the QE stock-pool freeze design is finalized.
