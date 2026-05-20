# BUG-086 Runtime Config Activation Boundary ????

## ????

- BUG: `BUG-086`
- GitHub: `#108` https://github.com/licong01-cloud/AIstock/issues/108
- ??: `bug/BUG-086-runtime-config-activation-boundary`
- Worktree: `F:\Dev\AIstock_worktrees\bug-086-runtime-config-activation-boundary`
- ??: `paper_v2`, `selection_center`, `strategy_package`
- ????: ??? `8001` / `3000`???????????????? MiniQMT ?????

## ????

- Selection Center ??????????????????????? `runtime_config`????????????????? `trade_enabled=false`?
- Paper v2 / MiniQMTSim / live session / readiness / day runner ?????????? `runtime_profile_binding`?????????? hash?
- Paper v2 ?? active runtime profile activation ????????????????????????? `runtime_profile` / `top_k` / HMM / risk policy ??????? fail-fast?
- Selection ????????? Paper v2 portfolio??? preview ??????????

## DESIGN-COMPLIANCE-001

| ??/Issue ?? | ???? | ???? | ?? | Gap/?? |
|---|---|---|---|---|
| Runtime ???????? profile/policy/release version reference ? hash | `backend/services/selection_center/runtime_profile.py`, `backend/services/paper_trading_v2/service.py` | `test_selection_center_rejects_unversioned_trading_runtime_config`, `test_paper_day_runner_rejects_unversioned_runtime_profile_override`, `test_paper_day_runner_rejects_platform_default_binding_for_behavior_override` | PASS | ? |
| Selection Center / Paper v2 / MiniQMT ??? exact version/hash | `backend/services/selection_center/service.py`, `backend/services/paper_trading_v2/service.py`, `backend/services/paper_trading_v2/day_runner.py`, `backend/services/paper_trading_v2/readiness.py` | `test_runtime_profile_activation_is_copied_into_day_run`, `test_replay_only_session_create_tick_and_progress`, MiniQMT readiness/day-run runtime activation assertions | PASS | Readiness ?? `runtime_config_keys`???? activation repository ??????????????????????? API ?? |
| Preview/diagnostic flow ???? non-trading??????????? durable ledger | `mark_non_trading_preview_runtime_config`, `create_paper_portfolio_from_run` gate | `test_selection_center_non_trading_preview_is_marked_and_cannot_create_paper_portfolio` | PASS | ? |
| Legacy top-level runtime keys ???? activation | `BEHAVIOR_CHANGING_RUNTIME_CONFIG_KEYS`, `ensure_runtime_config_version_boundary` | Selection/Paper regression tests and `paper_v2_backend` suite | PASS | ? |
| ?????/POC/???? | ?????????????? Selection/Paper/Readiness/DayRunner/Session/LiveSession/MiniQMT | `paper_v2_backend` 447 passed, `l0` passed, guardrails passed | PASS | ? |

## ?????

- `python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/paper_trading_v2/test_runtime_profile.py backend/tests/paper_trading_v2/test_session.py backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_live_session.py -q`
  - Result: `125 passed in 1.47s`
- `python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package -q -p no:cacheprovider`
  - Result: `447 passed, 1 skipped, 2 xfailed in 27.13s`
- `python -m nox -s paper_v2_backend`
  - Result: `447 passed, 1 skipped, 2 xfailed in 21.32s`; session successful
- `python -m nox -s guardrail_changed_files -- --changed-only`
  - Result: session successful; one P2 `ALGO-COMPLEXITY-001` informational finding on existing watchlist label construction, blocking=0
- `python -m nox -s l0`
  - Result: session successful; baseline/new P2 UI raw JSON findings are outside changed Paper v2 backend scope, blocking=0
- `git diff --check`
  - Result: pass; only Git CRLF warnings

## ????

- ???? `runtime_config={"runtime_profile": ...}` ? Paper v2 day runner ?? run ???????
- Selection Center ????????????? runtime profile binding??? preflight tests ???????????
- Paper v2 active runtime profile activation ????? `runtime_profile_activation` ? `runtime_profile_binding`???? day run / session / MiniQMT run context?
- ????????? Selection????? `runtime_config_scope=non_trading_preview` ? `trade_enabled=false`?????? Paper portfolio?

## ????

- ?????????? DB migration????? DB?
- ?????? `8001`/`3000`?
- ???? BUG-086 ??????BUG registry ???????
