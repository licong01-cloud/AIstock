# Validation Center / Coverage Contract Validation Matrix

This matrix covers the first-stage complete loop for AIstock validation-pipeline coverage contracts. The scope is coverage parsing, gate evaluation, evidence output, and run-record compatibility. It does not start backend/frontend services and does not touch production port `8001`.

## Production Isolation Rules

- Do not restart production backend `8001`.
- Do not restart remote APIs.
- Do not write to business schemas or modify protected trading/QE assets.
- Coverage parsing must use local report files and synthetic fixtures in tests.
- A failed coverage gate must be explicit in JSON evidence and must not be hidden by a silent fallback.

## L1/L2 Coverage Contract

Required coverage for the first-stage complete loop:

- `scripts/aistock_validate.py coverage` accepts exactly one source report: `--coverage-xml` or `--coverage-json`.
- Coverage snapshot JSON uses schema `aistock_validation_coverage_snapshot_v1`.
- Snapshot includes module, level, title, run id, git commit, operator, source report path, output path, totals, file-level details, diff coverage, quality gates, failed gates, and status.
- XML parser supports Coverage.py/Cobertura line hits and branch `condition-coverage` counts.
- JSON parser supports Coverage.py JSON files with executed/missing lines and summary branch totals.
- Thresholds are percent values in `[0, 100]`; invalid thresholds fail fast.
- `line`, `branch`, and `diff_line` gates fail when configured thresholds are not met.
- Branch thresholds fail when branch coverage is unavailable; they must not silently pass.
- Diff coverage can read a unified patch file or `git diff --unified=0` from a base ref.
- Diff coverage fails when changed files are missing from the coverage report.
- `--no-fail` can record a failed gate as evidence while returning zero for baseline-only runs.
- `record` metadata keeps a coverage placeholder with the same coverage snapshot schema and `status=not_collected`.
- `validation_coverage_backend` runs targeted pytest with `pytest-cov`, writes XML/JSON coverage reports under `tmp/validation/coverage/`, then gates the parser itself with 70% line and 55% branch thresholds.

## Nox Entry Points

```powershell
python -m nox -s validation_coverage_backend
python -m nox -s validation_center_backend
python -m nox -s validation_center_ui
python -m nox -s validation_center_live_readonly
python -m nox -s qe_data_contract_backend
python -m nox -s l0 -- scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py tests/aistock_validation/modules/validation_center.md
```

## L3 Controlled Runner Contract

The controlled runner is the first execution-capable Validation Center loop. It may start only allowlisted nox sessions and must not become an arbitrary shell.

- Only plans in `tests/aistock_validation/catalog/test_plans.yaml` with `runner_enabled=true` can be started.
- The backend command shape is fixed to `[sys.executable, "-m", "nox", "-s", plan.nox_session]` with `shell=False`.
- The runner rejects unknown plans, disabled plans, non-runner plans, plans that write business state, unexpected ports, and backend port `8001`.
- Plans that require a backend or frontend can use only catalog allowlist ports: backend `8011/8012`, frontend `3011/3012`.
- Job records use schema `aistock_validation_execution_job_v1` and are stored under `tmp/validation/runner/jobs`.
- Runner evidence uses schema `aistock_validation_runner_evidence_v1` and links the job record plus execution log.
- `GET /api/v1/validation/executions` supports pagination; `POST /executions` starts a job; `GET /executions/{job_id}` returns one job or 404.
- `/health` and `/summary` expose runner mode, execution root, job counts, no arbitrary shell, and `production_8001_touched=false`.
- The UI shows runner-ready plans, a guarded execute button, submitted job status, logs/evidence paths, and a refreshable execution queue.
- Tests must cover allowlisted success, rejected unsafe plans, production-port refusal, API list/detail/start, UI POST start, TypeScript, Playwright, coverage, and L0 guardrails.

## Evidence

Every implementation run should create a record under `tests/aistock_validation/history/validation_center/` with:

- Exact commands.
- Coverage snapshot sample path.
- Pytest and nox results.
- Guardrail result.
- Production impact statement.
- Bugs found, fixes, reruns, and residual risks.


## Future Gap Requirements From Paper v2 Incidents

`docs/architecture/aistock_automation_test_coverage_gap_requirements_20260504.md` is a future-stage requirement input, not extra implementation scope for the current Validation Center infrastructure step. Validation Center contracts must still reserve these semantics now:

- `pass_scope` distinguishes L0/L1/L2/mock/fail-fast/current-commit/real-business proof.
- `business_assertion` records whether a user can complete a named operation and which UI/API/DB/log evidence proves it.
- Mock UI evidence cannot be displayed as real business success.
- Negative fail-fast evidence cannot replace a positive StrategyPackage/Selection/Paper v2 success path.
- Historical L3 evidence is reference only; high-risk modules must rerun relevant paths on the current commit.
- Future sample registry must include complete minute QE, historical QE with missing StaticDataLoader parquet, missing model params/factor source, large Paper v2 portfolio list, and HMM coefficient complete/missing samples.

## L2 Read-only API Contract

The first read-only API loop must expose validation history without executing commands, writing DB rows, or starting services:

- `GET /api/v1/validation/health` returns read-only storage status.
- `GET /api/v1/validation/plans` and `/plans/{plan_key}` read the allowlist catalog and reject unsafe command keys or production backend ports.
- `GET /api/v1/validation/runs` supports pagination plus module/level/status/search filters.
- `GET /api/v1/validation/runs/{run_id}` returns Markdown path/text, metadata, coverage/evidence links, and optional `pass_scope` / `business_assertion` if present.
- `GET /api/v1/validation/coverage` and `/coverage/{snapshot_id}` expose coverage snapshots with explicit missing/parse-error states.
- `GET /api/v1/validation/evidence` and `/evidence/{manifest_id}` expose evidence manifests with `missing_count`.
- `GET /api/v1/validation/summary` provides a lightweight module/status/coverage summary.
- Missing metadata, missing coverage, missing evidence, and malformed JSON must be explicit fields; the API must not fake success.

## L3 Validation Center UI Contract

The Validation Center UI displays validation history and can submit controlled runner jobs without writing business state:

- `/validation-center` loads health, summary, plan catalog, run list, coverage list, and evidence list from `/api/v1/validation/*`.
- The UI may send only `POST /api/v1/validation/executions` as a controlled-runner start request; no PUT/PATCH/DELETE request is allowed.
- The UI must show that execution is `allowlist only`, not arbitrary shell.
- Run history must support module, level, status, search, include-markdown-only, page, and page-size controls.
- Run detail must display metadata path, coverage/evidence links, quality gates, `pass_scope`, and `business_assertion`.
- Missing `metadata`, parse errors, missing coverage, missing evidence, and absent success-scope records must be visible warnings.
- Mock UI evidence must not be presented as real business success; absent `pass_scope` must read as `未记录/未证明`.
- Coverage and evidence detail panes must be readable business tables, not raw JSON as the primary operator view.
- UI validation uses Playwright mocked APIs on frontend dev port `3011`/`3012`; it must not restart production backend `8001`.
- Runner UI validation must prove the execute button calls the POST endpoint, displays submitted job status, and keeps production `8001` marked untouched.

## L2 Quality Finding / Bug Registry Contract

The first quality-registry loop exposes guardrail findings, legacy inventory findings, and bug repair context without executing commands or writing business state:

- `GET /api/v1/validation/findings` supports pagination plus source type, module, severity, status, and search filters.
- `GET /api/v1/validation/findings/{finding_id}` returns a readable finding detail plus machine-readable `agent_context`.
- `GET /api/v1/validation/findings/summary` returns counts by source type, severity, status, and module.
- `GET /api/v1/validation/bugs` supports pagination plus module, severity, status, agent, and search filters.
- `GET /api/v1/validation/bugs/{bug_id}` returns trigger condition, reproduce command, failing run, evidence, fix fields, verification fields, and closure requirements.
- `GET /api/v1/validation/bugs/{bug_id}/agent-context` returns the repair input for Codex/Claude: problem statement, reproduce command, evidence, allowed write scope, suspected modules, required verification, and closure requirements.
- `/health` and `/summary` include quality counts and explicit parse errors.
- The first-stage store is read-only local JSON evidence/index storage; it does not create DB schema and must not touch production port `8001`.
- Malformed JSON must be surfaced as parse errors; missing quality roots must be explicit and must not be treated as success evidence.

## L3 Quality Registry UI Contract

The first quality-registry UI loop displays quality findings and bugs as operator-readable tables and agent-context panels:

- `/validation-center` loads finding summaries, bug summaries, paginated findings, and paginated bugs from `/api/v1/validation/*`.
- The UI shows quality finding count and Bug count next to run/coverage metrics.
- Findings table shows source type, severity, status, module, title, file/evidence path, allowed write scope, and required verification count.
- Bug table shows title, module, severity, status, reproduce command, evidence, GitHub issue link if available, fix commit, and verification run.
- Detail panels show finding/Bug fields and `agent_context` in labeled rows, not as raw JSON primary output.
- The UI must not send PUT/PATCH/DELETE requests in this phase; the only allowed write method is controlled-runner `POST /api/v1/validation/executions`.
- Playwright validation must mock the validation endpoints plus the controlled-runner POST, fail on console/page/request/API errors, and verify that agent-context content is visible.

## L3 Live Read-only API Smoke Contract

The live read-only smoke validates that the UI/API contracts work against a running dev backend, without starting services or touching production:

- `nox -s validation_center_live_readonly` probes only a running dev backend on `BACKEND_PORT` (default `8011`) and fails fast if the backend is unavailable.
- The smoke refuses to probe port `8001` unless explicitly overridden by `--allow-production-8001`, which must not be used for normal development validation.
- The smoke refuses non-localhost API bases unless explicitly overridden by `--allow-non-localhost`, which must not be used for normal development validation.
- `scripts/validation_center_readonly_smoke.py` sends only `GET` requests and records `write_methods_sent=[]`.
- Required endpoints: health, summary, plans, runs, run detail when present, coverage list/detail when present, evidence list/detail when present, executions list/detail when present, findings summary/list/detail when present, bugs summary/list/detail/agent-context when present.
- Health and summary must include runner status objects even though this smoke sends no POST requests.
- The JSON smoke output uses schema `aistock_validation_center_readonly_smoke_v1`, records endpoint status, counts, failures, read-only state, and `production_8001_touched=false` for normal dev runs.
- Empty run/coverage/evidence/finding/Bug lists are allowed, but endpoint shape and summary counts must remain explicit.
- This live smoke is an additional L3 proof; mocked UI E2E remains required for deterministic UI regression.
