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
python -m nox -s validation_center_real_port_ui
python -m nox -s validation_center_live_readonly
python -m nox -s validation_center_runner_smoke
python -m nox -s qe_data_contract_backend
python -m nox -s l0 -- scripts/aistock_validate.py backend/tests/test_aistock_validate_metadata.py backend/tests/test_aistock_validate_coverage.py noxfile.py tests/aistock_validation/modules/validation_center.md frontend/tests/validation-center/validation-center-real-port.spec.ts
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

## L3 Runner Auto Archive And Detail Contract

Every completed controlled-runner job must create a standard Validation History record or an explicit tmp-only archive, so runner evidence does not remain only in the live job queue.

- A job starts with `archive.status=pending` when archive is enabled; after terminal completion it must become `archive.status=archived` or explicit `archive.status=failed`.
- Archive output for task worktrees lives under that worktree's `tests/aistock_validation/history/<module>/` and includes Markdown run record, run metadata JSON, standard evidence manifest JSON, runner job JSON, runner log TXT, and runner evidence JSON.
- The canonical root `main` checkout is a sync/runtime target, not the default validation workspace. MCP/agent-triggered validation must pass a task worktree `workspace_path`; root `main` runs require the explicit tmp-only confirmation string and must not write in-repo Validation History artifacts.
- Curated evidence that should become part of project history must be copied or generated intentionally in the relevant task branch; automatic runner output must not leave the canonical root dirty.
- Copied Markdown artifacts must not pollute Validation History run lists; guardrail Markdown artifacts are archived as TXT for new runs, and legacy `*-guardrail-md.md` / `*-l0-guardrail.md` evidence files are ignored by run-history discovery.
- Run metadata must use schema `aistock_validation_run_v1`; standard evidence must use schema `aistock_validation_evidence_manifest_v1`; runner evidence remains `aistock_validation_runner_evidence_v1`.
- The archived run metadata must include runner job id, plan key, nox session, archive paths, quality gates, `pass_scope`, and `business_assertion`.
- Artifact discovery may copy known coverage, smoke, guardrail, and L0 artifacts from `tmp/validation/*` into the history folder; invalid coverage snapshots must not be treated as valid coverage.
- Archive failure must not hide the runner result: the job remains terminal with `archive.status=failed` and an error field, and runner evidence is still written.
- Executor exceptions must never leave jobs stuck in `running`; they must be converted to explicit failed jobs with log text, runner evidence, and archive metadata.
- `GET /api/v1/validation/executions` supports `status`, `plan_key`, `module`, `page`, and `page_size` filters.
- `GET /api/v1/validation/executions/{job_id}/log` returns only the local runner log or its archived copy, supports bounded `tail_lines`, and must reject invalid path-like job ids.
- `GET /api/v1/validation/executions/{job_id}/evidence` returns runner evidence plus the standard archived evidence manifest when present; if transient local runner evidence has been removed, the endpoint must fall back to the archived runner evidence path.
- The UI runner queue must show archive status/path, support filters and pagination, and expose a detail panel with log tail and evidence summary.
- `nox -s validation_center_runner_smoke -- 8012` is the live positive smoke for this contract: it may POST only to a running localhost dev backend, must refuse production port `8001`, starts the safe `guardrail_changed_files` allowlisted plan, verifies archive status, reads the archived run by `run_id`, and writes `tmp/validation/validation_center/runner_smoke.json`.
- The live read-only smoke should be run again after at least one runner job exists so executions detail, log, and evidence endpoints are covered with GET-only proof.

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

## L3 Real-port UI Smoke Contract

The real-port UI smoke validates that the operator-facing Validation Center page can load Git workspace status, recent commit activity, and module quality priority data from a running dev backend and a dev frontend:

- `nox -s validation_center_real_port_ui` probes only dev backend/frontend ports. Defaults are backend `8012` and frontend `3012`; `8001` and `3000` are refused.
- The session requires a running FastAPI dev backend and never starts, restarts, or stops production `8001`.
- If the frontend port is free, Playwright starts a temporary Next.js dev server with `NEXT_PUBLIC_API_BASE` pointing to the dev backend. If the frontend port is already occupied, the session uses that existing dev frontend.
- `frontend/tests/validation-center/validation-center-real-port.spec.ts` sends no Validation API writes. Any non-GET Validation API request is recorded in `write_methods_sent` and fails the smoke.
- Required UI assertions include the page title, Git workspace panel, module quality priority panel, needs-validation metric, recent commit panel, and file-ownership aggregation copy.
- Required backend responses include `GET /api/v1/validation/git/commit-activity` and `GET /api/v1/validation/modules/quality-summary` with HTTP `200`.
- The smoke fails on page errors, console errors, request failures, Validation API 4xx/5xx responses, missing UI assertions, or unexpected write methods.
- The JSON evidence uses schema `aistock_validation_center_real_port_ui_smoke_v1` and is written to `tmp/validation/validation_center/ui_real_port_smoke.json`.
- The nox session also writes a standard evidence manifest to `tmp/validation/validation_center/ui_real_port_smoke_evidence.json`.

## L2/L3 UI Target Catalog Contract

The route-level UI target catalog is the first durable coverage map between the official AIstock navigation surface, module ownership, and validation plans.

- `tests/aistock_validation/catalog/ui_targets.yaml` uses schema `aistock_validation_ui_targets_v1`.
- Every official `NAV_GROUPS` route must have exactly one `ui_targets.yaml` target, and every target href must exist in `NAV_GROUPS`.
- Each target must declare `route_id`, `href`, `label`, `nav_group`, `primary_module`, `impact_modules`, `risk_level`, `required_test_plans`, `recommended_test_plans`, `business_operations`, and `coverage_status`.
- `primary_module` and every `impact_modules` entry must exist in `module_registry.yaml`; test plan keys must exist in `test_plans.yaml`.
- Invalid YAML, duplicate `route_id`, duplicate `href`, missing business operations, unknown modules, unknown plans, or invalid risk/coverage status must fail fast.
- Coverage status values are `covered`, `partial`, `planned`, and `excluded`; excluded targets must include an explicit `exclusion_reason`.
- The backend exposes read-only endpoints: `GET /api/v1/validation/ui-targets`, `GET /api/v1/validation/ui-targets/summary`, and `GET /api/v1/validation/ui-targets/{route_id}`.
- API payloads enrich each route with module quality, latest validation run when available, warnings, and an explicit `proven_by_real_business_evidence` boolean; missing evidence must be warnings, not fake success.
- The Validation Center page displays the route catalog inside the page body, grouped by catalog/navigation metadata, without covering or replacing the global sidebar.
- Mock UI and real-port UI smokes must prove the page consumes `/ui-targets` and `/ui-targets/summary`, shows warnings/gaps, and supports selecting a route detail panel.

## Nightly Runner Preflight Contract

AIstock Nightly L3 + DR must fail fast with actionable evidence when the required self-hosted Windows runner is unavailable.

- The `runner-preflight` job runs on GitHub-hosted Ubuntu before any self-hosted job is queued.
- `scripts/aistock_runner_health.py doctor` checks repository Actions runners for online labels `self-hosted` and `windows`, writes `runner-health.json` and `runner-health.md`, and exits non-zero when no matching runner is available.
- The preflight is read-only: it queries GitHub Actions metadata only and must not touch production DB, production ports, or runtime services.
- `full-summary` must run on GitHub-hosted Ubuntu and include `runner-preflight` in the nightly summary so missing runner capacity can create an actionable issue instead of leaving the workflow queued for hours.
- If `runner-preflight` fails, downstream self-hosted DR/L3 jobs remain skipped, and the auto-filed issue should tell operators to restart or register the self-hosted Windows runner and inspect the runner-health artifact.
