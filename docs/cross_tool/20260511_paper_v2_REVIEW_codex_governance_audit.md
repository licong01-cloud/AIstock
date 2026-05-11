# [REVIEW] Claude Code audit of Codex governance branch

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 protocol §4)
> **date**: 2026-05-11
> **branch reviewed**: `codex/qe-governance-integration-20260509@d1ca0ba` (Codex HEAD as of audit)
> **reviewer branch**: `claude/paper-v2-vnpy-mvp-20260508`
> **protocol**: `docs/process/cross_tool_review_protocol_20260510.md`
> **prior P0 reviews**: `d1ca0ba` + `5bce68c` already covered by strategy session pre-audit — still valid baseline (see §7)

## §0 Summary

Total bugs found: **3**
- BLOCKING: 0
- HIGH: 0
- MED: 1
- LOW: 2

Layers contributed:
- **Layer 1 (static manual read)**: 4 source files inspected in depth (`service.py`, `repository.py`, `routers/strategy_packages.py`, `model_registry/registry.py`); 6 migration SQL files spot-checked.
- **Layer 2 (test quality read)**: 5 test files reviewed in full (`test_enable_paper_router_409.py`, `test_governance_eligibility.py`, `test_repository_service.py`, `test_governance_migration_smoke.py`, `test_model_registry_phase5.py`).
- **Layer 3 (dev DB SELECT)**: 4 read-only queries against `127.0.0.1:5433/aistock_dev` via `docker exec aistock-pg-dev psql` — schema parity for `strategy_pkg.package`, table inventory for the 3 schemas, outbox_event compat for T13, constraint inventory for `package_asset`.
- **Layer 4 (manual critical-path review)**: 3 paths — transition_status atomicity, enable_paper paper_ready check, 409 mapping.

Prior P0 verdict (`d1ca0ba` + `5bce68c`): **still valid**. Nothing in this expanded audit invalidates the prior P0 review. The Q1 strict gate (`d1ca0ba`) and the BUG-023 atomicity hardening (`5bce68c`) both check out cleanly under their respective regression tests and manual reads.

## §1 Audit scope + commit anchors

Codex HEAD audited: `codex/qe-governance-integration-20260509@d1ca0ba`. Branch log (last 20) includes:

- `d1ca0ba` feat(qe): enforce paper governance gate
- `5bce68c` fix(qe): harden strategy package transitions (BUG-023)
- `009e1c1` fix(qe): reset migration apply transaction state
- `e52fd00` fix(qe): harden governance dev migration apply
- `97ec0e9` test(qe): cover governance eligibility blockers
- `52bd086` feat(qe): add governance eligibility summary
- `83a569f` test(qe): add production readonly governance preflight
- `069ae8b` test(qe): harden governance migration DB smoke errors
- `de59847` test(qe): add strategy package governance readonly smoke

### Source files inspected (Codex worktree)
- `backend/services/strategy_package/service.py` — read in depth around `transition_status`, `enable_paper`, `governance_eligibility`, `_require_governance_paper_ready`, `_parse_jsonish`.
- `backend/services/strategy_package/repository.py` — read in depth around `transition_status` (lines 205-281) for atomicity / compare-and-set.
- `backend/services/strategy_package/` — directory grep for `except:`/`except Exception:` patterns (excl. `live_inference.py` per Claude D1 contract).
- `backend/services/model_registry/registry.py` — class layout read; suspect pattern grep.
- `backend/routers/strategy_packages.py` — `_raise_http` (line 140) for error-code mapping.
- `backend/migrations/strategy_pkg_package_asset_20260509.sql` — full read.
- `backend/migrations/model_registry_phase5_20260509.sql` — DDL idempotency grep.
- `backend/migrations/qe_phase4_master_seed_contract_20260509.sql` — ALTER guards verified.
- Additional `backend/migrations/strategy_pkg_*_20260509.sql` files referenced.

### Test files inspected
- `backend/tests/strategy_package/test_enable_paper_router_409.py` (2 tests, full read)
- `backend/tests/strategy_package/test_governance_eligibility.py` (6 tests, full read)
- `backend/tests/strategy_package/test_repository_service.py` (20 tests, BUG-023 region read in detail)
- `backend/tests/model_registry/test_governance_migration_smoke.py` (19 tests, structural skim + idempotency grep)
- `backend/tests/model_registry/test_model_registry_phase5.py` (17 tests, full test-name read)

### Read-only smoke scripts (existence verified only)
- `scripts/governance_migration_smoke.py`
- `scripts/strategy_package_governance_readonly_smoke.py`

## §2 Layer 1 findings (static read)

| File | Module purpose | Suspect patterns | Verdict |
|---|---|---|---|
| `service.py` (StrategyPackageService) | Orchestration layer: manifest creation, status transitions, governance eligibility, paper enable gate, runtime variants, validation runs | `_parse_jsonish` (line 264-275) silently swallows `json.JSONDecodeError` → returns None | BUG-AUDIT-003 (LOW) |
| `repository.py` (StrategyPackageRepository.transition_status) | Atomic DB-level status transition with compare-and-set + audit event | `except Exception:` (line 274) re-raises after rollback — legitimate pattern, NOT silent | Clean |
| `routers/strategy_packages.py` (`_raise_http`) | Maps `TradingCoreError` → HTTPException status codes | `InvalidStateTransitionError → 409`, `DataUnavailableError → 404`, `UnsupportedFeatureError → 422`, else 400 | Clean |
| `model_registry/registry.py` | Phase 5 4-layer architecture: enums → records → Protocol → InMemory + Postgres impl → Service | No `except: pass`; no f-string SQL; layers cleanly separated | Clean |

### Migration SQL idempotency
- `strategy_pkg_package_asset_20260509.sql` — uses `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, plus `DO $$ ... IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname=...) ... $$` for the named CHECK constraint. Fully idempotent.
- `model_registry_phase5_20260509.sql` — 5 `CREATE TABLE IF NOT EXISTS` + 8 `CREATE INDEX IF NOT EXISTS`. Idempotent.
- `qe_phase4_master_seed_contract_20260509.sql` — 7 `ADD COLUMN IF NOT EXISTS` on `strategy_pkg.package`. Idempotent.
- No NOT NULL columns without DEFAULT introduced on pre-existing tables. Apply-on-populated-data safe.

### Suspect static-read items
1. **`_parse_jsonish` silent fallback** — see BUG-AUDIT-003. The function is used only in `_qe_source_payload` to build display strings for the QE-packaging-sources picker (read-only UI helper), so impact is contained. Still flagged for the `feedback_no_silent_errors` user rule.

No SQL-injection vectors found. No hardcoded credentials. No `subprocess.shell=True`. No TOCTOU windows.

## §3 Layer 2 findings (test quality)

| File | # tests | Coverage assessment | Gaps |
|---|---|---|---|
| `test_enable_paper_router_409.py` | 2 | Covers both 409 (already PAPER_ENABLED) and 400 (validation error: paper_ready=false). Asserts `error_code`, `from_status`/`to_status` context, and blocker name. | None significant |
| `test_governance_eligibility.py` | 6 | Covers (a) all gates pass, (b) protected_asset missing, (c) runtime variant missing, (d) fragile seed stability, (e) disallowed package_status, (f) router-layer read-only contract | No explicit test for `governance_eligibility(limit=0)` ValueError, but `service.py:723` raises it; would be caught by repo router validation if exposed |
| `test_repository_service.py` (BUG-023 regression) | 2 dedicated transition tests | `test_postgres_repository_wraps_status_event_sequence_collision` — verifies UniqueViolation → InvalidStateTransitionError + rollback; `test_postgres_repository_commits_status_transition_atomically` — verifies autocommit toggling + commit() + restore. Both target the exact path hardened in `5bce68c`. | None |
| `test_governance_migration_smoke.py` | 19 | Read-only preflight wiring: ReadonlyPreflightCursor mocks pg_class/columns/indexes/constraints inspection, validates SQL queries are SELECT-only, asserts STACK_SPECS contains expected relations/columns/indexes/constraints. Includes production preflight guards. | **GAP**: No test that explicitly applies the migration twice to verify idempotency. Migration files use `IF NOT EXISTS` by construction (manually verified §2), so the gap is low-severity. → BUG-AUDIT-002 (LOW) |
| `test_model_registry_phase5.py` | 17 | 4-layer Phase 5 architecture covered: catalog (model_catalog_compat view), variants (model_spec + qe_selectable filter), registry (template+spec+trial+artifact lifecycle), bridge (legacy_aistock_model_catalog_bridge). Append-only audit (lifecycle_event). Write-API guarded by env flag. 403 before DB. Read routes don't require guard. Retired = terminal. | None significant |

### Test quality conclusion
The Codex test suite is dense and exercises the right error paths. Mocked-but-not-asserted paths are not in evidence. Conditional skips are not in evidence (no `pytest.skip` discovered in the audited files). Fixtures use `InMemoryStrategyPackageRepository` so no DB cleanup concerns.

## §4 Layer 3 findings (dev DB SELECT)

Connection: `docker exec aistock-pg-dev psql -U postgres -d aistock_dev` — verified `current_database()=aistock_dev`. Container `aistock-pg-dev` (port 5433 host). **Zero writes performed.**

### Q1 — `strategy_pkg.package` columns (20 columns)
```
package_id, package_name, package_version, source_type, source_id,
loop_id, run_id, package_status, manifest_json, manifest_sha256,
paper_portfolio_count, created_at, updated_at,
seed_policy, master_seed, seed_sequence, seed_contract,
seed_contract_sha256, reproducibility_level, nondeterministic_flags
```
All 7 columns added by `qe_phase4_master_seed_contract_20260509.sql` are present and nullable as declared. **No `paper_candidate` / `frozen_manifest_*` / `governance_eligibility` columns** exist — confirmed by code review that these are *computed in-memory* by `service.governance_eligibility(...)`, NOT persisted. This matches `service.py:716-764`. Parity OK.

### Q2 — Codex tables in dev DB (3 schemas)
- `strategy_pkg`: 10 tables (`package`, `package_asset`, `package_runtime_variant`, `package_status_event`, `package_validation_run`, `promotion_review`, `seed_fragility_score`, `selection_score_artifact`, `validated_execution_policy`, `model_retrain_job`, `model_state`).
- `model_registry`: 5 tables (`model_template`, `model_spec`, `model_trial`, `model_artifact`, `model_lifecycle_event`) + 3 views (`v_legacy_aistock_model_catalog_bridge`, `v_model_catalog_compat`, `v_qe_selectable_model_spec`). Matches phase5 migration declaration.
- `qe_archive`: 50+ tables (out-of-scope for this audit; not touched by Codex governance changes).

### Q3 — `model_registry.model_spec` Phase 5 columns (26 columns)
All Phase 5 contract fields present: `qe_selectable BOOL NOT NULL`, `qe_selectability_reason`, `lifecycle_status NOT NULL`, `source_type`, plus the 4 JSONB contracts (`hyperparam_schema`, `default_hyperparams`, `search_space_json`, `input_contract_json`, `output_contract_json`, `feature_schema_requirements`, `label_requirements`, `dependency_versions`, `architecture_config`). Migration applied cleanly.

### Q4 — `qe_archive.outbox_event` columns (14 columns)
```
event_id, event_type, source_system, source_id, source_sub_id,
payload, status, retry_count, next_retry_at,
locked_by, locked_at, error_message, created_at, updated_at
```
**Critical for T13 compat**: NO `routing_class` column was added by Codex governance branch. T13 (paper-v2 `91643f7`) stores `routing_class` inside `payload` JSONB — no schema collision. Compat preserved.

### Q5 — `strategy_pkg.package_asset` constraints
```
package_asset_pkey                    PRIMARY KEY (asset_id)
package_asset_package_id_fkey         FOREIGN KEY (package_id) → strategy_pkg.package(package_id)
package_asset_size_non_negative_check CHECK (asset_size_bytes IS NULL OR asset_size_bytes >= 0)
```
Plus indexes per migration. All declared constraints + the named CHECK from the `DO $$` block enforced.

### Layer 3 verdict
Schema parity between migrations-as-declared and dev DB state is clean. No missing-column / drifted-type issues. No outbox_event collision with T13.

## §5 Layer 4 findings (manual critical-path review)

### §5.1 `transition_status` atomicity (BUG-023 hardening, `5bce68c`)

**File**: `backend/services/strategy_package/repository.py:205-281`

The function:
1. Reads current status via `self.get(package_id)`.
2. Validates `record.package_status in allowed_from` → raises `InvalidStateTransitionError` if not.
3. Opens connection, **explicitly toggles `autocommit=False`** to make UPDATE + INSERT atomic.
4. Issues `UPDATE strategy_pkg.package SET package_status=%s, updated_at=NOW() WHERE package_id=%s AND package_status=%s` — **compare-and-set** on the old status. If `rowcount != 1`, raises `InvalidStateTransitionError("lost compare-and-set race")`.
5. Issues `INSERT INTO strategy_pkg.package_status_event(...)`.
6. `conn.commit()`. On `UniqueViolation` (sequence collision) → `rollback()` + `InvalidStateTransitionError("status event sequence is behind")`. On any other Exception → `rollback()` + re-raise (no silent swallow).
7. `finally` restores original `autocommit`.

**Verdict**: Clean. Race conditions are addressed via compare-and-set semantics; the multi-statement transaction is correctly wrapped; rollback is correct on all failure paths; original autocommit is restored. Tests in `test_repository_service.py:292-...` exercise both the UniqueViolation rollback and the normal-commit happy path. No issue found.

### §5.2 `enable_paper` paper_ready check (Q1 hard gate, `d1ca0ba`)

**File**: `backend/services/strategy_package/service.py:322-363` + `:775-782`

Flow:
1. `enable_paper(package_id)` → `transition_status(package_id, to_status=PAPER_ENABLED, reason="enable_paper")`.
2. `transition_status` first checks `STATUS_TRANSITIONS.get(to_status)` (fail-fast for unknown target).
3. If `to_status == PAPER_ENABLED`, fetches the record and calls `_require_governance_paper_ready(record)` **BEFORE** delegating to `repository.transition_status`. This is the strict gate per `d1ca0ba`.
4. `_require_governance_paper_ready` calls `self.governance_eligibility(package_id)` (default `limit=500`), reads `eligibility["paper_ready"]` (always a boolean — `not blockers` per line 756), raises `StrategyPackageValidationError(...)` if not ready.

**Failure mode analysis**:
- `governance_eligibility` is NEVER None and NEVER malformed — the function always returns a dict with `paper_ready: bool` (`not blockers`). KeyError impossible.
- Ordering: gate runs BEFORE state-machine. So `paper_ready=false` on a `BACKTEST_APPROVED` package → 400 (validation), not 409 (state). This matches `test_enable_paper_endpoint_keeps_validation_errors_at_400`.
- Already-`PAPER_ENABLED` package with gates still ready → gate passes (manifest_identity allows PAPER_ENABLED at line 786-790), then state-machine check fails → 409. Matches `test_enable_paper_endpoint_returns_409_on_invalid_transition`.
- Edge: a package that BECAME paper_ready=false after Paper enable (e.g. a regression hidden under new validation runs) would still 409 on re-enable since `manifest_identity` gate accepts PAPER_ENABLED. Benign — can't enable what's already enabled.

**Verdict**: Clean. Fail-fast on missing/disallowed. No silent error. Order of checks delivers correct HTTP status code per case.

**Note (MED severity)**: `_require_governance_paper_ready` calls `governance_eligibility(package_id)` without forwarding `limit` — uses the default `limit=500`. If a package accumulates >500 validation runs, the stability gate sees only the most recent 500, which could in theory let a `seed_stability=FRAGILE` evaluated over the full history pass the gate computed over the truncated window. Low real-world likelihood but worth filing. → BUG-AUDIT-001 (MED).

### §5.3 409 mapping (`_raise_http` integrity)

**File**: `backend/routers/strategy_packages.py:140-148`

```python
def _raise_http(exc: TradingCoreError) -> None:
    status_code = 400
    if isinstance(exc, DataUnavailableError):
        status_code = 404
    elif isinstance(exc, UnsupportedFeatureError):
        status_code = 422
    elif isinstance(exc, InvalidStateTransitionError):
        status_code = 409
    raise HTTPException(status_code=status_code, detail=exc.to_dict()) from exc
```

Mapping is:
- `InvalidStateTransitionError → 409` ✓
- `DataUnavailableError → 404` ✓
- `UnsupportedFeatureError → 422` ✓
- `StrategyPackageValidationError → 400` (default branch) ✓
- Detail shape: `exc.to_dict()` — preserved across all branches.

**Cross-check**: Compared to paper-v2 `4528a32` (T8-C) which established the 409 contract — no regression. The detail dict contains `error_code` + `context` per `test_enable_paper_router_409.py:113-115`.

**Verdict**: Clean. 409 mapping intact.

## §6 BUG entries

### BUG-AUDIT-001 [MED] — `_require_governance_paper_ready` uses default limit=500, possibly truncating validation history for stability gate

- **File:line**: `backend/services/strategy_package/service.py:775-782` (caller) and `:716-722` (callee signature)
- **Branch@SHA**: `codex/qe-governance-integration-20260509@d1ca0ba`
- **Layer**: Layer 4 (manual code review)
- **Reproduction**: A `StrategyPackage` with >500 historical validation runs reaches `enable_paper`. `_require_governance_paper_ready` calls `self.governance_eligibility(record.package_id)` (no `limit` kwarg). Inside `governance_eligibility`, `self.summarize_validation_stability(package_id, metric_key=..., limit=500)` runs on only the most recent 500 runs ordered by `created_at DESC` (per `repository.list_validation_runs`). The seed-stability gate is then computed over a truncated window.
- **Expected**: Either (a) the gate uses ALL `ORIGINAL_RETRAIN` runs for the current `manifest_sha256`, or (b) the truncation policy is explicitly documented and the gate emits a metadata field like `evaluated_run_count` / `truncated=True` so callers can detect it.
- **Actual**: Silent truncation at 500. A subset where the most recent 500 are stable but the full history shows fragility would incorrectly pass the gate. (Reverse case: most recent 500 are fragile but full history is stable would incorrectly block — symmetric concern.)
- **Recommended fix direction (text only, no code)**: In `_require_governance_paper_ready`, either bump `limit` to a very large value (e.g. 10_000) sufficient to cover realistic package histories, OR have `summarize_validation_stability` filter by `manifest_sha256` at the SQL layer instead of paginating by recency. Document the chosen policy in the function docstring + the `governance_eligibility` return dict.
- **Suggested owner**: Codex (original author of governance gate `d1ca0ba`).
- **Cross-references**: `test_governance_eligibility.py` tests use 2-3 validation runs — far below the 500 ceiling, so this issue is not detected by current tests.

### BUG-AUDIT-002 [LOW] — `test_governance_migration_smoke.py` lacks explicit "apply twice" idempotency assertion

- **File:line**: `backend/tests/model_registry/test_governance_migration_smoke.py` (19 tests, no idempotency-via-double-apply test)
- **Branch@SHA**: `codex/qe-governance-integration-20260509@d1ca0ba`
- **Layer**: Layer 2 (test quality)
- **Reproduction**: Read the 19 test names in the file — they exercise STACK_SPECS contents, SELECT-only enforcement, preflight cursor inspection, production guards. None calls `apply_migration_file()` (or equivalent) twice against the same DB to assert no error on second apply.
- **Expected**: At least one test that applies the full Codex migration set against a fresh schema, then applies the same set again and asserts no exceptions, no duplicate-key errors, and no constraint-creation conflicts.
- **Actual**: Idempotency is guaranteed only by manual inspection of `IF NOT EXISTS` guards in the SQL files (which I confirmed in §2). A test asserting it would catch future regressions where a developer adds a non-guarded DDL statement.
- **Recommended fix direction**: Add a test that uses the same `ReadonlyPreflightCursor` (or a more permissive Mock) to invoke `governance_migration_smoke.apply()` twice and assert idempotent behavior. Alternatively, an integration-test variant gated by a dev-DB env flag would be even stronger.
- **Suggested owner**: Codex (governance migration smoke author).
- **Cross-references**: §2 idempotency findings (migrations are correctly guarded by construction); BUG-AUDIT-003 (similar test-quality category).

### BUG-AUDIT-003 [LOW] — `_parse_jsonish` silently swallows `JSONDecodeError` and returns None

- **File:line**: `backend/services/strategy_package/service.py:264-275`
- **Branch@SHA**: `codex/qe-governance-integration-20260509@d1ca0ba`
- **Layer**: Layer 1 (static read)
- **Reproduction**: Pass a malformed JSON string to `StrategyPackageService._parse_jsonish('{"bad": ')` → returns `None` without logging, raising, or recording the parse failure. The user-facing `feedback_no_silent_errors` rule (per global memory) treats this as an anti-pattern.
- **Expected**: Either (a) propagate the JSONDecodeError (fail-fast), or (b) log a WARN with the package_id + raw value snippet before returning None so operators can detect data corruption in `qe_experiments.result_metrics`.
- **Actual**: Silent `return None`. The caller (`_qe_source_payload`) treats the result as an empty metrics dict — so a corrupted experiment would silently display as zero metrics in the QE-packaging-sources picker. Operator never sees the error.
- **Recommended fix direction**: Add a logger.warning with `experiment_id` (or row identifier) and the JSON parse error message before returning None. Or, since the only call site is read-only display, accept the current behavior but add a docstring noting the silent fallback.
- **Suggested owner**: Codex.
- **Cross-references**: User global memory rule `feedback_no_silent_errors`.

## §7 Verification of prior P0 review

The prior P0 review (strategy session, pre-audit) covered `d1ca0ba` (Q1 strict gate + Q2 409 mapping) and `5bce68c` (BUG-023 transition atomicity).

This audit's independent reads of:
- `d1ca0ba` service.py changes (governance_eligibility / _require_governance_paper_ready / transition_status PAPER_ENABLED guard) — see §5.2.
- `d1ca0ba` 409 mapping — see §5.3.
- `5bce68c` repository.py atomicity hardening — see §5.1.

…**did not invalidate** any portion of the prior P0 verdict. The expanded scope (model_registry Phase 5 + migration smoke tests + governance eligibility view + Q1 governance gate) adds 3 net-new findings (1 MED, 2 LOW) but none touches the `d1ca0ba` / `5bce68c` happy-path or hardening logic.

**Conclusion**: Prior P0 review **still valid**. The 3 new BUG entries (§6) are additive — they pertain to (1) limit-window completeness in the gate's data source, (2) test-coverage gap on idempotency, (3) a silent-fallback in an adjacent read-only helper.

## §8 Recommended next steps

Per protocol §5 (Bug-only mode, default):
1. Bug entries §6 are routed to **Codex** as original author of the governance branch.
2. paper-v2 team (this reviewer) takes **no fixes** to Codex code. The audit doc is the only deliverable.
3. Per protocol §6 (T0+2d): Codex fixes own-code bugs (BUG-AUDIT-001 / -002 / -003); reviewer re-verifies on the next cross-tool review pass.
4. Drawer `cross-tool/codex-claude-coord` notification posted (see Step 8 in the agent runbook).

Pipeline-foundation Stage 5 may ingest the §6 bug entries into their bugs tracker downstream.

## §9 Boundary confirmations

- `codex_code_modified=false` — zero edits applied to `backend/services/strategy_package/`, `backend/services/model_registry/`, `backend/migrations/*_20260509.sql`, `scripts/governance_*_smoke.py`, `backend/tests/strategy_package/`, `backend/tests/model_registry/` in the Codex worktree. All reads went through Read/Grep tools.
- `dev_db_writes=false` — all 5 `docker exec aistock-pg-dev psql` calls used `SELECT` against `information_schema` / `pg_catalog` only. Zero INSERT / UPDATE / DELETE / CREATE / ALTER / DROP. Connection guard: `127.0.0.1:5433/aistock_dev` (container `aistock-pg-dev`).
- `prod_db_touched=false` — never connected to `timescaledb` (prod `127.0.0.1:5432/aistock`).
- `prod_8001_touched=false` — no service started; no `curl http://localhost:8001/...` issued.
- Audit doc `docs/cross_tool/20260511_paper_v2_REVIEW_codex_governance_audit.md` in paper-v2 worktree is the **only** file written by this audit.
- `frontend/tsconfig.tsbuildinfo` NOT staged.
