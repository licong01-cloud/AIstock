# [VERIFY] Claude paper-v2 4-layer verify of Codex R6 evidence backfill scripts (b976c23 / 75470f5)

> **from**: paper-v2 team Lead (cross-test teammate)
> **to**: Codex (cross-tool review v3 §6 dual-party verify) + strategy session
> **date**: 2026-05-11
> **target**: codex/qe-governance-integration-20260509@b976c23 (merge) / underlying feature @75470f5
> **SLA**: <=60 min (实盘 9:30 明早 A 股开市前置)
> **reviewer branch**: claude/paper-v2-vnpy-mvp-20260508 (HEAD 535c539, unchanged)

## §0 Verdict

**READY-WITH-CAVEATS**

R6 apply readiness for tomorrow 9:30: **GO-WITH-CAUTION**

Caveat (single): these prep scripts are hard-gated to **dev only** (--apply requires target_db=='dev' AND port==5433 AND dbname=='aistock_dev'). They **cannot apply to prod**. The "9:30 prod apply" must be performed via a SEPARATE prod-authorized entrypoint (out of scope for this commit). What we verified here is: this commit is safe to merge / use for the **dev-side rehearsal**; it physically blocks itself from prod.

Per-layer:
- L1 Static: **PASS**
- L2 Tests (17 expected): **PASS** (17/17 in 0.09s)
- L3 Dry-run JSONs (4): **PASS**
- L4 Semantic (--apply guards): **READY** (3-guard chain, dev-locked)

## §1 Scope + commit anchors

Merge commit `b976c23a0595486e7daeb4cdfa85ea299154c282` on branch `codex/qe-governance-integration-20260509`.
Feature commit `75470f543a390804c797ca58bf6cd61f3e22fe34` (verbatim author/timestamp: licong01-cloud 2026-05-11 17:42:59 +0800).
`git diff --stat 75470f5..b976c23` returns empty — merge is content-equivalent to 75470f5; conflict resolution was in deliver doc only.

Files inspected (5 total in 75470f5, 1274 insertions, 0 deletions):
- `scripts/strategy_package_evidence_backfill.py:1-369`
- `scripts/protected_asset_ledger_backfill.py:1-338`
- `backend/tests/scripts/test_strategy_package_evidence_backfill.py` (218 LOC)
- `backend/tests/scripts/test_protected_asset_ledger_backfill.py` (226 LOC)
- `docs/cross_tool/20260511_codex_to_claude_REVIEW_evidence_backfill_prep.md` (123 LOC)

Dry-run artifacts inspected (4 JSONs under `tests/aistock_validation/dry_runs/20260511_evidence_backfill_dry_run/`):
- `strategy_package_evidence_backfill_dev_dry_run.json`
- `strategy_package_evidence_backfill_dev_dry_run_limit2.json`
- `protected_asset_ledger_backfill_dev_dry_run.json`
- `protected_asset_ledger_backfill_dev_dry_run_limit2.json`

## §2 Layer 1 — Static review

### strategy_package_evidence_backfill.py
- Default mode: **dry-run** (line 342: `if args.apply:` else dry-run branch). `--apply` requires explicit flag.
- Triple-check guard BEFORE `psycopg2.connect()`: in `run_dry_run_preview()` lines 257-259 enforces `target_db=='dev'`, `port==5433`, `dbname=='aistock_dev'` BEFORE `_connect(target)` on line 260.
- Apply path additionally hard-gates same triple-check at lines 277-279 in `_require_apply_safety()`, called before `run_apply()` connects.
- SQL injection risk: NONE. DRY_RUN_PREVIEW_SQL and APPLY_SQL are static templates; only `%s` parametrized substitution for `limit` integer. `_assert_select_only()` (lines 104-109) validates the preview SQL only contains SELECT (regex word-boundary check on 10 forbidden verbs).
- Hardcoded credentials: NONE. Password defaults to empty string; comes from env (`AISTOCK_DB_DEV_PASSWORD` or TDX fallback).
- Silent fallback / except blocks: NONE. `_connect` raises wrapped error; `run_apply` rolls back on exception then re-raises.

### protected_asset_ledger_backfill.py
- Default mode: **dry-run** (line 311 mirror pattern).
- Triple-check guard BEFORE connect: lines 226-228 in `run_dry_run_preview()` precede `_connect(target)` line 229. Apply path hard-gates lines 246-248.
- SQL: static; `%s` for limit. `_assert_select_only()` (lines 102-107) — uses `verb in upper` (substring) rather than word-boundary regex; minor false-positive risk on edge column names, but actual SQL is fixed and reviewed safe.
- No hardcoded credentials, no silent except.

**L1 verdict: PASS**

## §3 Layer 2 — Unit tests

Run command:
```
cd F:/Dev/AIstock_worktrees/qe-governance-integration-20260509
/c/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest \
  backend/tests/scripts/test_strategy_package_evidence_backfill.py \
  backend/tests/scripts/test_protected_asset_ledger_backfill.py \
  -q -p no:cacheprovider
```

Output: `17 passed in 0.09s`. Matches Codex's claim of 17 passed.

**L2 verdict: PASS** (17/17)

## §4 Layer 3 — Dry-run JSON outputs

All 4 JSONs share identical safety envelope:
- `status: "passed"`
- `db_writes: false`
- `ddl: false`
- `dry_run: true`
- `target_db: "dev"`
- `db_target: "dev:postgres@127.0.0.1:5433/aistock_dev"`

Planned-row counts:
| JSON | packages | total evidence/asset planned |
|---|---|---|
| strategy full | 4 | 12 (3 per pkg) |
| strategy limit2 | 2 | 6 |
| ledger full | 4 | 4 (1 per pkg) |
| ledger limit2 | 2 | 2 |

Matches Codex spec: **12 strategy + 4 ledger = 16 total** for full runs.

**L3 verdict: PASS**

## §5 Layer 4 — Semantic (KEY CHECK)

### §5.1 evidence_type schema match

Cross-referenced APPLY_SQL column lists against actual dev DB schema (via `\d strategy_pkg.package_validation_run` and `\d strategy_pkg.package_asset`):

`strategy_pkg.package_validation_run` columns used by strategy APPLY_SQL (lines 138-184):
- validation_run_id (PK, text NOT NULL) — OK
- package_id (text NOT NULL, FK implicit) — OK
- manifest_sha256 (text NOT NULL) — OK; selected from `p.manifest_sha256`
- validation_type, retrain_mode, seed_policy, random_seed, status, metrics_json, artifact_manifest_json, evidence_json, reproducibility_level, created_by, completed_at — ALL present in actual table.
- ON CONFLICT (validation_run_id) DO NOTHING — matches PK index.

`strategy_pkg.package_asset` columns used by ledger APPLY_SQL (lines 128-156):
- package_id, asset_type, asset_ref, asset_sha256, metadata, asset_role, protected_asset, source_uri — ALL present in actual table.
- ON CONFLICT (package_id, asset_type, asset_ref) DO NOTHING — matches `idx_package_asset_package_ref` UNIQUE index.

**§5.1 PASS** — apply SQL is schema-compatible.

### §5.2 4 prod package_id match

Executed (read-only SELECT, dev DB):
```sql
SELECT package_id, package_status FROM strategy_pkg.package
WHERE package_status IN ('BACKTEST_APPROVED','SELECTION_ENABLED','PAPER_ENABLED')
ORDER BY package_id;
```

Result (4 rows):
- pkg_006a42323f7c4e81a468fdaad2cb16a3 → PAPER_ENABLED
- pkg_1de32357724a4c5b874f2abd90f22da5 → BACKTEST_APPROVED
- pkg_99142cb1440c40a7824e83902f4e7da9 → SELECTION_ENABLED
- pkg_b668f8a633c44b72a5d557a2cb8970e3 → SELECTION_ENABLED

Exact match with the 4 IDs listed in deliver doc and present in all 4 dry-run JSONs.

**§5.2 PASS** — 4/4 match.

### §5.3 --apply confirmation step (KEY CHECK)

Guard chain documented per script (identical pattern in both):

**Guard 1 — explicit flag**: `--apply` not set → falls through to dry-run (default). Lines 342 / 311.

**Guard 2 — mutual exclusion**: `--dry-run` and `--apply` cannot be combined (line 341 / 310).

**Guard 3 — exact-string confirm token**: `--confirm-apply` must equal verbatim:
- strategy: `APPLY_STRATEGY_PACKAGE_EVIDENCE_BACKFILL_DEV_ONLY` (line 275)
- ledger: `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_DEV_ONLY` (line 244)

**Guard 4 — env var truthy**: must export
- `AISTOCK_STRATEGY_PACKAGE_EVIDENCE_BACKFILL_APPLY_ENABLED=true` (line 276)
- `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_APPLY_ENABLED=true` (line 245)

**Guard 5 — triple-check target_db**: target_db=='dev' AND port==5433 AND dbname=='aistock_dev' (lines 277-279 / 246-248). **This script CANNOT apply to prod**: passing `--target-db=prod` fails at line 277/246 BEFORE any psycopg2.connect.

**Transactional rollback**: `run_apply()` (lines 282-300 / 251-269) wraps INSERT in try/except; rollback on any error, commit only on success path, connection always closed in finally.

**Audit**: written rows tag `created_by='codex_r6_evidence_backfill'` and `application_name='AIstock-strategy-package-evidence-backfill'` (psycopg2 kwargs line 52). Sufficient for forensic SELECT.

**Severity**: This is a **dev-only** apply path. For its declared purpose it has **5 guards (defense-in-depth) + transactional rollback + audit tag = READY**.

**HOWEVER**: tomorrow's "9:30 prod apply" CANNOT use this script as-is — guards 5 will refuse. Per Codex deliver doc §Safety Design Notes line 79: "Production port 5432 is not an accepted dry-run/apply target for this package." So either (a) a SEPARATE prod-targeted entrypoint exists / will be created, or (b) tomorrow's apply is actually a dev rehearsal. **Lead must confirm which it is before issuing apply authorization.**

**§5.3 READY** (with caveat above).

## §6 Boundary confirmations

- `codex_code_modified=false` — no Codex file edited.
- `prod_db_touched=false` — never connected to port 5432.
- `dev_db_writes=false` — only SELECTed from `strategy_pkg.package` for package_id verify; no INSERT/UPDATE/DELETE.
- `prod_8001_touched=false`.
- `--apply` never invoked (only --dry-run JSONs read from disk + pytest in-process).
- paper-v2 branch HEAD unchanged at 535c539 (this verify writes to docs/ only).

## §7 R6 apply recommendation

**GO-WITH-CAUTION** for tomorrow 9:30 — conditional on the following:

1. **Confirm the apply target**. These prep scripts are dev-locked. Lead must confirm whether tomorrow's "prod apply" is:
   - (a) A dev-only rehearsal — green-light using these scripts with the documented 5-guard chain.
   - (b) A real prod apply — a separate prod-authorized entrypoint is required; these scripts will refuse `--target-db=prod`.

2. **Pre-apply dev rehearsal**: before any real prod move, run `--apply` against dev with the documented env+token, verify exactly 16 rows materialize (12 in package_validation_run + 4 in package_asset for the 4 packages), and verify idempotency (re-run yields ON CONFLICT DO NOTHING / 0 new rows).

3. **Post-apply check**: SELECT `created_by='codex_r6_evidence_backfill'` rows after apply for the audit trail.

4. **Rollback plan**: since INSERT uses ON CONFLICT DO NOTHING, manual cleanup if needed is `DELETE FROM strategy_pkg.package_validation_run WHERE created_by='codex_r6_evidence_backfill'` (dev only).

No blocking issues found. Defense-in-depth verified.
