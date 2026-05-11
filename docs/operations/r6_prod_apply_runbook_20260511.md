# AIstock R6 Production Apply Runbook - 2026-05-11

Status: draft operator-safe runbook. This document is not an authorization to execute production actions.

## 0. Hard Safety Boundary

Do not run any command in this runbook unless all of the following are true:

1. The user has explicitly authorized the exact R6 production cutover window in the current thread/session.
2. The strategy author or designated release owner has explicitly authorized the exact StrategyPackage/evidence set.
3. The operator has read the command, replaced placeholders, and confirmed that no real password or secret will be pasted into logs, chat, Git, shell history, or evidence files.
4. The operator is on the approved production host/session and understands whether each step is read-only, DB write, DDL, code sync, or runtime restart.

This runbook contains templates only. It intentionally uses placeholders such as `<PROD_DB_HOST>`, `<PROD_DB_PASSWORD>`, `<R6_RELEASE_COMMIT>`, and `<PACKAGE_ID>`. Never commit or paste real credentials into this file.

Production-impacting categories in this runbook:

| Category | Examples | Requires user + strategy authorization immediately before run? |
| --- | --- | --- |
| Read-only production preflight | catalog SELECTs, status checks, health checks | Yes, because it touches production targets |
| Production DB write | evidence backfill rows, promotion/evidence rows | Yes |
| Production DDL | six governance migrations | Yes |
| Code sync | merge/sync into `F:/Dev/AIstock` production root | Yes |
| Runtime restart | backend `8001`, daemon enable/restart | Yes |
| 9:30 cutover | A-share open monitoring and go-live decision | Yes |

Non-goals:

- It does not execute production commands.
- It does not authorize production DB writes or DDL.
- It does not contain real credentials.
- It does not replace strategy-owner sign-off for selected packages.
- It does not bypass existing dev-only guards in prep scripts.

Current verification caveat for the R6 backfill prep package:

- Paper-v2 verification reported Codex `b976c23` as `READY-WITH-CAVEATS` / `GO-WITH-CAUTION`.
- The four-layer verify passed static safety, tests, dry-run JSON content, and guard checks.
- The guard caveat is intentional and blocking for direct production use: the current backfill scripts are dev-locked and cannot apply to production.
- Production backfill therefore needs a separate strategy/user-approved production executor or reviewed SQL package; do not edit guards or pass `--target-db prod` to force the prep scripts.

## 1. Roles And Communication

| Role | Required owner | Responsibility |
| --- | --- | --- |
| Release commander | `<USER_OR_RELEASE_OWNER>` | Final go/no-go and explicit authorization for each production-impacting phase |
| Strategy author | `<STRATEGY_AUTHOR>` | Confirms R6 packages, evidence bundle, package ids, manifest hashes, and trading readiness |
| DB operator | `<DB_OPERATOR>` | DR snapshot, read-only preflight, evidence writes, DDL, rollback DB actions |
| Runtime operator | `<RUNTIME_OPERATOR>` | Backend `8001` and daemon enable/restart/cold-start checks |
| Observer/scribe | `<SCRIBE>` | Records timestamps, command ids, redacted outputs, go/no-go decisions, rollback evidence |

Communication rules:

- Keep one live incident/cutover log with timestamps in Asia/Shanghai time.
- Before every write/DDL/restart step, the release commander states: `AUTHORIZED: <step-id> <time> <scope>`.
- If any operator is unsure whether a command is read-only or mutating, stop and ask before running.
- If Claude Code and Codex App are both active, coordinate through the shared cross-tool channel and avoid overlapping edits or production commands.

## 2. Required Inputs

Fill these before the window. Do not include secrets in the filled copy unless it stays in a secure, non-Git operational vault.

| Input | Placeholder | Required value |
| --- | --- | --- |
| Production repo path | `<PROD_REPO>` | Example shape: `F:/Dev/AIstock` |
| Integration worktree | `<R6_WORKTREE>` | `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509` |
| Release branch | `<R6_BRANCH>` | `codex/qe-governance-integration-20260509` |
| Release commit | `<R6_RELEASE_COMMIT>` | Exact commit approved for production |
| Current production commit | `<PROD_PRE_CUTOVER_COMMIT>` | Exact commit before sync |
| Git remote | `<REMOTE>` | Usually `origin` |
| Production DB host | `<PROD_DB_HOST>` | Redacted host label in logs if sensitive |
| Production DB port | `<PROD_DB_PORT>` | Usually `5432`, confirm target |
| Production DB name | `<PROD_DB_NAME>` | Redact if policy requires |
| Production DB user | `<PROD_DB_USER>` | Least-privilege operator user |
| Production DB password source | `<PROD_DB_PASSWORD_SOURCE>` | Secure secret manager or protected env var only |
| Backend service name | `<BACKEND_SERVICE>` | Example shape: `aistock-backend.service` |
| Paper/R6 daemon service name | `<R6_DAEMON_SERVICE>` | Confirm exact unit before enable/restart |
| Backend URL | `<PROD_API_BASE>` | Example shape: `http://127.0.0.1:8001/api/v1` |
| Evidence bundle | `<R6_EVIDENCE_BUNDLE>` | Strategy-approved JSON bundle path/hash |
| Package ids | `<PACKAGE_IDS>` | Exact four R6 StrategyPackage ids or approved list |

Credential handling:

```powershell
# Template only. Do not paste real passwords into chat, run logs, or this document.
$env:TDX_DB_HOST = '<PROD_DB_HOST>'
$env:TDX_DB_PORT = '<PROD_DB_PORT>'
$env:TDX_DB_NAME = '<PROD_DB_NAME>'
$env:TDX_DB_USER = '<PROD_DB_USER>'
$env:TDX_DB_PASSWORD = '<READ_FROM_SECURE_SECRET_STORE_AT_RUNTIME>'
```

Do not use inline password flags in commands unless shell history is disabled and the release commander explicitly approves that method. Prefer environment variables injected from a secure secret store.

## 3. Time Budget

Recommended R6 cutover budget for a 9:30 A-share open:

| Time CST (UTC+8) | Deadline | Phase | Max duration | Abort if not complete by |
| --- | --- | --- | ---: | --- |
| T-1 trading day or 08:00 | Pre-window | Approval, package/evidence finalization, dry-run evidence reviewed | 30-60 min | 08:45 |
| 08:45 | Start | DR snapshot and final read-only preflight | 10-15 min | 09:00 |
| 09:00 | DDL | Six migrations apply, catalog verification | 8-12 min | 09:12 |
| 09:12 | DB writes | Evidence backfill apply, verification | 5-8 min | 09:20 |
| 09:20 | Code sync | R6 git merge/sync into production root | 3-5 min | 09:24 |
| 09:24 | Runtime | Backend `8001` plus daemon enable/restart | 3-4 min | 09:27 |
| 09:27 | Cold start | Health/governance/paper sanity checks | 2-3 min | 09:29 |
| 09:29 | Final gate | Go/no-go for 9:30 | 1 min | 09:30 |
| 09:30-09:45 | Observe | A-share open monitoring | 15 min | Roll back on red criteria |

If any phase reaches its abort deadline without a clean pass, default to no-go and rollback/hold according to the phase-specific rollback section.

Hard time gates:

- If package ids, manifest hashes, and the production-capable evidence/asset executor or reviewed SQL package are not approved by tonight's release review or by 08:30 CST at the latest, declare no-go for the 9:30 cutover.
- If DR snapshot plus production read-only catalog preflight are not green by 09:00 CST, declare no-go.
- If no reviewed production-capable evidence/asset executor or SQL package exists by 09:00 CST, stop/hold R6 and do not substitute the dev-only scripts.
- If DDL is not fully applied and verified by 09:20 CST, do not start code sync, backend restart, or daemon activation for the 9:30 open.

## 4. Preflight Checklist

### 4.1 Authorization Gate

Do not proceed unless all boxes are checked in the live cutover log:

- [ ] User explicitly approved R6 production cutover for the current date/time window.
- [ ] Strategy author approved package ids, manifests, runtime variants, and evidence bundle.
- [ ] DB operator approved snapshot/restore approach and confirmed restore has been tested recently.
- [ ] Runtime operator confirmed exact backend and daemon service names.
- [ ] Release commander accepted this runbook's rollback criteria.
- [ ] No other agent/operator is editing or syncing the production root concurrently.

### 4.2 Worktree Hygiene

Run only after authorization to inspect production and release worktrees. These are read-only Git/status checks.

```powershell
# Template only; do not run unless authorized for production cutover preflight.
Set-Location '<R6_WORKTREE>'
git status --short --branch
git rev-parse HEAD
git log --oneline -8

Set-Location '<PROD_REPO>'
git status --short --branch
git rev-parse HEAD
git log --oneline -8
```

Pass criteria:

- Release worktree is on `<R6_BRANCH>` at `<R6_RELEASE_COMMIT>`.
- Production root is at known `<PROD_PRE_CUTOVER_COMMIT>` before sync.
- Production root does not contain unclassified local functional changes.
- Any untracked production files are classified as expected runtime artifacts, backed up, or a no-go is declared.

No-go criteria:

- Production root has unknown modified tracked files.
- `origin/main` or release branch has advanced unexpectedly and has not been reviewed.
- Another operator/agent is actively editing the same production root.

### 4.3 Static Governance Plan

This is non-DB static validation from the release worktree.

```powershell
# Template only; safe only after authorization to run release preflight.
Set-Location '<R6_WORKTREE>'
python scripts/governance_production_apply_plan.py --json --output '<SECURE_EVIDENCE_DIR>/r6_governance_production_apply_plan.json'
python scripts/governance_migration_smoke.py --json > '<SECURE_EVIDENCE_DIR>/r6_governance_migration_static_smoke.json'
```

Pass criteria:

- `ddl_executed=false`.
- `db_writes_executed=false`.
- Migration apply order has exactly six files.
- Static smoke status is `passed`.
- Operator opens/parses the output JSON file and records those fields in the live cutover log; `--output` may write the evidence file without printing all details to the console.

### 4.4 Production Read-only Catalog Preflight

This step opens a production DB connection and performs SELECT-only catalog inspection. It is still a production touch and requires authorization.

```powershell
# Template only. Do not run unless user/release commander authorizes production DB read-only preflight.
Set-Location '<R6_WORKTREE>'
$env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT = 'true'
python scripts/governance_migration_smoke.py `
  --production-readonly-preflight `
  --confirm-production-readonly-preflight QE_GOVERNANCE_PROD_READONLY_PREFLIGHT `
  --db-host '<PROD_DB_HOST>' `
  --db-port '<PROD_DB_PORT>' `
  --db-name '<PROD_DB_NAME>' `
  --db-user '<PROD_DB_USER>' `
  --json > '<SECURE_EVIDENCE_DIR>/r6_prod_readonly_catalog_preflight.json'
Remove-Item Env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT -ErrorAction SilentlyContinue
```

Pass criteria:

- Connection target matches the approved production DB.
- Output contains no password or secret.
- Base dependencies exist: `strategy_pkg.package` and `public.aistock_model_catalog`.
- Missing objects match the expected not-yet-applied governance stack, or `apply_needed=false` if already applied.
- No DDL or writes are reported.

No-go criteria:

- Preflight cannot connect.
- Base dependencies are missing.
- Existing objects conflict with expected names/types.
- Output contains secrets.
- Any unexpected schema drift is found.

## 5. DR Snapshot And Restore Point

The DR snapshot must happen before migrations, evidence backfill, code sync, or restarts.

### 5.1 Database Snapshot

Use the DB team's approved snapshot method. Examples below are templates only.

Run DB backup commands from an ephemeral/operator-approved shell profile where command history does not persist secrets. After each command, confirm `PGPASSWORD` or equivalent env vars are removed before copying logs or sharing evidence.

Logical backup template:

```powershell
# Template only. Do not run unless DB operator and user authorize production DR snapshot.
$env:PGPASSWORD = '<READ_FROM_SECURE_SECRET_STORE_AT_RUNTIME>'
pg_dump `
  --host '<PROD_DB_HOST>' `
  --port '<PROD_DB_PORT>' `
  --username '<PROD_DB_USER>' `
  --dbname '<PROD_DB_NAME>' `
  --format custom `
  --file '<SECURE_BACKUP_DIR>/r6_pre_cutover_<YYYYMMDD_HHMMSS>.dump'
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
```

Managed snapshot template:

```text
# Template only. Use the cloud/VM/storage provider's approved snapshot action.
SNAPSHOT_NAME=r6-pre-cutover-<YYYYMMDD-HHMMSS>
TARGET=<PROD_DB_INSTANCE_OR_VOLUME>
ACTION=create-snapshot
```

Pass criteria:

- Snapshot id/path is recorded in the cutover log.
- Snapshot completion is confirmed by the DB operator.
- Restore command or provider rollback procedure is known and tested.
- Backup evidence includes checksum/size/status without secrets.

No-go criteria:

- Snapshot fails, is partial, or cannot be verified.
- Restore procedure is unknown.
- Backup path is on the same fragile disk without retention protection.

### 5.2 Production Code Snapshot

Before syncing `F:/Dev/AIstock`, preserve the current production commit and any local classified files.

```powershell
# Template only. Do not run unless production sync is authorized.
Set-Location '<PROD_REPO>'
git rev-parse HEAD > '<SECURE_EVIDENCE_DIR>/r6_prod_pre_cutover_commit.txt'
git status --short --branch > '<SECURE_EVIDENCE_DIR>/r6_prod_pre_cutover_git_status.txt'

# If local-only files exist, use the established backup area and classify before proceeding.
# Do not use git reset --hard or git clean unless the release commander explicitly authorizes the exact sync procedure.
```

## 6. Six Governance Migrations Apply

Apply migrations only after DR snapshot, production read-only preflight, and release-commander authorization for production DDL. Evidence backfill must not run before the required governance tables/columns exist. The current `governance_migration_smoke.py --apply` is guarded against production-like targets and is not the production DDL executor. Production DDL should be applied by the DB operator using reviewed SQL files in the exact order below, with one explicit transaction window per migration unless the DB team chooses a safer equivalent.

Required order:

1. `backend/migrations/strategy_pkg_package_asset_20260509.sql`
2. `backend/migrations/qe_phase4_master_seed_contract_20260509.sql`
3. `backend/migrations/strategy_pkg_runtime_variant_20260509.sql`
4. `backend/migrations/strategy_pkg_validation_run_20260509.sql`
5. `backend/migrations/strategy_pkg_promotion_review_20260509.sql`
6. `backend/migrations/model_registry_phase5_20260509.sql`

DDL safety settings template:

```sql
-- Template only. DB operator may adjust timeouts based on production policy.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '120s';
-- Execute exactly one reviewed migration file here.
COMMIT;
```

When using `psql --single-transaction --file`, do not add manual `BEGIN`/`COMMIT` wrappers around the file; `psql` owns the transaction. If the DB operator instead pastes SQL manually, use the explicit `BEGIN`/`COMMIT` template above for exactly one migration at a time.

PowerShell `psql` template per migration:

```powershell
# Template only. Do not run unless production DDL is authorized for this exact migration.
$env:PGOPTIONS = '-c lock_timeout=3s -c statement_timeout=120s'
$env:PGPASSWORD = '<READ_FROM_SECURE_SECRET_STORE_AT_RUNTIME>'
psql `
  --host '<PROD_DB_HOST>' `
  --port '<PROD_DB_PORT>' `
  --username '<PROD_DB_USER>' `
  --dbname '<PROD_DB_NAME>' `
  --set ON_ERROR_STOP=1 `
  --single-transaction `
  --file '<R6_WORKTREE>/backend/migrations/<MIGRATION_FILE>.sql' `
  *> '<SECURE_EVIDENCE_DIR>/r6_migration_<STEP>_<MIGRATION_FILE>.log'
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
Remove-Item Env:PGOPTIONS -ErrorAction SilentlyContinue
```

Do not use the dev-only rollback helper in production. `backend/migrations/model_registry_phase5_20260509_rollback.sql` is guarded by the dev-only token `DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY` and is not a production rollback plan.

After each migration, confirm `PGPASSWORD` has been removed from the environment before saving or sharing command logs. Redact psql logs before cross-tool/chat sharing because connection errors may include host/user/database context.

Post-apply verification template:

```powershell
# Template only. SELECT-only catalog verification after authorized migration apply.
Set-Location '<R6_WORKTREE>'
$env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT = 'true'
python scripts/governance_migration_smoke.py `
  --production-readonly-preflight `
  --confirm-production-readonly-preflight QE_GOVERNANCE_PROD_READONLY_PREFLIGHT `
  --db-host '<PROD_DB_HOST>' `
  --db-port '<PROD_DB_PORT>' `
  --db-name '<PROD_DB_NAME>' `
  --db-user '<PROD_DB_USER>' `
  --json > '<SECURE_EVIDENCE_DIR>/r6_post_migration_catalog_preflight.json'
Remove-Item Env:AISTOCK_QE_GOVERNANCE_PROD_READONLY_PREFLIGHT -ErrorAction SilentlyContinue
```

Pass criteria:

- Each migration exits successfully with no timeout/error.
- Post-apply catalog preflight shows `apply_needed=false` or zero missing governance objects.
- Required schemas/tables/views/indexes/constraints are present.
- No destructive DDL was executed outside the reviewed files.

Rollback per migration:

- Before commit failure: rollback the active transaction and stop cutover.
- After one or more migrations committed: prefer full DB restore from DR snapshot if schema state is unsafe.
- If a reviewed rollback file exists for a migration, use it only if DB operator and release commander approve the exact rollback step.
- Do not improvise `DROP TABLE`, `DROP SCHEMA`, or constraint removals in production.

## 7. Evidence Backfill Apply Order

Important: current prep scripts in this branch are intentionally dev-only for `--apply` and reject production targets by design. Do not bypass those guards. For production R6, run evidence backfill apply only if the strategy author/release commander has provided and approved the exact production-capable executor or SQL package derived from the reviewed plan.

Prerequisite: the six governance migrations above must already be applied and verified in the target production DB, because the backfill rows depend on `strategy_pkg.package_asset`, `strategy_pkg.package_validation_run`, `strategy_pkg.package_runtime_variant`, and `strategy_pkg.seed_fragility_score`.

No-go gate: if no reviewed production-capable evidence/asset executor or SQL package exists for this section, stop before any DB write. Do not substitute `scripts/strategy_package_evidence_backfill.py --apply`, `scripts/protected_asset_ledger_backfill.py --apply`, or any locally edited variant of those prep scripts.

Strategy-approved evidence input must include:

- Exact package ids and manifest hashes.
- Protected asset metadata and hash/URI evidence.
- Original fixed-weight validation evidence.
- Runtime variant validation evidence.
- Seed/regime sample-count and stability notes.
- Operator/reviewer identity and approval timestamp.

Recommended production apply sequence after DDL verification:

1. Re-run evidence plan against the approved bundle in dry-run/planner mode.
2. Apply protected asset ledger rows.
3. Apply StrategyPackage validation/runtime/seed evidence rows.
4. Verify idempotency and row counts.
5. Verify governance eligibility remains blocked or ready exactly as expected before runtime activation.

Exception path: if production read-only preflight shows all required governance objects are already present (`apply_needed=false`), the release commander may explicitly skip the DDL phase and proceed to evidence backfill. Record that decision in the cutover log with the preflight artifact path.

### 7.1 Evidence Plan Dry-run

```powershell
# Template only; planner mode should not open prod DB unless the approved tool explicitly says it does.
Set-Location '<R6_WORKTREE>'
python scripts/strategy_package_governance_evidence_backfill_plan.py `
  --evidence-bundle '<R6_EVIDENCE_BUNDLE>' `
  --package-id '<PACKAGE_ID_1>' `
  --package-id '<PACKAGE_ID_2>' `
  --package-id '<PACKAGE_ID_3>' `
  --package-id '<PACKAGE_ID_4>' `
  --output '<SECURE_EVIDENCE_DIR>/r6_evidence_backfill_plan.json'
```

Pass criteria:

- `package_count` equals the strategy-approved package count.
- `blocked_packages={}` unless the release commander intentionally decides no-go.
- `db_writes_executed=false` and `service_calls_executed=false` for planner mode.
- Planned rows match strategy author expectation.

### 7.2 StrategyPackage Evidence Backfill Apply

Template for a future approved production-capable executor. Do not run the current dev-only `scripts/strategy_package_evidence_backfill.py --apply` against production.

```powershell
# Template only. Do not run unless a production-capable executor is explicitly approved for R6.
# Current prep script is dev-only; replace this command shape with the approved R6 executor if different.
Set-Location '<R6_WORKTREE>'
$env:AISTOCK_R6_PROD_EVIDENCE_BACKFILL_APPLY_ENABLED = 'true'
python <APPROVED_R6_STRATEGY_EVIDENCE_BACKFILL_EXECUTOR>.py `
  --apply `
  --confirm-apply '<APPROVED_R6_PROD_EVIDENCE_BACKFILL_TOKEN>' `
  --evidence-bundle '<R6_EVIDENCE_BUNDLE>' `
  --db-host '<PROD_DB_HOST>' `
  --db-port '<PROD_DB_PORT>' `
  --db-name '<PROD_DB_NAME>' `
  --db-user '<PROD_DB_USER>' `
  --json > '<SECURE_EVIDENCE_DIR>/r6_strategy_package_evidence_backfill_apply.json'
Remove-Item Env:AISTOCK_R6_PROD_EVIDENCE_BACKFILL_APPLY_ENABLED -ErrorAction SilentlyContinue
```

Expected writes, if authorized by the approved executor:

- `strategy_pkg.package_validation_run` rows for original fixed-weight and runtime-variant evidence.
- `strategy_pkg.package_runtime_variant` rows for validated R6 runtime candidates.
- Optional `strategy_pkg.seed_fragility_score` rows if the strategy-approved bundle contains valid stability evidence.
- No mutation of frozen manifests, model assets, HMM snapshots, or raw QE artifacts.

### 7.3 Protected Asset Ledger Backfill Apply

Template for a future approved production-capable executor. Do not run the current dev-only `scripts/protected_asset_ledger_backfill.py --apply` against production.

```powershell
# Template only. Do not run unless a production-capable executor is explicitly approved for R6.
Set-Location '<R6_WORKTREE>'
$env:AISTOCK_R6_PROD_ASSET_LEDGER_BACKFILL_APPLY_ENABLED = 'true'
python <APPROVED_R6_PROTECTED_ASSET_LEDGER_EXECUTOR>.py `
  --apply `
  --confirm-apply '<APPROVED_R6_PROD_ASSET_LEDGER_BACKFILL_TOKEN>' `
  --evidence-bundle '<R6_EVIDENCE_BUNDLE>' `
  --db-host '<PROD_DB_HOST>' `
  --db-port '<PROD_DB_PORT>' `
  --db-name '<PROD_DB_NAME>' `
  --db-user '<PROD_DB_USER>' `
  --json > '<SECURE_EVIDENCE_DIR>/r6_protected_asset_ledger_backfill_apply.json'
Remove-Item Env:AISTOCK_R6_PROD_ASSET_LEDGER_BACKFILL_APPLY_ENABLED -ErrorAction SilentlyContinue
```

Expected writes, if authorized:

- `strategy_pkg.package_asset` metadata rows for protected governance/evidence assets.
- `protected_asset=true` must be explicit.
- No file copy, delete, overwrite, or manifest mutation.

### 7.4 Evidence Backfill Verification

Use SQL templates through the approved DB client. Redact output if any field contains sensitive paths or credentials.

```sql
-- Template only. Read-only verification after authorized evidence apply.
SELECT package_id, manifest_sha256, package_status
FROM strategy_pkg.package
WHERE package_id IN (<PACKAGE_ID_LIST>);

SELECT package_id, validation_type, status, runtime_variant_id, created_at
FROM strategy_pkg.package_validation_run
WHERE package_id IN (<PACKAGE_ID_LIST>)
ORDER BY package_id, created_at DESC;

SELECT package_id, variant_kind, validation_status, paper_candidate, variant_hash, manifest_sha256
FROM strategy_pkg.package_runtime_variant
WHERE package_id IN (<PACKAGE_ID_LIST>)
ORDER BY package_id, created_at DESC;

SELECT package_id, asset_type, asset_role, protected_asset, asset_sha256
FROM strategy_pkg.package_asset
WHERE package_id IN (<PACKAGE_ID_LIST>)
ORDER BY package_id, asset_type, asset_role;
```

Pass criteria:

- Package ids and manifest hashes match strategy-approved bundle.
- Row counts match apply report.
- Re-running apply in idempotent mode would plan zero duplicate writes or stable upserts only.
- No frozen manifest or asset hash changed unexpectedly.

Rollback for evidence backfill:

- Preferred rollback is restore from the pre-cutover DB snapshot if any evidence write is suspect and cutover cannot continue.
- If the approved executor provides exact row ids and a reviewed delete/revert SQL package, the DB operator may use that only with explicit rollback authorization.
- Do not manually delete evidence rows ad hoc without row-id evidence and release commander approval.

## 8. R6 Git Merge/Sync Into Production Root

Code sync and runtime reload are separate. Updating files in `<PROD_REPO>` does not mean backend `8001` has loaded them.

Pre-sync checks:

```powershell
# Template only. Do not run unless production code sync is authorized.
Set-Location '<R6_WORKTREE>'
git fetch '<REMOTE>'
git rev-parse HEAD
git status --short --branch

Set-Location '<PROD_REPO>'
git fetch '<REMOTE>'
git status --short --branch
git rev-parse HEAD
```

Safe sync template option A - fast-forward to approved release commit:

```powershell
# Template only. Do not run unless release commander authorizes this exact sync.
Set-Location '<PROD_REPO>'
git fetch '<REMOTE>'
git merge --ff-only '<R6_RELEASE_COMMIT>'
git rev-parse HEAD > '<SECURE_EVIDENCE_DIR>/r6_prod_post_sync_commit.txt'
git status --short --branch > '<SECURE_EVIDENCE_DIR>/r6_prod_post_sync_git_status.txt'
```

Safe sync template option B - merge release branch after review:

```powershell
# Template only. Do not run unless release commander authorizes this exact sync.
Set-Location '<PROD_REPO>'
git fetch '<REMOTE>'
git merge --no-ff '<REMOTE>/<R6_BRANCH>' -m 'merge(qe): R6 governance production cutover'
git rev-parse HEAD > '<SECURE_EVIDENCE_DIR>/r6_prod_post_sync_commit.txt'
git status --short --branch > '<SECURE_EVIDENCE_DIR>/r6_prod_post_sync_git_status.txt'
```

Do not run these unless explicitly authorized and backups are complete:

```powershell
# Dangerous templates. Prefer avoiding them in production root.
# git reset --hard <commit>
# git clean -fd
```

Pass criteria:

- Production root reaches the approved `<R6_RELEASE_COMMIT>` or an approved merge commit containing it.
- No unresolved conflicts.
- No unclassified local production files were removed.
- `git status` is clean or contains only approved runtime artifacts.

Rollback for code sync:

- If sync fails before runtime restart, stop and restore production root to `<PROD_PRE_CUTOVER_COMMIT>` using the pre-approved Git rollback path.
- If sync succeeded but runtime not restarted, production process may still be running old code; rollback can be a Git revert/sync back before restart.
- If runtime restarted on new code, rollback includes both Git rollback and backend/daemon restart to reload old code.

## 9. Backend `8001` And Daemon Enable/Restart

Do not restart backend `8001` or enable/restart daemons until DB migrations and code sync have passed and the release commander explicitly authorizes runtime activation.

### 9.1 Pre-restart Status Snapshot

```powershell
# Template only. Do not run unless runtime preflight is authorized.
Get-Date -Format o
Get-Process -Id (Get-NetTCPConnection -LocalPort 8001 -State Listen).OwningProcess | Format-List * > '<SECURE_EVIDENCE_DIR>/r6_pre_restart_8001_process.txt'
Invoke-WebRequest -Uri '<PROD_API_BASE>/health' -UseBasicParsing -TimeoutSec 10 > '<SECURE_EVIDENCE_DIR>/r6_pre_restart_backend_health.txt'

# If managed by systemd on a Linux host, use the host's service manager instead:
# systemctl status '<BACKEND_SERVICE>' --no-pager
# systemctl status '<R6_DAEMON_SERVICE>' --no-pager
```

### 9.2 Backend Restart Template

Windows/service-manager shape:

```powershell
# Template only. Do not run unless backend 8001 restart is explicitly authorized.
Restart-Service -Name '<BACKEND_SERVICE>'
Start-Sleep -Seconds 5
Invoke-WebRequest -Uri '<PROD_API_BASE>/health' -UseBasicParsing -TimeoutSec 20
```

Systemd shape:

```bash
# Template only. Do not run unless backend 8001 restart is explicitly authorized.
sudo systemctl restart '<BACKEND_SERVICE>'
sudo systemctl status '<BACKEND_SERVICE>' --no-pager
curl -fsS --max-time 20 '<PROD_API_BASE>/health'
```

### 9.3 R6/Paper Daemon Enable/Restart Template

Confirm the exact daemon name and environment flags before use. Do not enable a scheduler/daemon that can advance trading state unless strategy author and user explicitly authorize live/paper activation.

```bash
# Template only. Do not run unless daemon enable/restart is explicitly authorized.
sudo systemctl enable '<R6_DAEMON_SERVICE>'
sudo systemctl restart '<R6_DAEMON_SERVICE>'
sudo systemctl status '<R6_DAEMON_SERVICE>' --no-pager
journalctl -u '<R6_DAEMON_SERVICE>' --since '<YYYY-MM-DD HH:MM:SS +0800>' --no-pager | tail -200
```

Pass criteria:

- Backend `8001` responds on health/openapi endpoints.
- Expected R6/governance routes are present.
- Daemon is enabled only if explicitly intended.
- Daemon logs show no credential leak, import error, DB schema error, failed scheduler ownership, or unexpected order submission.

Rollback for runtime activation:

- If backend fails cold start, restart old code after Git rollback or disable R6-specific env/daemon flags and restart backend.
- If daemon fails, disable/stop daemon first, then assess backend health.
- If any order/trading action is unexpectedly attempted, stop daemon immediately and escalate to incident response.

## 10. Cold-start Sanity Checks

Run after backend `8001` restart and daemon activation decision. These are production runtime checks and require authorization.

```powershell
# Template only. Do not run unless production runtime smoke is authorized.
Invoke-WebRequest -Uri '<PROD_API_BASE>/health' -UseBasicParsing -TimeoutSec 20
Invoke-WebRequest -Uri 'http://127.0.0.1:8001/openapi.json' -UseBasicParsing -TimeoutSec 20 > '<SECURE_EVIDENCE_DIR>/r6_openapi_after_restart.json'
```

Governance route sanity template:

```powershell
# Template only. Read-only API checks; do not include secrets in URL/query.
foreach ($pkg in @('<PACKAGE_ID_1>', '<PACKAGE_ID_2>', '<PACKAGE_ID_3>', '<PACKAGE_ID_4>')) {
  Invoke-WebRequest -Uri "<PROD_API_BASE>/strategy-packages/$pkg/governance-eligibility" -UseBasicParsing -TimeoutSec 20 |
    Out-File "<SECURE_EVIDENCE_DIR>/r6_governance_eligibility_$pkg.json"
}
```

Optional existing read-only smoke, if production use is explicitly allowed for the window:

```powershell
# Template only. Existing smoke refuses production 8001 by default; use the explicit allow flag only when release commander authorizes production readonly smoke.
python scripts/strategy_package_governance_readonly_smoke.py `
  --api-base '<PROD_API_BASE>' `
  --allow-production-8001 `
  --timeout 5 `
  --limit 5 `
  --output '<SECURE_EVIDENCE_DIR>/r6_strategy_package_governance_readonly_smoke.json'
```

Pass criteria:

- Backend health is OK.
- OpenAPI contains expected R6/governance endpoints.
- Each package's `manifest_sha256` matches approved manifest.
- `paper_ready`, blockers, and satisfied gates match strategy-author expectations.
- No 500 errors, traceback, credential leak, or stale route mismatch.

No-go criteria:

- Backend fails health or route checks.
- Governance eligibility contradicts expected status.
- Any package id/manifest hash mismatch.
- Daemon logs show unexpected scheduler/order activity.

## 11. Final 9:30 A-share Cutover Gate

At 09:29 CST, release commander chooses go/no-go.

Go criteria:

- DR snapshot verified.
- Six migrations applied and catalog preflight clean.
- Evidence backfill apply verified.
- Production code sync clean.
- Backend `8001` restarted and health checks pass.
- Daemon enable/restart state matches the strategy-author plan.
- All four package ids/manifests/governance eligibility outputs match approved expectations.
- No unredacted secrets in logs/evidence.
- Strategy author explicitly says R6 is ready for A-share open monitoring.

No-go criteria:

- Any required evidence is missing or mismatched.
- Any DB migration produced unknown partial state.
- Backend/daemon health is not green by 09:29.
- Production root has unresolved Git state.
- Any operator reports uncertainty about target, credentials, package ids, or strategy intent.
- A market/data feed dependency is not ready.

At 09:30:

```text
# Template cutover log entry.
09:30:00 CST R6 GO=<yes/no> authorized_by=<release_commander> strategy_author=<strategy_author> packages=<PACKAGE_IDS> backend_8001=<healthy/unhealthy> daemon=<enabled/disabled/running/stopped>
```

If no-go, keep R6 disabled/rolled back and record the blocker. Do not improvise fixes during the market open unless the release commander opens an incident bridge and explicitly authorizes emergency remediation.

## 12. A-share Open Monitoring

Monitor from 09:30 to at least 09:45 CST.

Read-only monitoring templates:

```powershell
# Template only. Do not run unless production monitoring is authorized.
Invoke-WebRequest -Uri '<PROD_API_BASE>/health' -UseBasicParsing -TimeoutSec 10
# Add approved read-only R6/daemon status endpoint here when confirmed:
# Invoke-WebRequest -Uri '<PROD_API_BASE>/<APPROVED_R6_STATUS_ENDPOINT>' -UseBasicParsing -TimeoutSec 10
```

Service log template:

```bash
# Template only. Redact secrets before saving/sharing.
journalctl -u '<BACKEND_SERVICE>' --since '09:20:00' --no-pager | tail -300
journalctl -u '<R6_DAEMON_SERVICE>' --since '09:20:00' --no-pager | tail -300
```

Observe:

- Backend error rate and 5xx responses.
- DB lock waits, long transactions, failed migrations/backfill leftovers.
- Daemon scheduler ownership and state transitions.
- Package/governance eligibility stability.
- Market data readiness and timestamps.
- Any unexpected order generation or execution path activity.

Rollback during monitoring if:

- Backend `8001` becomes unhealthy.
- Daemon enters unexpected active trading/order state.
- Governance eligibility flips unexpectedly.
- DB errors/locks threaten production availability.
- Strategy author or release commander calls stop.

## 13. Rollback Matrix

| Failure point | Preferred immediate action | Full rollback path | Evidence to capture |
| --- | --- | --- | --- |
| Preflight fails | Stop, no production writes | None needed; production unchanged | Preflight JSON/log |
| DR snapshot fails | Stop, no production writes | None needed; production unchanged | Snapshot error |
| Evidence backfill fails before commit | Roll back transaction | Re-run dry-run after fix in later window | Error + transaction status |
| Evidence backfill commits wrong rows | Stop; do not continue to DDL | Restore DB snapshot or execute reviewed row-id rollback | Apply report + row ids |
| Migration fails before commit | Roll back active transaction | Stop; production schema unchanged for that migration | psql log |
| Migration commits but post-check fails | Stop; disable runtime activation | Restore DB snapshot or reviewed migration rollback | Catalog preflight |
| Git sync fails | Stop before restart | Restore production root to pre-cutover commit | Git status/log |
| Backend restart fails | Stop daemon; keep trading disabled | Git rollback + backend restart on previous commit | Service logs |
| Daemon fails or misbehaves | Stop/disable daemon immediately | Keep backend running if safe; rollback daemon config/code if needed | Daemon logs/status |
| 9:30 cutover no-go | Do not activate R6 | Keep prior production state or rollback already-applied DB/code per commander decision | Go/no-go log |

Rollback command templates:

```powershell
# Template only. Use only with explicit rollback authorization.
Set-Location '<PROD_REPO>'
git status --short --branch
git merge --abort  # only if a merge is in progress and release commander authorizes
# or approved rollback commit/revert procedure:
# git revert <R6_MERGE_COMMIT>
# git reset --hard '<PROD_PRE_CUTOVER_COMMIT>'  # destructive; requires explicit authorization and backups
```

```bash
# Template only. Use only with explicit rollback authorization.
sudo systemctl stop '<R6_DAEMON_SERVICE>'
sudo systemctl disable '<R6_DAEMON_SERVICE>'
sudo systemctl restart '<BACKEND_SERVICE>'
```

DB restore must be run by the DB operator using the pre-approved restore runbook for this production DB, recorded as `<RESTORE_RUNBOOK_ID>`. Do not invent a `pg_restore` command during the incident window: restore flags such as `--clean` or `--if-exists` are destructive, while omitting them may not restore over changed objects.

## 14. Evidence Package For Completion

After the window, archive a redacted evidence package outside Git if it contains sensitive operational details.

Required evidence:

- Authorization log with timestamps and approvers.
- DR snapshot id/path/checksum/status.
- Static governance plan JSON.
- Production read-only preflight JSON before DDL.
- Six migration logs and post-migration catalog preflight JSON.
- Evidence backfill plan/apply report and post-apply verification.
- Production Git pre/post commit and status files.
- Backend/daemon pre/post status and health check outputs.
- Governance eligibility outputs for approved package ids.
- 9:30 go/no-go log and 09:30-09:45 monitoring notes.
- Rollback actions, if any.

Redaction requirements:

- Replace passwords, tokens, DSNs, cookies, API keys, account ids, broker credentials, and private hostnames if policy requires.
- Do not commit raw service logs if they contain credentials or account/trading details.
- Do not paste secrets into cross-tool memory, chat, GitHub issues, Markdown docs, or screenshots.
- If a command accidentally prints a secret, rotate the secret and mark the evidence artifact as restricted.

## 15. Final Operator Checklist

Pre-apply:

- [ ] User and strategy author authorization recorded.
- [ ] Package ids and manifest hashes confirmed.
- [ ] DR snapshot verified.
- [ ] Production read-only preflight clean.
- [ ] Evidence plan clean.

Apply:

- [ ] Six migrations applied in exact order and verified.
- [ ] Evidence backfill applied and verified, or intentionally skipped with no-go/hold decision.
- [ ] Production code synced to approved commit.
- [ ] Backend `8001` restarted only after explicit authorization.
- [ ] Daemon enabled/restarted only after explicit authorization.

Cutover:

- [ ] Cold-start sanity passed.
- [ ] 09:29 final go/no-go recorded.
- [ ] 09:30 A-share open monitoring active.
- [ ] Rollback owner ready until at least 09:45.

Completion:

- [ ] Evidence package archived with secrets redacted.
- [ ] Production `8001` touched/restarted status recorded.
- [ ] Production DB write/DDL status recorded.
- [ ] Any residual risks and follow-ups assigned.
