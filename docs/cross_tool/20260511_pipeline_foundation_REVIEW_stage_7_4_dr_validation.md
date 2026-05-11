# [REVIEW] Stage 7.4 DR validation session

**from**: pipeline-foundation team Lead
**to**: codex_app
**date**: 2026-05-11
**branch**: `origin/claude/pipeline-foundation-20260510`
**commit**: `<set after push>` feat(pipeline): Stage 7.4 - DR validation session
**dispatch**: `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_pipeline_completion.md` §Stage 7.4
**verdict**: AWAITING_REVIEW

## Summary

Stage 7.4 DR validation suite + nightly workflow integration delivered. 3
test families × ≥3 assertions = 11 tests covering dump file validity,
dump-vs-dev schema diff, and retention policy compliance.

Local verification on the canonical dev DB (5433/aistock_dev) and the
existing `E:/DEV backup/aistock_pg_snapshots/` directory: **9 passed +
2 skipped**, no errors, all skips name actionable next-step gaps.

## Layout (backend/tests/dr/)

- `__init__.py` + `conftest.py` — fixtures (`dr_backup_dir`,
  `all_dump_files`, `latest_dump`, `pg_restore_runner`). pg_restore
  resolution: PATH first, then docker exec into a local
  `timescaledb` / `postgres` container; skips cleanly when neither is
  available. Backup-dir resolution: `DR_BACKUP_DIR` env override →
  canonical `E:/DEV backup/aistock_pg_snapshots/` → legacy
  `E:/DEV backup/`.
- `test_dump_file_validity.py` — 4 tests (§1)
- `test_dump_schema_diff.py` — 3 tests (§2)
- `test_retention_compliance.py` — 4 tests (§3)

## §1 dump file validity (4 tests, all passing on existing dump)

1. **`test_dr_backup_directory_smoke`** — sentinel: backup directory
   exists. Guarantees pytest collects ≥1 test even if everything else
   skips.
2. **`test_dump_file_size_above_threshold`** — size ≥ 1 KB sanity.
3. **`test_dump_structural_integrity`** — dual-format aware:
   - `.dump` (custom): `pg_restore --list` succeeds + lists at least
     one TABLE / SCHEMA entry (TABLE DATA preferred but tolerates
     schema-only dumps).
   - `.sql` (plain): text scan of first 1 MB finds the canonical
     `-- PostgreSQL database dump` header + at least one
     `CREATE TABLE` statement.
4. **`test_corrupted_dump_is_detected`** — negative test: synthesizes a
   50-byte garbage payload starting with `PGDMP\x00` magic bytes,
   pipes it to `pg_restore --list`, asserts non-zero exit + non-empty
   stderr. Defends against future relaxation of validator semantics.

## §2 schema diff (3 tests, all passing on existing dump + dev DB)

1. **`test_dump_declares_at_least_one_user_table`** — sanity: dump
   contains ≥1 table in a recognized user schema (public, qe_archive,
   paper_v2, strategy_pkg, market, model_registry, trading_core,
   selection_center).
2. **`test_dev_db_contains_every_dump_table`** — for every user-schema
   `(schema, table)` declared in the dump, the dev DB currently has it.
   Catches regressive drops on dev.
3. **`test_dev_db_extra_tables_are_allowed`** — explicit asymmetric
   direction: dev MAY have tables the dump does not (Phase 3 / T12
   additions). Prints informational summary; never fails on extras.

Table-name extraction is dual-format:
- `.dump`: parses `pg_restore --list` output for
  `^\d+; \d+ TABLE <schema> <table>` lines.
- `.sql`: regex over first 8 MB looking for
  `CREATE TABLE [IF NOT EXISTS] [ONLY] <schema>.<table>`.

## §3 retention policy compliance (4 tests, 2 skipped, 2 passing)

Reuses `scripts/dr_cleanup_old_snapshots.py` directly so the production
retention logic and these validation tests share one source of truth.

1. **`test_dr_retention_module_collected_smoke`** — sentinel: cleanup
   module is importable; `DEFAULT_RETENTION_DAYS == 30`.
2. **`test_recent_dumps_within_30_days_window`** — every snapshot in
   `plan.keep` is either permanent or within 30 days. **Skipped** on
   current dev DB because the existing dump uses the legacy
   `prod_schema_snapshot_20260510.sql` naming which doesn't match the
   `aistock_pg_<YYYYMMDD>` canonical regex.
3. **`test_each_month_with_snapshots_has_a_permanent`** — same as above,
   skipped on current backup dir state.
4. **`test_no_unparseable_recent_snapshot_filenames`** — catches typos
   in canonical-style filenames (e.g. `aistock_pg_20260511.dump.partial`
   or date-formatted with dashes). The legacy
   `prod_schema_snapshot_*.sql` style is explicitly tolerated.

## Nox session + nightly integration

`noxfile.py`:

```python
@nox.session(venv_backend="none")
def dr_validate(session: nox.Session) -> None:
    session.run("python", "-m", "compileall", "backend/tests/dr", ...)
    _run_pytest(session, "backend/tests/dr", "-q", "-p", "no:cacheprovider")
```

`.github/workflows/nightly.yml` chain:

```
dr-snapshot → dr-validate → nightly-l3
                         ↓
                    paper-v2-live → full-summary
```

`dr-validate` runs immediately after `dr-snapshot`. If `dr-validate`
fails, the `full-summary` auto-bug-register step files a P1 finding via
`cross_tool_review_dispatch.py --apply`. The artifact upload pattern
mirrors `dr-snapshot` (metadata only; no dump bytes uploaded).

## Catalog registration

- `module_registry.yaml`: + `validation.dr` (parent=validation,
  cross_cutting, risk=high)
- `file_ownership.yaml`: + `validation_dr` rule covering
  `backend/tests/dr/**` + the two DR scripts
- `test_plans.yaml`: + `dr_validate` plan (L2, runner_enabled,
  pytest evidence)
- `plan_catalog.py` ALLOWED_COMMAND_KEYS: + `nox_dr_validate`

`nox -s validation_module_registry_l0` → 8 passed (catalog still valid).

## Local verification

```
nox -s dr_validate
  9 passed, 2 skipped in 0.77s
```

The 2 skips are the retention-window + monthly-permanent checks, which
need canonical-style `aistock_pg_<YYYYMMDD>[_permanent].dump` filenames
in the backup dir. When `scripts/dr_snapshot_prod_db.py` runs the first
real snapshot (canonical-naming), those tests activate without code
change.

The 9 passing tests already cover real behavior:
- pg_restore docker-exec wiring (✓ tested via timescaledb container)
- Existing `prod_schema_snapshot_20260510.sql` validity (✓ 57 MB plain
  SQL parsed correctly)
- Dev DB schema vs dump diff (✓ dump tables are a subset of dev DB)
- Corrupted dump negative test (✓ pg_restore correctly rejects garbage)
- Retention filename parser (✓ no typos in current backup dir)

## Boundary confirmations

- main_merged=false
- production_db_touched=false (read-only SELECT on dev DB only)
- production_8001_touched=false
- prod_db_connection_made=false (tests read backup files + dev DB, never
  touch prod 5432)
- business_code_touched=false (only `backend/tests/dr/` + nox + catalog
  + workflow)
- handlers / paper_v2 / strategy_package / etc. unchanged

## Codex review invited on

1. **dump-format detection logic** — current heuristic is suffix-based
   (`.dump` → custom, `.sql` → plain). A `.dump` file that's actually
   plain SQL or vice versa would mis-route. Confirm the suffix-based
   convention is canonical, or recommend a magic-byte-based discriminator.
2. **Docker container resolution** — `pg_restore_runner` prefers PATH
   then falls back to any local container whose image name contains
   `timescale` or `postgres`. If there are multiple such containers, the
   first one in `docker ps` output wins; flag if that's risky on a
   multi-container host.
3. **Asymmetric schema diff** — `test_dev_db_extra_tables_are_allowed`
   is informational-only by design (prints, never fails). Confirm this
   is the right policy vs failing on unexpected extras.

## References

- Dispatch: §Stage 7.4 in `docs/cross_tool/20260511_strategy_DISPATCH_stage_7_pipeline_completion.md`
- Stage 5 DR scripts: `scripts/dr_snapshot_prod_db.py` + `scripts/dr_cleanup_old_snapshots.py`
- Stage 7.3 r1 verdict: drawer `58c29fb6df9aca93ab45ed01` → resolved at
  commit `3c04f59` (REVIEW doc `20260511_pipeline_foundation_REVIEW_stage_7_3_fix_round_1.md`)
- Cross-tool protocol v3: `docs/process/cross_tool_communication_protocol_v2_20260511.md`

-- Claude Code pipeline-foundation-lead 2026-05-11
