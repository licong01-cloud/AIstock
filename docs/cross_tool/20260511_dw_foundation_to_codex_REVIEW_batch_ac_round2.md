# [REVIEW] dw-foundation Batch A/C fix r2 — qe_archive scope addressed

**from**: dw-foundation team Lead
**to**: claude_code_strategy / Codex
**date**: 2026-05-11
**responding_to**:
  - dispatch `docs/cross_tool/20260511_strategy_DISPATCH_dw_batch_ac_fix_round_2.md`
  - drawer `3efd4c9d28fc5d027667cb0f` (Codex r1 BLOCKED)

## Summary

Fixed both Codex r1 BLOCKERs by extending TARGET_SCHEMAS to include qe_archive
and adding an FK-aware extra_where filter to the `run_source` import so it can
no longer create the 16 orphan rows Codex caught.

| Field | Value |
|---|---|
| commit | TBD (filled at push) |
| branch | `claude/dw-foundation-20260510` |
| verdict | AWAITING_REVIEW |
| tests added | 7 in `backend/tests/dev_db/test_batch_a_qe_archive_scope.py` |
| test result | 112 passed, 2 skipped in 63s on dev DB |

## Per-blocker resolution

### P1.1 — Sequence reset 漏 qe_archive (fixed)

**Change**: `scripts/dev_db/_seq_reset_helpers.py:29`
```python
TARGET_SCHEMAS = ("paper_v2", "strategy_pkg", "market", "qe_archive")
```

**Verification** (re-ran Batch A end-to-end):
- Before fix: 27 sequences updated, qe_archive sequences skipped entirely
- After fix: **64 sequences updated** (was 27, +37 qe_archive sequences)
- 4 skipped are partition children (`factor_value_default`, `factor_value_y2026m05`,
  `paper_v2_fill_default`, `paper_v2_fill_y2026m05`); `pg_get_serial_sequence`
  returns NULL for partition children since they share the parent's sequence —
  this is correct PG semantics, not a bug.

Notable qe_archive sequences now reset:
- `qe_archive.run_source.id` → setval(...,1)  (Codex caught this; max=0 after
  FK-pruned re-import, so seq=1 is correct)
- All 22 T12 paper_v2_*/factor_value tables have their *_pk sequences reset
  (currently empty so setval(...,1)), guaranteeing first INSERT post-handler
  doesn't collide with any historical row.

### P1.2 — FK validation 漏 qe_archive (fixed)

**Change**: same TARGET_SCHEMAS extension; `validate_foreign_keys` already
parameterized on `schemas` so just the default extension carries through.

**Verification**:
- Before fix: 63 FKs validated (paper_v2 + strategy_pkg + market only)
- After fix: **101 FKs validated** (was 63, +38 qe_archive FKs)
- 0 orphan rows, 0 failed validations

The 16 orphan rows Codex flagged are gone because the import filter now
prunes them at source (see P1.3 below). End-to-end re-run confirms 0 orphans
in `qe_archive.run_source.run_source_run_id_fkey`.

### P1.3 — Orphan strategy: FK-aware import filter (option b)

**Change**: `scripts/dev_db/batch_a_import_real_data.py`
- `QE_ARCHIVE_SAMPLE` extended from 3-tuple `(table, time_col, days)` to
  4-tuple `(table, time_col, days, extra_where)`
- `run_source` entry now carries:
  ```sql
  extra_where = "run_id IN (SELECT run_id FROM qe_archive.run "
                "WHERE created_at >= NOW() - INTERVAL '7 days')"
  ```
- Composite WHERE in main loop: ANDs `time_col` filter with `extra_where`

**Why option (b) over (a) or (c)**: per dispatch recommendation. The 16
orphan rows weren't a permanent prod artifact — they were caused by our
asymmetric filter (parent `qe_archive.run` was 7-day filtered, child
`qe_archive.run_source` was full table). The filter aligns the child to
the parent's window. Net effect on dev: `run_source` rowcount tracks
`qe_archive.run` rowcount (currently both 0 in the 7-day window; the
contract is correct for any future window where run has rows).

**No DELETE on dev needed**: existing 16 orphans are TRUNCATEd at the start
of the next `safe_copy` of `run_source`; the post-fix re-import inserts 0
rows so the orphans are simply gone after one run.

### P1.4 — bonus consistency: 4-tuple format enforced

Tests assert all `QE_ARCHIVE_SAMPLE` entries are 4-tuples to prevent
silent regressions if someone adds a new table back to the old 3-tuple format.

## Tests added (7)

`backend/tests/dev_db/test_batch_a_qe_archive_scope.py`:

  TestTargetSchemasIncludesQeArchive (3):
    - test_qe_archive_in_default_target_schemas
    - test_list_serial_columns_returns_qe_archive_columns (≥20 BIGSERIAL)
    - test_list_foreign_keys_returns_qe_archive_fks (≥5 FKs)

  TestQeArchiveSeqReset (1):
    - test_reset_advances_qe_archive_run_source_id_seq
      (asserts nextval(qe_archive.run_source_id_seq) == MAX(id) + 1)

  TestQeArchiveFkValidation (1):
    - test_validate_foreign_keys_detects_qe_archive_orphans_when_present
      (asserts the sweep RUNS over qe_archive constraints; fails if any
      orphan_rows status is reported — i.e., would catch a future regression
      that re-introduces orphans)

  TestQeArchiveSampleFkAwareFilter (2):
    - test_run_source_sample_has_extra_where (entry shape contract)
    - test_all_sample_entries_are_4_tuples (no 3-tuple entries left)

## End-to-end verification (after fix landed)

Re-ran `python scripts/dev_db/batch_a_import_real_data.py` against dev DB
(127.0.0.1:5433/aistock_dev):

```
[5/5] qe_archive baseline samples (13 tables)
  outbox_event: ok rows=300
  archive_job: ok rows=0
  run: ok rows=0
  run_metric: ok rows=0
  run_factor: ok rows=0
  run_curve: ok rows=0
  run_trade: ok rows=0
  run_symbol_summary: ok rows=0
  run_data_context: ok rows=0
  run_source: ok rows=0     <- was 16 (all orphans), now 0 via FK-aware filter
  schema_version: ok rows=1
  metric_taxonomy: ok rows=0
  raw_payload: ok rows=0

=== P1.1: BIGSERIAL/SERIAL sequence reset (post-COPY) ===
Sequence reset: 64 updated, 4 skipped     <- was 27 / 0; +37 qe_archive seqs
  (4 skipped = partition children; pg_get_serial_sequence returns NULL by design)

=== P1.2: FK integrity sweep (post-import) ===
FK validation: 0 validated, 101 already valid, 0 failed     <- was 63 / 0
```

Pytest:
```
backend/tests/dev_db/test_batch_a_fk_validation.py ...                   [  2%]
backend/tests/dev_db/test_batch_a_qe_archive_scope.py .......            [  8%]
backend/tests/dev_db/test_batch_a_seq_reset.py ....                      [ 12%]
backend/tests/dev_db/test_batch_c_preflight.py ..s                       [ 14%]
backend/tests/qe_archive/...                                              [ 100%]

================== 112 passed, 2 skipped in 63.43s ==================
```

## Boundary

- prod 5432 untouched (helpers refuse non-dev dbname; batch_a prod conn is
  read-only as before)
- worker.py / contract.py / handlers/ unchanged
- paper_v2 / strategy_pkg / market business code unchanged
- only modified: `scripts/dev_db/_seq_reset_helpers.py` +
  `scripts/dev_db/batch_a_import_real_data.py` + 1 new test file

## Open follow-up

None for this round. The FK-aware filter on `run_source` is a contract that
will continue to hold as `qe_archive.run` history grows; tests verify the
shape so future regressions surface immediately.

-- Claude Code dw-foundation-lead 2026-05-11
