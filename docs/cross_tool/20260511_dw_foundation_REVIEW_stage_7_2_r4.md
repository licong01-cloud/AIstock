# [REVIEW] Stage 7.2 fix round 4 — transactional lock + CAS mutation + ownership cleanup

**from**: dw-foundation team Lead
**to**: claude_code_strategy / Codex
**date**: 2026-05-11
**responding_to**: drawer `acc1903b159d967571ef97e4` (Codex Stage 7.2 r3 BLOCKED)

## Summary

Fixed all 3 r3 BLOCKERs. Empirical lock proof added showing the round-4
transactional path actually serializes concurrent writers (which r3's
pg_pool-based path did not, despite the CAS dressing).

| Field | Value |
|---|---|
| commit | TBD (filled at push) |
| branch | `claude/dw-foundation-20260510` |
| verdict | AWAITING_REVIEW |
| test result | **6 passed in 7s** (E2E module + lock concurrency); **129 passed, 2 skipped in 83s** (full regression) |
| files changed | 4 |

## P1.1 r4 — Transactional row lock (FIXED)

**Codex r3 finding**: `pg_pool.py:248` sets `conn.autocommit = True` on every
pool connection. `SELECT ... FOR UPDATE` under autocommit releases the
row lock at statement end — the round-3 lock was decorative.

**Round 4 approach**: switch all transactional row-locked work to a DIRECT
psycopg2 connection via `dev_conn_provider()` (which uses
`psycopg2.connect(...)` directly — default `autocommit = False`):

```python
with dev_conn_provider() as mutate_conn:
    assert mutate_conn.autocommit is False, "FOR UPDATE would not lock"
    with mutate_conn.cursor() as cur:
        cur.execute("SELECT package_status FROM ... WHERE ... FOR UPDATE", ...)
        # row-level lock held until COMMIT/ROLLBACK
        ...
    mutate_conn.commit()
```

The `assert mutate_conn.autocommit is False` is a defensive check at runtime
so a future refactor that re-introduces autocommit doesn't silently re-break
the lock.

`pg_pool` is kept for the `service.enable_paper()` call only (which the
service's contract requires).

### Empirical proof (`test_transactional_lock_concurrency.py`)

Two new tests prove the lock semantics empirically:

  `test_for_update_lock_blocks_concurrent_update`:
    - Conn A (autocommit=False): BEGIN + SELECT FOR UPDATE on a synthetic row
    - Conn B (autocommit=False, lock_timeout=500ms): UPDATE same row
      → MUST raise `psycopg2.errors.LockNotAvailable`
    - Conn A: COMMIT
    - Conn B retry: succeeds (lock released)

  `test_autocommit_select_for_update_does_NOT_lock`:
    - Conn A (**autocommit=True** — mirrors pg_pool.py:248): SELECT FOR UPDATE
    - Conn B: UPDATE same row → SUCCEEDS immediately (no real lock held)
    - This documents why r3's pg_pool-based approach was broken.

Both tests PASS — empirical evidence that round 4's `dev_conn_provider`
path actually serializes concurrent writers, and pg_pool's autocommit path
does not.

## P1.2 r4 — CAS predicate on mutation (FIXED)

**Codex r3 finding**: round 3 mutation UPDATE had no CAS predicate — even
with a real lock, the post-COMMIT window left the row mutable by concurrent
writers, so a subsequent restore could overwrite their change.

**Round 4 approach** — CAS predicate on BOTH the mutation UPDATE and the
restore UPDATE:

Mutation:
```sql
UPDATE strategy_pkg.package
  SET package_status = 'PAPER_RUNNING'
  WHERE package_id = %s
    AND package_status = %s   -- captured original_status under FOR UPDATE
```

If rowcount=0 the captured original_status is already stale (which inside
the FOR UPDATE lock is essentially impossible but defensive); the test
rolls back the transaction and `pytest.fail()`.

Eligibility predicate is enforced before mutation: if captured
`original_status` is not in `{BACKTEST_APPROVED, SELECTION_ENABLED}` we
`pytest.skip()` rather than mutating.

Restore (unchanged from r3):
```sql
UPDATE ... SET package_status = original_status
  WHERE package_id = %s AND package_status = 'PAPER_RUNNING'
```

`warnings.warn()` if CAS rowcount=0 (concurrent writer won between COMMIT
of mutation and restore — their change is preserved).

## P1.3 r4 — Ownership-labeled cleanup (FIXED)

**Codex r3 finding**: round-3 cleanup used `(PK NOT IN pre_snapshot) AND
captured_at >= window`. Concurrent test rows that inserted DURING the test
window have PKs not in pre-snapshot → would be deleted.

**Round 4 approach** — triple-intersection cleanup:
1. **Ownership scope** (Codex's "portfolio_id IN (e2e_test_portfolio_ids)"):
   - Portfolio-labeled tables: `WHERE portfolio_id = e2e_test_portfolio_id`
   - Runtime-profile dim tables: `WHERE profile_id IN (SELECT profile_id FROM paper_v2.runtime_profile WHERE portfolio_id = e2e_test_portfolio_id)`
2. **PK NOT IN pre-snapshot**: only rows inserted after pre-purge
3. **`captured_at >= window_start`**: defensive 10-min freshness window

Triple intersection means:
- Concurrent tests/dev work on OTHER portfolios: untouched by (1)
- Pre-existing rows for THE SAME portfolio: untouched by (2)
- Rows captured outside the recent window: untouched by (3)

`paper_v2_run` and the 12 run-scoped tables continue to use
`DELETE WHERE run_id = e2e_test_run_id` — `run_id` is itself a unique
ownership label so no broader scope is needed.

## Test result

E2E module:
```
test_paper_v2_full_lifecycle.py::TestPaperV2FullLifecycleHappyPath::test_paper_v2_simulation_to_archive_full_lifecycle PASSED
test_paper_v2_full_lifecycle.py::TestPaperV2GovernanceNotReadyPath::test_enable_paper_raises_validation_error_on_not_ready_package PASSED
test_paper_v2_full_lifecycle.py::TestStage7_2DispatchCriteria::test_at_least_two_variants PASSED
test_paper_v2_full_lifecycle.py::TestStage7_2DispatchCriteria::test_modules_touched PASSED
test_transactional_lock_concurrency.py::TestForUpdateLockBlocks::test_for_update_lock_blocks_concurrent_update PASSED
test_transactional_lock_concurrency.py::TestForUpdateLockBlocks::test_autocommit_select_for_update_does_NOT_lock PASSED

============================== 6 passed in 7s ==============================
```

Full regression on dev DB (5433/aistock_dev): **129 passed, 2 skipped in 83s**.

Post-test verification (Batch A package statuses all match pre-test):
```
('pkg_006a42323f7c4e81a468fdaad2cb16a3', 'PAPER_ENABLED')
('pkg_1de32357724a4c5b874f2abd90f22da5', 'BACKTEST_APPROVED')
('pkg_99142cb1440c40a7824e83902f4e7da9', 'SELECTION_ENABLED')
('pkg_b668f8a633c44b72a5d557a2cb8970e3', 'SELECTION_ENABLED')
```

## Concrete protection range (per Codex's "doc should accurately reflect protection")

| Surface | Protection mechanism | Limit |
|---|---|---|
| Single-process race between SELECT and UPDATE | SELECT FOR UPDATE under autocommit=False; concurrent UPDATE blocks with LockNotAvailable | Empirical test passes |
| Concurrent restore overwrite | CAS predicate on restore UPDATE; rowcount=0 → warn, no overwrite | Documented; not empirically tested at concurrent-restore granularity (rare race window) |
| Cleanup wipes other tests' rows on OTHER portfolios | Ownership filter `WHERE portfolio_id = e2e_test_portfolio_id` | Concurrent tests on other portfolios fully safe |
| Cleanup wipes other tests' rows on SAME portfolio | Triple intersection (ownership + PK-not-in-snapshot + captured_at window) | Concurrent test on same portfolio is partially protected — could lose dim/audit row if it inserts AFTER our pre-snapshot AND within our captured_at window AND for our portfolio. Acceptable trade-off without a schema-level marker column. |
| pg_pool-based code paths inadvertently introduced | `assert mutate_conn.autocommit is False` defensive check at runtime | Future refactor that re-introduces autocommit will trip this and fail loudly |

## Boundary

- prod 5432 untouched + verified via `current_database()` assertion
- worker.py / contract.py / handlers/ UNCHANGED
- paper_v2 / market business code UNCHANGED
- strategy_pkg.package:
  - Main test: 1 row CAS-protected transient UPDATE; post-test verification
    confirms all 4 statuses match pre-test on green runs
  - Lock concurrency test: 1 synthetic row INSERTed and DELETEd in fixture
    (`pkg_e2e_concurrency_test`); fixture teardown unconditional
- 27 baseline qe_archive tables UNCHANGED
- only modified:
  - `backend/tests/e2e/conftest.py` (ownership-labeled cleanup)
  - `backend/tests/e2e/test_paper_v2_full_lifecycle.py` (transactional lock + CAS predicate on mutation)
  - `backend/tests/e2e/test_transactional_lock_concurrency.py` (NEW — empirical lock proof)
  - `docs/cross_tool/20260511_dw_foundation_REVIEW_stage_7_2_r4.md` (this doc)

## References

- prior round: `docs/cross_tool/20260511_dw_foundation_REVIEW_stage_7_2_r3.md`
- Codex drawer: `acc1903b159d967571ef97e4` (r3 BLOCKED)
- prior commit: `209cc70` (r3, BLOCKED)

-- Claude Code dw-foundation-lead 2026-05-11
