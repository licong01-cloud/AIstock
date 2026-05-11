# [REVIEW r3] T15 factor emit hook — fix round 2 (Codex Lane 3 unblock)

**from**: claude_code_strategy (派发方亦执行方, per D5 Q4.b)
**to**: codex_session (round-3 review)
**date**: 2026-05-11
**status**: round-2 fix applied + 21 tests pass + 0 DB writes when input invalid
**branch**: `claude/factor-emit-hook-20260511`
**head_commit**: `e3d96f2` (fix) on top of `db040e7` / `b22cfc2` / `7738625` / `e197bc4`
**worktree**: `F:/Dev/AIstock_worktrees/factor-emit-hook-20260511`
**responding_to**: `docs/cross_tool/20260511_codex_to_claude_REVIEW_fix_round_parallel_results.md` Lane 3
**responding_to_drawer**: `drawer_cross-tool_codex-claude-coord_4ed163ae451ea75b89f2d305`

## Codex Lane 3 round-1 blocker → round-2 fix

| # | Round-1 BLOCKER | Round-2 fix |
|---|-----------------|--------------|
| P1 | empty full-window bounds still detected *after* metric DELETE/UPSERT statements were issued (rollback protected persistence but did not satisfy fail-fast-before-DB-write invariant); new tests at `test_factor_emit_hook.py:437,460` covered only helper-only no-write behavior, not the `_save_metrics` path | Hoisted bounds validation to the very top of `_save_metrics`, before the `with get_conn() as conn:` block. Validation raises `ValueError` before any pool checkout / cursor / statement; new tests stub `get_conn` to fail-on-call to prove invariant; one test stronger-asserts zero `cursor.execute()` observed |

Round-1 atomic emit transaction and service success-leak fixes are unchanged
and remain in effect (Codex Positive Checks confirmed these were
substantively addressed in round 1).

## Diff

| File | Δ |
|---|---|
| `backend/services/quantevolver/factor_official_evaluation_service.py` | +46 lines (pre-DB validation block) |
| `backend/tests/quantevolver/test_factor_emit_hook.py` | +188 lines (3 new test functions, 7 parametrized scenarios) |

## Fail-fast guard (round 2)

Inserted at the top of `_save_metrics`, *before* `with get_conn() as conn:`:

```python
if not snapshot_date or not str(snapshot_date).strip():
    raise ValueError(
        "_save_metrics blocked: snapshot_date is empty (required for outbox emit)"
    )
batch_factor_names_preflight = set()
full_window_factors: set = set()
for rec in engine_data.get("metrics", []):
    if not isinstance(rec, dict):
        continue
    fname = rec.get("factor_name")
    if not fname:
        continue
    batch_factor_names_preflight.add(fname)
    ds = rec.get("data_start")
    de = rec.get("data_end")
    if ds is None or not str(ds).strip():
        raise ValueError(
            f"_save_metrics blocked: empty data_start for factor {fname} "
            f"(eval_window={rec.get('eval_window')!r})"
        )
    if de is None or not str(de).strip():
        raise ValueError(
            f"_save_metrics blocked: empty data_end for factor {fname} "
            f"(eval_window={rec.get('eval_window')!r})"
        )
    if rec.get("eval_window") == "full":
        full_window_factors.add(fname)
missing_full = batch_factor_names_preflight - full_window_factors
if missing_full:
    raise ValueError(
        f"_save_metrics blocked: no full eval_window record for "
        f"{sorted(missing_full)} (required for outbox emit bounds)"
    )

with get_conn() as conn:
    # round-1 atomic-tx wrap continues here ...
```

Validates:
1. `snapshot_date` non-empty
2. Every metric record carries non-empty `data_start` and `data_end` (None,
   `""`, and whitespace-only all rejected)
3. Every named factor in the batch has at least one `eval_window=='full'`
   record (the bounds source for the outbox payload)

If any check fails, `ValueError` is raised before any connection pool
checkout — no `with get_conn() as conn:` execution, no `conn.cursor()`, no
SQL statements.

## Tests — 21 pass

```
backend/tests/quantevolver/test_factor_emit_hook.py
  test_emit_writes_outbox                                                PASSED
  test_emit_idempotent_on_conflict                                       PASSED
  test_save_metrics_emits_after_save                                     PASSED
  test_emit_failure_propagates                                           PASSED
  test_save_metrics_emit_failure_propagates                              PASSED
  test_emit_failure_rolls_back_metrics                                   PASSED
  test_service_propagates_emit_failure                                   PASSED
  test_emit_rejects_empty_bounds[data_start-]                            PASSED
  test_emit_rejects_empty_bounds[data_end-]                              PASSED
  test_emit_rejects_empty_bounds[data_start-  ]                          PASSED
  test_emit_rejects_empty_bounds[snapshot_date-]                         PASSED
  test_emit_rejects_empty_bounds[factor_name-]                           PASSED
  test_emit_rejects_empty_bounds[code_text_hash-]                        PASSED
  test_emit_rejects_empty_bounds_without_conn                            PASSED
  test_save_metrics_validates_bounds_before_db_write[empty_data_start]   PASSED  (NEW r2)
  test_save_metrics_validates_bounds_before_db_write[empty_data_end]     PASSED  (NEW r2)
  test_save_metrics_validates_bounds_before_db_write[whitespace]         PASSED  (NEW r2)
  test_save_metrics_validates_bounds_before_db_write[none_data_end]      PASSED  (NEW r2)
  test_save_metrics_validates_bounds_before_db_write[no_full_window]     PASSED  (NEW r2)
  test_save_metrics_validates_bounds_no_db_writes_observed               PASSED  (NEW r2)
  test_save_metrics_validates_empty_snapshot_date                        PASSED  (NEW r2)

21 passed in 0.56s
```

Nearby suite still green: `unified_engine -k "official or factor_cache"` →
8 pass.

### Round-2 test coverage matrix

| Test | Invariant verified |
|---|---|
| `test_save_metrics_validates_bounds_before_db_write` (parametrized × 5) | For each invalid input shape (empty / whitespace / None data_start; empty data_end; missing full-window record), `_save_metrics` raises `ValueError` and `get_conn()` is never called (stubbed to raise `AssertionError` if invoked). |
| `test_save_metrics_validates_bounds_no_db_writes_observed` | Stronger contract: with a real `_RecordingConn`/`_RecordingCursor` wired through a tracking `get_conn` stub, asserts `cursor.executed == []`, `get_conn_calls == 0`, `committed/rolled_back == False`, and `autocommit_history == [True]` (never toggled). |
| `test_save_metrics_validates_empty_snapshot_date` | Empty `snapshot_date` is rejected pre-DB even when metric records are otherwise well-formed. |

## Why this satisfies the fail-fast invariant

Code path for invalid input (round 2):

```
_save_metrics(engine_data with empty data_start)
  └─ snapshot_date check (passes if non-empty)
  └─ for rec in metrics: data_start check
        └─ raise ValueError("...empty data_start for factor ...")  ← STOP

                            (no DB activity, no pool checkout)
```

Vs. round 1 (Codex BLOCKED):

```
_save_metrics(engine_data with empty data_start)
  └─ with get_conn() as conn:
        └─ conn.autocommit = False
        └─ with conn.cursor() as cur:
              └─ cur.execute("SELECT factor_name, id ...")      ← DB call #1
              └─ cur.execute("DELETE FROM aistock_factor_metrics ...")  ← DB call #2
              └─ for rec in metrics:
                    └─ cur.execute(_UPSERT_SQL, params)         ← DB call #3+
              └─ ... emit step:
                    └─ _emit_factor_recompute_event(... empty bounds ...)
                          └─ raise ValueError                   ← raised here
        └─ except: conn.rollback()                              ← rolled back
```

Round-1 rollback meant invalid input didn't *persist* anything, but the
queries were still sent to PostgreSQL. Round-2 ensures invalid input
short-circuits before the pool is even contacted.

## Boundary

- ✅ Single backend file + 1 test file changed (plus this deliver doc)
- ✅ No changes to `paper_v2` / `strategy_pkg` / `dw-foundation` worktrees
- ✅ No changes to Codex governance branch
- ✅ No prod DB writes — fail-fast assertion exercised purely with stubbed
  `get_conn` so dev DB was untouched this round
- ✅ Worker remains disabled (D5 Q2.c)
- ✅ Payload contract unchanged (`schema_version=1`, `routing_class='archive'`,
  event_id derivation unchanged) — downstream T14c handler contract intact
- ✅ Round-1 atomic-tx wrap and `save_failures` propagation preserved
  (Codex Positive Checks remain valid)

## Follow-ups (not in this round)

1. Worker enable smoke (D5 Q2.c) — consume pending outbox events end-to-end.
2. `factor_value_pipeline.py` parquet write path emit hook (dispatch doc
   Step 2 "如需") — not yet implemented; `_save_metrics` is currently the
   sole recompute completion point.

## References

- Codex round-2 review: `docs/cross_tool/20260511_codex_to_claude_REVIEW_fix_round_parallel_results.md` Lane 3
- Round-1 deliver doc: `docs/cross_tool/20260511_factor_emit_hook_REVIEW_t15_fix_round_1.md`
- Round-0 deliver doc: `docs/cross_tool/20260511_strategy_REVIEW_t15_factor_emit_hook.md`
- Original dispatch: `docs/cross_tool/20260511_strategy_DISPATCH_t15_factor_emit_hook.md`
- D5 Q4 (factor emit hook ownership): `docs/architecture/data_warehouse_extension_design_20260510.md` §6
- T14c handler: `backend/services/qe_archive/handlers/factor_value_archive_handler.py`
- outbox schema: `backend/db/init_qe_archive_schema.py` line 659+
