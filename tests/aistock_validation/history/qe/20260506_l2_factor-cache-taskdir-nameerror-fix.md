# Factor Cache TASKS_DIR NameError Fix Validation - 2026-05-06

## Scope

- Module: QE factor backtest cache backfill.
- Bug: `scripts/backfill_factor_cache.py` crashed while creating ST PIT eligible-index parquet with `NameError: name 'TASK_DIR' is not defined`.
- Fix: use the existing `TASKS_DIR` constant for `{task_id}.eligible_index.parquet`.

## Risk

- False success risk: the script may import/compile but still reference the old undefined name in the PIT eligible-index branch.
- Business impact: factor cache backfill cannot proceed for ST PIT reindexed cache windows.

## Validation

Commands run from `F:\Dev\AIstock_worktrees\factor-cache-taskdir-20260506`:

```powershell
python -m pytest tests/test_backfill_factor_cache_task_dir.py -q
python -c "from pathlib import Path; p=Path('scripts/backfill_factor_cache.py'); compile(p.read_text(encoding='utf-8'), str(p), 'exec'); print('compile_ok')"
rg -n "\\bTASK_DIR\\b" scripts/backfill_factor_cache.py
python -c "import scripts.backfill_factor_cache as b; print('import_ok', b.TASKS_DIR.name)"
```

## Results

- `tests/test_backfill_factor_cache_task_dir.py`: 1 passed.
- Script compile smoke: `compile_ok`.
- Undefined-name scan: no `TASK_DIR` references remain in `scripts/backfill_factor_cache.py`.
- Import smoke: `import_ok _tasks`.

## Production Impact

- No production backend restart performed.
- No cache files or database rows were deleted or modified during validation.
