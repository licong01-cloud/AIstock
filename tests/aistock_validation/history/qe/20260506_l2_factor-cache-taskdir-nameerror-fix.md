# Factor Cache TASKS_DIR / Loader-Style Code Fix Validation - 2026-05-06

## Scope

- Module: QE factor backtest cache backfill.
- Bug 1: `scripts/backfill_factor_cache.py` crashed while creating ST PIT eligible-index parquet with `NameError: name 'TASK_DIR' is not defined`.
- Bug 2: factors written as `calculate_<factor>(instruments, start_date, end_date)` against `_REALTIME_LOADER` did not write `result.h5`, so the backtest cache wrapper failed before caching values.
- Fixes:
  - Use existing `TASKS_DIR` for `{task_id}.eligible_index.parquet`.
  - Add a backtest-H5 compatibility loader inside the subprocess wrapper and call `calculate_<factor>` when the factor code does not create `result.h5` by itself.

## Risk

- False success risk: script may compile but still reference the old undefined name in the PIT eligible-index branch.
- False success risk: loader-style Alpha158 factors may still fail because `_REALTIME_LOADER` is undefined or no `result.h5` is produced.
- Business impact: factor cache backfill cannot proceed for ST PIT reindexed cache windows or loader-style factors used by the factor library.

## Validation

Commands run from `F:\Dev\AIstock_worktrees\factor-cache-taskdir-20260506` and final root `F:\Dev\AIstock`:

```powershell
python -m pytest tests/test_backfill_factor_cache_task_dir.py -q
python -c "from pathlib import Path; p=Path('scripts/backfill_factor_cache.py'); compile(p.read_text(encoding='utf-8'), str(p), 'exec'); print('compile_ok')"
rg -n "\\bTASK_DIR\\b" scripts/backfill_factor_cache.py
python -c "import scripts.backfill_factor_cache as b; print('import_ok', b.TASKS_DIR.name)"
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock && python - <<'PY'
from pathlib import Path
p=Path('scripts/backfill_factor_cache.py')
source=p.read_text(encoding='utf-8')
compile(source, str(p), 'exec')
print('wsl_compile_ok')
print('TASK_DIR_present', 'TASK_DIR' in source)
import scripts.backfill_factor_cache as b
print('wsl_import_ok', b.TASKS_DIR.name)
PY"
```

## Results

- `tests/test_backfill_factor_cache_task_dir.py`: 2 passed.
- The regression test covers both the `TASKS_DIR` constant and a minimal `_REALTIME_LOADER`/`calculate_<factor>(instruments,start,end)` factor executed through `_execute_factor_subprocess`.
- Script compile smoke: `compile_ok`.
- Undefined-name scan: no `TASK_DIR` references remain in `scripts/backfill_factor_cache.py`.
- Import smoke: `import_ok _tasks`.
- WSL compile/import smoke after merge to root: `wsl_compile_ok`, `TASK_DIR_present False`, `wsl_import_ok _tasks`.

## Production Impact

- No production backend restart performed.
- A new factor-cache task was started through production API after the first fix. It passed the original `TASK_DIR` crash point and began writing task state; later loader-style failures motivated the compatibility-loader fix.
- No H5/Bin/Qlib production dataset files were modified by this validation.
