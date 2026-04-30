# L1 Validation: Factor Cache WSL DB Env Bridge

Date: 2026-04-30
Module: QE / RD-Agent / factor cache
Scope: Backend helper and WSL subprocess environment bridge for `/api/v1/quantevolver/factor-cache/compute`.

## Business Goal

Factor cache backfill tasks launched from AIstock must read DB settings after service restart without manual shell environment setup. The WSL child process must receive `TDX_DB_*` and PostgreSQL alias vars from the AIstock process environment loaded from `.env`, while secrets are not embedded in the command line or committed.

## False-Success Risks

- API queues a task but WSL immediately fails because `TDX_DB_PASSWORD` is absent.
- Password exists in AIstock `.env` but is not propagated across the Windows-to-WSL boundary.
- A secret is written into code, tests, logs, or Git history.
- The task returns no actionable log when preflight fails before Python starts.

## Commands Run

```powershell
python -m pytest backend/tests/test_factor_cache_wsl_env.py -q -p no:cacheprovider
python -m py_compile backend\routers\quantevolver.py
git diff --check -- backend/routers/quantevolver.py backend/tests/test_factor_cache_wsl_env.py
python %TEMP%\test_wslenv_bridge.py
rg -n "<local-secret-patterns>|TDX_DB_PASSWORD=|PGPASSWORD=" backend\routers\quantevolver.py backend\tests\test_factor_cache_wsl_env.py
```

## Results

- Targeted pytest: 4 passed.
- Python compile: passed.
- Diff whitespace check: passed.
- WSLENV bridge smoke with dummy values: returncode 0.
- Secret scan: no real password pattern found; one expected test assertion checks that `TDX_DB_PASSWORD=` is not embedded in the command.

## Asset Safety

No StrategyPackage manifests, model weights, HMM snapshots, validated execution policies, QE/RD-Agent workspaces, factor cache parquet files, or database rows were modified by this validation.

## Production Impact

Production backend port 8001 was not restarted. The code change requires an AIstock backend restart to take effect. RD-Agent `.env` was updated locally with DB keys but remains ignored/untracked and was not staged.
