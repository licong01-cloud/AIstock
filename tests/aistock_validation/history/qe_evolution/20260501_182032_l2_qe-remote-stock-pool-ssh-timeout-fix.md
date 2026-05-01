# L2 QE Remote Stock Pool SSH Timeout Fix - 2026-05-01

## Scope

- Fix custom_evo distributed task creation failure during remote stock_pool sync.
- Reported failure: `Command '['ssh', 'lc999@192.168.50.215', 'mkdir', '-p', '/home/lc999/data/qlib_bin/instruments']' timed out after 10 seconds`.
- No real QE training/backtest was executed.

## Root Cause

- Remote stock_pool sync used raw `ssh` for `mkdir`/checksum without explicit non-interactive SSH options.
- When custom_evo parallel tasks contain many loops using the same filtered pool on the same node, the API synchronously repeated the same SSH mkdir/scp/checksum sequence for every loop before task creation.
- A timeout raised by `subprocess.run` surfaced as a generic Python command timeout, without the stock_pool sync context or the exact actionable phase.

## Fix

- Added shared SSH options for all remote stock_pool SSH calls:
  - `BatchMode=yes`
  - `ConnectTimeout=10`
  - `ConnectionAttempts=1`
  - `StrictHostKeyChecking=accept-new`
  - `NumberOfPasswordPrompts=0`
  - `ServerAliveInterval=5`
  - `ServerAliveCountMax=2`
- Applied those options to SSH mkdir, SSH checksum, and WSL-side SCP calls.
- Kept connection timeout fail-fast at 10 seconds, but raised the total SSH command watchdog to 30 seconds so a valid key-based remote login is not killed before OpenSSH can return its own actionable error.
- Wrapped `subprocess.TimeoutExpired` in a `RuntimeError` that includes the sync phase, timeout, and command.
- Deduplicated custom_evo stock_pool sync per `(node_id, stock_pool)` during request preparation. Same node + same filtered pool is synced once, while different nodes still sync independently.

## No Silent Fallback Confirmation

- Duplicate sync skipping happens only after one successful sync for the same `(node_id, stock_pool)` in the same request.
- If the first sync fails, task creation still fails fast.
- No remote sync failure is ignored.
- Local-node stock_pool handling still verifies the authoritative WSL file exists and checksums it.

## Commands Run

```powershell
python -m py_compile backend/services/quantevolver/stock_pool_sync.py backend/routers/quantevolver_evolution.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
ssh -o BatchMode=yes -o ConnectTimeout=5 -o ConnectionAttempts=1 -o StrictHostKeyChecking=accept-new -o NumberOfPasswordPrompts=0 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 lc999@192.168.50.215 "mkdir -p -- /home/lc999/data/qlib_bin/instruments && test -d /home/lc999/data/qlib_bin/instruments && echo remote_mkdir_ok"
$env:AISTOCK_WSL_DISTRO='Ubuntu'; $env:QLIB_DATA_PATH_WSL='/home/lc999/data/qlib_bin'; @'
from backend.services.quantevolver.stock_pool_sync import sync_stock_pool_to_remote_node
result = sync_stock_pool_to_remote_node(
    'filtered_pool_20260501',
    {
        'node_id': 'rdagent-node1',
        'api_base_url': 'http://192.168.50.215:9000',
        'qlib_data_path': '/home/lc999/data/qlib_bin',
        'ssh_user': 'lc999',
    },
)
print(result)
'@ | python -
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_node_execution.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
git diff --check -- backend/services/quantevolver/stock_pool_sync.py backend/routers/quantevolver_evolution.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py
rg -n "fallback|except Exception|pass|return None|skipped|continue|ignore|ignored|default|TimeoutExpired|subprocess.run|silent|try:" backend/services/quantevolver/stock_pool_sync.py backend/routers/quantevolver_evolution.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py
```

## Results

- Targeted stock_pool/custom_evo tests: 30 passed.
- Wider QE targeted suite: 40 passed.
- Remote SSH mkdir smoke: `remote_mkdir_ok`.
- Remote stock_pool sync smoke: `filtered_pool_20260501` synced to `rdagent-node1` with sha256 `3b4ad5e17e49166df13840f453619a87cafaa70dda129daafee8adf0fca4e1b5`.
- `py_compile`: passed.
- `git diff --check`: passed; only existing line-ending normalization warnings were reported.
- Silent-fallback scan: reviewed matches in modified files; the only new `continue` is per-request stock_pool sync dedupe after a successful sync key, and remote sync failures still raise.

## Regression Coverage Added

- Remote stock_pool sync SSH/SCP commands include non-interactive SSH options.
- Timeout errors include actionable stock_pool phase context.
- Custom_evo preparation syncs each filtered pool once per node even if multiple parallel loops use it.

## Residual Risk

- If the remote SSH service or key authentication is unavailable, task creation will fail fast with a clearer message. This is intentional because running with an unsynced stock_pool would produce incorrect backtests.
