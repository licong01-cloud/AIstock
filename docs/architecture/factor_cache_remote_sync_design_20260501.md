# Factor Cache Remote Sync Design - 2026-05-01

## Goal

AIstock factor-value cache is an acceleration layer for QE factor preparation.  A
remote QE node must be able to use synchronized cache files when available, but
must still run by recomputing factors when a cache entry is missing or stale.

This design covers:

- automatic remote sync after local factor-cache computation succeeds;
- factor-library UI visibility for local WSL cache, remote-node cache, and
  per-factor remote sync status;
- a manual "补充同步" flow for factors that were not synchronized or failed
  during previous sync attempts;
- QE runtime behavior that treats cache miss as recompute, not as a task-blocking
  error.

## Current Baseline

Source-backed findings:

- Local authoritative backtest factor cache lives under
  `rdagent_assets/factor_values` with `_meta.json` plus `single/*.parquet`.
- `backend/routers/quantevolver.py` exposes local cache compute/status APIs and
  runs `scripts/backfill_factor_cache.py` in WSL.
- `backend/services/quantevolver/config_composer.py` already exports
  `FACTOR_CACHE_DIR` into generated QE commands.  If
  `infra.compute_nodes.factor_cache_dir` is configured, remote QE uses that
  directory; otherwise it falls back to the local WSL path, which is not valid on
  an independent Linux node.
- `infra.compute_nodes` already has `ssh_user` and `factor_cache_dir` columns.
  No schema migration is required for the first implementation.
- Generated `prepare_factors.py` returns `None` on cache miss and then computes
  the factor from `factor_data_dir`; therefore remote QE can continue without
  cache as long as factor computation itself succeeds.

## Terminology

- **Local cache**: the Windows/WSL authority at
  `rdagent_assets/factor_values`.
- **Remote cache**: a synchronized copy on a compute node, usually
  `/home/<ssh_user>/aistock_cache/factor_values`.
- **Cache hit**: parquet file exists, source hash matches, and the cached window
  covers the requested QE window, including recorded warm-up windows.
- **Cache miss**: no matching remote cache entry.  This is not a QE task failure;
  QE recomputes from data.
- **Sync failure**: rsync/ssh/remote meta update failed.  This should be visible
  in UI but must not make the local cache computation fail.

## Runtime Rules

1. Local cache remains the authority.
2. Remote caches are derived copies and may be rebuilt at any time.
3. `_meta.json` is updated on the remote only after parquet files have synced.
4. Remote cache missing/stale does not block QE:
   - generated QE scripts log cache miss;
   - factor code runs normally;
   - successful recompute writes into remote `FACTOR_CACHE_DIR`;
   - factor computation failure still fails QE.
5. Sync jobs must never silently change factor values.  They only copy existing
   local parquet files and their metadata.
6. The implementation must not use Windows UNC paths or hard-coded WSL distro
   paths.

## Remote Directory Resolution

For a node:

1. Use `infra.compute_nodes.factor_cache_dir` if set.
2. Otherwise derive `/home/<ssh_user>/aistock_cache/factor_values`.
3. During sync, if the DB field is empty and `configure_default_dir=true`, write
   the derived path into `infra.compute_nodes.factor_cache_dir` so future QE
   commands use the synchronized cache.
4. QE command generation also derives the same remote default when a remote node
   has no `factor_cache_dir` yet. This prevents remote jobs from falling back to
   a local WSL path; a cache miss still recomputes from `factor_data_dir` and
   writes the cache into the derived remote directory.

Only nodes with a non-local `api_base_url` host and a configured `ssh_user` are
treated as remote sync targets.

## Sync Algorithm

For each target node:

1. Load local `_meta.json`.
2. SSH into the remote node and read remote `_meta.json` if present.
3. Build a per-factor plan:
   - `sync`: local parquet exists and remote entry is missing/stale/hash-mismatch.
   - `skip`: remote entry and parquet already match local metadata.
   - `local_missing`: local meta exists but local parquet is absent.
4. Run rsync for selected parquet files:
   - source: local WSL path to `rdagent_assets/factor_values/single/`;
   - destination: `<ssh_user>@<host>:<remote_cache_dir>/single/`.
5. Merge remote meta with synced local entries.
6. Sync merged meta to `_meta.json.tmp`.
7. Atomically move `_meta.json.tmp` to `_meta.json` on the remote.
8. Write a local sync audit file under
   `rdagent_assets/factor_values/_remote_sync/`.

## Backend API

Add:

- `GET /api/v1/quantevolver/factor-cache/remote-stats`
  - query: `node_id?`
  - returns local stats, remote node stats, and per-factor remote status for the
    selected node.
- `POST /api/v1/quantevolver/factor-cache/sync-to-node`
  - body:
    - `node_id?`: sync one node, blank means all remote nodes;
    - `factor_names?`: sync selected factors, blank means all valid local cache;
    - `force`: sync even when remote metadata appears current;
    - `configure_default_dir`: write derived remote cache dir to DB when missing.

Extend local cache compute:

- after a successful `factor-cache/compute` background task, automatically run
  remote sync for the factors included in that task;
- when the compute request uses `force=true`, automatic sync also uses
  `force=true` so the remote file is refreshed even when metadata keys already
  look current;
- sync failure is recorded in task status but does not change successful local
  cache computation into a failed task.

## Frontend UI

On the factor-library cache panel:

- show WSL local cache count and size;
- show remote node selector;
- show remote cached/synced/missing/stale/failed counts;
- show last sync result;
- provide a "补充同步" button that triggers incremental sync for factors missing
  or stale on the selected remote node;
- add a per-factor "远端同步" column for the selected node.

## Verification Plan

1. Unit-level:
   - service can compare local/remote metadata;
   - missing remote meta produces a sync plan instead of an exception;
   - local missing parquet is reported and skipped.
2. Integration smoke:
   - configure `rdagent-node1.factor_cache_dir`;
   - compute or reuse three small factor caches;
   - sync exactly those three factors;
   - verify remote `_meta.json` has matching entries;
   - verify remote `single/*.parquet` files exist.
3. QE behavior:
   - generated `prepare_factors.py` must still return `None` on cache miss and
     recompute; no fail-fast on missing cache.

## Anti-Patterns

- Do not copy via `\\wsl$` or `\\wsl.localhost`.
- Do not sync `_meta.json` before parquet files.
- Do not make remote cache a hard prerequisite for QE task submission.
- Do not silently fabricate factor values or fill defaults.
- Do not use `--delete` in normal sync.
