# L2 QE Factor Cache Streaming Sync Validation - 2026-05-06

## Scope

- AIstock factor-cache remote sync no longer uses multipart `requests.post(..., files=...)` for parquet uploads.
- RD-Agent node API accepts raw streaming `PUT /api/v1/qe_workspace/factor-cache/factors/{factor_name}/file` and separate `POST /api/v1/qe_workspace/factor-cache/meta`.
- Existing legacy `POST /sync` remains compatible, but server-side file writes now copy upload file objects in chunks instead of `await upload.read()`.
- Production FastAPI `8001` was not restarted. It was queried read-only once for cache status.

## Changed Runtime Surfaces

- AIstock backend service: `backend/services/quantevolver/factor_cache_remote_sync_service.py`
- AIstock backend API request model: `backend/routers/quantevolver.py`
- AIstock regression tests: `backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py`
- RD-Agent node API: `rdagent/app/api_endpoints/factor_cache_api.py`

## Commands And Results

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/quantevolver/factor_cache_remote_sync_service.py backend/routers/quantevolver.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py
# PASS
```

```powershell
wsl bash -lc "cd /mnt/f/Dev/RD-Agent-main && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -m py_compile rdagent/app/api_endpoints/factor_cache_api.py"
# PASS
```

```powershell
ssh lc999@192.168.50.215 'cd /home/lc999/projects/RD-Agent-main && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -m py_compile rdagent/app/api_endpoints/factor_cache_api.py'
# PASS
```

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py -q
# 7 passed in 0.82s
```

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
# 19 passed in 25.80s
```

```powershell
rg -n "requests\.post\([^\n]*files=|files=files|await upload\.read\(|subprocess|\brsync\b|\bssh\b|\bwsl\b|_run_wsl|_run_rsync" backend/services/quantevolver/factor_cache_remote_sync_service.py F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/factor_cache_api.py -S
# PASS for dangerous upload/path patterns; only a benign RD-Agent comment mentions the local WSL default cache path.
```

## Node API Evidence

Local WSL node after restart:

```text
GET http://127.0.0.1:9000/health -> {"status":"ok"}
OpenAPI factor-cache paths include:
/api/v1/qe_workspace/factor-cache/meta
/api/v1/qe_workspace/factor-cache/factors/{factor_name}/status
/api/v1/qe_workspace/factor-cache/factors/{factor_name}/file
/api/v1/qe_workspace/factor-cache/sync
```

Remote node after deploy/restart:

```text
GET http://192.168.50.215:9000/health -> {"status":"ok"}
OpenAPI factor-cache paths include:
/api/v1/qe_workspace/factor-cache/meta
/api/v1/qe_workspace/factor-cache/factors/{factor_name}/status
/api/v1/qe_workspace/factor-cache/factors/{factor_name}/file
/api/v1/qe_workspace/factor-cache/sync
```

Remote deployment backup:

```text
/home/lc999/aistock_backups/rdagent_factor_cache_streaming_20260506_225249/factor_cache_api.py
```

## Streaming Upload Sample

A direct `FactorCacheNodeApiClient.upload_sync_bundle()` test uploaded 4 files x 32 MiB to isolated `/tmp/aistock_factor_cache_stream_test/factor_values` on each node with `max_workers=4`.

```text
local-wsl  ok=True uploaded_count=4 upload_workers=4 elapsed_s=0.389 throughput_mib_s=328.68 max_rss_mb=54.0 meta_factor_count=4 status=True
remote-node ok=True uploaded_count=4 upload_workers=4 elapsed_s=6.617 throughput_mib_s=19.34 max_rss_mb=54.1 meta_factor_count=4 status=True
```

Business meaning:

- The local Python sync process did not buffer the 128 MiB payload in memory; max RSS stayed near 54 MiB.
- Uploads are raw streaming `PUT` requests followed by one metadata update, not all-in-one multipart body construction.
- Remote throughput in this small sample is still below theoretical LAN capacity and appears constrained by current Wi-Fi/remote write path, but the implementation now supports configurable concurrent uploads up to 16 workers.

## Cache State Evidence

Read-only production `8001` status check after remote node restart:

```text
rdagent-node1 reachable=True remote_cached=713 synced=575 missing=0 stale=0
wsl2-5080    reachable=True remote_cached=575 synced=575 missing=0 stale=0
```

Worktree service using the production factor-cache root in read-only stats mode reported:

```text
local_cached=575
rdagent-node1 reachable=True remote_cached=713 synced=575 missing=0 stale=0 sync_transport=node_api_streaming_put
wsl2-5080    reachable=True remote_cached=575 synced=575 missing=0 stale=0 sync_transport=node_api_streaming_put
```

## Dev Backend Note

A dev backend start on `8012` was attempted with schedulers disabled and `PYTHONIOENCODING=utf-8`. Full app startup is currently blocked before this feature route loads by a pre-existing missing import:

```text
ModuleNotFoundError: No module named 'backend.services.rl_execution'
```

This is outside the factor-cache sync change. No production `8001` restart was performed.

## Residual Risks

- Production `8001` must be restarted or replaced by the merged code before UI one-click sync uses the streaming implementation.
- The remote sample did not saturate the theoretical LAN link; further tuning may require larger real cache batches, worker-count tuning, and checking Wi-Fi/remote disk throughput.
- The legacy multipart `/sync` endpoint remains for compatibility, but AIstock no longer uses it for normal factor-cache sync.
