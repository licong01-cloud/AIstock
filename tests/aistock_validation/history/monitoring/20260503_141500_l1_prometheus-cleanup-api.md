# Prometheus Cleanup API Validation - 2026-05-03

## Scope

- Monitoring config: Prometheus retention set to `14d` and `30GB` with admin API enabled.
- Backend API: `/api/v1/prometheus-admin/status`, `/cleanup/preview`, `/cleanup`.
- No production backend restart and no live Prometheus cleanup was executed.

## Risk Matrix

- False success risk: cleanup endpoint returns success without calling Prometheus admin APIs.
- Safety risk: cleanup runs without explicit confirmation text.
- Operational risk: retention settings are missing from docker-compose, so future data growth is not capped.

## Commands

```powershell
python -m py_compile backend/services/prometheus_admin.py backend/routers/prometheus_admin.py
pytest backend/tests/test_prometheus_admin.py -q
python -m py_compile backend/main.py
```

## Results

- `pytest backend/tests/test_prometheus_admin.py -q`: 5 passed.
- Python compile checks passed for changed backend files.
- Cleanup confirmation guard verified: wrong text returns HTTP 400 and performs no side effect.
- Preview endpoint verified as read-only and defaults to 14 days.
- Execute endpoint verified to call delete-series and tombstone cleanup only with `DELETE_PROMETHEUS_HISTORY`. Real Prometheus admin API empty HTTP 204 success responses are accepted.

## Evidence

- Test file: `backend/tests/test_prometheus_admin.py`.
- Config file: `monitoring/docker-compose.yml`.
- Operations note: `docs/operations_prometheus_cleanup.md`.

## Residual Risks

- Real Prometheus cleanup was not executed in validation to avoid deleting monitoring history during code validation.
- F-drive space is not reclaimed until Docker/WSL VHDX compaction is performed after internal cleanup.



## Live Operation - 2026-05-03 23:01 Asia/Shanghai

- Recreated only the `prometheus` Docker Compose service; `timescaledb` was not restarted and remained `Up 2 weeks`.
- Prometheus accepted the new runtime flags: `storage.tsdb.retention.time=2w`, `storage.tsdb.retention.size=30GiB`, `web.enable-admin-api=true`, `web.enable-lifecycle=true`.
- Prometheus startup retention deleted obsolete TSDB blocks.
- Executed confirmed cleanup through AIstock API: `POST /api/v1/prometheus-admin/cleanup` with `DELETE_PROMETHEUS_HISTORY`; result HTTP 200 with delete-series and tombstone cleanup success.
- `monitoring_prometheus_data` changed from about `113.5GB` before the operation to `28.32GB` after cleanup.
- Docker internal disk usage changed from about `223.6G` before the operation to `145.8G` after cleanup.
- Host VHDX `F:\DockerData\wsl\disk\docker_data.vhdx` remained large (`303.55GB`); VHDX compaction was intentionally not performed because it requires stopping Docker/WSL and would interrupt the database container.
