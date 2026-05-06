# Docker Desktop Disk Cleanup Progress - 2026-05-04

This note records the current Docker Desktop / WSL disk cleanup state so the next session can continue without re-discovering the baseline.

## Current Snapshot

- Snapshot time: `2026-05-04 19:05:32 +08:00`.
- Repository: `F:\Dev\AIstock`.
- Database container status: `timescaledb` remained running, `Up 2 weeks`.
- Prometheus container status: `prometheus` running, `Up 20 hours` after the 2026-05-03 Prometheus-only recreate.
- No Docker Desktop shutdown, WSL shutdown, or VHDX compaction has been performed yet because those would interrupt the database container.

## Completed Cleanup

On 2026-05-03, Prometheus was reconfigured and cleaned:

- `monitoring/docker-compose.yml` now starts Prometheus with:
  - `--storage.tsdb.retention.time=14d`
  - `--storage.tsdb.retention.size=30GB`
  - `--web.enable-lifecycle`
  - `--web.enable-admin-api`
- AIstock backend has Prometheus admin endpoints:
  - `GET /api/v1/prometheus-admin/status`
  - `POST /api/v1/prometheus-admin/cleanup/preview`
  - `POST /api/v1/prometheus-admin/cleanup`
- Confirmed cleanup text: `DELETE_PROMETHEUS_HISTORY`.
- Prometheus cleanup was executed through AIstock API and tombstones were cleaned.

Prometheus API currently reports:

```json
{
  "ok": true,
  "retention": {
    "time": "2w",
    "size": "30GiB"
  },
  "admin_api_enabled": true,
  "lifecycle_enabled": true
}
```

## Docker Space Breakdown

`docker system df` on 2026-05-04:

```text
TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          16        7         3.175GB   1.126GB (35%)
Containers      8         6         3.981MB   3.981MB (99%)
Local Volumes   4         4         142GB     0B (0%)
Build Cache     66        0         7.389GB   7.389GB
```

Important Docker volumes:

```text
monitoring_prometheus_data                                         28.12GB
10f3de2305f0333f10f4ceaeecf00e99491dc45a6ac960effcc66616de93e1b4   113.8GB
9649065168f4ff2176643897ef148c2c12c7de54c990b74608d6edf673a2556e   0B
monitoring_grafana_data                                            31.9MB
```

Internal Docker Desktop disk:

```text
Filesystem     Size      Used      Available  Use%  Mount
/dev/sdf       1006.9G   145.7G    810.0G     15%   /mnt/docker-desktop-disk
```

Major internal Docker directories:

```text
26.2G    Prometheus volume data
106.0G   TimescaleDB/PostgreSQL active data volume
12.5G    Docker overlay2
223.7M   Docker containers metadata/log area
```

## Major Host-Side Files

Large WSL/Docker files on `F:`:

```text
F:\WSL\Ubuntu\ext4.vhdx                  927.82GB
F:\DockerData\wsl\disk\docker_data.vhdx  303.55GB
F:\wsl-swap.vhdx                         256.01GB
```

Current host drive free space:

```text
C: 243.37GB free / 952.83GB total
F: 421.27GB free / 3815.43GB total
G: 112.77GB free / 3725.90GB total
I:  11.21GB free / 3726.01GB total
```

## What Is Still Reclaimable

Low-risk Docker object cleanup that does not require stopping the database container:

- Build cache: about `7.389GB` reclaimable.
- Unused images: about `1.126GB` reclaimable.
- Stopped containers: about `3.981MB` reclaimable.

Host-side Docker VHDX compaction opportunity:

- Docker internal disk used: about `145.7G`.
- Host Docker VHDX file: about `303.55GB`.
- Approximate host-side space that may be recoverable by compacting `F:\DockerData\wsl\disk\docker_data.vhdx`: up to about `150GB`.
- This requires stopping Docker Desktop / WSL and will interrupt `timescaledb`, so do it only in a maintenance window.

## Important Do-Not-Delete Areas

- `10f3de2305f0333f10f4ceaeecf00e99491dc45a6ac960effcc66616de93e1b4` is the active TimescaleDB/PostgreSQL data volume, about `113.8GB` by Docker accounting / `106.0G` by WSL `du`.
- Do not run `docker system prune --volumes` unless the active database and monitoring volumes have been explicitly backed up and confirmed disposable.
- `F:\WSL\Ubuntu\ext4.vhdx` is the main Ubuntu WSL distro disk and is larger than Docker; it should be analyzed separately before any compaction or cleanup.
- `F:\wsl-swap.vhdx` is WSL swap. Changing/removing it requires WSL configuration review and a WSL restart.

## Safe Continuation Commands

Read-only status checks:

```powershell
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
docker system df
docker system df -v
wsl -d docker-desktop -- sh -c "df -h /mnt/docker-desktop-disk; du -sh /mnt/docker-desktop-disk/data/docker/volumes/*/_data 2>/dev/null"
curl.exe -sS --max-time 20 http://localhost:8001/api/v1/prometheus-admin/status
```

Optional low-risk cleanup, no volumes:

```powershell
docker builder prune
# or more aggressive build cache cleanup:
docker builder prune -a

# unused images only; do not include --volumes:
docker image prune -a
```

Prometheus cleanup API, if needed again:

```powershell
$body = @{ older_than_days = 14; confirm_text = 'DELETE_PROMETHEUS_HISTORY' } | ConvertTo-Json -Compress
Invoke-WebRequest -Uri 'http://localhost:8001/api/v1/prometheus-admin/cleanup' -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 300 -UseBasicParsing
```

Maintenance-window-only host VHDX compaction outline:

```powershell
# This stops Docker/WSL and interrupts containers, including TimescaleDB.
# Run only after confirming downtime is acceptable.
wsl --shutdown
# Then compact F:\DockerData\wsl\disk\docker_data.vhdx with an available Windows VHDX compaction method.
```

## Related Files

- Prometheus cleanup implementation note: `docs/operations_prometheus_cleanup.md`.
- Live validation record: `tests/aistock_validation/history/monitoring/20260503_141500_l1_prometheus-cleanup-api.md`.
- This continuation note: `docs/operations/docker_desktop_disk_cleanup_progress_20260504.md`.
