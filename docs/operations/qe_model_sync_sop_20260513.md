# QE Model Sync SOP

Date: 2026-05-13
Scope: offline/on-demand synchronization of QE-trained execution model artifacts into an explicit AIstock model cache destination.

## Safety Boundary

- The sync utility is local/offline only and does not connect to production DBs, backend services, Paper v2, QE archive, or QMT.
- The destination must be supplied by the caller with `--cache-root`; the tool has no production default.
- Dry run is the default. Model bytes and sidecars are written only when `--apply` is passed.
- Existing destination files with different hashes are blocked unless `--overwrite` is passed explicitly.
- The tool never discovers alternate filenames, searches fallback directories, or silently maps one execution algorithm to another.

## Command Pattern

Dry-run first:

```powershell
$env:PYTHONIOENCODING='utf-8'
python scripts/sync_qe_models_to_aistock_cache.py `
  --source-dir F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE `
  --cache-root F:/Dev/AIstock/rdagent_assets/model_cache/execution `
  --algo-code V25_1_SMALL_CAP `
  --model v25_early_net_joint_fixed.pt `
  --model v25_late_net_joint_fixed.pt `
  --json
```

Apply only after checking the dry-run plan:

```powershell
python scripts/sync_qe_models_to_aistock_cache.py `
  --source-dir F:/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE `
  --cache-root F:/Dev/AIstock/rdagent_assets/model_cache/execution `
  --algo-code V25_1_SMALL_CAP `
  --model v25_early_net_joint_fixed.pt `
  --model v25_late_net_joint_fixed.pt `
  --apply
```

## Hash-Gated Sync

When a QE run or release note provides expected hashes, pass one assertion per file:

```powershell
python scripts/sync_qe_models_to_aistock_cache.py `
  --source-dir /mnt/f/Dev/AIstock/rdagent_assets/model_cache/execution/V25_TWO_STAGE `
  --cache-root F:/Dev/AIstock/rdagent_assets/model_cache/execution `
  --algo-code V25_1_SMALL_CAP `
  --model v25_early_net_joint_fixed.pt `
  --expected-sha256 v25_early_net_joint_fixed.pt=<64-hex-sha256> `
  --apply
```

A mismatch fails before any write.

## Windows and WSL Paths

- WSL mount paths like `/mnt/f/Dev/AIstock/...` are translated to `F:\Dev\AIstock\...` for Windows callers.
- Linux absolute paths like `/home/lc999/...` on Windows require `--wsl-distro <name>` or `AISTOCK_WSL_DISTRO`; they are translated to `\\wsl.localhost\<distro>\...`.
- Translation is deterministic. The tool does not probe alternate distros or fallback locations.

## Sidecar Metadata

Each applied model write creates or refreshes a sidecar next to the model:

```text
<model_filename>.aistock-sync.json
```

The sidecar records:

- `algo_code`
- `model`
- `source` and `destination`
- `source_dir` and `cache_root`
- `sha256`
- `size_bytes`
- `created_at_utc`
- `write_mode`

This is an audit sidecar only. It does not change StrategyPackage manifests, QE workspaces, catalog rows, or validated policies.

## Validation

Run the offline tests after changing the utility:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/scripts/test_sync_qe_models.py -q -p no:cacheprovider
```

The tests use temporary directories only.
