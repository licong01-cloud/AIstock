# StrategyPackage V25 Model Cache Resolution Validation

Date: 2026-05-05

## Scope

- Issue: StrategyPackage/Paper v2 readiness failed with
  `DATA_UNAVAILABLE: V25_TWO_STAGE early_model_path is not accessible from AIstock backend`.
- Fix scope:
  - `backend/services/strategy_package/model_asset_resolver.py`
  - `backend/tests/strategy_package/test_model_asset_resolver.py`
- Business goal: a QE experiment that records Linux `/home/.../rl_models/...`
  model paths can be resolved through AIstock-owned model cache files without
  touching original model weights or falling back to another execution algo.

## Risk Controls

- No WSL UNC or `/mnt/...` probing was added.
- No V25-to-TWAP/default execution fallback was added.
- Original model path is preserved in `original_early_model_path` and
  `original_late_model_path`.
- Resolved paths are copied into hashed AIstock cache files with sidecar
  metadata before manifest freeze/validation.
- Production backend port `8001` was not restarted.

## Commands And Results

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock python -m pytest backend/tests/strategy_package/test_model_asset_resolver.py -q -p no:cacheprovider
```

Result: `7 passed in 0.41s`

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock python -m pytest backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/strategy_package/test_repository_service.py backend/tests/trading_core/test_execution_algo_capabilities.py -q -p no:cacheprovider
```

Result: `29 passed in 0.30s`

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock python -m pytest backend/tests/strategy_package backend/tests/trading_core/test_execution_algo_capabilities.py -q -p no:cacheprovider
```

Result: `35 passed in 0.99s`

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
conda run -n AIstock python -c "import importlib; m=importlib.import_module('backend.routers.strategy_packages'); print(len(m.router.routes))"
```

Result: router import passed; `25` routes loaded.

```powershell
# Direct resolver smoke using existing AIstock-owned V25 cache files.
conda run -n AIstock python .codex_tmp/asset_smoke.py
```

Result:

```text
early_model_path=F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_early_net_joint_fixed_ccaaad87ee9199a6.pt exists=True size=174701 sidecar=True
late_model_path=F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_late_net_joint_fixed_deaa8a920f474542.pt exists=True size=363630 sidecar=True
status {'early_model_path': 'copied', 'late_model_path': 'copied'}
```

```powershell
# Real DB/router smoke; local .env was loaded without printing secrets.
conda run -n AIstock python .codex_tmp/router_readiness_smoke.py
```

Result:

```text
router_readiness_ok True
algo V25_TWO_STAGE
early_model_path F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_early_net_joint_fixed_ccaaad87ee9199a6.pt
late_model_path F:\Dev\AIstock\rdagent_assets\model_cache\execution\V25_TWO_STAGE\v25_late_net_joint_fixed_deaa8a920f474542.pt
runtime_asset_cache_status {'early_model_path': 'copied', 'late_model_path': 'copied'}
```

Sample experiment used for the real smoke:
`qe_20260504_110457_5400_L8`.

```powershell
git diff --check -- backend/services/strategy_package/model_asset_resolver.py backend/tests/strategy_package/test_model_asset_resolver.py
```

Result: no whitespace errors; Git reported existing Windows line-ending
normalization warnings only.

## Business Outcome

- The exact V25 `early_model_path`/`late_model_path` Linux values now resolve
  through AIstock-owned cache files.
- The router-level readiness path returns `ok=True` for a completed V25 QE
  experiment after runtime asset resolution.
- The validated manifest points to local hashed cache paths, not to `/home/...`
  paths.

## Residual Risks

- The running production backend process must be restarted or redeployed before
  port `8001` uses the new resolver code.
- This validation did not create a new QE experiment; it validates existing
  completed QE experiment metadata and existing AIstock cache assets.
- The resolver still fails fast if neither the original path nor the AIstock
  cache source exists.
