# BUG-398 官方因子缓存 Meta 不完整修复验证记录

## 背景

运行时已确认官方因子值缓存以 `rdagent_assets/factor_values/single/*.parquet` 为物理缓存来源，但 `_meta.json` 只记录了部分因子，导致 UI、远端同步统计和 QE cache-hit 合约把已有 parquet 误判为无缓存或允许后续静默重算。BUG-398 要求官方路径只能使用 `rdagent_assets/factor_values`，不得消费 `factor_values_realtime`，并且必须把 disk/meta/orphan 状态暴露给 UI 和 QE 回测合约。

## DESIGN-COMPLIANCE-001 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| 官方缓存状态以 `single/*.parquet` 磁盘文件为物理事实，并展示 disk/meta/orphan 数量 | `backend/services/quantevolver/factor_value_pipeline.py`, `backend/routers/quantevolver.py`, `frontend/src/app/quantevolver/components/FactorList.tsx`, `frontend/src/app/quantevolver/factor-correlation/components/ComputePanel.tsx` | py_compile、临时契约脚本输出 `disk_factor_count=1/meta_factor_count=0/orphan_parquet_count=1`、前端范围 tsc/eslint | pass | 无 |
| `_meta.json` 缺失/不完整时不得被 UI 误报为无缓存；应标记待补元数据 | `backend/routers/quantevolver.py`, `frontend/src/app/quantevolver/components/FactorList.tsx` | 因子列表新增 `missing_meta_reconcile_required` filter 与行级 `△ 待补元数据` 展示；`no_cache` 排除待补元数据 | pass | 无 |
| QE prepare_factors/cache-hit 合约在 parquet 存在但 meta 缺失时 fail-fast，不能静默重算 | `backend/services/quantevolver/config_composer.py`, `backend/services/quantevolver/factor_value_loader.py` | `test_qe_prepare_factors_cache_contract.py` 包含在 targeted pytest 22 passed；py_compile 通过 | pass | 无 |
| 远端同步统计以本地磁盘 parquet 全集为比较基准，但只上传 meta 完整条目 | `backend/services/quantevolver/factor_cache_remote_sync_service.py`, `frontend/src/app/quantevolver/components/FactorList.tsx` | targeted pytest 22 passed；临时契约脚本验证 orphan entry `_metadata_status=missing_meta_reconcile_required`；UI 显示 local meta status | pass | 无 |
| 相关性页面继续只使用官方离线 cache，不走 realtime cache | `backend/services/quantevolver/correlation_compute_service.py`, `frontend/src/app/quantevolver/factor-correlation/components/ComputePanel.tsx` | 代码路径检查；相关性 cache status 展示磁盘/Meta 和待补元数据 | pass | 无 |
| 生产安全门禁 | 无 DDL/依赖新增；未重启生产服务；未写生产 DB | `production_ddl_gate=noop`, `production_backend_dependency_gate=noop`, `production_frontend_dependency_gate=noop` | pass | 无 |

## 验证命令与结果

```powershell
python -m py_compile backend\routers\quantevolver.py backend\services\quantevolver\config_composer.py backend\services\quantevolver\factor_cache_remote_sync_service.py backend\services\quantevolver\factor_value_loader.py backend\services\quantevolver\factor_value_pipeline.py
# PASS

python -m pytest backend/tests/test_factor_cache_wsl_env.py backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py -q -p no:cacheprovider
# PASS: 22 passed

python -m nox -s l0
# PASS

python -m nox -s validation_module_registry_l0
# PASS: 8 passed; ownership scan mapped=12 unmapped=0 ambiguous=0

$env:NO_PROXY='127.0.0.1,localhost,::1'; $env:no_proxy='127.0.0.1,localhost,::1'; $env:QE_READ_L3_SKIP_UI='1'; python -m nox -s qe_read_l3
# PASS: qe_read_l3 + qe_read_backend; backend tests 14 passed

cd frontend; npm exec eslint -- src/app/quantevolver/components/FactorList.tsx src/app/quantevolver/factor-correlation/components/ComputePanel.tsx
# PASS

cd frontend; .\node_modules\.bin\tsc.cmd --noEmit --incremental false --pretty false --project .\tsconfig.bug398.tmp.json
# PASS with temporary project including changed files and src/types/**/*.d.ts; temp config removed

python scripts/code_intelligence_adapter.py verify-clients --item-id BUG-398 --module qe --changed-file backend/routers/quantevolver.py --changed-file backend/services/quantevolver/config_composer.py --changed-file backend/services/quantevolver/factor_cache_remote_sync_service.py --changed-file backend/services/quantevolver/factor_value_pipeline.py --changed-file backend/services/quantevolver/factor_value_loader.py --changed-file frontend/src/app/quantevolver/components/FactorList.tsx --changed-file frontend/src/app/quantevolver/factor-correlation/components/ComputePanel.tsx --changed-file tests/aistock_validation/history/qe --changed-file tests/aistock_validation/bugs/20260617_BUG-398-official-factor-cache-meta-incomplete-causes-qe-cache-miss-and-misleadin.json --changed-file tests/aistock_validation/bugs/.bug_id_allocator.json
# PASS: workflow_gate=ready codegraph=ok clients_ready=4/4

临时契约脚本：在 `rdagent_assets/factor_values/_tmp_bug398_contract` 创建 1 个 parquet + 空 `_meta.json`，验证后删除目录
# PASS: {'inventory': {'disk_factor_count': 1, 'meta_factor_count': 0, 'orphan_parquet_count': 1}, 'remote_entry_status': 'missing_meta_reconcile_required'}

git diff --check
# PASS
```

## 已知环境阻塞与处理

- 直接运行 `python -m nox -s qe_read_l3` 时，`qe_read_l3` 与 `qe_read_backend` 通过，但 `qe_read_ui` 在服务预检失败：当前窗口只有 `HTTP_PROXY/HTTPS_PROXY/ALL_PROXY=http://127.0.0.1:7896`，没有 `NO_PROXY`；默认请求 `127.0.0.1:8011/openapi.json` 经代理返回 HTTP 502。直连 `--noproxy 127.0.0.1,localhost` 为 connection refused，说明测试后端 8011 未启动。该失败是验证环境/测试服务状态问题，不来自 BUG-398 代码改动。
- 全量 `npm exec tsc -- --noEmit --incremental false` 现在可运行，但被既有非本次变更文件阻断：`frontend/tests/research-assistant/phase5-mcp-gateway-ui.spec.ts(226,11): Type 'null' is not assignable to type 'string'`。BUG-398 范围 tsc 和 eslint 已通过。
- `npm ci` 仅在 BUG worktree 恢复现有 lockfile 依赖用于前端静态验证；未修改 `package.json`/`package-lock.json`，无前端依赖新增。

## 生产门禁

- `production_ddl_gate=noop`：无 DDL。
- `production_backend_dependency_gate=noop`：无后端依赖新增。
- `production_frontend_dependency_gate=noop`：无前端依赖新增。
- 未重启生产 backend/frontend，未写生产 DB，未触碰生产数据。
