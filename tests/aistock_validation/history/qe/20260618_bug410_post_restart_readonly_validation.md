# BUG-410 重启后只读验证记录

## 范围

- 验证对象：已合入的 BUG-410 `factor-cache/stats` 轻量统计与官方因子缓存 UI 展示。
- 运行环境：生产后端 `127.0.0.1:8001`、生产前端 `127.0.0.1:3000`，由用户完成重启。
- 约束：只读 GET/API 与页面查看；未调用 `factor-cache/compute`、相关性重算 POST、QE/RD-Agent/WSL 计算类任务；未重启任何服务。

## API 证据

| 验证项 | 结果 |
| --- | --- |
| `GET /openapi.json` | 200，OpenAPI `3.1.0`，耗时 1660ms |
| `GET /api/v1/quantevolver/factor-cache/stats` 首次 | 200，耗时 1502ms，`stats_mode=lightweight_inventory`，`stats_cache_hit=false` |
| `GET /api/v1/quantevolver/factor-cache/stats` 二次 | 200，耗时 55ms，`stats_cache_hit=true` |
| 默认 hash 行为 | `hash_check_enabled=false`，`db_hash_check_skipped=true` |
| 官方缓存口径 | `total_cached=575`，`effective_cached=575`，`total_code_factors=575`，`coverage_pct=100.0` |
| 磁盘/Meta 拆分 | `factor_parquet_count=575`，`all_parquet_count=576`，`merged_panel_present=true`，`merged_panel_size_mb=2821.2` |
| Meta 待补 | `meta_valid_cached=89`，`meta_factor_count=89`，`orphan_parquet_count=486`，`reconcile_required=486`，`no_cache=0` |
| 缓存路径 | `F:\Dev\AIstock\rdagent_assets\factor_values`；单因子目录 `single` |
| 因子列表待补过滤 | `GET /quantevolver/factors?...cache_filter=missing_meta_reconcile_required&limit=5` 返回 `total=486`，样本状态均为 `missing_meta_reconcile_required` |
| 远端统计 | `GET /factor-cache/remote-stats` 200，`local.disk_cached=575`，`local.effective_cached=575`，远端节点可见 `local_disk_factor_count=575` / `local_effective_factor_count=575` |
| 活动任务 | `GET /factor-cache/active-tasks` 200，`tasks=[]` |
| 相关性概览 | `GET /evolution/correlations/overview` 200，启用因子 `575/575` 已有相关性缓存，cache root 为 `rdagent_assets/factor_values` |
| 相关性矩阵 | `GET /evolution/correlations/matrix?threshold=0&include_disabled=true` 200，`factor_count=575` |
| 旧链路扫描 | active backend/frontend/scripts 范围内无 `factor_values_realtime`、`DataSnapshotManager`、legacy `/factor-values` 命中；官方路径 `FactorValueLoader` 均显式 `source="single"` |

## UI 证据

- 因子库页面：`http://127.0.0.1:3000/quantevolver/factors` 200，无 console error、无 request failed、无 4xx/5xx；页面显示“官方因子缓存（独立指标 / 相关性 / QE 回测共用）”、`启用 575/575`、`磁盘 575 / Meta 89`、`元数据待补486`、`merged panel`。
- 因子相关性页面：`http://127.0.0.1:3000/quantevolver/factor-correlation` 200，无非 GET 请求，无 console error、无 request failed、无 4xx/5xx；页面显示 575 因子矩阵，说明相关性只使用 `rdagent_assets/factor_values/single`，不会回退旧快照。
- 截图：`tests/aistock_validation/history/qe/evidence/bug410_restart_readonly_factor_list.png`
- 截图：`tests/aistock_validation/history/qe/evidence/bug410_restart_readonly_factor_correlation.png`

## 发现的偏差/风险

1. 因子库独立页面 `frontend/src/app/quantevolver/factors/page.tsx` 仍向 `FactorList` 传入 `backtestEnd: "2026-03-10"`，导致页面“当前窗口”显示 `2018-08-01 ~ 2026-03-10`，和默认全量数据集 `2026-04-30` 不一致；这不影响 stats API 的 575/575 轻量统计，但 UI 默认窗口存在旧上下文覆盖。
2. 运行时 stats/overview 从现有 meta 推断出的 `window_backtest_end=2026-04-28`，同时单因子 parquet 日期范围为 `2018-08-21~2026-04-30`、`as_of_date=2026-04-30`；Meta 缺口仍有 486 个，需要单独元数据 reconcile/backfill，不能等同于因子值缺失。
3. 当前 `F:\Dev\AIstock` 本地 `main` 含 BUG-410 修复，但落后 `origin/main` 1 个提交（`BUG-411 issue workflow fix`）；本次验证针对已重启运行时当前代码路径，不执行同步/重启。

## 结论

- BUG-410 核心验收通过：`factor-cache/stats` 默认走轻量 inventory，秒级返回并二次命中缓存；磁盘单因子 parquet 被作为官方缓存事实来源，575 个启用因子均识别为有效缓存；缺 Meta 被分类为 `reconcile_required`，没有误报为 `no_cache`。
- UI 展示核心口径通过：因子库和相关性页均展示官方共用缓存、磁盘/Meta 拆分、待补元数据、相关性/QE/独立指标共用关系。
- 未触发全量因子独立指标计算、相关性重算、QE/RD-Agent/WSL 计算任务；生产 DDL/依赖/服务重启均为 `noop`。
