# 因子缓存 / 相关性 / QE 回测缓存统一链路全量验收

- 验收日期：2026-06-20（Asia/Shanghai）
- 验收范围：官方因子独立指标全量计算、official single factor cache、因子相关性全量计算、QE 回测缓存命中、旧 realtime cache 链路清理、生产 UI/API 只读验证
- 相关 BUG：BUG-420、BUG-421、BUG-422、BUG-425、BUG-426、BUG-427、BUG-429、BUG-432、BUG-433
- 最终结论：通过；官方链路统一使用 `rdagent_assets/factor_values/single`，未发现 official / correlation / QE 回测读取 `factor_values_realtime`

## 业务目标

本次验收验证以下目标是否已满足：

1. 官方因子独立指标计算只使用训练到回测窗口 `2018-08-01 ~ 2026-04-30` 的离线回测数据与 official single cache，不走实时 DB snapshot / realtime cache。
2. 因子相关性计算只读取同一份 official single parquet cache，不回退到 `factor_values_realtime` 或其他历史快照目录。
3. QE 回测在任意子窗口读取因子值时可以直接命中 official cache，避免重复生成独立回测缓存。
4. UI 必须展示独立指标 / 相关性 / QE 回测共用的缓存时间段，且默认显示全量数据集窗口。
5. 旧链路保留必须仅限兼容或非官方 live/simulation 路径，不得影响官方独立指标、相关性和 QE 回测。

## 关键结果

| 验收项 | 结果 |
|---|---:|
| 启用因子数 | 575 |
| official single parquet cache | 575 / 575 |
| official cache Meta 因子数 | 575 |
| orphan parquet | 0 |
| 官方独立指标成功因子 | 575 / 575 |
| 独立指标入库行数 | 2875 |
| 相关性请求因子 | 575 |
| 相关性成功矩阵因子 | 574 |
| 相关性排除因子 | 1 (`quality_structure_composite`, `no_valid_pairs`) |
| 相关性 DB 记录 | 164429 |
| QE cache hit 子窗口 | 3 / 3 全部通过 |
| 生产 UI 只读 smoke | 通过，HTTP 200，显示 575 / 574 / 2018-08-01 / 2026-04-30 |

## 证据路径

- 验收摘要：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/full_chain_summary.json`
- 生产 UI smoke：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/ui_smoke_after_bug433.json`
- 生产 UI 截图：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/factor_correlation_after_bug433.png`
- BUG-433 前置失败 UI 证据：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/ui_smoke.json`
- BUG-433 前置失败截图：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/factor_correlation.png`
- 因子库缓存截图：`tests/aistock_validation/history/qe/evidence/20260620_full_chain_validation/factor_list_cache.png`
- 本机临时完整日志：`debug_tools/qe/20260619_factor_cache_full_chain_validation/`（未提交超大日志，仅作为本机复现材料）

## 执行与验证摘要

### 1. 官方独立指标全量计算

- 执行环境：WSL official full compute
- 数据窗口：`2018-08-01 ~ 2026-04-30`
- 结果：`success_count=575`、`fail_count=0`、`total_metrics_inserted=2875`
- official cache：`rdagent_assets/factor_values/single/*.parquet`，共 575 个 parquet
- runtime validation：`gate_status=passed`
- 内存门禁：批次释放后 `single_cache_entries=0`，swap 保持 `0.0MB`
- 基础数据缓存策略：全量 bin/h5/parquet 等底层数据加载一次后按批计算因子值，因子批次完成后释放 batch single cache

### 2. DB / cache baseline

- `cache.single_parquet_count=575`
- `cache.meta_factor_count=575`
- `cache.orphan_parquet_count=0`
- `top_level_as_of_date=2026-04-30`
- `top_level_window_train_start=2018-08-01`
- `top_level_window_backtest_end=2026-04-30`
- `data_source_mode=official_offline_backtest_factor_data`
- DB `2026-04-30` 五个 eval window 均为 575 个因子

### 3. 因子相关性全量计算

- WSL 相关性计算使用 payload：`correlation_payload_after_full_cache.json`
- cache root：`/mnt/f/Dev/AIstock/rdagent_assets/factor_values`
- cache source：`offline_research_backtest_factor_values`
- 结果：`requested_factor_count=575`、`success_factor_count=574`、`failed_factor_count=1`
- 排除分类：`quality_structure_composite` 被归类为 `no_valid_pairs`，不是缓存缺失
- DB records：`164429`
- runtime validation：`gate_status=passed`
- 重要边界：未使用 `factor_values_realtime`

### 4. QE 回测 cache hit 验证

| 子窗口 | 起始 | 结束 | 命中情况 |
|---|---|---|---|
| full_dataset | 2018-08-01 | 2026-04-30 | 575 / 575 |
| qe_subwindow_2020_2021 | 2020-01-01 | 2021-01-01 | 575 / 575 |
| recent_backtest_slice | 2025-01-01 | 2026-04-30 | 575 / 575 |

结论：只要回测窗口是 official cache 全量窗口的子集，QE 回测使用同一份 official cache 完全可行。

### 5. 旧链路扫描

扫描结论：

- official 独立指标、相关性、QE 回测路径显式使用 `source="single"`。
- `FactorValueLoader()` 默认 fail-fast，非官方读取必须显式声明 source。
- `RealtimeFactorDataLoader` 仍存在于非官方 live/simulation transform/data-service 路径。
- `backfill_factor_cache.py` 已处于 retired/兼容测试遗留状态，不再作为官方回填入口。
- 未发现 official / correlation / QE 路径读取 `factor_values_realtime`。

### 6. BUG-433 修复与验收

发现问题：生产因子相关性页面 `/quantevolver/factor-correlation` 在前置 UI smoke 中 500，错误为缺少 `@babel/runtime`，import trace 指向 `react-syntax-highlighter` / `PairDetail.tsx`。

修复：

- PR：`https://github.com/licong01-cloud/AIstock/pull/1334`
- close-sync：`https://github.com/licong01-cloud/AIstock/pull/1335`
- 修复提交：`d393546ed9ab6a5b5f2a13e378926f16b86d96b5`
- 变更：`frontend/package.json`、`frontend/package-lock.json` 增加 `@babel/runtime@7.29.7`
- 二次冗余 close-sync PR `#1336` 已关闭，原因是它会把 BUG JSON 的真实修复 PR 信息错误覆盖为 close-sync PR 信息

BUG-433 验证：

- `npm ls @babel/runtime --depth=0` 通过
- `npm run build` 通过，`/quantevolver/factor-correlation` 编译成功
- `npm run lint` 通过，仅既有 react-hooks warnings
- `nox -s l0` 通过
- `nox -s validation_module_registry_l0` 通过，8 passed
- `QE_READ_L3_SKIP_UI=1 nox -s qe_read_l3` 通过；完整 `qe_read_ui` 仍被既有非本 BUG 的 `frontend/tests/research-assistant/phase5-mcp-gateway-ui.spec.ts:226` TS2322 阻断
- 根仓库 `npm install --prefer-offline --ignore-scripts` 已执行
- 根仓库 `npm run build` 已执行通过
- 生产端口 3000 只读 UI smoke 已通过，页面显示 575、574、`2018-08-01`、`2026-04-30`

## API 只读验证

- `GET /api/v1/quantevolver/factor-cache/stats`：HTTP 200，575 cache，窗口 `2018-08-01 ~ 2026-04-30`
- `GET /api/v1/quantevolver/evolution/correlations/cache-status`：HTTP 200，575 cached，0 uncached
- `GET /api/v1/quantevolver/evolution/correlations/overview`：HTTP 200，enabled total/evaluated/cached=575，correlation_computed=574，metadata num_factors=574
- `GET /quantevolver/factor-correlation`：HTTP 200，UI 展示 official cache window、575 缓存、574 矩阵因子、1 待计算因子

## 设计合规矩阵

| 设计要求 | 实现 / 证据 | 状态 |
|---|---|---|
| 官方独立指标只使用训练到回测窗口数据 | full compute 数据窗口 `2018-08-01 ~ 2026-04-30`，`cache_source=official_offline_backtest_factor_data` | 通过 |
| 相关性只使用 official single cache | runtime validation `official_cache_only=true`，cache root 为 `rdagent_assets/factor_values` | 通过 |
| QE 回测复用同一 official cache | 3 个回测子窗口 `official_cache_hit=true`，575/575 命中 | 通过 |
| 禁止 official/correlation/QE 使用 realtime cache | old path scan 未发现 official / correlation / QE 读取 `factor_values_realtime` | 通过 |
| UI 展示统一缓存窗口 | 生产 UI smoke 可见 `2018-08-01 ~ 2026-04-30`、575、574 | 通过 |
| 长任务内存可控 | WSL full compute 批次释放，`single_cache_entries=0`，swap `0.0MB` | 通过 |
| BUG 修复按流程同步 GitHub | BUG-433 Issue #1333 closed，PR #1334/#1335 merged | 通过 |

## 剩余风险 / 非本次阻断

- `frontend/tests/research-assistant/phase5-mcp-gateway-ui.spec.ts:226` 存在既有 TS2322（`null` 不可赋值给 `string`）阻断完整 `qe_read_ui` 的 tsc 阶段；历史 BUG 记录已多次标注该问题，非 BUG-433 范围。
- `npm install` 输出 11 个 npm audit vulnerabilities；本次未变更审计策略，未执行 breaking `npm audit fix --force`。
- 本次写入了生产 DB 中 official 独立指标和相关性结果；未执行 DDL，未重启生产服务。

## 生产门禁

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=applied_and_verified`
- 生产 runtime：未由 Codex 重启；仅执行只读 UI/API 验证和 root 前端依赖安装 / build 门禁
- 生产 DB：官方独立指标与相关性计算已写入指标/相关性结果；无 DDL
