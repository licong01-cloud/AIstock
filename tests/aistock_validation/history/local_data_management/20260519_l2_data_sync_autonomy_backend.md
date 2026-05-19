# 本地数据同步自治开发验证记录

- 模块：local_data_management
- 级别：L2
- 日期：2026-05-19T13:52:09+08:00
- 分支：`feature/data-sync-autonomy-20260519`
- Implementation commit：`9dd0d7a`
- 执行者：Codex

## 验证范围

- 实现本地数据同步自治控制面：以 `dataset_date_refresh_audit` 为唯一业务就绪依据，补充可持久化 retry target、final Alert Gate 和看板状态展示。
- `cyq_perf` 接入统一 `TushareSyncEngine`；`cyq_chips` 暂时保留 legacy 脚本路径，等待独立的 BY_CODE/per-date audit 方案。
- 新增 `market.data_sync_targets` 与 `market.data_sync_attempts` 迁移，包含表/字段注释、dataset/date 唯一约束和 retry/final 索引。
- `cyq_perf` 首次无 audit cursor 时先从物理表种子化 audit；只有 audit 和物理表都为空时才从 `2018-01-01` cold start。
- Tushare BY_DATE 增加 provider contract fail-fast：缺少主键/date 字段或返回日期不等于请求日期时记录 `provider_contract_error`。
- retry target 支持 scheduler 重启后恢复；自动重试成功后关闭 target，避免恢复后重复重试和重复报警。
- `/api/data-stats` 与 `/local-data` 展示 audit/cache/sync-target/operator-action 状态。
- 不在本次范围：生产 DB 应用迁移、真实 Tushare 补数、重启生产 `8001`、重启生产 `3000`、合入 `main`。

## 业务断言

| 断言 | 期望结果 | 证据 | 结果 |
|---|---|---|---|
| audit-first readiness | `dataset_date_refresh_audit` 是唯一业务就绪依据；job success 和物理表仅为证据 | unit/API/scheduler tests | PASS |
| `cyq_perf` 首次同步 | audit cursor 为空时先审计物理表；物理表已有数据时不得从 `2018-01-01` 全量重拉，物理表也为空时才 cold start | `test_cyq_perf_audit_cursor_missing_seeds_from_physical_table_before_bootstrap`、`test_compute_auto_range_seeds_audit_from_physical_rows_before_bootstrap`、`test_cyq_perf_bootstrap_incremental_uses_full_start_only_when_audit_and_table_empty` | PASS |
| audit gap cursor | `cyq_perf` audit/physical 审计发现中间交易日缺口时，safe cursor 停在缺口之前，不用 `MAX(success)` 跳过失败日期 | `test_cyq_perf_audit_cursor_stops_before_unresolved_audit_gap`、`test_cyq_perf_audit_seed_returns_safe_cursor_before_physical_gap` | PASS |
| audit-cursor 数据集复核 | `stock_st_events`、`cyq_perf`、3 个财务 raw 数据集 audit 为空但物理表有数据时均先种子化 audit，不直接 bootstrap | `test_all_audit_cursor_specs_seed_from_existing_physical_table_when_audit_missing` | PASS |
| 交易日展开 | `cyq_perf` bootstrap 跳过周末/节假日，避免非交易日 0 行误报 | `test_cyq_perf_by_date_sync_skips_non_trading_dates` | PASS |
| retry 可恢复 | delayed/due retry 写入 `data_sync_targets`，scheduler refresh 后可恢复提交 | scheduler reconciliation tests | PASS |
| 最终报警门禁 | 只有 final 不可恢复状态可写报警；DB CHECK 允许 `final_blocked` | migration assertion + Alert Gate test | PASS |
| target 生命周期 | 数据恢复后 target 置为 `success` 并清除 failure/retry，避免后续重复重试 | finalizer/recovery tests | PASS |
| 看板可解释 | 看板展示 target date、failure category、retry/deadline 与 operator action，避免“等待自动重试 + 需人工处理”矛盾 | TypeScript/build/code review | PASS |

## 命令与结果

```powershell
python -m compileall backend/services/data_sync_autonomy.py backend/services/tushare_dataset_specs.py backend/services/tushare_sync_engine.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/services/validation/plan_catalog.py
# PASS

python -m pytest backend/tests/test_data_sync_autonomy.py backend/tests/test_dataset_refresh_audit.py backend/tests/test_tushare_sync_engine.py backend/tests/test_ingestion_data_stats_readiness_api.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/test_validation_center_api.py -q -p no:cacheprovider
# PASS: 57 passed

python -m nox -s data_sync_autonomy_backend
# PASS: compileall + 46 backend tests + frontend tsc

python -m nox -s local_data_management_audit
# PASS: 46 tests + dataset_refresh_audit_schema smoke

python -m nox -s l0 -- noxfile.py scripts/create_data_alerts_table.py backend/services/data_sync_autonomy.py backend/services/tushare_dataset_specs.py backend/services/tushare_sync_engine.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/services/validation/plan_catalog.py backend/tests/test_data_sync_autonomy.py backend/tests/test_dataset_refresh_audit.py backend/tests/test_tushare_sync_engine.py backend/tests/test_ingestion_data_stats_readiness_api.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/test_validation_center_api.py tests/aistock_validation/catalog/test_plans.yaml tests/aistock_validation/catalog/file_ownership.yaml tests/aistock_validation/modules/local_data_management.md frontend/src/app/local-data/page.tsx
# PASS: no HIGH findings and no blocking new P0/P1 findings; raw-json/complexity P2 为既有非阻断审查项

cd frontend; npm exec tsc -- --noEmit --incremental false
# PASS

cd frontend; npm run lint
# PASS with existing react-hooks warnings

cd frontend; npm run build
# PASS: /local-data built successfully

git diff --check
# PASS; only line-ending warnings reported
```

## 审查问题与修复

| 问题 | 修复 | 验证 |
|---|---|---|
| `final_blocked` 可能违反 `market.data_alerts.alert_type` CHECK | migration 与建表脚本均加入 `final_blocked` | migration assertion + scoped L0 |
| `cyq_perf` scheduled auto-range 在 audit 为空时可能直接使用 bootstrap | `cursor_source=refresh_audit` 时先读 safe audit cursor；audit 为空先物理表种子化，物理表也为空才 bootstrap | scheduler unit tests |
| `cyq_perf` 长区间 bootstrap 包含非交易日会导致 0 行误失败 | spec 加 `date_sequence="trading"`，engine 通过 `trading_calendar` 展开 | Tushare engine unit test |
| scheduler 重启后 due target 可能要等夜间任务才恢复 | `refresh_schedules()` 增加 due target reconcile | scheduler unit test |
| target 恢复后可能仍保持 due 状态并重复重试 | retry finalizer 与 auto-retry recovery 将 target 关闭为 `success` | target finalizer unit test |
| UI 可能显示矛盾状态 | 看板优先展示 failure category、target date、final deadline；需人工处理时不再显示下一次自动重试 | `tsc`、`lint`、`build` |
| Validation Center plan 声明 coverage_required 但 nox 不产出 coverage | 该 plan 明确 `coverage_required: false`，并把 frontend typecheck 纳入 evidence | validation catalog tests + nox session |

## 数据与运行安全

- 生产 backend `8001`：未触碰、未重启。
- 生产 frontend `3000`：未触碰、未重启。
- 生产数据库：未写入；migration 未应用。
- Tushare live API：未调用。
- 受保护资产：未修改 StrategyPackage manifest、模型权重、HMM snapshot、QE/RD-Agent artifact、Paper trading ledger。
- 工作区中 MiniQMT/QMT 相关未提交文件为非本任务内容，已按用户要求忽略且未纳入本次提交范围。

## 残余风险

- `frontend/src/app/local-data/page.tsx` 存在既有 raw JSON/乱码展示 P2 guardrail；本次只修复数据同步状态行，不重构整个 legacy 页面。
- 当前 Python 环境未安装 `coverage`/`pytest-cov`，本 nox entry 不收集 coverage；已在 plan 中显式标记 `coverage_required: false`。
- 迁移需要在用户确认后再应用到目标数据库；应用前运行时代码不能实际创建新表或修改 alert CHECK。
- 真实数据完整性需要在 migration 应用且 scheduler 运行后单独验证；本记录只证明代码、测试与流水线具备合入候选条件。

## 合入准备状态

- 最终状态：PASS（代码/测试/流水线验证范围）。
- 可作为 merge candidate 推送到远端分支供用户 review。
- 未合入 `main`；是否合入需要用户确认。
