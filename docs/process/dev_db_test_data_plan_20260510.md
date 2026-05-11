# Dev DB 测试数据导入方案 (T17 + Phase 3 准备)

> **作者**: Claude Code 战略 session 2026-05-10
> **状态**: AUTHORITATIVE — 用户已确认数据策略
> **关联文档**: `docs/architecture/data_warehouse_extension_design_20260510.md`

## §1 数据策略（用户 2026-05-10 确认）

**核心原则**: **尽量使用纯生产真实数据**，仅当生产环境本身没有该数据（如新增 schema / 新增表）时使用合成 fixtures。

**只读保护**: 所有从生产 DB 的读取严格 SELECT-only，目标统一写到 dev DB (127.0.0.1:5433/aistock_dev)，绝不向生产写。

**Qlib bin / h5 文件**: 直接复用生产环境（read-only），不复制到 dev 区，因为这些是大体量的不可变历史数据。

## §2 数据导入分批

### Batch A — 真数据 (优先, 解锁 Codex Phase 3 live smoke)

| 表 / 数据 | 来源 | 体量预估 | 用途 |
|---|---|---|---|
| `market.index_daily` | prod SELECT 5y CSI300/CSI500/CSI1000/中证全指/上证综指 | ~6,250 行 | regime_label fetch_percentile + paper_v2 benchmark 注入 |
| `public.aistock_model_catalog` | prod SELECT 全表（~100 行） metadata | ~100 行 | model_registry view + StrategyPackage 关联 |
| `strategy_pkg.package` | prod SELECT 全表 | ~10-50 行 | governance_eligibility / enable_paper |
| `strategy_pkg.package_validation_run` | prod SELECT 全表 | ~50-200 行 | enable_paper fail-fast 路径 |
| `strategy_pkg.package_runtime_variant` | prod SELECT 全表 | ~30-100 行 | runtime variant 选择 |
| `strategy_pkg.package_asset` | prod SELECT 全表 | ~50-200 行 | asset ledger 验证 |
| `strategy_pkg.promotion_review` | prod SELECT 全表 | ~10-50 行 | 状态机覆盖 |
| `strategy_pkg.seed_fragility_score` | prod SELECT 全表 | ~10-50 行 | seed contract |
| `paper_v2.*` 21 张表 | prod SELECT 全部历史 simulation runs | ~5,000-20,000 行 | T5/T6.1/T6.2 真实写路径回放 + Codex Paper candidate 选择验证 |
| `qe_archive` 现有 27 张表（含 27 张 baseline） | prod SELECT 关键样本（最近 30 天 paper / model trial 等） | ~5,000 行 | 现有 baseline outbox/job 行为验证 |

**Batch A 总量**: 约 16k-30k 行，< 50MB。

### Batch B — 真数据 (T12 apply 后)

> Batch A 不依赖 T12，可立即跑。Batch B 在 dw-foundation apply T12 (22 张新 qe_archive 表) 完成后跑。

| 表 / 数据 | 来源 | 体量 | 用途 |
|---|---|---|---|
| 新建 22 张 `qe_archive.paper_v2_*` + `factor_value` | **空表**, 由 PaperV2ArchiveHandler / FactorValueArchiveHandler 通过 outbox event 在测试中产出 | 在 Phase 3 集成测试中 0 → 增长 | 验证 ETL 写路径 |

### Batch C — 合成 fixtures (仅新功能, 无 prod 对应)

| Fixture | 体量 | 用途 |
|---|---|---|
| `qe_archive.outbox_event` 新增 4 个 event_type 测试 row | 4 类型 × 10 = 40 行 | handler dispatch 测试 |
| `qe_archive.archive_job` 测试 job 状态机 | ~20 行 | worker 状态流转测试 |
| `paper_v2.fills` T5 capture fields 8 列合成填充 | 修改既有 row 而非新增 | T5 字段空值兜底测试 |
| `paper_v2.run.model_params_origin` 合成填充 | 修改既有 row | T1 CHECK 约束测试 |
| factor_pipeline_v2 因子重算批次合成 | 10 因子 × 60 trade_date = 600 行 | T15 factor.recompute.completed 触发 |

**Batch C 总量**: < 700 行，全部添加 `source='dev_seed'` 标识列（如 schema 允许）便于清理。

## §3 实现脚本（dw-foundation worktree 派发）

```
F:\Dev\AIstock\scripts\dev_db\
├── batch_a_import_real_data.py           # 全部真数据导入 (Batch A)
│   ├── --table market.index_daily
│   ├── --table public.aistock_model_catalog
│   ├── --table strategy_pkg.*  (所有子表)
│   └── --table paper_v2.*  (所有子表)
├── batch_b_import_qe_archive_baseline.py  # T12 apply 后的 baseline
└── batch_c_synthetic_fixtures.py          # 合成 fixtures (新功能)
```

### 安全约束

每个脚本必须遵守：

```python
# 强制 dev target
DEV_DB_HOST = "127.0.0.1"
DEV_DB_PORT = 5433
PROD_DB_PORT = 5432

assert os.environ.get("TDX_DB_DEV_HOST") == DEV_DB_HOST
assert int(os.environ.get("TDX_DB_DEV_PORT", 0)) == DEV_DB_PORT

# Production 仅 SELECT, dev 才允许 INSERT
prod_conn = connect(prod_url, options="-c default_transaction_read_only=on")
dev_conn = connect(dev_url)  # writeable
```

每个 INSERT 前 idempotent 清理：
```sql
DELETE FROM <table> WHERE source='dev_seed' OR <date_filter>;
```

## §4 时序

```
T0      战略 session 派发 Batch A 给 dw-foundation
T0+30m  Batch A done → drawer 通知 Codex
        Codex 启动 Phase 3 governance live smoke

(异步) Codex T12 review 通过
        dw-foundation apply T12 (22 张表)
        派发 Batch B (handler 在 Phase 3 测试中自然填充)
        派发 Batch C (合成 fixtures)
T0+1d   Phase 3 三方 smoke 全绿
        启动交叉检查 (见 cross_tool_review_protocol_20260510.md)
```

## §5 验证

每个 Batch 完成后跑：
```sql
-- 各 schema 行数
SELECT
  'paper_v2' AS schema, COUNT(*) FROM paper_v2.run
UNION ALL SELECT
  'strategy_pkg', COUNT(*) FROM strategy_pkg.package
UNION ALL SELECT
  'market', COUNT(*) FROM market.index_daily
UNION ALL SELECT
  'public.aistock_model_catalog', COUNT(*) FROM public.aistock_model_catalog;
```

通过 cross-tool drawer 报告导入数 + 验证 SELECT 结果。

## §6 清理

测试结束后（合并 main 之前），dev DB 数据可整体 truncate 或保留（dev DB 本身不是生产，无清理压力）。
