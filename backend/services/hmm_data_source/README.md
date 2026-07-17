# HMM 数据源抽象层

> Phase 0 hardening 与 Prediction Store 零副本收尾<br>
> 状态：代码契约、专用 CI、Prediction Store 零副本和受控只读 DB/PIT sector integration 均已验收；Phase 1 implementation unlocked。

## 边界

该模块只为 HMM 独立研究生产线提供两类输入：

- `BacktestDataSource`：优先只读复用 Prediction Store 中指定 QE task/loop 的
  `pred.pkl`、`label.pkl`；缺失时才回退 QE workspace。
- `RealtimeDataSource`：读取 canonical market 表和显式注册的研究候选预测 provider。

允许只读访问 QE workspace、`qe_evolution_tasks`、`market.kline_daily_raw`、
`market.trading_calendar`、`market.sw_index_member`。禁止修改或下载
`model_train_configs`、`model_train_snapshots`、`strategy_packages`、`paper_v2.*`
配置，禁止把分析结果接入交易决策。

## 真实运行契约

### 回测数据源

```python
from datetime import date

from backend.services.hmm_data_source import BacktestDataSource

async with BacktestDataSource(
    base_loop_ref="qe_20260502_131502_9b54/Loop1",
    cache_dir="tmp/hmm_evolution_cache/",
    artifact_source_preference="prediction_store_first",
    label_horizon_days=10,
) as source:
    predictions = await source.get_predictions(
        date(2024, 7, 1),
        date(2024, 7, 5),
    )
```

运行链路：

1. 将 `<task_id>/LoopN` 映射为 Prediction Store run key `<task_id>_LN`。
2. 通过 `ModelStoreService.resolve_archive_manifest()` 校验 manifest identity、SHA256、
   size、row count、`collection_status=available` 和 `parser_status=parsed`。
3. 命中时直接读取 content-addressed blob，不复制到 HMM cache；source receipt 标记
   `source=prediction_store`、`zero_copy=true`。
4. manifest 缺失或缺少目标 artifact 时，`prediction_store_first` 才进入 workspace
   fallback；`prediction_store_only` 明确失败。
5. manifest/物理 blob 已存在但损坏时 fail loud，禁止用 workspace fallback 掩盖完整性问题。
6. workspace fallback 从 `qe_evolution_tasks.node_id` 解析权威 compute node，通过
   `QEWorkspaceClient` 读取 recorder/可信远端 manifest，只下载白名单 artifact，并在
   SHA/size/row count 全部匹配后原子发布到本地缓存。
7. 内部创建的 client 由 data source 关闭，外部注入 client 由调用方管理。

Prediction Store 与 workspace 两条链均只读。任何已存在的损坏 manifest、SHA/size/row
count 不一致或 provenance 不可信均 fail closed，不使用另一来源或旧缓存伪装成功。

### 实时数据源

```python
from datetime import date

from backend.services.hmm_data_source.realtime_source import RealtimeDataSource

source = RealtimeDataSource(
    candidate_id="hmm-candidate-id",
    as_of_date=date(2026, 7, 16),
    prediction_provider=registered_candidate_provider,
)
```

- `candidate_id` 必须显式提供；`snapshot_id="latest"` 不可用于预测身份解析。
- 默认 repository 使用同步 `backend.db.pg_pool.get_conn()`，异步 service 通过线程执行器调用。
- 最新可用日取 `as_of_date` 之前的完成交易日，不使用自然日减法或 `CURRENT_DATE`。
- 行情字段为 `ts_code`、`close_li`；行业映射使用 `market.sw_index_member` 的 PIT 区间。
- 仓库没有隐含的 `model_train_predictions` 表；未配置 provider 时预测接口明确失败。

## Workspace fallback cache

Prediction Store 命中时不创建 artifact 副本。仅 workspace fallback 使用
`sha256(loop_ref)` 隔离目录，artifact 使用独立 manifest：

```text
tmp/hmm_evolution_cache/
└── <loop-ref-sha256>/
    ├── pred.pkl
    ├── pred.pkl.manifest.json
    ├── label.pkl
    └── label.pkl.manifest.json
```

缓存保证：

- loop/artifact 名称校验与 resolved-root containment；
- QE remote provenance 默认必需，`test_fixture` 仅显式测试模式可用；
- artifact/manifest 原子替换；线程锁 + 独占 lock-file 跨进程互斥；
- 强制 SHA、size、row count、TTL 校验；
- 单 artifact 和总缓存容量上限，超限淘汰最旧 entry；
- clear/淘汰前扫描 reparse point，不递归穿透 junction/symlink。

可通过 `DataSourceConfig` 或 `BacktestDataSource` 参数配置
`artifact_source_preference`（`prediction_store_first` / `prediction_store_only` /
`workspace_only`）、`label_horizon_days`、`max_artifact_bytes`、`max_cache_bytes`、
`cache_ttl_seconds`。

## 验证

### 专用 CI（默认、无外部依赖）

```bash
rtk python -m nox -s hmm_data_source_backend
```

该 session 运行 `backend/tests/hmm_data_source/` 的非 integration 测试，生成：

- `tmp/validation/hmm_data_source/coverage.xml`
- `tmp/validation/hmm_data_source/junit.xml`（含最慢 10 项时长）

CI change classifier 会把以下路径路由到该 session：

- `backend/services/hmm_data_source/**`
- `backend/tests/hmm_data_source/**`

覆盖率门槛是可审计的最低线（70% branch-aware），不是“功能已完整验收”的替代品。

### 受控只读 integration（不在普通 PR 自动执行）

```powershell
$env:AISTOCK_HMM_READONLY_INTEGRATION = "1"
$env:HMM_TEST_QE_LOOP_REF = "<authoritative-task-id>/<LoopN>"
$env:HMM_TEST_AS_OF_DATE = "YYYY-MM-DD"
rtk python -m nox -s hmm_data_source_readonly_integration
```

该 session 绑定 canonical Prediction Store，使用 `prediction_store_only`，并给 DB
repository 注入 `REPEATABLE READ + transaction_read_only=on` 的连接 factory。它只执行
SELECT 和 QE artifact 读取，不运行服务、不执行 INSERT/UPDATE/DELETE/DDL。缺少显式开关
或可重放坐标时拒绝运行；不得使用硬编码旧 task 作为通过证据。

## 测试目录

```text
backend/tests/hmm_data_source/
├── conftest.py
├── test_backtest_source.py
├── test_realtime_source.py
├── test_cache_manager.py
├── test_integration.py
└── test_isolation_constraints.py
```

异步测试由本目录 `conftest.py` 使用 `asyncio.run()` 驱动；integration 默认跳过。

## Phase 0 外部验收状态

- 普通单元/contract CI 通过不等于真实 QE/DB smoke 已通过。
- 2026-07-17 已使用 `qe_20260706_013235_bbd4/Loop8`、
  `as_of_date=2026-07-17` 完成受控 integration：4 passed；DB completed date
  `2026-07-16`；PIT mapping 5,864 symbols / 131 L2 codes；transaction read-only。
- Store prediction 2,260,161 rows，`zero_copy=true`，未创建 HMM cache 副本；该任务为
  h20，仅作为 prediction/data-source receipt，不作为 10 日 label 证据。
- 若 QE producer 尚未发布可信 artifact manifest，cold download 应失败；不得回退到无 manifest pickle。
- 任何性能结论必须引用 JUnit durations、输入行数和冷/热缓存条件，不在文档中写未经测量的固定秒数。

## Phase 0 readiness helper

`scripts/deploy_hmm_data_source.py` 已退役原有建用户、GRANT、建 schema 和修改
`.gitignore` 的行为。默认命令只输出 plan；唯一允许的 apply 动作是幂等创建 repo
`tmp/` 下的 cache 目录：

```powershell
rtk python scripts/deploy_hmm_data_source.py plan --json
rtk python scripts/deploy_hmm_data_source.py verify --json
rtk python scripts/deploy_hmm_data_source.py bootstrap-cache `
  --apply --confirm phase0-cache-only --json
```

Phase 0 不需要数据库 DDL。Phase 1 的 `hmm_evolution.*` / `hmm_risk.*` 必须由独立、
带 PostgreSQL `COMMENT ON`、幂等复跑证据和 production DDL gate 的 Python bootstrap
交付，不得恢复本 helper 中的角色/权限/DDL 逻辑。
