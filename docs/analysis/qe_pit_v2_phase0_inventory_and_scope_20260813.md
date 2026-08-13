# QE PIT v2 第一阶段（W0）基线、库存与窗口范围

- 扫描时间：2026-08-13 01:52 CST
- 实施分支：`feature/qe-pit-v2-phase0-inventory-20260813`
- 基线与当前 HEAD：`708d22395ecc8a6c5097a3441dd0e3ae133cc4a7`
- 主设计：`docs/architecture/qe_pit_v2_dataset_upgrade_master_f2_design_20260813.md`
- 本阶段性质：只读盘点与研发范围冻结；没有数据构建、导出、DDL/DML、activation、distribution、restart、cleanup 或进程控制

## 1. W0 结论

W0 已形成可供 W1、W2 使用的权威起点，但不代表 v2 已可激活：

1. 当前生产 QE 数据身份仍是 2026-06-30 v1，生产 Qlib Bin 三个固定摘要与源码常量完全一致。
2. 2026-07-31 的已验证全量数据仍位于独立 X 盘 candidate 路径，receipt 明确记录 `activation=not_requested`、生产写入和生产指针变化均为 0；本阶段未修改该目录。
3. 新控制库 `X:\AIstock_dataset_release_control\control.sqlite3` 已初始化但 release/candidate/catalog/lease 均为 0 行。现有 19 个 7 月 31 日目录没有被该控制库登记，不能据此推断重复副本可删除。
4. 数据库、源码和运行态均仍存在明确 v1 消费者。尤其有 1 个 `RUNNING` Paper run、1 个 `pending` QE loop 固定到 v1/2026-06-30；activation readiness 当前为 `BLOCKED_BY_PINNED_OR_UNMIGRATED_CONSUMERS`。
5. WSL2 与 node1 都能只读解析同一 2026-06-30 long-trend snapshot，但都报告 `qe_dataset_manifest_missing`，完整 dataset identity 为 `complete=false`。这是 W2/W6 必须关闭的 manifest 缺口，不允许用路径存在代替身份签收。
6. 6 月 30 日生产数据、7 月 31 日全量候选以及所有失败/样本候选当前统一分类为 `FULL_IMMUTABLE`。失败状态本身不构成无引用证明，自动删除始终为 false。

结构化事实分别冻结在：

- `tests/aistock_validation/pit_v2/source_freeze_receipt.json`
- `tests/aistock_validation/pit_v2/retention_inventory.json`
- `tests/aistock_validation/pit_v2/window_scope_receipt.json`
- `tests/aistock_validation/pit_v2/design_compliance_receipt.json`

## 2. 冻结身份

### 2.1 源码、profile 与日历

| 项目 | 冻结值 |
|---|---|
| `origin/main` / W0 HEAD | `708d22395ecc8a6c5097a3441dd0e3ae133cc4a7` |
| v1 config digest | `aef6dff42371e0891cec0d73e8495551119f4601a614e85acfb4f8f43aed139c` |
| v1 semantic profile digest | `fa75a5bec2251d9a39146789fd5c898caeb72c8400d02b4bb92037fdc8a100d5` |
| v2 config digest | `7ba9b5ced83bb6e377ee52f9c6585b15a2ae9020258e8f15e6b6614f087d372d` |
| v2 semantic profile digest | `6eb56920fbbf5485e0285831beee0612d3edd74abc0ed24742cc34cb4081706c` |
| resource policy digest | `58fb44371741e9b8f42de11e7a03c251bebf787f4ae2f8ebe61e1e6d998eefbc` |
| Qlib toolchain profile digest | `2e124eff8ad3eb414d6c86ddb4f9686648aeb2f7d945549d65763f73ff9a55bd` |
| 首次迁移 cutoff | `requested_cutoff=effective_cutoff=2026-07-31` |
| calendar encoding | 按 `cal_date` 升序的 `YYYY-MM-DD|0/1\n` |
| calendar range/count | `2018-08-01..2026-07-31`，2922 日历行、1940 交易日 |
| calendar digest | `2b826bf599fa88f0496d91a517ebfdca6ab4ea4575a2c5cac11b6131d2d6b745` |

`market.trading_calendar` 证明 2026-07-31 为交易日且为该冻结范围最后交易日。后续 W7 不得把首次迁移 cutoff 动态漂移到其他月份。

### 2.2 当前生产数据

| 组件 | 路径/身份 | 只读证据 |
|---|---|---|
| Qlib daily Bin | WSL `/home/lc999/data/qlib_bin`；snapshot `qlib_bin_st_pit_active_daily_candidate_20180801_20260630` | `all.txt=94c9d82d...05ca4`、`day.txt=6ab71db1...6031d`、`meta_export.json=66c5c070...eefb3`，与 `qe_dataset_contract.py` 完全相同；物理大小 405,619,378 bytes |
| H5/static | WSL `/home/lc999/data/factor_data_versions/qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2` | `/home/lc999/data/factor_data` 解析到该目录；物理大小 5,558,977,337 bytes |
| QE contract | `qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2` | 源码默认截止 2026-06-30；`.env` 没有覆盖 `QE_DATASET_CONTRACT_ID` |
| compute nodes | `wsl2-5080`、`rdagent-node1` | 两节点 long-trend manifest SHA 均为 `f281a206...6838`，但完整 dataset manifest 缺失 |

注意：生产 `meta_export.json` 中的旧 `pit_ensure` 记录来自 2026-07-06 的 rolling v1 状态，而正式 QE 使用的是固定的 `all.txt`/calendar/meta 摘要。该历史差异必须保留用于复现，不能原地重写成 v2。

### 2.3 2026-07-31 全量 candidate

权威待保留路径：

`X:\AIstock_dataset_candidates\backtest_dataset_candidates\20260731-qe_hmm_full_v1-full-edcfec4b-20260810T212744Z-candidate`

其 receipt 记录：

- `final_status=candidate_validated`
- `receipt_sha256=7c4de132b0d67df84be17cd6aeb4aca374ff182f514e46e4d9bfb01f8111c1bb`
- producer provenance 为 `DIRTY_SOURCE_TREE`：receipt 固定 commit `41504a205b9372a4e709587dc2310fd8143c6c6d`、source-tree digest `6be14ac112800937969b7bc5a342bbb40e19ca10f416fb99ac2045925d389155`，并列出 dirty paths；因此不能把 component validation PASS 等同于可直接发布的干净源码 release
- 125,032 个 component file entries；基于 receipt 中 `(component,path,size,sha256)` 排序 canonical JSON 的 W0 file-graph digest 为 `8b431fbcfd0ee60f49ce307740dffa24d10bc56e907bd6fe1dc14309a77d1893`
- receipt 声明 component 总大小 43,468,364,866 bytes
- 日线、分钟、121 列 H5/static、12 指数和 source overlays 均为 PASS
- 分钟补齐 1,096 个请求全部成功，TDX 选择 1,096，Tushare 选择 0
- `activation=not_requested`、`node1=not_requested`、所有生产写/删/指针变化/进程控制计数均为 0

该候选是 dirty producer provenance 下生成的 v1 PIT 语义 7 月 31 日候选，不是未来 v2 candidate。W1/W2 不得覆盖、原地修补或把它伪称为 v2；后续只能将其作为只读复用/对照来源，并由新控制面重新证明可复用分区。

## 3. v1 消费者库存

### 3.1 源码与记录扫描

对 `backend/`、`scripts/`、`configs/`、`.codex/`、`tests/` 执行固定模式扫描，共找到 67 个文件，已全部归类，无 `unknown`：

| 分类 | 文件数 | 处理原则 |
|---|---:|---|
| `production_consumer` | 12 | W3/W4/W5 迁移到共享 resolver 或 frozen manifest；不得继续模块常量投票 |
| `candidate_builder` | 8 | W1/W2 迁移；历史 v1 profile 本身保留用于 reproduction |
| `reproduction_only` | 17 | 保持不可变，不参与普通新任务 admission |
| `test_fixture` | 30 | 仅在对应窗口增加 v2/legacy 双向断言，不做全局替换 |

生产消费者集中于：QE contract/HMM evolution、Selection runtime、StockUniverse PIT service、StrategyPackage projection，以及 Advisory onboarding/historical-range/modeling。完整逐文件清单在 source freeze receipt 中。

### 3.2 PostgreSQL JSON 与运行态

所有查询均在显式 `SET TRANSACTION READ ONLY` 事务内执行。

| 权威源 | 总量/状态 | v1 或 2026-06-30 直接引用 | 分类 |
|---|---|---:|---|
| `strategy_pkg.package` 非 RETIRED | 7 | 4 个 manifest 使用 v1；其中 1 PAPER_ENABLED、3 BACKTEST_APPROVED | W5 immutable legacy package + re-certification decision |
| 当前 `paper_v2.runtime_profile_version` | 98 | 2 个当前版本显式使用 v1 | W4 versioned migration，不原地改旧版本 |
| `paper_v2.runtime_config_activation` ACTIVE | 96 | context 中 0 个显式 PIT identity | W4 implicit binding，需新版本显式化 |
| `paper_v2.run` RUNNING | 16 | 1 个显式 v1 | activation blocker；仅自然结束或独立授权处置 |
| `paper_v2.simulation_daily_run` INTRADAY_RUNNING | 1 | 0 个显式 PIT identity | W4 经 release/package 解析；不能据此推定安全切换 |
| `qe_evolution_tasks` running/paused | 3 | `strategy_params` 中 0 个显式引用 | W3 需按 loop/frozen release 解析 |
| `qe_evolution_loops` pending/running | 1 | `qe_20260810_224522_b0d9_Loop3` 固定 v1 和 2026-06-30 | activation blocker |
| `qe_experiments` created | 173 | 114 个 custom_params 使用 v1 | 历史模板/未执行记录，须分类后再决定 admission |

历史全表还存在：13 个 StrategyPackage manifest、1,271 个 terminal Paper session、152 个 Paper run、1,190 个 QE experiment 和 1,119 个 QE loop 含 v1。它们不是新生产 authority，但必须保留 reproduction identity，禁止全局字符串替换。

`market.stock_universe_pit_state` 当前只有 rolling v1 与固定 2026-06-30 v1 snapshot；计划中的三个 authority registry 表不存在，canonical v2 state/spans 也不存在。因此 W1 的 DEV migration 与 v2 builder 尚未执行。

## 4. 控制面与独立 gate

| Gate | W0 状态 | 证据/下一步 |
|---|---|---|
| DB migration | `NOT_EXECUTED` | W1 先提交 preflight/forward/rollback，并在现有 DEV DB 验证；生产 DDL/DML 未授权 |
| Backend runtime | `READY_LEGACY_SOURCE` | `/api/v1/health=ok`，运行 commit `7fb64a7e3df5e2d0ab1dbe5e0bb57badf82a5b21`；不等于当前 W0 HEAD |
| Dataset worker | `IDLE` | PID 1254472，code SHA `034ccd36...b799`，加载 v2 config digest `7ba9b5ce...372d`，`claim_id=null` |
| Control catalog | `EMPTY_INITIALIZED` | submissions/runs/releases/candidate registrations/leases 均 0 |
| WSL2 dataset identity | `INCOMPLETE` | 2026-06-30 long-trend identity可读，但缺 `qe_dataset_manifest.json` |
| node1 dataset identity | `INCOMPLETE` | 与 WSL2 同一 snapshot/manifest SHA，仍缺完整 dataset manifest |
| Activation | `BLOCKED` | v1 running Paper run、pending QE loop、未迁移 profile/package，以及 manifest 缺口 |
| Cleanup | `BLOCKED_NOT_AUTHORIZED` | 引用图未闭合；所有候选 `FULL_IMMUTABLE`；没有可执行删除目标 |

## 5. W1/W2 单写范围与交接

W1 必须先于 W2 合入。精确范围以 `window_scope_receipt.json` 为准；核心约束如下：

### W1 PIT Core/Registry

- 只修改 canonical binding/resolver、StockUniverse PIT builder/service、三件套 migration 及直接测试。
- migration 只创建 versions/pointer/events，登记 v1 `DEPLOYED_LEGACY_PENDING_MIGRATION` 并建立 generation 0 pointer；不得激活 v2。
- builder 必须实现 252 交易日暖机、历史 L/P 生命周期、ST/退市 as-of、exception ledger 和参数 digest；状态/跨度/generation 不一致时 fail closed。
- W1 不改 QE、Selection、Paper、StrategyPackage、Advisory 或 dataset release 消费者。

### W2 Dataset Release

- 只修改 v2 profile binding、PIT snapshot/dependency planner、流式 materializer、candidate validator、首次迁移白名单 plan、月更 CLI/Skill/Runbook 及直接测试。
- W2 必须消费 W1 最终公共 binding；不得自行再实现一个 PIT resolver。
- 所有测试仅使用 scratch/sample candidate；禁止运行真实全量、覆盖 6 月 30 日/7 月 31 日目录或写生产 DB。
- streaming/COW/资源硬上限继续有效；不得恢复无界 `frames`、全市场预分配矩阵或用更高并发掩盖性能问题。

W3/W4/W5 在 W1/W2 合入后才能并行；共同核心文件转交 W6 单写。W0 报告不授权任何后续窗口执行 DDL/DML、数据构建、分发、激活、重启或清理。

## 6. W0 验收与真实 blocker

W0 本身满足：基线 HEAD/profile/toolchain/calendar 已冻结；源码引用全部归类且 `unknown=0`；数据库 JSON、当前 runtime profile、active/unfinished session、控制目录、WSL2/node1 manifest 均已只读扫描；历史路径没有写入；当前 worker 为 IDLE 且没有 claim。

进入 W1 的真实 blocker 不是数据导出，而是独立的实现与授权边界：

1. W1 需要在新专用窗口按 receipt 精确 scope 实现 registry/resolver/builder，并先在现有 DEV DB 验证 migration。
2. 生产 DDL/DML 仍未授权，W1 合入也不代表生产 registry 已建立。
3. 任何 v2 activation 必须等待 W1～W8 全部闭合；现有 v1 运行会话、旧包/profile 和 compute-node manifest 缺口均不得绕过。
