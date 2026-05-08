# QE/HMM 两项热修复与治理方案补充设计

日期：2026-05-08
状态：设计交接稿，供 Codex App 多 Agent 后续实现使用
范围：QuantEvolver / QE、HMM QE 验证、Strategy Catalog、StrategyPackage、Paper v2 衔接、QE SOTA 治理

## 0. 结论摘要

本方案把 `docs/operations/qe_hmm_experiment_infra_issues_20260508.md` 中发现的两个 QE 侧问题纳入现有 SOTA/StrategyPackage/模型库治理方案，并明确热修复与长期治理分支的边界。

| 项目 | 类型 | 优先级 | 合入策略 | 生产可见性 | 核心约束 |
| --- | --- | --- | --- | --- | --- |
| backtest-only 并行 recorder 隔离 | Bug 修复 | P0 | 修复验证后优先合入 `main` | 后端代码合入并按用户授权重载后可见 | target recorder 必须 loop-local；不允许 symlink 写入 source `mlruns`；same realpath 必须 fail-fast |
| ScoreWeighted V2 容量参数化策略 | 策略资产修复 | P1；影响 HMM 解释时按 P0 处理 | 新策略文件/新策略 ID，可作为资产注册；如需平台代码则走分支测试合入 | DB/UI 可选择新策略；旧策略保留 | 不直接修改原 `score_weighted_topk_v2` 行为；必须标识版本号 |
| SOTA/StrategyPackage/模型库治理 | 架构增强 | P1/P2 分阶段 | 独立长期集成分支 | 默认不影响生产 | 生产 DB 仅 additive-only；现有生产记录只读；feature flag 默认关闭 |

两项热修复可以先于长期治理独立推进：

1. `backtest-only` 并行 recorder 隔离是明确 bug，修复和自动化验证通过后应优先合入 `main`。
2. 容量参数化策略以新策略资产方式上线，不回写旧策略，不改变历史实验解释；生产 UI/DB 中可选后，新 HMM/QE 实验才能区分“固定 5M 容量约束”与“按权重充分建仓”。
3. SOTA 殿堂、StrategyPackage 增强、模型库 seed/reproducibility 等长期工作继续使用独立分支，不与热修复混合。

## 1. 已确认事实和代码证据

源问题记录：`docs/operations/qe_hmm_experiment_infra_issues_20260508.md`。

该文档记录了两个 QE 侧问题：

- `qe_20260508_120507_d279` 的 backtest-only 并行 loop 失败，错误为 `ValueError: Metric 'Rank IC' is malformed. No data found.`，并出现 target loop `mlruns` symlink 到 source task `mlruns` 的证据。
- 多个 HMM QE 验证任务中最终现金约 60M，50 只持仓乘以固定 5M 上限约等于 250M 投入，说明固定单票/单笔 5M cap 抑制了 HMM 仓位权重效果。

后续实现必须优先阅读以下真实代码入口：

| 主题 | 文件 | 关键点 |
| --- | --- | --- |
| backtest-only runner | `scripts/qrun_limit_minute.py` | `_run_backtest_only()` 会从当前 `mlruns` 读模型，并用 `R.start()` 创建新 recorder；若 `mlruns` 是 source symlink，则读写混在同一 file store |
| 普通 runner tracking URI | `scripts/qrun_limit.py` | 默认 `MLFLOW_TRACKING_URI = cwd/mlruns`，说明 cwd 下 `mlruns` 语义必须明确 |
| QE strategy 参数白名单 | `backend/services/quantevolver/config_composer.py` | ScoreWeighted 白名单已包含 `max_single_order_value`、`max_weight`、`max_position_ratio`、`lot_size` |
| Paper/StrategyPackage 回测合同 | `backend/services/strategy_package/backtest_contract.py` | 当前 `SCORE_WEIGHTED_DEFAULTS.max_single_order_value = 5_000_000.0`，旧合同默认会保留固定 5M cap |
| Paper 目标仓位实现 | `backend/services/strategy_package/runtime.py` | `target_value = min(total_equity * weight, params["max_single_order_value"])`，容量参数会直接影响模拟盘/未来实盘目标仓位 |
| 当前 V2 策略注册脚本 | `scripts/register_score_weighted_strategy_v2.py` | `DEFAULT_KWARGS` 未显式写入 `max_single_order_value`；新策略应另建文件和 strategy_id |
| QE UI 策略选择 | `frontend/src/app/quantevolver/evolution/page.tsx` | 当前选择 catalog strategy 后只显示 `initial_cash`，后续需让新策略参数 schema 可见 |

必须同时遵守：

- `docs/codex_project_memory.md`：所有新开发从独立 worktree/branch 开始；`F:\Dev\AIstock` 是生产 root，不做正常开发。
- `docs/standards/aistock_development_standard_v1.1_20260504.md`：不干扰生产 `8001`；交易/QE/HMM 不允许静默降级；新增 DB 表/字段必须有 comment；高风险功能必须设计 L0-L5 测试。
- `tests/aistock_validation/modules/qe.md`：backtest-only 不应依赖直接读取 worker workspace；模型参数应通过 API/payload 流转。

## 2. 热修复一：backtest-only 并行 recorder 隔离

backtest-only 的正确语义是：复用已训练模型权重或 `params.pkl`，但为本次回测创建新的、独立的 recorder 和指标产物。

当前问题的本质是同一 source model 的 `mlruns` 目录可能被多个 target loop 通过 symlink 复用：

```text
source model recorder: 只读读取 params/model
target backtest recorder: 写入 pred、label、sig_analysis、portfolio_analysis、metrics
多个 target loop: 并行创建/读取 MLflow file-store metric files
```

如果 target loop 的 `MLFLOW_TRACKING_URI` 解析到 source `mlruns`，target loop 会把新 recorder/metrics 写回 source file store。MLflow file store 对这种并行写读没有强隔离，容易出现空 metric 文件或部分写入，最终触发 malformed metric。

### 2.1 目标合同

后续实现必须满足：

```text
source_recorder = read-only model source
target_recorder = loop-local write target
source_mlruns_realpath != target_mlruns_realpath
target_mlruns 不是指向 source_mlruns 的 symlink
target_mlruns 不在 source_mlruns 内部
target loop 启动前写入 isolation manifest
target loop 完成后 qe_current_recorder 指向 target recorder，不指向 source recorder
```

禁止：

- 同节点 backtest-only 继续把 target `LoopX/mlruns` symlink 到 source `LoopY/mlruns`。
- 只靠 Qlib `R.start()` “创建新 recorder”但仍使用同一个 source `MLFLOW_TRACKING_URI`。
- 捕获 malformed metric 后直接重试而不先证明 target recorder 已隔离。
- 为了通过测试伪造空 metrics 或把 loop 标成 complete。

### 2.2 推荐实现形态

推荐把 backtest-only 拆成两个物理目录：

```text
LoopX/
  source_model/
    params.pkl
    source_recorder_ref.json
  mlruns/                      # target recorder，本 loop 独占，非 symlink
  qe_recorder_isolation.json
  qe_current_recorder.json
```

如果为了兼容旧逻辑仍需要读取 loose `params.pkl`，runner 应按以下顺序查找：

1. `QE_BACKTEST_SOURCE_PARAMS_DIR` 或 `source_model/params.pkl`。
2. 本地 payload 解压目录中的 `params.pkl`。
3. 仅在非 backtest-only 或显式 legacy 诊断模式下读取 `mlruns/**/params.pkl`。

在 backtest-only 模式下，`Path("mlruns")` 的用途只能是 target recorder；它不能同时作为 source model 目录。

### 2.3 fail-fast 检查

runner 或 worker API 在启动前必须执行：

```text
source_mlruns_realpath = realpath(source_mlruns) if source_mlruns else null
target_mlruns_realpath = realpath(loop_dir / "mlruns") after parent creation
if target_mlruns is symlink: fail
if source_mlruns and source_mlruns_realpath == target_mlruns_realpath: fail
if source_mlruns and target_mlruns_realpath is under source_mlruns_realpath: fail
if source params missing/unreadable: fail
```

建议错误码：

```text
QE_BACKTEST_RECORDER_NOT_ISOLATED
QE_BACKTEST_SOURCE_PARAMS_MISSING
QE_BACKTEST_TARGET_MLRUNS_IS_SYMLINK
QE_BACKTEST_SOURCE_TARGET_REALPATH_COLLISION
```

### 2.4 需要持久化的审计字段

建议写入 `qe_recorder_isolation.json` 并同步到 loop metrics/config 摘要：

```json
{
  "schema_version": "qe_backtest_recorder_isolation_v1",
  "mode": "backtest_only",
  "source_task_id": "...",
  "source_loop_id": "Loop1",
  "source_recorder_id": "...",
  "source_mlruns_realpath": "...",
  "target_task_id": "...",
  "target_loop_id": "Loop4",
  "target_mlruns_realpath": "...",
  "target_mlruns_is_symlink": false,
  "parallel_group_id": "...",
  "recorder_isolation_status": "passed",
  "created_at": "..."
}
```

### 2.5 验收标准

- 同 source + 两个 target loop 并行时 target realpath 不同，且都不是 symlink。
- target `mlruns` 是 symlink 时 fail-fast。
- source 与 target realpath 相同时 fail-fast。
- 用同一 source model 启动两个 backtest-only target loop，两个 target recorder 均写出独立 `qe_current_recorder.json` 和非空 metrics。
- full train 模式仍使用 loop-local `mlruns`，不受影响。
- 不改 source `mlruns` mtime/文件列表；不修改 HMM snapshot、模型权重、StrategyPackage frozen manifest、Paper ledger。

## 3. 热修复二：新建容量参数化 ScoreWeighted V2 策略资产

当前 ScoreWeighted V2/HMM 验证中，固定 5M `max_single_order_value` 会覆盖 `max_weight` 的预期效果：

```text
NAV = 300M
max_weight = 5%
理论 top position = 15M
固定 max_single_order_value = 5M
实际 top position <= 5M
```

这会导致最终现金偏高，HMM 对权重和分散度的作用被低估。该问题影响“实验解释”，不代表 HMM 模型失效。

### 3.1 设计决策

不得直接修改现有 `score_weighted_topk_v2` 的默认行为：

- 历史 QE 实验、StrategyPackage、Paper v2 回放都可能依赖旧策略含义。
- 直接修改旧策略会改变历史 backtest contract，造成“同一 strategy_id 不同语义”。
- 量化机构通常把策略代码、参数 schema、默认参数、回测合同视为版本化资产；任何行为变化都产生新版本。

应新增策略资产：

```text
strategy_id: score_weighted_topk_v2_capacity_v1
class_name: ScoreWeightedTopkStrategyV2CapacityV1
source_file: score_weighted_strategy_v2_capacity_v1.py
display_name: ScoreWeightedTopk V2 Capacity Parameterized v1
family: score_weighted_topk_v2
capacity_profile: capacity_parameterized
legacy_relation: derived_from score_weighted_topk_v2
```

### 3.2 默认参数建议

```yaml
strategy_params:
  topk: 50
  n_drop: 5
  weight_method: softmax
  temperature: 1.0
  score_clip_quantile: 0.0
  max_weight: 0.05
  min_weight: 0.005
  max_position_ratio: 0.95
  max_single_order_value: 1000000000.0
  lot_size: 100
  enable_dynamic_ndrop: true
  max_n_drop: 5
  min_n_drop: 0
  threshold_method: adaptive
  min_improvement: 0.01
  adaptive_multiplier: 0.5
  threshold_floor: 0.005
  hold_thresh: 2
  only_tradable: true
  forbid_all_trade_at_limit: false
```

`max_single_order_value` 默认设为足够大，是为了让 `max_weight` 和 `max_position_ratio` 成为主要容量约束；不是鼓励实盘单票无限下单。进入 Paper v2 或未来实盘前仍必须由 StrategyPackage runtime variant / execution policy 做资金规模和流动性审查。

### 3.3 新策略文件要求

策略修改必须新建文件，不能覆盖旧文件：

```text
score_weighted_strategy_v2.py                  # 保持不变，legacy capacity constrained
score_weighted_strategy_v2_capacity_v1.py       # 新增，capacity parameterized
```

如果 suspend/risk wrapper 需要适配，也应避免改变旧 wrapper 的默认分派语义。推荐做法之一：

```text
qe_suspend_filter_score_weighted_strategy.py                # 旧行为保持
qe_suspend_filter_score_weighted_strategy_capacity_v1.py     # 新 wrapper 或显式 class 映射
```

如果实现者选择只扩展现有 wrapper 的 class mapping，也必须证明旧 strategy_id 生成的 class/module/kwargs 完全不变。

### 3.4 DB/UI 可选要求

新策略应以新记录进入 strategy catalog，而不是覆盖旧记录：

```text
aistock_strategy_catalog.strategy_id = score_weighted_topk_v2_capacity_v1
aistock_strategy_catalog.source_code = 新策略文件内容
aistock_strategy_catalog.param_schema 包含 max_single_order_value/max_weight/max_position_ratio
aistock_strategy_catalog.default_config 包含 max_single_order_value=1000000000.0
```

UI 要求：

- QE 新建、自定义演进、strategy_evo loop 都能选择新 strategy_id。
- 选择新策略后 UI 能显示并编辑 `max_single_order_value`、`max_weight`、`max_position_ratio`。
- 旧 `score_weighted_topk_v2` 应显示 `legacy_5m_cap` 或等价提示，避免用户误解。
- 生成任务/loop 时必须持久化 requested config 和 effective config。

### 3.5 StrategyPackage/Paper v2 衔接

如果新策略进入 SOTA/StrategyPackage：

- StrategyPackage manifest 必须锁定新 `strategy_id` 和参数。
- `backend/services/strategy_package/backtest_contract.py` 需要识别新 strategy_id，并把它归入 `score_weighted_v2` family。
- 不得把旧 package 的 manifest 自动迁移成新策略。
- Paper v2 runtime 使用 frozen manifest 内的 `max_single_order_value`；若缺失则旧包继续走 5M legacy default，新包走新 default。

### 3.6 容量审计要求

所有使用 ScoreWeighted 新旧策略的 QE 结果都应逐步补充 capacity audit：

```text
final_cash
final_stock_count
avg_holding_count
max_holding_count
gross_exposure
cash_idle_ratio
position_value_p50
position_value_p95
position_value_max
target_weight_vs_actual_weight_summary
clipped_by_max_single_order_value_count
clipped_by_max_weight_count
clipped_by_max_position_ratio_count
turnover
cost_drag
```

该审计不一定阻塞热修复上线，但必须进入后续治理分支的测试矩阵和 SOTA 评审表。

## 4. 与 SOTA/StrategyPackage/模型库治理方案的整合

本补充方案不改变 `docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md` 的主结论：

```text
QE = 发现新组合
SOTA 殿堂 = 人工评审和晋级工作台
StrategyPackage = Selection/Paper/未来实盘唯一标准策略资产
模型库 = 模型规格、trial、artifact、seed 和演进历史
数仓 = 持久分析事实和大规模实验数据
QE runtime DB = 短期运行态和 UI 操作态，可清理
```

本次新增的两项要求应落入该治理链路：

- backtest-only recorder 隔离：属于 QE runtime 正确性；是所有 backtest-only retest、SOTA 原始配置复测、runtime variant 回测的基础设施前置条件。
- 容量参数化策略：属于 strategy asset versioning；进入 SOTA 前必须明确使用 legacy capacity 还是 capacity-parameterized 策略。

SOTA 评审页应展示：

```text
backtest_mode: full_train | backtest_only | rerun
recorder_isolation_status: passed | failed | legacy_unknown
strategy_capacity_profile: legacy_5m_cap | capacity_parameterized | unknown
max_single_order_value
max_weight
max_position_ratio
capacity_audit_status
```

如果 `recorder_isolation_status != passed`，该 loop 不能作为晋级资产进入 Paper-ready。

如果 `strategy_capacity_profile = legacy_5m_cap`，可以进入 SOTA 评审，但 UI 必须提示：结果代表“容量受限策略”，不能解释为 HMM 权重效果已充分验证。

SOTA 原始配置复测必须分清：

- 复测旧实验：保留旧策略和 5M cap，作为历史结果复现。
- 复测新容量策略：使用新 strategy_id 和显式容量参数，作为新资产验证。
- 不允许在“原始配置复测”中偷偷把旧策略替换成新策略。

若用户想比较两者，应创建 runtime variant 或新 QE loop：

```text
core alpha: same model/factor/weight
variant A: score_weighted_topk_v2 legacy_5m_cap
variant B: score_weighted_topk_v2_capacity_v1 max_single_order_value=1e9
```

## 5. 分支和生产数据库策略

建议拆成两个短分支：

```text
codex/qe-backtest-recorder-isolation-20260508
codex/qe-score-weighted-capacity-v1-20260508
```

两个短分支可以并行开发，但合入顺序建议：

1. recorder isolation bugfix：通过 L0-L3 后优先合入 `main`。
2. strategy capacity asset：如果只做 DB/asset 注册，可按资产操作上线；如果修改 backend/frontend，必须走测试后合入 `main`。

长期治理继续使用：

```text
codex/qe-governance-integration-20260508
```

该分支吸收 seed contract、模型库、SOTA 手工晋级、StrategyPackage 增强、复测模式、runtime variant 等工作，不应阻塞两个热修复。

如果没有单独 dev DB，长期分支可以读生产 DB，但写入必须满足：

- 新表放在独立 schema，例如 `model_registry` 或 `strategy_pkg`，不落 `public`。
- 新字段必须 nullable 或有 default。
- 不 drop table/column，不 alter column type。
- 不对现有表加 cascade delete 外键。
- 开发分支只能写 `dev/test` 前缀 ID。
- 现有生产记录只读，除非用户明确授权一次性资产注册或 legacy 标记。
- 新表和字段必须有 PostgreSQL comment。

## 6. 自动化流水线要求

后续实现必须同步补充测试，不能只依赖人工 QE 跑通。最低要求：

- L0：`git diff --check`、guardrail scan、禁止 source/target recorder realpath collision 的静态/单测检查。
- L1：runner/worker path isolation unit tests，strategy default/schema unit tests。
- L2：backend service + config composer integration tests。
- L3：dev port API smoke + QE 小样本 business oracle。
- L4：真实 worker/backtest-only 并行小规模验证。
- L5：长周期 HMM/QE 验证，仅在用户明确授权后运行。

详细测试矩阵见：`tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`。

## 7. Codex App 多 Agent 交接入口

Codex App 后续多 Agent 开发应以以下文档为入口：

- 本设计文档：`docs/architecture/qe_hmm_hotfix_and_governance_detailed_design_20260508.md`
- 多 Agent 交接包：`docs/operations/qe_hmm_hotfix_multi_agent_handoff_20260508.md`
- 测试矩阵：`tests/aistock_validation/modules/qe_hmm_hotfix_and_governance.md`
- 源问题记录：`docs/operations/qe_hmm_experiment_infra_issues_20260508.md`
- 主治理方案：`docs/architecture/qe_sota_strategy_package_asset_governance_design_20260508.md`

Agent 必须先阅读 `docs/codex_project_memory.md` 和 `docs/standards/aistock_development_standard_v1.1_20260504.md`，再开始任何代码修改。

## 8. 不做事项

- 不直接清理历史 QE workspace 或历史 `mlruns`。
- 不修改旧 `score_weighted_topk_v2` 的默认语义。
- 不把旧 StrategyPackage manifest 自动改成新策略。
- 不把所有 QE loop 自动加入 SOTA。
- 不重启生产 `8001`。
- 不在长期治理分支中修改生产历史记录。
