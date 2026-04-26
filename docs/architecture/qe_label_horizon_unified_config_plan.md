# QE 单 Alpha label_horizon 统一配置层实施方案

> 日期: 2026-04-26
> 状态: 阶段 1 已实施并完成 5d UI 真实回测验证
> 范围: QE 单 Alpha 优先；多 Alpha、动态权重、分布式性能优化后置
> 目标: 在不改变当前 1d 行为的前提下，新增 `label_horizon=3/5/10d` 训练标签能力，并通过统一配置层覆盖所有 QE 单 Alpha 场景。

---

## 0. 2026-04-26 实施与验证记录

阶段 1 已在本地 `main` 工作树完成实现并完成 8012/3012 独立端口 UI 验证。

- 后端端口：`127.0.0.1:8012`；前端端口：`127.0.0.1:3012`。
- 验证模型：`LGBModel Golden Seed (validated 2026-03-14)`，`model_id=__seed_LGBModel_golden_v1__`。
- DB 迁移：已实际执行 `qe_evolution_tasks.label_horizon` 字段与 `1/3/5/10` 约束迁移，并通过 schema preflight。
- Playwright smoke：`QE_E2E_RUN_BACKTEST=0`，`QE_E2E_HORIZONS=1,5`，通过 UI 生成 1d/5d 配置并验证非法 `label_horizon=2` 返回 400。
- Playwright 真实回测：`QE_E2E_RUN_BACKTEST=1`，`QE_E2E_HORIZONS=5`，因子 `neg_size_adjusted_turnover`，完成 UI 创建、提交、轮询、读取 enhanced metrics 全流程。
- 真实回测结果：实验 `qe_20260426_202531`，`label_horizon=5`，`hold_thresh=5`，`Rank IC=0.06175099917762745`，`Rank ICIR=0.7346773137677719`，`annualized_return=0.35135033809422517`，`max_drawdown=-0.09052216643777933`，`information_ratio=1.5585249745126368`。
- 测试期修复：3012 CORS、搜索请求 stale overwrite、Enter 搜索 stale state、非法 horizon 500 转 400、UI payload 显式持久化 `hold_thresh`、Playwright 指标键后缀识别。

阶段 1 的功能链路已走通；研究验收仍需继续补齐同一因子/模型/data_split 下 `1/3/5/10d` 全量对照，比较 IC、收益、回撤、换手与有效样本数后，才能决定是否进入 horizon-aware multi-alpha。

---

## 1. 背景与目标

当前 QE 单 Alpha 训练标签只支持 `label_type=close/open/vwap`，预测期限固定为当前 1d 口径：

```text
close: Ref($close, -2) / Ref($close, -1) - 1
open:  Ref($open,  -2) / Ref($open,  -1) - 1
vwap:  Ref($vwap,  -2) / Ref($vwap,  -1) - 1
```

这相当于在 T 日用特征，预测 T+1 到 T+2 的可执行 forward return，避免把 T 日收盘后才知道的信息用于 T 日交易决策。

已有因子在 3/5/10 日 RankIC 远高于 1 日 RankIC，说明当前固定 1d 标签可能错配因子的有效信息半衰期。多 Alpha 架构要真正提升收益、回撤和 IC，必须先在单 Alpha 架构下验证不同训练标签期限是否确实能提高 OOS RankIC/收益质量，而不是先把未验证的动态权重、多持仓周期、多节点并行全部叠加。

本方案新增 `label_horizon`，但第一阶段只服务 QE 单 Alpha：

- 保持现有 1d 模式完全可用，旧记录、旧请求不需要迁移。
- 新增 3/5/10d 训练标签分支，按统一配置层注入，不在各调用路径重复拼参数。
- 覆盖 QE 单次实验、自动演进、自定义演进、从 QE Loop 开始新实验、retry/backtest-only 等所有单 Alpha 场景。
- 禁止跳过训练的场景修改 `label_horizon`，从根源避免“复用旧模型但声称新训练标签”的错误。

---

## 2. 已确认设计决策

| 问题 | 已确认选择 | 设计含义 |
|---|---|---|
| 存储方式 | 方案 B | `ExperimentConfig.label_horizon` 作为统一配置字段；单次实验落在 `qe_experiments.custom_params.label_horizon`；自动演进任务在 `qe_evolution_tasks.label_horizon` 增加任务级字段并作为权威来源。 |
| 标签公式 | 方案 A | 继续采用 T+1 可执行口径：`Ref($field, -(h+1)) / Ref($field, -1) - 1`。 |
| 支持 horizon | 方案 A | 第一阶段只允许 `1/3/5/10`，不开放任意整数。 |
| 自动演进是否允许 Agent 改 horizon | 方案 A | 创建任务时固定；后续 Loop 不允许 reviewer/agent 修改。 |
| 从 Loop 开新实验 | 方案 A | 默认继承源 Loop；普通全量重训 fork 可手动覆盖；backtest-only/strategy-evo 必须锁定源模型 horizon。 |
| backtest-only/策略演进 | 方案 A | 如果跳过训练，禁止修改 `label_horizon`；发现不一致直接 fail-fast。 |
| Git 分支 | 方案 A | 不创建新 Git 分支，不维护两份代码；“新增分支”只指运行时代码中的 `label_horizon != 1` 新逻辑分支。 |

---

## 3. 实施硬性约束

本阶段的验收目标不是“程序流程能跑通”，而是证明 `label_horizon` 能否带来更高 RankICIR、更好的收益/回撤质量，并且所有关键业务路径都能由 UI 完成。以下约束必须写入开发任务，不允许降级：

1. **数据库结构必须实际完成变更**
   - 不能只提交 SQL migration 或 Python migration 脚本。
   - 实施阶段必须对当前开发/验证数据库实际执行迁移。
   - 应同时更新 schema bootstrap/init 逻辑，保证新环境初始化后也包含必要字段和约束。
   - 后端启动、API 提交、Playwright E2E 前必须做 schema preflight；缺字段或约束缺失时直接 fail-fast，不能静默改用 1d。

2. **UI 必须提供完整操作入口**
   - 不能只完成后端参数，前端不提供入口。
   - 用户必须能在 UI 中自助选择因子、模型、训练标签期限、策略持仓参数、数据切分并提交回测。
   - 单次实验、自动演进、自定义演进、Loop fork、backtest-only 锁定展示都必须在 UI 上可见、可确认。

3. **严禁静默兜底影响业务逻辑**
   - 不允许非法 `label_horizon` 静默改成 1d。
   - 不允许找不到 task/loop/source horizon 时静默继承默认值。
   - 不允许 backtest-only horizon 不一致时静默忽略用户输入。
   - 不允许数据不足、未来 label 不可计算时继续提交。
   - 所有无法确认的业务关键状态必须 400/409/fail-fast，并给出用户可理解错误。

4. **收益提升是最终验收目标**
   - 回测能完成只是最低功能门槛。
   - 阶段 1 验证必须比较 1d/3d/5d/10d 的 RankIC mean、RankICIR、收益、最大回撤、换手率、有效 IC 天数。
   - 如果 3/5/10d 不能改善收益/回撤或稳定性，不能以“功能已跑通”为完成结论。

5. **测试期 bug 修复必须可追踪**
   - 测试期间发现并修复的每个业务 bug 必须单独提交到 GitHub `main`，不能混入无关文件。
   - 每次提交前必须明确列出 staged 文件，禁止 `git add .`。
   - 如果 Playwright 或真实回测发现新 bug，先修复并复测，再提交并推送，保证后续回溯能定位到具体修复。

---

## 4. 研究与实践依据

本需求不是单纯为了“跑通”，而是为了提升可盈利性。`label_horizon` 有实践意义，原因是训练标签期限会改变模型学到的 Alpha 结构。

参考资料：

- Robeco / Journal of Financial Data Science: *The Term Structure of Machine Learning Alpha*，SSRN 4474637，https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4474637 。核心启示：同一模型、同一特征集，只改变训练标签期限，会学到不同的因子暴露；训练期限应与 Alpha 半衰期和交易周期协调。
- arXiv 2602.03395: *The Label Horizon Paradox*，https://arxiv.org/abs/2602.03395 。核心启示：最优训练标签期限不一定等于目标持仓期限，A 股场景中中等 horizon 标签可能比最短标签更稳健。
- Kakushadze & Yu, arXiv 1603.05937: *How to Combine a Billion Alphas*，https://arxiv.org/abs/1603.05937 。核心启示：组合 Alpha 前应先理解各 Alpha 的 horizon、相关性、有效性，不能把弱信号机械平均。
- 项目内研究文档：`reports/multi_horizon_factor_investing_research_20260419.md` 和 `reports/multi_horizon_architecture_analysis_20260419.md` 已经提出 `3d/5d/10d` 标签公式和先单 Alpha 验证的路线。

结论：先在单 Alpha 下加入 `label_horizon` 是合理优先级；动态权重、多持仓周期、多 Alpha 合成要以后基于已验证的单 Alpha horizon 结果再做。

---

## 5. 当前代码现状与阻塞点

### 5.1 已有统一配置层

可复用的现有架构：

- `backend/services/quantevolver/experiment_config.py`
  - `ExperimentConfig` 是 QE 配置层对象。
  - `build_custom_params()` 是当前唯一参数注入点。
- `backend/services/quantevolver/experiment_config_builders.py`
  - 已按路径构造 `ExperimentConfig`：单次实验、自动演进、策略演进、自定义演进、retry、多 Alpha。
- `backend/services/quantevolver/executors/backtest.py`
  - `BacktestExecutor.submit()` 调 `config.build_custom_params()` 后再调用 `ConfigComposer.compose_experiment_in_memory()`。
- `backend/services/quantevolver/config_composer.py`
  - 负责最终生成 Qlib 配置，当前在 `label_type` 处生成固定 1d 公式。

这些是必须继续沿用的“允许 API/模式”，不应再新建一套参数组装逻辑。

### 5.2 当前缺口

| 缺口 | 文件/位置 | 风险 |
|---|---|---|
| 没有 `label_horizon` 字段 | `backend/services/quantevolver/experiment_config.py` | 统一配置层无法表达 3/5/10d。 |
| 只有固定 1d 公式 | `backend/services/quantevolver/config_composer.py` | 即使前端传 horizon，训练标签仍是 1d。 |
| 自动演进 task 只有 `label_type` | `backend/routers/quantevolver_evolution.py`、`qe_evolution_service.py` | Loop 后续可能丢失 horizon 或被 reviewer 改写。 |
| 自定义演进 loop payload 只有 `label_type` | `frontend/src/app/quantevolver/evolution/page.tsx`、`CustomEvoLoopConfig` | 自定义 Loop 无法选择 3/5/10d。 |
| strategy/backtest-only 未锁定 horizon | `submit_strategy_evo_loop`、`retry_loop`、custom backtest-only | 可能复用旧模型，却宣称使用新标签。 |
| 前端展示不显示 horizon | `StrategyConfigCard`、实验详情、演进列表 | 用户无法确认实际训练标签期限。 |

---

## 6. 目标行为定义

### 6.1 标签公式

`label_type` 仍表示价格基准字段：

```text
close -> $close
open  -> $open
vwap  -> $vwap
```

`label_horizon` 表示 forward return 的预测期限，允许值：

```text
1, 3, 5, 10
```

统一公式：

```text
LABEL0(h, field) = Ref(field, -(h + 1)) / Ref(field, -1) - 1
```

展开：

```text
1d close:  Ref($close, -2)  / Ref($close, -1) - 1   # 当前旧公式，必须不变
3d close:  Ref($close, -4)  / Ref($close, -1) - 1
5d close:  Ref($close, -6)  / Ref($close, -1) - 1
10d close: Ref($close, -11) / Ref($close, -1) - 1
```

### 6.2 1d 兼容性原则

1d 行为必须满足：

- `custom_params` 没有 `label_horizon` 时，按 `1` 处理。
- `label_horizon=1` 时生成的 Qlib 标签公式必须和当前旧公式字面一致。
- 旧实验记录不需要补字段即可重跑。
- 不允许把 `label_horizon=1` 解释成 `Ref($close, -1) / $close - 1`。

### 6.3 标签数据边界原则

多 horizon 标签需要未来 `h+1` 个交易日价格。否则测试集尾部会产生更多 NaN label，导致 RankIC 统计样本变少。

实施时必须加入边界规则：

- 训练/回测 segment 的 `test_end` 不变。
- data handler 可读取的原始数据结束日需要至少覆盖 `test_end + h + 1` 个交易日，或明确 fail-fast。
- 第一阶段建议对 `label_horizon > 1` 启用严格校验：如果本地 Qlib 最新交易日不足以计算 `test_end` 的完整 horizon label，则拒绝提交并提示缩短 `test_end`。
- 1d 旧模式不改变原有容错行为，避免旧实验大面积不可重跑。

---

### 6.4 `label_horizon` 与持仓周期的关系

`label_horizon` 是模型训练目标；持仓周期/最短持仓天数是组合执行策略参数。两者相关但不能混为一个字段。

第一阶段必须支持 UI 同时显示并持久化两类配置：

| 配置 | 含义 | 是否改变模型训练 | 是否改变回测交易行为 |
|---|---|---|---|
| `label_horizon` | 监督学习标签期限，决定模型预测目标 | 是，训练 label 改变 | 间接影响预测分数 |
| `hold_thresh` / `min_hold_days` | 股票入选后最短持仓周期或卖出锁定 | 否 | 是，直接影响换手和收益实现 |
| `n_drop` / rebalance | 每期替换数量/调仓强度 | 否 | 是，直接影响换手 |

设计判断：

- 模型训练在阶段 1 **不应自动更换模型架构或复杂训练方法**，否则无法判断收益变化来自 label horizon 还是训练方式变化。
- 不同 `label_horizon` 已经意味着不同训练目标；同一模型类、同一超参 baseline 下先比较 1d/3d/5d/10d，才能得到可信结论。
- UI 可以提供“horizon 策略预设”，例如选择 5d 时建议 `label_horizon=5`、`hold_thresh=5`、较低 `n_drop`，但必须明确展示并由用户确认，不能后台静默改参数。
- 如果用户只改持仓周期而不改 `label_horizon`，系统必须显示提示：“训练目标仍是 Xd，持仓周期为 Yd，二者不一致可能影响收益转换”，但不应静默替用户修改。

最短持仓建议：

- 当前策略若已有 `hold_thresh`，阶段 1 应直接把它作为最短持仓控制入口。
- 如果实际策略没有严格执行 `hold_thresh`，需要补实现或 fail-fast，不能 UI 显示“最短持仓”但回测未执行。
- 默认预设建议：

```text
label_horizon=1d:  hold_thresh 保持当前默认，避免改变旧行为
label_horizon=3d:  建议 hold_thresh=3
label_horizon=5d:  建议 hold_thresh=5
label_horizon=10d: 建议 hold_thresh=10
```

风险控制例外：

- 最短持仓不应阻止强制风控退出、停牌/涨跌停不可交易处理、数据异常 fail-fast 等安全逻辑。
- 这些例外必须在回测交易明细中可追踪，不能静默吞掉。

---

## 7. 数据流设计

### 7.1 统一配置模型

新增：

```python
class ExperimentConfig(BaseModel):
    label_type: str | None = None
    label_horizon: int | None = None
```

规则：

- `effective_label_horizon = label_horizon or 1`。
- 允许值只支持 `{1, 3, 5, 10}`。
- `build_custom_params()` 是唯一注入点：当 `effective_label_horizon > 1` 时写入 `custom_params["label_horizon"]`；1d 可省略以最大限度保持旧 custom_params 形态。
- 如果实现选择始终写入 `label_horizon=1` 作为元数据，也必须证明生成 Qlib 配置和旧公式完全一致；优先建议省略 1d。

### 7.2 单次 QE 实验

入口：`frontend/src/app/quantevolver/compose/page.tsx` -> `POST /quantevolver/config/generate` -> `POST /experiments/{id}/run`

数据流：

1. 前端新增训练期限选择器，默认 `1d`。
2. `buildRuntimeCustomParams()` 在选择 3/5/10 时写入 `label_horizon`；1d 可省略。
3. 后端 `GenerateConfigRequest.custom_params` 接收后统一校验。
4. `qe_experiments.custom_params` 持久化 `label_horizon`。
5. `_run_experiment_unified()` 读取 `qe_experiments`，`build_config_from_exp_record()` 将 `label_horizon` 从 `custom_params` 提升为 `ExperimentConfig.label_horizon`。
6. `BacktestExecutor` 通过 `build_custom_params()` 注入 composer。
7. `ConfigComposer` 根据 `label_type + label_horizon` 生成标签公式。

验收点：

- 未选择 horizon 的旧单次实验生成旧 1d 公式。
- 新建 3/5/10d 单次实验生成对应公式。
- `label_horizon=2/20/abc` 返回 400。

### 7.3 自动演进

入口：`frontend/src/app/quantevolver/evolution/page.tsx` -> `POST /quantevolver/evolution/tasks`

数据流：

1. `EvolutionTaskCreateRequest` 增加 `label_horizon: Optional[int]`。
2. DB migration 给 `qe_evolution_tasks` 增加：

```sql
ALTER TABLE qe_evolution_tasks
ADD COLUMN IF NOT EXISTS label_horizon INTEGER DEFAULT 1;

ALTER TABLE qe_evolution_tasks
ADD CONSTRAINT qe_evolution_tasks_label_horizon_check
CHECK (label_horizon IN (1, 3, 5, 10));
```

3. 创建任务时：
   - 若请求传入 `label_horizon`，使用请求值。
   - 若未传入，从 `base_experiment.custom_params.label_horizon` 继承。
   - 若仍无值，默认为 `1`。
4. `build_config_from_evolution_loop()` 使用 `task.label_horizon` 作为权威来源。
5. reviewer/agent 产生的 `validated_config` 不允许修改 horizon：
   - 若 `validated_config.model_params.label_horizon` 缺失，自动用任务级值。
   - 若存在且与任务级值不一致，fail-fast，并在 loop error 中说明“自动演进阶段不允许修改 label_horizon”。

验收点：

- 自动演进任务的 Loop1/Loop2 都使用同一个 `label_horizon`。
- Agent 不能把 3d 改成 5d。
- 从 3d 单次实验创建自动演进时，默认继承 3d。

### 7.4 自定义演进

入口：`POST /quantevolver/evolution/custom-tasks`

数据流：

1. `CustomEvoLoopConfig` 增加 `label_horizon: Optional[int]`。
2. 前端每个自定义 Loop 增加 `1d/3d/5d/10d` 选择器。
3. 普通 full-train loop 可以独立选择 horizon。
4. `build_config_from_custom_evo_loop()` 从 `loop_config.label_horizon` 构造 `ExperimentConfig.label_horizon`。
5. 保存 `qe_evolution_loops.config_json` 时增加可审计字段：

```json
{
  "label_type": "close",
  "label_horizon": 5,
  "model_params": {"label_horizon": 5}
}
```

6. 如果 `backtest_only=true`：
   - 必须读取源模型 Loop 的 `label_horizon`。
   - 当前 Loop 不允许覆盖为不同值。
   - 不一致直接返回 400，不提交 RDAgent。

验收点：

- 自定义 full-train Loop 可跑 3/5/10d。
- 自定义 backtest-only Loop 只能继承源模型 horizon。
- UI 加载 QE 实验或演进 Loop 时自动带出源 horizon。

### 7.5 从 QE Loop 开始新的实验 / Fork

入口：`POST /quantevolver/evolution/tasks/{task_id}/fork`

数据流：

1. fork 默认从源 Loop 的 `config_json.label_horizon` 或 `config_json.model_params.label_horizon` 继承。
2. 普通 fork 是 full-train，因此可以在请求中提供新的 `label_horizon` 覆盖源值。
3. 若覆盖，必须写入：
   - 新 base experiment 的 `custom_params.label_horizon`。
   - 新 `qe_evolution_tasks.label_horizon`。
4. 若不覆盖，继承源值。

验收点：

- 从 5d Loop fork 出新任务，默认仍是 5d。
- 普通 fork 可显式改为 10d，但 Loop1 必须重新训练。
- fork 详情页显示新任务使用的 horizon。

### 7.6 策略演进 / backtest-only

入口：`POST /quantevolver/evolution/tasks/{task_id}/strategy-fork`

原则：策略演进复用已训练模型，只修改策略参数，不重新训练，所以不能改变训练标签。

数据流：

1. `StrategyLoopConfig` 第一阶段不暴露 `label_horizon` 可编辑项。
2. `strategy_fork_task()` 从源 Loop 读取 horizon，并写入新任务/新 base experiment。
3. `build_config_from_strategy_evo_loop()` 强制使用源模型 horizon。
4. 如果未来 API payload 中出现与源值不同的 `label_horizon`，后端直接 400/fail-fast。

验收点：

- 源模型 3d，策略演进所有 Loop 都是 3d。
- 任何 3d 源模型 + 5d backtest-only 请求必须失败。

### 7.7 Retry Loop

入口：`POST /quantevolver/evolution/tasks/{task_id}/loops/{loop_index}/retry`

数据流：

1. retry 从原 loop `config_json` 和 task 读取 horizon。
2. 如果 retry 走 full train，使用原 loop horizon。
3. 如果 retry 判断为 backtest-only，锁定原模型 horizon。
4. 不允许 retry 时传入新 horizon；如未来有参数入口，必须 fail-fast。

验收点：

- 5d 失败 Loop retry 后仍是 5d。
- retry 不会因为 task 默认 1d 覆盖原 loop 5d。

---

## 8. 后端实施阶段

### Phase 0: 文档与现状确认

已完成：

- 读取 `docs/codex_project_memory.md`。
- 读取统一引擎设计：`docs/unified_engine_design.md`、`docs/unified_engine_test_plan.md`。
- 读取多 horizon 研究与架构文档：`reports/multi_horizon_factor_investing_research_20260419.md`、`reports/multi_horizon_architecture_analysis_20260419.md`。
- 确认当前代码只有 `label_type`，没有 `label_horizon`。

### Phase 1: 公共校验与公式生成

实现内容：

- 新增公共常量：

```python
ALLOWED_LABEL_HORIZONS = {1, 3, 5, 10}
ALLOWED_LABEL_TYPES = {"close", "open", "vwap"}
```

- 在 `ConfigComposer` 中替换固定 `_LABEL_FORMULAS` 为函数：

```python
def build_label_formula(label_type: str, label_horizon: int) -> str:
    field = {"close": "$close", "open": "$open", "vwap": "$vwap"}[label_type]
    return f"Ref({field}, -{label_horizon + 1}) / Ref({field}, -1) - 1"
```

- 保证 `h=1, close` 输出字面保持当前公式：

```text
Ref($close, -2) / Ref($close, -1) - 1
```

- 将 `label_horizon` 加入 `_NON_STRATEGY_PARAMS`，防止误传给策略类构造函数。

验证：

- 参数化测试 `label_type x label_horizon`。
- `label_horizon=1` 生成结果与旧字符串完全一致。
- 非法 horizon 抛 `ValueError` 或 API 400。

### Phase 2: ExperimentConfig 与 Builders

实现内容：

- `ExperimentConfig` 增加 `label_horizon: int | None = None`。
- 增加 validator：只允许 `1/3/5/10`。
- `build_custom_params()` 按统一规则注入。
- 所有 builder 读取并传递 horizon：
  - `build_config_from_exp_record()` 从 `custom_params` 读取。
  - `build_config_from_evolution_loop()` 从 `task.label_horizon` 读取，覆盖/校验 reviewer config。
  - `build_config_from_custom_evo_loop()` 从 `loop_config.label_horizon` 读取。
  - `build_config_from_strategy_evo_loop()` 从源模型 config/task 读取，不允许 loop override。
  - `build_config_from_retry_loop()` 从原 loop config/model_params 恢复。
  - `build_config_from_multi_alpha()` 第一阶段不作为主任务，但可预留透传，避免后续冲突。

验证：

- `build_custom_params()` 单元测试。
- 各 builder 的 horizon 来源测试。
- 旧记录无 horizon 时默认为 1。

### Phase 3: DB 与 API

实现内容：

- 新增 migration：`qe_evolution_tasks.label_horizon INTEGER DEFAULT 1`。
- 实施阶段必须实际执行 migration，并在当前开发/验证数据库完成结构变更；只生成脚本不算完成。
- 更新数据库 bootstrap/init 逻辑，保证新环境初始化时也有 `label_horizon` 字段和 CHECK 约束。
- 创建/更新自动演进任务时写入 `label_horizon`。
- fork/custom/strategy-fork/retry 涉及任务创建时写入或继承 horizon。
- API 层统一校验：
  - `GenerateConfigRequest.custom_params.label_horizon`
  - `EvolutionTaskCreateRequest.label_horizon`
  - `EvolutionTaskForkRequest.label_horizon`
  - `CustomEvoLoopConfig.label_horizon`

必须执行的 schema 验证：

```sql
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'qe_evolution_tasks'
  AND column_name = 'label_horizon';

SELECT conname
FROM pg_constraint
WHERE conrelid = 'qe_evolution_tasks'::regclass
  AND pg_get_constraintdef(oid) LIKE '%label_horizon%';
```

运行期 preflight：

- 后端启动或 QE label horizon 请求进入时，检查 `qe_evolution_tasks.label_horizon` 是否存在。
- 缺失时直接返回明确错误：`DB schema missing qe_evolution_tasks.label_horizon; run and verify migration before submitting label_horizon experiments`。
- 不允许因为字段缺失而退回旧 1d 或绕过任务级字段。

验证：

- 400 测试：`label_horizon=2`、`0`、`20`、字符串。
- 新 task DB 行 `label_horizon` 正确。
- 旧 task 读取为默认 1。
- 手工/自动验证 active DB schema，确认 migration 已应用。

### Phase 4: backtest-only 锁定

实现内容：

- 新增工具函数：

```python
def extract_label_horizon_from_config(config: dict) -> int:
    return int(
        config.get("label_horizon")
        or (config.get("model_params") or {}).get("label_horizon")
        or 1
    )
```

- 新增锁定校验：

```python
def assert_backtest_only_label_horizon_locked(source_horizon: int, requested_horizon: int | None):
    if requested_horizon is not None and requested_horizon != source_horizon:
        raise ValueError("backtest-only cannot change label_horizon; retraining is required")
```

- 应用到：
  - `strategy_fork_task()`
  - `submit_strategy_evo_loop()`
  - `create_custom_evolution_task()` 的 `backtest_only` 校验
  - `retry_loop()` 的 backtest-only 分支

验证：

- 源 5d + 请求 10d backtest-only 失败。
- 源 5d + 未传 horizon backtest-only 成功并继承 5d。

### Phase 5: 标签数据边界校验

实现内容：

- 在 `ConfigComposer` 或提交前增加 horizon 数据边界检查。
- 对 `label_horizon > 1`，确认本地 Qlib 数据能覆盖 `test_end + h + 1` 个交易日。
- 若不足，返回清晰错误：

```text
label_horizon=10 requires price data at least 11 trading days after test_end=YYYY-MM-DD. Please shorten test_end or refresh Qlib data.
```

- 不改变 1d 旧模式默认容错。

验证：

- 构造最新日期附近的 10d 请求，必须 fail-fast。
- 构造历史区间 10d 请求，能正常生成配置。

### Phase 6: 前端与展示

实现内容：

- `compose/page.tsx`
  - 新增训练期限选择：`1d / 3d / 5d / 10d`。
  - 新增/强化策略持仓参数入口：`hold_thresh`、`n_drop`、TopK；需要与训练期限同屏展示。
  - 提供 horizon 策略预设按钮，但所有被设置的参数必须在 UI 中明示，用户可修改。
  - 默认 1d。
  - 3/5/10 才写入 `custom_params.label_horizon`。
- `evolution/page.tsx`
  - 自动演进创建表单增加 `label_horizon`。
  - custom_evo loop 增加 `label_horizon`。
  - 从实验/Loop 加载配置时带出 horizon。
  - backtest-only 模式下 horizon 锁定为源模型值，UI 禁用编辑。
  - 普通 fork 允许覆盖 horizon，并明确提示“会重新训练”。
  - strategy_fork 不提供 horizon 修改入口。
- `StrategyConfigCard` 和实验详情
  - 显示：`Label: close / Horizon: 5d`。
  - 同时显示策略持仓参数：`hold_thresh`、`n_drop`、TopK、执行算法。
  - 来源优先级：loop config -> custom_params/model_params -> task.label_horizon -> default 1d。
- 实验结果详情页
  - 必须能从 UI 查看完整回测结果：收益曲线、关键指标、RankIC/ICIR、最大回撤、换手率、交易/持仓明细、有效 IC 天数、原始增强结果 JSON 的可下载或可展开视图。
  - 如果 enhanced artifacts 缺失，不允许显示“成功但无详情”；必须显示缺失 artifact 名称和排查路径。

验证：

- UI 默认仍显示 1d。
- 选择 5d 后请求 payload 包含 `label_horizon: 5`。
- backtest-only UI 不允许修改 horizon。
- UI 可完成完整业务流程：选择因子、选择模型、设置 label horizon、设置持仓参数、提交回测、查看详细结果。

---

## 9. 测试计划

### 9.1 单元测试

| 测试 | 目的 |
|---|---|
| `test_label_horizon_default_1d_formula_unchanged` | 没有 horizon 和 `label_horizon=1` 都生成旧公式。 |
| `test_label_horizon_formula_matrix` | `close/open/vwap x 1/3/5/10` 公式正确。 |
| `test_label_horizon_invalid_values` | 非法值 fail-fast。 |
| `test_experiment_config_build_custom_params_label_horizon` | 统一配置层注入正确。 |
| `test_builders_preserve_label_horizon` | 单次、自动、自定义、fork/retry 路径不丢 horizon。 |
| `test_label_horizon_not_strategy_param` | `label_horizon` 不进入策略 kwargs。 |
| `test_backtest_only_rejects_horizon_change` | 跳过训练时禁止改 horizon。 |

### 9.2 API 测试

| API | 验证 |
|---|---|
| `/quantevolver/config/generate` | 1/3/5/10 可用，非法值 400。 |
| `/quantevolver/evolution/tasks` | task 行写入 `label_horizon`。 |
| `/quantevolver/evolution/custom-tasks` | loop 级 horizon 可用，backtest-only 锁定。 |
| `/quantevolver/evolution/tasks/{id}/fork` | 默认继承，full-train 可覆盖。 |
| `/quantevolver/evolution/tasks/{id}/strategy-fork` | 禁止覆盖。 |
| `/quantevolver/evolution/tasks/{id}/loops/{i}/retry` | 保持原 horizon。 |

### 9.3 Smoke 验证

最小 smoke 顺序：

1. 旧 1d 单次实验，小窗口跑通，生成公式与当前一致。
2. 3d 单次实验，小窗口跑通，`qlib_results_enhanced.json` 正常生成。
3. 5d 自动演进任务，至少 Loop1/Loop2 使用同一 horizon。
4. 10d 自定义演进 full-train Loop 跑通。
5. 从 5d Loop fork，新任务默认 5d。
6. 从 5d Loop 做 strategy_fork，所有策略回测 Loop 继承 5d。
7. 手工构造 5d 源模型 + 10d backtest-only 请求，确认后端拒绝。

---

### 9.4 Playwright UI 全流程测试方案

阶段 1 必须补充基于 Playwright 的 E2E 测试，目标是证明用户可以只通过 UI 完成完整业务流程，而不是依赖后端脚本或手工 API。

本次落地执行的强制验证配置：

- 后端必须使用独立端口 `8012` 启动，不能重启或干扰当前 `8001` 服务。
- 前端必须使用独立端口 `3012` 启动，不能干扰当前桌面操作或现有 `3000` 服务。
- Playwright 必须以 headless/background 方式运行，禁止控制当前桌面的鼠标键盘。
- UI 全流程验证只能选择 `LGBModel Golden Seed (validated 2026-03-14)` 模型，保证训练时间可控并用于快速全量验证。
- Playwright 运行前必须校验 UI 下拉/模型列表中确实存在该模型；不存在时 fail-fast，不允许自动换成其他模型。
- 所有回测验证都必须读取并断言收益、回撤、RankIC/ICIR、换手、有效 IC 天数等结果，不能只断言状态为 completed。
- 验证发现的任何问题必须修复后复跑，直到 8012/3012 下 UI 业务流程可以完整走通。
- 测试期间修复的每个 bug 必须作为独立 Git 提交推送到 GitHub `main`，提交说明需包含触发场景和验证命令。

测试前置：

- 使用临时后端端口和临时前端端口，避免影响正在运行的 8001/3000。
- 准备可控测试数据：
  - 至少 2 个可选因子。
  - 必须包含 `LGBModel Golden Seed (validated 2026-03-14)` 可用模型，且测试只能选该模型。
  - 至少 1 个可用策略。
  - 一个短窗口 data_split，保证 1d/3d/5d/10d 都能计算 label。
- 确认 DB migration 已在测试库执行；Playwright 启动前运行 schema preflight。

核心 E2E 用例：

| 用例 | UI 操作 | 断言 |
|---|---|---|
| 单次 1d 回归 | 打开 QE compose，选择因子、模型、默认 1d、默认持仓参数，生成并运行实验 | 请求 payload 不含或含 `label_horizon=1`；结果页显示 Horizon 1d；旧公式不变；回测完成并有详细结果。 |
| 单次 3d/5d/10d | 分别选择 3d、5d、10d，设置对应 `hold_thresh`，运行回测 | payload 正确；结果页显示对应 horizon 和持仓参数；增强指标可读取；无静默退回 1d。 |
| 不同持仓周期回测 | 固定同一因子/模型/data_split，分别设置 `hold_thresh=1/3/5/10` 并运行 | 每次结果配置准确记录持仓参数；回测结果能区分收益、回撤、换手变化；不能复用旧结果冒充新结果。 |
| 非法 horizon 阻断 | 通过 UI 或拦截请求注入非法 horizon | 页面显示后端 400 错误；不创建 running 实验；不能静默改成 1d。 |
| horizon/持仓不一致提示 | 选择 `label_horizon=10d` 但 `hold_thresh=1` | UI 显示不一致提示；若用户确认提交，结果配置中准确记录二者不一致。 |
| 自动演进创建 | 从已完成实验进入 evolution，创建 5d 自动演进 | task 列表显示 5d；Loop1 提交 payload 保持 5d；Loop 详情显示 5d。 |
| 自定义演进 | UI 自助添加 custom loop，选择因子、模型、5d、持仓参数，提交 | loop config 保存 `label_horizon=5`；Loop 详情可查看完整配置和结果。 |
| Loop fork full-train | 从 5d Loop fork，新任务默认继承 5d，并手动改为 10d | UI 明确提示会重新训练；新任务 DB/API 显示 10d；不是 backtest-only。 |
| strategy/backtest-only 锁定 | 从 5d Loop 创建 strategy fork | UI 不提供 horizon 修改入口；详情显示锁定为 5d；拦截请求改 10d 时后端拒绝。 |
| retry 锁定 | 失败的 5d Loop 点击 retry | retry 后仍显示 5d；不能被 task 默认值覆盖为 1d。 |
| 结果详情完整性 | 打开实验详情页和 enhanced metrics 区域 | 能看到收益、回撤、Sharpe、RankIC/ICIR、换手、有效 IC 天数、持仓/交易明细或明确 artifact 缺失错误。 |

Playwright 断言要求：

- 不能只断言按钮可点击；必须断言网络请求 payload、响应状态、页面展示和 DB/API 回读一致。
- 每个测试用例都要检查 `label_horizon`、`label_type`、`hold_thresh`、`n_drop`、TopK 是否一致。
- 对回测完成用轮询等待最终状态，但设置明确超时；超时即失败，不允许继续读取旧结果。
- 对所有错误场景，必须断言 UI 显示具体错误消息。

建议文件组织：

```text
frontend/e2e/qe-label-horizon.spec.ts
frontend/e2e/helpers/qe.ts
frontend/e2e/fixtures/qe-label-horizon.json
```

建议命令：

```bash
BACKEND_PORT=8012 FRONTEND_PORT=3012 npm run test:e2e -- qe-label-horizon.spec.ts
```

验收标准：

- Playwright 能从 UI 完成至少一个 1d 和一个 5d 的完整回测流程。
- Playwright 过程中只使用 `LGBModel Golden Seed (validated 2026-03-14)`。
- UI 与 API/DB 回读的 `label_horizon`、持仓参数、结果指标一致。
- 所有 fail-fast 场景都有前端可见错误。
- 结果页能准确获取并展示回测详细数据，不能只显示“completed”。

---

## 10. 量化验证目标

功能跑通不是最终目标。每个 horizon 需要在同一批因子、模型、数据切分下与 1d baseline 对比：

| 指标 | 目标 |
|---|---|
| OOS RankIC mean | 至少一个 `3/5/10d` 高于 1d，且不是由尾部 NaN 样本造成的假提升。 |
| RankICIR | 更稳定，不只是均值偶然抬高。 |
| 年化收益 | 不能只提升 IC 却收益恶化；优先选择收益和 IC 同时改善的 horizon。 |
| 最大回撤 | 新 horizon 不应显著扩大回撤；若收益大幅提升但回撤扩大，需要单独评估收益/回撤比。 |
| 换手率 | 更长 horizon 理论上应降低噪音和不必要换手；若换手反而升高，要检查策略参数是否错配。 |
| 有效样本天数 | 每个 horizon 必须报告有效 IC 天数，避免 10d 因缺失尾部标签导致样本不公平。 |

建议验证矩阵：

```text
固定: 因子集合、模型、data_split、topk/n_drop、执行算法
变量: label_horizon = 1 / 3 / 5 / 10
输出: RankIC mean、RankICIR、Annual Return、Max Drawdown、Sharpe、Turnover、有效IC天数
```

只有单 Alpha 验证出稳定优势后，才进入多 Alpha horizon-aware 组合。

---

## 11. 分阶段总体路线图

本方案不是只为 `label_horizon` 写一个孤立功能，而是作为 QE 单 Alpha -> 单节点 Multi-Alpha -> horizon-aware Multi-Alpha -> 动态权重 -> 持仓/换手转换收益 的第一阶段。所有阶段必须按投资有效性递进，不能为了“架构完整”提前堆叠复杂度。

当前可执行范围只包含 **阶段 1：`label_horizon=3/5/10d` 的单 Alpha 验证**。阶段 2 到阶段 6 只作为后期目标和候选方向记录在本文档中，不进入当前代码实施范围；必须等阶段 1 的真实 OOS 验证完成后，再针对验证结果单独编写后续阶段的详细设计方案。

阶段 2 及以后的详细设计触发条件：

- 阶段 1 已完成至少一组固定因子/模型/data_split 的 `1d/3d/5d/10d` 对照实验。
- 已确认某个 horizon 的提升不是数据尾部 NaN、样本差异、未来函数或单次窗口偶然造成。
- 已形成阶段 1 验证报告，说明哪些 horizon 有效、哪些无效，以及收益/回撤/换手是否支持继续扩展。
- 若阶段 1 未证明 3/5/10d 相对 1d 有真实优势，则暂停 Multi-Alpha 后续复杂设计，先回到因子质量、标签定义和策略参数匹配问题。

### 11.1 第一优先级：`label_horizon=3/5/10d` 的单 Alpha 验证

目标：

- 在 QE 单 Alpha 统一配置层内加入 `label_horizon`。
- 保持现有 1d 模式不变。
- 对同一批因子、模型、数据切分，比较 `1d/3d/5d/10d` 的真实 OOS 表现。
- 同时验证 matched holding profile，即 3d/5d/10d 训练标签是否需要对应更长最短持仓才能转化为收益。

实施重点：

- 只改训练标签，不同时改多 Alpha 组合、动态权重、HMM/regime 或分布式。
- baseline 对照先保持模型架构/超参一致；是否引入 horizon-specific training profile，要在阶段 1 结果基础上另行设计。
- 输出必须包含 RankIC mean、RankICIR、年化收益、最大回撤、换手率、有效 IC 天数。
- 如果 3/5/10d 只提升 RankIC 但收益/回撤恶化，不能直接进入多 Alpha，应先分析策略参数和换手错配。

退出标准：

- 至少一个 horizon 在 OOS RankICIR、收益/回撤质量上优于 1d baseline。
- 确认提升不是由尾部 label NaN、样本减少、数据泄漏或单次随机窗口造成。

### 11.2 第二优先级：单节点 Multi-Alpha 的 `rank_icir_corr_ewma` group 组合（后期目标）

目标：

- 在单节点内验证多个 Alpha group 的组合是否比最佳单 group 和等权组合更有价值。
- 组合方法聚焦 IC、ICIR、相关性和近期有效性，不先上复杂 stacking。

候选方向（非当前详细设计）：

```text
每天每组输出 pred_g(t, stock)
-> 按 date 做 rank-normalize / z-score normalize
-> 用仅截至 t-1 的滚动窗口计算 group 质量：
   ic_ewma_g
   icir_ewma_g
   avg_abs_corr_g_to_other_groups
-> group weight:
   raw_w_g = max(0, icir_ewma_g) / (1 + avg_abs_corr_g_to_other_groups)
-> normalize weights
-> final_score = sum_g w_g * normalized_score_g
```

关键约束：

- 权重只能使用历史窗口或验证集，不能用 test period 全量 IC 回看。
- 必须保留 equal-weight 组合作为 baseline。
- 组合目标是超过“最佳单 group”，不是只超过很弱的平均基线。
- 先做单节点串行/本地执行，避免分布式问题掩盖组合方法问题。

退出标准：

- `rank_icir_corr_ewma` 组合的 RankICIR、收益/回撤优于等权组合。
- 相比最佳单 group，至少在回撤、稳定性或收益/回撤比上有明确增益。
- 若组合没有价值，必须输出原因：group 同质化、负 IC group、预测尺度不一致、相关性过高、权重泄漏、标签 horizon 错配或策略参数错配。

### 11.3 第三优先级：horizon-aware Multi-Alpha（后期目标）

目标：

- 把已验证有效的 `3d/5d/10d` group 按 horizon-aware 方式组合，而不是把所有 group 当作同一种 1d 信号。

候选方向（非当前详细设计）：

- 每个 group 必须携带 `label_horizon` 元数据。
- 组合时分层计算 group 权重：
  - 同 horizon 内先做 group 组合。
  - 不同 horizon 间再做 horizon layer 组合。
- 不能直接把 3d/5d/10d 原始预测分数相加，必须先按 date 做 rank/z-score 归一化。
- horizon layer 的权重也必须使用截至 t-1 的历史表现，避免未来函数。

候选结构：

```text
3d groups  -> score_3d
5d groups  -> score_5d
10d groups -> score_10d

final_score = w_3d * score_3d + w_5d * score_5d + w_10d * score_10d
```

退出标准：

- horizon-aware 组合优于 horizon-agnostic group 组合。
- 组合收益不是由单一 horizon 完全贡献；否则应降级为单 horizon 策略，而不是维持伪多 horizon 架构。
- 有明确解释：哪些 horizon 在什么市场窗口贡献收益/降低回撤。

### 11.4 第四优先级：动态因子权重，先单 group 验证，再接入 Multi-Alpha（后期目标）

目标：

- 因子 IC 随市场变化波动，固定权重可能无法发挥价值；但动态权重容易过拟合，必须先在单 group 内验证。

候选验证方向（非当前详细设计）：

- 对 group 内每个因子按滚动窗口计算：
  - RankIC EWMA
  - ICIR EWMA
  - 命中率/正 IC 比例
  - 与其他因子的相关性
- 权重只使用 t-1 以前数据。
- 对权重做约束：
  - 非负或有限 short 权重。
  - 单因子最大权重上限。
  - 权重变化速度限制，避免过度换手。

候选公式起点：

```text
raw_w_i = max(0, icir_ewma_i) / (1 + avg_abs_corr_i)
w_i = clip_and_normalize(raw_w_i)
group_score = sum_i w_i * normalized_factor_i
```

接入 Multi-Alpha 的前提：

- 动态因子权重在单 group 中优于固定权重。
- 提升同时体现在 RankICIR 和收益/回撤，而不是只提高训练期 IC。
- 权重变化没有引入不可接受的换手率。

退出标准：

- 单 group 动态权重通过 OOS 验证后，才能替换 Multi-Alpha group 内部固定权重。
- 若单 group 无效，不允许直接接入 Multi-Alpha。

### 11.5 第五优先级：持仓周期/换手策略配合，让 5d/10d IC 转化为收益（后期目标）

目标：

- 5d/10d RankIC 高不等于自动赚钱。训练标签期限、调仓频率、`hold_thresh`、`n_drop`、TopK 和交易成本之间必须匹配。

候选方向（非当前详细设计）：

- 对不同 horizon 设置策略参数候选：

```text
3d:  较高调仓频率，中等 n_drop，关注短期衰减
5d:  中等调仓频率，降低 n_drop，强调稳定持有
10d: 更低调仓频率，更强 turnover cap，避免高 IC 被频繁交易损耗
```

- 对每个 horizon 单独搜索或网格验证：
  - `hold_thresh`
  - `n_drop`
  - TopK
  - rebalance frequency
  - turnover cap / max daily turnover
- 目标不是最大 RankIC，而是最大化收益/回撤比、降低换手损耗。

退出标准：

- 5d/10d 的 IC 优势能在回测收益、最大回撤、换手率上体现。
- 如果高 IC horizon 无法转化为收益，需要分析：
  - 信号衰减和调仓周期错配。
  - TopK 过小导致噪声放大。
  - n_drop 过高导致过度交易。
  - 组合分数和策略持仓逻辑不匹配。

### 11.6 最后优先级：分布式、多节点、HMM/regime、复杂 stacking（后期目标）

这些能力必须后置：

- 分布式/多节点只解决性能，不解决 Alpha 是否有效；只有单节点投资逻辑验证通过后再做。
- HMM/regime 只能在稳定 baseline 上做增量验证，不能用来掩盖因子或组合方法无效。
- 复杂 stacking/meta-model 需要严格 walk-forward / nested validation，否则极易过拟合。
- 若简单的 `rank_icir_corr_ewma` 都不能超过等权或最佳单 group，不应直接升级到 stacking。

进入条件：

- 第一到第五阶段至少完成核心 smoke 和一组真实 OOS 对照实验。
- 已证明多 horizon / 多 group / 动态权重 / 换手策略中至少有一个模块带来可解释的收益提升。
- 已有足够样本支持更复杂模型，且有严格的防泄漏测试。

---

## 12. 后续与多 Alpha 的衔接

本方案完成后，多 Alpha 不立即改组合逻辑，但会获得必要基础：

1. 每个单 Alpha 实验都有明确 `label_horizon` 元数据。
2. 可先筛选“在 3/5/10d 标签下真正有效”的 Alpha，再组合。
3. 后续多 Alpha 可以按 horizon 分组：fast/mid/slow，但必须基于真实 OOS 表现，而不是机械按因子类别分组。
4. 动态权重和多持仓周期策略应在单 Alpha 或单组 Alpha 上验证有效，再进入多 Alpha 总组合。

暂不实施：

- 不做多 Alpha meta-model 动态权重。
- 不做 Agent 自动搜索 horizon。
- 不做分布式优先优化。
- 不做行业中性、市值中性、交易冲击建模。

---

## 13. 反模式与风险控制

必须避免：

- 把 `label_type` 扩展成 `close_5d` 这类混合字段；价格基准和预测期限必须拆开。
- 在多个路由里各自拼 `label_horizon`，绕过 `ExperimentConfig.build_custom_params()`。
- 允许 reviewer/agent 在自动演进中偷偷改 horizon。
- backtest-only 复用旧模型时允许修改 horizon。
- 只看 10d RankIC 高就直接认为策略更赚钱；必须同时看收益、回撤、换手和样本数。
- 因数据尾部未来价格不足导致 label NaN，却不提示用户。
- 把本功能直接塞进多 Alpha，掩盖单 Alpha horizon 是否有效的问题。
- 只生成 DB migration 脚本但不在当前验证库执行。
- 后端支持新参数但 UI 没有可操作入口。
- UI 显示持仓周期或 horizon，但后端实际没有执行或没有持久化。
- enhanced metrics/artifacts 缺失时仍显示实验成功且不暴露错误。

---

## 14. 最终验收标准

功能验收：

- 旧 1d 实验不受影响。
- `label_horizon=3/5/10` 能在 QE 单次实验跑通。
- 自动演进、自定义演进、Loop fork、retry、strategy backtest-only 都统一使用同一套配置层。
- 跳过训练时无法修改 horizon。
- 前端和详情页能显示实际 horizon。
- 当前数据库结构已实际迁移并通过 schema preflight，不只是存在脚本。
- UI 可完成完整流程：选择因子、选择模型、配置 label horizon、配置持仓参数、提交回测、查看详细结果。
- 所有静默兜底路径已清理或改为 fail-fast。

研究验收：

- 至少完成一组固定因子/模型/data_split 的 `1/3/5/10d` 对照实验。
- 报告 RankIC mean、RankICIR、收益、回撤、换手、有效样本数。
- 如果 3/5/10d 没有明显优于 1d，需要回到因子有效性和持仓/调仓参数匹配问题，而不是继续叠加多 Alpha 复杂度。
- 报告必须说明 matched holding profile 是否改善 5d/10d 收益转换。

UI/E2E 验收：

- Playwright 覆盖单次实验、自动演进、自定义演进、Loop fork、backtest-only 锁定、retry 和结果详情读取。
- Playwright 至少完成一个 1d 和一个 5d 的真实 UI 回测闭环。
- 页面展示、API payload、DB/API 回读、结果 artifact 四者一致。

---

## 15. 建议执行顺序

1. 实施后端公共校验与 `ConfigComposer` 公式生成。
2. 实施 `ExperimentConfig.label_horizon` 和所有 builder 透传。
3. 增加 DB migration、更新 bootstrap/init、实际执行迁移并通过 schema preflight。
4. 实施 backtest-only horizon 锁定。
5. 实施标签数据边界校验。
6. 实施前端选择器、持仓参数入口、配置展示和结果详情展示。
7. 补充 Playwright E2E，保证 UI 可完成完整业务闭环。
8. 跑单元/API/smoke/Playwright 测试。
9. 跑真实 1/3/5/10d 单 Alpha 对照实验，并加入 matched holding profile 对照。
10. 根据结果决定是否进入多 Alpha horizon-aware 组合阶段。
