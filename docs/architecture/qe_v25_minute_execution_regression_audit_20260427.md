# QE / V25 分钟线执行回归审计与修复边界

日期：2026-04-27  
范围：QE 回测/演进中 `V25_TWO_STAGE` 分钟执行策略、涨跌停/停牌/分钟线异常处理、旧版本降级行为、当前版本需要回滚或修复的问题。  
原则：本文件只记录审计结论和修复边界，不修改策略、模型、数据库资产。

## 1. 结论摘要

1. 当前 QE 使用的 V25 权威模板不是一个“原始正确处理涨跌停/停牌”的 GitHub 历史版本；它是在严格配置真实性修复中新增/打包的版本。
2. 当前版本把部分正常市场状态，例如停牌导致的 `prev_close=NaN`、分钟价格/涨跌停价格缺失，错误地当成程序错误抛出 `RuntimeError`，会导致整个 Loop 失败。
3. 旧版 `.backup` 执行器中的 TWAP fallback 不是由涨跌停触发，而是由 V25 plan 未生成触发；该降级行为本身属于静默修改业务逻辑，不能整体恢复。
4. 正确修复方向不是恢复静默 TWAP，而是区分两类情况：
   - 配置/模型/执行计划问题：必须 fail-fast，明确报错。
   - 涨跌停、停牌、无分钟线等正常市场不可交易状态：必须作为业务状态处理，记录原因，不应让程序崩溃。
5. V25 模型权重修复（前 30 分钟约 88.79%、后 210 分钟约 11.21%）不应回滚；需要纠正的是 QE/Qlib 包装层和执行框架对正常市场状态的错误 fail-fast。

## 2. 已核查的关键文件

### 2.1 当前 QE / RD-Agent V25 包装层

- `scripts/tail_twap_v25_strategy.py`
- `F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\tail_twap_v25_strategy.py`
- `F:\Dev\RD-Agent-main\qe_workspace\qe_20260426_234914_9c7b\Loop4\tail_twap_v25_strategy.py`

当前问题主要集中在这些包装层：

- `_generate_plan_for_order()` 读取当前价格和 `$prev_close` 后，遇到 `NaN` / 缺失直接抛错。
- P0 涨跌停检测读取 `$up_limit_price` / `$down_limit_price` 后，遇到缺失或无效直接抛错。
- `plan is None` 时不再 fallback TWAP，这一原则本身正确，但当前没有把“正常不可交易状态”与“V25 plan 未生成”分开。

### 2.2 V25 核心执行器

- `rl_execution/executor/v25_two_stage_executor.py`
- `rl_execution/executor/v25_two_stage_executor.py.backup`
- `F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\rl_execution\executor\v25_two_stage_executor.py`

`rl_execution/executor/v25_two_stage_executor.py.backup` 是本机保留的早期行为参考，但不是 Git 受控文件，也不是 QE 当前直接加载的权威模板。

该 `.backup` 的关键行为：

- 模型路径缺失时不报错，只是不生成 `_current_plan`。
- `_current_plan is None` 时 fallback 到 TWAP。
- 买入遇涨停、卖出遇跌停时返回 `0.0`，表示当前分钟不执行，不是整天降级。

因此它只能作为业务语义参考，不能直接回滚恢复。

## 3. GitHub / Git 历史核查结论

### 3.1 AIstock 仓库

远端：

- `origin/main`
- `origin/backup/pre-factor-eval-unify-20260417`

结论：

- `origin/backup/pre-factor-eval-unify-20260417` 中没有 V25 相关文件。
- `scripts/tail_twap_v25_strategy.py` 在 Git 历史中首次出现于：
  - `2de9e7f fix(qe): enforce config execution truthfulness`
- 未找到更早可直接恢复的 `tail_twap_v25_strategy.py` 原始版本。

### 3.2 RD-Agent 仓库

远端：

- `origin/main`
- `upstream/*` Microsoft RD-Agent 分支

结论：

- `F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\tail_twap_v25_strategy.py` 首次出现于：
  - `d4f4716b fix(qe): package strict V25 execution templates`
- Microsoft upstream 已知分支中没有 `TailTWAPWithV25TwoStageStrategy` / `tail_twap_v25_strategy.py`。
- 未找到可直接回滚的公开 GitHub 原始正确版本。

### 3.3 本机残留版本

本机存在：

- `rl_execution/executor/v25_two_stage_executor.py.backup`

该文件提供早期行为参考，但不是完整正确版本：

- 优点：涨跌停作为当前分钟不可交易状态处理，不会因涨跌停清空 plan 或整天降级。
- 问题：模型缺失时静默 TWAP fallback，违反“UI 配置必须真实执行”的要求。

## 4. 当前版本错误修改清单

### 4.1 错误地把正常市场状态当成程序错误

当前包装层中以下情况会抛出 `RuntimeError`：

- `open_price` / 当前分钟 close 无效。
- `$prev_close` 为 `None` / `NaN` / 非正数。
- `$up_limit_price` 缺失或无效。
- `$down_limit_price` 缺失或无效。
- P0 涨跌停检测过程中遇到数据异常。

问题：

- 停牌、临停、无分钟成交、某些数据源在停牌日没有 `prev_close`，都可能是正常市场/数据表现。
- 如果这些情况有 `market.suspend_d` 或交易所停牌状态佐证，应当作为“不可交易/未成交/跳过”处理，而不是让整个 QE Loop 失败。

需要修复：

- 在策略包装层和执行层增加市场状态分类。
- 对确认停牌/临停/不可交易的标的记录结构化原因并跳过当前分钟或保留未成交。
- 只在数据应当存在但缺失，且无法被停牌/不可交易状态解释时，才作为数据完整性错误 fail-fast。

### 4.2 P0 涨跌停检测过度 fail-fast

当前版本对 P0 涨跌停检测更严格：

- `close_price` 无效直接报错。
- `limit_up` / `limit_down` 无效直接报错。

问题：

- P0 是执行优化规则，不应因为某一分钟缺少涨跌停价就让整个回测失败。
- 如果全局要求 `stk_limit` 数据权威可用，应在任务启动前做数据 readiness 检查，而不是在单个订单的分钟执行中崩溃。

需要修复：

- 启动前检查 `stk_limit` 数据覆盖和审计状态。
- 运行中如果某只股票某分钟因停牌导致涨跌停价缺失，应记录 `limit_data_missing_due_to_suspend` 或类似原因并跳过 P0。
- 不允许静默跳过：必须在 diagnostics / artifacts / run log 中可追踪。

### 4.3 停牌识别来源不足

当前包装层主要依赖：

- `self.trade_exchange.check_stock_suspended(...)`
- Qlib quote 中的价格和限制价字段

问题：

- `qe_20260426_234914_9c7b` Loop4 中，`688143.SH` 在 `market.suspend_d` 有停牌记录，但执行时仍进入 `_generate_plan_for_order()` 并因 `prev_close=NaN` 报错。
- 说明只依赖 Qlib exchange 的停牌判断不够，需要接入已同步到本地数据库的 `market.suspend_d` 或导出的停牌 artifact。

需要修复：

- QE 日频选股信号生成阶段提供 `filter_suspended_on_signal` 开关。
- QE 分钟回测执行阶段也应使用同一份停牌信息，避免“配置层显示启用，执行层没用”。
- 建议生成每次实验固定的 suspend artifact，例如 JSON/H5/Parquet，放入 workspace，避免每个交易日/每个订单实时访问数据库。
- artifact 必须带数据日期范围、来源表、生成时间、hash，确保可追溯。

### 4.4 `plan is None` 的语义需要收紧

旧版本中：

- `_current_plan is None` 会触发 TWAP fallback。

在分钟线完整的前提下，真正会触发 `_current_plan is None` 的场景主要是：

1. V25 early / late 模型路径缺失。
2. V25 模型文件不存在。
3. executor 未正确调用 `reset()` / 未生成计划。
4. 上游流程错误导致 plan 未赋值。

不会因为以下情况触发：

- 某一分钟涨停。
- 某一分钟跌停。
- 一天内有若干分钟涨跌停。
- 分钟线完整、模型完整、`reset()` 正常执行的普通交易日。

当前版本取消 TWAP fallback 的方向是正确的，但需要改进错误边界：

- V25 plan 没生成：fail-fast，并明确是模型/配置/流程错误。
- 股票停牌或当前分钟不可交易：不应归类为 plan 没生成，应记录业务状态并跳过或保留未成交。

### 4.5 当前工作区存在多份 V25 版本，语义不完全一致

已发现至少两类 V25 语义：

1. 当前严格版本：
   - 包含 `missing prev_close for V25 plan`
   - 包含 `V25 P0 limit data missing`
   - 包含 `refusing to fall back to TWAP`
   - 会把部分正常市场状态变成 RuntimeError。

2. 较早半严格版本：
   - P0 对缺失限制价更宽松。
   - `plan is None` 时仍会按剩余分钟均分，等价于静默 TWAP。
   - 仍会对 `prev_close=NaN` 报错。

问题：

- 多份文件同时存在，未来 QE / RDAgent 可能复制不同版本进入 workspace。
- 如果仅修复 AIstock 一侧而未同步 RD-Agent factor template，未来实验仍可能使用旧语义。

需要修复：

- 明确 V25 执行策略的唯一权威来源。
- 修改后同步 AIstock 和 RD-Agent 模板。
- 每次生成 QE workspace 时记录 `tail_twap_v25_strategy.py` hash 和来源。
- 旧实验恢复时明确使用原 workspace 文件还是重新注入当前权威模板，不能静默替换。

## 5. 分钟线场景分类与正确行为

| 场景 | 数据表现 | 当前风险 | 正确行为 |
|---|---|---|---|
| 普通交易分钟 | close、prev_close、limit_up、limit_down 均有效 | 正常 | 生成并执行 V25 plan |
| 买入遇涨停 | 当前价接近/等于涨停价，买单无法成交 | 不应报错；可能由 exchange 拒单/未成交 | 当前分钟不买，保留未成交，继续后续 V25 plan |
| 卖出遇跌停 | 当前价接近/等于跌停价，卖单无法成交 | 不应报错 | 当前分钟不卖，保留未成交，继续后续 V25 plan |
| 买入遇跌停 | 当前价接近/等于跌停价，买入为有利价格 | P0 可全量买入 | 可以按 P0 全量尝试执行，并记录 P0 触发 |
| 卖出遇涨停 | 当前价接近/等于涨停价，卖出为有利价格 | P0 可全量卖出 | 可以按 P0 全量尝试执行，并记录 P0 触发 |
| 全天停牌 | `suspend_d` 有记录，分钟线/prev_close/limit 可能缺失 | 当前可能因 `prev_close=NaN` 失败 | 标记 suspended，跳过该标的当日订单或保留未成交，不让 Loop 失败 |
| 临时停牌 / 盘中停牌 | 部分分钟有数据，部分分钟无成交或无 bar | 当前可能在某分钟读价失败 | 对停牌分钟跳过，恢复交易后继续执行剩余计划 |
| 分钟线完整但 `prev_close=NaN` | 可能是数据质量问题，也可能是停牌/复牌边界 | 当前直接失败 | 若有 suspend_d 佐证则不可交易处理；否则数据 readiness fail-fast |
| 限制价缺失但价格有效 | `limit_up/down` 缺失 | 当前 P0 可能失败 | 启动前检查数据覆盖；运行中不可静默，记录并禁用该分钟 P0 或按配置 fail-fast |
| V25 模型文件缺失 | early/late `.pt` 不存在 | 旧版静默 TWAP，当前 fail-fast | 必须 fail-fast，不允许 TWAP |
| V25 plan 未生成 | `_current_plan is None` / `plan is None` | 旧版静默 TWAP | 必须 fail-fast，除非明确是停牌/不可交易业务状态 |

## 6. Loop4 失败关联结论

实验：

- `qe_20260426_234914_9c7b`

Loop4：

- 使用 V25。
- `hold_thresh=5`
- `label_horizon=5`
- 配置中策略类为 `TailTWAPWithV25TwoStageStrategy`。

失败关键错误：

```text
RuntimeError: missing prev_close for V25 plan: stock=688143.SH prev_close=nan
```

已核查市场数据：

- `market.suspend_d` 中 `688143.SH` 在 2024-11-01、2024-11-04、2024-11-05 有停牌记录。
- 对应日期 `market.stk_limit` 中 `pre_close` 为 `NaN`。

结论：

- Loop4 不是因为 V25 使用旧版本失败。
- Loop4 使用的是当前严格 V25 权威模板。
- 失败本质是当前包装层把停牌/缺前收盘价这种正常市场状态当成程序错误。

## 7. 哪些内容不应回滚

以下内容不建议回滚：

1. 不应整体恢复 `v25_two_stage_executor.py.backup`。
   - 原因：它允许模型缺失时静默 TWAP fallback。
2. 不应回滚 V25 88.79% / 11.21% 权重修复。
   - 原因：这是 V25 执行计划修复的核心目标。
3. 不应恢复“模型缺失仍继续跑”的行为。
   - 原因：这会造成 UI 选择 V25，但实际执行 TWAP。
4. 不应直接修改数据库中的模型资产或 `.pt` 权重文件来规避当前问题。
   - 原因：问题在 QE/Qlib 包装层和执行框架，不是模型权重资产。

## 8. 哪些内容需要回滚或修复

### 8.1 需要回滚的错误行为

需要回滚“正常市场状态直接导致 Loop 崩溃”的行为，包括：

- 停牌日 `prev_close=NaN` 直接 `RuntimeError`。
- 停牌/临停分钟 close 缺失直接 `RuntimeError`。
- P0 所需涨跌停价缺失时直接让整个 Loop 失败。

这里的“回滚”不是回到静默 TWAP，而是回滚错误的异常分类。

### 8.2 需要保留的严格行为

必须保留或加强：

- UI 选择 `V25_TWO_STAGE` 时必须真实加载 V25 模型。
- V25 模型文件缺失必须 fail-fast。
- CUDA / torch / 模型结构不匹配必须 fail-fast。
- V25 plan 权重不符合预期必须 fail-fast。
- 配置层和执行层不一致必须 fail-fast。
- 不允许在用户选择 V25 时静默改成 TWAP。

### 8.3 需要新增的显式业务状态

建议至少新增以下状态码或诊断 reason：

- `suspended_by_suspend_d`
- `suspended_by_exchange`
- `intraday_halt_or_no_bar`
- `limit_up_buy_blocked`
- `limit_down_sell_blocked`
- `p0_limit_buy_at_down_limit`
- `p0_limit_sell_at_up_limit`
- `limit_price_missing`
- `prev_close_missing_with_suspend`
- `prev_close_missing_data_error`
- `v25_plan_missing_config_error`
- `v25_model_missing_config_error`

这些状态必须写入日志和 artifacts，避免“没有报错但业务逻辑被静默修改”。

## 9. 建议修复方案

### 9.1 QE 信号生成阶段

增加可配置项：

- `filter_suspended_on_signal: true | false`
- `suspend_filter_source: market.suspend_d`
- `suspend_filter_strict: true | false`

行为：

- 开启后，日频选股信号生成时剔除当日已确认停牌股票。
- 被剔除股票保留 trace，记录原始排名、剔除原因、数据来源。
- 如果配置要求 strict，但 `suspend_d` 数据覆盖不完整，应 fail-fast。

### 9.2 QE 回测执行阶段

执行阶段也必须使用同一份停牌信息：

- 不允许 UI 显示已启用停牌过滤，但执行阶段仍对停牌股票生成订单。
- 对已停牌股票，执行阶段应跳过或保留未成交，并记录 reason。
- 对临停/分钟无 bar，按当前分钟不可交易处理，不应让整个 Loop 失败。

### 9.3 suspend_d artifact

建议每个 QE workspace 生成固定 artifact：

- `qe_suspend_filter.json`
- 或 `qe_suspend_filter.parquet`
- 或 H5 格式，如果回测侧读取 H5 更高效。

建议字段：

- `trade_date`
- `symbol`
- `suspend_type`
- `suspend_timing`
- `source_table`
- `source_refresh_audit_id`
- `generated_at`
- `sha256`

选择建议：

- 小规模实验可用 JSON，便于排查。
- 大范围回测或多 Loop 共享建议 Parquet/H5，减少运行时 DB 访问。
- 无论格式如何，workspace 内必须保留 hash 和生成配置。

### 9.4 V25 包装层错误边界

应拆分三类判断：

1. 配置错误：
   - 模型路径不存在。
   - torch / CUDA 不可用。
   - plan 生成失败。
   - 权重不符合 V25 设计。
   - 处理方式：fail-fast。

2. 市场不可交易：
   - 停牌。
   - 临停。
   - 买入涨停无法成交。
   - 卖出跌停无法成交。
   - 处理方式：跳过当前分钟或保留未成交，记录 reason。

3. 数据质量错误：
   - 数据应存在但缺失，且无停牌/不可交易证据。
   - 处理方式：根据 strict 配置 fail-fast，或显式记录为数据缺口并终止该标的执行，不能静默。

### 9.5 版本一致性

修复后必须同步：

- `scripts/tail_twap_v25_strategy.py`
- `F:\Dev\RD-Agent-main\rdagent\scenarios\qlib\experiment\factor_template\tail_twap_v25_strategy.py`
- 相关测试 fixture / workspace 注入逻辑

并且记录：

- 文件 hash。
- 提交号。
- 是否影响新实验。
- 是否影响旧实验恢复。

旧 workspace 已复制的 `tail_twap_v25_strategy.py` 不会自动随模板变化；恢复旧实验时必须明确选择：

- 使用旧 workspace 文件复现实验。
- 或重新注入当前修复后的权威模板并记录为恢复迁移。

## 10. 验证用例清单

### 10.1 单元测试

至少覆盖：

1. UI / API 配置选择 `V25_TWO_STAGE` 后，生成 conf 中必须是 `TailTWAPWithV25TwoStageStrategy`。
2. V25 模型缺失时不能 fallback TWAP。
3. `prev_close=NaN` 且 `suspend_d` 确认停牌时，不应抛程序错误。
4. `prev_close=NaN` 且无停牌证据时，应 fail-fast 为数据错误。
5. 买入遇涨停时，当前分钟不成交并记录 `limit_up_buy_blocked`。
6. 卖出遇跌停时，当前分钟不成交并记录 `limit_down_sell_blocked`。
7. 买入遇跌停时，P0 全量尝试并记录 `p0_limit_buy_at_down_limit`。
8. 卖出遇涨停时，P0 全量尝试并记录 `p0_limit_sell_at_up_limit`。
9. P0 涨跌停价缺失时不能静默跳过，必须有 diagnostics。
10. `plan is None` 时必须作为 V25 plan 生成失败，不允许 TWAP。

### 10.2 集成测试

至少覆盖：

1. 使用包含停牌股票的真实日期运行 QE 回测。
2. 使用 `688143.SH` 在 2024-11-01、2024-11-04、2024-11-05 的停牌样本验证不崩溃。
3. 验证 diagnostics 中能看到停牌剔除或不可交易原因。
4. 验证 V25 plan 权重仍为 88.79% / 11.21%。
5. 验证持仓周期 1/3/5/10 的 `hold_thresh` / `label_horizon` 确实进入 conf 和执行。
6. 验证恢复实验时不会静默替换策略文件。

### 10.3 UI 测试

必须使用开发端口，不得使用生产端口：

- 后端：`8011` 或 `8012`
- 前端：`3011` 或 `3012`

验证项：

1. UI 中执行策略选择 V24/V25 后，详情页显示和后端 conf 一致。
2. UI 中停牌过滤开关启用/禁用后，执行结果和 diagnostics 一致。
3. Loop 详情中显示 V25 plan / 停牌 / 涨跌停处理 trace。
4. 前端不能只显示配置已启用，但后端实际未执行。

## 11. 禁止事项

1. 禁止为了让回测通过而把 V25 静默降级为 TWAP。
2. 禁止把正常涨跌停/停牌当作程序异常导致 Loop 失败。
3. 禁止直接修改已入库的策略/模型资产来掩盖框架问题。
4. 禁止在恢复旧实验时静默替换 workspace 中的策略文件。
5. 禁止使用生产端口 `8001` / `3000` 做开发验证或重启。
6. 禁止只修复 AIstock 一侧而不同步 RD-Agent 权威模板。

## 12. 建议后续执行顺序

1. 固化当前审计结论，确认需要修复的是框架语义而不是模型资产。
2. 在 QE 配置层加入 `suspend_d` 信号过滤开关和 strict 数据检查。
3. 在 QE workspace 生成 suspend artifact，避免回测过程中频繁访问数据库。
4. 修复 V25 包装层市场状态分类，取消正常市场状态的错误 `RuntimeError`。
5. 保留 V25 模型/plan/config 失败的 fail-fast。
6. 同步 AIstock 与 RD-Agent 模板。
7. 增加单元测试、集成测试和 UI E2E。
8. 使用 `8011/3011` 或 `8012/3012` 验证，不触碰生产端口。
9. 通过真实实验恢复/新建实验验证 V24/V25 和不同持仓周期。
10. 完成后再通知用户由用户决定是否重启生产服务。

