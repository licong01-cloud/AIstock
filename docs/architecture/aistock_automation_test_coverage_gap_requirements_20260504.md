# AIstock 自动化测试覆盖缺口与流水线研发需求

> 日期：2026-05-04  
> 目的：提交给自动化测试流水线研发，补齐“测试通过但用户基本功能仍失败”的覆盖缺口。  
> 定位：这是测试流水线研发需求和覆盖缺口分析，不替代 `docs/standards/aistock_development_standard_v1.1_20260504.md` 作为项目规范源。

## 1. 背景

近期 Paper v2、Selection Center、StrategyPackage、QE 实验和运行时资产链路连续暴露多个“用户点击基本功能即失败”的问题：

- 从 QE 创建策略包时，历史/新实验缺少分钟线运行时契约字段，触发 `STRATEGY_PACKAGE_VALIDATION_ERROR`。
- 策略包卡片的状态、选股准入、模拟盘准入、退役语义在 UI 上不够清晰，导致操作含义不透明。
- 用户点击真实策略包选股时，`qe_20260413_084216/Loop1` 因节点 API 缺少 `combined_factors_df.parquet` 而失败。
- 活跃模拟组合总览曾因对大量组合做前端扇出请求而出现加载慢或 `Failed to fetch`，后续虽增加 running-summary 聚合，但仍需要持续用真实数据量回归分页、筛选、排序。

这些问题说明：当前测试不是没有，而是存在严重的“覆盖证明不足”。单元测试、构建测试、mock UI、API 可达性、负向 fail-fast 测试，不能自动等价于真实用户业务路径通过。

## 2. 核心问题结论

### 2.1 “通过”的含义被混用

当前历史记录中存在多种不同级别的通过：

| 记录类型 | 能证明什么 | 不能证明什么 |
|---|---|---|
| `npm run build` / TypeScript 通过 | 前端可编译 | 用户点击真实按钮后业务成功 |
| mock Playwright UI smoke 通过 | 页面文案、布局、按钮状态在模拟 API 下正确 | 真实后端、DB、副作用、资产链路正确 |
| 后端单元测试通过 | 某个服务函数或错误分支正确 | 真实 UI + 真实 DB + 真实节点 API 完整成功 |
| API 200 / openapi 可达 | 服务可访问 | 返回内容满足业务目标 |
| fail-fast 负向测试通过 | 缺数据时不会假成功 | 正向路径可成功生成业务结果 |
| 历史 L3 通过 | 某个 commit、某个时间点、某组数据通过 | 后续改动和当前资产状态仍通过 |

流水线必须强制记录“本次通过的确切含义”，禁止把低层级通过描述成完整业务可用。

### 2.2 真实资产状态没有被持续纳入回归

例如 `qe_20260413_084216/Loop1`：

- 节点 API 中 `conf.yaml` 可下载。
- 因子源码可下载。
- mlruns 参数包可下载。
- 但 `combined_factors_df.parquet` 返回 404。

之前测试没有覆盖“历史 QE 实验缺失 StaticDataLoader schema parquet，但 DB 中有 `qe_experiments.factor_names` 且因子源码可用”的真实历史资产场景。用户点击选股时才触发。

### 2.3 Mock UI 被用来证明交互，而不是证明业务

例如策略包状态能力卡片验证记录明确属于 mocked API smoke。它能证明：

- 状态卡显示。
- 按钮文案清晰。
- 禁用态和退役确认 UI 存在。

但它不能证明：

- 真实策略包状态转换写入 DB。
- 真实策略包可生成 selection run。
- 真实策略包可创建 Paper v2 portfolio。
- 真实错误上下文能从后端完整透传到 UI。

流水线需要把 mock UI 定义为 L1/L2，不允许它替代真实业务验收。

### 2.4 负向 fail-fast 覆盖不足以代表产品可用

AIstock 的安全原则要求禁止 silent fallback。很多测试证明“缺资产时报错”，这是必要的。但用户关心的是基本功能能否成功，例如：

- 创建策略包。
- 启用选股。
- 运行选股。
- 从选股创建模拟组合。
- 运行/回放组合。
- 查看分页、筛选、排序后的真实组合列表。

流水线必须把“正向成功路径”设为 P0/P1 门禁，而不仅是错误路径。

## 3. 必须新增的通过等级定义

建议自动化测试流水线统一使用以下等级，并在 run record 中强制写入 `pass_scope`。

| 等级 | 名称 | 通过标准 | 可否声明功能可用 |
|---|---|---|---|
| L0 | 静态与守卫 | 编译、类型、lint、路径/secret/危险 fallback 扫描 | 否 |
| L1 | 单元/契约 | 单个函数、repository、service、数据契约通过 | 否 |
| L2 | 集成/API | 真实或测试 DB 下 API 通过，检查响应与 DB 副作用 | 仅限 API 层 |
| L3 | UI 真实流 | 真实后端 + 真实 DB + UI 点击 + DB/API 结果一致 | 可声明该 UI 流通过 |
| L4 | 业务价值链 | 真实资产/节点/数据源完整链路产生业务结果 | 可声明业务功能通过 |
| L5 | 发布候选 | 全模块关键路径、回归、性能、数据质量、资产安全均通过 | 可声明版本候选 |

禁止出现以下表述：

- “mock UI 通过，所以功能完成。”
- “API 返回 200，所以业务通过。”
- “fail-fast 正确，所以用户流程可用。”
- “历史 L3 通过，所以当前 commit 必然通过。”

## 4. 自动化流水线必须覆盖的真实用户关键路径

### 4.1 StrategyPackage 基本路径

每次涉及 StrategyPackage、QE runtime contract、选股、Paper v2 的改动，至少覆盖：

1. 从真实 QE 实验创建策略包。
2. 检查策略包 DB 行：
   - `source_type`
   - `source_id`
   - `loop_id`
   - `run_id`
   - `package_status`
   - `manifest_sha256`
   - runtime contract 字段，例如 `backtest_freq=1min`
3. 启用选股。
4. 启用模拟盘准入。
5. UI 状态和 DB 状态一致。
6. 错误时 UI 能显示结构化错误详情，不展示原始 JSON 作为主要视图。

### 4.2 Selection Center 真实选股路径

必须覆盖至少一组真实 QE 包：

- `qe_20260413_084216`
- 最近成功的分钟线 QE 实验。
- 至少一个故意缺资产的历史实验。

验证项：

1. 从 UI 或等价 API 触发单策略包选股。
2. 运行时资产通过节点 API 或 AIstock-owned cache 物化。
3. `factor_order.json` 生成且记录来源：
   - `qe_static_dataloader`
   - 或 `qe_experiments.factor_names_after_missing_static_loader`
4. 不能使用 QE backtest `pred.pkl` 作为权威当前选股结果。
5. selection run 成功持久化。
6. DB 中 selection results 非空，rank、score、symbol 合法。
7. UI 展示候选股和错误/排除原因。

### 4.3 Paper v2 模拟组合路径

必须覆盖：

1. 从单策略包选股结果创建模拟组合。
2. 检查 portfolio 冻结字段：
   - `package_id`
   - `manifest_sha256`
   - `initial_cash`
   - `start_date`
   - `data_source`
   - `execution_policy`
3. 就绪检查 readiness 能解释每个阻塞点。
4. 历史回放或单日运行产生：
   - run
   - run events
   - orders
   - fills
   - cash ledger
   - positions
   - snapshots
   - errors
5. UI 账本、绩效、运行控制台可读取真实持久化结果。

### 4.4 活跃模拟组合总览路径

该场景必须用真实或隔离生成的“大量组合”数据验证，不能只测少量 fixture。

覆盖要求：

1. 后端 `/api/v1/paper-v2/running-summary` 默认分页：
   - 默认每页 20。
   - 最大每页 50。
   - 超过最大值必须被拒绝或截断并记录。
2. UI 分页：
   - 下一页、上一页、第一页、末页。
   - 页码和总数正确。
3. 排序：
   - 状态。
   - 初始资金。
   - 最近运行时间。
   - 升序/降序。
4. 筛选：
   - 状态。
   - 组合 ID。
   - 策略包 ID/名称。
   - 数据源。
   - 执行策略。
   - 除组合名外的字段关键字。
5. 性能：
   - 不允许前端按组合扇出请求 runs/errors/snapshots。
   - 单次页面加载应主要依赖后端聚合接口。
   - 大量组合下不得出现 `net::ERR_INSUFFICIENT_RESOURCES` 或 `Failed to fetch`。

### 4.5 QE 实验数据写入路径

涉及 QE 创建、配置入库、数仓同步、策略包生成时，必须覆盖：

1. 新实验生成后立即检查 DB 写入：
   - minute `backtest_freq`
   - execution mode / runtime contract
   - HMM 配置
   - blacklist / stock pool 配置
   - factor list
   - enhanced metrics
   - artifact manifest / quality status
2. 历史实验补齐脚本：
   - dry-run。
   - 实际回补。
   - 回补前后差异报告。
   - 不能把日频历史实验错误标记为分钟线可执行。
3. 数仓模块文档和运行记录同步更新。
4. 新字段不能只写入 QE 端，必须验证下游 StrategyPackage、Selection Center、Paper v2 能读取。

## 5. 流水线需要新增的强制门禁

### 5.1 pass scope 门禁

每个 run record 必须包含：

```yaml
pass_scope:
  level: L0|L1|L2|L3|L4|L5
  real_backend: true|false
  real_database: true|false
  real_node_api: true|false
  real_frontend_click: true|false
  writes_business_state: true|false
  positive_business_success: true|false
  negative_failfast_only: true|false
  mock_api_used: true|false
  production_8001_touched: false
```

如果 `mock_api_used=true` 或 `positive_business_success=false`，报告不得写“功能完成”。

### 5.2 用户路径门禁

任何 UI 按钮新增或改名，流水线必须验证：

1. 按钮在真实状态下是否可见。
2. disabled 原因是否可读。
3. 点击后调用真实 API。
4. API 成功时 DB 状态变化正确。
5. API 失败时 UI 展示结构化错误。
6. 重新刷新页面后状态仍一致。

### 5.3 资产完整性门禁

涉及 QE / StrategyPackage / Selection / Paper v2 的正向测试必须检查：

- 不直接读取 WSL/RD-Agent worker workspace。
- 所需资产来自节点 API、AIstock-owned cache、DB 或正式 artifact store。
- 缺失资产不允许 silent fallback。
- 允许的 fallback 必须有明确业务依据、diagnostics 字段和测试覆盖。
- 原始 QE artifact、模型权重、HMM snapshot、StrategyPackage manifest、Paper ledger 不被测试静默修改。

### 5.4 当前 commit 门禁

历史通过记录只能作为参考。流水线必须在当前 commit 上重新执行对应路径，并记录：

- 当前 git commit。
- 当前 DB 样本 ID。
- 当前节点 API 状态。
- 当前 frontend/backend 端口。
- 当前 run record 路径。

不允许用旧 commit 的 L3 记录证明当前功能。

## 6. 测试数据策略

### 6.1 固定真实样本

维护一组“真实但受控”的样本：

| 样本类型 | 用途 |
|---|---|
| 完整分钟线 QE 实验 | 正向策略包、选股、Paper v2 路径 |
| 缺 StaticDataLoader parquet 的历史 QE 实验 | 历史资产兼容路径 |
| 缺模型参数的实验 | fail-fast 路径 |
| 缺因子源码的实验 | fail-fast 路径 |
| 大量 Paper v2 组合样本 | 分页、筛选、排序、性能 |
| HMM 系数完整样本 | HMM 正向调整 |
| HMM 系数缺失样本 | HMM fail-fast |

### 6.2 隔离写入策略

真实业务成功路径需要写 DB，但必须隔离：

- 使用 `E2E_` 或 `VALIDATION_` 前缀。
- 写入 run record 中的 created IDs。
- 支持 cleanup 或软归档。
- 不能修改生产正在使用的真实策略包 manifest 或历史 ledger。

### 6.3 大数据量性能样本

活跃组合总览、running-summary、历史 run 列表必须有数据量门槛：

- 至少 120 个活跃组合。
- 至少 5 页分页。
- 至少覆盖 READY / RUNNING / PAUSED / FAILED 状态。
- 至少覆盖多个初始资金档位。
- 至少覆盖有 run 和无 run 的组合。

## 7. 报告模板要求

每个验证报告必须包含：

1. 本次验证是否证明真实业务成功。
2. 如果没有，明确写“未证明功能可用”。
3. 用了 mock 还是真实 API。
4. 是否写入业务 DB。
5. 写入了哪些 ID。
6. 是否重启或触碰生产 `8001`。
7. 失败是否是预期 fail-fast。
8. 正向路径是否非空、非默认、非假成功。
9. 真实 UI 是否点击。
10. 后端、DB、UI 三方证据是否一致。

建议增加报告字段：

```yaml
business_assertion:
  can_user_complete_operation: true|false
  operation_name: string
  evidence:
    ui: string
    api: string
    db: string
    logs: string
  unresolved_blockers:
    - string
```

## 8. 优先级整改清单

### P0：立即补齐

- StrategyPackage 从真实 QE 实验创建策略包的正向和失败路径。
- 单策略包真实选股成功路径，覆盖 `qe_20260413_084216` 等历史资产场景。
- Paper v2 从单策略包选股创建组合的正向路径。
- 活跃模拟组合总览分页、筛选、排序、大数据量性能验证。
- QE 新实验生成后 runtime contract 和全量配置入库检查。

### P1：短期补齐

- 历史 QE 实验回补脚本 dry-run/actual/backout 验证。
- 数仓入库与文档同步检查。
- HMM 启用/禁用/缺系数/完整系数路径。
- UI 错误详情从后端结构化透传的 Playwright 断言。
- 运行时缓存和受保护资产 diff 审计。

### P2：持续改进

- 覆盖率阈值和 diff coverage。
- Playwright trace、截图、console/request failure 自动归档。
- 性能预算：页面首屏、API 总耗时、DB 查询耗时、请求数。
- run record 自动生成和机器可读索引。

## 9. “功能完成”的新定义

对于 AIstock 的高风险业务模块，功能完成必须同时满足：

1. 当前 commit 通过对应 L0/L1。
2. 当前真实样本通过 L2 API/DB 检查。
3. 有 UI 的功能通过 L3 真实点击。
4. 涉及 QE、选股、Paper v2、HMM、执行算法的数据链路通过 L4 业务价值验证。
5. 负向 fail-fast 和正向成功路径都覆盖。
6. 报告明确列出未覆盖能力，不能用“通过”掩盖。
7. 未重启生产 `8001` 时，报告必须写明“生产服务需重启后才生效”。

## 10. 对自动化测试流水线研发的落地建议

建议按以下阶段实施：

1. **阶段 1：报告语义治理**
   - 增加 `pass_scope` schema。
   - 禁止 mock/负向/构建测试报告写“功能完成”。
   - 所有 run record 自动生成机器可读 JSON 索引。

2. **阶段 2：关键路径 E2E**
   - StrategyPackage -> Selection Center -> Paper v2 建立真实端到端流水线。
   - 固定真实 QE 样本和隔离 E2E 写入前缀。
   - UI/API/DB 三方断言。

3. **阶段 3：数据和资产契约**
   - QE runtime contract 入库检查。
   - 历史实验补齐检查。
   - runtime asset manifest / missing artifact matrix。

4. **阶段 4：性能与大数据量**
   - running-summary 大量组合验证。
   - 页面请求数门禁。
   - API/DB 查询耗时门禁。

5. **阶段 5：发布候选门禁**
   - 每次合并涉及高风险模块时自动选择对应 L3/L4。
   - 发布候选必须跑全模块最小业务价值链。

## 11. 本次事件的工程教训

本次连续暴露问题的关键教训是：

- 测试数量不等于覆盖质量。
- mock 通过不等于真实业务通过。
- fail-fast 通过不等于正向功能可用。
- 历史通过不等于当前 commit 通过。
- 资产链路、DB 写入、UI 点击和业务结果必须同时验证。

后续流水线的目标不是“让测试看起来更多”，而是让测试能回答一个明确问题：

> 用户现在打开页面，点击这个真实按钮，针对这个真实对象，能否完成该业务操作？如果不能，阻塞原因是什么，是否在 UI、API、DB、日志中一致可追踪？
