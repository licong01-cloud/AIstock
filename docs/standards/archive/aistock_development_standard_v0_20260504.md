# AIstock 项目开发规范 v0

> 日期：2026-05-04
> 状态：v0，作为 AIstock 唯一项目级开发规范入口；后续依据全仓 baseline scan 和模块治理结果迭代。
> 文档位置：`docs/architecture/aistock_development_standard_20260504.md`
> 关联文档：`docs/architecture/aistock_development_standards_and_guardrails_20260504.md`

## 1. 定位

AIstock 不再拆分“Python 开发规范”和“量化开发规范”。本项目只有一个开发规范入口：本文档。原因是 AIstock 的 Python 代码大多直接服务于量化研究、QE 实验、数仓、Paper Trading v2、HMM、Qlib 数据链路和未来实盘前置验证，普通 Python 工程规则必须和量化业务红线一起执行。

本文档用于统一：

- Python 代码风格、类型、异常、资源、配置、测试。
- 量化实验、回测、交易执行、数据口径、数仓归档、可复现要求。
- Guardrail 机器规则、全仓 baseline、changed-files 门禁和 agent 修复流程。

## 2. 外部参考

| 来源 | AIstock 采用方式 |
|---|---|
| PEP 8 / PEP 257 / PEP 484 | 作为 Python 命名、docstring、类型标注的基础，不做全仓一次性重排。 |
| Google Python Style Guide | 采用其异常、全局状态、线程/并发、类型和可维护性建议。 |
| Black / Ruff / mypy / pytest | 作为后续格式、lint、类型和测试工具链；新增/修改代码优先执行。 |
| Qlib Recorder / code standard | 采用 Experiment -> Recorder/run 思想；补充 AIstock loop、配置快照、artifact manifest、成本、持仓和执行事件。 |
| QuantConnect LEAN | 借鉴研究、回测、组合、证券、事件引擎和实盘路径的清晰边界。 |
| NautilusTrader | 借鉴 backtest/live 一致性、事件驱动、执行适配器测试、capability matrix、可观测行为验证。 |
| Freqtrade / Hummingbot | 借鉴 backtest summary、starting/final balance、profit、timerange、dry-run/live、connector/controller/executor 模块化。 |

## 3. 风险等级

| 等级 | 处理方式 | 示例 |
|---|---|---|
| P0 | 新代码立即阻断；历史问题优先修复或隔离 | 静默返回成功、生产端口操作、直接访问 WSL/远端 workspace、交易 fallback 改变结果。 |
| P1 | 新代码阻断；历史问题进入高优治理 | DB schema 无 comment、artifact 只有路径无 manifest、缺 timeout、缺幂等性。 |
| P2 | 新代码 warning 或按模块阻断；历史分批治理 | 大函数、弱类型、raw dict 横传、UI raw JSON、测试覆盖不足。 |
| P3 | 记录和趋势治理 | 命名不一致、局部风格不统一、注释不足。 |

## 4. Python 工程规范

### 4.1 代码组织

- router 只做 API contract、鉴权/确认文本和请求响应映射；service 做业务逻辑；repository/data access 做持久化。
- service 不得隐式执行 DDL；DB schema 由 migration/bootstrap 管理，并带表字段 comment。
- CLI 脚本必须拆分参数解析、业务执行、输出写入，方便单元测试。
- 高风险业务逻辑必须可注入依赖，不直接绑定全局 DB、全局 HTTP client、全局 clock。
- 新增大型模块必须同步设计文档、测试矩阵、run evidence 策略。

### 4.2 命名、类型和数据结构

- 函数、变量、模块使用 `snake_case`；类使用 `PascalCase`；常量使用 `UPPER_SNAKE_CASE`。
- API request/response、DB DTO、config、manifest、run metadata 应使用 Pydantic model、dataclass 或明确 TypedDict。
- 禁止在核心业务中长期传递未定义 schema 的大 dict；必须使用 JSON 时应有 schema/version/source/quality 字段。
- 新增公共函数应标注参数和返回类型；高风险分支的 `Any` 必须有注释或后续治理任务。
- 时间、金额、比例、收益率字段必须在命名或注释中说明单位和口径。

### 4.3 错误处理

P0 禁止：

```python
try:
    do_business_work()
except Exception:
    return []  # forbidden
```

要求：

- 业务错误必须 fail-fast，返回结构化错误码和可操作上下文。
- 不能用空数组、`None`、`True`、默认 0、默认价格、默认资金伪装成功。
- 允许兜底时必须满足：显式配置、可审计日志、UI 可见、测试覆盖、不会改变交易/回测语义。
- catch broad exception 时必须重新抛出业务异常或记录 `partial/failed` 状态，不得吞掉。
- parser 可以收集多条错误，但最终必须给出 `complete/partial/failed`，不能默默丢字段。

### 4.4 日志、配置和资源

- 长任务必须记录 `run_id/task_id/loop_index/step/status/duration`。
- 日志不得泄露 token、密码、数据库连接串。
- 禁止硬编码本机路径、用户目录、生产端口、远端 worker 路径、密钥。
- 路径和端口必须来自配置、环境变量、DB catalog、manifest 或 API request。
- Windows 侧 backend 不得直接访问 WSL/远端 worker workspace；必须通过 API 或 AIstock-owned artifact store。
- HTTP 请求、subprocess、DB 查询、文件读写必须有 timeout 或上下文管理。
- 大 DataFrame/CSV/parquet 处理必须考虑 chunk/batch；禁止无边界全量读大文件。
- 全局 cache 必须有 max size、TTL、clear 或生命周期说明。

## 5. 量化与交易工程规范

### 5.1 QE 实验与 loop 数据

每个实验和 loop 必须形成可复现快照：

- experiment/task id、loop index、experiment type、创建时间、完成时间、状态、节点来源。
- 模型类型、模型版本、训练参数、超参数、随机种子、label horizon、数据频率、数据集 snapshot。
- 因子列表、因子版本、因子分类、因子独立指标、相关性矩阵版本。
- effective strategy config：topk、n_drop、持仓数量、每日换股数量、最短持股时间、初始资金、成本、benchmark、limit/suspend 处理。
- 训练过程：loss、valid metric、early stop、best iteration、特征/因子权重或 attribution，LSTM 等深度模型需要保存可支持的 attribution schema。
- 回测结果：期初/期末资产、绝对收益、超额收益、with/without cost、最大回撤、Sharpe、turnover、cost、benchmark、收益曲线、回撤曲线。
- 持仓和交易：position summary、holding audit、stock trades、order/fill/execution events。
- artifact manifest：URI、sha256、size、row_count、schema_version、created_at、source、是否可清理。

### 5.2 回测与实验可复现

- QE 创建阶段必须保存 requested config 和 effective config，不能只保存用户显式填写字段。
- 默认参数必须被展开并快照；后续默认值改变不能影响历史实验解释。
- backtest-only loop 必须记录训练来源：source task、source loop、model artifact、训练指标引用。
- 实验完成后的指标采集必须通过 API 或已入库 payload，不得由 Windows backend 直接读 worker workspace。
- 任何 `complete` 状态都必须满足 required sections；否则必须是 `partial` 并列出缺失字段。

### 5.3 数据口径和 PIT 原则

- 交易日历、股票池、ST/停牌/涨跌停、行业、指数、复权因子必须有 PIT 语义。
- Qlib adjusted price 与原始涨跌停价格比较时必须显式转换口径，不得默认 `factor=1`。
- 研究数据和交易可买卖资格必须分离；不能因为未来状态删除历史事实。
- 日频回测如果未处理涨跌停/停牌，不得作为 QE 优先级排序的权威结果。
- 数据缺失时必须区分 `data_error`、`env_error`、`contract_error`、`business_error`。

### 5.4 成本、现金、持仓和订单对账

- report cost 与现金扣费路径必须一致。
- 期初资产、期末资产、现金、持仓市值、NAV 必须可重算。
- 订单意图、子单、成交、未成交原因、tail-substitute 候选和最终成交方向/金额必须可追踪。
- 最短持股时间、持仓数量、每日换股数量、换手率必须从实际持仓/交易推导核对。
- 缺订单或缺持仓明细不能显示为完整成功。

### 5.5 Paper Trading v2 和未来实盘边界

- Paper v2 不得使用 QE 回测 pred.pkl 作为权威 live selection；必须通过 StrategyPackage/Selection runtime 重新生成。
- Paper v2 缺分钟线、pre_close、limit、suspend、Torch/context、HMM coefficient 时必须 fail-fast。
- 回放、catch-up、live session 必须有独立状态、幂等 run、session lock、cancel/timeout。
- 执行算法必须有 capability matrix，说明支持的订单类型、时间粒度、市场状态、拒单和撤单语义。
- 实盘前必须证明 backtest/paper/live 共享关键 contract，不能各自隐藏 fallback。

### 5.6 数仓和归档独立性

- QE 源 DB 和 worker workspace 未来可清理，因此数仓必须能独立回答历史实验详情。
- 小型结构化指标可在 QE DB 和数仓重复保存；大明细不应塞入 QE DB 大 JSON。
- 大持仓/交易/曲线/训练 trace 优先 artifact store + manifest；数仓按分析需求抽取结构化索引。
- 入仓后必须支持 source cleanup simulation：源 workspace/源 QE DB 不可用时，数仓仍能展示完整历史详情。
- 历史补录和新增 loop 补录都必须通过 API 完成，不依赖手工脚本。

## 6. UI 和分析可用性

- UI loop 详情必须显示所有增强指标缺失原因，不能用空值伪装成功。
- 所有策略可调参数应在总览中显示实际值。
- 图表应能比较实验、loop、模型、因子、时间窗口、成本前后、风险收益。
- 因子分析应支持“某因子参与过哪些实验、对应指标如何、与哪些因子相关、在什么组合中表现更好”。
- LLM agent 只能读取受控、只读、可审计的数据视图，不得直接读取 workspace 或执行任意 SQL/shell。

## 7. 测试与提交流程

新增/修改代码提交前至少执行：

```powershell
python -m compileall <changed-python-paths>
python -m pytest <targeted-tests> -q -p no:cacheprovider
python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
```

涉及 UI/API/DB/QE/Paper 的变更还必须执行对应 nox session 和 run evidence。

高风险代码 coverage 第一阶段目标：

- 新增/修改 line >= 80%。
- 新增/修改 branch >= 70%。
- QE 数据完整性、warehouse/archive、交易执行、成本/ledger、HMM、cleanup gate 必须有 L1/L2 和业务 oracle。

## 8. 第一阶段不做

- 不对全仓历史代码一次性 Black 格式化。
- 不要求历史代码立刻补满类型标注。
- 不把 P2/P3 历史问题作为第一版阻断项。
- 不直接启用 QE 实时生产 hook。
- 不清理 QE workspace 或 QE DB 历史记录。
- 不直接做 LLM agent 自动演进。
- 不一次性实现所有模型 attribution；先保留 schema 和 LGB/LSTM 最小路径。
- 不引入新的 NoSQL/复杂存储系统。

## 9. 测试设计

| 用例 | 层级 | 验证内容 | 自动化路径 |
|---|---|---|---|
| DEV-STD-001 | L0 | 规范文档可读取、引用路径正确 | UTF-8/read check |
| DEV-STD-002 | L0 | 机器规则能识别静默 fallback、路径红线 | `backend/tests/test_aistock_guardrail_scan.py` |
| DEV-STD-003 | L0 | changed-files 扫描可输出 JSON/MD | `scripts/aistock_guardrail_scan.py` |
| DEV-STD-004 | L0 | baseline scan 不写业务数据、不启动服务 | 只读命令和 evidence |
| DEV-STD-005 | L1/L2 | completion payload required schema 和 partial/complete 语义 | `qe_data_contract_backend` |
| DEV-STD-006 | L2 | artifact manifest 拒绝 worker raw path | completion contract tests |
| DEV-STD-007 | L2/L3 | 成本、现金、持仓、订单可对账 | 后续 QE/Paper data-quality smoke |
| DEV-STD-008 | L3/L4 | source cleanup 后数仓独立可查 | QE archive independence plan |

## 10. 外部参考

- PEP 8: https://peps.python.org/pep-0008/
- PEP 257: https://peps.python.org/pep-0257/
- PEP 484: https://peps.python.org/pep-0484/
- Google Python Style Guide: https://google.github.io/styleguide/pyguide.html
- Black code style: https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html
- Ruff linter: https://docs.astral.sh/ruff/linter/
- mypy: https://mypy.readthedocs.io/en/stable/
- pytest good integration practices: https://docs.pytest.org/en/stable/explanation/goodpractices.html
- Qlib Recorder: https://github.com/microsoft/qlib/blob/main/docs/component/recorder.rst
- Qlib code standard: https://github.com/microsoft/qlib/blob/main/docs/developer/code_standard_and_dev_guide.rst
- QuantConnect LEAN algorithm engine: https://www.quantconnect.com/docs/v2/writing-algorithms/key-concepts/algorithm-engine
- NautilusTrader execution testing: https://nautilustrader.io/docs/latest/developer_guide/spec_exec_testing/
- NautilusTrader testing guide: https://nautilustrader.io/docs/nightly/developer_guide/testing/
- Freqtrade backtesting: https://docs.freqtrade.io/en/stable/backtesting/
- Hummingbot documentation: https://hummingbot.org/docs/
