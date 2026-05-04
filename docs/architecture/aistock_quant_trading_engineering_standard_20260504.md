# AIstock 量化与交易系统工程规范 v0

> 日期：2026-05-04
> 状态：v0，作为 QE、Qlib、数仓、Paper Trading v2、Selection Center、HMM、未来实盘前置验证的专项工程规范。
> 文档位置：`docs/architecture/aistock_quant_trading_engineering_standard_20260504.md`
> 关联文档：`docs/architecture/aistock_python_development_standard_20260504.md`、`docs/architecture/aistock_development_standards_and_guardrails_20260504.md`

## 1. 目标

AIstock 是量化研究、实验、回测、模拟盘和未来实盘前置系统，工程规范必须高于普通 Web 系统。所有核心链路都要证明：数据口径正确、实验可复现、成本和持仓可对账、回测和模拟/实盘边界清晰、缺数据时 fail-fast。

## 2. 外部参考和采用方式

| 来源 | AIstock 采用方式 |
|---|---|
| Qlib Recorder | 采用 Experiment -> Recorder/run 的实验管理思想；补充 AIstock loop、配置快照、artifact manifest、成本、持仓和执行事件。 |
| Qlib Record Templates | 参考 Signal/IC/backtest record 分层；AIstock 必须额外保存 absolute return、with/without cost、positions、trades、config。 |
| QuantConnect LEAN | 借鉴研究、回测、组合、证券、事件引擎和实盘路径的清晰边界。 |
| NautilusTrader | 借鉴 backtest/live 一致性、事件驱动、执行适配器测试、capability matrix、可观测行为验证。 |
| Freqtrade | 借鉴 backtest summary、starting/final balance、profit、timerange、export result、dry-run/live 边界。 |
| Hummingbot | 借鉴 connector/controller/executor 模块化，但不照搬 crypto market making 假设。 |

## 3. QE 实验与 loop 数据规范

每个实验和 loop 必须形成可复现快照：

- experiment/task id、loop index、experiment type、创建时间、完成时间、状态、节点来源。
- 模型类型、模型版本、训练参数、超参数、随机种子、label horizon、数据频率、数据集 snapshot。
- 因子列表、因子版本、因子分类、因子独立指标、相关性矩阵版本。
- effective strategy config：topk、n_drop、持仓数量、每日换股数量、最短持股时间、初始资金、成本、benchmark、limit/suspend 处理。
- 训练过程：loss、valid metric、early stop、best iteration、特征/因子权重或 attribution，LSTM 等深度模型需要保存可支持的 attribution schema。
- 回测结果：期初/期末资产、绝对收益、超额收益、with/without cost、最大回撤、Sharpe、turnover、cost、benchmark、收益曲线、回撤曲线。
- 持仓和交易：position summary、holding audit、stock trades、order/fill/execution events。
- artifact manifest：URI、sha256、size、row_count、schema_version、created_at、source、是否可清理。

## 4. 回测与实验可复现

- QE 创建阶段必须保存 requested config 和 effective config，不能只保存用户显式填写字段。
- 默认参数必须被展开并快照；后续默认值改变不能影响历史实验解释。
- backtest-only loop 必须记录训练来源：source task、source loop、model artifact、训练指标引用。
- 实验完成后的指标采集必须通过 API 或已入库 payload，不得由 Windows backend 直接读 worker workspace。
- 任何 `complete` 状态都必须满足 required sections；否则必须是 `partial` 并列出缺失字段。

## 5. 数据口径和 PIT 原则

- 交易日历、股票池、ST/停牌/涨跌停、行业、指数、复权因子必须有 PIT 语义。
- Qlib adjusted price 与原始涨跌停价格比较时必须显式转换口径，不得默认 `factor=1`。
- 研究数据和交易可买卖资格必须分离；不能因为未来状态删除历史事实。
- 日频回测如果未处理涨跌停/停牌，不得作为 QE 优先级排序的权威结果。
- 数据缺失时必须区分 `data_error`、`env_error`、`contract_error`、`business_error`。

## 6. 成本、现金、持仓和订单对账

必须支持以下对账：

- report cost 与现金扣费路径一致。
- 期初资产、期末资产、现金、持仓市值、NAV 可重算。
- 订单意图、子单、成交、未成交原因、tail-substitute 候选和最终成交方向/金额可追踪。
- 最短持股时间、持仓数量、每日换股数量、换手率必须从实际持仓/交易推导核对。
- 缺订单或缺持仓明细不能显示为完整成功。

## 7. Paper Trading v2 和未来实盘边界

- Paper v2 不得使用 QE 回测 pred.pkl 作为权威 live selection；必须通过 StrategyPackage/Selection runtime 重新生成。
- Paper v2 缺分钟线、pre_close、limit、suspend、Torch/context、HMM coefficient 时必须 fail-fast。
- 回放、catch-up、live session 必须有独立状态、幂等 run、session lock、cancel/timeout。
- 执行算法必须有 capability matrix，说明支持的订单类型、时间粒度、市场状态、拒单和撤单语义。
- 实盘前必须证明 backtest/paper/live 共享关键 contract，不能各自隐藏 fallback。

## 8. 数仓和归档独立性

- QE 源 DB 和 worker workspace 未来可清理，因此数仓必须能独立回答历史实验详情。
- 小型结构化指标可在 QE DB 和数仓重复保存；大明细不应塞入 QE DB 大 JSON。
- 大持仓/交易/曲线/训练 trace 优先 artifact store + manifest；数仓按分析需求抽取结构化索引。
- 入仓后必须支持 source cleanup simulation：源 workspace/源 QE DB 不可用时，数仓仍能展示完整历史详情。
- 历史补录和新增 loop 补录都必须通过 API 完成，不依赖手工脚本。

## 9. UI 和分析可用性

- UI loop 详情必须显示所有增强指标缺失原因，不能用空值伪装成功。
- 所有策略可调参数应在总览中显示实际值。
- 图表应能比较实验、loop、模型、因子、时间窗口、成本前后、风险收益。
- 因子分析应支持“某因子参与过哪些实验、对应指标如何、与哪些因子相关、在什么组合中表现更好”。
- LLM agent 只能读取受控、只读、可审计的数据视图，不得直接读取 workspace 或执行任意 SQL/shell。

## 10. 测试设计

| 用例 | 层级 | 验证内容 | 自动化路径 |
|---|---|---|---|
| QT-STD-001 | L1/L2 | completion payload required schema 和 partial/complete 语义 | `qe_data_contract_backend` |
| QT-STD-002 | L2 | artifact manifest 拒绝 worker raw path | completion contract tests |
| QT-STD-003 | L2/L3 | 成本、现金、持仓、订单可对账 | 后续 QE/Paper data-quality smoke |
| QT-STD-004 | L3/L4 | source cleanup 后数仓独立可查 | QE archive independence plan |
| QT-STD-005 | L0 | 禁止 WSL/远端 workspace 直读和交易 fallback | `aistock_guardrail_scan.py` |

## 11. 第一阶段不做

- 不直接启用 QE 实时生产 hook。
- 不清理 QE workspace 或 QE DB 历史记录。
- 不直接做 LLM agent 自动演进。
- 不一次性实现所有模型 attribution；先保留 schema 和 LGB/LSTM 最小路径。
- 不引入新的 NoSQL/复杂存储系统。

## 12. 外部参考

- Qlib Recorder: https://github.com/microsoft/qlib/blob/main/docs/component/recorder.rst
- Qlib code standard: https://github.com/microsoft/qlib/blob/main/docs/developer/code_standard_and_dev_guide.rst
- QuantConnect LEAN algorithm engine: https://www.quantconnect.com/docs/v2/writing-algorithms/key-concepts/algorithm-engine
- NautilusTrader execution testing: https://nautilustrader.io/docs/latest/developer_guide/spec_exec_testing/
- NautilusTrader testing guide: https://nautilustrader.io/docs/nightly/developer_guide/testing/
- Freqtrade backtesting: https://docs.freqtrade.io/en/stable/backtesting/
- Hummingbot documentation: https://hummingbot.org/docs/
