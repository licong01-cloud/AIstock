# LocalSIM 增量 TWAP 零成交量分钟修复验收

关联 BUG-1938、Issue #6186。本次修复共享 TradingCore 的增量分钟执行及 LocalSIM 等待原因识别，属于 BUG/T2，不调整选股、账户、资金和账本语义。

## 问题与修复

正式 LocalSIM 的 002889.SZ 买单尚余 74,900 股，持久化游标停在 10:35。原生 TDX 后续 10:36 分钟 OHLC 为 14.31、成交量为零，10:37 有量但仍处涨停。原实现将增量窗口的零成交量分钟视为数据不可用，阻止因果游标推进。

TWAP 现在显式声明零成交量处理能力。零量分钟仍须通过价格、涨跌停、标的及日期等原有校验，然后产生 ZERO_VOLUME / NO_FILL，推进算法步数和分钟游标，不产生成交。后续涨停仍走原有 LIMIT_UP_BUY_BLOCKED 等待逻辑。完整历史窗口全部零量仍报错，普通可交易股真缺价仍报错。

## 实现范围与边界

- risk.py 提取 validate_order_market_data，共用价格及涨跌停校验；普通 validate_order_execution_context 仍要求可执行分钟。
- minute_execution.py 仅为声明 HANDLES_ZERO_VOLUME 的算法，在增量模式放行合规零量分钟。原有标的、时间、重复 bar、行情完整性和停牌校验保留。
- twap_algo.py 在唯一 TWAP 实现中记录 ZERO_VOLUME、推进原有步数；不复制评分、调仓或执行算法，不改变拆单数量公式。
- LocalSIM runtime 识别 ZERO_VOLUME 等待原因，沿用原有持久化、重启、幂等及收盘残量规则。订单、持仓、资金、成交和账本接口不变。
- 未修改 Advisory、QE、StrategyPackage、HMM、数据模块、数据集、保护资产、数据库或 CI 选择规则；没有启动或重置生产回放。

## 验证与证据

最终定向矩阵命令：

`python -m pytest backend/tests/trading_core/test_minute_execution.py backend/tests/trading_core/test_risk.py backend/tests/simulation_execution/test_localsim_runtime.py -q -p no:cacheprovider --basetemp=X:/AIstock_temp/LOCALSIM/workflow/BUG-1938-pytest-directed-final --junitxml=X:/AIstock_temp/LOCALSIM/workflow/BUG-1938-directed-final-r2.xml`

结果为 **136 passed，5.38 秒**。该数字为最终实际修改范围的单轮结果，不与前轮重复计数。新增数据校验测试已放入所选 CI 实际执行的 test_minute_execution.py，test_risk.py 没有内容修改。较早 RTK 矩阵和迁移后的单点结果仅保留为历史证据；工作流不接受 RTK 验证命令前缀，故最终使用上述原始命令生成收据。

| 场景 | 结论 |
|---|---|
| 零量、混合分钟、后续有量、JSON 状态恢复、重复 bar | 正常等待或成交；幂等及因果身份保持 |
| 缺少 limit、坏价格、非有限价格或 limit | 继续严格拒绝 |
| 全日全部零量、原有涨跌停、停牌及日期身份 | 原有严格合同保持 |
| LocalSIM 健康 peer、持久化重启、同 bar 重复及冲突 | 定向矩阵通过 |
| 同一真实冻结输入的源码基线与候选对比 | 基线报原有 RiskRuleError；候选记录零量及涨停等待，游标推进至 10:37，零成交，剩余 74,900 股不变 |
| CI、后端加载及生产业务验收 | 以实际流水线和用户重启后的独立回执为准 |

原始冻结输入为 X:/AIstock_temp/LOCALSIM/workflow/BUG-1938-frozen-native-market-inputs-r1.json；基线和候选回执分别为 BUG-1938-native-frozen-baseline-r1.json、BUG-1938-native-frozen-candidate-r1.json。同一冻结 context hash 为 77b58b0b03f84424180c26177b04372a342bc3e1c3894f93760412520f97d259。脚本显式校验四个实际源码模块的加载位置与哈希；原生 context、订单及执行状态装配经过真实类型和身份校验；数据库访问被禁止，TDX 入口只读取本次冻结分钟。原失败 tick 的完整全量快照未留存，本证据是当前原生事实冻结后的离线源码对比，不能冒充原失败 tick 的完整生产复现或运行态验收。

changed-file Ruff 与 git diff --check 已通过。广回归交给 CI 的 paper_v2_backend、platform_api_backend、simulation_core_l2；更深模块验证按现有预算交给 Nightly。没有执行 QE 训练或扩大本地测试矩阵。

## 多轮审核与 DESIGN-COMPLIANCE-001

已完成三轮自审和修正：先核对市场不可成交与数据错误边界，再检查完整历史严格校验及停牌隔离，最后核对持久化、重启、幂等和 CI 真实测试覆盖。以下四项单独审核：

1. 逐项核对原生分钟执行合同与故障触发条件，以上定向矩阵及冻结输入比较覆盖实际修复点。
2. 没有以默认价格、伪造成交、宽泛异常捕获、放宽缺价或跨日复用替代合同要求。
3. 继续复用同一 TradingCore/TWAP，未复制选股、推理或业务容器；无数据库迁移和订单、持仓、资金、账本语义修改。
4. 源码 PR、CI、合入、用户重启、实际运行身份和生产业务验收分别记录；后者未通过前 Issue 和 close-sync 保持待验收。

技术验收不证明策略收益；回放未成交残量与程序错误须分别报告。
