# LocalSIM 消费公共停牌日权威合同（BUG-1953）

关联 Issue #6228。接续公共 BUG-1942 / BUG-1944 返回的有效市场状态，以及 LocalSIM 既有停牌估值和冻结输入合同。仅修改 LocalSIM 薄消费层及其自身选股身份，不修改数据、Advisory、QE、StrategyPackage、HMM、公共排序或 TopK 算法。

## 触发与实现

原 LSTM `lsreplay_81ca085e8ec56e05` 已完成 2026-01-21，持续在 01-22 重试。`603616.SH` 的原始 `market.suspend_d` 同日保留 S、R 两条记录。数据 owner 的 `project_suspension_days` 已依据官方披露返回 FULL_DAY_SUSPENDED，但原 LocalSIM 消费端要求单条原始 S，因而先报 `LOCALSIM_SUSPENSION_FACT_CONFLICT`。

1. 原始 S/R 事件不改、不排序推断。消费者调用唯一公共 `load_authority`、`project_suspension_days`；只有公共投影为 FULL_DAY_SUSPENDED 才进入原生全天停牌路径。UNRESOLVED 仍明确阻塞，普通无事件股票和合法日内停牌保持原交易价格要求。
2. 在 LocalSIM 自身 status / suspension / valuation 证据冻结公共 authority 原文、policy、authority 摘要、实现摘要、原始事件摘要、完整日投影、D 和 C，以及消费者 data identity。标识 `usage=D_EFFECTIVE_MARKET_STATE`；保留 `historical_publication_time_proven=false`，不声明其是 C 时点已知信息。
3. 默认规则为消费者进程实际加载的规则副本，文件后续更新不静默替换当前进程的规则。验证已有冻结证据只使用该证据自己的 authority 原文，重新调用同一个公共投影；拒绝输入、身份、实现或时钟冲突。旧的单 S 冻结证据保持旧验证语义，旧 S/R 证据不得借用当前规则补齐。
4. LocalSIM 的独立选股 request identity 加入实际加载的停牌 policy identity，输出 envelope 同时记录该身份。因此新规则不能复用旧独立 checkpoint / 过滤投影；仍复用原生完整评分、排序、过滤、递补和 TopK 代码。已完成日不重跑。
5. 全天停牌的资格判定不要求价格；持仓估值继续优先使用 D 日权威 pre_close，否则通过既有公共参考价入口取得 C 截止的真实历史 close 和完整可用性证明。普通可交易股的缺价检验不放宽，合法停牌但无可用估值证据仍阻塞。不新增补值、默认价格或股票/包 ID 白名单。

## 验收索引

| 要求 | 实现位置 | 证据与结论 |
|---|---|---|
| A01 使用唯一公共投影，不复制 S/R 判定 | LocalSIM suspension provider | 官方已解歧 S/R、普通未知 S/R、矛盾日内事件均有直接合同测试 |
| A02 D 有效状态与 C 信息边界分开 | 冻结 authority carrier 和原生 reference evidence | 保留 disclosure/hash/time-proven=false；时钟与用途篡改均拒绝 |
| A03 旧冻结输入不被当前规则暗改 | `validate_suspension_fact` | 冻结 authority 后修改当前默认规则仍可验证自身旧输入；旧 S/R 不自动合法化 |
| A04 独立缓存与输出身份更新 | LocalSIM selection service / writer | 政策变化不复用旧 checkpoint；完整原始评分、UNKNOWN、排序、TopK、提交前后失败恢复相关测试保持 |
| A05 价格与经济语义保持 | 原停牌 native pre_close / reference 路径 | 权威昨收优先、真实 C 参考价、缺失参考价继续拒绝；原订单、估值、恢复测试通过 |
| A06 真实原账户输入，只读且不接续生产 | 新进程原生 provider | `BUG-1953-source-candidate-native-readonly-r1.json`：01-22 全天停牌、两条原事件及完整摘要保留，C=01-21，真实引用报价日 01-20、close=6.23，原冻结身份和成功前缀不变 |

最终定向矩阵 188 passed，覆盖 LocalSIM execution runtime、公共 authority adapter、共享选股适配及原回放恢复。Ruff、编译与 diff/scope 检查通过。第一轮发现旧通用冲突测试恰好使用现已被官方披露解歧的真实股票/日期，已为该通用夹具显式注入空 authority，保持其原“无权威不可解歧”断言；没有修改公共算法来迎合夹具。

三轮审核覆盖公共唯一实现与所有权、冻结输入和 C/D/缓存边界、真实原生只读消费及未知/缺价拒绝。跨模块 `simulation_core_l2`、平台 API 由 PR CI 验证，深度场景按既有 nightly 去重计划执行。

## 仍需公共 owner 处理的参考价缺口

真实只读入口 `StrategyPackage.load_reference_prices` 在 C=2026-01-21 可原生返回 6.23（报价日 01-20），本修复未制造该值。但是 C=01-22 对同股仍返回 REFERENCE_CLOSE_MISSING；其当前代码直接要求所有原始记录均为单纯全天 S，没有消费公共日投影。因此不能据本次 01-22 的成功消费，声称 01-23 或全程回放可通过。

该缺口归共享参考价/StrategyPackage owner。数据窗口已提供权威状态，不需要再次要求补造 pre_close；LocalSIM 不复制参考价回溯算法、不手工延用 6.23，也不修改共享模块。应由公共 owner 明确冻结权威、有效市场状态与信息可用性的边界后修复公共入口并返回只读证据。重启后的实际日运行、Top50/递补、订单、经济事实和连续进度仍由 LocalSIM owner 验收。

本次单股完整 status 序列化约 11,564 字节，候选只读加载约 2.386 秒；这只是资格/估值入口探针，不是单日选股、回放端到端性能或收益证据。

## DESIGN-COMPLIANCE-001

| 条款 | 审核结论 |
|---|---|
| 禁止简化交付 | LocalSIM 接入、冻结验证、缓存身份及实际只读输入已实现；公共参考价后续缺口、运行激活和全程验收明确未完成 |
| 禁止静默错误 | 未知或矛盾状态、冻结身份冲突和无权威价格保持拒绝，不伪造停牌价或回放成功 |
| 禁止改变业务逻辑 | 只消费已返回的公共有效状态；订单、持仓、资金、账本及共享计算代码保持原有语义 |
| 禁止私增门禁审批 | 无新增请求开关、人工审批、重跑 QE 或包白名单；继续原任务原生重试 |

源码合入、用户重启、运行实际加载、生产回放验收和收益分别报告。生产数据库无本窗口写入，未调用 recover、创建回放或控制服务。
