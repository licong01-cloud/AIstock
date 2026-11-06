# 共享选股 provider 时钟合同 v1（BUG-1912）

## 冲突与适用边界

BUG-1904 已规定原生停牌 eligibility 使用 D；旧共享内核却把 risk 和整个
tradability 的 D 统一改成 C。现修订该包装层及实施验收，不改变原生评分、
过滤、递补、融合或 TopK 算法。C≤D≤T，T 不成为取数截止。

| 能力 | 合法观测时钟 | 证据与约束 |
|---|---|---|
| 模型、特征、完整原始评分 | C | 原生输入 context 的 effective_trade_date=C；不重训或重算已验证评分 |
| 参考价及其连续停牌证明 | C | 沿用 reference_price_availability_v1；quote_date≤C、as_of_date=C |
| risk/ST PIT | C | evaluate、apply 均传 C；不使用 D 的事后风险变化 |
| industry blacklist/PIT 行业 | C | 原生 TradabilityFilter 的独立 industry_trade_date=C |
| 停牌 eligibility | D | 按冻结 exclude_suspended 开关调用原生 suspend provider；关闭时不读停牌 |
| HMM | 保留原生 as-of/生效标签，行情和训练信息截止 C | 不把 D 标签换成 C 行；input_data_max_dates 和 training_information_cutoff≤C，资产可用日期不得晚于 D；不生成系数 |

D 日停牌仅属于冻结政策允许的 eligibility 观测，不代表 D 收盘行情已经可用。
本日级合同不声称拥有历史盘前发布时刻的证明，不提升旧证据等级。
若场景要求比 D 日更早的停牌可见性，由其原生政策提供对应证据，不能挪用未来 T。

## 接口与兼容

SelectionSignalKernel.run/iter_results 继续接收原 SelectionSignalRequestV1，
不要求 LocalSIM 添加私有过滤或新的请求参数。显式合同版本为
`selection_provider_clocks_v1`；prepare_day 仍得到完整 C/D/T。
compute_prepared 是既有 C=D 原生兼容入口，不偷偷重映射其日期。

TradabilityFilter.filter_candidates_with_receipt 新增可选 industry_trade_date；
未提供时仍使用原 trade_date，所有旧直接消费者保持原行为。
共享内核在 C<D 且行业过滤开启时显式传 C，而停牌查询和 selection receipt
保留 D。自定义 tradability provider 如使用跨日行业过滤，须支持同一可选参数；
不支持时明确报接口错误，不静默读 D 行业、不复制算法。

## 身份、证据、缓存与断点

每个新结果保存 provider_clock_evidence；risk/selection 原生 stage 的 semantic_payload
及对应 metadata 同步携带合同版本、请求日期和实际 provider 调用日期。
这里记录 provider 调用，不将禁用分支伪称为 SQL 读取；实际 SQL 次数未测不填。
HMM 记录原生日标签及已验证的输入截止；未启用明确 NOT_APPLICABLE。

新 semantic_identity 绑定合同版本、观测证据及原生 receipt；storage_identity
同时绑定新语义身份，完成前缀保留同一合同版本。旧 C 过滤结果不兼容新 D 过滤结果，
不得覆盖旧产物、改写旧收据或冒称 exact retry。已校验的原始评分不因后处理变更而失效，
可以作为本消费者明确授权的只读重算输入，不能跨消费者发现或借用结果。

LocalSIM 现有请求身份已包含共享 kernel 源码字节，更新后自然进入新来源空间；
LocalSIM owner 仍须在用户重启后确认真实 checkpoint 未复用旧 C 过滤投影。
此处不修改其私有 writer、账户、checkpoint 或回放状态。

## 验收与状态边界

必须通过真实 SelectionSignalKernel.run 而非直接 compute：C<D、C=D、
C停牌/D复牌、C正常/D停牌、关闭过滤、普通与递补缺价、行业/risk/HMM未来隔离、
日期及身份隔离。完整 raw 评分/排序/池/UNKNOWN 不变；变更仅是合法日期上的
eligibility 结果及明确版本化的来源身份。真实已存输入另做只读新进程核对。

源码、合同符合性、结果一致性、断点兼容和运行激活分别汇报；单元测试或只读 smoke
不能冒称生产账户通过。后端重启由用户执行，生产链由 LocalSIM owner 复验。

### BUG-1911 已确认事实及责任边界

603616.SH 在 2026-01-21 合法停牌，且没有权威 `stk_limit.pre_close` 替代值；
不能把该事实笼统表述成等待数据 owner 补数。共享链按冻结政策负责 D 日停牌资格过滤，
不推算、补造价格，也不放宽普通可交易股票及原生递补候选的真实缺价校验。
停牌持仓估值、旧计划修订、TCN 受控恢复和 LSTM 原回放恢复由 LocalSIM owner 负责；
本修复不修改其账户、估值、执行或恢复合同，不操作回放和数据库。

### 重启后交给 LocalSIM 的验收依据

- 日期为 2026-01-20，C=2026-01-19、D/T=2026-01-20；实际调用
  `LocalSimKernelComputation → SelectionSignalKernel.run`，不以直接 computation 替代。
- 核对 4912 评分、4918 原始成员、6 UNKNOWN、完整原始排序和 Top50；
  检查 301362.SZ 的真实历史参考价、实际 `quote_date` 及截止 C 的连续停牌证明，
  不把历史报价改称 C 日新价格；核对 D 日 `suspended_by_suspend_d` 排除及原生递补。
- 核对 `selection_provider_clocks_v1`、实际 provider 日期和来源身份；
  旧 C 过滤投影/断点不能冒充新 D 结果，原产物及已完成经济提交均保留。
- 先只读核验已完成的 01-20 运行及新加载的共享链，不重做该日订单、成交和账务。
  后续旧计划修订或受控恢复由 LocalSIM owner 按单独授权处理，技术验收不是收益证明。
