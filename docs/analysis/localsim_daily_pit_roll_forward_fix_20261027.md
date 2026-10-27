# LocalSIM 跨日 PIT 租约续接修复与重启交接

问题：BUG-2103，GitHub Issue #6759。修复范围仅限 LocalSIM 逐日准入及其定向测试；不修改 PIT 权威、选股算法、账户本金、成交、费用或账本。

## 运行态诊断

2026-10-27 用户重启后的实际 backend identity 为 `107cb4f5782b1d473459040aa0ed1308ce211068`，包含双模式 PR #6751。原有四账户的20项 summary/positions/trades/orders/pnl GET 均成功；真实浏览器总览及账户持仓、成交、委托和区间盈亏没有页面异常、原始 JSON 或机器身份显示。左侧导航未修改。

但三个正式原生账户的当日日任务均为 `FAILED_RETRYABLE / PRE_RUN_FAILED / CANONICAL_PIT_RUNTIME_INVALID`。TCN、LSTM 冻结租约截止10月23日，LightGBM 截止10月26日；三个 live binding 的 `effective_to=null`。从正式只读 PIT 迁移计划读回的全局 canonical authority 已覆盖10月27日，规则仍为 `aistock_equity_pit_canonical_v2 / shsz_a_252td_st_delist_asof_v2`。

原因是 open-ended binding 一直被视为当前有效，未进入只支持到期 binding 的逐日 roll-forward；原生 canonical profile 还缺少 migrated-policy 标记，原更新分支未覆盖。租约过期校验在下单前阻断，三个失败日任务没有 selection evidence、execution plan、订单或成交。全局 PIT 可用，不能据此要求数据窗口补行情或手动延长旧租约。

证据目录：`X:/AIstock_temp/localsim-human-operation-implementation-20261026/postrestart-verification-20261027/`。其中 `pit-migration-readonly-plan.json`、`active-release-pit-readback.json`、`pit-binding-diagnosis.json` 分别绑定权威覆盖、原冻结身份和仍在生效的 open-ended binding。

## 修复合同

1. 正常后台日切在现有绑定列表内先检查 LocalSIM canonical 租约。已覆盖本日的租约继续保持身份；只有当前交易日超过原覆盖终点才产生不可变逐日 release/binding。历史/未来日期、显式历史 release、冻结 source plan、停用自动接续、非 canonical 原生 TWAP 和 MiniQMT 均保留原路径。
2. 复用公共 PIT freeze 接口和 LocalSIM 既有 release/binding 创建流程。原生及 migrated profile 都核对原政策指纹；当日权威覆盖不足、政策改变或原 lease 与 selection projection 不一致继续 fail closed。原账户、原 release、原 profile、已有评分和账务不改写。
3. 只有明确的 `FAILED_RETRYABLE / PRE_RUN_FAILED`、canonical PIT 原因、确认 broker 未调用、提交数为整数零、无 selection/plan/成交证据的旧日任务，才由正式调度流程记为带 successor 身份的 `CANCELLED`。保留原失败 payload 及结构化审计。`FAILED_TERMINAL`、执行中、有计划、有评分、未知副作用或已有经济写入均不重绑。
4. 新 binding 使用现有幂等身份与 superseded lineage 选择；额度已满也在原槽内替换。成功发布后审计步骤中断，下次 tick 可完成审计；不重复生成经济计划。一个账户准入失败单独记录，不阻塞其他绑定。
5. 旧 TWAP 的订单、撮合、费用和持仓逻辑未修改。没有新增训练、价格补值、行情回退、数据库迁移、进程控制或逐单人工审批。

## 审核与验证

第一轮修复原生/migrated 两类分支与 open-ended 准入；第二轮补充无副作用失败前缀、终止/在途/未知保护、发布中断恢复和额度满场景；第三轮核查显式历史与冻结 source plan 不被刷新、原政策及严格截止不被放宽、旧 TWAP 不查询新 authority，失败按账户隔离。

首次16项直接回归通过，包括真实公共 PIT freeze 调用和严格 frozen profile 回读；随后54项关联回归通过，覆盖旧原生 TWAP、迁移、真实 profile、两种 backend 的既有日切及身份语义。10项已有 DEV 数据库用例未执行，本轮不运行数据库操作。补充历史/未来日期边界两项定向回归。L0 两个扫描均无阻断，Ruff及 diff 检查通过。CI及源码合入收据由 BUG 工作流补充；不得将离线测试当作已经完成运行态验收的证明。

DESIGN-COMPLIANCE-001：按现有不可变逐日 release 及 PIT 上位合同逐项实现；没有未批准的简化/占位/算法替代；保留严格资格/资金/撮合与历史断点语义；完整功能、测试、合入、运行态及经济结论分别报告。本修复不证明策略收益提升。

## 激活与清理

用户拥有后端重启权。本窗口只进行现有只读 GET、冻结文件核验和离线内存测试，不直接查询生产数据库、不执行数据修补或账户恢复、不操作服务进程。

双模式比较目录已单独追加到 canonical `.env` 并独立读回：`LOCALSIM_DUAL_MODE_RESEARCH_ROOT=X:/AIstock_temp/localsim-human-operation-implementation-20261026/delivery-studies`。旧进程仍未加载这一变量，需要用户重启；目录只用于读取已完成的13账户两日研究，不会启动生产回放或在线人工账户。原不可变结果 SHA 为 `13f1b569057dedda1f8a9fdbe60d289c3ecaeae60add7e6117b7023ed1102393`。

PR #6751 的任务工作树及本地/远程分支已完成四态清理，共享 node_modules 与研究结果均保留。BUG-2103 自身工作树在源码合入后按已授权清理处理；BUG 保持待用户重启验收，不能提前关闭。

重启后最小只读验收：身份接口确认加载修复 merge；comparison overview 配置已生效、13个账户和五类明细读回原结果；scheduler scoped verification 核对当日 PIT 阻断及旧失败 successor 审计；核对正式账户原 release/ledger 未被覆盖、旧失败日没有补做历史订单。若出现行情/资金/价格约束等新原因，单列原因及成交事实，不能将所有未成交都认定为程序失败。
