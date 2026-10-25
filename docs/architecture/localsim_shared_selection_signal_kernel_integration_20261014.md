# LocalSIM 接入共享选股信号内核

日期：2026-10-14；F2 / T3。批准来源：本窗口用户的七项实施要求。基线：origin/main@13e5b5e778f14710c241e620bb8cfc1d0fdce8cc，包含共享 PR #6038。

## 背景与范围

沿用 [共享详细设计](advisory_shared_selection_signal_kernel_detailed_design_20261014.md) 和其原生计算合同。LocalSIM 每次独立执行公共 InferenceEngine、strategy_package_live_inference.py 与 SelectionSignalKernel；不读取 Advisory 结果或私有容器。按用户明确允许的逐日方案交付；多日腿模型/日绑定 workspace 计划由公共 owner 后续提供。

精确写范围：simulation_runtime/localsim_selection_inference.py、localsim_selection_signal.py、localsim_selection_probe.py、scheduler.py；backend/tests/simulation_runtime/test_localsim_shared_selection_signal.py、test_localsim_selection_inference.py；本文及 docs/analysis/localsim_shared_selection_signal_kernel_acceptance_20261014.md。公共计算、Advisory、StrategyPackage、QE、HMM、数据源码及 catalog 不修改。

## 非目标与架构

不训练、不改数据集或数据库、不启动生产账户回放、不重启后端。订单、持仓、资金、执行计划和账本继续使用原 lifecycle。实现本身保留原生 writer 的调用，验证只注入内存 writer。

原 scheduler 仅为 LOCAL_SIM 自动评分路由到专属服务，保留注入的测试/其他 broker 服务。专属服务继承原 StrategyPackageSelectionService 的政策解析和每日证据流程；计算依赖委托公共 SelectionSignalKernel，原生准备仍委托 StrategyPackageSelectionArtifactService，包括多 Alpha、seed、权重分派。LocalSIM adapter 仅映射资产地址、运行信封、完整产物和事务断点。

## 接口与存储契约

1. C 为原政策合法数据截止，D 为旧评分请求日，T 为账户目标交易日；此消费接口 D=T。不能跨日复用绑定股票池或输入。HMM、risk/tradability 使用各自原合同的合法时钟，交易日成交限制继续由执行层处理。
2. 动态因子/Alpha158/冻结处理器/模型真实序列长度/seed/权重由原生元数据和公共执行器消费，不设包 ID 分支。
3. Windows 运行文件在 X:/AIstock_temp/LOCALSIM，WSL 对应路径先实测访问。模型权重原址只读，工作区只写小型配置/代码；不跨盘复制大模型或行情。WSL 内核用独立 consumer_scope、run_id、缓存和输出。
4. 全局 selection_score_artifact 唯一键缺消费者/run；不借其命中。专属临时 raw repository 只读取当前 LocalSIM run。完整原生 SelectionScoreArtifact 随现有 DailySelectionEvidence JSON 在其原 writer 事务内发布；政策 hash 不掺运行身份，运行身份独立保存。
5. 文件先同卷原子写；每日证据的原生 writer 事务确认返回、并核验返回内容属于自身来源后才推进断点。提交后断点前崩溃，按预先记录的自身 evidence ID 实际回读修复，不能用文件存在表示账户提交。普通路径保留原生 writer 提交确认合同，恢复路径要求原生持久证据读回。未提交 DB 输入没有冻结 revision 保证时不跨进程复用 raw，重新独立计算。账户提交仍由原交易日志幂等保护。
6. 保留完整评分、原人口和未评分 UNKNOWN，policy/manifest/data/source identity 与原生异常。零候选与计算失败区分；批次失败保留完成前缀。

## Design Acceptance Index

| design_item | requirement |
|---|---|
| F-001 | 唯一公共推理/评分/融合/过滤/TopK，无私有业务依赖 |
| F-002 | 原生元数据驱动，单多腿与新身份无需包专用代码 |
| F-003 | LOCALSIM/run 独立，X/WSL 和只读资产引用 |
| F-004 | 完整原生产物、人口 UNKNOWN、C/D/T 和身份保留 |
| F-005 | 原每日证据事务、幂等、失败前缀和自身恢复 |
| F-006 | 日期绑定不复用，逐日推进与公共最小接口交接 |
| F-007 | 真实 LSTM/TCN 同截止/政策全评分、排序、TopK/状态对照 |
| F-008 | 连续日、增量、失败恢复、独立性定向验证 |
| F-009 | 端到端时间、峰值内存、磁盘实测及限制 |
| F-010 | 多轮审核、定向回归、CI/合入和用户重启分报 |

## 实施方案与验证方案

先实现并自审消费适配/交易边界，再做直接合同反例。真实测试使用现有 LSTM、TCN 包只读数据和原冻结模型；两臂分别独立计算，reference 仅选择公共执行器的正确原公式，不恢复未来数据或重复大文件布局。完整 raw、排序、TopK、状态和输入身份逐项比较。覆盖连续日期、追加日期和提交/断点窗口的失败恢复。各轮仅重跑失败点，源码稳定后最后一次定向矩阵；广泛模拟盘回归交由既有 PR CI，使用 aistock-validation-delegation。

## 风险与失败模式

节点 Python 3.10 不能导入含 datetime.UTC 的原生业务类；业务准备保留 Windows，由公共 Python3.10 推理脚本执行计算。Windows 不能创建符号链接且 X/F 跨卷硬链接不可用，故工作区 manifest 用原址模型引用、WSL 模型代码采用经过验证的链接，不提供大文件复制降级。没有公共多日计划时单日模型/数据初始化成本仍存在，不承诺每日40秒。

## 发布、回滚与生产门禁

通过 F2、DESIGN-COMPLIANCE-001、直接反例与真实对照、最终 HEAD 必需 CI 后按现有授权合入。回滚使用原合入版本源代码；不切换账户、数据或政策。production_ddl_gate=noop、dependency_gate=noop、QE_training=noop。runtime activation=pending_user_restart；不将源码合入或性能验证作为收益证明。

## 公共接口 owner 交接

现有单日公共 CLI 能支撑本次交付。后续跨日加速需要公共推理/StrategyPackage owner 提供下列最小数据契约；本窗口只记录需求，不修改其模块：

1. `prepare_daily_workspace_plan(manifest, native_runtime_config, clocks)`：返回每个 D 的 package/manifest/leg/seed 身份、原始成员和合法成分日期、明确推理 PIT identity、冻结模型/处理器/代码 URI+hash、原生因子顺序和 Alpha158 schema。必须区分训练兼容绑定与推理身份。
2. 将上述日绑定 workspace/腿/seed 计划直接送入已有公共多日 CLI，允许会话复用不可变模型和增量数据读取；共享仅发生在同一个 consumer/run 内。种子集合、融合权重和各日股票池仍由包 owner 解析，不能由模拟盘重写。
3. 返回消费者无关的完整 raw envelope、输入/来源 revision 证据、实际人口及 UNKNOWN、C/D/T 和有内容哈希的已完成前缀；生产提交仍由消费方自己的 writer 处理。另须区分数学内容身份与独立产物的 lineage UUID：已实测相同 raw canonical payload 会因 alpha receipt 的随机 artifact_id 导致公共 semantic_identity 不同，当前消费方保留该原生凭据并核验其指向自身产物，不自行修改公共身份算法。
4. 若未来对未提交输入做跨进程复用，需要数据 owner 的不可变 revision/token 和复核接口。当前没有这一保证时重新计算；不能把“某个文件已经存在”升级为同一数据身份。

这些接口是后续降低逐日启动/读数开销的工作，不增加本次逐日接入的人工审批或上线门禁。

## Design Acceptance Matrix

详见 [独立验收记录](../analysis/localsim_shared_selection_signal_kernel_acceptance_20261014.md)。
