# QE L2 版本化资产准入与独立批次入口

日期：2026-10-25。Tier：F1。状态：QE_CODE_VALIDATED_H1_INPUTS_READY_PENDING_CI。新资产、六份运行包和远端消费预检已通过；实验提交与常驻 API 激活分别报告。

## Background / 背景

用户已批准执行 `qe-hmm-mainline-long-plan-20261025/plan.md`，并要求继续 QE 侧任务。本设计实现其 H1 第3项前置条件；沿用已批准的 `QE-L2-RISK-ENTRY-GATE-D1..D7`，不改变评分、买卖、持仓、现金或 NA 语义。原设计为 `docs/architecture/qe_l2_risk_optional_consumer_f2_20261023.md`。

最初风险文件只适用于旧522身份，H1目标是ded1。2026-10-25经用户后续执行授权和完整manifest差异审计，本批次显式更新为afa9158的独立H1版本，详见下方更新记录。准入不能由请求自行声明一个新SHA，也不能删掉旧哈希检查。

## Scope / 范围与 Non-goals / 边界

本 PR 只修改 QE 消费与直接测试、所有权及本设计。旧消费路径、旧任务ID、旧预设、默认关闭行为不变；不修改 HMM、数据集、数据库、服务或执行公式，不重跑 R。独立 BUG-2093 负责 HMM 文件 reader 兼容修复。用户已授权前置核验就绪后执行 H1 的125日六账户，排除后端重启。

## Contracts / 合同

1. `qe_l2_risk_asset_admission_v1` 为源码内版本化准入目录。目录只登记已经核验的实际资产；不登记空SHA或虚构待批准资产。每条绑定风险SHA、完整release identity摘要、去掉窗口coverage SHA后的物理dataset binding摘要、authority canonical SHA。请求只可选择版本，不能提供目录、扩大许可或覆盖旧项。保留522资产；另登记真实核验的 `l2-risk-afa9158-h1-v1`，不登记未交付的ded1资产。
2. 未传版本的旧请求仍执行原5e6哈希检查，并核对其522身份；不得以旧资产换绑ded1。显式版本同时用于compose及fresh worker；worker包携带相同源码目录。缺失、未知、错hash或错身份在读取预测/启动回测前失败。
3. 新批次可提供 `study_batch_receipt.json`（schema `qe_l2_controlled_batch_v2`），固定batch_id、generation、manifest identity、release_id、cutoff、admission版本、D1～D7合同、共享预测回执原文件SHA及三份consumer manifest原文件SHA。共享回执包含实际模型、拟合处理器、预测来源与SHA，不能改写后继续提交。旧入口无该文件时沿用原固定generation与任务ID。新batch命名须独立于旧 `qe_l2_entry_20261023`，seed仍42/2024/2026，每seed B/L。
4. compose开始前验证恰好三个有序seed、每seed consumer manifest与共享预测SHA一致、三seed风险/物理数据相同、batch pins与正式active解析一致。提交再验证这些pins和运行包中的consumer manifest，保留原intent/远端preflight与去重语义。批次元数据不构成新资产批准。

## Implementation Plan / 实施方案与允许文件

- `scripts/qe_l2_risk_admission.py`：无第三方依赖的共享准入目录及验证。
- `backend/services/quantevolver/l2_risk_consumer.py`：消费同一目录，打包目录源码。
- `scripts/qe_l2_risk_consumer.py`：worker在资产IO前验证相同准入。
- `scripts/run_qe_l2_risk_controlled_study.py`：版本化批次、预检查、提交前复核。
- `backend/tests/quantevolver/test_qe_l2_risk_consumer.py`：拒绝换绑、目录与批次反例、fresh-process验证。
- `tests/aistock_validation/catalog/file_ownership.yaml`：精确登记新准入脚本。
- 本设计。

## Verification Plan / 验证方案

最小直接矩阵验证：旧资产仍可读取、未知版本/自报SHA/错release/PIT/provider失败、coverage窗口摘要例外不掩盖其他漂移、默认关闭等价、新batch不复用旧ID、seed缺失/重复或AF错配失败、提交前新旧身份交叉失败。fresh process已消费真实旧manifest与新目录，拒绝旧资产换绑；真实H1三seed消费和六账户compose预检也已通过。changed-file lint/compile、scope和diff检查，相关CI按现有QE分类执行。

## Design Acceptance Index / 设计验收索引

- F-001：源码受控版本目录和完整身份，禁止请求自授权。
- F-002：compose/worker共享准入、旧默认及公式不变。
- F-003：独立批次及严格三seed共享输入预检查。
- F-004：提交前重新核验、保留一次提交与原意向。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_risk_admission.py | backend/tests/quantevolver/test_qe_l2_risk_consumer.py; artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_qe_preparation_20261025/fresh_consumer_receipt.json | PASS | - |
| F-002 | backend/services/quantevolver/l2_risk_consumer.py; scripts/qe_l2_risk_consumer.py | backend/tests/quantevolver/test_qe_l2_risk_consumer.py; artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_qe_preparation_20261025/fresh_consumer_receipt.json | PASS | - |
| F-003 | scripts/run_qe_l2_risk_controlled_study.py | backend/tests/quantevolver/test_qe_l2_risk_consumer.py | PASS | - |
| F-004 | scripts/run_qe_l2_risk_controlled_study.py | backend/tests/quantevolver/test_qe_l2_risk_consumer.py | PASS | - |

直接矩阵39 passed；本机无Qlib，两个原策略/末日执行测试模块skip，不计为通过。新改动未改变订单或calendar算法。远端fresh process使用真实旧522资产：16,375行风险、655,343行映射、125日、首日3/31上下文、末日9/29上下文、实际冻结预测与provider校验通过；显式旧版本完整identity匹配，旧资产改绑ded1在IO前拒绝。该证据不替代未来新H1资产的端到端验证。

DESIGN-COMPLIANCE-001复核：源码准备和实际实验状态分开；无空SHA准入记录、默认安全回落或成功占位；D1～D7语义与125日要求不变；沿用原资产审核/批次授权及active变更停止条件，没有新增用户审批环节。

## Risks / 风险

准入目录只覆盖已经核验的精确资产及身份，不能描述为任意未来资产或ded1资产获准。HMM产物仅为历史回溯分类研究预测，不证明严格历史as-known、未消费holdout或收益增益。125日真实账户结果与经济验收仍待实验完成。

2026-10-25实时发现active已切至`afa9158b9b1d8cc76ec1e40a6efdb2060365bb3039ea791a0b5caa2bae8c5337`，generation追加`-suspendfacts1`。节点物理identity为complete；与R冻结binding比较的day/minute/factor_meta/index/suspend五组pin中，仅suspend_pins变化。按原长任务停止条件，AF推理、H1 compose及提交均未启动；不能在本实现中把H1常量静默换为afa。先复核新release及停牌组件差异，再同步版本化实验设计与HMM目标。原ded1的R结果保持原身份，不重贴为新数据结果。证据：`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_qe_preparation_20261025/identity_drift_receipt.json`。

### H1 执行版本更新：afa9158

用户随后明确授权继续完成所有后续操作直到启动实验，排除后端重启。完整比较 ded1 与 afa 的所有 manifest component pins 后确认仅四个文件改变：停牌 parquet、其 meta、suspension amendment 及 correction source；日线、因子、PIT、日历等其余 pin 完全一致。新 H1 generation 固定为 `20260930-monthly-v2-unified-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1`，manifest 固定为上文 afa9158。本次只更新独立批次的目标，不修改 active，不回写或重跑 R。

共享 AF 已按原计划的零 fit 分支生成：复用三 seed R-FIXED 冻结模型及原拟合处理器，每 seed 655343 行、126 个信号日，原 Q3 保存预测及 6 月 30 日 context 逐值完全一致。实际来源记录在 shared prediction receipt；运行 provenance 的 base_model_task 使用实际 source_model_task。完整 C-013 映射可在 keys、authority、PIT SHA 一致时复用，不重新转换或按可用性裁样本。

新 release 风险资产已由原冻结 HMM 模型经正式双 fresh-process 零训练推理生成并核验：风险文件 SHA `a21f228d22f37aea474bd61a6dd4601bc2818db0da52ebbf1873256b4af2d5d2`；验收 receipt `9a68086cd466eae97ffe0cd1ea61d440d0688ee4716ac9daa5762f8f09865a99`；125日、16375行、131行业、两子进程逐字节一致，全部 available。HMM monthly reader 兼容修复独立归 BUG-2093，不改模型、特征、阈值或数据集。常驻 API 重启和运行身份验收独立待办；研究使用审核后的 fresh-process CLI 与冻结 worker 包，无后端重启。

H1远端复核：三seed真实新资产消费通过；六份运行包的AF provider、PIT、模型来源、费用及策略与原冻结配方一致；保留655343原预测行和617223股票池内信号行，126信号日，4913条9月30日原始标签行保留为NA。真实Qlib订单及末日calendar测试通过；daily及minute executor均验证9月30日终端区间，预计净值125日。实际证据：`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_active_execution_20261025/fresh_consumer_receipt.json` 和 `artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_active_execution_20261025/composed/qe_l2_h1_20261025_afa_s42_b/remote_preflight.json`。这些是消费和compose预检，不冒充已运行的账户结果。

本版本执行设计及输入审计：`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_active_execution_20261025/execution_plan.md`、`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_active_execution_20261025/release_and_reuse_authorization.json`、`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_active_execution_20261025/shared_prediction_receipt.json`。D1～D7、125 日、三 seed 六账户及禁止重复提交的限制不变。

## Production Gates / 生产门禁与 Rollout / Rollback

原准备阶段仅隔离开发；用户随后授权后续操作直到启动实验，排除后端重启。本版本CI通过后可合入源码，使用审定的fresh-process CLI及冻结worker包按原六账户预算执行，无需重批D1～D7。正式登记及任务写入只经现有QE服务，不修改数据集或HMM生产资产库。原历史运行包保持原SHA；API常驻身份与源码合入分别报告。回滚为停用新batch入口，保留全部收据和运行包。
