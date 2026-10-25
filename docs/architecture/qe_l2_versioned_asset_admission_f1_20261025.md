# QE L2 版本化资产准入与独立批次入口

日期：2026-10-25。Tier：F1。状态：QE_CODE_PREPARED_H1_BLOCKED。本状态只说明下述源码准备与直接验证完成，不代表新资产已准入、六账户已运行或生产已激活。

## Background / 背景

用户已批准执行 `qe-hmm-mainline-long-plan-20261025/plan.md`，并要求继续 QE 侧任务。本设计实现其 H1 第3项前置条件；沿用已批准的 `QE-L2-RISK-ENTRY-GATE-D1..D7`，不改变评分、买卖、持仓、现金或 NA 语义。原设计为 `docs/architecture/qe_l2_risk_optional_consumer_f2_20261023.md`。

当前风险文件只适用于旧522身份；H1目标是ded1。准入不能由请求自行声明一个新SHA，也不能删掉旧哈希检查。新资产尚未交付不妨碍实现严格的版本选择、身份验证及独立批次入口。

## Scope / 范围与 Non-goals / 边界

只修改 QE 消费与直接测试、所有权及本设计。旧消费路径、旧任务ID、旧预设、默认关闭行为不变。不修改HMM、数据集、数据库、服务或执行公式，不创建实验、不重跑R。H1的真实125日六账户仍以匹配风险资产、共享AF预测及远端核验就绪为前提。

## Contracts / 合同

1. `qe_l2_risk_asset_admission_v1` 为源码内版本化准入目录。目录只登记已经核验的实际资产；不登记空SHA或虚构待批准资产。每条绑定风险SHA、完整release identity摘要、去掉窗口coverage SHA后的物理dataset binding摘要、authority canonical SHA。请求只可选择版本，不能提供目录、扩大许可或覆盖旧项。首次目录保留现有522资产；ded1须收件核验后追加独立版本。
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

最小直接矩阵验证：旧资产仍可读取、未知版本/自报SHA/错release/PIT/provider失败、coverage窗口摘要例外不掩盖其他漂移、默认关闭等价、新batch不复用旧ID、seed缺失/重复或AF错配失败、提交前新旧身份交叉失败。fresh process消费真实旧manifest与新目录，证明拒绝把旧资产换绑ded1；真实新风险全流程待HMM交付。changed-file lint/compile、scope和diff检查，相关CI按现有QE分类执行。

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

准入目录更新仍需真实资产及身份核验，不能把目录框架完成描述为ded1资产获准。HMM产物仅为历史回溯分类研究预测，不证明严格历史as-known、未消费holdout或收益增益。固定AF共享预测和125日真实账户未完成前，H1保持阻断。

2026-10-25实时发现active已切至`afa9158b9b1d8cc76ec1e40a6efdb2060365bb3039ea791a0b5caa2bae8c5337`，generation追加`-suspendfacts1`。节点物理identity为complete；与R冻结binding比较的day/minute/factor_meta/index/suspend五组pin中，仅suspend_pins变化。按原长任务停止条件，AF推理、H1 compose及提交均未启动；不能在本实现中把H1常量静默换为afa。先复核新release及停牌组件差异，再同步版本化实验设计与HMM目标。原ded1的R结果保持原身份，不重贴为新数据结果。证据：`artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-mainline-long-plan-20261025/run_v1/h1_qe_preparation_20261025/identity_drift_receipt.json`。

## Production Gates / 生产门禁与 Rollout / Rollback

本轮仅在隔离worktree开发及验证，不合入、不部署、不操作服务、不写生产资产或数据库。新目录文件须随worker包冻结；原历史运行包保持原SHA。新批次在全部输入齐备并通过远端preflight后才可按既有实验授权执行，无需重批D1～D7公式。回滚为停用新batch入口，保留所有既有收据和运行包。
