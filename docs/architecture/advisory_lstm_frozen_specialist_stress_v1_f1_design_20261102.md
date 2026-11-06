# 冻结LSTM专属模型跨期压力检验（F1）

2026-11-02，Advisory/model_first。用户在研究复盘后授权“开始按建议执行”。本次只检验已经在P20观察的P19第3折原模型，不选择三折赢家、不拟合、不搜索参数。实现和结果按以下索引验收。

真实118D检验及独立算术复核完成：106完整配对D主增量−136.023748bps，STRESS_NOT_SUPPORTED_CURRENT_PAIR，按下方事前三分支停止为此冻结模型追加主动开发。累计248实际fit＋1旧index不变；原V2、P20及生产不改。详[结果报告](../analysis/advisory_lstm_frozen_specialist_stress_20261102.md)。

## Background / Goal / Scope

原P18验证共享最终模型在2026-03-11～08-28的118D/5900原Top50，结果负面；P19专属模型的正结果仅为2025三折开发。唯一问题：固定当前专属第3折在同一已消费后续历史能否保持成本后Top10增量。源数据、旧共享模型结果已被研究者看过，任何结果都只能用于开发压力诊断，不能作为独立样本外、历史PIT或正式经济确认。

用户此次授权将旧设计“暂不再读取旧压力窗”的离线范围明确扩展为一次固定模型压力检验。P19原试验、结果、归档源码和预算不改；原V2正式协议及十文件源码闭包、150D、100bps业务目标、P20自然观察、生产Top20保持。显式8文件范围见X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/scope.json。

## Inputs / Non-goals

模型仅P19 fold3，训练截止2025-07-31，物理SHA256 `8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813`，精确package/manifest/run/source沿P19与P20原身份。原模型内部median/scale/coef及missing bits全部固定；中位数填补是原模型既有编码，不能新增源数据补零或前填。

原日期轴、原5900行输入、旧共享预测和已按2026-08-28截止投影的金融文件直接来自P18不可变阶段。验证原P18 protocol与阶段链终点 `cc368bdac2fce51acb93c74ac9639654b7b972047ebb6c88e697dd6dc7213120`；P19原恢复链与模型亦核对。只读取指定文件，不查询数据库或网络、不补采、不重建Top50、不读其它包/封存收益、不更换日期。

0fit/新候选/新原始行情标签/生产写/服务控制。无需UI或新生产API；不是新一轮P18共享模型计算，也不修改生产日期限制来补造历史前瞻预测。

## Architecture / Method

离线CLI先冻结单一protocol、明确模型/日期/源码/物理文件hash；校验当前提交中的新研究代码已提交。原旧共享结果已知事实明确入册，不将预登记说成全盲研究。

predicted阶段只解码固定输入和模型，复用`spo_top10_5td_v1.predict`，新增独立的专属跨期身份/训练截止/原人口校验，不改变`predict_specialist`原2025持出窗口或生产`predict_current/capture`。全部5900预测落不可变阶段后，evaluated才解码已消费的P18 target_finance；原共享预测直接复用。

公共发布器仅支持既有阶段名，逻辑predicted阶段落在物理`trained/`（沿P18零fit推理惯例），receipt明确`FROZEN_WEIGHT_INFERENCE_ZERO_FIT`；不改变公共发布器枚举、不把阶段名计为模型拟合。

主比较为专属−原生的双臂完整日；辅助比较为三臂共同完整日上的原生/共享/专属均值、专属−原生和专属−共享。两种分母分别披露，保留全部118D、原未成熟/UNKNOWN/CASH，不用共享完整性筛选主端点。旧共享panel和report直接复用并核对公共cohort算术一致性，不重训或重做旧研究结论。

当前`lstm_complementarity_v1.stress_report`、公共固定十槽cohort、四项分解、时间块区间和固定费用原样复用。另逐股核对D参考锚毛价格比除以1+observed_gap，再扣buy0.95/sell5.95bps；不将D锚当开盘买入价。所有区间均为已消费历史上的描述性区间，不作选择校正后的正式显著性。

## Decision / 事前处理分支

复用P18原压力规则：均值非正、区间上界负、下尾改善小于−100bps或额外UNKNOWN超过2个百分点时不支持；正支持仍沿原10bps/区间下界正、覆盖80%、20干预D/20%/两季度等条件。原10bps仅开发压力分类，不能替代用户100bps实用目标。

1. STRESS_NOT_SUPPORTED_CURRENT_PAIR：停止为这一冻结模型追加主动开发、拟合或救参数。原V2自然观察、生产排序和正式去留规则不因本次诊断自动变化。
2. STRESS_SUPPORTS_LSTM_PROSPECTIVE：只保留冻结观察；另报告点估计是否达到100bps，低于目标不扩大推广；即使达标也不确认或激活。
3. STRESS_INCONCLUSIVE：保留不确定，无新增拟合、窗口、参数或工程扩张，不把不确定改判成功。

输入/身份错误使本次任务显式失败，不形成模型负面结论；只修工程问题后继续同一范围，实质代码修正需要新独立尝试并保留失败产物。重跑复用已完成阶段，不产生第二份有效经济判断。

## Implementation Plan

新增一个Advisory研究叶、一个离线CLI及直接合同测试；增加精确CI/ownership条目。设计冻结后实现、先提交producer再执行predicted→evaluated→独立算术审核。三轮自审修订，完成结果报告、蓝图当前队列和逐项验收，再按既有提交/合入授权交付。后台重启用户拥有，本轮无需重启。

## Verification Plan

直接测试覆盖模型/日期轴/身份/人口错误、未来估值、费用/UNKNOWN/现金、两种分母和决策三分支。真实118D运行后，新进程以NumPy/标准库独立核对5900预测及排名、旧共享结果、十槽算术、四项损益闭合与描述性区间；检查来源、正式V2、生产模型/release/先行预测hash不变。最小本地测试、lint/compile、L0/ownership/CI路由、F1及DESIGN-COMPLIANCE-001通过后PR；较广模块回归交既有CI，不在本地重跑旧研究。

## Design Acceptance Index

| ID | Requirement |
| --- | --- |
| F-801 | 固定当前第3折与118D/5900原人口，来源及旧链校验，0fit且明确已消费历史 |
| F-802 | 预测不可变落盘后才解码金融值，复用公共计算且不放宽旧生产/研究窗口 |
| F-803 | 双臂主端点与三臂共同日分报，原UNKNOWN/IMMATURE/CASH/费用和旧共享结果保留 |
| F-804 | 事前三分支与100bps目标分报，不自动改生产或原V2 |
| F-805 | 直接合同、真实全人口、独立预测/算术/区间读回和原工件保护 |
| F-806 | 三轮自审、结果/蓝图与F1完整验收，源码/CI/合入分态 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-801 | backend/services/advisory_model_first/lstm_frozen_specialist_stress_v1.py; scripts/advisory_lstm_frozen_specialist_stress.py | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/recovery-v2/protocol.json，原链/5900行/0fit | PASS | none |
| F-802 | CLI.predicted/evaluated、公共不可变发布器 | backend/tests/advisory_model_first/test_lstm_frozen_specialist_stress_v1.py; artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/recovery-v2/evaluated/read_receipt.json | PASS | none |
| F-803 | 公共cohort/stress_report与新evaluate三臂汇总 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/recovery-v2/evaluated/report.json，118D全保留，主/辅助106D独立核验 | PASS | none |
| F-804 | 新研究叶decision及结果报告 | backend/tests/advisory_model_first/test_lstm_frozen_specialist_stress_v1.py，三分支与100bps分报；真实STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL | PASS | none |
| F-805 | 23项直接测试、新进程独立审核 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/independent_audit_receipt.json，5900预测/5623费用/三组区间与35源/V2不变 | PASS | none |
| F-806 | 本设计、分析报告、蓝图F-310及精确CI/ownership条目 | artifact: X:/AIstock_temp/ADVISORY/lstm-frozen-specialist-stress-20261102/validation_receipt.json，三轮自审和逐项符合性；CI/合入独立见delivery_receipt | PASS | none |

## Risks / Rollout

该窗和模型选择均非独立；固定第3折只是已在观察的候选，不代表训练至2026的模型。共享模型训练截止不同，三臂差值不能隔离专属化因果。原NON_VINTAGE、5TD重叠、完整组选择和开发复用保持。任何负面/未知保留，不私增审批或把等待新交易日作为工程门禁。仅离线文件交付，无生产DDL/DML、依赖/客户端变更、服务或激活操作；新任务树未获删除授权则保留。

## Production Gates

production_ddl_gate=noop，production_dml_gate=noop，dependency_install=noop，backend_restart_required=false，production_activation=false。当前任务不改变生产调用路径或正式V2协议，无运行激活步骤；源合入、主目录同步、研究结果和生产状态分别记录。无任何新人工审批或数据准备窗口要求。
