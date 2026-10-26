# P17 全部启用策略包的冻结SPO+适用性验证（F1）

版本v1.1，2026-10-27，Advisory/model_first。用户授权先验证目前荐股全部策略包，再接续前向确认建议；现已完成全部5包输入审查、可评价4包经济比较及后续差异归因/前向输入核查。旧单包历史效果未能评价，独立确认未启动；当前SPO+不推进全包推广。本轮0fit，研发累计244实际fit＋1旧index不变。

## Background / Goal / Feature Card

P16仅在LSTM、TCN两包上研究，共同100D平均增量+16.570982bps/5TD，区间跨零，两包异向。共享权重不等于已验证所有策略包。本轮回答固定模型对当前五包的适用性，完整保留不能评价的包，不能以扩大包数声称独立经济确认。

## Scope / Inputs

以只读API冻结5个ENABLED任务对应的5包：multi Alpha、旧single Alpha、LSTM、TCN、LGB。DRAFT重复父包单列。原两包P16报告和panel复用；新增三包只从其原始评分文件/存量原Top50取人口，禁止从Top20补成50、重新推理或按收益筛包。API返回的旧metrics不用于选择，实际新增收益数组在预测冻结后才读取。

固定原开发窗2024-07-04～2025-09-30；评价三折仍为2025-04-01～05-30、06-01～07-31、08-01～09-30，对应P16训练截止03-31/05-30/07-31的candidate和matched JSON。同折各包使用同权重/median/scale，不允许用09-30最终模型评价其训练期。原8字段、50→10、固定5TD、费用、UNKNOWN保槽、原binding均不变。原native为主对照，matched仅归因。

评分文件为AIstock自有、manifest/descriptor SHA及大小绑定的score-only pickle。允许从全量原评分按既有score降序/代码升序投影原Top50，记录这是冻结回测评分研究人口，不冒称历史实时荐股名单或PIT。所有日期先按日历冻结，缺任一原日期/不足50明确报告，不另找有利窗口。

单Alpha候选→原QE实验身份桥接只读，不为补数据触发QE/归档/重推理。查完既有指针和同身份选择工件仍缺源时NOT_EVALUABLE，并保留五包分母。多Alpha历史组合评分可用性与当前seed资产/每日复评状态分开。

## Implementation Plan / Experiment

新增`spo_all_packages_validation_v1.py`：受限评分读取、完整原Top50、严格fold模型与包身份验证、零fit预测及完整包清单汇总。共享特征/估值/predict仅复用，不修改。X盘任务脚本编排四个不可变阶段preregistered→prepared→trained→evaluated，绑定协议、源码闭包、源文件和模型SHA。既有研究只读。异常显式停止，不能降级或自动换源；中断可读取已发布阶段，不覆盖已发布内容。

在新金融值解码前固定protocol、评分来源、原日期和模型；prepared只计算D时点向后特征，trained先发布两臂预测，随后仅解码当前折H≤折末的原估值。旧outer/sealed不读。每包逐折报告native/candidate/matched、共同成熟D、干预、UNKNOWN、尾部、10-session block区间。包间共同日期额外横向表与原P16结果分开，缺包不能形成“全五包平均改善”。不改变P16分类，不新增收益门槛、不按结果挑包上线。

资源上限2CPU线程、0GPU、2GiB、每阶段30分钟、输出200MiB；最多新增两包（第三包如确有同身份原评分，则应在protocol冻结前纳入）、0fit、无参数搜索。输出完整缺口即输入评审完成，但经济验证不能评价的项仍显式未完成，不把功能交付等同全五包效果确认。

## Evaluation / 后续执行

先完成所有可评价包的一次冻结模型评价与第二算术路径复核。之后审查原五包当前全Top50、时钟、数据binding和最终模型的前向可运行性；已有完整合格新输入才做文件影子预测。当前数据不合格则交付精确owner输入清单、最终模型身份和固定执行顺序，不伪造影子运行。每日复评Top20属于运行覆盖，不能替代5TD模型效果证据。

原两包反复使用开发窗，新包共享股票/市场日期及源模型回测资格，均不足以建立独立OOS或历史PIT。全部区间为描述性，重叠持有期不按股票数扩张样本量。模型不输出买卖最优价，本任务不推进研报/现金流/Exit/仓位支线。

## Verification Plan / 多轮审核

第一轮：源/时钟/模型/缺包分母和先预测后收益；直接测试篡改、少于50、错版本/错折/最终模型训练期、缺包、UNKNOWN。第二轮：真实工件独立算术核对、源未变、0fit、结果与P16复用一致。第三轮：逐条设计对照及文档状态，最小lint/pytest/F1/F2/L0、scope、diff检查与CI。审核通过按既有授权提交合入；不重启后端。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-541 | 全部启用五包身份冻结、DRAFT排除和缺输入包保留 |
| F-542 | 原评分/Top50/日期/manifest可追溯，缺失不可重建 |
| F-543 | 同折冻结模型、八字段、0fit、拒绝训练期自测 |
| F-544 | 不可变阶段与先预测后标签，原费用/5TD/UNKNOWN不变 |
| F-545 | 每包逐折完整结果、描述性区间/风险及第二算术核查 |
| F-546 | 前向可执行性和精确缺口、研究/生产状态分离 |
| F-547 | 精确写范围、资源、直接测试及多轮审核合入 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-541 | 本文Scope / backend/services/advisory_model_first/spo_all_packages_validation_v1.py | artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/inventory.json | COMPLETE_FIVE_INPUT_REVIEWS | none |
| F-542 | backend/services/advisory_model_first/spo_all_packages_validation_v1.py | backend/tests/advisory_model_first/test_spo_all_packages_validation_v1.py; artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/protocol.json | VERIFIED | none |
| F-543 | backend/services/advisory_model_first/spo_all_packages_validation_v1.py | backend/tests/advisory_model_first/test_spo_all_packages_validation_v1.py; artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/research_audit_receipt.json | VERIFIED_ZERO_FIT | none |
| F-544 | 本文Experiment / X盘run_once.py | artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/study/evaluated/manifest.json | VERIFIED_FOUR_IMMUTABLE_STAGES | none |
| F-545 | 本文Evaluation / docs/analysis/advisory_spo_all_packages_validation_20261027.md | artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/research_audit_receipt.json | COMPLETE_FOUR_EVALUATED_ONE_UNAVAILABLE | none |
| F-546 | docs/analysis/advisory_spo_all_packages_validation_20261027.md | artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/metadata/current_selection_coverage.json | VERIFIED_NOT_PRODUCTION_READY | none |
| F-547 | 本文Verification Plan / 精确五文件范围 | backend/tests/advisory_model_first/test_spo_all_packages_validation_v1.py; artifact: X:/AIstock_temp/ADVISORY/p17-spo-all-packages-20261027/validation_receipt.json | VERIFIED | none |

## 实际结果 / 完成状态

协议SHA `4721ecc7808a97cf092811ad094ac7534a265437442e3665e6ee45e04754d668`，source producer `bd3063c`。4阶段一次执行，新增两包12,600输入行、25,200两臂预测，真实0fit；原两包报告及panel原样复用。四包95共同成熟D主增量−10.427423bps，95%[−45.072984,21.504028]；LSTM正、TCN/LGB/multi负。原P16的100共同D及INNER_SUPPORTED不回改。旧单包原2025评分未在已核对登记来源找到，NOT_EVALUABLE而非0收益。

8个直接测试节点通过；第一轮仅因测试临时目录的父目录尚未创建发生6个setup错误，创建授权目录后只复跑这6项通过，另2项此前通过。研究前完成，真实研究无失败重试。第二算术路径核查25,200预测、固定十槽估值与1,743个重叠stock-D的六量价字段（容差1e-12），4阶段链及原两包复用一致。详细风险/UNKNOWN/逐折结果和下一步owner交付要求见[分析报告](../analysis/advisory_spo_all_packages_validation_20261027.md)。

依据新结果，已执行后续零fit换股损益归因及前向输入核查；当前候选停止全包推广和大规模确认排期。四single包现有评分数量足够Top50，但前向D特征/binding/因果时钟尚未交付，multi当前日缺失；没有伪造影子运行。后续只先准备固定五包前向输入，再提出一个事前适用性假设，不择包/改阈值/追加fit。这是完整输入评审及可评价分支交付，未声称全五包经济效果、独立确认或业务接入完成。

## Non-goals / Risks / Production Gates

仅新增本文、分析报告、蓝图、单个验证模块及直接测试；详见X任务scope.json。生产DDL/DML、依赖、客户端均noop；无模型登记、榜单替换、服务/脚本/QE/数据集操作。旧单包不可评价和多Alpha复评停滞是需原owner接续的问题，不纳入本次静默修复。backend_restart_required=false。
