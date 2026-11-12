# LSTM-BUSINESS-1：Top10辅助观察的业务承接与可评估性（F2）

2026-11-01。用户授权复盘后的长任务，明确选择100bps（1个百分点）作为5TD批次净增量的实用价值研究目标。本轮交付只读业务视图与事前V2协议，不宣称该目标已达成。

## Background / Goal / Scope

首个对象限定精确P19第3折LSTM包的Top10辅助观察；原Top50中比较模型Top10与原分数Top10，不替换生产Top20或产生交易指令。模型、包/manifest/run/source/program/binding沿P20；原模型和release物理身份保持。固定5TD、买0.95/卖5.95bps、十槽等权、UNKNOWN/CASH不变。

100bps是用户选择的批次平均净增量研究目标，已扣原费用，不再重复扣一轮费用；不代表单股必涨1%、账户收益率、实际成交或已证明真均值超过100bps。统计主假设仍μ>0，与点估计是否达到100分别报告。旧开发整体+57.28bps低于新目标，保留原结果，不选最好折自证；本轮0fit/新政策/新收益窗，研报/现金流/价格/Exit暂停。

精确13文件见X:/AIstock_temp/ADVISORY/lstm-business-review-20261101/scope.json。原V1脚本和协议不编辑；原资产和数据根只读，无DB/数据集/依赖/服务控制。新增能力复用现有API、页面、估值器和确认函数。

## Non-goals / 非目标

本轮不训练或更换模型，不查询新原始收益，不替换生产Top20、不自动交易或激活，不恢复其他研究方向或启动脚本任务。成熟合成样本只验证合同，真实经济确认须等待原自然时钟与事前研究条件。

## Architecture / Contracts / API与UI

新增纯展示叶模块，由既有LstmSpoShadowService.status读取已保存文件后调用。叶模块不访问DB、TDX、文件或训练器；对50行原排名和模型排名/唯一KEY、评价父hash/原paired_result逐项核对，拒绝错误身份和篡改结果。status只增加business_contract、business_summary和selected.business_review；原字段和调度逻辑不改。

逐股显示RETAINED/ENTERED/EXITED/OUTSIDE（原生与模型Top10的集合关系，不是实际持仓买卖），原/新rank、已保存5TD参考净收益、对重排差值的贡献。贡献=(模型入选−原生入选)×原净估值/10。UNKNOWN的收益/贡献保持null；无成熟评价时所有收益未知，不能用最新价替代T开盘→T+4收盘标签。CASH显示0与未执行含义。

两臂都完整时分解新增盈利、错失盈利、避免损失、新增损失；净避亏＋净盈利机会必须还原原delta。缺失影响主结果时组件保持未知，不拿部分已知组件冒充总增量。汇总仅使用同一组完整配对D，显示完整/未知/未评价数量，明确DESCRIPTIVE_COMPLETE_PAIRS_ONLY，不作为正式确认。

UI沿用ModelObservation和QuoteCell：保留当前行情；明确Top10辅助用途及100bps目标；显示两臂批次参考净收益和差值/未知槽、四项经济分解；默认模型Top10，可切原生Top10或原50行；换入/换出/保留/范围外用既有颜色token和文字。历史表增加两臂收益，所选日与历史汇总分别展示。后端旧版本缺新增字段时标注业务视图待加载，不能用默认零/假数据补齐。其它策略包保持NOT_APPLICABLE。

## V2研究合同 / 时钟与失效处理

原V1 protocol SHA 9b78a785c44158fb689089514756d0a590a28db5d34d80a531b53cd15cb094e2完整保留，只作归档；V2是唯一后续正式协议。新operator绑定V1物理hash/manifest及其producer的Git规范化源码闭包，证明历史协议原样，不通过改hash伪造旧验证。共享status源码新增展示会改变main闭包；V1原实现可在其固定producer checkout复核，不强行令其在新main通过。

V2保留从2026-11-02起首150个官方预定D、原生Top10主对照、唯一经济判断、5会话修复宽限和最晚2027-08-31结束；排除10月30日先行批次但P20照常评价。只增加100bps业务参考目标及收益无关的提前可评估性检查。注册必须早于首个D开始；旧V1若已有正式终态则拒绝静默另起确认。

health只读取官方日历与预测/MISS，无评价文件入口。部分未来日历可检查已经到期的原预定D；未提供的交易日不能猜。T开盘前预测缺失为窗口尚开，T开盘后仍无合法预测为不可恢复漏捕获；MISS要核对原身份和时钟，预测/MISS并存拒绝。只要一个固定D不可恢复漏捕获，finalize即可提前保存NOT_EVALUABLE_CAPTURE_GAP，0收益读取，原P20不停止，不把它解释为无alpha。UNKNOWN评价仍可能修复，不在宽限到期前宣布不可恢复。

若无捕获缺口，沿V1官方日历/固定look_at检查，届时才用原collect/decision/描述统计计算。所有150D完整才有正式统计；点估计≥100仅表示达到参考目标，结合统计正支持给出WORTH_BUSINESS_REVIEW，仍不自动启用。未达目标不证明真均值低于100；未显著不证明无alpha。原NIST条件预算/HAC假设、NON_VINTAGE及非成交限制保留。

V2的protocol、budget、supersession、health/preflight和单次终态放在独立新X根，create-only，唯一study_root。原V1不再用作第二次有效检验；不因运行不顺换日期/扩样/另拟合。已终止时复用终态，不继续打开收益。

## Implementation Plan / Verification Plan

1. 纯投影及现有status追加；合同测试覆盖50人口/排名/双臂/UNKNOWN/CASH/分解和不执行源读取。
2. V2复用V1固定统计/发布接口，验证历史Git身份、事前注册/单一目录、未来日历不足/开盘前等待/过时漏日/矛盾/UNKNOWN不提前终止及成熟完整/缺失终态。所有成熟案例仅合成合同测试。
3. UI与API类型同步，测试成熟/未成熟/未知/旧后端、两臂与目标、集合切换/颜色文字，保持只读请求和当前行情；使用既有runner-owned隔离前端，绝不操作用户后端。
4. 真实新进程只消费10月30日已保存50输入和当前0评价，冻结V2并健康预检，文件打开拦截证明未读收益；全部受保护文件前后hash一致。不能造150真实结果。
5. R1算术/时钟、R2业务与统计、R3接口/UI/身份/无副作用多轮自审并修复；F2逐项、直接lint/compile/tests、L0、ownership/CI路由后提交合入。

## Design Acceptance Index

| ID | Requirement |
|---|---|
| F-601 | 精确Top10辅助业务、用户100bps目标与统计/生产决策分离 |
| F-602 | 原50身份/两臂/UNKNOWN/CASH、换股与贡献分解真实只读投影 |
| F-603 | V1归档身份与V2事前冻结/唯一目录/原统计和有限预算 |
| F-604 | 官方部分日历、收益无关的不可恢复漏日提前终止，UNKNOWN宽限保留 |
| F-605 | 原API追加与现有观察页的两臂收益/目标/换股/当前行情和错误态 |
| F-606 | 新进程真实预检/0标签/源hash、合成成熟路径及后端直接测试 |
| F-607 | 浏览器行为验证、多轮审核、全设计合入及运行激活独立报告 |
| F-608 | 按精确冻结模型身份展示已有压力终态、原分母/区间及可追溯来源，身份不足保持UNKNOWN |
| F-609 | 压力结论与每日描述性收益、100bps目标、正式V2及生产状态分开；旧接口与历史回放不误套结论 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-601 | 本文Contracts；业务叶模块 | backend/tests/advisory_model_first/test_lstm_top10_business_review.py | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-602 | backend/services/advisory_model_first/lstm_top10_business_review_v1.py | backend/tests/advisory_model_first/test_lstm_top10_business_review.py | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-603 | scripts/advisory_lstm_top10_business_review.py | backend/tests/advisory_model_first/test_lstm_business_protocol.py | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-604 | scripts/advisory_lstm_top10_business_review.py | backend/tests/advisory_model_first/test_lstm_business_protocol.py | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-605 | frontend/src/components/advisory/workbench/ModelObservation.tsx；API类型；status | frontend/tests/paper-v2/advisory-workbench.spec.ts | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-606 | 本文Verification Plan | artifact: X:/AIstock_temp/ADVISORY/lstm-business-review-20261101/validation_receipt.json | VERIFIED_IMPLEMENTATION_REAL_PREFLIGHT_AND_DIRECT_CONTRACTS | none |
| F-607 | 本文Rollout；结果报告 | frontend/tests/paper-v2/advisory-workbench.spec.ts | VERIFIED_SOURCE_BROWSER_USER_OWNED_ACTIVATION | approved_by_user：既有用户指令“后端重启我来执行”；源码/新进程/浏览器验收已完成，运行进程加载由用户重启后另验 |
| F-608 | backend/services/advisory_model_first/lstm_top10_business_review_v1.py；lstm_spo_shadow_v1.py | backend/tests/advisory_model_first/test_lstm_top10_business_review.py；artifact: X:/AIstock_temp/ADVISORY/top10-research-evidence-20261109-v1/source_audit.json | VERIFIED_SOURCE_AND_FRESH_PROCESS | none |
| F-609 | frontend/src/components/advisory/workbench/ModelObservation.tsx；API类型 | frontend/tests/paper-v2/advisory-workbench.spec.ts；artifact: X:/AIstock_temp/ADVISORY/top10-research-evidence-20261109-v1/validation_receipt.json | VERIFIED_SOURCE_BROWSER_USER_OWNED_ACTIVATION | approved_by_user：用户明确“后端重启由我执行”；源码/新进程/浏览器验证完成，运行加载待用户重启后核验 |

## 2026-11-09增量：已有研究结论进入业务观察

用户授权盘点业务缺口并选择最高价值方向直接实施。当前原生发布/工作台/双Top10与V2均已有实现；54字段模型轮换、关系、h64、ID1272已结束，暂无可据以立项的新训练候选。选择一个可立即影响辅助名单解读的缺口：正在观察的固定P19 fold3模型，已有[跨期压力终态](../analysis/advisory_lstm_frozen_specialist_stress_20261102.md)，当前GET只显示自然观察收益，缺少该模型的停止主动研发结论。此交付不制造新Alpha、训练或新准入门。

P0在新任务根`X:/AIstock_temp/ADVISORY/top10-research-evidence-20261109-v1/task.json`冻结计划、8个精确写路径与源身份；P1在原纯投影叶中保存原压力结果的最小只读投影，status追加`research_assessment`；P2现有观察页先展示研究用途与压力证据，再展示原每日收益；P3直接合同、相关浏览器小矩阵、独立新进程核对原report物理hash/全部投影值及零副作用；P4按现有授权交付源码，用户重启后的运行验收另报。

证据只适用于固定model SHA `8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813`、payload、原package/manifest/run/source/program/binding、fold3、训练截止2025-07-31及原估值策略。匹配身份使用本次冻结的字面常量，不能从未来可变release反向继承。任一身份缺失/不符返回UNKNOWN、null经济数值和可见原因，不将此压力结论套给ID1272、其它包或回放折模型。原`_release`的错误合同保持。

投影原2026-03-11～08-28的118原D、113成熟D、106完整配对D；专属−原生−136.02374797220642bps，原描述性95%时间块区间，成熟不完整7D/截止未成熟5D和双臂UNKNOWN槽53/54均保留。来源包含原report SHA、evaluated stage SHA、producer与报告路径；运行读取无需访问X盘研究目录或新市场源。旧已消费NON_VINTAGE压力、非独立经济样本/非真实成交限制保持。固定T开盘至T+4收盘、买0.95/卖5.95bps及100bps目标不变。

UI显示“已停止该冻结模型的主动研发；仅作辅助观察”，不把暂停训练说成停用生产或停止P20。压力历史、当前所选批次及自然观察汇总分别标注；缺新字段明确状态未加载，UNKNOWN明确身份未能匹配；历史工程回放不应用当前模型压力结论。无需新增页面、调度、研究注册平台或外部审批。原榜单、模型/release、V2文件、数据库与服务不写，累计269实际fit＋1旧index、本轮预算0。

已实现上述增量。27项直接后端合同、9项相关浏览器场景及TypeScript/ESLint/Ruff通过；新进程仅从原report核对统计与来源，投影期间禁止文件打开，PASS。浏览器首轮因测试临时目录物理大小写差异在加载前失败，保留该记录并以一致物理TEMP重跑通过，未修改业务代码或测试断言。L0无阻断，P2复杂度提示为原固定50行1:1连接，新增研究投影只有固定13项身份比较和固定大小复制；RAW_JSON_UI提示来自测试transport的JSON序列化，产品仅展示字段及折叠来源。当前Agent三轮自审检查来源/身份、分母/UNKNOWN与业务/范围，属于自审而非外部独立审核。

## Risks / Production Gates / Rollout / Rollback

2026-11-12接续：修复现有观察组件可选`assessment`的显式类型收窄，保留旧后端/身份不匹配的UNKNOWN提示；非增量TypeScript检查与相关浏览器合同另行绑定本次交付HEAD。正式确认使用现行V2，归档V1不改；本次V2按F-603/F-604核验6份原预测与两日不可恢复漏捕获，依既有合同发布`NOT_EVALUABLE_CAPTURE_GAP`并验证终态复用。回执`X:/AIstock_temp/ADVISORY/top10-research-evidence-20261109-v1/confirmation-viability-20261112.json`绑定原协议、终态stage及20项未变工件，评价文件读取0；这是批准的收益盲可评价性终态，不是新增统计检验、没有Alpha的证明或停止P20。原压力展示、自然观察与V2终态分报；无需修改确认脚本、放宽源码/时钟校验、重新冻结或替换缺日。

短期样本、市场非平稳、旧训练NON_VINTAGE、未来日历不齐和严格全人口仍可能使正式研究无法评价；不放宽缺失换取好看结论。150D只保证资源边界，不保证检测100bps的未来功效。单批预测/finance各固定50行，排名校验最多排序50元素、金融连接为50键1:1，新增汇总只遍历已有捕获D；不引入业务数据库查询或新调度。日历operator仅显式执行既有只读日历查询。

后端status变更需要用户重启后做只读API/业务验收；本轮完成源码、CI及独立新进程业务证明，不把用户重启等待伪报完成。0DDL/DML，DEV/生产迁移、依赖/客户端/数据集/QE均noop。frontend按既有运行方式加载，禁止操作生产服务。回退源码即可移除展示/新operator，原模型、预测和P20逻辑保持；不删工件。工作树/分支保留。
