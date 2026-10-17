# L2风险冻结模型无标签指定日期推断详细设计

> 版本：v1.0；日期：2026-10-18；tier=F2；selected_lane=feature。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md` §1.0/§2.3/§7。
> 授权：用户批准本轮连续任务并于2026-10-18要求开始执行；不变更模型、特征、训练窗、seed、阈值或消费公式。

## 1. Background / 问题与目标

独立L2风险模型已有冻结参数、20维C-010/A5特征和历史因果预测，现有产品/API只读取历史研究资产。旧研究CLI固定日期与日历，不能作为可随数据时间延伸的指定日期入口。本包交付HMM侧可复用service和薄CLI：显式日期及冻结数据身份→无标签20维→冻结参数推断→完整131目录研究输出和最多30条风险展示索引。

终极目标仍是可验证经济价值的申万L2轮动与风险辅助。本包只解决既有风险模型的合法消费能力，不把工程能力当作净增益。QE正式实验由QE窗口后置；不再启动第三个轮动候选。

## 2. Scope / Non-goals

新增`backend/services/hmm_risk/risk_l2_date_inference.py`、`scripts/hmm_risk/run_risk_l2_date_inference.py`、`backend/tests/hmm_risk/test_risk_l2_date_inference.py`；只对`formal_state_input.py`的共享有界reader增加可选显式calendar pin（旧调用默认完全不变），以及本设计、父蓝图必要状态。

不改QE、Selection、Paper、Advisory、数据生产、CI/nox/test plan。零fit，不读取标签/未来收益资产，不重新过滤HMM或拟合scaler；不发布新模型、写DB、增加调度器/环境变量/registry、不迁移历史证据。该离线文件接口不声称新增数据库实时推断、API/UI部署或任何真实场景采用。

### 2.1 Architecture / 最小复用架构

显式request经正式direct-v2 source reader进入原C-010/A5 kernel，冻结scaler/logistic参数只做predict_proba，输出独立研究文件。模型认证与本次数值输入分离；CLI只承担一次prepare、两child和readback，不引入模型平台或其他模块入口。

## 3. Contracts D1：固定模型与双身份

只复用已批准独立L2风险模型，`risk_l2_value_replay.APPROVED_PINS`中的model/sealed/acceptance、`risk_l2.SOURCE_REQUEST_FILE_SHA`和`risk_l2.CONTRACT`均不变。训练期mean/scale/active/coef/intercept/classes直接恢复，warning仍`probability >= 0.20`。

模型bootstrap只认证原source request、sealed、acceptance及其直接链接，不要求原features/labels文件存在。acceptance.model必须逐项等于sealed除predictions/receipt之外的carrier；parameters canonical hash必须等于原model hash，原feature、input及PIT身份仍链接原pins。原source中的旧numeric root不访问。

分别保存`model_identity`（训练时input/PIT/model身份）与`inference_identity`（本次release/calendar/PIT/security/provider/source facts）。兼容新release只改变显式请求；不得改旧hash、私有行业编号或把未来数据身份冒充原训练身份。新数据authority可用既有正式C-013接口显式绑定，必须保持131个正式L2文本代码、原20维定义和train-only eligibility；不能静默替换权威。

## 4. Contracts D2：输入、日期与有界读取

request使用`hmm_risk_l2_date_inference_v1_request`，精确字段：schema_version、model_assets、source、decision_dates、display_limit、receipt_sha256。拒绝未声明字段（含labels/outcomes）。source显式提供candidate_root、六字段release_binding、calendar_sha256、security_identity_manifest及canonical SHA、provider_absence_manifest及canonical SHA、industry_authority、work_parent。所有路径为绝对普通路径，禁止symlink/junction、latest搜索或旧目录/数据库fallback。decision必须晚于冻结模型训练截止日，不得把训练前回推冒充当时可获得的模型预测。

既有正式direct-v2 reader验证state/schema/profile/cutoff/CANDIDATE_READY/组件及manifest pins。日历来自同一候选且SHA必须匹配本次请求及reader inventory，不硬编码月份。decision_dates必须非空、ISO、排序唯一，全部为冻结日历内正式open dates且不晚于cutoff；不自动改为最近交易日。`as_of(t)=prev_open(t)`，输入数值截止最后as-of；不读取decision当日或后续收益作为特征。

首个decision前读取264个open source days（包含首个as-of）。最长合成滚动为ATR14→rank250，外加价格前置观察，264为源公式推导的上下文，不是训练窗或调参。复用reader真实严格前置价格/市值上下文，禁止补零、前填或缩短所需窗口；上下文不足typed失败。任意多日期批量只构造一次共享source，逐日kernel必须是因果向后滚动；单日和同身份批量的相同日期输出一致。

## 5. Contracts D3：特征与自然NA

复用`_bounded_l2_stock_facts`和`build_c010_feature_domain_panel`；原20维ALL_CORE_FEATURES顺序及feature_definition严格相等。security alias、严格前置circ_mv、PIT、双层moneyflow及cross-section合同不变。行情/资金流/日历等数值只读同一release；provider absence沿正式受控manifest pin。

131目录不因无成员/单成分/停牌/行业停发消失。合法自然NA输出`probability=null, warning=null, availability=unavailable, reason_code=<原typed reason>`，不改成neutral/0/1系数，不要求131行业全部可预测。未知缺数、身份漂移、schema/重复键/非有限派生数值、非法未来日期仍fail closed，不把执行错误吞成合法NA；全NA可以如实交付完整目录，不宣称有可用预测。

## 6. Contracts D4：service/consumer 输出

`prepare(request, source_commit)`构造无标签features bundle；`infer(request,bundle)`只调用既有`risk_l2.predictions_from_parameters`，不估计任何参数。模型authority与request/bundle receipt逐项闭合，features严格20个有限实数或合法null+reason，禁止bool、额外/缺少date-sector或as-of漂移。

输出`hmm_risk_l2_date_inference_v1_prediction`：request/source/model/feature identity、按日期和canonical L2排序的131×日期完整predictions、warning、probability、availability/reason、as_of_date/effective日期及每日期的coverage。`display_limit`为1..30整数，只产生风险概率降序、文本代码稳定解tie的展示索引，不裁剪计算/消费集合，不把低风险等同trending。研究概率与真实应用overlay严格分离；不生成preset_A资产、不默认应用连续或二值政策。

capability仅research-only；全NA时明确NOT_AVAILABLE，`forward_confirmed=false, policy_applied=false, fits=0, labels_read=false, outcomes_read=false`。历史标签曾用于原模型训练不等于本次读取标签；输出不是收益/事件效果报告。

## 7. Contracts D5：双fresh-process、稳定性和失败

薄CLI接受显式request/pin/output；源代码先提交且clean，输出repo/dataset-external普通新目录write-once。parent准备一次features，两fresh child只读取本次bundle与固定模型、使用原固定数值环境和单线程，业务payload必须bytes完全一致；parent核对request、bundle、model、source commit、numeric environment及完整date-sector结构。不重复全量特征构造，不安装依赖。

已有poison禁止fit/filter/DB/network/active-profile动态解析；允许且只使用参数predict_proba。固定数值线程变量只属于既有执行环境，不是实验状态绑定；实验结果只在普通不可变文件，不要求环境变量切换或后端重启。

failure记录真实stage、typed reason与已成功child数量（未知保持null），collision不覆盖。源文件读前后稳定；模型输入哈希不变。测试只覆盖直接合同/故障反例；最小HMM矩阵/L0/F2，广域矩阵交CI/Nightly，不新增资源门禁。

## 8. Contracts D6：使用边界与停止

仅做指定日期功能验证，优先冻结日历最后两个合法日期（预期2026-08-28/31，以真实日历为准）。这不是新的独立预测效果验证，不读取tail outcome。单日/批量、同身份旧sealed概率逐项核对；若真实source修复导致身份不同，只能解释差异，不能强行改旧结果。

源码/直接测试/两轮以上作者审修/小矩阵和实际fresh-process完成后提交、推送、创建PR，读最终HEAD CI后停止；不自动合入或清理。等待九月唯一release不阻断本包，数据owner负责基础交付，HMM自行执行消费者业务预检。遇身份/数据/授权阻断如实报告，不另开模型/阈值搜索。

## 9. Implementation Plan / 一个业务交付包

同任务连续完成设计、reader可选calendar pin、service/CLI/直接测试、真实双process验证、既有103日价值取舍分析及蓝图状态；不把adapter/审核/记录拆独立阶段。模型、scaler、原结果与旧QE链不动。若发现必须改模型/消费公式或其他模块，则停在精确差异，不自行扩范围。

## 10. Verification Plan / Design Acceptance Index

### 10.1 Design Acceptance Matrix（设计合同，不代报源码/实验完成）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §3～§5；risk_l2_date_inference.py；formal_state_input.py | test: backend/tests/hmm_risk/test_risk_l2_date_inference.py；模型pin、release/calendar/authority、20D、as-of、自然NA与未知缺失 | DESIGN_REVIEW_VERIFIED | 无 |
| F-012 | §6～§7；run_risk_l2_date_inference.py | test: backend/tests/hmm_risk/test_risk_l2_date_inference.py；零fit/无labels、date-sector闭合、双进程、collision/failure | DESIGN_REVIEW_VERIFIED | 无 |
| F-013 | §8/§12；父蓝图§7 | test: backend/tests/hmm_risk/test_risk_l2_date_inference.py；实际单日/批量/旧sealed同身份读回；既有holding-reference acceptance | DESIGN_REVIEW_VERIFIED | 无 |

当前源码实施中，直接测试及fresh-process尚未通过，不将本表设计结构PASS冒充执行PASS；最终结果在§12增量同步。

## 11. Risks / Rollout / Rollback / Production Gates

当前数据足以验证历史指定日期能力，不意味着已有当前实时20D完整reader或前瞻确认。既有PIT使用retrospective effective-date authority，不额外宣称historical-as-known。输入未来authority不兼容应明确失败，不自动重训。概率过时风险由后续场景验证决定，不靠数据更新自动更新权重。

Rollout仅新增可选离线消费者接口；rollback为不采用新接口，不删旧结果或旧snapshot。普通backend变更应按canonical workflow实际changed-files归类backend-main，不能因离线手动降级；fresh import不是后台已加载，本包不需要为记录重启。

production_ddl_gate=noop；production_dml_gate=noop；dependency_gates=noop；database_write=false；dataset_write=false；active_profile_write=false；runtime_activation=false；process_control=false；QE_experiment=false。

## 12. DESIGN-COMPLIANCE-001 / 审修和结果

1. 完整交付：全部131目录/20维/显式日期与consumer，不以fixture或CLI冒称API/UI上线。
2. 无静默错误：身份/未知缺失typed失败，合法NA不填值、不隐删，旧root不fallback。
3. 业务不漂移：固定参数/scaler/阈值、原训练身份及既有kernel不变，仅开放日期和兼容输入身份。
4. 不私增门禁：不增逐行业全通过、效果/资源/审批门；源码、数据、runtime及经济结论分别报告。

初次作者设计审查已明确：不能直接调用固定历史schedule；模型认证不读取features/labels；264日上下文由最长组合窗口推导；旧calendar默认pin保留；不把文件推断交付称为实时数据库/API完成。

源码第一轮作者审修：发现旧calendar解析器仍强制历史development端点，修正为显式release日历的canonical日期/排序/唯一/实际cutoff/稳定性验证；保留旧kernel默认calendar SHA，新增可选pin只给新caller。补上decision必须晚于原模型训练截止日，避免早期回推使用未来训练模型。首次接线测试失败后仅重跑failed节点，最终修复。

源码第二轮作者审修：feature carrier严格拒绝labels/outcomes及未知字段；model/acceptance/原source auth不要求旧features文件；全NA明确NOT_AVAILABLE；parent核对完整grid/as-of/有限概率/warning/NA/coverage和展示，不只相信child自报hash。prepare/第二child启动/readback/collision故障保留0/1/2真实完成数；修改测试以模拟正式间接路径扫描结果，不依赖Windows管理员权限创建junction，未降低实际路径保护。首轮最终小矩阵38 passed/1 OS权限skip；该skip已改为无权限依赖的直接合同测试并将定向复跑；Ruff、registry 8项、L0零blocking、设计F2 PASS。此处为作者审修，不冒称独立第三方审核；真实指定日期推断未执行。
