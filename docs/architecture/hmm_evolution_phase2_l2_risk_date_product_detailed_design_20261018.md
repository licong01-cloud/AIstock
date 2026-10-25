# L2风险指定日期输出的可选产品消费详细设计

> v1.0，2026-10-18，tier=F2，lane=feature。用户授权执行下一步P0；只实施HMM自有产品链，不增加模型/消费公式。父蓝图§1.0/§1.19/§7；前置日期推断#6291（merge d13ef6a）。

## 1. Background / Goal

终极目标是有经济价值的申万L2轮动与风险辅助。冻结20D独立风险模型已经支持显式日期零fit文件推断；原产品只接受424日development带评价资产。把新预测接入同一repository、两个read API和风险页面，允许明确选择run，不建设第二套平台，不替换旧QE模型。工程可用不代表收益已证实。

## 2. Scope / Architecture / Non-goals

允许文件：risk_l2_prediction.py、新risk_l2_date_product.py、import_risk_l2_predictions.py（如需结果字段）、backend/tests/hmm_risk对应产品/API/CLI直接测试、frontend/src/lib/hmm-risk/api.ts、frontend/src/components/hmm-risk/RiskL2Panel.tsx、frontend/tests/hmm-risk/risk-l2.spec.ts、HMM自有risk_l2两表的附加migration、本设计和父蓝图必要状态。

显式import request → 已有日期request/features/两child/acceptance的严格读取和验证 → 同一RiskL2Product → 原事务writer/readback → 原run_id API → 原页面。源资产不重建；不运行推断/fit或读取标签。禁止改QE/Selection/Paper/Advisory、全局CI/nox/test plan/规范或数据生产。禁止dataset/profile写入、进程控制、依赖安装、训练、tail outcome读取。

## 3. Contracts / D1：输入与模型身份

新增独立import schema `hmm_risk_l2_date_product_import_v1`，精确声明日期request、features、acceptance及两份prediction绝对普通文件路径与各自canonical SHA。沿原reader禁止symlink/junction、读中变化及unknown schema。两child还比较实际文件SHA，以验证真实字节一致，而非相信self-reported boolean。全部外部pin明确给出，禁止latest/数据库/旧root fallback。

复用已合入date_inference.validate_bundle、validate_prediction与model_source核验原模型、20D、calendar、PIT、release及完整date-sector grid。只读原固定模型认证资产，不读取原labels/features/outcomes；不重新推断。acceptance必须COMPLETED、2 children且bitwise一致、fits=0，以及labels/outcomes/DB/dataset/profile/runtime/policy/forward flags=false。request/bundle/source commit/model/inference identities、summary及prediction SHA均闭合。保持同模型不同输入双身份。

## 4. D2：存储语义与兼容

复用HMM自有`risk_l2_run`和`risk_l2_prediction`列，无新表或平台。旧basis=`HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT`的424日、55,544行、原effect/outcome校验保持不变。

新增basis=`CAUSAL_DATE_INFERENCE_NO_OUTCOMES`，日期非空、ISO排序唯一、晚于原train_end且不晚于绑定cutoff，每日精确131目录；expected_row_count=131×日期数。effect_status=`NOT_EVALUATED_FOR_INFERENCE_DATES`；各行outcome_status=`OUTCOME_NOT_EVALUATED`，event/drawdown/return必须null。不得以OUTCOME_NOT_MATURE、负标签或旧development指标冒充本次评价。

run_id仍为独立acceptance hash；model_hash为原模型，input_hash为本次inference identity的canonical SHA；新分支mapping_hash钉住完整industry_authority_sha256（authority内含PIT pins）及security_identity_sha256，不伪造旧carrier顶层的pit_bundle_sha256字段；原历史mapping公式不变。compact_summary保存日期coverage/day hashes、model训练身份和inference身份及禁用动作标志，不填假precision/HAC。完整目录和合法NA保留，不因停牌/无成员而缩小分母；未知数据错误仍失败。capability最多RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED；全NA为NOT_AVAILABLE；forward/advisory保持未确认/不可用，不推导策略采用。

## 5. D3：事务、读回及迁移

沿既有每run锁、不可变insert、同run幂等冲突校验和commit前完整readback。API显式run_id，不选latest、不回退旧run。旧分支和新分支在源码、SQL约束及UI均按basis识别；非法basis拒绝。

附加migration只调整HMM两表中阻止新basis/date count/effect/outcome的约束，保留原分支和概率/身份/主键/因果检查。不更新旧行、不查询其他业务表。本次仅提交迁移源码；实际DEV执行、生产授权、migration、数据写入/readback、runtime加载均为独立状态。未迁移时writer须显式失败，不能切换文件store或吞错。未经授权不连接数据库验证。

## 6. D4：原API/UI的诚实展示

保留`/hmm-risk/risk-l2/overview`及`/risk-l2?trade_date=&run_id=`，无需新增router。API返回basis及精确输入/模型身份；页面根据basis校验旧424日或新非空日期，不任意放宽旧合同。日期推断展示“指定日期风险推断 / 本次未读取未来收益、未评价效果 / 未前瞻确认 / 未应用交易策略”，训练模型身份与本次release明确分开。旧development指标只在原分支展示；新分支不渲染不存在的precision/HAC/标签。

前后数量合计1～30只影响展示，默认10+10；后台/下载保留全131。未知概率不进低风险榜，真实零报警不冒充失败。显式run切换清空旧数据、拒绝身份错配，异步旧响应不得覆盖新版本。产品surface receipt沿现有run+row hash身份分离；源码测试不升级surface状态。

## 7. D5：验证及审修

直接RED/GREEN覆盖真实新carrier→Product→事务/读回→read API同一身份、旧424日回归、child/acceptance/source drift、缺日/行业/NA/tie边界、NOT_EVALUATED防标签注入、数据库显式目标及write rollback/冲突。新增最少的UI合同测试覆盖新分支/旧分支/错配/NA，不删除已有安全测试、不重复全矩阵。使用既有真实两日期资产做只读validate及无fit/DB/network poison；不重跑特征或实验。

最小pytest、Ruff/compile、ownership/module registry、fresh-process imports、L0、F2及相关frontend检查。至少两轮作者审修并记录finding/fix；不冒称独立第三方审核。广域回归由CI/Nightly负责，不盲跑完整HMM矩阵。

## 8. D6：完成、Rollout / Rollback 与停止

源码完整交付包括migration、loader、repository/API、UI及直接验证；PR前按实际changed-files计算backend/frontend影响，不能手工降级。真实迁移/write/readback和无mock浏览器在单独授权后验收，未执行保持PENDING，不以fixture/CI作为生产成功。现有用户服务不启动/停止/重启。rollback为不采用新run，旧run/旧QE入口保持原样；不删历史。

本次任务只含源码/设计/测试；不自动合入或cleanup，除非用户明确授权当前目标。无九月数据时不等待、不重跑旧103日回放、不新增研究方向。

## 9. Design Acceptance Index / Design Acceptance Matrix / Verification Plan

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §3～§4，risk_l2_date_product.py | test: backend/tests/hmm_risk/test_risk_l2_date_product.py；source/model/双身份/完整131/无标签合同 | DESIGN_REVIEW_VERIFIED | 无 |
| F-012 | §4～§5，risk_l2_prediction.py及附加migration | test: backend/tests/hmm_risk/test_risk_l2_prediction.py；旧run和新日期不可变事务/读回 | DESIGN_REVIEW_VERIFIED | 无 |
| F-013 | §6～§8，RiskL2Panel.tsx/api.ts | test: frontend/tests/hmm-risk/risk-l2.spec.ts；新旧basis与状态/NA/异步identity；test_risk_l2_api.py | DESIGN_REVIEW_VERIFIED | 无 |

本矩阵是设计合同审核，不宣称源码/部署完成；真实DB授权、迁移/write/readback、无mock页面均在§8/§12明确保持pending，不能用本表或validator PASS冒充业务验收。

## 10. Implementation Plan

同一业务包连续实施carrier reader、分支化run/row验证、附加migration、既有CLI/API/UI消费、直接新旧回归与两轮审修。真实两日期资产只读转换验证，不重新生成。完成后给出精确源码、CI及未执行数据库/运行态项目，不把每个小步骤拆为独立阶段。

## 11. DESIGN-COMPLIANCE-001 / Risks

1. 不简化：完整源码链，不把离线validate称API/UI已上线。
2. 无静默错误：身份漂移/缺失/schema/SQL失败均typed；合法NA保留null与原reason。
3. 不迁移业务：固定参数、20D、warning=.20和训练身份保持；不接交易/改QE。
4. 不私增门禁：无新效果/资源/全行业成功/人工审批门；生产与源码本来独立，不为记录重启。

## 12. Production Gates / Rollout State

风险：现有表约束需真实迁移后才能写新run；固定模型可能随时间失效，不能把日期延伸当作经济有效。production_ddl_gate=pending_not_authorized，production_dml_gate=pending_not_authorized；dependency_gates=noop；database_write=false，dataset_write=false，active_profile_write=false，runtime_activation=false，process_control=false。runner-owned临时frontend的fixture验证不是生产UI验收，不启动用户后端。

## 13. 作者审修与实际结果（源码/运行验收分开）

第一轮：按已合入真实carrier核对模型及输入，修复把旧研究的顶层PIT字段假设用于新日期identity的问题；新mapping绑定完整industry authority及security identity，旧mapping hash不变。新入口精确字段白名单、两child实际文件bytes比较，不接受重哈希追加labels或仅自报成功。未改冻结20D、模型参数、阈值或原资产。

第二轮：相关小矩阵首跑62 passed/1 failed，故障反例显示“将新run伪装成历史basis”时漏出KeyError；已加显式mapping合同检查，失败节点复测通过。增加全NA产品测试（目录不消失、capability=NOT_AVAILABLE、概率/warning=null）及页面basis/model/标签隔离校验；新入口12项直接测试通过。此处均为作者审修，不称独立第三方审核。

实际只读validate及fresh-process：复用§1.19既有request/features/acceptance/两child，model=`37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d`，run=`e200669ea2f434041eaa8ac13926ee8e2b9590d5f762493485da97dfa637b16d`。新产品row hash=`896dfc37689e531e1dc63718f0c3a5e9c1f1a43d0b98fc7132cfebc8ed6053f8`；262行，2026-08-28/31各131目录、129 available/2 typed NA，分别26/28报警。fit/prepare/infer/evaluate及DB/network poison下只做验证，再用内存事务double与真实ASGI handler读回；**没有实际数据库写入，不是生产API或浏览器验收**。

UI：现有HMM fixture计划23 passed/3 live项skipped；页面细化后只复跑risk-l2直接spec，3 passed/1 live项skipped；相关TypeScript及直接imports通过。RTK Playwright wrapper一次解析失败后改现有node CLI，不安装依赖、不以该包装错误当业务失败。复用现存frontend依赖，临时前端在验证端口由runner管理，不操作用户后端。

Ruff/format、py_compile、scope ownership（10/10、无unmapped/ambiguous）、registry（8 passed）、L0（0 blocking）、本设计和父蓝图F2通过。L0的既有MEDIUM raw-JSON提示对应“下载完整131结果”的序列化，不是新增原始JSON页面，未因此改动既有功能。源码影响如实为backend、target=[backend-main]；正式runbook=`docs/operations/backend_main_runtime_restart_runbook.md`，identity=`/api/v1/runtime-identity`，business smoke为带本run_id的`/api/v1/hmm-risk/risk-l2/overview`；canonical contract pre_pr_ready=true、blocking=[]，fresh process直接导入hmm_risk/health及新依赖。这不等于已加载或surface已AVAILABLE；需后续真实迁移、写入、API/无mockUI及receipt验证后才可升级，记录本身不要求重启。

最终HEAD与CI随PR报告单独核对；当前未合入、未清理、未执行DEV/生产DDL/DML、未激活模型或服务，QE/数据生产等模块零修改。
