# HMM Evolution Phase 2：L2绝对回撤风险研究产品完整详细设计

> 版本：v1.3；修订日期：2026-10-06；tier：F2；owner：HMM。
> 父蓝图：`hmm_evolution_and_risk_management_system_design_20260716.md`，F-011/F-012/F-013。
> 模型依据：`hmm_evolution_phase2_rotation_l2_p0_detailed_design_20260922.md` §8.2，用户已批准的`hmm_risk_l2_absolute_drawdown_logistic_v1`。源码PR #5444已按独立授权合入，merge=`2fa41eab6b7efc76173ae3a0ced4143006b1671c`；不由此推导产品完成。
> 用户于2026-10-05批准本完整产品D1～D6及后续源码；该次设计批准不自动授权数据库或进程控制。随后各目标的DEV/生产写入、receipt绑定、用户重启及精确清理已分别授权并执行，实际状态见下文及§13；历史授权边界不回写。

> 当前实现：设计#5447与完整源码#5449已合入，#5449 merge=`d0a074ddce7bd992b2d45d660b765245615c679f`。55,544行/424日/131行业的DEV先验、生产事务读回及用户重启后真实API/无mock浏览器均完成；surface=AVAILABLE_EXPERIMENTAL、capability=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED、forward=NOT_STARTED、advisory=NOT_AVAILABLE。实际进程SHA=`c3e4b3ace5c269e00a6c5747af1a23923679c357`包含本源merge；源工作树/分支已按授权清理，正式资产保留。不代表经济收益，当前无需再次重启。

## 1. Background、目标与Non-goals

终极目标仍是有价值的申万二级行业轮动预测与独立风险提示，并在未来各自场景验证后作为QE、荐股和模拟盘的版本化可选输入。当前最值得交付的是已经达到历史development效果要求的独立L2风险研究产品，而不是为未达标的R1再开候选、重跑正式HMM、归档历史或建设平台。

本包把既有封存概率完整接到HMM自有持久化、只读API与真实研究页面；不重新生成概率。历史研究页面是本包明确产品，既不是实时行情预测，也不是forward-confirmed advisory、可执行组合收益或自动风控。无可用数据时显示具体不可用原因，不以页面存在代报模型能力。

### 1.1 直接事实与代价

独立风险模型使用train-only标准化后的原C-010/A5 20D、一个pooled logistic，预测决策后10交易日绝对路径回撤`DD_min<=-0.08`。报警为`p_event>=0.20`，不是强制每日Top20%。两fresh-process共2fit一致，development为2024-07-01..2026-03-31，424日×131目录=55,544行。它是`HISTORICAL_CAUSAL_FIXED_TRAIN_DEVELOPMENT`，不能写作OOF或untouched。

- 合法M=54,012，事件7,299；known warnings=11,908，TP=3,008/FP=8,900/FN=4,291。
- precision=0.25260329190460196；base rate=0.13513663630304376；lift=0.1174666556015582；recall=0.4121112481161803，达到已批准0.05/0.25刻度。HAC和参照只诊断，不增加晋升门。
- known warnings中74.7397%未发生目标事件；其未来平均收益+3.3874%，漏报事件平均回撤-10.7998%。这些是行业路径诊断，不是交易PnL、卖出可避免损失或净收益增量。
- 完整预测中55,388有概率、156合法不可用；最后1,310目录行标签未成熟，另120为合法outcome NA，不记作event=0。全部报警12,444；每日报警0/中位25/最多131，159日超过30报警。因此UI上限绝不能变成计算或消费上限。
- 原HMM neutral-centered R1 IC=-0.0093413，未达0.02且已停止。本包不重评分、重建标签、改变seed或复活退役链。

### 1.2 Scope

包含封存资产验证、完整历史行导入、HMM自有run/预测表、read API、日期与版本显式选择、L2风险页面、真实write/readback/API/no-mock浏览器验证及回滚合同。一份设计、一个完整业务包，不按reader/API/UI/迁移拆阶段。

非目标：新模型/调参/训练、tail或新实时推理、风险概率校准、解释性训练、组合模拟、QE实验、其他模块源码、数据集改写或发布、全历史hash/资产复制、通用registry/feature store/scheduler。是否把warning用于减仓/禁买只能由后续批准消费合同和对应owner验证，不能从本包外推。

## 2. Architecture：最小复用与身份

`已封存risk acceptance + sealed prediction + 现有features catalog` → `risk_l2_prediction验证/映射` → `一次事务写入HMM run+prediction` → `risk-l2只读API` → `L2风险研究页面`。

复用当前HMM canonical JSON、typed exception、pg_pool事务/参数绑定与router、现有L2产品的写入读回和页面选择模式；新模型不套用L1的31分母、百分位报警、九维输入、relative label或旧surface receipt。仅新增当前风险产品的业务代码，不复制整个L1实现、不创建平行数据层。

两张HMM业务表分别保存一次run摘要和完整预测，避免55,544行重复嵌入同一评估摘要。它们不是全局模型注册表；不修改已有rotation、risk_L1、QE、Selection或Advisory表。

## 3. Contracts D1：显式绑定封存预测，零refit

导入request必须显式给出绝对普通文件路径及预期canonical hash：acceptance、一个已封存process prediction、features；不得latest扫描、自动换run、读active profile补齐、查询market表或重新调用fit/predict。拒绝symlink/junction；校验前后文件身份/hash相同，变动立即typed失败，不覆盖。

当前已发布的不可变资产（本次不重新导入）：

| 对象 | 位置或精确身份 |
|---|---|
| acceptance | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json`；canonical `88341607f8772bcb97d1832cd1941f92971f35d62c1f0c8ed90261a8c8df7d26` |
| sealed | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/process_1.sealed.json`；canonical `255cf2ee7dbbbc11738108fde9202c7bdee7168be5eb1a5085737bb6d5704b77` |
| features | `F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/inputs-file-only-2/features.json`；canonical `2e9911a5fd2a83c15803e120b1e3c9a21a1a9ffed7f336e53e156c9acdde1c70` |
| model parameters | canonical `37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d` |
| execution source | `02d15a1f899cf399182f45b0c85e99644a043173`；后续doc HEAD不要求重跑fit |
| dataset | v17 / manifest `97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`；full-v3 PIT `051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313` |

authority是request中独立的预期pins，不接受只改payload再自算hash的漂移。验证完整原模型CONTRACT/schema、执行COMPLETED、两process bitwise、planned/completed fits=2、no-tail、basis、feature/input/model参数hash。sealed与acceptance.model（除sealed自身receipt/predictions）逐字段一致；sealed逐行预测只能包含当时可得字段，acceptance附加outcome不能改变概率、warning、availability/as-of或人口。

表中acceptance/sealed/features的canonical值沿仓库原receipt规则：`canonical_sha256({k:v for k,v in payload.items() if k != 'receipt_sha256'})`，必须同时等于原`receipt_sha256`和request独立pin；不是完整JSON（含receipt字段）的hash，也不是文件字节SHA。model parameters hash仍直接对原parameters对象canonical计算。两种身份不得混用；读前后原文件字节稳定性仅用于检测并发改写，不替代上述业务身份。

features的catalog及calendar经过其原hash验证。严格要求每个424日具有同一131个正式文本代码，日期/代码唯一，as_of是同冻结calendar的严格前一开市日；不使用稀疏整数ID作数组下标，不查询当前股票列表。全部参数和input identity按原样绑定，不把quote-unavailable偷换成member不存在；C-010股票事实模型不是官方指数版。

所有概率须有限且在[0,1]，warning必须精确为`probability>=0.20`。unavailable必须为null/null及非空原reason，不填0、1或no-warning。合法停牌/预热NA保持原行，不把其他行业一并判坏。全部未知身份/非法值/缺行fail closed；本验证不新增逐行业效果或三态门。

原catalog只有代码、没有正式名称。本轮`sector_name`只可空并显式`name_authority=CANONICAL_CODE_ONLY`，UI显示代码而非猜名称；今后同一冻结authority提供正式名称才能显式扩展，不读运行时行业表。

## 4. Contracts D2：不可变持久化、完整事务与读回

### 4.1 最小DB schema

`hmm_risk.risk_l2_run`：run_id char64主键，acceptance_hash、sealed_prediction_hash、feature_hash、model_hash、contract_hash、input_hash、mapping_hash、executor_commit、model_version、validation_basis、catalog jsonb（131正式代码）、dates jsonb（424日）、input_identity jsonb、compact_summary jsonb、expected_row_count、effect_status、risk_l2_capability_status、forward_power_status、forward_confirmation、advisory_status、created_at。

`hmm_risk.risk_l2_prediction`：外键run_id ON DELETE RESTRICT，trade_date、as_of_date、sector_level='L2'、sector_code、sector_name nullable、name_authority、probability double、warning boolean nullable、availability、reason_code、structural_eligible boolean、outcome_status、event nullable、realized_drawdown nullable、realized_return nullable；主键(run_id,trade_date,sector_code)。

run_id取已验证acceptance canonical hash；其他hash按对应原JSON或参数canonical规则产生，不包含本机绝对路径。概率部分与事后outcome身份分别保留sealed/acceptance hash；本包不新增revision、更名旧run或in-place correction。真正的新合法结果只能形成不同run，不使同一身份变异。

`contract_hash=canonical_sha256(原risk CONTRACT)`；`input_hash=canonical_sha256(原model.input_identity)`；`mapping_hash=canonical_sha256({pit_bundle_sha256,industry_authority_sha256})`。不重算全历史原始文件hash。业务row identity不包含DB created_at或本机路径，浮点保持原值，按(trade_date,sector_code)排序；行重排不能改变canonical结果。

compact_summary只保留总体/三个固定报告块、HAC、参照与代价、日度计数（原424条）、原证据/coverage和所有状态；不要复制股票事实、features或每棵模型日志到DB。

import validate从封存rows重算覆盖、TP/FP/FN/precision/lift/recall/Brier及原effect判定，核对源summary，不信任篡改后自报达标；这只是读取既有development结果，不重建标签或新fit。同时生成一份全run row hash及424个日row hash，存入同一run摘要。API每次只验证所选131行对应该日hash和run摘要身份，不反复扫描全55,544行；完整hash验证在导入事务与正式readback完成。

CHECK至少覆盖：日期as_of<trade_date；代码属于run catalog且L2（跨表成员校验在writer/readback，DDL固定level及非空代码）；available↔有限[0,1]概率、warning阈值一致且reason=null；unavailable↔概率/warning=null且原reason非空。AVAILABLE outcome须event为0/1且有限合法路径数值、event与-8%精确一致；OUTCOME_LEGAL_NA/OUTCOME_NOT_MATURE均event/drawdown/return=null，不能填event=0。

### 4.2 写入语义

CLI `import_risk_l2_predictions.py --request <explicit> --mode validate|write --database-target <explicit>`。validate不访问DB、不做fit；write只接入既有明确数据库target，不打印凭据，生产不得从DEV或环境缺失默认推导。脚本不执行DDL。

一个事务插入run及55,544行，参数化批量SQL；在同一事务内按日期/代码读回每一业务字段并比较canonical hash、完整日期/目录和行数，通过后才commit。任何冲突或读回不一致rollback全部本次写入。不得commit之后才发现错误、漏掉事务失败或按日期留下半run。

幂等同run：已存在完整同hash资产则无新行写入，仍完整读回验证；同key不同payload或残缺run均typed conflict，不能ON CONFLICT吞错。导入不修改其他run/历史记录，不调用runtime activation；不为本包增加repair机制或新写队列。

## 5. Contracts D3：状态、真实评价与surface独立

保留原`effect_status=DEVELOPMENT_RISK_EFFECT_REACHED_FORWARD_UNCONFIRMED`。正式效果/coverage未达标的合法run可保存诚实研究结果，但不能自动晋升能力；不改变已批准0.05/0.25或增加显著性AND门。

`risk_l2_capability_status=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`只在原执行/证据/效果及本包完整身份验证成立时给出；否则NOT_AVAILABLE。它说明历史development效果合格，不声称实时或消费增益。`research_surface_status`独立为NOT_AVAILABLE或AVAILABLE_EXPERIMENTAL，仅在真实writer/readback、API、无mock浏览器全部完成并有匹配外部验证记录时晋升，不由DB自报。

本版本固定`forward_power_status=UNAVAILABLE`、`forward_confirmation=NOT_STARTED`、`advisory_status=NOT_AVAILABLE`。没做功效核算不能写PENDING_INSUFFICIENT_POWER；没有forward结果不能写PASSED。不改变原acceptance的NOT_AVAILABLE surface字段。

沿现有产品receipt模式，仅一份紧凑`hmm_risk_risk_l2_product_validation_v1`，绑定run、完整stored row hash及实际验证的日row hash、model/acceptance/input identity、部署源码commit、target、writer_readback/api_readback/browser_no_mock结果。reader计算matched surface，不回写55,544预测行以循环证明验证成功。receipt缺失显示surface NOT_AVAILABLE；存在却损坏/身份不匹配时报typed readback错误，不静默当不存在或借用L1 receipt。

日详情的全部预测（包括未成熟/无标签）只依据原封闭概率，outcome明确标为事后评价并与预测区隔。摘要的precision/recall只在合法M算，不混用55,544目录分母；同时显示catalog、P/S、M及缺失/成熟计数。读端核对存储内容、状态与固定摘要一致，不能信任自报成功。

## 6. Contracts D4：两个read API、全量消费与≤30展示

新增HMM-owned端点，保持旧L1/rotation端点不变：

- `GET /api/v1/hmm-risk/risk-l2/overview?run_id=<64hex>`：返回显式run/model/version/hash、历史起止及424个日期、最后stored trade/as-of、131目录与覆盖计数、当日全部报警/未知数、完整compact_summary、四类状态。
- `GET /api/v1/hmm-risk/risk-l2?run_id=<64hex>&trade_date=YYYY-MM-DD`：严格返回这一run这一天的完整131行及日摘要/model/input等相同identity；不把UI展示上限作为API限制，不回退邻日或其他model。

run_id、日期必须显式。overview末日只表示同run的最后历史记录，不表示今天或latest模型。无run/日期HTTP404，身份冲突409，损坏/DB读取错误500，参数错误422；错误统一detail.reason_code/message/context，不返回HTTP200+空array假成功。参数化只读SQL，无API写入/训练/重训/激活端点。

页面在现有`/hmm-risk`新增“L2风险研究”入口，使用`view=risk-l2&risk_run_id=<hash>`，未给run时要求选择明确run，不默认为latest。复用现有HMM页面和tokens，新增`RiskL2Panel`；L1历史risk只留在L1历史入口，L2主页面不把L1风险卡当L2能力。

从该run overview拿冻结日期，用户可选历史日期；用完整131行才生成显示排名。默认概率最高10+最低10，自定义前后数量均非负整数、合计1..30；高低集合去重、tie按代码只作确定显示，不改变原warning。原availability为unavailable的行在目录计数/原因清单可见，不挤入低风险榜或标为安全。空可用人口显示具体原因，不伪造卡片；无warning但有合法概率是合法结果。

页面显著显示：历史trade/as-of、L2/code-only名称身份、research-only/未前瞻确认、绝对10D/-8%事件定义、固定0.20报警阈值、模型/run/version/输入身份、总体precision/base/lift/recall及原HAC区间/不可计算原因、误报与漏报代价。报警以原boolean显示，不用颜色/fading/排名推导。摘要必须显示`全部报警N；当前显示报警n；隐藏报警N-n`，159个>30报警日仍全量消费。

概率是冻结分类模型输出，不声称独立概率校准成功。低概率不是无风险、低位排名不是推荐买入；没有禁买、调仓、下单按钮，不连接Selection/Paper/荐股/QE。切换日期发生请求竞态时以当前run+date请求identity拒绝旧响应覆盖；API失败不能保留旧日卡片伪装新日成功。

## 7. Contracts D5：直接验证、零新增fit和合入标准

只使用当前封存概率做产品验证，不重建labels/features、不调用模型fit/predict、不读取2026-04-01及之后tail。导入validate/直接测试用DB connection和fit poison；授权write阶段只允许目标HMM表，不查询market。合成fixture是反例单测，不计真实产品成功。

最小直接矩阵：

| 验证点 | 必须包含 |
|---|---|
| identity/causal | canonical pins、self-rehash漂移、参数/input/contract错配、sealed与result概率差异、日期前置、未知/重复code、缺行、行重排canonical一致 |
| availability/outcomes | 停牌/预热NA保留；null概率不能no-warning；0/1阈值边界；未成熟/合法NA不算负样本；outcome改变不改变sealed预测 |
| writer/readback | 全run原子提交；同请求幂等；conflict/读回字段漂移在commit前rollback；部分日期/少于131拒绝；其他run/L1/rotation零修改 |
| API/UI | explicit run/date、不回退；131分母；0/131报警、全不可用、159日>30报警；最多30+隐藏报警；过期响应拒绝；错误状态与研究身份可见 |
| 真实验收 | 既有DEV授权后完整导入55,544行及三种日期（最后未成熟日2026-03-31；首日2024-07-01；实际零报警日由封存结果取）readback/API/no-mock页面 |

本地只跑新risk产品、API、CLI及直接PIT/schema/isolation合同的最小矩阵；失败先重跑nodeid，稳定后最多一次关联矩阵，剩余全HMM交CI/Nightly。Ruff、py_compile、frontend相关typecheck、diff/ownership/module最小门禁、F2；不修改nox/test plan/CI以迁就代码。当前封存结果的真实零报警日为2024-08-01，正常显示概率并明确“0报警”，不是空数据失败；2026-03-31保留全部131概率及未成熟标签。

代码最多三轮作者审修，有阻断就修；零阻断可提前结束；三轮仍阻断停并如实交接。三轮不是通过标准，不声称独立第三方审查。源PR全绿及文档合同批准后才待合入，不能以test pass替代真实DB/API/UI完成。

## 8. Contracts D6：Rollout / Rollback与授权停止点

精确提案和设计PR获批准并进入main后，单一实现PR原子交付reader/repository/CLI/migration/API/UI与直接测试。先复用已有模型成果；不重新训练以绑定新代码HEAD。

DEV使用**现存**`aistock_dev`，迁移/完整导入/读回按用户具体授权执行，不新建测试DB，不要求backup/export。生产目标及migration/55,544行导入另行明确授权，先完成DEV证据。source merge、DEV、生产DDL、DML、配置激活、用户重启、readback/no-mock验证和cleanup分别记录。

运行影响按实际changed files分类，本实现为`runtime_impact=backend`、`target_ids=[backend-main]`、`catalog_error=null`，并具有frontend与database源码影响；fresh-process import覆盖`backend.routers.hmm_risk`、`backend.routers.health`及新依赖。文档PR当时runtime=none只描述文档，不给本源码降级。后端启停仍归用户；没有target授权不启动开发/生产服务。

回滚先停止引用新run的配置/导航（仅在授权范围内），保留不可变研究数据；迁移rollback在非空时拒绝删除。不要为回滚删除历史模型/预测或恢复旧失败状态，不碰其他业务数据。cleanup须精确授权本任务branch/worktree，忽略路径和旧validation资产不据此删除。

**原产品长任务停止条件**：完整真实产品验证成功或授权/12小时/三轮阻断边界。该任务已于2026-10-06达到真实验证成功终态，不再次启动。下一价值消费的D1～D6由独立直接设计和精确批准确定；本产品授权不扩展为消费策略或新生产动作。

## 9. Design Acceptance Index

- **F-011**：保留risk已批准模型与历史效果终态，不重训、不开第二候选、不得宣称收益增益。
- **F-012**：显式封存input/model/概率身份、完整131目录及合法NA/因果边界，零数据集修改。
- **F-013**：一包完整历史风险持久化/API/UI，完整消费、≤30显示和真实surface验证；源码/DB/运行状态独立。

## 10. Implementation Plan与allowed_write_scope

实现范围一次登记：

- `backend/services/hmm_risk/risk_l2_prediction.py`（新业务验证/持久化）；复用`risk_l2.py`的既有CONTRACT与纯函数，不改模型算法。
- `scripts/hmm_risk/import_risk_l2_predictions.py`（新显式导入CLI）。
- `backend/db/migrations/create_hmm_risk_risk_l2_prediction_20261005.sql`及同名`.rollback.sql`。
- `backend/routers/hmm_risk.py`（只加L2 risk read接口）。
- `frontend/src/lib/hmm-risk/api.ts`、`frontend/src/components/hmm-risk/RiskL2Panel.tsx`、`frontend/src/app/hmm-risk/page.tsx`；`frontend/src/components/hmm-risk/RotationL2Dashboard.tsx`仅移除误置在L2主面的L1历史risk卡（L1历史入口保留）。
- `backend/tests/hmm_risk/test_risk_l2_prediction.py`、`test_risk_l2_api.py`、`test_import_risk_l2_predictions.py`与`frontend/tests/hmm-risk/risk-l2.spec.ts`；相关隔离/schema/security测试只在直接合同受影响时扩展。
- `frontend/tests/hmm-risk/risk-l1.spec.ts`仅将两处旧风险测试入口明确为`?level=l1`，直接对应L2面移除L1卡；不改变L1模型/API/效果或历史能力。
- 本设计与父蓝图只同步实际状态/链接，不改其他设计精确模型合同、工作流/规范或其他模块。

实施由同一HMM owner顺序完成：正式资产只读校验→同一PR完整代码/直接测试及最多三轮审修→最小门禁/CI→获授权合入→已授权目标DEV/生产写入与用户重启→真实API/UI验收。上述是一个包内部依赖，不是新的微阶段/不同方向。

## 11. Verification Plan与Design Acceptance Matrix

下表按真实已执行证据更新：F-011原模型效果保留，F-012正式file-only及完整DEV/生产事务读回完成，F-013实际API/无mock浏览器/receipt和用户重启后验证完成。`verified`仅覆盖本历史研究产品，不代表消费净增益或forward；本地合成fixture只验证反例，不代替真实产品验收。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | §1/§3/§5及原risk模型；backend/services/hmm_risk/risk_l2_prediction.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/run/acceptance.json；原效果指标精确重核，模型及封存概率不变 | verified | 无 |
| F-012 | §3/§4/§7；backend/services/hmm_risk/risk_l2_prediction.py；scripts/hmm_risk/import_risk_l2_predictions.py | artifact: F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/product/dev_database_validation_20261006.json；同目录production_database_validation_20261006.json；完整row hash=`cd31fa9b2dbe2d72f2b4d17438113f35b2db994d5cc3d5698ac8af26636c9f95` | verified | 无 |
| F-013 | §4～§8；backend/routers/hmm_risk.py；frontend/src/components/hmm-risk/RiskL2Panel.tsx；本包两份migration | artifact: F:/Dev/AIstock_runtime/hmm_l2_risk/20261005/product/product_validation.json；2024-07-01/2024-08-01/2026-03-31真实API/no-mock、identity及最多30展示验证 | verified | 无 |

文档门禁：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/hmm_evolution_phase2_risk_l2_product_detailed_design_20261005.md --tier F2`；UTF-8/diff、范围与前后语义复审。设计F2只验证结构/引用，不可推导模型批准、源码或产品验收。

## 12. Risks / Failure modes

- development已参与研发选择，不能升级为独立forward证据；需要更强独立验证时另批方案，不读既有tail。
- 多数报警不发生目标事件，错过上涨是实际消费风险。UI必须显示原精确率和代价，不用warning直接控制业务；本包不模拟或宣传可实现风险下降。
- 单成员、小行业和同日期相关性保持原来源/样本定义；不得新设最小成员数门。过期历史不能标“今日预警”。
- 概率/标签/口径混合、误报比例与FPR混淆、显示30变成报警30、L1版本冒充L2均为显式反例。
- 现有代码/运行可能与文档变化，执行前以own worktree、正式资产hash、最终HEAD与runtime evidence核验，不使用旧启动缓存。

## 13. Production gates及批准索引

已完成动作分别记录：现存DEV `aistock_dev:5433`先完成迁移、55,544行事务、幂等和负例rollback；获具体授权后在生产`aistock:5432`应用本包迁移并写入/完整读回同一55,544行，同请求重放新增0行。实际表仅`hmm_risk.risk_l2_run`、`hmm_risk.risk_l2_prediction`，非market/QE表。迁移文件byte SHA=`51fe080d7aadfa8a96b202b4be7cdf007f21a3f42ce16c69356386ac5909812e`；正式产品receipt canonical SHA=`6c55e644c59ab8cfd53f920382d43179aae8c15da05a0aa927f5b8b471c59e2b`。

用户完成backend-main重启后，以实际runtime SHA `c3e4b3ace5c269e00a6c5747af1a23923679c357`核验3日API/无mock页面及配置receipt，完成于2026-10-05 17:25:11 UTC（北京时间2026-10-06）。main随后前进不据此冒称进程加载最新HEAD；直接HMM链已核对包含本实现。正式资产保留，源任务树及其local/remote branch、精确validation树已按授权清理。当前无再次重启需求，进程控制仍不授权给agent。

本次v1.3仅状态文档：production_ddl_gate=noop、production_dml_gate=noop、dependency_gates=noop、runtime_impact=none；database_write/dataset_write/active_profile_write/training/new_fit/tail/QE/runtime_action/process_control=false。未来新代码与实际部署按真实changed files/目标重新分类，不继承文档none或重复历史生产授权。

| 决策 | 当前状态 | 推荐 |
|---|---|---|
| L2-RISK-PRODUCT-D1～D6 | APPROVED_BY_USER_FOR_IMPLEMENTATION | 已批准完整包；原模型/输入/窗口/0.20/0.05/0.25全部不变 |
| PR #5444 | 已合入，merge=2fa41eab6 | 不因此重跑2fit或清理正式资产引用树 |
| 本文设计PR #5447 / 源PR #5449 | 均已合入，merge=e58cea30b / d0a074ddc | 完整研究产品已验证，不重复实施/导入 |
| DEV/生产/激活/用户重启/精确cleanup | 已分别授权并完成 | 只覆盖原目标和资产，新的消费策略/生产动作仍需对应授权 |

## 14. DESIGN-COMPLIANCE-001与文档审核

| 检查 | 本设计约束 |
|---|---|
| 禁止简化交付 | 一包完整持久化/API/UI及真实验收；131计算不减为30，历史研究范围显式，不代报实时或增益 |
| 禁止静默错误 | 固定identity、原始NA、未知/非有限/缺行fail closed；不默认概率/no-warning/邻日/其他run |
| 禁止业务逻辑迁移 | 原模型/标签/阈值/窗口及旧L1/QE资产不变，warning不接交易，其他模块零修改 |
| 禁止私增门禁审批 | 不增显著性、逐行业效果或三态AND门；本文业务合同已由用户明确批准，不靠F2 PASS制造批准 |

已完成三轮作者自审/修订（不是独立第三方审核）：

1. 逐字段对照真实acceptance/sealed/features schema及现有HMM repository/router/UI，修复父蓝图旧状态，明确本包只用历史封闭概率、名字code-only、完整131目录和原模型效果不变。
2. 修复input/mapping/contract hash未精确定义、summary自报与每日API可能重扫全run的问题；补齐封存预测对照、原指标重算、日hash与原子commit前读回、未成熟/零报警/显示限额。
3. 核对严格授权、sourcePR依赖、全量/展示分母、历史保留、callback/订单禁接与状态耦合；真实零fit读回验证55,544封闭概率逐字段不变，修正父蓝图preflight资产引用，未发现剩余文档阻断。

上述三轮是原设计审修，保留当时事实。2026-10-05批准并合入后，新增三轮源码作者自审（非独立第三方）：第一轮闭合原receipt算法、字段投影、重排/指标重核及文件稳定性；第二轮补齐真实health进程identity、首日/零报警/末日surface receipt、旧响应隔离与L1历史入口；第三轮修复writer显式目标/实际dbname校验、损坏或断链receipt不能当缺失，并补齐UI region语义。模型、原始概率/标签和D1～D6公式/阈值未改。截至该轮源码审修时UI只完成list，真实DEV/生产/API/browser/runtime未运行；之后这些动作已完成，实际状态按§11/§13新证据记录，不回写当时事实。

v1.3两轮状态文档复审：第一轮逐项对照#5449、完整55,544行hash与DEV/生产/用户重启后receipt，修正当前实现、Matrix及生产状态；第二轮确认历史授权/审修文字仍为当时事实、实际进程身份不是随后main HEAD、已完成清理不删除正式资产，risk surface不推导经济/forward或rotation完成。原D1～D6模型与产品公式不变，未发现状态文档阻断。
