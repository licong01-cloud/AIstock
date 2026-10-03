# Advisory M1日频15D纯组合 F1 Feature Card

2026-10-04；完整范围仅单D纯计算及调用方逐D复用，不是数据源/每日API或qualified模型。上层详细设计PR #5422先完成合入，本源码PR不越过它独立宣称daily完成。

## Background

M1首次正NAV，core12D与sector3D分别已实现，但每日旧入口9D且fitted.request不适用。sector需要21D close而core需要20D，必须显式21D+下一T的22节点组合、用原函数保留原候选与正常未知。计算准备无需训练、DB/profile或新holdout来源，不把纯计算当原native训练或收益证明。

## Scope / 精确登记

自己的worktree advisory-sector-daily-core-20261004 / feat/advisory-sector-daily-core-20261004；仅三新文件：本Card、`backend/services/advisory_model_first/economic_sector_daily_core_v1.py`及`backend/tests/advisory_model_first/test_economic_sector_daily_core_v1.py`。不改任何原core/sector/math/registry、QE/Selection/StrategyPackage/HMM/公共数据或现有API/UI。

## Contracts / 单一入口

`build_sector_daily_features_v1(core_inputs, calendar, classification_rows, sector_quotes, crosswalk, expected_crosswalk_values_sha256)`。

core_inputs仅7个既有核心参数，不含calendar；22个唯一递增date节点明确为21D+T，核心取最后20D+T，sector取21D。日历连续/紧邻T真实性调用方权威来源验证；计算器只核定显式序列，不以排序或hash证明交易日历已原生验证。query/labels/returns/outcomes不进入接口。

分类表必须与原candidate KEY集合精确一对一、只含KEY和classification_l2_code/known_from，不重新选股、缩名单或改group/rank。D后已知分类、重复/冲突/外部候选拒绝；正常缺分类/缺代码映射保留UNKNOWN。crosswalk只作已有结构翻译，不做当前成员回填；外部pin核对本次实际mapping值hash，不能称此值hash为官方snapshot文件hash或PIT证明。industry是六位string，id为非负strict int或None、0合法，已知id一对一、至多134项；结构真实性仍由上游合同负责。

sector quotes仅datetime/l2_code_id/sw2_close、最多420行，日期只21D内、已知id仅本D原候选需要的映射id。必须先拒绝未来/外部日期，再调用原sector_quotes_v1验证id/有限正报价/重复矛盾，不能让其过滤未来成为成功。原sector_dynamic_rows_v1算ret5/20return波动/relative ret5，完整21close要求保持，无缺日压缩或补0。

输出原顺序KEY+15D值，不含query坐标；receipt绑定核心子receipt、分类/quotes/mapping值及有序输入/特征hash、每候选UNKNOWN及sector状态。正常缺raw、部分停牌/未知行情、缺sector、warmup不删除候选；非法源格式/时间/矛盾fail closed。返回COMPUTATION_ONLY/old_training_parity UNPROVEN/native UNPROVEN/deployable=false/outcomes_read=false，不提供正式model scope、qualified role或生产绑定。

## Implementation Plan

先本Card审核/校验，再实现单叶函数，多轮方法/时钟/工程自审修复。复用既有core测试packet作为fixture，不复制整套市场数据或实现快照。

## Verification Plan

直接测试组合与原两函数逐字段parity、id0/缺分类、缺sector日不压缩、core正常缺失、22/21错位/未来/外部/重复拒绝及crosswalk pin。固定同包同输入重复调用同值/hash，证明逐D复用数学而非完整批量读取。

最终直接矩阵一次、Ruff/F1/diff通过后提交PR；#5422先合入、本源码必需CI绿后合入及官方自身清理。此纯scope没有训练或真实新市场输入，不能以合成fixture称业务/PIT/经济验收。真实原D输入/model查询由其它切片另验，不为旧负实验补证。

## Risks / 边界

最大风险是组合schema兼容被当作训练来源/真实daily资格；receipt明确unproven，价格第16query不在D值中。quotes外部行必须拒绝不能静默filter，分类未知不可删名单；字典pin只验证值不赋予官方/历史权威。派生不可用不是整业务停止，原Selection基线不变。自审是本窗口不同视角，不冒称独立外审。

## Design Acceptance Index

| ID | 必须验收 |
|---|---|
| F-734 | 22节点组合原20D/21D两纯函数、15D字段/原顺序/0query |
| F-735 | 原候选唯一与精确KEY、分类严格D及id0/外部值pin |
| F-736 | 有界quotes/未来毒化/矛盾拒绝、正常缺失停牌保留 |
| F-737 | 输入/语义/特征hash及COMPUTATION_ONLY/native UNPROVEN不可升级 |
| F-738 | 三文件范围/最小重复fixture测试、多轮修复及0I/O/训练/服务 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-734 | `backend/services/advisory_model_first/economic_sector_daily_core_v1.py` | `backend/tests/advisory_model_first/test_economic_sector_daily_core_v1.py`的原两函数逐字段parity/22节点/原序列 | IMPLEMENTED_PURE_CORE_VERIFIED | none |
| F-735 | 同叶_crosswalk/分类键与clock验证 | `backend/tests/advisory_model_first/test_economic_sector_daily_core_v1.py`：id0/原KEY/未来分类/重复与pin拒绝 | IMPLEMENTED_PURE_CORE_VERIFIED | none |
| F-736 | 同叶quote边界及原sector计算复用 | `backend/tests/advisory_model_first/test_economic_sector_daily_core_v1.py`：未来/外部/矛盾/超预算拒绝、缺日及正常raw未知保留 | IMPLEMENTED_PURE_CORE_VERIFIED | none |
| F-737 | 同叶RECIPE/组合receipt及source限制 | `backend/tests/advisory_model_first/test_economic_sector_daily_core_v1.py`：重复输入同值hash/COMPUTATION_ONLY/native UNPROVEN/0query | IMPLEMENTED_PURE_CORE_VERIFIED | none |
| F-738 | Scope/Verification/Production及同叶入口 | artifact: 三文件/15直接测试/Ruff/diff与零I/O；非业务或收益验收 | IMPLEMENTED_PURE_CORE_VERIFIED | none |

## 多轮实际审核及直接验证

方法轮核对原core12/sector3公式逐字段parity、原price query不入D值、21close/20return ddof=1；来源时钟轮核对未来quote先拒绝、原候选KEY/分类clock/crosswalk pin、id0与正常未知，原语义未改变；工程轮核对纯scope、零I/O/正式资格、bounded frames及fixture复用。均为本窗口不同视角自审，不冒称独立外审。

初轮15直接测试中14通过，duplicate分类本已正确拒绝，但复用的AdvisoryModelFirstError不是ValueError，修复测试接受这两种明确合同异常；只复验失败node通过，不改原错误处理或降低拒绝条件。稳定后最终15直接测试一次PASS（2.24秒）、Ruff/diff通过，临时全X。F1初次Implementation标题未被识别，改为明确Implementation Plan后五项PASS；实现矩阵修订后再次校验。测试只证明纯计算，没有加载真实新source/model/行情/收益，不称原生或每日业务通过。

## Rollout / Production Gates

无I/O/DB/API、profile/source加载或fit/登记；backend_restart_required=false，不挂调度/API/UI，不改变老M1数学hash或原artifact。源码就绪不等于每日业务、经济确认或角色启用；回退停止新调用，无数据动作。本PR自身DB=NOOP，不掩盖#5420已经披露的metadata GET upsert事件。

DESIGN-COMPLIANCE-001：完整单D纯组合范围，不冒充完整daily；正常UNKNOWN、PIT冲突、模型/来源/经济等级分开；原native限制不补造，不静默回退或为新数据走旧训练路径；来源适配和正式接入未实现保持未完成。
