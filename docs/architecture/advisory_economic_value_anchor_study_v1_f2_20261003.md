# Advisory H-VALUE-ANCHOR-1 一次D-only价值研究切片（F2）

> 日期2026-10-03；v0.1；源码交付范围为一次固定离线研究，非每日业务/正式模型启用。父[F2设计](advisory_economic_value_anchor_v1_f2_design_20261003.md)与[内核F1](advisory_economic_value_anchor_kernel_v1_f1_20261003.md)已分别经#5344/#5346合入。
> 最新接续：源码PR #5347已通过CI37115926268并合入3e20e922d5922838cccc9de99cfa0f4e3df9318b；run `advvalue_a4bc66a30cfa7d9d5078850c`已完成两头及三臂，结果为STOP_CURRENT_CANDIDATE_NOT_GLOBAL_DIRECTION；经济确认/启用仍0，详见末节。原源码矩阵不代替经济证据。

## Background / 当前事实

H-TIMING-1停止，无新合格模型。本切片只测试独立价格无关场景的D-only预测价值，不挽救旧权重/改风险阈值。旧386D窗口已经消费，不是独立OOS。设计/内核正确不代表新模型有经济增量。

## Scope / 精确实现

新增`backend/services/advisory_model_first/economic_value_anchor_training_v1.py`、`economic_value_anchor_pipeline_v1.py`、`economic_value_anchor_evaluation_v1.py`及`backend/tests/advisory_model_first/`对应三个同名叶测试和本文。前置三叶内核只消费不改。本次不包含每日新family/API/UI/资格或activation。

## Non-goals / 授权与边界

不修改QE、Selection、HMM、数据准备、Paper/Execution或公共验证。不提交QE任务、不读数据库、不写库/激活数据或binding、不控制服务/进程、不安装依赖。fit前查公开QE活动状态，非idle仅暂停fit；历史只读准备可继续。临时X、持久F，不用C。不得读取sealed/新holdout或修改旧产物。

## Architecture / 固定流程

新plan预登记 → 父源只读授权/精确manifest检查 → 新labels/完整rows原子prepared → 两头fit-started永久标记 → 一次trained → 真实T开盘三臂 → evaluated及导航结论。直接复用现有publish_stage/registry/simulator，不添加平台或新审批。

## Contracts / 统计、身份、时钟

Plan仅接受三个精确角色文件引用、源码算法身份及NAV/nondeploy固定字段，绑定原父切分/数据、独立场景/成本和固定参数。源码算法UTF8_LF_BYTES_V1绑定新六叶、core/共享episode/shadow/费用/转换引擎及直接统计依赖；仅CRLF规范，artifact字节仍精确。父读取授权属于既存已消费source，不冒充新policy激活；registry按新场景policy identity另记，源原native限制原样保留。

prepare直接消费已交付输入，忽略timing两个字段和旧103字段；标签/12D/观察KEY必须完全相等。先检查价格日期列属于原已消费source clock/budget再读取OHLC；D模型只投影12D及D可见边界，无query、T行情/未来成熟状态。只为新labels重新计算场景，不重跑Selection或读取旧研究收益来选参数。source/roster/coordinate/policy矛盾fail closed。

train/validation按label_information_end purge，两头同一eligible交集；AVAILABLE标签必须有限正值。support只用train的完整D及真实观察，不以标签成熟/盈利筛选，且T≤train_end，否则该观察gap不进入support。D的min/max只作诊断，**不新增D边界弃权门槛**。常数锚只用同train监督，validation仅诊断，不调阈值/校准/early stop；test不训练/选点。

一配置两头、200轮、seed20261002、2线程、LightGBM4.6.0、mean L2/q10 path。registry planned/generated/evaluated按1个模型配置记，参数/fit-started/metadata明确2个物理头与常数规则对照；单位测试中的synthetic调用不是研究trial。中断未发布trained时保留标记拒绝隐式再fit；exact retry仅核验已发布stage。模型加载校验新family/order/parameters/runtime/registry，不把旧权重带入。

开盘决策只投影raw_open/法规模型界/已知停牌/source：市场准入不明三臂都不进入，开盘涨停不等盘后HLC才决定能买。两锚共同support，未知D或support仅属研究基线控制，贡献单列；非法预测报错而非控制。原Top5被过滤不补第6名，原rank退出不改。后续静态D价格网格由已交付F1纯内核支持，不在本切片宣称每日接入。

三个实际组合使用同场景、同5槽/费用/真实日历和原Top40；全持有日mark及所有episode端点须通过只读核验。盘后日线才能证明的端点或未知mark使结果`BLOCKED_EXECUTION_OR_MARK_UNPROVEN`，不产出比较指标；原记录/产物保留，不修改bar救结果。终端padding仅对证明已空仓的尾部cash日，活跃持仓/缺中间日不可补0。报mean/pinball诊断、成对经济增量/MDD、真实干预及UNKNOWN贡献；旧19.17%不作新场景基线。

增量区间block5/reps2000/seed20261002；MDE为事后开发方差代理，不是确认功效或已达标。两个正增量且非零实际干预只能导航建议另立确认合同；其它停止当前candidate、不改变风险/种子/期限/支持域、不补证。三级证据与regime UNPROVEN保持分离。

## Implementation Plan / 当前切片与后续

三叶实现、多轮自审、直接测试/Ruff/diff及F2结构验收；从最终审核源码身份生成新plan，再prepare并核对覆盖/状态。QE公开资源idle时最多两头一次fit及三臂导航，结果持久F。完成后再按授权提交/CI/合入及自身精确清理、更新蓝图真实进度。没有值得继续的模型不接每日新family，不重跑旧负结果。

## Verification Plan / 最小合同覆盖

训练测试保护双头D-only、label-end purge/test毒化不改变fit/支持、非法时钟/值/人口；编排测试保护实现身份、fit中断不可隐式重试、QE gate及C/relative禁用；观察测试保护不读未来OHLC/label、市场未知不控制买入、持有未知标记及source/reference/法规矛盾拒绝。真实准备与运行用于验证源合同和三臂链路，其状态/覆盖/经济结果另报，不用synthetic测试冒充实证。

## Design Acceptance Index

| ID | 要求 |
|---|---|
| F-651 | 新独立scene身份、父授权与精确输入引用，不修改旧产物 |
| F-652 | D-only双头同eligible/purge、train-only支持及真实预算 |
| F-653 | 原子stage/registry与fit-started拒绝隐式重fit |
| F-654 | 真实观察共同准入、未知控制分列、原Top5不补位 |
| F-655 | 同场景shadow/端点与mark、干预/功效及导航隔离 |

## Design Acceptance Matrix

下表验收源码合同，不声称已完成真实模型确认或日常推荐。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-651 | backend/services/advisory_model_first/economic_value_anchor_pipeline_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_pipeline_v1.py | VERIFIED | none |
| F-652 | backend/services/advisory_model_first/economic_value_anchor_training_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_training_v1.py | VERIFIED | none |
| F-653 | backend/services/advisory_model_first/economic_value_anchor_pipeline_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_pipeline_v1.py | VERIFIED | none |
| F-654 | backend/services/advisory_model_first/economic_value_anchor_evaluation_v1.py | pytest backend/tests/advisory_model_first/test_economic_value_anchor_evaluation_v1.py | VERIFIED | none |
| F-655 | backend/services/advisory_model_first/economic_value_anchor_evaluation_v1.py；本文 | pytest backend/tests/advisory_model_first/test_economic_value_anchor_evaluation_v1.py | VERIFIED | none |

## Risks / 多轮审核修复

业务轮：维持共同市场准入而非支持未知就实际买入；基线虽无价值过滤仍遵守同开盘可见合法性，label/portfolio全沿新场景。PIT轮：补T≤train_end支持截止，原名单/price-clock在拟合前核验，不读sealed；修正开发草稿的D bounds门槛，min/max只诊断而非设计外弃权。身份/失败轮：源码hash包含共享转换引擎/费用，fit marker中断拒绝隐式再fit；端点/全持有日未知均使经济比较阻断，不由模拟器carry未知凑收益。三轮为本窗口不同视角自审，非外部独立审核。

12D仍可能没有收益信号，估值单调性不能证明坏消息下低价安全。价格支持不能识别任意限价干预/分钟成交。任何导航均不得触发绑定、正式退出替换或资金分配。

## Rollout / Rollback / Production Gates

纯离线显式入口，无router/scheduler默认路由变化，源码合入可离线执行但不证明后端已加载。旧生产功能不变，只有值得继续的模型才另设计consumer scope及confirmation。runtime/profile/binding/DDL/DML/依赖/进程全noop，后端重启用户负责。DESIGN-COMPLIANCE-001四项为范围内真实源码、未知不伪成功、固定合同不事后放宽、无新平台/日期/旧证据门禁。

## 本次真实研究结果

386D/7720原候选完整保留，7699新标签可用/21未知，4250/1622共同train/validation；test监督1521仅为purge后的计数，实际模型评价仍保留全部81D/1620候选。prepare15.163秒、fit0.880秒（含源核验）、fit＋评价12.169秒，一配置两头一次；前后QE7项活动状态0，端点和持有mark核验三臂均无问题。

新场景同100日baseline/constant/model名义收益21.3220%/25.4597%/17.4642%；模型相对常数/基线日增量-6.9022/-3.4196bps，描述性区间均跨零。模型81 TAKE＋3 UNKNOWN控制episode、相对常数进入不同58日，非恒等输出，但增量未通过。固定停止当前candidate，不事后回选常数规则或追加模型/阈值/窗口，不建daily family，不激活。完整数值与原生身份限制见蓝图§1.3；此段不修改事前合同。
