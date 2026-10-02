# Advisory 经济价格模型日频增量信息包 F1 设计 v1.0

> 日期：2026-10-03；状态：LOCAL_IMPLEMENTATION_VERIFIED_CI_PENDING；交付类型：F1 单模块纯计算能力。父方向为荐股蓝图的“先扩信息集、后换模型”，不是新模型收益确认。

## 1. Background / 目标

当前aligned v3固定九字段，真实研究日增量-8.888bps，不确认、不启用。继续变换旧阈值、风险预算或label不属于本任务。后续唯一新增信息假设使用D可见量价信息，先完成独立、有界、可验证计算能力，再单独设计十三字段训练与消费者分派。

本F1交付完整四字段计算、输入边界、未知处理及内容身份，不交付模型训练、bundle/API/UI或正式激活；不得将其称为“经济买价模型已完成”。它是十小时任务中可独立验收的上游输入切片，不替代尚待浏览器验收的每日消费者F2。

## 2. Scope / 精确范围

只新增三个文件：

- `backend/services/advisory_model_first/economic_daily_information_v1.py`
- `backend/tests/advisory_model_first/test_economic_daily_information_v1.py`
- 本设计。

旧九字段contracts/训练/推理、每日consumer、QE、Selection、Paper、Execution、公共验证和数据准备代码均不修改。函数不读文件、网络、数据库或时钟，不安装依赖、不写工件，不触发训练、选股、仓位或订单；不影响既有线上调用链。功能供下一版本显式接入，不自动注入现有模型。

## 3. Architecture / 输入输出

单一纯函数`build_economic_daily_information_v1`接受D、保持原顺序的候选roster、20个有序唯一交易日（末日为D）、已统一到D复权坐标的候选日频panel，以及明确D截止的沪深300五日收益标量。panel索引为datetime/instrument，可缺正常源行/字段；输入不得包含窗口外日期、盘中时间、未来行、其它股票或重复键。

限Top20（含合法空名单）、rank为严格整数1..20且唯一有序、instrument为合法SH/SZ/BJ代码；20日panel最多400行，不创建股票×日期笛卡尔积。日期索引可规范为无时区日历日期，原timestamp/date/ISO标签归一后计算与身份相同，不保留字符串键导致查询漏失。交易日历由后续权威source提供，本函数仅验证其内容边界，不凭日历hash伪造原生来源资格。

输出固定四字段，每个原候选恰有一条记录，缺失为null及逐字段原因；不删候选、不重排序、不补0、不向未来或跨股票补值。正常停牌需由已有suspension-aware来源提供合法归一panel；本函数不自己制造停牌价格/成交量。已确认停牌的0成交量可参与均值，原始缺失不能当0。

输出receipt绑定D、候选顺序/hash、日历hash、实际消费的四列panel及benchmark内容hash、语义hash及整包hash，声明COMPUTATION_ONLY/NAVIGATION_ONLY/deployable=false/outcomes_read=false。hash证明内容，不证明原生capture、PIT修订历史、模型资格或经济效果；不能由调用者声明native=true升级。

## 4. Contracts / 冻结计算口径

| 字段 | 定义与有效条件 | 正常未知 |
|---|---|---|
| ret_10 | C(D)/C(D-10)-1；最近11交易日close全部已知、有限、正值 | warmup/窗口内close缺失 |
| relative_ret_5_vs_csi300 | C(D)/C(D-5)-1减D可见沪深300同口径五日收益；最近6日close已知 | benchmark缺失或close缺失 |
| close_location_in_day | (C(D)-L(D))/(H(D)-L(D))，D高低收齐全且0<L≤C≤H | D缺失/高低相等/字段缺失 |
| volume_ratio_5_to_20 | 最近5交易日原始同单位volume均值/20日均值；20日全部已知、非负，分母>0 | volume缺失/20日均量0 |

收益为小数而非bps；后两字段为无量纲。relative_ret_5是既有信息派生，不声称第四个独立alpha。价格已统一D坐标、成交量为同一原始单位是显式输入口径，身份绑定该声明，不把声明当数据库真实来源证明。新训练必须另证原生source、时间语义和价格坐标。

非有限数、bool、非数值字符串、负成交量、非正已知价格、矛盾OHLC、benchmark收益≤-1、异常日历/候选键为fail closed；normal null（含pandas nullable缺值）不全局失败。字段不存在只使依赖字段UNKNOWN，其余可计算字段保留。没有D行则四字段UNKNOWN，不借D-1报价假装D。

序列计算按显式交易日索引，不按现有记录数压缩停牌缺口。原panel顺序不影响规范内容hash，原候选顺序必须保持。即使某个源值不改变当前输出，也要被source hash绑定，不能只hash四个结果冒充输入一致；实际字段存在性也绑定，因此缺volume列与存在但全null的volume列不能混为同一source。

## 5. Verification Plan / 验证和合入要求

复用一处最小20日fixture覆盖明确手算值、未提供行情候选、正常停牌/缺字段/稀疏窗口/flat/zero-volume、非法候选/未来或重复来源/非法数值、内容hash漂移和不修改调用者输入。禁止实现快照/重复大fixture，测试仅该叶文件。

设计至少两轮审核修订；源码两轮语义/资源/副作用审查及直接节点复验，稳定后一轮小矩阵。Ruff、diff、F1结构validator及完整下列矩阵通过才能提交PR；必需CI通过后可沿用用户合入授权。未实际调用真实历史来源时，不声称DB/PIT真实业务验证通过；当前纯模块不需要重启或DDL。

## 6. Design Acceptance Index

| ID | 验收 |
|---|---|
| F-582 | 四字段固定公式/单位、完整计算成功路径 |
| F-583 | 严格D、20日窗口、候选唯一性/原顺序与资源上限 |
| F-584 | 正常缺失/停牌不删候选或制造值；未知与错误分开 |
| F-585 | 内容身份、语义身份、输出hash及输入不变 |
| F-586 | 零外部副作用、旧模型合同不变、不得升级证据 |

## 7. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-582 | backend/services/advisory_model_first/economic_daily_information_v1.py | test: backend/tests/advisory_model_first/test_economic_daily_information_v1.py::test_hand_calculated_values_identity_and_missing_candidate | VERIFIED | none |
| F-583 | backend/services/advisory_model_first/economic_daily_information_v1.py | test: backend/tests/advisory_model_first/test_economic_daily_information_v1.py::test_foreign_time_identity_and_malformed_values_fail_closed | VERIFIED | none |
| F-584 | backend/services/advisory_model_first/economic_daily_information_v1.py | test: backend/tests/advisory_model_first/test_economic_daily_information_v1.py::test_normal_missing_and_suspension_never_drop_or_manufacture | VERIFIED | none |
| F-585 | backend/services/advisory_model_first/economic_daily_information_v1.py | test: backend/tests/advisory_model_first/test_economic_daily_information_v1.py::test_source_digest_binds_unused_values_and_is_row_order_invariant | VERIFIED | none |
| F-586 | backend/services/advisory_model_first/economic_daily_information_v1.py | test: backend/tests/advisory_model_first/test_economic_daily_information_v1.py::test_hand_calculated_values_identity_and_missing_candidate; artifact: docs/architecture/advisory_economic_daily_information_v1_f1_design_20261003.md | VERIFIED | none |

## 8. Review / 后续边界

设计审核1：新增信息与已知信息派生分开；只提供计算能力，不把四字段注入固定九字段旧权重；单位和20日位置明确。修订了“缺失列全局失败”为依赖字段UNKNOWN，保留正常缺失候选。

设计审核2：缺行不能压缩交易日或用0量补齐；来源内容与输出内容hash分开，禁止仅凭receipt宣称native。修订为显式20日窗口、每字段最少支持、400行预算、未知与非有限错误区分。Design compliance四项：不宣称全模型/UI完成；错误不静默；既有业务/风险/研究合同不变；不增加平台审批或等待未来行情。

源码审核第1轮：明确手算成功值与真实停牌归一输入的volume口径；修正测试的最后5日索引，而不改正确公式。源码审核第2轮：4项先复现失败，再修复nullable正常缺值、非法日期的统一fail-closed错误、不可实现的benchmark收益和缺列/all-null源身份碰撞；仍不补数据或删除候选。

源码审核第3轮：33项叶测试通过、Ruff通过后，再发现Timestamp/ISO混合标签能绕过原始index唯一性并在规范日期查询中覆盖旧值；先新增一个alias节点复现，再按规范后的日期+股票键拒绝重复，保留调用者原panel。最终直接复验覆盖alias、三种手算标签及source hash，CI与合入状态另行报告。

十三字段model/scope、统一训练/每日source、收益功效/支持度和独立确认的详细设计是下一切片；现阶段新增模型trial=0、角色启用=0。历史可产性可用已消费D-only输入验证，不读取收益/封存窗口，不重复QE上游研究。

## 9. Implementation Plan / 实施顺序

设计两轮审核→纯函数与一处fixture→失败节点修复→第二轮边界/内容身份审查→一次稳定叶矩阵→设计结构/静态/范围门禁→独立小PR及CI→官方合入善后。不借本切片扩展现有每日消费者PR。

## 10. Risks / 风险

量价特征只是新信息候选，可能没有经济增量；复权声明和日历内容不是原生PIT证明。0成交量只来自已归一的真实停牌输入，不能当缺失填值。后续模型必须保持候选/eligible和政策对照、清楚区分开发导航和独立确认，不能因计算PASS激活负模型。

## 11. Production Gates / 生产门禁

production_ddl_gate=noop，production_backend_dependency_gate=noop，production_frontend_dependency_gate=noop。纯计算模块不接线上入口、不改数据库/资料指针、无需后端重启；不会修改旧binding、模型权重或生成角色启用记录。后续若实际运行接入需要用户重启，仍由用户执行，不能从本源码合入推导运行态完成。
