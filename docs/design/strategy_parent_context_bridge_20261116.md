# 新研究多Alpha父包原执行上下文桥接（F1）

## 背景

仅恢复新研究 OLD5 / UPDATED3 父包正式导出所需的原执行上下文；不修补或覆盖旧父包38项冻结身份。16份真实模型、原拟合处理器及源码已经完成CAS闭合。本次不训练、不重新生成完整预测、不重跑账户；DEV整包冷消费最多16模型×3日期=48次真实预测。

原已发布组合的 `backtest_config_json` 缺少扁平上下文字段，但选定发布attempt的 SHA 校验配置、数据绑定和构建规格均存在。通过版本化原上下文证据封装解析原 `n_drop=1`、`max_n_drop=2`、PIT pool pins、h20，以及精确执行类/模块/参数；执行标识只能映射到现有且未退役的正式定义，不能臆造旧alias、缺省填充或绕过退役。原冻结来源记录及其哈希不修改。

## 范围与非目标

仅显式 original_context_bridge=true 的新父包启用。旧父包、默认晋升路径、退役检查、原权重及seed集合均保留；禁止生产登记、激活、服务操作及以新模型冒充旧资产恢复。原38份权重存在，原32份LSTM处理器闭合仍缺失，历史原因UNKNOWN；本次16份新研究模型形成不同版本，不能覆盖旧冻结身份。

## Design Acceptance Index

- F-001：仅采用原选定成功发布attempt的不可变上下文字节及SHA。
- F-002：提取原换仓/预算/PIT/期限，缺失或冲突失败关闭。
- F-003：原执行类精确映射至现有未退役实现，不创造执行别名。
- F-004：真实DEV登记、序列化读回及结构验收；生产与数值门禁独立。
- F-005：恢复原明确的模型输入排除规则，保留原始因子资产。
- F-006：父包再次冻结时保留拟合处理器已冻结代码依赖。

## 实施方案

OriginalContextResolver核验原artifact manifest、发布attempt及conf/binding/build spec的SHA，把原始字节作为版本化source evidence保存。promotion仅在显式开关下调用桥接，将桥接SHA纳入新包身份，版本为1.0.1；不改原run/attempt/旧manifest。原n_drop=1、max_n_drop=2分别保留；使用真实PIT pins，旧filtered_pool alias没有证据时保持UNKNOWN。

逐seed输入投影仅采纳原请求exclude_from_model，不按处理器少列猜测。DEV检出LSTM原清单27动态＋20 Alpha158，而原请求排除Price_Deviation_Historical_High，模型实际26＋20=46列；FUND为原8列排除后7列。逐seed保存原请求、清单及处理器SHA；原始因子仍纳入父包资产保护账本，模型、处理器及代码字节不变。

父包再冻结时将已声明model_code_assets.module_name作为处理器pickle代码闭合的根，防止qe_custom_loaders在没有原训练工作区时丢失。只使用已冻结CAS代码，不回退到工作区找替代源码。

## 验证方案

直接验证覆盖原发布来源/哈希错误、字段冲突、退役阻断、精确输入投影、原资产留存及无原工作区的处理器依赖再冻结。17项相关测试通过。现有DEV登记OLD5 pkg_ma_4f155008e142d60d983743d4及UPDATED3 pkg_ma_71b0f07c258b684965a88fd0，完整序列化读回与真实模型/处理器结构反序列化通过，预测调用为0；见validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/parent_dev_registration_v5.json。

数值冷消费另按真实DEV父包清单，经正式CAS resolver读取16个不同seed模型及原处理器，用原来源数据面板检查2026-09-01～03完整共同股票集合；计划每模型一次predict覆盖三天，共16次调用、48个模型日期单元，上限48次真实调用。GPU LSTM在原WSL CUDA0，CPU FUND在原远端环境；各节点一次有界诊断并行，无训练及完整窗口推理。逐seed保留原dtype，最终调用正式seed均值、逐日rank、固定权重组合，核对原发布分数和Top20；数值结果与结构收据分开报告，不替代完整荐股API或经济准入。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/strategy_package/original_context_bridge.py | backend/tests/strategy_package/test_original_context_bridge.py | PASS | 无 |
| F-002 | backend/services/strategy_package/multi_alpha_promotion.py | backend/tests/strategy_package/test_original_context_bridge.py | PASS | 无 |
| F-003 | backend/services/strategy_package/original_context_bridge.py | backend/tests/strategy_package/test_original_context_bridge.py | PASS | 无 |
| F-004 | promotion与既有repository/freezer/self-check | validation-receipt: tmp/handoff/qe-trend-parent-longtask-20261116/parent_dev_registration_v5.json | PASS | 无 |
| F-005 | backend/services/strategy_package/original_input_projection.py | backend/tests/strategy_package/test_original_input_projection.py | PASS | 无 |
| F-006 | backend/services/strategy_package/package_asset_freeze.py | backend/tests/strategy_package/test_frozen_preprocessor_assets.py | PASS | 无 |

DESIGN-COMPLIANCE-001：逐项审核F-001～006源码与直接证据；无未批准简化、占位成功、语义替换或准入绕行。矩阵不表示新包收益已经确认或旧父包已经恢复。

## 风险与失败模式

原请求、发布attempt、执行定义、因子引用、处理器及模型SHA任一不可证实都失败关闭。结构成功不能代替数值一致；数值误差容差预先固定1e-6，不为通过而放宽、改设备或重拟合。缺失历史指纹保持UNKNOWN。失败、恢复来源及实际成本完整保留，重新调用预测前核对已有输出和原预算。

## 生产门禁

现有DEV仅恢复既有登记源类型约束的schema parity，生产该约束已具备，无生产DDL/DML。本次新包不替换旧身份、不自动晋升、不写生产包或模拟盘。源码/CI/合入、DEV结构、整包数值、运行加载及业务/经济验收分别报告；若需API加载，用户重启8001后再验证。
