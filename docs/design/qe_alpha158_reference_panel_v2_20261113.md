# QE Alpha158 共同参考面板扩展（F1）

## 背景

用户要求并行执行主线双腿新版本与测试包重训。任务 qe_20261113_152355_f90c 已启动9个Loop；原预算内3个LSTM依赖Alpha158与FUND相同的PDH参考面板。v1目前明确拒绝Alpha158，不能删除保护后继续使用外连接的NestedDataLoader，也不能事后裁剪预测。

## 范围

显式新增 qe_observation_panel_v2，沿既有 reference_factor_intersection 生成原始参考掩码。仅修改 config_composer.py、qe_custom_loaders.py、新增对应测试及本设计。保持v1与无面板实验的行为；不改数据、模型公式、训练预算、策略或服务。

## 非目标

不恢复旧父包，不新增因子、不扩搜索，不修改当前运行的9个Loop；不训练、热补丁或重启后端。源代码交付与用户重启后的运行验收分开。

## Design Acceptance Index

- F-001：v2显式准入，v1仍拒绝Alpha158；Alpha158不接受raw_state_controls。
- F-002：先在原始因子上做参考掩码再移除参考列，Alpha15820列及顺序不变。
- F-003：复用原DynamicFactorsOnlyLoader的PIT/时间/标签合同，最终输入行严格等于参考自定义面板；禁止外连接扩行、静默丢行或填标签。
- F-004：Alpha158标签与参考标签逐值一致（含NA）；重复键、重名列、缺失参考键或结构错误明确失败。
- F-005：完整Composer输出、生成脚本与新加载器fresh-process验证；旧v1与无面板路径回归。

## 实施方案

v2专用ReferencePanelAlpha158Loader接收原dataloader_l配置以保留原Alpha158表达式及num_features计算。它先调用DynamicFactorsOnlyLoader获取参考面板与同一provider标签，再调用原Alpha158DL。确认参考行全部存在、标签完全一致后，按参考行与原列顺序合并；因子NA仍交给原处理器，不填安全值。保留NestedDataLoader的默认分支。

## 验证方案

小面板验证参考行、PIT剔除、Alpha158外部行、标签NA、重复键/列、缺行、标签漂移。验证完整Compose产物与prepare_factors掩码/投影顺序及20列公式。实际拟合不属于开发验证；源代码合入后等待用户重启8001，再核验版本并审核原预算追加请求。

## 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | ConfigComposer两项normalize | backend/tests/quantevolver/test_qe_alpha158_reference_panel.py::test_v2_explicit_opt_in_keeps_v1_alpha_rejection_and_raw_controls_boundary | passed | 无 |
| F-002 | prepare_factors及Alpha158配置 | backend/tests/quantevolver/test_qe_alpha158_reference_panel.py::test_prepare_v2_reference_mask_is_raw_and_projection_is_later；下述3份完整Composer收据 | passed | 无 |
| F-003 | ReferencePanelAlpha158Loader.load | backend/tests/quantevolver/test_qe_alpha158_reference_panel.py::test_alpha_rows_never_restore_reference_or_pit_exclusions | passed | 无 |
| F-004 | ReferencePanelAlpha158Loader.load | backend/tests/quantevolver/test_qe_alpha158_reference_panel.py 反例及同NA测试 | passed | 无 |
| F-005 | 完整Compose及旧路径 | python -m pytest backend/tests/quantevolver/test_qe_alpha158_reference_panel.py backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py backend/tests/quantevolver/test_qe_custom_loader_instruments.py -q -x --disable-warnings：87 passed、2 skipped；完整Composer与真实QLib收据如下 | passed | 无 |

## 风险

大面板额外标签读取有开销，但不新增拟合；禁止展开笛卡尔积。新路径要求标签与索引精确一致，真实不一致应阻断而非修补数据。不同seed不是独立市场样本，源代码通过不能作为收益证据。

## 生产门禁

数据库、生产资产写入、依赖变更均noop。后端重启由用户执行。现有实验不暂停；未核验已加载版本前不提交新LSTM。旧冻结身份与失败历史均保留。

## 验证收据与设计符合性复核

2026-11-13 两轮当前作者审核（非独立外审），检查真实调用/分发、v1兼容、重复键/列、参考缺行、PIT约束、标签漂移与NA保留。回归首次发现v2在custom-only声明下的异常码与旧合同不同，已恢复原schema_invalid前缀，先恢复失败用例，再完成最终87项相关测试；两个既有环境跳过项不作实际provider证据。

独立Python进程沿实际BacktestExecutor完整capture三个seed的46列LSTM请求，冻结模型代码SHA `fab0d14c023dd1101925983a1b6ab413b6feb59ca356833036b17916ac4c2b99`、26项原因子及参考源码，确认20个Alpha158名称、h20成熟处理器、565日与10/29估值；transport为0。收据`F:/Dev/AIstock/tmp/handoff/qe-dual-track-new-version-20261113/alpha-reference-v2/preview.json`，内容及源码SHA均在收据固定。首轮AST诊断遇到既有BOM头，改为仅在语法检查时去除BOM；生成文件字节未改。

真实WSL fresh-process使用同一772绑定的QLib provider，以1股2日临时输入验证20项Alpha158与原始NA、h20未成熟标签NA均保留；source SHA `ab74750e2c59ded2558c6d4b59cb5d0c8caad2cf6a6795af015941bd245a1d56`。仅在TemporaryDirectory写两行合成自定义输入，不修改节点实验或数据集，不新增拟合/预测/回测。收据`F:/Dev/AIstock/tmp/handoff/qe-dual-track-new-version-20261113/alpha-reference-v2/fresh_process_receipt.json`。这是加载器验证，不代表全量训练或收益证据。

DESIGN-COMPLIANCE-001：五项源码验收逐项满足；未将运行加载、三次训练或组合账户称为完成。不静默扩样本/填NA、不改批准的因子模型与策略、不私增审批。现有9账户由原观察器先验收；剩余3个Loop只有用户重启8001、加载核验、同任务查重和追加请求审核完成后才提交，避免新Loop污染当前9账户收集集合。
