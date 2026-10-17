# QE 请求级模型输入与裁剪配置（F1）

## Background

依据已合入因子研发蓝图 §24、方法论 §2.9，以及用户 2026-10-17 批准的补充约定。提供历史高点特征消融与独立取消裁剪的正式配置能力；不修改蓝图，不预设经济收益。

## Scope

QE owner 实施，精确文件范围：

- backend/services/quantevolver/config_composer.py
- backend/services/quantevolver/experiment_config_builders.py
- backend/routers/quantevolver_evolution.py
- backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py
- 本文档

## Non-goals

不修改 Qlib、因子公式、数据集、active 解析、股票池、模型超参、标签、成本或策略；不建表/身份系统/审批，不启动实验或操作服务。新配置不改变既有默认行为。

## Design Acceptance Index

- F-001：独立请求级 `feature_processing`，默认无排除、`clip_outlier=true`；无跨请求污染，不作为模型或策略构造参数。
- F-002：先用全部计算因子形成原 observation panel，再排除模型输入列；不改变索引和其他列，不覆盖共享因子缓存。
- F-003：最终模型消费的 run-local Parquet 保存计算列表、实际输入列表、排除及裁剪配置；保留既有任务配置记录，不以计算因子 digest 冒充模型输入身份。
- F-004：训练和推理共同使用生成的 RobustZScoreNorm；只改变 clip_outlier，拟合范围、Fillna、分箱、h20 成熟处理不变。
- F-005：正式 custom_evo 请求、配置构建、持久化快照及重试保持选项；没有新数据绑定入口或后台任务。

## Implementation plan

custom_evo 的顶层 `feature_processing` 对象包含 `exclude_from_model: list[str]` 和 `clip_outlier: bool`，未传则保持旧输出。现有配置快照中的同名对象可复用于 retry；不把字段平铺到 LGBM/策略 kwargs。仅支持本轮批准的自定义因子、Alpha158 关闭路径。非法字段、非布尔裁剪、未知/重复/全部排除因子按普通参数错误报告，不静默忽略。

调用约定（各 loop 分别配置，不叠加 A/C）：

```json
{
  "B": {"exclude_from_model": [], "clip_outlier": true},
  "A": {"exclude_from_model": ["Price_Deviation_Historical_High"], "clip_outlier": true},
  "C": {"exclude_from_model": [], "clip_outlier": false}
}
```

上表每个值放入相应 loop 的 `feature_processing`；原 `factor_keys`/`factor_names` 与 `observation_panel` 保持原合同。B 也可省略新字段，保持既有配置和输出格式。代码只提供配置能力，不在运行时自动创建三臂。

A 仍计算原11因子，保留 Price_Deviation_Historical_High 面板参考，模型消费10列；B/C 消费11列。原单因子共享缓存不变，仅本任务 combined_factors_df.parquet 做投影。有效输入名单和配置保存在该既有 Parquet 的 schema metadata；不增加资产文件或数据库表。新选项仅在 single-alpha 训练请求中设置，不能向纯回放、旧模型复用或多腿请求注入新预处理后宣称已生效；此类矛盾参数明确报错，原有重试快照保留已经执行过的配置。这是参数消费校验，不是审批或资源门禁。

数据窗口交接后一次决定执行数量：严格可比原 B 可复用则新增6次训练/回测；仅交易行情变化则另回放 B；训练输入修复则 B/A/C 共9次训练/回测。新任务解析当时 active 一次并保留重试绑定，不硬编码版本、不增加全量哈希。

信号窗口与标签数据可得边界分别记录。若仍为8月底可得边界，按实际 h20 和日历计算成熟日；若9月数据允许更多8月标签成熟，应三臂统一并分开报告原共同区间和新增成熟区间，不机械硬编码7月31日，不静默扩窗。此项是后续实验计划，不在本次源码中修改标签算法。

评价保留沪深300原基准，并以中证全指为补充市场参照；逐 seed、时期报告 A-B/C-B、成功/失败趋势排名、净收益/超额/回撤/换手。既有历史均为 research feedback，不将三个 seed 视为独立市场。

## Verification plan

实际执行生成的 prepare_factors main（小型 tmp_path 产物，不运行因子研发或训练），验证面板及 Parquet、共享输入不变；连续生成 B/A/C/B，解析最终 YAML 并核对真实 RobustZScoreNorm 的裁剪行为。API schema 到 builder 到重试快照合同验证；默认配置与原合同数值/结构一致。定向 pytest、Ruff、py_compile、diff/scope 及 F1 验收。多轮自审不冒充独立 agent 审核。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_b_a_c_b_yaml_isolation_and_real_normalizer | PASS | 无 |
| F-002 | backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_real_panel_output_keeps_cache_and_identity | PASS | 无 |
| F-003 | backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_real_panel_output_keeps_cache_and_identity | PASS | 无 |
| F-004 | backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_b_a_c_b_yaml_isolation_and_real_normalizer | PASS | 无 |
| F-005 | backend/routers/quantevolver_evolution.py; backend/services/quantevolver/experiment_config_builders.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_public_request_and_retry_snapshot_are_request_local; backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py::test_feature_processing_cannot_silently_skip_training | PASS | 无 |

源码验收证据（2026-10-17）：在现有 AIstock Python 环境运行 test_qe_prepare_factors_cache_contract.py 与 test_experiment_config.py 两个完整测试文件，84 passed。真实小型 Parquet 写入、共享输入字节不变、原面板锚点保留、Qlib DataHandlerLP 的 DK_L/DK_I 共同预处理、序列 B/A/C/B 和重试快照均通过。只运行单元/合同测试，没有模型训练或投资回测。更广的 QE read/sector overlay 回归交现有 CI/nightly，不据此声称全系统业务验收。

CI 路由复核：原 test_experiment_config.py 不在本次选中的 CI 计划中，新增四项配置测试完整移入已由 qe_sector_risk_overlay_backend 执行的 test_qe_prepare_factors_cache_contract.py；断言不变，原配置测试文件无 PR 差异，不修改流水线、计划或 ownership。迁移后该完整文件39 passed；分类器 workflow_gate=passed、unexecuted_test_files=[]，F1、Ruff、py_compile、L0 和归属检查通过。

多轮自审：第一轮修正生成脚本 JSON 别名及测试 YAML 日期序列化；第二轮补齐实际训练/推理消费路径和不支持模式的参数报错；第三轮核对默认兼容、缓存不变、实际输入身份、成熟边界及授权范围。没有借用其他 agent 的名义报告审核。

DESIGN-COMPLIANCE-001：① 两项配置均有正式请求、真实产物消费与直接验证，非占位；② 非法/未消费配置明确报错，无静默降级；③ 只有显式 A/C 改变对应输入/预处理，默认、标签和交易合同不变；④ 无新增审批、资源门禁或数据身份系统。

## Risks

原始因子列表与模型列不同；不得只用原11因子 digest 描述 A。取消裁剪可能增加噪声，消融会改变树的抽样结果；均须重新训练才可评价，不保证收益。新 active 若改变训练数据则重建共享 B，不能将数据修复收益归给模型。源码验证不代表正式实验完成。

## Production gates

DDL/DML、依赖安装、数据发布、客户端安装：noop。影响 backend-main 的新装配配置；后端重启由用户执行，源码交付不代表运行时激活。用户已授权多轮审核修复、CI 通过后的源码合入；未获本批实验启动授权，不训练、不回测。
