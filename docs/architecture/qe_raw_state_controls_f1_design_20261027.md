# QE 原始输入状态诊断列 F1 设计与验收

Feature tier: `F1`。2026-10-27 用户已批准按接入建议开发、多轮审核修复、提交及合入。批准范围来自 `qe-id1555-state-value-intake-20261026/consumer_extension_request.md` 与因子窗口 `short-inventory-trend-preqe-20261026/qe-handoff.md`。

## 背景

ID1555 的既有结果不能区分原始输入可得状态与融券余额数值的贡献。现有 `feature_processing` 只能排除输入和配置 clip；本次扩展正式 composer，使两臂从同一原始 F 产生状态列，不覆盖官方因子或编辑生成文件。

## 范围

复用 `CustomEvoLoopConfig.feature_processing`。可选 `raw_state_controls` 必须且只能包含 `schema_version=qe_raw_state_controls_v1`、`source_factor`、`valid_column`、`zero_column`。源必须属于本次计算因子；输出名为不同的 Python 标识符且不与计算列冲突。声明错误、非数值源、正值、无穷、非唯一索引均明确报错。

在官方因子读取/缓存及原参考面板之后、模型投影和原 RobustZScoreNorm/clip/Fillna 之前，从未标准化的非正有限值或 NA 派生 `isfinite(F)` 和 `F==0`，类型 int8。NA=(0,0)，报告零=(1,1)，报告正余额 F<0=(1,0)；缺失不表示没有融券压力。不新增 shift、不读标签、不裁掉 F 缺失样本。

保持 AF11 相对顺序，先放共同 AF/Z 列，ZF 最后增加 F。PDH 仍只作参考。13 个正式源计算因子、2 个局部派生列，实际 Z/ZF 输入为13/14列；parquet 元数据与 `.factor_env` 记录实际输入。训练和推理共享生成表与既有处理器。

## 非目标与边界

只修改 `config_composer.py`、现有 API 字段说明、对应 cache-contract 测试和本设计。无新端点、目录、注册、数据集、DDL/DML、依赖或服务控制。不更改 ID1555、旧缓存、HMM、默认策略、观察器及运行中实验。不提供任意公式或表达式执行平台。

本次是消费能力开发，不启动六 Loop，不重跑原九 Loop。后续六 Loop 排在主线完成、分析入仓及用户重启加载新代码之后；创建时正常绑定当时 active。旧 AF 只有严格可比才复用，身份变化须报告共享基准重建方案，不自动扩展预算。

## Design Acceptance Index

| item | requirement |
| --- | --- |
| F-001 | 正式配置声明受限且严格校验；请求、JSON 保存读回及 retry 不丢声明；仅完整 single-alpha 训练可用。 |
| F-002 | 原 F 在标准化前派生两列，三状态、int8、无重复滞后；正值、无穷、非数值、重复索引报错。 |
| F-003 | 参考面板与两臂索引一致，不按 F 非空筛样本；原 F、官方代码和共享缓存不变。 |
| F-004 | 源计算13列、派生2列、模型13/14列可核验，共同列顺序一致，F 在 ZF 最后。 |
| F-005 | 派生表经真实 Qlib 处理器供训练和推理消费；保留原 RobustZScoreNorm/clip/Fillna、模型及策略配置。 |
| F-006 | 未声明新配置时，原生成脚本、缓存身份及公开请求行为保持不变；不引入默认回退或跨请求污染。 |
| F-007 | 新进程通过正式 composer 生成并执行消费，保留可复查收据；代码审核、F1 验收及必要测试通过。 |
| F-008 | 生产写入、服务控制、运行中实验修改及新增实验提交均为0；源码、运行时激活、经济验证分开报告。 |

## 实施方案

1. 扩展 `_normalize_feature_processing`，仅在显式声明时记录状态规则和派生列，将源数值列排在共同列后。
2. 正式生成 prepare 脚本增加受限派生函数，在参考面板后调用；输入错误抛异常，生成表保留原处理元数据。未启用时不生成新分支。
3. 更新 API 字段说明；沿用现有构建器、快照及 retry，不新增入口。
4. 增加真实生成表、失败路径、完整配置隔离、Qlib 处理器及新进程消费测试。两轮自审分别检查业务语义、交付边界与验证缺口，发现问题先修复再验收。

## 验证方案

直接运行 `backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py` 的相关测试；真实 Qlib 环境运行处理器验收，不以 API 环境的 skip 作为通过。新进程执行正式生成脚本并读取实际输入，验证缓存字节、样本、状态和13/14列；原数据保持只读。验证不训练新 QE 模型、不产生实验账户。

运行 changed-file lint/compile、`git diff --check` 及 `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_raw_state_controls_f1_design_20261027.md --tier F1`。广泛跨模块回归交 CI，相关小矩阵稳定后只运行一次，失败只重跑失败节点。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | config_composer.py; quantevolver_evolution.py | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py 声明及公开快照/retry 测试 | PASS | none |
| F-002 | config_composer.py 生成派生函数 | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py 三状态及错误输入测试 | PASS | none |
| F-003 | config_composer.py prepare 主流程 | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py 新进程两臂及缓存哈希测试 | PASS | none |
| F-004 | config_composer.py 输入投影及元数据 | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py 新进程13/14列测试 | PASS | none |
| F-005 | 既有 Qlib handler 及生成表 | artifact: F:/Dev/AIstock/tmp/handoff/qe-raw-state-controls-dev-20261027/fresh_qlib_receipt.json | PASS | none |
| F-006 | config_composer.py 可选分支 | backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py 完整 YAML 隔离、旧缓存及面板回归 | PASS | none |
| F-007 | 正式 composer/prepare fresh-process | artifact: F:/Dev/AIstock/tmp/handoff/qe-raw-state-controls-dev-20261027/fresh_qlib_receipt.json; backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py | PASS | none |
| F-008 | 四文件变更范围及下列生产门禁 | artifact: 本设计生产门禁及审核记录 | PASS | none |

## 风险与失败模式

常数状态列可能经原 RobustZScoreNorm 后全为0，这是原处理器行为，不改变标准化。NA 的 F 最终 Fillna 为0与原始真实零不同，不以处理后值重建状态。参考面板外仍持仓的归因属于后续实验分析，本功能不推断收益原因。错误输入须失败，不填安全状态、不改方向、不隐藏缺失因子。

## 生产门禁 / Production Gates

`production_ddl_gate=noop`；`production_dml_gate=noop`；`dependency_install=noop`；`dataset_write_count=0`；`official_catalog_write_count=0`；`experiment_submit_count=0`；`backend_restart_owner=user`。

DEV 为现有环境中的文件消费验证；本次没有数据库迁移。源码合入不代表运行时已加载；用户重启后还需只读运行时核验。六 Loop 尚未启动、没有经济结论。

## DESIGN-COMPLIANCE-001 与审核记录

两轮均为本窗口自审，没有冒充独立窗口审核。

1. 业务语义审查：发现历史合并流程会删除重复列，可能隐藏源歧义；已在新配置的合并前检查逐因子唯一索引及精确单列。F 正值/无穷/非数值明确失败。状态在原面板后、投影和标准化前生成，缺失仍在样本中。
2. 验证及交付边界审查：补上公开请求保存/JSON读回/retry 和全配置隔离；真实原输入初始取样未覆盖报告零，补取原冻结输入中的真实零行，使用 parquet 二进制传输，避免 JSON 浮点精度变化。修复后验收通过；无数据集或原冻结输入写入。

定向小矩阵：`rtk pytest backend/tests/quantevolver/test_qe_prepare_factors_cache_contract.py -q --tb=short`：58 passed，2 skipped。本机无 Qlib；两个 skip 明确保留，未计为通过。现有 WSL Qlib 0.9.6.99 通过独立新进程运行实际 RobustZScoreNorm、Fillna、LongHorizonLabelMaturityPurge、DropnaLabel、CSZScoreNorm。

真实输入取自已完成 `qe_20261026_130549_50d9/Loop2` 的原参考面板，抽取每日期/读取批中的前三行并补实际报告零，6,454行，1,982日；状态为 NA 2,044、真实零145、原F负值4,265。抽样仅作文件消费验证，不代表六Loop实验样本或收益验证。新进程以同一原F比较两臂，13/14列，训练6,254行、推理6,454行，训练与推理共有行的特征完全相同，两臂共同原始和处理后列完全相同。仅测试标签用于驱动原学习处理器，没有收益或IC声明。

原 ID1555 源 SHA256：`253d58a18ad4f57b403af2b156ad959827a52d8ef7074111bba651084c55bcc3`；冻结绑定 manifest：`afa9158b9b1d8cc76ec1e40a6efdb2060365bb3039ea791a0b5caa2bae8c5337`；精确原值样本 parquet SHA256：`c00f6c85f610b325db21be6e1e4e3de48692d2001257fa97a87be243991b1889`。该绑定仅证明本次 DEV 消费验证来源，未来创建实验仍正常解析 active。

| 检查项 | 结论 | 直接依据 |
| --- | --- | --- |
| 无未经批准的缩减交付 | PASS | F-001～F-008 覆盖正式生成、保存恢复、缓存、两臂及真实处理器；源码交付与尚未启动的经济实验分别报告。 |
| 无静默错误或假成功 | PASS | 未知声明、冲突、非法源、歧义索引均失败；skip 单列，真实 Qlib 收据独立保留。 |
| 无未经批准的业务语义变化 | PASS | 默认分支和完整 YAML 隔离测试通过；源F、参考面板、原处理器及费用不变。 |
| 无新增审批或人工门禁 | PASS | 原正式入口扩展；没有新审批流程，服务仍由用户按原约定重启。 |
