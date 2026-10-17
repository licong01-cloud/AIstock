# LocalSIM 与共享选股的冻结文件输入模式

日期：2026-10-17；F2 / T3。批准来源：用户要求先完成数据集文件读取模式的共享代码切换，数据集问题在代码交付后交数据准备 owner。沿用共享选股详细设计和 LocalSIM 接入设计；本次授权覆盖必要的公共只读输入接口及消费者薄适配，不涉及训练、数据生产或数据库迁移。

## 背景与范围

荐股 BUG-1946 已提供日频历史收益文件读取，现有共享推理与 LocalSIM 分钟回放仍读取数据库。新增显式冻结文件模式，公共读取代码供两端消费；独立 consumer_scope、run_id、缓存、产物和断点，原选股与交易算法不复制。

精确写范围（新增文件也属于范围）：

- backend/data_service/frozen_dataset.py
- backend/services/advisory_historical_range/inference_inputs.py
- backend/services/advisory_historical_range/outcome_file_source.py
- backend/inference_engine.py
- scripts/strategy_package_live_inference.py
- noxfile.py（现有数据与历史推理验证计划登记公共文件读取和入口测试，不新增 runner）
- backend/services/selection_center/risk_policy.py
- backend/services/strategy_package/reference_prices.py
- backend/services/strategy_package/selection_artifact.py（共享入口显式文件来源能力校验，不改变计算）
- backend/services/simulation_data/contracts.py
- backend/services/simulation_data/minute_provider.py
- backend/services/simulation_data/frozen_file_adapters.py
- backend/services/simulation_execution/localsim/runtime.py
- backend/services/simulation_execution/broker.py（登记文件行情通道类型）
- backend/services/simulation_execution/localsim/suspension.py
- backend/services/simulation_runtime/localsim_file_inputs.py
- backend/services/simulation_runtime/localsim_product_control.py
- backend/services/simulation_runtime/localsim_replay_runtime.py
- backend/services/simulation_runtime/localsim_replay_recovery.py（文件任务恢复必须读冻结来源）
- backend/services/simulation_runtime/localsim_native_context.py
- backend/services/simulation_runtime/localsim_selection_inference.py
- backend/services/simulation_runtime/localsim_selection_signal.py
- backend/services/simulation_runtime/localsim_selection_probe.py（原生只读探针增加显式冻结文件输入）
- backend/services/simulation_runtime/scheduler.py
- frontend/src/lib/simulation/localsim/api.ts
- frontend/tests/paper-v2/localsim-unified-control-plane.spec.ts（新建默认文件来源请求，保留旧 DB 任务 fixture）
- backend/tests/data_service/test_frozen_dataset.py
- backend/tests/advisory_historical_range/test_file_inference_inputs.py
- backend/tests/test_inference_engine_historical_readonly.py（修正现有 __new__ fixture 的 temp_root 初始化）
- backend/tests/simulation_data/test_frozen_file_adapters.py
- backend/tests/simulation_runtime/test_localsim_file_inputs.py
- 本文和 docs/analysis/localsim_shared_frozen_file_inputs_acceptance_20261017.md

如实现需要其他文件，先在本文登记具体路径与理由。不得修改 AGENTS、标准、QE、HMM、数据发布或 ownership catalog；公共读取代码接受既有版本化资产，不生产/补写数据。

## 边界

本次交付显式文件来源和消费者接入；用户已允许数据完整性问题独立交接。保留原任务、原账户、已完成前缀和原数据库来源，正式迁移需要后续受控合同；本次不以创建替代任务绕过失败。数据准备和 canonical PIT 权威激活由对应 owner 处理。

## 架构与接口契约

公共 FrozenDataset 只读访问同一权威资产，冻结 profile、manifest、组件内容身份与合法范围；路径为 locator，Windows/WSL 可使用不同可验证 locator。日线、静态因子、PIT 区间、停牌、日历和分钟切片从同一冻结绑定读取，漂移、越界、错误单位及真实缺失明确失败，无 DB/网络降级。

共享 InferenceInputSession 新增文件工厂，沿用现有静态因子 builder、特征计算、冻结 processor、模型序列及 seed/权重分派。通过现有 price_reader/static_builder 接口接入 InferenceEngine，不新建评分算法；CLI 用显式冻结绑定选择来源。池成员每日绑定；C 限制推理输入，D 限制停牌资格，risk/行业/参考价仍按原合法截止。

LocalSIM 用 DATASET_FILES 历史来源与独立 source identity，薄适配器将分钟 bin 恢复为原生 raw CNY、手、金额口径，再交现有 MinuteExecutionMarketInput、撮合与估值链。不得把研究特征聚合、日线或填充 bar 当作分钟行情。合法停牌沿用公共 day projector 与现有停牌估值合同；可交易股真缺价继续失败。

荐股私有 outcome adapter 仅复用下沉后的基础文件读数/校验，不向 LocalSIM 暴露收益标签、主体、writer 或私有业务缓存。控制面、订单、账本及模型注册元数据仍可读取数据库；文件模式禁止行情热路径隐式 SQL。

## 存储、幂等与恢复

新文件来源在创建时冻结到现有 release metadata，source hash 与 replay identity 绑定，沿用现有 JSON/事务存储，不新增 DDL。重试读已冻结绑定，不重解析当前 active profile。旧 DB 任务保持原来源，不能借本次部署自动升级、改 FAILED_TERMINAL 或替换回放；来源迁移需要单独正式合同，不在本次执行生产变更。

每个消费者/run 独立缓存；文件 hash/时间指纹与实际读值收据进入来源证据。Windows 新临时对象只在 X:/AIstock_temp，WSL 经可访问性验证使用对应目录；模型和数据原址只读，不复制大资产。

H5 和小型权威文件绑定内容 SHA256。分钟/日频 bin 使用在注册时冻结的 `consumer_bin_stat_lease_v1`（相对路径、size、mtime_ns）作文件世代检查，实际日切片另记录字节 hash；stat lease 明确不是 producer 全量内容 manifest。压缩后的纯元数据放入 release JSON，当前真实发布包约 0.80 MB、62,384 个 bin，未复制 bin。任何触及已冻结文件的修复都需新发布身份和受控恢复，不会在旧冻结任务内热换输入。提供完整 producer 内容 manifest 仍是数据发布 owner 可以补充的更强校验能力。

## 实施方案

1. 公共冻结文件读取及荐股现有适配复用。
2. 共享 CLI/推理文件输入接入，保留原计算与合法时钟。
3. LocalSIM 历史分钟/日绑定/估值薄适配与显式新来源登记。
4. 多轮人工审查、失败点修复、直接测试、真实文件只读 smoke、F2 验证和独立 PR；通过必要 CI 后按本窗口已有合入授权交付。用户负责后端重启。

## 验证方案与结果

直接测试覆盖冻结身份漂移、越界/未来毒化、按日股票池与 UNKNOWN、raw 价格/复权/成交量/金额换算、停牌/真缺失、双消费者隔离、增量与 exact retry、旧 DB 身份兼容、共享算法调用及资金/持仓/订单语义不变。使用真实数据文件只读验证普通样本及已知缺失样本；允许数据缺口保持有证据 BLOCKED，不以错误修补、mock-only 或训练绕过。广泛跨模块回归交必需 CI/nightly，直接失败只重跑对应 nodeid。

记录实际读量、耗时和内存边界；技术验收不是生产回放或收益证明。不得宣称每天固定耗时。未启动生产回放，未读到的市场覆盖保持未知。

## Design Acceptance Index

| design_item | requirement |
|---|---|
| F-001 | 公共冻结文件读取复用，无重复选股/收益算法 |
| F-002 | profile/manifest/实际读取身份、范围与漂移明确校验 |
| F-003 | 唯一共享推理接文件输入，原元数据/processor/时钟/人口保留 |
| F-004 | 原生分钟与日绑定适配、单位正确，缺失 fail closed |
| F-005 | 停牌规则及账户/订单/账本/收益原语义不变 |
| F-006 | 消费者隔离、冻结重试、旧回放/完成前缀不被升级 |
| F-007 | 直接回归及真实只读文件证据，数据缺口独立登记 |
| F-008 | 多轮审核、DESIGN-COMPLIANCE-001、F2/CI 与运行加载分报 |

## 风险与依赖

现有数据集可能缺分钟、参考价或政策需要的字段；本次明确报数据合同缺口，交数据 owner，不推算补值、不放宽政策。文件行业层级与原生政策不等价时不能偷换。当前 active profile 与旧 replay 的 DB 来源不相同，禁止隐式来源切换。

## 发布、回滚与生产门禁

production_ddl_gate=noop；dependency_gate=noop；训练/数据激活/生产回放=noop。源码合入后运行加载仍待用户重启与实际身份复核。回滚源代码保留新旧输入证据和经济前缀；不允许旧代码消费不认识的新文件来源。

## Design Acceptance Matrix

实施与测试证据统一记录在 [验收记录](../analysis/localsim_shared_frozen_file_inputs_acceptance_20261017.md)，未完成前不预填 PASS。
