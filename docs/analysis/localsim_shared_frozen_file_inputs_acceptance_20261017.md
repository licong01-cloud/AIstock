# LocalSIM 共享冻结文件输入验收记录

日期：2026-10-17。设计：[冻结文件输入设计](../architecture/localsim_shared_frozen_file_inputs_20261017.md)。本记录区分代码合同、数据覆盖、生产回放和运行加载；源码状态以独立 PR 为准。

批准范围：用户明确要求“先完成数据集文件读取模式的共享代码切换，数据集问题可以完成后再让数据准备窗口处理”。下表验收的是该范围的代码能力和严格失败行为，不把数据缺口、完整评分对照或生产回放记作通过。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/data_service/frozen_dataset.py；advisory_historical_range/outcome_file_source.py、inference_inputs.py | backend/tests/advisory_historical_range/test_file_inference_inputs.py 调用原静态 builder 代码；原 outcome 回归；唯一 InferenceEngine、SelectionSignalKernel、reference_prices 未复制算法 | PASS | 无 |
| F-002 | FrozenDataset；localsim_file_inputs.py；CLI _file_binding | backend/tests/data_service/test_frozen_dataset.py：内容 hash、世代漂移、日期/池/字段边界、冻结身份篡改；backend/tests/simulation_data/test_frozen_file_adapters.py：首次读取前 bin 漂移拒绝 | PASS | 无 |
| F-003 | InferenceEngine；strategy_package_live_inference.py；selection_artifact.py；localsim_selection_inference.py | backend/tests/advisory_historical_range/test_file_inference_inputs.py：C 截止、未来数据毒化、逐日池身份；backend/tests/simulation_runtime/test_localsim_file_inputs.py：无文件能力的共享 provider 明确拒绝；真实 LSTM/TCN 探针走到原生 PIT 校验并拒绝身份不一致 | PASS | 用户明确批准数据问题后续交接；真实完整评分/排序/UNKNOWN/TopK 对照尚待匹配的数据与原生 PIT 权威合同，未宣称通过 |
| F-004 | frozen_file_adapters.py；minute_provider.py；contracts.py | backend/tests/simulation_data/test_frozen_file_adapters.py：原生分钟单位、raw li 精度、真缺 pre_close、全日无分钟失败；真实 600519.SH 240 根分钟、001205.SZ pre_close=15.30 | PASS | 用户明确批准缺失数据后续交数据 owner；603900.SH/2026-07-10 全 NaN 分钟保持明确失败 |
| F-005 | 原生 reference_prices、suspension、MinuteExecutionMarketInput、broker/scheduler；文件薄适配 | backend/tests/simulation_data/test_frozen_file_adapters.py 及 native halt/reference 回归；真实 301362.SZ 历史参考价44.98、603616.SH合法停牌不补 pre_close；backend/tests/simulation_runtime/test_localsim_file_inputs.py 文件/DB 同输入 TWAP 订单、100股、现金及5元佣金一致 | PASS | 无 |
| F-006 | localsim_file_inputs.py；product_control、native_context、replay_runtime、replay_recovery | backend/tests/simulation_runtime/test_localsim_file_inputs.py：新来源原生创建事务、active pointer 改变后的冻结重试、异常后 scope 清理、旧 DB 身份不变；现有 recovery/lifecycle/product/native_context 定向回归 | PASS | 无 |
| F-007 | 四个新增直接测试文件；原生只读 localsim_selection_probe.py | artifact: X:/AIstock_temp/LOCALSIM/shared-file-inputs-tests/real-file-native-smoke-r3.json；真实输入及两包探针收据见下文；最终小矩阵和失败 nodeid 修复复验 | PASS | 用户明确批准数据缺口独立交接；真实多日完整评分性能、全量一致性与生产业务验收保持待数据就绪 |
| F-008 | 本文四项符合性审核；设计；noxfile.py 现有验证计划 | artifact: X:/AIstock_temp/LOCALSIM/shared-file-inputs-tests/final-matrix-r1.xml；下述多轮审核与直接回归；F2、changed-file gate 和 CI 结果单独记录于 PR | PASS | 无 |

## 直接回归与审核

证据目录：`X:/AIstock_temp/LOCALSIM/shared-file-inputs-tests/`。最终小矩阵覆盖 15 个直接相关测试文件：256 项通过、1 项条件跳过、2 项旧 fixture 初始化失败。修正两个使用 `InferenceEngine.__new__` 的测试 fixture，按 nodeid 复验通过；后续文件来源能力守卫新增 1 项通过。有效唯一结果为 259 项通过、1 项条件跳过，不把第一次失败的收据改写成全绿。

- `final-matrix-r1.xml`：最终直接矩阵原始收据。
- `fixture-fix-r2.xml`、`review-r3.xml`：失败点及后续 PIT 身份错误码修复复验。
- `provider-guard-r1.xml`：禁止默认 DB provider 冒充文件来源。
- `forwarding-final-r4.xml`：最终 forwarding fixture 可移植调整，两个 nodeid 通过；语法/lint、精确写范围、F2 结果在 PR 交付记录补充。

三轮人工审核处理了实际发现：第一轮修正 H5 日期查询、文件来源入口守卫及原生静态字段口径；第二轮修正 float32 原始限价还原和 dataclass 适配；第三轮补齐 bin 注册世代校验、恢复时冻结来源核对和文件 risk provider 隔离。新增测试覆盖对应合同，不靠复制评分、过滤或财务逻辑规避问题。

DESIGN-COMPLIANCE-001 逐项结论：

1. 禁止简化交付：文件工厂、CLI、共享推理、逐日选股、分钟撮合输入、估值、创建/恢复和 UI 新建来源均接入。用户明确批准数据问题后续交接；真实评分业务验收仍未通过，不以 mock 或数据阻塞冒充已完成生产回放。
2. 禁止静默错误：缺字段、世代漂移、原生池政策不匹配、真实缺价、分钟整日缺失和不可覆盖的风险政策均有明确错误；无 SQL/网络/日线/填充价格降级。
3. 禁止改变业务逻辑：沿用唯一共享内核与原生停牌/参考价/订单/佣金/账本；旧来源及完成前缀不变。新来源是显式冻结身份，UI 仅改变新建历史回放的默认来源。
4. 禁止私增门禁审批：沿用已有 PR/CI 和本窗口合入授权；本轮无 DDL、训练、生产任务启动或服务控制。后端重启继续由用户执行，没有新增人工批准流程。

## 真实文件证据与性能边界

冻结 profile：`F:/Dev/RD-Agent-state/dataset_profiles/active_dataset_profile.json`，发布 `qe_hmm_full_v2_20260831` / `20261015-v18-unified-quote1`。冻结绑定收据 `real-file-binding-r2.json`，hash：`589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c`。

注册扫描 62,384 个消费字段 bin 的 stat 元数据，耗时约29.38秒，压缩绑定约0.80MB；未复制模型、H5 或 bin。H5 与小型权威组件有内容 SHA256，bin 有冻结 stat lease 和实际切片 hash。此注册耗时不是每日选股时间。

`real-inference-inputs-smoke-r2.json`：600519.SH、001205.SZ 的 2026-01-05 至 C=01-19 输入，原生价格22×7、静态22×121，7次读取/1次缓存命中，缓存约2.66MB，耗时6.13秒、采样 RSS 约149.1MB。它只证明真实输入链，不代表全市场完整推理性能或峰值上界。

`real-file-native-smoke-r3.json`：

| 样本 | 原生文件结果 |
|---|---|
| 600519.SH / 2026-01-20 | 240根分钟；pre_close1376.00，涨跌停1513.60/1238.40；首根70200股、96799608元；分钟读取约0.155秒 |
| 001205.SZ / 2026-05-08 | 权威 pre_close15.30，涨跌停16.83/13.77，可交易 |
| 301362.SZ / 2026-01-20 | D日原生停牌；未补 pre_close；C=01-19 可用真实历史参考价44.98（报价01-16） |
| 603616.SH / 2026-01-21 | 原生停牌；未补 pre_close；C=01-20 的合法历史参考价6.23 |
| 603900.SH / 2026-07-10 | 分钟 OHLC 全NaN，FROZEN_DATASET_MINUTE_MISSING，保持BLOCKED |

上述组合读取13,716字节 bin 切片（另有 H5 查询），采样 RSS 约373MB；不是全市场/全日任务的峰值性能。

真实包只读探针：LSTM `simbind_ba4a6ed70edc1f45` 和 TCN `simbind_784e69d045ad8a34`，D/T=2026-01-20，C=01-19。只读控制库取原政策/模型注册，计算走原共享 WSL 入口，写入消费者自有 X 盘探针目录和内存 writer，未创建/恢复生产回放。

- LSTM：`X:/AIstock_temp/LOCALSIM/shared-file-lstm-r4/progress.json`。
- TCN：`X:/AIstock_temp/LOCALSIM/shared-file-tcn-r3/progress.json`。

两包均在原生 PIT 身份不一致处正确停止：原生权威为 `shsz_st_pit_active_v1` / `st_pub_next_trade_restore_active_l_v1`，文件为 `aistock_equity_pit_canonical_v2` / `shsz_a_252td_st_delist_asof_v2`。不能更名元数据或偷换原包政策强制通过；目前没有完整评分、4912/4918/6、完整排序、Top50、连续多日推理或生产回放验收结论。

## 数据准备窗口统一交接提示词

```text
请只处理冻结数据发布与权威合同，不修改 LocalSIM 状态、断点、账本或原策略政策。

LocalSIM 数据集文件模式共享代码已完成本地接入，沿用唯一 InferenceEngine / StrategyPackageSelectionArtifactService / SelectionSignalKernel。当前只读证据根目录 X:/AIstock_temp/LOCALSIM/shared-file-inputs-tests/；冻结 binding hash 589e99827698eba9ffce0745b17c69f4feb1cb48a7629da0a819af5a1bef6f0c。

1. 核查当前冻结文件 PIT 身份与原生控制面身份不一致：文件 universe_key=aistock_equity_pit_canonical_v2，rule_version=shsz_a_252td_st_delist_asof_v2；原包当前原生权威 universe_key=shsz_st_pit_active_v1，rule_version=st_pub_next_trade_restore_active_l_v1，authority_id=aistock_equity_pit_canonical，activation_generation=0，status=DEPLOYED_LEGACY_PENDING_MIGRATION。请返回语义相同且身份匹配的不可变文件发布；如应正式迁移 canonical PIT，请交对应权威 owner 给出激活、原生政策/池等价性、消费者受控迁移合同。禁止仅改字符串冒充相同政策，禁止在本轮静默切换权威。
2. 核查603900.SH/2026-07-10分钟OHLC全NaN（real-file-native-smoke-r3.json），用权威源判断缺失/停牌/其他事实。仅在有真实数据和正式依据时补录到新发布；不造分钟、不日线替代、不补推算pre_close。
3. 当前发布覆盖截至2026-08-31，不能支撑追平至当前已完成交易日。请提供所需日历、分钟、日线、因子、PIT与停牌等组件一致的新版本，返回manifest/hash/覆盖/权威读回收据；不要覆盖旧冻结任务所引用的文件。
4. 当前文件已验证001205.SZ/2026-05-08 pre_close=15.30，301362.SZ/2026-01-20停牌且合法历史参考价44.98，603616.SH/2026-01-21合法停牌且无pre_close，不需为这些样本制造补值。
5. 可补充完整producer bin内容manifest，增强现有consumer stat lease与读取切片hash，但不得把stat元数据说成内容hash。

请分别返回数据事实、源码/发布身份、覆盖范围与缺口、需要canonical PIT owner决定的事项。LocalSIM owner收到合法冻结发布后再做完整评分/排序/UNKNOWN/TopK、多日/恢复及原任务受控迁移验收；本回执不代表生产回放已恢复。
```

## 发布与运行状态

production_ddl_gate=noop；dependency_gate=noop；生产数据写入、训练、数据激活、生产回放、服务控制均未执行。旧 LSTM/TCN DB 回放保留原来源、状态和完成前缀；部署不会自动变为文件任务。源码 PR/CI/合入结果交付时报告；用户重启后的实际加载、数据就绪与生产回放业务验收另行验证。
