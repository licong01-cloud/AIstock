# QE冻结预测回放末日边界：F1单模块设计

## 背景与授权

蓝图v6.27要求先验证1000万元、目标20只组合，并保留2026-09-30执行/估值。已冻结C1六份预测覆盖末日，但原账户显式backtest_end=2026-09-29；已有BoundedTerminal执行器只接入HMM专用consumer。用户已授权按长任务方案直接执行，不中断现有实验，历史合同全部结束后再启动新账户。

## 范围、边界与非目标

只增加QE可选正式prediction_replay参数和Composer接线，复用既有末日执行器；不修改执行器本身、策略评分/持仓/现金/费用、HMM、训练、数据集、目录或调度。普通配置不启用。此能力不生成预测，也不把9月未成熟h20标签补零。

精确源码范围：本设计；backend/services/quantevolver/terminal_replay_consumer.py、config_composer.py、executors/backtest.py；scripts/qe_terminal_replay_preflight.py；backend/tests/quantevolver/test_qe_terminal_replay.py；如归属目录需要，仅补精确ownership规则。其余任务草稿、真实资产预检和分析留在ignored交接目录。

## Design Acceptance Index

- F-001：明确可选合同qe_terminal_replay_v1，冻结开始日、末日、账户日数、active manifest及原预测SHA；普通路径完全不变，冲突/无效合同报错。
- F-002：仅允许PREDICTION_REPLAY；冻结预测身份与正式source一致，无fit、重新推理、其他模型来源或额外时序滞后。
- F-003：Composer只替换同一个分钟NestedExecutor及其SimulatorExecutor为已有BoundedTerminal类，不改变策略/费用/inner执行选项。
- F-004：真实day与minute日历哈希、末日物理bar、账户日期数、原预测及配置窗口在worker运行前核验；无未来bar、补数据或默认值。
- F-005：本次2026-01-05..09-30为181日；首日原信号可得情况如实保留，9月30日使用前交易日信号并真实估值。此前179/180日期不静默改写。
- F-006：直接合同测试及fresh-process六份原预测完整Composer/双节点末日验证；技术就绪与经济改善分开。源码合入/运行加载/首轮六账户单列状态。

## 实施方案

公开custom loop沿用model_params字典携带qe_terminal_replay对象；由正式builder传递，不改路由schema。对象字段为schema_version、start、terminal_date、expected_sessions、dataset_manifest_sha256、prediction_sha256。当前已有末日执行器仅支持2026-09-30，因此此版本明确只接受该日期；不把这一限制伪装为任意月份通用能力。

在Composer可选consumer边界移除该参数，核对服务器解析的active摘要、物理绑定及完整分钟配置，将预检合同和已有执行器作为实验文件分发。只按YAML节点位置修改四个class/module_path标量，保留原ALSTM模型模板、锚点及引用，不渲染或重新定义原模型。BacktestExecutor在提交前核对PREDICTION_REPLAY模式及原预测SHA。worker预检只读calendar、prediction和生成配置，随后运行既有--pred-backtest路径。HMM专用consumer与此参数互斥，避免二次替换；不触碰其原合同。

已有源C1 Top50/1亿元与新Top20/千万元/补末日不可直接当作单一机制对照；后续必要Top50桥接同为181日千万元。动态max_n_drop=5等源参数在首轮保持，数量比例影响单独报告。

## 验证方案

先直接验证无参数no-op、错身份/模式/窗口/非分钟配置失败、只改变executor类及正式runner接线；真实资产fresh-process比较六份完整经济配置，检查181日日历与最后一分钟、冻结预测读回及无训练。旧HMM仅验证未启用本参数时接线无改变，不重跑其实验。

## 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | terminal_replay_consumer.py::extract_terminal_replay | backend/tests/quantevolver/test_qe_terminal_replay.py::test_source_identity_conflict_and_invalid_spec | verified | 无 |
| F-002 | executors/backtest.py；require_terminal_replay_mode | backend/tests/quantevolver/test_qe_terminal_replay.py::test_disabled_is_identity_and_mode_requires_exact_prediction | verified | 无 |
| F-003 | config_composer.py；terminal_replay_consumer.py::apply_terminal_replay | backend/tests/quantevolver/test_qe_terminal_replay.py::test_only_executor_classes_change_and_real_assets_read；同文件ALSTM模板/锚点测试 | verified | 无 |
| F-004 | scripts/qe_terminal_replay_preflight.py::validate | backend/tests/quantevolver/test_qe_terminal_replay.py::test_worker_never_accepts_missing_or_altered_physical_inputs | verified | 无 |
| F-005 | 既有scripts/qe_terminal_interval_executor.py | artifact: tmp/handoff/qe-top20-sw2-longtask-20261030/terminal_preflight_v5/Loop1.native.json；Loop2至6同类证据 | verified | 无 |
| F-006 | 本设计及真实源Composer | artifact: tmp/handoff/qe-top20-sw2-longtask-20261030/terminal_preflight_v5/ | verified_source_pending_runtime | 用户批准按方案直接执行，并要求后端重启由用户执行；六份真实预测完整Composer/双节点fresh-process已通过，合入/8001加载/六账户收益仍单列，未宣称经济验证完成 |

## 多轮审核与结果

第一轮检查固定身份、模式隔离、无参数no-op及资金/股票池/费用不变；10项直接测试通过。真实ALSTM完整Composer暴露模板不能直接safe_load及portfolio锚点需要保留，第二轮将实现收敛到四个标量修改，并增加模板/引用回归。最终11项直接测试通过；无人工降级或忽略失败。

第三轮使用C1六个真实源预测逐seed完整Composer及两节点fresh-process：每份887541预测行原样保留，181账户日、09-30日区间及15:00真实分钟边界通过，末日4913条标签保持NA。预检零训练、零模型推理、零回测。临时检查程序曾直接从TSDatasetH取label产生类型错误，已改为正式runner相同的DatasetH class_casting；这属于预检程序修正，不修改生产runner或源模型。

DESIGN-COMPLIANCE-001：完整覆盖F-001..006，运行加载与收益明确尚待；身份/缺项/错误显式失败；原业务经济字段只允许计划中的资金、K、末日变化，策略和费用不变；不增加审批，用户重启沿既有要求。扩大回归交CI的qe_read_backend及Nightly，不在交互窗口重复整个QE测试。

## 风险与失败模式

末日不等于h20标签成熟；保留NA。provider/manifest/预测或区间不符立即失败，不延长物理日历、不切数据源。预检通过不等于收益改善。现有R实验使用已冻结worker源码，本实现只在独立worktree，不打断它们。

## 生产门禁与交付

DDL/DML、依赖、HMM、数据集均noop；合入后本机8001需由用户重启，随后核对真实加载再提交Top20。远端与WSL执行服务不需重启，worker源码由正式实验包分发。历史HMM六账户完成及分析/入仓收口仍是新实验先决条件，不新增其他人工审批。
