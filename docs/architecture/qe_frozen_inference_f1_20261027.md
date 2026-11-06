# QE冻结权重及已拟合处理器外推设计

???F1????QE??????????????compose???LGB/ALSTM????????????PR/CI??????API????????????????????

?????? `QE-DOWNTREND-VALUE-20261027-V1` ?P3/P4???????????? `QE-ADAPTIVE-24H-20261028-V1` ?E/R2/R3?????????????????????????

## 背景

真实源为 `qe_20261026_154936_d834` 的LGB及 `qe_20261026_161516_59d7` 的ALSTM，各取AF的Loop1/3/5。六份原权重及dataset均已按既有结果收据重新读取并核对字节SHA；dataset包含已拟合处理器，缺的是正式冻结推理入口。现有 `scripts/qrun_limit_minute.py::_run_backtest_only` 会从新配置构造dataset，可能重新拟合处理器，不能用于声称零refit的Q3外推。

源训练2022-03-15..2025-05-29，验证2025-05-30..2025-12-31，h20 purge。原normalizer的可见期截止2025-12-31。AF模型为原11列，PDH仅共同参考；ALSTM为TSDatasetH、20步。权重及dataset的真实pin以主计划 `P3_frozen_source_asset_audit.json` 为准，不把本文日期或模型名当作资产身份。

## 范围

只在QE正式custom-evo入口新增显式可选的冻结处理器消费合同。继续采用原backtest-only的来源任务/Loop及源资产运输，不改变默认路径。新合同须贯穿公开schema、builder、调度、执行封装和worker runner，不藏在model_params中。旧feature_processing拒绝规则保留。

allowed_write_scope（实现前冻结；扩展时先更新此清单）：

- `docs/architecture/qe_frozen_inference_f1_20261027.md`
- `backend/routers/quantevolver_evolution.py`
- `backend/services/quantevolver/experiment_config.py`
- `backend/services/quantevolver/experiment_config_builders.py`
- `backend/services/quantevolver/frozen_inference_contract.py`
- `backend/services/quantevolver/qe_evolution_service.py`
- `backend/services/quantevolver/executors/backtest.py`
- `backend/services/quantevolver/qe_batch_execution_policy.py`
- `backend/services/quantevolver/execution_manifest.py`
- `tests/aistock_validation/catalog/file_ownership.yaml`
- `scripts/qe_frozen_inference.py`
- `scripts/qrun_limit_minute.py`
- `backend/tests/quantevolver/test_frozen_inference_contract.py`
- `backend/tests/quantevolver/test_frozen_inference_runner.py`

## 非目标与边界

不fit模型或processor，不重训、换窗、调分数尺度或阈值，不修改HMM、数据集、默认策略、preset_A、策略包或官方因子指标。不修改RD-Agent服务，也不启动、停止或重启任何API。新字段缺失时旧路径行为不变；新字段存在但不完整时显式失败，不回退为旧backtest-only或完整训练。force_full_train不得覆盖冻结推理合同。

主计划选择了R分支，核心六账户零训练；本可选P4最多六次Q3模型推理及六账户。入口源码或常驻API未就绪时不创建P4任务，不以重训替代。

## 接口和冻结契约

公开字段暂定 `frozen_inference`，类型为严格版本化对象；仅允许 `backtest_only=true`、single-alpha、固定源task/Loop、prediction_replay=false。对象包含源模型SHA、dataset SHA、源训练manifest、目标输入manifest、源模型代码及loader/因子代码SHA、预期模型列序、normalizer可见期、dataset类型及step_len。由控制端读取完成来源的正式配置及真实资产验证，不信任请求自带allowlist或路径。禁止在model_params、strategy_params或runtime_flags中嵌套同名字段绕过公开合同。

权重及dataset从已有worker只读资产API运输，原字节不改写。源资产目录和新输出recorder分离。源训练身份保留旧 `afa9158...`，本轮输入身份为实际解析的 `590b3b...`；须附主计划P0限定2026消费范围的等价证据，不能把同generation当作相同身份。任何身份或pin不匹配都在提交前失败，worker消费时再次核验。

模型代码、源loader及因子公式不能仅凭类名匹配；核对实际字节SHA。新目标raw输入来自正式绑定和原公式，参考面板仍包含PDH，不因标签或候选非空缩小证券集合。模型仅按源拟合schema取原11列；缺列、重复列、顺序不符、未知处理器或未拟合状态显式失败。

worker只调用原已拟合shared/infer processor的transform，不调用fit、fit_process_data或新processor构造。learn processor不得改变推理样本；标签使用既有h20成熟口径独立生成。Qlib的DataHandlerLP默认setup_data是fit顺序，不能直接重建dataset后声称冻结；必须用明确的无fit装载及处理路径，并以poison验证。

实现细化：从原dataset直接恢复handler与shared/infer处理器，调用原loader读取当前工作区原公式生成的raw输入，按源schema排除原PDH参考列，再直接执行原shared/infer调用链；不调用dataset/handler的setup_data或任何learn处理。保持原learn处理器及拟合状态作来源证据，推理时不提供学习帧。更新的是dataset测试segment和TSDatasetH的当前输入calendar。严格核对去除已声明参考列后的原列序，不能任意重排或填补缺列。

公开合同包含原权重/dataset/原H1预测SHA、原workspace loader/因子代码SHA、源模型配置SHA、原AF11列序、原normalizer可见期、源训练和目标输入manifest及dataset类型/20步。生成的完整预测在开始组合回测前，与原H1冻结预测做重叠样本/数值核验，不能用拼接旧预测代替真正外推；完整预测只生成一次。原normalizer/权重状态与源文件hash前后保持一致。

ALSTM???TSDatasetH?step_len=20??fill???????????????19???????????R2???2026-01-05..09-30????09-29?R3???????Q3??????????????????trade/label lag???????????????CPU??????GPU?????WSL?????????Loop?????????4???

H1?????????????float64?atol/rtol?1e-10/1e-8?float32?1e-6/1e-5?????????????????????????????????????? `QE_FROZEN_REFERENCE_NUMERIC_DRIFT_REQUIRES_REVIEW` ??????????????????????

worker输出冻结消费收据：源与目标独立身份、源文件SHA前后值、processor状态指纹前后值、列序、序列步数、输出索引/有限性及唯一性、fit调用数=0、权重加载数和实际推理次数。新recorder保留完整config、pred、成熟label/IC、真实回测/费用和对应收据。完整Q3推理占P4预算，不以重复推理验证为由生成第二套正式实验。

## Design Acceptance Index

- F-001：公开opt-in、来源约束、拒绝隐藏字段及force_full_train覆盖；默认行为保留。
- F-002：原模型、dataset、代码和训练身份pins，目标输入身份独立，运输及recorder隔离。
- F-003：原拟合处理器及列序完整消费，所有fit被禁止，状态前后不变。
- F-004：ALSTM原20步、预热/共同面板/h20成熟及PIT无泄漏。
- F-005：CPU/GPU模型节点与全Loop配额，显式调度、无纯回放误分类。
- F-006：实际消费收据、错误可见、不可静默回退，旧路径定向回归。
- F-007：真实fresh-process验证、三轮审核、CI及常驻API激活分列。

## 实施方案

1. 先将源资产盘点与本设计对应到FI索引；实现严格合同/schema和controller pin核验。
2. 在原来源运输中新增dataset/代码受控资产，执行端按版本化合同注入runner；不更改旧请求或实验生成文件。
3. runner从源冻结对象恢复推理处理器，按原schema装入正式raw数据；隔离旧recorder，记录零fit及不可变指纹。
4. 接通模型节点和配额分类，完成小型直接矩阵及三轮审核修复；检查文档与实现每一项对应。
5. 准备PR前运行feature validator、变更lint/compile/diff和范围检查。源码合入、CI、运行身份、用户重启和P4实际实验是独立状态。

## 验证方案

最小矩阵证明：源SHA错误和缺dataset拒绝；schema错序/缺列拒绝；未知/未拟合processor拒绝；训练或processor.fit poison触发时不能成功；同一冻结状态的诊断输入重放一致；ALSTM真实20步及首日19日上下文与原sampler一致；目标样本、h20与PIT不变；force_full_train及嵌套字段不可绕过；GPU类不是回放4并行；旧无opt-in路径保留。

真实源消费在Qlib独立进程验证，单独记诊断probe与正式P4全窗口推理次数。只用mock不能满足FI-07。源码可读或pytest成功不能证明常驻API已加载。广泛模块回归交CI/Nightly，不在主计划中重跑所有实验。

## Design Acceptance Matrix

????F-001..F-007???FI-01..FI-07???????????????????????

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | frozen_inference_contract.py?ExperimentConfig???schema?builder?executor/????/???? | backend/tests/quantevolver/test_frozen_inference_contract.py | passed | none |
| F-002 | source_assets?_validate_frozen_source_metadata?execution_manifest??source_model?? | backend/tests/quantevolver/test_frozen_inference_contract.py?artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/P3_preflight_v2/receipt.json | passed | none |
| F-003 | scripts/qe_frozen_inference.py::consume/prepare/finish??shared/infer???fit?setup_data?? | backend/tests/quantevolver/test_frozen_inference_runner.py?artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/p3_alstm_probe_v3.json | passed | none |
| F-004 | ?TSDatasetH/20????????????raw???Fillna??feature | artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/p3_alstm_probe_v3.json?backend/tests/quantevolver/test_frozen_inference_runner.py | passed | none |
| F-005 | qe_batch_execution_policy.py::is_pure_backtest?executor???? | backend/tests/quantevolver/test_frozen_inference_contract.py::test_opt_in_is_explicit_and_inference_is_not_pure_replay???compose???? | passed | none |
| F-006 | _run_frozen_inference??????results_only???????retry???? | backend/tests/quantevolver/test_frozen_inference_contract.py::test_completion_rejects_refit_state_drift_or_shortened_window?backend/tests/unified_engine/test_qe_prediction_replay.py?backend/tests/unified_engine/test_qrun_recorder_isolation.py | passed | none |
| F-007 | ?????????????????? | backend/tests/quantevolver/test_frozen_inference_runner.py?artifact:tmp/handoff/qe-adaptive-24h-20261028/run_v1/P3_preflight_v2/receipt.json | passed | none |

?????dataset???????loader/?????????API??????????????????POST=0???LGB?WSL?ALSTM???????????2026-06-29/30??????9,786??????H1?????????0??????????????ALSTM??sampler?20?11???????????????????????API??????helper?????????????

??????????????????????????????summary????????????????????????Qlib??????????RobustZScoreNorm???????GeneralPTNN.dnn_model??????????????????????????????????????????????????????agent????????????CI????API?????


## 风险及失败模式

最主要风险是只复用权重却重新拟合normalizer，或先构造dataset再阻断fit，造成假冻结。其次是ALSTM上下文丢失、列序错位、源模型代码漂移、源/目标身份合并、force_full_train覆盖，以及把GPU推理当成纯回放而超额调度。均通过合同、提交前核验和worker消费证据闭合，不加独立经济阈值审批，不以更长超时或默认处理器掩盖失败。

## 生产门禁

production_ddl_gate=noop；不写生产资产或数据集。开发使用隔离worktree，运行根目录源码不在本工作树修改。提交与合入按授权和feature门禁完成，常驻API加载由用户单独重启并验收；本计划不控制服务。关闭opt-in即可回到原路径，不覆盖旧资产或冻结身份。

Final worker SHA256: `98913ecd72cd3140fd99e0ef8e55665a6acd6184b718b14b9cd9c1ad248d977d`; direct contract/runner tests: `35 passed`; related replay/isolation/default-transport matrix: `69 passed, 1 skipped`. No formal inference, account, service control or production migration was performed by these probes.
