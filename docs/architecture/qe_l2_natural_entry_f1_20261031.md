# QE自然新买损失调权：F1单模块设计

## 背景与批准

用户于2026-10-31明确授权原精确提案 `QE-L2-LOSS-NATURAL-ENTRY-V1` D1—D10及六账户，主线完成并分析后自动接续。批准回执为 `F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/approval_v1.json`，提案SHA256为 `f4dcea94f6e406f681daaea87785c36d3ab4b60c18f17dcf0656bbfa689e0078`。历史“待批准”快照已被本次批准取代。旧B*整仓调仓增加了已有持仓交易；本方案研究只改变原自然新买金额是否有价值。

## 范围与非目标

只增加QE独立可选consumer、精确源码预算接口、版本化准入和测试。默认关闭，旧consumer、旧preset_A、默认策略、HMM、数据集和数据库不变。零训练、零重新推理，不重跑历史15+6账户，不自动晋升。后端重启仍由用户执行。

精确文件范围：本设计；`backend/services/quantevolver/l2_natural_entry_consumer.py`、`config_composer.py`、`executors/backtest.py`；`scripts/qe_l2_natural_entry_admission.py`、`qe_l2_natural_entry_consumer.py`、`qe_l2_natural_entry_strategy.py`；`backend/tests/quantevolver/test_qe_l2_natural_entry.py`、`backend/tests/unified_engine/test_qe_l2_natural_entry_strategy.py`；`tests/aistock_validation/catalog/file_ownership.yaml`。任务收据与编排仅写独立ignored目录，不修改正在运行的主线owner或冻结合同。

## Design Acceptance Index

- F-001 / D1：冻结590b3b20…manifest、三seed原C1 AF、loss75250a22…、C-013/PIT映射b0d4fef1…；1亿元、Top50、125日。
- F-002 / D2：B0与R-entry共用确定性自然交易源码1a3d00c2…；旧B仅旁证，不使用B*整仓执行器。
- F-003 / D3：原目标S按AF排序分前ceil(n/2)/余下两组；J为原自然买入且决策前未持有者，Jg=J∩Sg。持仓不因风险额外买卖。
- F-004 / D4：在price/factor/现金/整手前一次取得全部J原预算b=min(NAV×weight,max_single_order_value)，不从成交反推。
- F-005 / D5：完整131行业loss升序平均秩r=(rank−1)/130，mu=sum(b*r)/sum(b)，b'=b*(1+0.10*(mu-r))。组预算守恒；0.90..1.10倍、原5%和单笔上限；越界失败，零/空/单股/同风险恒等。
- F-006 / D6：decision=t、as_of=prev_open(t)，不重复滞后。完整availability/reason，NA和身份缺项失败，无安全默认值；空J仍保留完整当日风险读回。
- F-007 / D7：原交易价格/现金/费用/整手/约束/尾单流程不变；9月30日真实执行/估值，保留125日。
- F-008 / D8：版本化默认关闭的预算接口，只返回同键预算和审计。关闭不读HMM；不生成目标、退出、订单或写持仓。
- F-009 / D9：新增独立准入、双节点CAS、完整Composer、fresh-process与多轮审核；默认关闭订单恒等、边界与持仓保护测试。
- F-010 / D10：主线完成分析入仓后一个任务B0/R-entry×42/2024/2026六账户；逐seed全期/Q2/Q3/月度收益、指数超额、回撤、费用/换手/现金、订单和预算账本交HMM分析。

## 实施方案

新增 `qe_l2_natural_entry_manifest`，与其他overlay及独立terminal_replay参数互斥，自身使用已批准终值执行器。正式入口只接受冻结预测回放。每节点使用独立版本化ADMISSIONS，原C1 AF及物理绑定保持不变，新增确定性源码身份。Composer按冻结源码SHA注入预算钩子，保留原选股、正常/强制退出、全部下单代码。以AST逆变换证明除预算调用和取值点外原逻辑不变。

接口 `adjust_natural_entry_budgets(decision_date, as_of, ordered_targets, natural_new_buys, base_budgets_cny, nav_cny, original_caps, risk_context)` 不改变任何输入。R-entry按J在S中的原分组计算；B0返回原金额且不打开loss文件。已有持仓正常买卖和现金由原策略/执行器管理。每天写只读预算账本，完整保存131行业availability/reason，记录自然集合、原预算、调整金额、组预算及实际订单，禁止把意图当成交。

R-entry重用原有冻结loss格式/身份验证函数，不复用旧B*交易器或目标日程，不新增HMM公式。两臂根据自身实际持仓独立自然交易；成交后名单和现金路径可以分化。

## 验证方案

对明确数学例、空/零/单股/同风险、非均匀预算、全S分组与J分组差异、负数/NaN/缺项/超限验证；核对同键返回及输入不变。使用真实Qlib策略fresh-process验证同状态下原确定性策略与B0逐目标/订单一致，R-entry已有持仓无额外订单、预算调整仅影响原新买；覆盖空交易日及9月30日。正式资产全量读取、同源AF/映射/125日、双节点CAS/完整Composer另留实际收据。最终源码小矩阵一次，失败仅补相关点；跨模块回归交CI。

## 风险与失败模式

预取整预算守恒不保证实际敞口相同；现金、成交及后续名单会分化，报告是端到端收益差，不冒称每日同名单的纯调权因果。原5%上限触发时明确停止，不clip。历史回溯分类和已消费窗口不是strict as-known或新holdout；三seed不等于独立市场样本。h20未成熟保留NA。无合理收益增量则结束本分支，不搜索κ或额外模型。

## Production gates

DDL/DML、资产覆盖、HMM训练推理、数据集操作、服务控制均为noop。源代码、准入/CAS、fresh-process、运行加载、实验和经济价值分别验收。用户已批准实验，不重复请求合同批准；若需要加载新后端源码，则完成具体可审核交付后通知用户重启。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_l2_natural_entry_admission.py | backend/tests/quantevolver/test_qe_l2_natural_entry.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/preflight_v2/Loop4.native.json | verified | 无 |
| F-002 | backend/services/quantevolver/l2_natural_entry_consumer.py::bind_natural_entry_hook | backend/tests/quantevolver/test_qe_l2_natural_entry.py::test_only_reviewed_source_and_budget_sites_are_adapted | verified | 无 |
| F-003 | scripts/qe_l2_natural_entry_consumer.py::adjust_natural_entry_budgets | backend/tests/quantevolver/test_qe_l2_natural_entry.py::test_money_oracle_and_groups_use_entire_target_list | verified | 无 |
| F-004 | backend/services/quantevolver/l2_natural_entry_consumer.py::bind_natural_entry_hook | backend/tests/unified_engine/test_qe_l2_natural_entry_strategy.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/preflight_v2/Loop1.native.json | verified | 无 |
| F-005 | scripts/qe_l2_natural_entry_consumer.py::adjust_natural_entry_budgets | backend/tests/quantevolver/test_qe_l2_natural_entry.py | verified | 无 |
| F-006 | scripts/qe_l2_natural_entry_consumer.py::NaturalEntryInputs | backend/tests/quantevolver/test_qe_l2_natural_entry.py::test_full_reader_end_date_and_b0_never_reads_loss | verified | 无 |
| F-007 | backend/services/quantevolver/l2_natural_entry_consumer.py::apply_natural_entry | artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/preflight_v2/Loop6.native.json | verified_source | 完整125日、9月30日日频和分钟区间已实测；用户批准实际账户在主线完成及加载后运行，不将预检称为回测完成 |
| F-008 | scripts/qe_l2_natural_entry_strategy.py | backend/tests/unified_engine/test_qe_l2_natural_entry_strategy.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/preflight_v2/Loop3.native.json | verified | 无 |
| F-009 | backend/services/quantevolver/config_composer.py；独立ADMISSIONS | backend/tests/quantevolver/test_qe_l2_natural_entry.py；artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/preflight_v2/Loop5.native.json | verified_source_pending_runtime | 用户批准源码/准入先行，后端重启仍由用户执行；真实双节点fresh-process已通过，8001实际加载仍待重启验证 |
| F-010 | 独立任务编排和批准回执 | artifact: F:/Dev/AIstock/tmp/handoff/qe-hmm-natural-entry-20261031/approval_v1.json | pending_experiment | 用户批准分阶段执行；等待前序主线、实现和加载后运行六账户，不冒称已启动 |

## 审核与验证收据

两轮作者源码审核，不称独立agent审核。第一轮逐项核对金额公式、全S分组、NA、身份和不变源AST，修正测试中金额浮点精确等号。第二轮沿真实Suspend/PIT包装器检查空信号路径，发现强制退出仍可产生卖单；修正为保留原卖单并记录空J完整风险读回，新增对应真实Qlib用例。

Windows直接矩阵20 passed、1项原生Qlib用例因本机未安装Qlib而显式skipped；该用例在既有远端和WSL均通过。最终六份正式Composer分别完成真实冻结AF读取（原文件887541行、正式消费617223行）、映射键等价、125日、末日真实执行区间/NA标签验证。六份fresh-process每份包含4种实际Qlib订单场景：初始建仓、已有持仓/幽灵退出、空信号、空信号PIT强制退出。B0逐订单等于原确定性包装器，R-entry仅调整原自然新买金额，持仓订单不增加。最终收据为上述`preflight_v2/Loop1..6.native.json`，六账户尚未提交。

DESIGN-COMPLIANCE-001：完整源码条款已实现，没有用mock验算代替正式资产读取；NA/身份/预算/金额上限明确失败；原默认、正常及强制退出不变；合同和六账户已批准，不增加重复审批。运行加载、实际回测和经济评价按用户批准顺序保留为独立未完成状态。
