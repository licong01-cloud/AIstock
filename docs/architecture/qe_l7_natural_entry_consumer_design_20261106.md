# QE L7 有限自然新买消费设计

版本：2026-11-06 v1；FEATURE-WORKFLOW-001：F1；模块：QuantEvolver。

## 背景与目标

依据共享协作 revision114、115 与已批准执行的 `QE-L7-TREND-NATURAL-ENTRY` 合同 v2，比较原 S10 十一列与独立 L7 七列在相同入场政策下的实际扣费收益、回撤和上涨参与。资格为信号日原始 return20、return60 均有限且严格大于零。研究标签优势不能代替账户收益；2025H2 的负证据及原 S10 七月 347 单零通过、八月 315 单仅 27 单通过必须保留。

## 范围与非目标

新增独立可选 QE consumer、冻结资格资产构建和准入、正式 Composer 接线及测试。资格不进入模型列，不裁剪训练或预测共同面板。保持原 SELL、动态换仓、费用、仓位和 TAIL_BOOST。旧策略无参数时保持原行为。

不改 HMM、数据集、官方因子或策略目录，不控制服务，不新增第三臂、参数搜索、整仓再平衡、禁重入或持仓延长。后端加载、真实账户、入仓与经济验收分开报告。

## Design Acceptance Index / 设计验收索引

| ID | 合同 | 可观察验收点 |
|---|---|---|
| F-001 | D1、D9 | S10/L7 同政策，资格与模型输入独立；正常 active 绑定及严格复用检查 |
| F-002 | D2、D8 | 完整预测键、日历、原收益值和合法 NA 原因；资产错误失败，不补默认值 |
| F-003 | D3 | 仅适配精确冻结源码，恢复 AST 相同；同状态 SELL 完全相同 |
| F-004 | D4 | 决策开始持仓分类；旧持仓原加仓保留、当天新买尾盘仍受资格限制 |
| F-005 | D5 | 保留合格原买单，仅对实际形成的拒绝订单补位；原排名、配对阈值和排除集 |
| F-006 | D6 | 复权数量转换与原整手算法一致，替代名义预算不超原槽位，不跨槽位分配 |
| F-007 | D7 | TAIL_BOOST 仅消费过滤后的 outer BUY，禁止切换 SUBSTITUTE；原执行源码不变 |
| F-008 | D2、D9 | 上一交易日信号，无重复滞后；明确 producer 附带账户与完整549日最终账户 |
| F-009 | D10、D11 | 审计原决策、阈值、预算、拒绝/替代原因；不把计划订单当成交 |
| F-010 | G3～G5 | 正式参数与完整 Composer 可消费，默认不启用；部署与加载须独立核验 |

## 实施方案

1. `qe_build_frozen_trend_entry.py` 只从已签名研究矩阵和缺失端点证明取值，逐日映射冻结预测键；不运行因子研究脚本。压缩数组使用日期偏移、股票字典、两列 float32 与原因码；所有索引均为逐元素索引，禁止外积广播。缺少证明立即报具体键。
2. `qe_trend_entry_admission.py` 校验清单签名、资产哈希、完整键、日历映射及物理数据绑定。只保留一天查询缓存，资产大小有界。合法 NA 不买；Inf、未解释 NA、缺行、重复、错绑失败。
3. Composer 仅在显式参数存在时适配冻结策略。AST 插入记录原槽位、完整预测键和原决策上下文的钩子，移除这些钩子后必须与原 AST 完全一致。
4. 新策略在原策略及停牌/ST 包装器生成完整决策后处理自然新买。原合格订单和全部 SELL 对象保留；只对被拒绝的已形成订单按原排名寻找合格、可交易、符合原阈值的替代。预算来自原已形成订单的价格、factor、数量和整手限制；不重新计算整仓权重。
5. 通过 `held_before`、原实际买卖配对和 threshold 记录区分新买、原加仓与补仓；当前准备卖出的名称不能补位。无候选属于正常现金结果，输入异常不能伪装成全现金成功。

## 验证方案

本机运行纯准入、适配与 Composer 测试；既有 Qlib worker 环境运行真实策略决策和 TAIL_BOOST 路径测试。覆盖全通过一致性、全拒绝保留卖单、阈值边界、报价及整手、factor 非一、同日新增/已有持仓、重入、重复键/缺行、合法 NA/异常、节假日前一交易日及末日。禁止用假 Qlib 实现充当完整验证。

G4 使用冻结 S10 三份预测和实际 active 对照；L7 producer 完成后验证七列、原源码、同 PIT/PDH、标签/预处理/索引。G5 预算为同任务先 6 后 3：三次 L7 训练及附带账户、三次 S10 最终回放，再追加三次 L7 最终回放。总计九个物理 Loop，六个最终账户，无 S10 新训练。CPU 训练/纯回测可分配两节点；并行上限须按实时资源准入，不能拿配置上限冒充可用容量。

## 风险

补位可能超出原 TopK，而原退出仍依原 TopK，可能产生短持有和成本。资格可能提高现金并错失反转；收益除以仓位不能作为同敞口因果证据。实际共同面板超出研究缺失证明时，QE 必须补充映射证据或报告具体阻断，不要求因子重做已完成的验证。历史行业 as-known 未验证。

## 生产门禁

本功能不写生产目录/数据库、不激活策略包。源码审核、CI、合入与主目录同步不等于 API 加载或实验成功；后端重启仍由用户执行。正式提交前刷新原任务/提交意图及加载状态，UNKNOWN 不重发，不启动竞争观察器。经济结果完成后才做结果评价，不自动晋升。

## Design acceptance matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | scripts/qe_build_frozen_trend_entry.py | backend/tests/quantevolver/test_trend_entry_reuse_contract.py；真实2,682,417键映射；六Loop正式Composer | verified | none |
| F-002 | scripts/qe_trend_entry_admission.py | backend/tests/quantevolver/test_trend_entry_consumer.py；双节点fresh-process输入核验 | verified | none |
| F-003 | backend/services/quantevolver/trend_entry_consumer.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_all_pass_matches_original_frozen_decision；AST恢复证明 | verified | none |
| F-004 | scripts/qe_trend_entry_strategy.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_no_fill_or_position_mutation_and_original_add_kept | verified | none |
| F-005 | scripts/qe_trend_entry_strategy.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_slot_pair_threshold_is_strict_and_not_recomputed_on_filtered_scores | verified | none |
| F-006 | scripts/qe_trend_entry_strategy.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_factor_conversion_and_lot_do_not_inflate_slot_budget | verified | none |
| F-007 | scripts/qe_trend_entry_strategy.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_tail_boost_only_uses_final_outer_buys_and_never_backups | verified | none |
| F-008 | scripts/qe_trend_entry_admission.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_qlib_previous_session_and_last_day_never_use_same_day_signal；双节点末日240分钟日历 | verified | none |
| F-009 | scripts/qe_trend_entry_strategy.py | backend/tests/unified_engine/test_qe_trend_entry_strategy.py::TrendEntryStrategyTests::test_legal_na_is_blocked_and_invalid_factor_is_an_error；原资格与替代资格分字段回读 | verified | none |
| F-010 | backend/services/quantevolver/config_composer.py | backend/tests/quantevolver/test_trend_entry_consumer.py；真实BacktestExecutor提交前截获6个载荷；双节点完整consumer读取 | verified | none |

### 本版源码验收与后续运行分界

2026-11-06：本机19项纯准入/Composer/键集测试通过，既有WSL Qlib环境10项真实策略测试通过；未安装或修改执行环境依赖。测试发现并修复原拒绝资格被替代资格覆盖的审计问题；宽CI暴露的包/脚本双重导入异常类身份问题已修复，并加入fresh-process导入顺序复现。第二轮验证包括6个正式Composer载荷、精确7列L7原源码、完整参考面板、S10三份冻结预测传输SHA，以及双节点物理数据/源码/资格文件核验。真实面板549日、2,682,417键；5,727个合法NA全部对应原停牌证明，未增加未解释缺失。资格压缩资产SHA256为`7911ed3778b18c9e261d1868086161af91a60dcfaa79215748ddbd4a8c25d7c3`。

运行回执保存在忽略目录 `tmp/handoff/qe-l7-trend-entry-contract-20261106/run_v3/`，包括`panel_build_receipt.json`、`composer_receipt.json`、`native_receipt_remote.json`与`native_receipt_wsl.json`。这些验证没有提交实验、训练或回测。上述源码验收不等于API已加载、L7 producer真实面板已产出或最终账户经济验证通过；实际训练后仍按G4核验七列及PIT/PDH/标签/索引，再向同任务追加三账户。后端用户重启、加载核验和G5九Loop运行属于后续明确门禁，不作为已完成经济结果。
