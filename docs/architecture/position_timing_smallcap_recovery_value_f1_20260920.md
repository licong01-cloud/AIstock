# PT-NEXT-026：500万元小市值恢复／继续持有价值实验详细设计

> 版本：v1.1；日期：2026-09-20；Feature tier：F1；状态：`IMPLEMENTED_RESEARCH_COMPLETE_INCONCLUSIVE`。
> 本轮由用户明确要求执行，唯一目标是验证小于50亿元股票中“减仓后何时恢复”能否相对同股长期持有同时改善成本后终值和最大回撤。
> 主蓝图：`position_timing_advice_f2_redesign_20260903.md`；父研究：PT-NEXT-024/025。唯一开发权威为 `docs/standards/aistock_development_standard_v1.5_20260523.md`。

## 1. Background / Goal / 可证伪目标（F-001）

每股使用独立500万元现金账户，无杠杆、无追加资金、减仓现金只留在本股账户。正式基准为同股同起点买入持有（BH）。候选必须同时报告成本后终值差与同频最大回撤改善；正点估计、Oracle空间、验证误差或测试通过均不能替代正式历史回放。

本轮不再扫描卖出阈值、市值阈值、恢复天数或模型族。唯一机制变量是：20%战术减仓已经成交后，每个日频决策点选择“下一交易日恢复”还是“继续保持减仓”。

## 2. Scope / Population / Capital / Execution（F-002）

主人口固定为`SMALL_LT_50B`：当日PIT有效、技术特征ready，且T−1 `db_total_mv < 500000`万元时首次登记；市值未知不填充、不入池。首次入池后持续到统一终点，不因之后市值、ST或退市状态改变而删除。

账户初始现金固定5,000,000元；共同首次买入、费用、100/200股手数、T+1、停牌、方向性涨跌停和终止清算沿用PT-NEXT-024 E0日收盘代理。暂不模拟市场冲击或成交参与率，receipt固定`market_impact_simulated=false`；这只允许研究信号，不证明小市值真实大额成交能力。

训练2018-08-01～2022-12-31，验证2023-01-01～2024-06-30，测试首次执行2024-07-01，统一终点2026-08-31。标签跨越分段边界或尚未成熟即排除并计数。

## 3. Frozen Sell Side / 唯一变化边界（F-003）

卖出侧冻结PT-NEXT-024的候选事件与父GBDT身份：风险guard、连续两日收盘低于`SMA60-ATR20`、盈利状态下R0加速放量事件；父GBDT预测大于0才允许20%减仓。父模型hash、14项特征顺序和0 bps门槛不变。

BH不减仓；`PARENT_FIXED5_GBDT`使用完全相同卖出侧并在成交后第5个后续交易日恢复；`RECOVERY_RIDGE_V1`与`RECOVERY_GBDT_V1`仅替换恢复时机。三者必须共享首次买入、卖出候选、父卖出模型、成本和成交限制。卖出侧漂移即fail closed。

## 4. Recovery Label / Oracle Isolation（F-004）

训练与验证标签不得用父GBDT在其自身训练期内的预测筛选样本，否则会把上游同样本拟合选择带入恢复模型。标签事件来自500万元参考账户中仅依赖当时可见信息的冻结卖出规则候选，并须实际成交；测试期正式策略仍只允许父GBDT接受的20%减仓。对每个标签事件，在成交后第1～20个日频决策点构造单一成熟标签：

`label_recover_now_advantage_bps = 10000 × (wealth_recover_T1 - wealth_wait_5_sessions) / 5000000`

两臂从同一笔卖出所得现金和数量状态出发：立即臂在决策后T+1尝试恢复；等待臂从决策后第5个交易日起尝试恢复；遇停牌／涨停按同一规则逐日重试；两臂在决策后T+21同价盯市。标签记录`label_window_end`及不早于该时点的`label_available_at`。

历史最佳1～20日恢复和受限完整账户Oracle继续是`policy_access=false`诊断，不作为标签、特征或模型选择依据。Oracle最优日不得直接变成分类目标；所有训练标签均是预先冻结的两动作反事实。

## 5. Features / Models（F-005）

恢复模型只使用决策时可见字段：PT-NEXT-024的14项日频市场特征，加`session_since_trim`、`return_since_trim_bps`和T−1 `log1p_total_mv_wanyuan`。缺失或非有限值不填充，样本及运行决策返回typed unavailable。

Ridge为线性校准，浅层LightGBM继续使用父研究已冻结的100轮、深度3、7叶和确定性种子规格；两者使用相同样本、标签、特征和0 bps决策门槛，不调参、不early-stop、不按验证收益择优。模型、训练行、预处理、标签合同均单独哈希。

运行时，模型预测大于0则从下一交易日起尝试恢复；预测不大于0则继续保持80%暴露并在下一决策日重评。成交后20个交易日仍未给出正值时，按冻结风险边界强制恢复；执行失败继续逐日重试，不把失败当模型拒绝。

## 6. Comparators / Statistics（F-006）

正式family固定为两个恢复模型相对同股BH的终值差和最大回撤改善，共4个端点。25交易日移动块bootstrap、5,000次、固定seed；family-wise使用Bonferroni `0.05/4`。`SUPPORTED`要求校正后下界大于0，`NEGATIVE`要求校正后上界小于0，其余为`INCONCLUSIVE`。

相对`PARENT_FIXED5_GBDT`的差、逐股双胜率、恢复等待分布、年度／市场段、U0与ALL迁移均为diagnostic-only，不反向选择模型、阈值或股票池。只有收益和MDD两端点均SUPPORTED才可记`JOINT_SUPPORTED_EXPLORATORY`，仍不自动发布运行模型。

## 7. Outputs / Immutable Archive（F-007）

新namespace为`research/causal_recovery_value_v1`，至少封存request、source audit、父模型引用、标签、模型、trial spec、enrollment audit、逐股摘要、逐日组合路径、成交、正式区间、诊断、receipt和manifest。所有文件逐项hash；exact retry只允许复用相同身份或返回`ALREADY_MATERIALIZED`。

完整对比继续以版本化Parquet/JSON研究artifact归档，不新增数据库／数据仓库，不写正式预测、卡片、alert、registry/current或交易记录。

## 8. Causality / Fail-closed（F-008）

正式测试信息路径固定为`R8 frozen source → as-of features/state → frozen sell model → recovery model → intent → E0 execution → account/outcome`。训练标签路径固定为`R8 frozen source → as-of frozen sell-rule candidate → actual fill → two-action mature label`，不使用父模型预测筛选训练行。Oracle走独立输出路径。训练reader只接受截至训练／验证截止已成熟标签；测试期标签不能参与拟合、标准化或模型选择。

未来扰动、标签越界、Oracle列注入、`shift(-1)`、父模型hash漂移、500万元合同漂移、候选身份漂移、串并行差异、非有限factor或特征均fail closed。自动测试提供冻结实现的有界证据，不宣称全部历史数据绝对无修订或未来盈利。

## 9. Minimal Implementation Plan（F-009）

只新增恢复研究合同／模型／回放／benchmark所需的最少文件及一个聚合定向测试；优先复用R8 reader、市场特征、父GBDT validator、候选触发、账户、费用和E0执行函数。不新增路由、页面、scheduler、worker平台、模型注册表或运行服务。

执行顺序：设计校验 → 小样本标签与账户手算 → 串并行一致 → prepare → WSL 8进程全量run → inspect → exact retry → 结果回填。阴性结果同样完成任务，不沿结果追加参数搜索。

## 10. Verification Plan（F-010）

直接测试覆盖500万元现金守恒、恢复两臂标签、成熟时钟、小市值T−1边界、父卖出模型冻结、Oracle隔离、模型训练专属预处理、恢复强制上限、停牌／涨停重试、artifact身份及无DB／runtime写入。完成ruff、compile、`git diff --check`、F1/F2 validator和required CI。

执行三轮复核：业务／因果；数值／统计；隔离／文档一致性。不存在收益、MDE、人工审批或等待未来交易日的研发门禁。

## 11. Risks / Production Gates / Rollback（F-011）

- 小市值方向来自已观察诊断，本轮属于探索性专用训练，不能伪装成全新sealed holdout。
- 同一减仓事件的多个恢复标签相关；统计以连续账户路径为正式结果，标签行数不冒充独立样本量。
- 忽略市场冲击可能高估小市值可执行性；本轮不因此增加流动性筛选，也不声称生产容量。
- 500万元与旧1,000万元结果不可直接拼接；旧artifact保持不变。
- 回滚仅停止引用新bundle；不删除历史证据。

Production Gates：DDL/DML=`NOT_APPLICABLE`；生产激活=`NOT_REQUESTED`；backend restart=`NOT_REQUIRED`；在线写入=`FORBIDDEN`；`selected_for_live=0`直至正式证据另行支持。

## 12. Design Acceptance Index

| design_item | 设计要求 |
| --- | --- |
| F-001 | 500万元小市值账户的收益／MDD双目标和单一恢复问题 |
| F-002 | 小于50亿元T−1人口、独立账户、E0与成交约束 |
| F-003 | 正式测试父卖出事件／GBDT完全冻结；训练标签不受父模型同样本筛选，只改变恢复时机 |
| F-004 | recover-now对wait-5成熟标签和Oracle隔离 |
| F-005 | 17项因果特征、Ridge／浅层GBDT零搜索 |
| F-006 | 两模型对BH四端点正式family及诊断边界 |
| F-007 | 完整结果比较与不可变文件化归档 |
| F-008 | 成熟时钟、反前视、身份与并行fail-closed |
| F-009 | position_timing内最小离线实现，不建平台 |
| F-010 | 定向测试、WSL回放、多轮审核和CI |
| F-011 | 探索性／容量限制、零生产发布和回滚 |

## 13. Design Acceptance Matrix

`DESIGN_VERIFIED`只表示设计闭合，不表示实现、收益或运行发布完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | §1 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-002 | §2 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-003 | §3 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-004 | §4 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-005 | §5 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-006 | §6 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-007 | §7 | artifact: `research/causal_recovery_value_v1` | DESIGN_VERIFIED | none |
| F-008 | §8 | test: `backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-009 | §9 | `backend/services/position_timing/causal_recovery_benchmark.py`；`backend/tests/position_timing/test_causal_recovery_value.py` | DESIGN_VERIFIED | none |
| F-010 | §10 | test: `python -m pytest backend/tests/position_timing/test_causal_recovery_value.py -q` | DESIGN_VERIFIED | none |
| F-011 | §11 | artifact: `research/causal_recovery_value_v1/receipt.json` | DESIGN_VERIFIED | none |

## 14. Initial Review

1. 目标复核：卖出侧、股票池阈值、模型族与执行不同时变化；正式目标仍是同股BH收益和MDD。
2. 因果复核：Oracle不进标签；标签只比较两个事前冻结动作且按成熟时钟进入训练。
3. 过度工程复核：一个离线研究namespace、无API/UI/runtime；不扫描参数或新增模型族。
4. 隔离复核：只读R8及父artifact，只写timing-owned不可变研究文件；DB、网络行情、其他模块和进程控制均为false。

## 15. Implementation and Formal Result

实现提交为`e4dfe6cf02efe3365833a45c7ff22e46772bc9b8`，诊断闭合提交为`995e6782ecfbbebb451126c90c7bd3356f8fed94`。首次完整bundle `f9ee535e...e46ab`保留为不可变历史；最终权威request/bundle为`a05ca8c1e1534d10ad90018b8384e7961d1debface7823e55ba3b30df41abe76`，manifest SHA256为`f8167847c8fe920fb03bb622344de36d109e19d5e05d194826716bfba9646433`，receipt SHA256为`71f0c1244440d7ad73d8374c5724f6fe26f3a10edc673faf17a71033b4e013cd`。R8 dataset manifest canonical SHA256为`6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283`；父GBDT SHA256仍为`52b27c56494b4e72add859dbfd1555a72531aa96586ad2a8e5001280a9c2503a`。

WSL以8进程完成5,144只源股票、3,135只小市值入池账户、81个chunk和7,492,842条成熟／边界受控标签；训练3,170,603条、验证1,644,192条，其余2,678,047条因分段外或未在相应截止前成熟而不进入拟合。8股串并行audit SHA256为`0dafbfa4da5fd19d6a3c59774530ccd7bdc30a42ecd68f39957b99306c829fd5`，inspect为`VERIFIED`，exact retry为`ALREADY_MATERIALIZED`。诊断修订前后的`labels/stocks/pool_daily/fills`四个Parquet SHA256全部精确相等，证明补报表未改变策略路径。

| 政策 | 组合收益 | 组合MDD | 相对BH终值 | MDD改善 | 校正区间与分类 |
| --- | ---: | ---: | ---: | ---: | --- |
| BH500 | 81.8254% | -27.9964% | 基准 | 基准 | 基准 |
| PARENT_FIXED5_GBDT_V1 | 83.2787% | -26.5994% | +145.33 bps（诊断） | +139.70 bps（诊断） | 非正式family，不晋级 |
| RECOVERY_RIDGE_V1 | 82.0680% | -27.2078% | +24.26 bps | +78.86 bps | 终值`[-1255.01,+510.42]`、MDD`[-16.86,+193.58]`，均`INCONCLUSIVE` |
| RECOVERY_GBDT_V1 | 82.3815% | -27.2147% | +55.61 bps | +78.18 bps | 终值`[-1252.94,+513.55]`、MDD`[-9.89,+193.18]`，均`INCONCLUSIVE` |

Ridge／GBDT验证MAE分别为115.33／116.21 bps，而验证标签均值仅+1.61 bps；浅层GBDT没有优于Ridge。两模型相对BH的终值胜率为51.71%／50.91%，收益与MDD双胜率为45.49%／44.31%，不能用组合正点估计代替跨股稳定性。固定5日诊断的终值、MDD与双胜率分别为58.02%、76.75%和52.89%，但它已被本轮数据观察，不能追认为预注册正式支持。

机制上，两模型都倾向过早恢复：Ridge／GBDT恢复等待中位数均为1日、均值2.24日，完成循环46,190／43,396次，平均每股费用24,445.59／23,489.06元；固定5日中位数5日、均值5.03日、30,255次循环、平均费用18,499.79元。模型平均暴露98.41%／98.47%，高于固定5日的97.75%，所以本轮问题不是长期空仓，而是弱标签下的过早恢复、额外循环和费用侵蚀。13只终点字段无效、9只方向性跌停和4只停牌的清算失败均保留实际状态并按最后已知价估值，没有伪造成交。

结论固定为`INCONCLUSIVE/selected_for_live=0`。当前17项特征和“立即恢复对再等5日”的点式价值目标不值得在已消费测试段继续调阈值、等待天数或扩大模型。固定5日父策略是当前更好的研究候选，但只能保留为诊断，不能发布。下一优先级应改为单一的“小市值专用减仓动作价值”：固定5日恢复不变，只重新训练小市值卖出／继续持有价值，检验通用U0父卖出模型迁移到小市值是否是主要错配；仍只用Ridge与浅层GBDT、同一500万元账户和同股BH。若该单一方向仍无支持，则停止在本技术信息集内堆叠择时模型，后续把主要alpha预算转向独立股票筛选／QE组合，而非继续扫描恢复参数。

本轮`database_read/write`、`market_network_accessed`、`runtime_action_performed`和`service_process_control_performed`均为false；未修改R8、父artifact、在线卡片、registry/current或其他模块，无需后端重启。
