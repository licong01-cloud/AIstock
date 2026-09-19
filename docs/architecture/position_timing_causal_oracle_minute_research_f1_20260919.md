# PT-NEXT-024：因果择时、受限 Oracle 与分钟执行验证详细设计

> 版本：v1.0；日期：2026-09-20；Feature tier：F1；状态：APPROVED_FOR_IMPLEMENTATION。
> 用户已于2026-09-20确认按本详细设计启动连续实施；设计批准不代表代码、训练、回放或收益结论已经完成。
> 主蓝图：[F2 v2.50 §9.27、§10.23](position_timing_advice_f2_redesign_20260903.md)。唯一开发权威：`docs/standards/aistock_development_standard_v1.5_20260523.md`。
> 设计核对基线：`21932927ef2e5b9c2cf7ea42f2440d08a88e6625`。`DESIGN_VERIFIED` 仅表示设计条目闭合，不表示实现、无泄漏证明或超额收益已完成。

## 1. Background / 终极目标与本轮问题（F-001）

目标是让特定自选股或持仓股在独立现金账户中，同时获得高于同股长期持有的成本后终值和更低最大回撤。降低仓位造成的机械性降回撤，不等于择时有效；Oracle 事后赚钱，不等于可学习或可实盘。

PT-NEXT-023 的既有 R8 证据显示：U0 同股 BH 合成收益约 142.03%，S1/S2 约 92.85%/74.96%；组合曲线最大回撤约 -34.83%/-29.44%/-26.96%。S1/S2 平均暴露约 81%/72%，有仓天数约 99.8%，减仓至恢复中位数约 17 个交易日、均值约 57–61 日。它表明持续核心仓已缓解长期空仓，但降回撤伴随明显收益牺牲；尚不能用组合回撤代替逐股回撤，也不能把筛选带来的 BH 差异当择时 alpha。

本轮依次回答：

1. S1/S2 的损失主要来自错误卖出、回补过迟、交易费用还是执行价假设？比被动保留现金是否更好？
2. 在相同交易限制下，原信号附近到底有多少可利用的卖出／回补机会？
3. 固定缩短战术仓离场周期，以及只过滤不划算的减仓，能否改善收益／回撤的联合结果？
4. 改善在分钟成交代理下是否仍成立，能否被只看过去的轻量模型识别？

这是有限假设的机制实验，不承诺找到最优策略，不按结果无限搜索参数，不把本轮当成未被观察过的封闭测试。

## 2. Scope / Non-goals / 授权和隔离（F-002）

本轮实现边界为 `backend/services/position_timing/`、其直接测试及本设计／主蓝图的进度更新；只写 timing-owned 新研究 artifact。只读复用交易日、PIT、涨跌停、费用、Qlib 单位与现有技术特征实现，不改共享默认值。

不改 QE、HMM、Selection、Advisory、Paper、MiniQMT、local_data，不接选股信号／新闻 Agent，不新增数据源、数据库表、服务、调度器、平台或在线 API。不得写现有 registry/current 路由、推荐／预测仓库或旧研究产物；不激活 profile、不修改 R8、不操作数据库或服务。研究在 WSL 子进程执行，不需要后端重启。

用户确认后的授权是：按本设计连续开发、研究、多轮审核修复，通过必要测试／CI 后提交合入并清理本任务资源。后端重启仍仅用户操作。若需其他模块修改，只提交需求给用户转发；不跨界修复。研究阴性、MDE 偏大或等待新交易日均不是停工条件。真实输入／因果合同错误只阻断受影响的结果，不新增人工审批链。

## 3. Contracts / 冻结输入与时间边界（F-003）

### 3.1 数据身份

| 输入 | 冻结值 |
| --- | --- |
| R8 Windows 根 | `X:/AIstock_dataset_candidates/backtest_dataset_candidates/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate` |
| R8 WSL 根 | `/mnt/wsl/aistock-qe-data-v1/releases/20260831-qe_hmm_full_v2-direct-20260918-r8-candidate` |
| candidate manifest 文件 SHA256 | `07db01d8c24108ebfe2b85bc42f18f33c77fb0e09451999d7520faa08ebe1b18` |
| candidate manifest canonical SHA256 | `6bb6096aada59541f58d05e1c44d5dabd8838661936d98539adcb79d7ac39283` |
| minute calendar SHA256 | `11be357f1fc5e481c61911bf7ff6b39d9552ba36423b0ccff05fc7ebc690fb8c` |
| minute meta SHA256 | `33bee6c154e9aba445fbdb3896defb9156b385f86fd99bd73ff24b931f49ccbc` |
| suspend_d parquet SHA256 | `7f1895fe3c3a025708c1afa525d17e594f536180a86d6a9cf22b3c25d87bee3f` |
| PT-NEXT-023 父 bundle id | `3f8667817971ae7b10a867d28ac2b1a39d7b635ac49b2e82e0eee1e565c4462c` |
| 父 manifest canonical SHA256 | `1b352b7f8d25cb501df2b48fc4becf0cce8693240d3d33e069f05ce2313d2794` |
| 父 manifest 文件 SHA256 | `6e6c5f96a066c8a951d949aec34cc0b54b24ac954fce0c12771161ed81ec540b` |

父 bundle 位于 WSL timing root 的 `research/core_tactical_proxy_v1/bundles/<id>`。新 namespace 为 `research/causal_timing_v1`，不覆盖父产物。prepare 固定全部实际使用的日线、分钟字段、指数、PIT、代码及依赖身份，不能只记录上述顶层 hash。

设计时已核对 R8 WSL manifest 身份及分钟日历端点；尚未执行本轮全股票分钟字段覆盖扫描。日线范围为 2018-08-01～2026-08-31，分钟日历范围为 2024-01-02～2026-08-31。不能用分钟日历存在推断每股每天每字段完整。meta 中保留 r3 来源路径属于沿袭元数据，不允许据此切回 r3。

沿用纯 Qlib 信号研究：adjusted OHLC = raw × factor，adjusted volume = raw volume ÷ factor；下游调整后视图 factor=1，原始交易视图另存。单位为虚拟复权单位，不是券商股份。不重开现金分红、配股缴款、股份到账或公司行动公告清算。

### 3.2 固定时间切分

| 用途 | 时间／规则 |
| --- | --- |
| 原策略归因 | 只读 PT-NEXT-023 原窗口产物，保持其原始起点，不与下行公共窗口终值直接相减 |
| 模型训练决策日 | 2018-08-01～2022-12-31；特征窗口不足自然记 NOT_READY；标签必须在 2022-12-31 20:00 前成熟 |
| 模型验证决策日 | 2023-01-01～2024-06-30；标签须在 2024-06-30 20:00 前成熟；只报告，不调参或重新选阈值 |
| 公共回放窗口 | 首个执行日 2024-07-01；最后普通决策日 2026-08-27；最后普通执行日 2026-08-28；2026-08-31 统一终值／终止清算 |
| 决策时点 | 全局交易日 T 的 20:00；最早执行 T+1；首次入场可用 2024-06-28 20:00 的信息 |
| 模型逻辑生效 | 2024-07-01 开盘前；一次训练，全测试段固定权重；真实 artifact_created_at 另记，不伪装成历史文件创建时间 |

训练／验证分界核对自 `quantevolver/config_composer.py::RDAGENT_DEFAULT_DATA_SPLIT`。上述测试结束日是本设计明确提出的 R8 公共窗口，不宣称已经与某个 QE 正式 request 完全一致：本次未取得该 request 的可核验身份。后续若取得，只读报告一致性／差异，不静默改本轮日期，不等待 QE 才研发。

所有加减天数使用冻结全局交易日，不使用自然日或个股非停牌行数。全历史数据仍用于训练、预热和旧结果归因；公共窗口限制是分钟与模型比较的明确 estimand，不冒充原长窗口复验。

## 4. Population / 账户、股票池与基准（F-004）

源人口保留 R8 的 5,144 个代码及其历史生命周期；按当时 PIT 资格决定新买入，不按今日 ST、退市状态删代码。每股独立初始现金 1,000 万元，无杠杆、无追加资金、无跨股调拨，现金利息为 0，不模拟市场冲击。

主研究人口为 U0：使用 T−1 `db_total_mv` 的 50～500 亿元范围、当时 PIT 资格、共同技术特征 ready，在公共窗口内首次满足时登记入场，T+1 下单。所有候选和 BH 共用此入场决定；暂不可成交均按同一既有首次买入规则重试。入场后不因退出筛选、ST 或退市风险删除账户，也不重置资金。不能把旧研究最终入选的 4,360 股反过来当 2024-07-01 已知股票池。

本轮不重复优化 U1/U2，不把 bak 增长代理喂给模型；避免把缺失财务 PIT 和模型收益混在一起。U0 外的代码保留未入选／UNKNOWN 原因和数量。这是已明确缩小为一个筛选假设的机制实验，不宣称对全部 5,144 股完成了策略有效性验证。

共同基准：

- `BH100`：同股、同日起点、可用现金尽量买满，持有至统一终点。
- `PASSIVE80`、`PASSIVE70`：首次只用初始预算的 80%／70% 买入，其余现金不动；不是每日维持固定权重的再平衡组合。
- `LEGACY_S1`、`LEGACY_S2`：原规则身份不变，在公共窗口重新初始化账户回放；原长窗口结果仅作历史上下文。
- 全 U0 和逐股：沪深300价格指数背景；指数成员分组：沿用父研究的五类 PIT 指数池与各自价格指数，不把所有组都配沪深300。

逐股主比较为 candidate 与 BH；被动现金基准用于解释降暴露，指数只作背景，不用于宣称择时优于 BH。指数分组按首次登记入场日已知成员关系固定分层，另报成员变动人数；不按期末成分回溯选股。日动态成员视图如保留，单独命名，不冒充固定独立账户组合。报告引用实际 pool→index code 映射及 hash，不能靠名称猜代码。

已核对映射：csi300→000300.SH、csi500→000905.SH、csi1000→000852.SH、star50→000688.SH、star100→000698.SH，来源为 `pattern_close_cash_report.py::INDEX_CODES`。主视图固定为“入场时成员分层”；父研究的dynamic／annual_fixed视图只作为附加兼容报告，不混写其成员定义。最终入选账户的合成曲线是事后固定研究cohort，不是可在窗口开始提前买齐的投资组合；未入选代码总量和原因另列。

费用沿用净佣 0.85 bps、每父订单最低 5 元，规费分项另加、卖出印花税另加的冻结组件；PER_PARENT_ORDER 标 BROKER_UNVERIFIED。数量、现金、可卖单位、板块最小量及递增量使用现有规则，不写成统一“买100、卖任意”。规则快照与 code hash 一并绑定；factor 变化不产生账户现金或真实股份。

## 5. Experiments / 本轮有限策略规格（F-005）

除基准外，只新增下面三个候选；不把 Oracle 最优阈值自动加入候选。

这里仅有一个新动作模板CYCLE5；无过滤版本是机制对照，Ridge／GBDT是同一动作的两个预注册函数族，不是三套不同止盈参数搜索。这是主蓝图“选择一个恢复或动作价值方向”的本轮具体提案，须与全文一起由用户确认。

| policy_id | 减仓决定 | 恢复决定 | 目的 |
| --- | --- | --- | --- |
| `CYCLE5_UNFILTERED_V1` | 沿用 S1 原因信号与80%核心 floor，不作模型过滤 | 成功卖出的第5个后续全局交易日执行回补 | 测试回补拖延机制 |
| `CYCLE5_RIDGE_V1` | 相同候选信号，仅预测净动作价值 >0 bps 才减仓 | 与上行完全相同 | 线性可学习性 |
| `CYCLE5_GBDT_V1` | 相同候选信号，仅预测净动作价值 >0 bps 才减仓 | 与上行完全相同 | 有限非线性参考 |

选择 5 日是事前提出的短周期机制，不声称它由 Oracle 求得或历史最优；本轮不比较 3/5/10 多个可交易阈值。S1 原信号的代码、参数和状态顺序以冻结快照为 authority，禁止从 R0／R6 臂名解释语义。

规则细节：

1. 成功初始买入后维持 S1 的虚拟单位 anchor 和80% floor；减仓不超过 anchor 的20%，lot rounding 后不得跌破 floor。模型过滤同时作用于 S1 的普通、趋势和研究风险减仓；**不修改在线风险规则**。
2. 卖出未成交不进入 recovery，不把“发信号”当成交。事件仍按原 S1 的失败／重试及边沿状态规则处理，不人为消耗一次信号。
3. 成功卖出日 S 的第5个后续交易日是预定回补日；此计划在 S 收盘后即可确定，不需要等该日收盘生成信号。只用账户实际可用现金买入，不要求原趋势重新成立；费用导致恢复不到旧单位数时接受真实差额。
4. 回补因涨停／停牌失败时，每个后续交易日前提交同一“可用现金恢复”政策，直到成交或终点；不得未来挑最低价、借钱补齐或删除失败 episode。待恢复期间不再卖出战术仓，保持单一在途 cycle；回补成功后重置 anchor 并至少等待下一决策日。
5. 距普通执行终点不足5个交易日时不新开 cycle；已有未闭合 cycle 保留并按统一终点处理。未回补、未清算和数据不可用分别计数。
6. 每个风险／趋势／R0边沿只产生一次模型判断：模型拒绝会消费该候选episode并清除相应pending状态；模型缺失单列 `MODEL_UNAVAILABLE`，也不降级成无过滤卖出。若模型已同意但市场未成交，则不消费边沿，后续合法交易日按同一episode重试。R0回落为false后才可形成新的上升边沿。

模型只判断“是否执行同一固定动作”，不额外预测仓位比例、买回日期、分钟点位，也不同时搜索止盈阈值。三候选均有机会被证伪，不要求输出一个赢家。

## 6. Oracle / 上界、归因与标签隔离（F-006）

四类 Oracle 输出独立 `oracle/`，不可由 policy feature loader 打开：

| 层次 | 计算合同 | 允许的结论 |
| --- | --- | --- |
| 原信号事件 | 固定旧 S1／S2 实际减仓起点与账户；枚举1～20个后续交易日的可行回补，含不减仓选项；重放费用、T+1和不可成交 | 该事件附近是否有机会、原回补错过多少；重叠事件不得相加成账户收益 |
| 卖出／恢复拆分 | 原卖出+原恢复、原卖出+固定5日恢复、不卖出；保持相同初态和评价日期 | 区分卖出判断与现金闲置损失，不把不同起点差归给模型 |
| 受限全账户 | 公共窗口每20个全局交易日一个事前固定决策格点；最多一次20%减仓和一次恢复，另含BH；穷举格点路径 | 仅是该有限集合的精确最优，不是允许任意多次交易的全局上界 |
| 执行诊断 | 固定方向、数量、日期，回看当日可行分钟价格范围；不反向改变订单 | hindsight execution opportunity，不能用最优分钟价作实盘成交 |

受限全账户同时输出最大终值、回撤不超过20%／30%时的最佳可行终值；不存在可行路径则标 `NO_FEASIBLE_PATH`。格点从该股共同初次买入后的首个决策日起每20个全局交易日生成，到最后普通决策日止；每个卖出格点只与其后合法买回格点配对，实际执行仍为T+1。包含不交易BH及只卖未回补至终点的路径。格点集合、回撤定义、初态、实际穷举数和未决终值均可复核。不把这一单 cycle 上界用于声称多 cycle 模型“捕获率超过100%”或仍有确定改进余量。

模型标签不是全历史 Oracle 动作。训练日 T 的标准化反事实是：以 T 已知raw价格和合法数量规则构造初值合计1,000万元的已持有头寸及取整后残余现金，再转换为Qlib单位；买入成本视为沉没、已满足T+1。分别执行“不减仓”与上述20%减仓／第5日回补的固定动作，在 T+21 收盘按同口径盯市，得到：

`label_net_action_value_bps = 10000 × (wealth_action - wealth_hold) / 10000000`。

减仓执行日为 T+1；数量必须在 T 由已知价格／factor 和规则确定预算或虚拟单位上限，实际交易撮合才解析执行价／factor并约束现金／lot。未成交卖出标签为真实无动作差额；未能回补则保留现金与剩余股票盯市，不删除“难成交”样本。T+21 缺失终值的非真实停牌情况为 unavailable，不填零。此标签是有限期动作价值，不是完整账户最终收益。

特征只用 T 的市场状态，不带未来 label／Oracle。该标准账户与实际自然复投账户的规模／状态不同，必须报告成本比例、信号原因、样本覆盖和回放状态偏移，不能宣称消除分布偏移。训练样本取原 S1 候选减仓条件在标准账户上的因果实现，净盈利前提不从虚构买入成本推算：用现有参考 S1 历史账户在 T 的状态决定事件资格，标准账户仅用于统一标签量纲。样本身份绑定 reference-policy hash 与 reference-state hash。

参考S1从2018年起按当时U0首次入场独立运行；训练资格只看当时状态，不借测试期最终入选名单。只对有可执行战术仓的候选事件生成标签，统一量纲后不重新判断虚构入场收益。每个label样本只使用其自身窗口，不把邻近事件的相互重叠收益累加；正式绩效只能来自连续账户回放。

`label_window_end` 为实际用到的最后行情时点，`label_available_at` 不早于该时点和所有支持输入的可用时间。全账户全历史 Oracle 最早在末日之后成熟；永远不被当作 T 已知的方向标签。Oracle 计算可先完成，但模型训练 reader 按成熟时间隔离，而非“文件既已生成就可用”。

## 7. Model / 最小模型合同（F-007）

仅使用 `action_value.py::market_features(..., information_block=CORE_INFORMATION_BLOCK)` 的14个既有价量／沪深300相对特征，按其现行有序列表和计算 spec hash 冻结；不使用旧9项账户状态、bak增长、HMM、QE alpha或新的因子搜索。来源 availability 单独核验，函数本身不能创造 PIT。

本轮feature adapter按现有 `action_value.py::market_features` 的真实接口传入raw CNY OHLC、raw volume及source factor，由该函数仅为收益距离内部乘factor；输入基准单独绑定新 `feature_input_basis_sha256`。原S1的 `build_research_features` 同样接收raw bars，但在自己的Qlib视图适配器内转换，两个接口不能互换。执行reader始终使用raw价格／量，且与模型特征输入分别标记身份。

两模型同一标签、同一训练行、同一过滤阈值0 bps；每日决策，模型不滚动调参，不按验证收益挑一个再隐去另一个。

- Ridge：训练集逐特征 mean/std 标准化，零方差 scale=1；目标原始 bps，带不惩罚截距；最小化 `mean((y-pred)^2) + 0.001 * sum(coef^2)`，确定性线性求解。无目标截尾／事后剔除亏损事件。
- LightGBM：沿用仓库已支持的4.6.0；regression/l2，100树，learning_rate=0.05，max_depth=3，num_leaves=7，min_data_in_leaf=200，feature_fraction=1，bagging_fraction=1，bagging_freq=0，lambda_l2=1，seed=20260919，deterministic=true，force_col_wise=true，num_threads=1；不使用 early stopping、超参搜索或测试集重训。
- 所有有效特征必须有限；缺行情导致窗口不完整则 NOT_READY，不用未来填充或默认值生成交易。训练不补全未知 source availability。标准化仅训练数据拟合。
- 训练采用全体符合条件且已成熟的历史事件；同一(symbol,T)一行；不因测试期收益／最终入池身份删行。训练、验证、测试分别输出纳入／排除原因计数。

旧 `action_value_model.fit_local_model` 绑定双头任务与固定 policy identity，不是本单头标签的可替换入口。复用 LightGBM 环境、训练时点检查及无pickle序列化方式，新合同写独立最小模型适配器；不得借旧模型身份保存新目标。

模型预测 `>0` 只表示估计动作价值为正，不是个股胜率或已获统计支持。所有缺特征／预测异常情况显式标注不执行模型减仓；不静默换成规则减仓。模型失效与策略选择 HOLD 在报告中分开。

数值合同：金额／费用／预算使用Decimal并固定舍入规则；模型计算float64，比较阈值仍严格0，不用临时epsilon改变动作。串并行排序及相同环境下的序列化结果要求一致；跨平台浮点诊断允许的数值误差在request中冻结，但不能豁免意图方向、成交状态、数量或结果身份变化。

## 8. Minute execution / 三个执行视图（F-008）

分钟线本轮只改变成交评估，不产生新信号、不训练2018年并不存在的分钟特征。冻结三个视图，不能看到结果后选择最有利窗口：

| 视图 | 规则 | 角色 |
| --- | --- | --- |
| E0 `DAILY_CLOSE` | 保留 T+1 日线收盘撮合模型 | 与旧研究可衔接的主研究口径 |
| E1 `MINUTE_CLOSE_PROXY` | 预先确定的 T+1 收盘订单，用15:00分钟条的raw close／volume／方向限价审核 | 检查日线收盘与分钟收盘一致性；并不等于取得收盘集合竞价成交证明 |
| E2 `SCHEDULED_1000_PROXY` | 前一决策日已经决定订单；使用 T+1 标记10:00分钟条的raw open作为该预定分钟首价代理 | 一个预定较早执行窗口的敏感性验证，不挑最好分钟 |

E2 不读取10:00的high/low/close生成本条订单；bar volume只在撮合结果判断中验证交易存在，不反向用于决定是否发单或数量。标记10:00不被臆断成精确10:00:00毫秒：分钟时间标签对应的区间语义写入输入审计；无法证明真实撮合／队列时保留 `PRICE_PROXY_NOT_QUEUE_PROVEN`。本轮不声称分钟OHLCV还原了真实成交优先级。

预定价格必须正数且有限，停牌禁止交易，买入触及涨停／卖出触及跌停时保守记未成交，不因出现成交量就假定己方能排到。缺失规定条或字段不能改取未来最近价、日线合成价或其他时间点。只核验实际需要的条和持仓估值支持数据，不要求为了单个订单读取全部241条。

无市场冲击假设下，一次政策订单模拟一次完整可负担成交，不把分钟成交量当可无限证明的容量。成交价格／数量合法性、真实可成交性假设和代理口径同时披露。日／分钟factor归一化交叉核对，禁止重复复权；分钟暂停时段如未获可靠字段，只能保守无成交并明确原因，不由 `suspend_type=S` 推断全天停牌后的精确分钟恢复。

先做同一冻结订单的 E0/E1/E2 逐腿价差诊断，再分别运行完整连续账户。后者的现金、数量、费用、下次信号状态会发生反馈，不能将逐腿差额简单加到旧账户收益上。BH及被动基准的初始买入、终止清算也使用相同执行视图。

终点统一在2026-08-31发出预先确定的终止清算订单；E0/E1按收盘代理，E2按10:00代理，清算后现金保持到收盘。主终值为“终止订单执行后现金 + 尚未卖出单位按当日收盘估值”，费用只扣实际成交腿；若未全部卖出必须标 `RESIDUAL_POSITION_NOT_CLEARED`，另列可用现金、残余估值和可清算子样本，不能称全部到账现金。仅保留无终止交易的影子盯市值作清算成本归因，不能替代主终值挑结果。

已证实停牌且无新交易价格的估值可沿用最后已知有效价格，并记录估值日期／陈旧天数；不能对无原因数据缺口或退市后未知价值无限forward-fill。无法可靠估值的路径标 `VALUATION_UNAVAILABLE`；全cohort主曲线不可伪称完整，另列明确配对子集及覆盖率。每日MDD沿各视图实际账户（包括终止交易）在收盘采样，不冒称分钟最大回撤。

## 9. Causality / 不读取未来信息的实现与验证（F-009）

信息路径必须是：`frozen sources → as-of feature/state view → policy → intent → execution simulator → account/outcome`。Oracle／labels 单独路径，只允许成熟标签进入训练；策略函数不接收全历史DataFrame或oracle对象。

每次决策记录 `decision_as_of / max_feature_available_at / source_ref / feature_spec_sha256 / model_sha256 / model_effective_at / intent_sha256`。训练记录 `training_cutoff / max_label_available_at / preprocessing_fit_end / training_rows_sha256 / label_contract_sha256`。物理生成时间和历史模拟可用时间分别记录。

必须满足：

1. `feature_available_at <= decision_as_of`，训练标签成熟不晚于 training_cutoff，模型逻辑生效不早于其全部拟合／选择输入可用时间。
2. X(T) 使用 T 时已可用的来源版本；不能在未来训练时拿修订后财报回填成历史已知。没有版本证据的输入标记 availability assumed/proxy，不宣称严格 PIT。本轮排除 bak财务输入也不自动证明其他数据严格PIT。
3. 时间切分剔除标签跨界行，不随机打散同股相邻时序，验证和测试不参与标准化／缺失处理／阈值选择。日期分界本身不能代替逐行成熟审计。
4. 成交模拟可以读后续行情来判定已提交订单的成交，策略决策不能读它。T收盘后才计算的信号不得在T收盘成交；预先排定订单使用未来执行价只是撮合结果，不是未来输入。
5. 对复权序列测试恒定缩放不改变无量纲信号；对历史factor重述核对实际来源身份。不能用缩放不变掩盖真实历史重述。

反前视测试包含：截断未来和扰动未来后重建特征、训练、预处理及决策，历史输出必须不变；标签越界必须拒绝；故意注入Oracle列和 `shift(-1)` 特征必须被白名单／时序验证或变形测试抓住；同条分钟结果不能反向改变已经封存的intent；订单不能透支、重复花现金或卖出当日新买数量。

完整截断对照先用确定性代表样本逐步重建验证；正式全量对所有行执行availability/hash/白名单断言，并按冻结抽样清单重建复查。测试范围写入receipt，不宣称对所有代码路径数学证明。任何实质泄漏使受影响bundle标 `INVALID_LOOKAHEAD`，修复后新request重跑，旧产物保留。数据真实PIT、程序因果测试、研究反复试验偏差分三栏披露；均通过也不保证实盘盈利。

## 10. Outputs / 比较、统计与完整性（F-010）

所有新旧候选在同一公共窗口、相同U0入场记录、相同执行视图横向比较；旧长窗口数字只放背景列。必须报告：

- 逐股：BH/候选/被动现金基准成本后终值、收益差、每日MDD、MDD改善百分点、换手、费用、暴露、有仓天数、条件暴露、最长空战术仓段、回补等待分布。
- 联合成功率：`candidate_terminal > BH_terminal` 且 `candidate_MDD_magnitude < BH_MDD_magnitude`，两条件严格成立；另列仅收益改善／仅回撤改善／均未改善与 UNKNOWN。MDD统一用正的损失幅度，展示负数回撤时显式转换。
- 组合：独立账户初始等资金净值之和的合成曲线、该曲线MDD、逐股MDD分布；不会把逐股MDD平均数称组合MDD。晚入场账户此前保持初始现金，分母不因已知未来可交易而漂移。
- U0及五类指数分组、对应指数收益；年度、行情涨跌段和持有期分层均 diagnostic-only，不用分层选择策略或阈值。
- 全部 source/未入选/未知/未成交/终值不可用/不能清算人数；有效配对数与总分母同时显示。故障、停牌和退市不能从收益或MDD统计中静默消失。

父研究报告可能使用“每日等权收益连乘”的合成指数，本轮主汇总为“独立账户净值求和”，二者不是同一再平衡口径。公共窗口基准必须一起重算；兼容展示旧汇总时单列aggregation_id，不用汇总方式差异冒充策略改善。分组成立也只说明该研究cohort，不证明一开始即可预知其最终成员。

预注册正式比较 family 为 E0 下四项：CYCLE5_UNFILTERED vs BH、Ridge vs BH、GBDT vs BH、CYCLE5_UNFILTERED vs LEGACY_S1。模型相对无过滤、E1/E2、被动现金基准和指数分层为诊断，不挑诊断胜者来替代正式比较。

每项报告两个经济端点：同账户等初资合成净值终值差、同曲线MDD损失幅度改善；四项×两个端点共8项同时区间。采用共同日期的配对向量移动块bootstrap，block=25交易日、5000次、seed=20260919；全部账户／比较器共用日期抽样索引，重建净值和MDD；Bonferroni总alpha=0.05。输出未校正与校正区间。它估计观察窗口上的时间重抽样敏感性，不证明跨市场独立泛化；非平稳、异步入场、MDD路径依赖另作限制说明。逐股联合比例主要是描述统计，不把5144个相关股票当独立样本做显著性。

`economic_threshold_bps=0`；终值和MDD两个校正下界均>0才记本窗口 `JOINT_SUPPORTED_EXPLORATORY`；仅一个改善须直说收益／风险权衡；区间跨0为INCONCLUSIVE，负向上界<0才可称该端点NEGATIVE。不以MDE拒绝开跑，不把功效不足当没有效果。复用2/3父订单敏感性标注成本假设，不按敏感性重新选策略。全部仍为研究结果，`selected_for_live=0`。

## 11. Architecture / 复用清单与新增面（F-011）

### 11.1 已核对的 Allowed APIs

| 现有实现 | 可复用内容 | 禁止误用 |
| --- | --- | --- |
| `action_value_data.py::DailyCandidate.open/bars`、`file_reference` | 日线候选身份、PIT、raw视图与文件不可变核验 | 不将日线可用时间默认值当所有源真实发布时间；不把全天暂停键冒充精确分钟暂停 |
| `minute_execution_pipeline.py::_read_bin_values`、`_load_calendar` | bin偏移与日历格式 | 不调用写旧registry的研究调度入口；不要每个分钟反复读整个文件 |
| `pattern_close_cash_replay.py::fee/affordable_quantity` | 父订单组件费用和合法可负担量 | `execute`是日线全账户mutation，不能原样当分钟部分成交引擎 |
| `fundamental_timing.py::build_research_features`、`core_tactical_timing.py` | 原信号和S1/S2状态、既有回放参考 | 不改旧政策默认参数、不借旧hash装新恢复规则 |
| `action_value.py::market_features` | 已有14项价量特征 | 不传已经复权且factor未置1的输入造成二次复权 |
| `r8_proxy_screen.py::screen_masks_for_symbol` | U0的T−1对齐与UNKNOWN语义 | 不为了调用便利把U1/U2新增为正式假设 |
| `core_tactical_benchmark.py`、`pattern_close_cash_benchmark.py` | 不可变封存、父进程归并、manifest验证模式 | 不改旧schema／namespace；不复用假定旧六项family的报告 |
| `action_value_execution_audit.py::audit_minute_execution` | 样本订单诊断作对照 | 默认sample_limit=256，不是全人口连续账户回放 |
| `action_value_model.py` | LightGBM环境、成熟时点验证、非pickle保存模式 | 旧fit_local_model为双头合同，不能直接训练本轮单头任务 |

不引入第三方训练平台／在线特征库。新代码建议控制为六个文件：

1. `causal_timing_contracts.py`：冻结spec、身份、PIT视图、标签边界。
2. `causal_timing_execution.py`：E0/E1/E2、只读分钟缓存、费用和交易限制适配。
3. `causal_timing_replay.py`：逐股连续账户、三候选与基准。
4. `causal_timing_oracle.py`：隔离的反事实和受限枚举，不被策略模块import。
5. `causal_timing_model.py`：单头标签消费、Ridge／GBDT与模型身份。
6. `causal_timing_benchmark.py`：单CLI、并行、检查、报告与不可变封存。

仅新增四个行为测试文件 `test_causal_timing_contracts.py`、`test_causal_timing_replay.py`、`test_causal_timing_model.py`、`test_causal_timing_benchmark.py`。优先参数化公共case，不复制大fixture／快照、不测私有实现排版；直接覆盖因果、资金、交易限制和artifact/API合同。

本轮设计初始写集为本F1文档；实现获批后的精确写集为以上六个业务文件、四个直接测试文件、本F1及主F2进度条目。若发现必须修改其他共享代码，交用户转其他窗口。本任务不需要前端、API、数据库或业务服务变化。

### 11.2 产物与运行合同

单CLI计划：`python -m backend.services.position_timing.causal_timing_benchmark prepare|run|inspect`。prepare只读source/preflight及已存在父receipt身份，冻结spec后产生绝对request路径；run按request执行三个工作块；inspect读封存bundle；随后同输入重复prepare/run/inspect检验exact retry。接口尚未实现，当前不可执行。

request绑定candidate、父bundle、calendar/PIT/指数映射、实际源文件、全部contract、source commit、依赖和运行环境hash；source preflight通过并封存前不读取新的策略收益。旧已公开结果是设计背景，必须在trial背景登记，不能伪称此前未观察。

产物包括 source_audit、trial_spec、feature/label/model manifests、oracle报告、逐股／逐日／逐腿parquet、pool报告、causality_receipt、chunk manifests、总manifest、receipt。单文件可按已有表合并，避免为每项指标建新体系。`outcomes_read_phase`明确为source封存后的diagnostics/oracle/labels/replay，不把读取标签装作还没看结果。

exact retry不能覆盖文件；语义内容与哈希稳定，机器时间等操作元数据单独保存，不进入需复现的结果身份。worker仅写本request私有临时chunk，父进程排序归并、校验并发布。失败chunk不计成功，重试复用同输入已封存chunk，不重新搜索参数。

## 12. Implementation Plan / 不限时长的连续长任务（F-012）

按三个工作块连续完成，不拆成十几个阶段、不等明天数据，也不以跑满某个小时数为目标。具体耗时先用固定样本测吞吐再给区间，不预先承诺全量能在10小时结束。

| 工作块 | 实施和范围 | 可核验出口 |
| --- | --- | --- |
| A：归因与执行基础 | 冻结request；新增共同账户／报告和分钟适配；读旧S1/S2逐腿记录；公共窗口BH/被动现金/原S1/S2；检查分钟差异与逐股MDD；接口依据§11的DailyCandidate、fee、原策略和minute reader | source_audit、旧亏损归因、公共基准、分钟coverage、资金／成交因果测试；禁止抽样审计冒充连续账户 |
| B：Oracle与固定候选 | 隔离Oracle；实现CYCLE5；产生训练／验证成熟标签；固定Ridge+GBDT；完成截断／扰动／负例测试；规格依据§5～§7、§9和action_value.py | oracle有限集合证明、label/training审计、模型hash、三候选规格不变；禁止旧双头身份、未来label或按收益调参 |
| C：全量比较与交付 | WSL并行公共窗口连续回放E0/E1/E2；全部pool报告；inspect→exact retry；多轮审核修复、定向测试、CI、提交合入和本任务清理；依据§10、§11及core_tactical_benchmark封存模式 | 完整比较矩阵、缺失明细、不可变身份闭合、因果receipt、PR/CI/merge/cleanup证据；禁止旧产物覆盖和借用旧六项family |

前置文档／接口发现已在本次设计完成，来源见§11；实施启动仍需读取当时最新AGENTS／项目memory／router及本设计，按客户端状态执行规范要求的verify-clients，核对origin/main和task scope。若main已改动依赖实现，先报告快照差异并以新request保留本设计语义，不能暗中继承新默认值。

每块先核对接口和订单行为，再写代码；不得用尚不存在的旧接口名“计划复用”。不因A的收益不佳跳过已批准B/C，也不根据Oracle结果改5日、20%或模型阈值。数据缺口只暂停受影响分支，继续可独立验证的块并向数据窗口输出具体缺口；绝不把日线替代分钟后宣称全部完成。

WSL默认8个股票worker、每chunk64股、在途最多8chunk；每个worker单线程BLAS/LightGBM，模型拟合不与8个重回放worker争抢资源。读bin按股按字段缓存／mmap，避免每天重开完整分钟文件。只读取已挂载R8，不复制整份数据或修改Windows/WSL profile。

先用canonical symbol排序固定前32股做串并行结果相等和吞吐／内存预检，不看其收益决定样本／政策。若资源不足可降worker到4或2并记录，不换股票或时间、不降模型规格；执行参数不改变研究语义。正式结果覆盖全部应入选人口，pilot绝不冒充全量证据。进度记录完成chunk／总chunk、失败数、实测ETA和复用数，不建监控平台。

旧策略处理：S1/S2在本轮公共窗口对照；PT-NEXT-018 R0～R7、PT-NEXT-019 VCB、PT-NEXT-021其他形态策略、PT-NEXT-022 T1/T2只做证据目录和机制归类，不本轮全矩阵重跑。若本轮指出特定卖出／恢复机制可改，后续单独提出有限复验，不据此宣布旧策略永久无效。

## 13. Verification Plan / 多轮审核、合入和停止边界（F-013）

设计审核顺序：业务目标／蓝图对齐 → 数学和因果时序 → 实现接口／数据边界 → F1结构验证。实现后至少两轮实质审核：第一轮审账户／标签／订单因果，修复并定向复测；第二轮按最终diff和独立产物审人口／hash／统计／scope，再跑稳定后的最小相关测试矩阵。真实BUG按规范登记；不通过大量重复测试堆积测试债务。

直接行为证据：

1. 同股独立现金、floor、T+1、买入现金上限、费用下限、卖出受限、未回补终点、自然复投守恒。
2. 三种成交视图的价格／factor方向、已封存订单因果、停牌和方向涨跌停、minute缺条拒绝降级、BH同口径。
3. 训练标签成熟、未来截断/扰动重训不变、Oracle注入负例、未来shift负例、preprocessing仅训练、PIT缺口显式状态。
4. 串并行一致、chunk缺失不成功、candidate漂移／文件变动拒绝、inspect全身份闭合、exact retry复用。
5. 小型可人工核算价格路径：BH最优时Oracle含BH；5日回补盈利／亏损；无可行受限Oracle；重复费用／透支及同日卖出不得发生。

文件lint/compile、上述定向测试、`git diff --check`、F1/F2 validator、当前规范L0及required CI通过后才按用户授权合入。广泛跨模块回归交既有CI，不本地全仓扫描。若线上功能未动，则不要求用户重启后端才能完成纯研究交付。

DESIGN-COMPLIANCE-001：不把pilot、价格代理或缺失人口当完整实盘证明；不吞掉错误；不改变已批准策略／比较器；不新增收益门槛审批。研发完成与获得alpha是不同验收：可以完整完成阴性实验，不能为交付而宣称盈利。

## 14. Risks / Production Gates / Rollback / 待确认内容（F-014）

- 最重要风险是可学机会不足，而不是模型不够深；维持简单模型和有限候选，不以RL/Transformer替代证据。
- R8历史已反复观察，test为时间外但不是研究意义的未见数据；本轮不自动上线，后续新日期实盘影子观察不阻塞本轮研发。
- 分钟价格代理不证明排队成交或容量，无冲击假设、限价保守处理、收盘采样MDD和复权虚拟账户边界均须显式报告。
- 标签标准账户与部署账户差异、财务PIT不足、静态模型漂移、bootstrap非平稳限制，不靠哈希或validator掩盖。
- 本设计没有运行时发布。回滚为停止使用本研究bundle／回退本模块代码，保留旧证据，不删candidate、不改在线卡片、不重启服务。

Production Gates 状态：DB DDL/DML=NOT_APPLICABLE；生产激活=NOT_REQUESTED；backend restart=NOT_REQUIRED，owner=user；在线业务写入=FORBIDDEN。Git/CI网络访问与市场数据访问分开记录，前者用于交付，后者研究运行必须为false；不得把整次开发有Git访问包装成全程network=false。

用户已确认本文件整体规格：U0单人口、公共窗口、固定5日回补+两个轻量过滤模型、三种执行视图、受限Oracle、三个连续工作块。实现须保持这些身份；任何业务语义变化先更新本设计并取得相应确认，不在运行中隐式改规格。

## 15. Design Acceptance Index

| design_item | 设计要求 |
| --- | --- |
| F-001 | 超BH与降低逐股MDD的联合目标、既有证据不冒充新结果 |
| F-002 | position_timing隔离、只读数据和零runtime／DB／跨模块写入 |
| F-003 | R8及父产物身份、时间切分、真实分钟覆盖边界 |
| F-004 | U0因果入场、独立现金账户、BH/现金/指数基准 |
| F-005 | 固定5日回补及两模型候选、失败交易状态 |
| F-006 | Oracle有限集合和训练标签隔离、成熟时点 |
| F-007 | 固定14特征、两个模型、不搜索和不借旧模型身份 |
| F-008 | 日线/收盘分钟/固定早盘价格代理与连续账户 |
| F-009 | 反前视合同、截断扰动、负例与证据限制 |
| F-010 | 逐股和组合双目标、八端点family与缺失披露 |
| F-011 | 已核接口、六个模块、三个测试文件和artifact隔离 |
| F-012 | 三块连续WSL并行长任务、资源与旧策略保留 |
| F-013 | 多轮审核修复、CI、合入清理和四项符合性 |
| F-014 | 风险／回滚和用户确认前不实施 |

## 16. Design Acceptance Matrix

以下为设计验收；`implementation_refs` 是章节及拟实现归属，不声称新代码已经存在；运行证据须在用户确认后逐项补齐。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | §1、§10 | artifact: 本文§1父证据与§17审核记录 | DESIGN_VERIFIED | none |
| F-002 | §2、§11 | artifact: 本文§17 scope审核 | DESIGN_VERIFIED | none |
| F-003 | §3；拟contracts | artifact: 本文§3输入身份与§17核对记录 | DESIGN_VERIFIED | none |
| F-004 | §4；拟replay | artifact: 本文§4账户／分母合同 | DESIGN_VERIFIED | none |
| F-005 | §5；拟replay | artifact: 本文§5候选状态机 | DESIGN_VERIFIED | none |
| F-006 | §6；拟oracle | artifact: 本文§6有限集合／label合同 | DESIGN_VERIFIED | none |
| F-007 | §7；拟model | artifact: 本文§7冻结模型规格 | DESIGN_VERIFIED | none |
| F-008 | §8；拟execution | artifact: 本文§8三视图合同 | DESIGN_VERIFIED | none |
| F-009 | §9；拟contracts/tests | artifact: 本文§9及§13因果测试规格 | DESIGN_VERIFIED | none |
| F-010 | §10；拟benchmark | artifact: 本文§10统计合同 | DESIGN_VERIFIED | none |
| F-011 | §11；拟六模块 | artifact: 本文§11已核对源码接口 | DESIGN_VERIFIED | none |
| F-012 | §12；拟benchmark CLI | artifact: 本文§12实施计划 | DESIGN_VERIFIED | none |
| F-013 | §13 | artifact: 本文§17审核与validator记录 | DESIGN_VERIFIED | none |
| F-014 | §14 | artifact: 本文§14确认边界 | DESIGN_VERIFIED | none |

## 17. 设计审核记录

- 第一轮，接口与目标审核：已核对现有日线、minute bin、账户费用、S1/S2、14项特征、模型入口和并行封存；修正“旧抽样分钟审计即完整账户回放”和“旧双头训练入口可直接套新标签”两种不成立的复用假设。
- 第二轮，数学与因果审核：明确终止清算后残余头寸不等于到账现金；固定估值缺口处理和MDD采样；补标准标签初值取整、参考事件资格、受限Oracle格点；明确入场时指数分层与父研究dynamic视图不同。F1首次验证发现缺Production Gates标题，已补本设计实际的零生产操作合同，没有新增门禁。
- 第三轮，整体一致性审核：对照主蓝图§9.27.1～§9.27.6，明确一个动作模板与两个函数族、未见数据限制、模型生效时点与真实创建时间、事后报告cohort、旧／新合成方法差别、数据缺口不自动剔除及零生产操作；区分模型adjusted/factor=1输入与旧信号raw输入，避免价量基准和特征身份混用。未发现需要新增跨模块基础设施的实现依赖。
- 文档结构验证：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/position_timing_causal_oracle_minute_research_f1_20260919.md --tier F1`，14项／14行通过；未修改的主F2校验59项／59行通过。最终文本继续执行相同F1检查及staged diff检查。仅文档验证，不是因果测试／回放已通过。
- 最终结构复查曾发现跨文档验收编号被validator误作本F1条目，已改为引用主蓝图章节，保留本F1独立编号；不通过给矩阵虚增条目掩盖引用错误。
- 2026-09-20用户确认：授权按本长任务规划开始实现、研究、多轮审核修复、提交合入与任务清理；后端重启继续由用户执行。本条只改变实施授权状态，不提前填写实现证据。
