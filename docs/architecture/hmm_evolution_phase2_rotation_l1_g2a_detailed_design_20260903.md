# HMM Evolution Phase 2 G2-A `rotation_L1` 端到端详细设计

> **设计层级**：F2
> **版本**：v1.2
> **日期**：2026-09-04
> **状态**：`DESIGN_READY_USER_APPROVED_EXACT_CONTRACT_NOT_IMPLEMENTED`
> **父权威**：`docs/architecture/hmm_evolution_and_risk_management_system_design_20260716.md` v2.42
> **终极目标**：在同一个G2-A闭环内交付真实日度L1板块轮动预测、最小repository/read API和真实`/hmm-risk` L1热力图，而不是只交付模型、fit、artifact、receipt或market regime页面。
> **2026-09-04批准边界**：MDE只决定forward-confirmation状态；`tail_access_gate`与`research_product_gate`独立；`min_child_samples=310`且训练后每叶`min_leaf_distinct_dates=20`；forward effect failure使用one-sided 95% HAC上置信界`<=0`；fold-local market context对5D/10D horizon共享；§10.1其余精确合同也已一次性批准。该批准不等于源码、fit、tail读取、DDL执行或runtime activation已获授权。
> **v1.2批准边界**：用户已一次性批准§10.1的504日rolling、特征完整性、Ridge/horizon规则、LightGBM 4.6.0 profile、coverage、state projection与最小DB/API设计合同。该批准只允许设计状态回填和F2验收，不授权源码、39 fits、tail读取、DDL或runtime activation。
> **2026-09-06 MARKET-CONTEXT-A批准边界**：每个decision date `t`仅使用同release CSI300截至`t-1`的`daily_return`与`volatility_3d=population_std(ddof=0)`；每fold在固定504日target-free train上执行train-only z-score，复用K=2 jump、`lambda=4.0`、`seed=42`。semantic score固定为standardized center `daily_return-volatility_3d`，较高state映射`risk_on`、较低state映射`risk_off`。5D/10D共享同一fold-local fit；缺数、非有限、state tie或因果递推失败均fail closed，不补默认状态、不重新拟合、不读取target。39-fit总预算及其余v1.2合同不变。

---

## 0. 背景、权威、现状与批准边界

本设计只展开父蓝图F-011/F-012/F-013的首个真实产品闭环。用户已确认以下方向：

1. HMM/jump只负责提供因果market context，不再由逐sector隐状态承担未来轮动语义；
2. `rotation_L1`使用一个直接监督、浅层、强正则的非线性横截面scorer；
3. development-only有界battery、唯一candidate、全新未消费尾部和真实prediction/API/UI属于同一个G2-A；
4. 旧P2-3A～P2-4、HR1、RW1保持不可变终态，不重跑、不调参、不复用已消费窗口；
5. 不建设通用feature/evidence/training平台，不并行模型，不单独产品化market regime。

本文件的目标方向、页首四项合同及§10.1全部精确值均已获用户批准。文档通过F2 validator只证明设计合同结构和批准状态闭合，不等于源码、依赖变更、实验、tail读取、DDL或runtime activation已获授权或已经完成。

## 1. Scope、Non-goals与术语

### 1.1 In scope

- 完整canonical L1分母上的未来相对强弱连续`rotation_score`；
- 从连续score预注册派生的`trending|neutral|fading`展示状态；
- 一个因果market context输入；
- development-only 5D/10D battery、线性对照和统计功效核算；
- 一个冻结参数的浅层GBDT candidate；
- rolling walk-forward、purge/embargo与全新未消费评估尾部；
- 独立coverage与typed availability，不使用performance-based abstention；
- `research_product_gate`通过后的真实OOF prediction、最小repository/read API与L1热力图；
- typed failure、紧凑receipt、writer/readback及advisory-only隔离。

### 1.2 Non-goals

- 不交付`rotation_L2|risk_L1|risk_L2`，三者继续显式`NOT_AVAILABLE`；
- 不把market regime单独标记为`CAPABILITY_AVAILABLE`；
- 不重新训练或修补旧HMM、jump-label、Ridge、HR1或RW1；
- 不运行LightGBM/Ridge/feature/horizon grid，不early stopping调树数，不在结果后改参数；
- 不做自动feature generation、全因子库搜索、PCA、神经网络、ensemble或失败fallback；
- 不建设新registry、feature store、通用训练服务、evidence平台或scheduler；
- 不进入Selection、QE、Paper、QMT、订单、持仓或调仓链；
- 不在本设计任务执行依赖安装、DDL/DML、runtime activation或服务启停。

### 1.3 状态语义

- 状态使用五个正交字段，禁止互相推导：`research_surface_status=NOT_AVAILABLE|AVAILABLE_EXPERIMENTAL`、`rotation_l1_capability_status=NOT_AVAILABLE|RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED|ADVISORY_PREDICTION_AVAILABLE`、`forward_power_status=UNAVAILABLE|INSUFFICIENT|SUFFICIENT`、`forward_confirmation=NOT_STARTED|PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE|PASSED|FAILED`、`advisory_status=NOT_AVAILABLE|AVAILABLE`。
- `research_surface_status=AVAILABLE_EXPERIMENTAL`只证明真实因果OOF预测、repository/API/UI与writer/readback工程链闭合；若development效果未达到binding MBE，`rotation_l1_capability_status`必须仍为`NOT_AVAILABLE`，不得把工程闭环冒充预测能力。
- 只有development效果达到binding MBE，才允许`rotation_l1_capability_status=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；只有forward confirmation通过，才允许`rotation_l1_capability_status=ADVISORY_PREDICTION_AVAILABLE`且`advisory_status=AVAILABLE`。
- `INSUFFICIENT_STATISTICAL_POWER`是forward evaluation conclusion/reason，不是模型失败。它不阻止唯一GBDT candidate或真实research surface，但禁止advisory升级。
- `INSUFFICIENT_DATA_CONTRACT`表示输入、标签、日历或PIT不满足执行合同；不是模型失败。
- 顶层`CAPABILITY_AVAILABLE`只能由至少一个明确命名、达到其批准capability状态的component推导；`AVAILABLE_EXPERIMENTAL`、battery、market context、模型文件或API壳均不能推导它。

## 2. Architecture（架构）与端到端数据流

```text
versioned H5/Bin + C-013 PIT/identity + calendar
                │
                ▼
minimal immutable rotation input bundle
                │
      ┌─────────┴─────────┐
      ▼                   ▼
development battery   causal market context
      │                   │
      └──── horizon ──────┘
                │
                ▼
single rolling GBDT candidate
                │
                ▼
      research_product_gate
         │ pass              │ fail
         ▼                   ▼
real causal OOF          typed NOT_AVAILABLE
repository/API/UI
AVAILABLE_EXPERIMENTAL
         │
         ▼
      tail_access_gate
         │ pass              │ fail
         ▼                   ▼
single sealed-tail       keep experimental surface;
evaluation              no capability / no tail read
         │
         ▼
forward passed/inconclusive/failed
         │ passed only
         ▼
advisory product bundle + same real L1 heatmap state upgrade
```

所有箭头均携带source/model/config/hash。每个分支失败只停止其被授权的下游：research gate失败停止全部产品写入；tail gate失败禁止读取tail但不回滚已闭合的experimental surface；forward失败禁止advisory升级和新增日度预测，但保留不可变历史OOF研究回读。Battery与market context没有独立产品出口，旧模型没有fallback箭头。

## 3. D1：产品目标、target与输出（USER_APPROVED）

### 3.1 决策时点与target

对canonical open day `t`、L1 sector `s`和候选horizon `h∈{5,10}`：

```text
sector_return(s,t,h) = close_index(s,t+h) / close_index(s,t) - 1
market_return(t,h)   = close_CSI300(t+h) / close_CSI300(t) - 1
raw_relative(s,t,h)  = sector_return(s,t,h) - market_return(t,h)
target(s,t,h)        = raw_relative(s,t,h) - median_s(raw_relative(s,t,h))
```

`t`日预测只允许读取截至`t-1`收盘已完成并通过PIT/source authority的数据。`t+h`仅用于离线outcome评价，绝不进入`t`日feature、market context、availability或模型选择。

### 3.2 产品输出

- `rotation_score`：GBDT对`target(s,t,h)`的有限实数预测，不解释为概率；数值越大表示未来相对走强预期越高。
- `forecast_state`：在同一交易日所有available L1 score上稳定排序，按`(rotation_score,sector_code)`确定审计顺序；令`N_available`为available数、`q=max(5,ceil(0.20*N_available))`，最低q项为`fading`、最高q项为`trending`、其余为`neutral`。tie epsilon固定`1e-12`：从最低score开始，以组内首个score为anchor构造最大连续组，只有`score-anchor<=epsilon`的项属于同组；任一tie group跨越第q或第`N_available-q`边界时整组改为neutral并如实减少extreme coverage。任一extreme最终少于5项时当日spread unavailable，但Rank IC仍按全部available项计算。禁止按hidden-state index、sector code或当前收益强拆tie与命名。
- `feature_contributions`：使用同一LightGBM model的原生`pred_contrib`输出每个feature贡献与base value；贡献只解释模型输出，不声明经济因果。对每行要求`abs(sum(contributions)+base_value-rotation_score) <= 1e-12 + 1e-10*max(1,abs(rotation_score))`。任一行不满足即model/writer完整性失败，必须阻断该批canonical写入并保留typed failure；不得静默忽略，也不得把仍然有限且身份闭合的单条score单独降级为业务unavailable。
- `market_regime`：固定复用P2-3B已验证的K=2 jump结构及`lambda=4.0/seed=42`身份，但不复用旧板块聚合feature。每个decision date `t`只使用同release CSI300截至`t-1`的两列：`daily_return=close(t-1)/close(t-2)-1`与`volatility_3d=population_std(ddof=0)`，后者严格取CSI300在`t-3..t-1`三个canonical open day的close-to-close return。每个fold只在不读取任何5D/10D target的公共504日rolling train上拟合train-only mean/std z-score与jump model，并由两个horizon共享同一个fold-local identity；不winsorize、不补值。standardized centroid的`daily_return-volatility_3d`为唯一semantic score，较高state=`risk_on`、较低state=`risk_off`，两score差`<=1e-8`即typed tie失败。尾部前在完整development 504日上重拟合一次并冻结。只输出`risk_on|risk_off`及availability作输入/解释，不复制旧path、不读取旧score，也不是轮动答案。尾部递推若出现输入缺口、状态非有限、identity不闭合或无法从冻结参数因果延续，整日market context与全部sector prediction typed unavailable；不允许前值、默认regime或重新拟合。
- `prediction_availability`：`available|unavailable`；unavailable不得补neutral、前值或旧模型score。首个G2-A不存在performance-based `abstained`状态。
- 产品声明固定为“研究分析，不构成投资或交易建议”。

## 4. D2：数据、日期、rolling与因果隔离（USER_APPROVED）

### 4.1 Source authority

复用C-013 security/industry PIT与现有immutable bundle builder的source precedence、typed停牌/provider-absence和hash/readback能力。现有development authority覆盖`2020-07-30..2026-03-31`、1373个canonical open days、31个L1 sector；其成功只证明数据身份与日历可读，不证明v1.2新增feature已经物化。development bundle固定截止`2026-03-31`；全新tail使用独立最小sealed bundle，只能在candidate、horizon、feature、参数、MBE与availability合同全部冻结并写入不可变identity后构建/读取。两个bundle都只增加D3批准的最小feature columns、market context和构造所需lineage；不得复制无关原始表、把tail并入battery reader或回退实时数据库。

v1.2必须使用新schema `hmm_risk_rotation_l1_g2a_input_bundle_v1`，不得把旧C-012 bundle的9个同数量但不同语义字段改名后复用：

- target及四个relative momentum、downside volatility、max drawdown固定来自versioned `sector_data.h5`的申万L1指数close与同release CSI300 benchmark close；不得用成分股等权收益或旧`daily_return/excess_return_Nd`近似替代；
- breadth固定来自同release Qlib日频close、C-013在`t-1`有效的PIT成分关系及typed停牌/provider状态；
- moneyflow intensity固定来自同release `moneyflow.h5` CNY amount、Qlib traded amount与同一PIT成分关系；单位、分子分母及provider-absence必须进入feature identity；
- market context只消费同release CSI300 daily return/volatility；不读取sector future outcome；
- bundle只保存`trade_date/sector_code`、9列feature、每列validity/reason、5D/10D target及其maturity标志、calendar/benchmark、最小PIT/source lineage与canonical hashes。旧bundle仅作calendar/identity可执行性证据，不进入新模型或产品identity。

### 4.2 时间域

- 已消费证据：`<=2026-03-31`全部只作development；P2-4的`2025-04-01..2026-03-31`不得再次称为untouched。
- 新尾部开始：`2026-04-01`。
- formal request必须显式绑定一个已批准immutable source release及其`source_cutoff`；不得使用`latest`、当前数据库或运行时日期。对horizon `h`，尾部最后decision date精确定义为：该release内满足`shift_open_day(t,h)<=source_cutoff`且31-sector/market outcome完整的最后canonical open day。request冻结时写入实际start/end/count/date-set hash；不足一个可计算日期时typed停止。
- 当前起止日只允许通过零模型preflight读取calendar/source completeness确定；不得读取尾部收益、IC、spread、feature与model score。
- preflight必须证明该尾部从未被本candidate的feature、horizon、参数、MBE或availability决策访问；无法证明时`INSUFFICIENT_DATA_CONTRACT`。

### 4.3 Development rolling walk-forward

- 使用一个rolling训练窗，不比较expanding或多个window。
- v1.2唯一候选为`rolling_window_open_days=504`，表示约两个A股交易年；它不是从RW1结果搜索得到，而是由固定模型容量与现有日历可执行性一次冻结。每个horizon先从validation首个canonical open day向前排除`purge=h`个open days，再取其前连续504个open days作为训练decision dates；`embargo=0`。
- development validation固定复用五个已消费、彼此不重叠的区间：`2023-09-04..2024-03-14`、`2024-03-15..2024-09-18`、`2024-09-19..2025-03-31`、`2025-04-01..2025-09-30`、`2025-10-01..2026-03-31`。每fold train为validation首日前严格连续的最后`rolling_window_open_days`个canonical open days；不足时合同不可执行，不缩窗。
- 每个fold必须列明train start/end、purge、validation start/end、outcome-eligible dates及hash。
- 精确日历列账如下；表中purged日期不进入fit或target，validation边界仍以原五fold合同为准：

| fold | validation | 5D train / purge | 10D train / purge |
|---|---|---|---|
| 1 | `2023-09-04..2024-03-14`，126日 | `2021-07-30..2023-08-25` / `2023-08-28..2023-09-01` | `2021-07-23..2023-08-18` / `2023-08-21..2023-09-01` |
| 2 | `2024-03-15..2024-09-18`，126日 | `2022-02-10..2024-03-07` / `2024-03-08..2024-03-14` | `2022-01-27..2024-02-29` / `2024-03-01..2024-03-14` |
| 3 | `2024-09-19..2025-03-31`，126日 | `2022-08-15..2024-09-09` / `2024-09-10..2024-09-18` | `2022-08-08..2024-09-02` / `2024-09-03..2024-09-18` |
| 4 | `2025-04-01..2025-09-30`，126日 | `2023-02-23..2025-03-24` / `2025-03-25..2025-03-31` | `2023-02-16..2025-03-17` / `2025-03-18..2025-03-31` |
| 5 | `2025-10-01..2026-03-31`，116日；首个open day为`2025-10-09` | `2023-08-28..2025-09-23` / `2025-09-24..2025-09-30` | `2023-08-21..2025-09-16` / `2025-09-17..2025-09-30` |

- 每个train恰为504日；feature lookback只读取train首日前最多60个source days，不能进入target或增加训练样本。window合同必须同时满足：五fold完整、tail不参与fit/selection、D5功效计算可执行；任一日历漂移均显式失败，不缩窗或改fold。
- target-free market context不需要purge：每fold固定使用validation首个open day之前的最后504日，依次为`2021-08-06..2023-09-01`、`2022-02-17..2024-03-14`、`2022-08-22..2024-09-18`、`2023-03-02..2025-03-31`、`2023-09-04..2025-09-30`，由5D/10D共享。tail前full-development market fit固定使用`2024-03-01..2026-03-31`；full-development GBDT的5D候选为`2024-02-23..2026-03-24`并purge`2026-03-25..2026-03-31`，10D候选为`2024-02-08..2026-03-17`并purge`2026-03-18..2026-03-31`。这些日期由已冻结1373日calendar精确回读，正式request仍须逐项hash比对。

### 4.4 尾部功效只读边界

尾部功效preflight只读取日期数量、outcome成熟标志和source completeness。波动、自相关、HAC bandwidth及任何分布参数只能来自development或批准的理论常量；禁止读取尾部actual outcome来计算MDE、选horizon或改变模型。

## 5. D3：有界battery、特征与horizon（USER_APPROVED）

### 5.1 Battery不是产品candidate

Battery只承担四项职责：horizon选择、线性对照、功效估计、特征方向合理性检查。其输出永远不能写model/product bundle或`AVAILABLE`；基线IC低不得淘汰D4唯一GBDT。数据、标签或因果切片不可执行时模型停止；仅功效公式/LRV不可执行时只设置`forward_power_status=UNAVAILABLE`，不得阻止GBDT或research surface。

### 5.2 预注册输入上限

总模型输入严格不超过10列。已批准feature profile为：

1. `relative_momentum_5d`；
2. `relative_momentum_10d`；
3. `relative_momentum_20d`；
4. `relative_momentum_60d`；
5. `relative_downside_volatility_20d`；
6. `relative_max_drawdown_20d`；
7. `pit_breadth_above_ma20`；
8. `moneyflow_intensity_20d`；
9. `market_regime_sign`。

令`r_s(u)`和`r_m(u)`分别为sector与CSI300在open day `u`的close-to-close return，`e_s(u)=r_s(u)-r_m(u)`，全部窗口截至`t-1`：

| feature | 已批准公式与source contract |
|---|---|
| `relative_momentum_Ld`, L=5/10/20/60 | `prod_(u=t-L..t-1)(1+r_s(u)) - prod_(u=t-L..t-1)(1+r_m(u))`；L个canonical open days与两个return序列必须全部有限，少一日即该feature为NaN并保留typed reason |
| `relative_downside_volatility_20d` | `-sqrt(mean(min(e_s(u),0)^2))`；20个`e_s`必须全部有限，负号使更小下行波动方向更高 |
| `relative_max_drawdown_20d` | `MDD_market(20d)-MDD_sector(20d)`，MDD为正的最大峰谷损失比例；sector/market 20日close path必须完整且有限 |
| `pit_breadth_above_ma20` | `t-1` PIT成分股中`close(t-1)>mean(close(t-20..t-1))`的比例；分母只包含在`t-1`有权威PIT成员身份、20日close完整且非typed停牌的股票。当日停牌按`not_applicable_suspended`从分子分母同时排除；provider缺失不得前值填充。有效分母须`>=5`且占当日期望非停牌成员`>=90%`，否则该feature为NaN |
| `moneyflow_intensity_20d` | 对`t-20..t-1`的20个canonical open days与当日PIT成分股，计算`sum(valid net_mf_amount_CNY)/sum(valid traded_amount_CNY)`；每个交易日有效非停牌contributors须`>=5`且覆盖期望非停牌成员`>=90%`，20日必须全部满足，最终分母须有限且`>0`。provider-absence按既有typed contract从对应日分子分母排除并计入coverage，不得以0或前值补足 |
| `market_regime_sign` | §3.2 train-only K=2 jump context，`risk_off=-1,risk_on=+1` |

当前profile共9列，未用满的第10列不是运行时扩展槽位；任何新增、删除、替换、lookback或公式变化仍需新合同。单条sector/date prediction必须有`market_regime_sign`且其余8个连续feature至少7个有限，即总可用输入至少8/9；不满足时该条typed unavailable。该规则允许一个明确缺失feature，但不改变任何feature公式或隐式缩短窗口。battery只检查方向和线性对照，不得根据结果删除feature。

### 5.3 预处理与线性对照

- sector连续feature在每个date只对available canonical L1做average-rank，映射到`[-0.5,0.5]`；market regime固定映射`risk_off=-1,risk_on=+1`，不参与截面rank。market不可用时对应日期全部prediction unavailable，不补0。
- 缺失保持NaN并携带typed reason；不得填0、均值、中性值或前值。
- 线性对照固定为同feature、同rolling folds、同target的`Ridge(alpha=100.0,fit_intercept=true,solver="svd",tol=1e-4,max_iter=null,positive=false,random_state=null)`；它只解释GBDT相对增量，不具备promotion资格，不构成第二candidate。
- Ridge不支持NaN，因此每fold fit/predict只使用9/9 feature全部有限的complete-case行；不得插补或增加missing indicator。每个comparator metric date仍须至少28/31 complete-case sectors，development metric-valid daily ratio须`>=90%`，否则horizon selection typed失败。该complete-case限制只属于Ridge comparator，不把GBDT的8/9产品availability偷换为9/9。
- daily cross-sectional average-rank的tie使用原始有限值精确相等分组；映射公式为`rank_pct-0.5`。NaN保持NaN，不参与当日rank分母，也不生成missing indicator。

### 5.4 Horizon选择规则

两个horizon均先完成development-only comparator与各自的forward功效核算；`MBE_IC<MDE_h`只改变该horizon的forward-confirmation可行性状态，不淘汰horizon、不阻止唯一GBDT：

1. 只在两个comparator都产生metric-valid的相同decision dates上形成`d_t=IC_5D,t-IC_10D,t`；若`mean(d_t)>0.005`且Bartlett Newey-West lag=`9`的one-sided t-stat `>=1.645`，选择5D，否则保留10D默认；
2. 相同日期不足、任一IC非有限或HAC统计不可计算时使用`hmm_risk_rotation_horizon_selection_failed`并停止，不以10D默认掩盖数据/计算错误；只有完整可计算但未超过两项边界时才选择10D；
3. horizon由Ridge comparator而非GBDT结果冻结，这是为避免用唯一candidate结果选择horizon的保守隔离；receipt必须记录`horizon_selection_model_class=RIDGE_COMPARATOR`和`gbdt_horizon_optimality_not_claimed=true`，不得以GBDT可能偏好另一horizon为由事后重选；
4. K=2 market context对horizon独立：五个fold各拟合一次，由5D/10D comparator共享；battery预算为`5 market + 5 Ridge(5D) + 5 Ridge(10D)=15 fits`。

## 6. D4：唯一浅层GBDT candidate（USER_APPROVED）

### 6.1 Estimator identity

唯一candidate为`LGBMRegressor`风格的浅层GBDT，对daily-centered target执行回归，以横截面Rank IC验收；不使用需要离散relevance编码的`LGBMRanker`。估计器、loss或任一参数变化都产生新candidate identity，正式结果后不得原地修改。

已批准profile：

```text
package=lightgbm==4.6.0
boosting_type=gbdt
objective=regression_l1
class_weight=null
max_depth=3
num_leaves=7
learning_rate=0.03
n_estimators=240
subsample_for_bin=200000
min_child_samples=310
min_child_weight=0.001
min_split_gain=0.0
reg_alpha=1.0
reg_lambda=10.0
subsample=1.0
subsample_freq=0
colsample_bytree=1.0
max_bin=63
min_data_in_bin=3
feature_pre_filter=false
use_missing=true
zero_as_missing=false
extra_trees=false
path_smooth=0.0
max_delta_step=0.0
random_state=42
n_jobs=1
deterministic=true
force_col_wise=true
importance_type=split
verbosity=-1
early_stopping=forbidden
```

`max_depth<=3`与`num_leaves<=7`共同约束树复杂度。用户已批准`min_child_samples=310`按模型容量控制，禁止再用`31×rolling_window百分比`机械替换；训练后每棵树每个实际叶节点必须覆盖至少`20`个distinct decision dates，低于20即typed fit-contract failure且不得调参重训。receipt只保存全模型leaf-date coverage的`minimum/p05/median/violating_leaf_ids/canonical_hash`紧凑摘要，不保存逐叶大JSON。该低位门只防止少量日期主导的结构退化，不声明20日足以证明样本外预测力。仓库`requirements.txt`与当前Conda `AIstock`环境均已只读核验为`lightgbm==4.6.0`，因此v1.2候选复用既有精确pin，不升级到4.7.0、不修改NumPy、也不新增依赖安装动作；fresh-process仍须在执行时回读版本与build identity，不一致即停止。

### 6.2 训练与复现

- 每fold只用其rolling train rows拟合；validation和尾部绝不参与fit、tree count或参数选择。
- 不使用validation early stopping；240棵树完整训练，失败显式终止。
- 两个fresh Python processes必须读取同一bundle/model contract并独立执行全部fold；canonical feature、prediction和metric payload必须bitwise hash相同。数值allclose只用于定位，不替代hash equality。
- 环境、LightGBM build、Python/NumPy/SciPy/scikit-learn版本、CPU/threadpool及线程数进入receipt；有效线程数不为1时fail closed。
- 不允许第二seed、bagging、model ensemble、run后调参或扩大fit预算。

### 6.3 Development双门合同

唯一GBDT的development OOF输出同时进入两个用途不同、互不替代的门：

1. `research_product_gate`只验证因果walk-forward OOF、禁止in-sample prediction、canonical 31-sector分母、coverage、有限score、model/input/mapping identity、fresh-process复现和writer/readback。它不含效应量或显著性阈值；通过后只允许`research_surface_status=AVAILABLE_EXPERIMENTAL`，不得推导rotation capability。
2. `tail_access_gate`在上述工程与因果条件全部通过后，另要求`development_oof_mean_rank_ic >= binding_MBE_IC`。不增加development显著性AND门。失败时`tail_accessed=false`、`forward_confirmation=NOT_STARTED`、`rotation_l1_capability_status=NOT_AVAILABLE`，但不回滚已经真实闭合的research surface。
3. development的Rank IC与top-bottom realized spread必须同时进入receipt；符号不一致记录非阻断诊断`metric_direction_divergence_observed`，不能自动归因为代码、tie或coverage缺陷，也不得触发重训。

### 6.4 Fit上限

在五fold合同下，最大fit数固定为39：battery为5个market context fit加`2 horizons × 5`个Ridge fit，共15；GBDT两个fresh processes各自执行5个fold market fit、5个fold GBDT fit、1个full-development market fit和1个full-development GBDT fit，共24。尾部验收和产品prediction只做inference，不新增fit。少跑只能来自合同内fail-closed停止，必须记录planned/started/completed/failed，不能把局部fit写成完整成功；任何超过39的执行视为未批准grid。

## 7. D5：MBE、MDE与产品验收（USER_APPROVED）

### 7.1 MBE与MDE必须分离

- 唯一binding `minimum_business_effect`固定为平均daily L1 Spearman Rank IC `MBE_IC=0.02`，`derivation=CONVENTIONAL_PRIOR_MAGNITUDE_NOT_VALUE_DERIVED`。该数值延续既有研究量级、不是由已经验证的用户收益函数推导，也不得按battery或tail结果降低。
- top-bottom gross relative spread不再拥有独立binding MBE；它按5D/10D realized outcome继续完整报告，作为forecast-state极端组的经济解释和Rank IC方向分歧诊断。任何基于正态分布、截面标准差或固定分位比例的`IC↔spread`换算只能作development敏感性说明，不能成为正式恒等式或反向调整MBE。
- MDE只回答当前尾部能否以批准显著性/功效检测MBE，不决定MBE数值，也不得用于降低产品门槛。
- v1.2精确功效合同固定为one-sided `alpha=0.05`、power=`0.80`、`z_(1-alpha)=1.6448536269514722`、`z_power=0.8416212335729143`。只用development daily metric序列估计Newey-West long-run variance `LRV_dev,h`，再按尾部成熟decision-date数量`N_tail,h`投影标准误：

```text
SE_tail,h = sqrt(LRV_dev,h / N_tail,h)
MDE_h = (z_(1-alpha) + z_power) * SE_tail,h
```

- `LRV_dev,h`固定来自D3同horizon Ridge对照的development daily Rank IC序列；HAC lag固定为`h-1`，核函数固定为Bartlett。`N_tail,h`只来自calendar/outcome成熟标志，不读取actual outcome。LRV非有限、非正或日期不足时`forward_power_status=UNAVAILABLE`并保留具体reason；`MBE_IC < MDE_h`则记录`PENDING_INSUFFICIENT_POWER`。两者均不得阻止唯一GBDT、research surface或通过`tail_access_gate`后的单次tail评价；该近似不能淘汰模型或改变MBE。forward promotion仍必须由candidate自身tail IC序列形成有效实际HAC统计。

用户已批准的forward effect边界只使用candidate实际tail daily Rank IC序列。令`mean_tail`为其算术均值，`LRV_tail`为Bartlett核、lag=`h-1`的Newey-West long-run variance，`N_tail_actual`为实际进入统计量的成熟available decision dates：

```text
SE_tail_actual = sqrt(LRV_tail / N_tail_actual)
U95_one_sided  = mean_tail + z_0.95 * SE_tail_actual
z_0.95         = 1.6448536269514722
```

只有`N_tail_actual/LRV_tail/SE_tail_actual/U95_one_sided`全部有限、`N_tail_actual>h`且`LRV_tail>0`时才允许作effect判断。`U95_one_sided<=0`是唯一forward effect failure边界；否则若未满足passed则为inconclusive。统计量不可计算时使用`hmm_risk_rotation_tail_effect_unavailable`，不得冒充effect failed或passed。任何按当前规划样本反推的约`-0.067`观测均值只是说明性例子，不进入配置、源码或receipt验收常量。

### 7.2 正式验收只保留四个正交维度

1. **唯一binding预测指标**：尾部每个available date对`rotation_score`与future relative outcome使用average-rank Spearman；跨日均值达到`MBE_IC=0.02`，且one-sided HAC检验拒绝`mean<=0`；
2. **经济幅度展示**：尾部按D1预注册state projection计算`mean(outcome_trending)-mean(outcome_fading)`；任一extreme因tie调整后少于5个sector时该日metric unavailable。spread点估计、HAC区间及其与Rank IC的符号关系必须显示，但不形成第二个promotion gate；
3. **跨期稳定性诊断**：完整calendar-month mean Rank IC、正向月份比例、最差月份和月度序列全部报告，但不形成第三个效果promotion gate；不得用月度结果重选horizon、feature或模型；
4. **coverage/availability**：每个metric date的canonical分母固定31，至少28/31条prediction available才允许计算当日IC/spread；development OOF与tail的metric-valid daily ratio均须`>=90%`，每个sector的prediction availability ratio均须`>=90%`。行业/规模/流动性代表性独立报告。coverage通过不能补足预测失败，预测通过不能掩盖coverage不足；不足时使用typed coverage failure，不删日期或sector改善指标。

### 7.3 Availability（无performance-based abstention）

- 首个G2-A删除基于score dispersion、预测强弱或任何表现代理的date-level abstention；不保留q10或其他可调分位数。
- 只有输入少于批准最小feature数、market context不可用、model score非有限或identity/causal contract失败时才允许typed unavailable；不得填0、neutral、前值或旧模型结果。
- receipt始终报告canonical 31-sector完整分母、28/31 metric门、daily/per-sector 90% coverage和代表性，不允许通过删日期或删sector改善指标。

## 8. D6：停止条件、产品闭环与失败决策树（USER_APPROVED）

### 8.1 顺序与停止条件

1. input/calendar/source preflight；
2. development-only battery与forward功效说明；
3. horizon冻结；
4. 两fresh-process唯一GBDT development执行；
5. 执行`research_product_gate`；通过时以真实causal OOF prediction闭合repository/read API与L1热力图，状态最多为`AVAILABLE_EXPERIMENTAL`；
6. 执行`tail_access_gate`；development效果未达到binding MBE时不读取tail，但不回滚research surface；通过时设置`rotation_l1_capability_status=RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`与`forward_confirmation=PENDING_INSUFFICIENT_POWER|PENDING_INCONCLUSIVE`；
7. tail gate通过后冻结candidate/model/feature/threshold并一次性读取全新尾部outcome；
8. 按forward passed/inconclusive/failed更新能力状态；只有passed才写advisory canonical product bundle并升级能力。

任何步骤失败均不得跳到后续步骤、换参数、换horizon、启用第二candidate或回退旧模型。

### 8.2 决策树

- development数据、标签或因果切片不可执行：research surface与rotation capability均`NOT_AVAILABLE`，使用对应typed data reason。仅功效公式/LRV不可执行时只设置`forward_power_status=UNAVAILABLE`，不得伪装为模型失败或阻止research/tail gate执行。
- `research_product_gate`失败：research surface与rotation capability均`NOT_AVAILABLE`；不得用mock、in-sample或静态矩阵补足。
- research gate通过但`tail_access_gate`失败：`research_surface_status=AVAILABLE_EXPERIMENTAL`、`rotation_l1_capability_status=NOT_AVAILABLE`、`tail_accessed=false`；页面必须直接展示development OOF Rank IC、HAC区间、`BELOW_BINDING_MBE`和未开始forward confirmation。
- tail gate通过后先形成`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`；若`MBE_IC<MDE_h`则预先记录`forward_confirmation=PENDING_INSUFFICIENT_POWER`，功效充足或功效状态不可用则记录`PENDING_INCONCLUSIVE`。三者均允许一次性tail评价，且不得降低MBE或显著性标准。
- **forward passed**：tail mean Rank IC达到0.02且one-sided HAC拒绝`mean<=0`，coverage/data/model合同均有效；升级`rotation_l1_capability_status=ADVISORY_PREDICTION_AVAILABLE`、`forward_confirmation=PASSED`、`advisory_status=AVAILABLE`。
- **forward inconclusive**：未满足passed，且one-sided 95% HAC上置信界仍`>0`；保持`RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED`。若`MBE_IC<MDE_h`则`forward_confirmation=PENDING_INSUFFICIENT_POWER`，否则为`PENDING_INCONCLUSIVE`；不调参、不换模型，可继续记录同一冻结模型的前瞻预测。
- **forward failed**：仅当`upper_one_sided_95pct_HAC_confidence_bound <= 0`时设置`forward_confirmation=FAILED`、`rotation_l1_capability_status=NOT_AVAILABLE`、`advisory_status=NOT_AVAILABLE`并停止新增日度预测；已闭合的历史OOF研究回读仍保持`research_surface_status=AVAILABLE_EXPERIMENTAL`，但页面必须显著显示forward failed且不得继续展示为当前预测能力。该公式按实际tail成熟日期和实际HAC标准误计算，当前约`-0.067`只作规划示例、不得硬编码。数据、identity、coverage或non-finite失败使用各自typed reason，不混入effect failure。

### 8.3 真实产品纵切

- 使用全部development因果walk-forward OOF日期生成真实研究预测；禁止in-sample fitted prediction。另至少选择一个outcome已完整的真实历史交易日进行31-sector完整分母端到端回读；
- 新增最小`hmm_risk.rotation_l1_prediction` repository authority，保存score/state/availability/reason/model/input/revision identity；不得把非概率score伪装为`state_probabilities`；
- `GET /api/v1/hmm-risk/overview`返回能力、模型、as-of、coverage和未实现能力；
- `GET /api/v1/hmm-risk/rotation-l1?trade_date=YYYY-MM-DD`返回完整31-sector列表、score/state/availability/reason和lineage；
- `/hmm-risk`真实L1热力图直接消费上述API，展示score方向、状态、unavailable、development OOF Rank IC点估计与HAC区间、forward-confirmation状态和研究声明；不得使用mock、静态矩阵、旧Ridge输出或market regime冒充轮动预测。development未达到MBE时页面必须显著显示`AVAILABLE_EXPERIMENTAL / BELOW_BINDING_MBE`，不得显示为capability available；
- 本G2-A不注册日调度、不切换生产默认导航、不启用backend；source merge、DDL、依赖、runtime activation和用户重启分别授权。

## 9. API/DB/UI Contracts（契约）与最小持久化

旧`sector_state_timeline.state_probabilities NOT NULL`面向HMM三态概率，不能承载非概率GBDT score。禁止填造概率。已批准的唯一设计合同是单独的`hmm_risk.rotation_l1_prediction`最小表；不修改旧概率列语义，也不建设通用prediction store。schema设计已随D1～D6获用户批准，DDL实施仍须另行授权并先在DEV验证。

最小字段：`prediction_id`、`product_bundle_id NULL`、`trade_date/as_of_date`、`sector_level`固定L1、`sector_code/name`、`rotation_score NULL`、`forecast_state NULL`、`feature_contributions JSONB NULL`、`availability`、`reason_code NULL`、`research_surface_status`、`rotation_l1_capability_status`、`forward_power_status`、`forward_confirmation`、`advisory_status`、`validation_basis`、`development_oof_rank_ic`、`development_oof_rank_ic_hac_lower/upper`、`model_hash`、`input_hash`、`mapping_snapshot_hash`、`revision`、`supersedes_prediction_id NULL`、`created_at`。同一model/date/sector/revision唯一；不可原地覆盖，读取current取最大revision。research-only OOF行允许`product_bundle_id=NULL`但必须有完整model/input/mapping identity。`availability=available`时`rotation_score/forecast_state/model_hash/input_hash/mapping_snapshot_hash`必须非空且score有限；`availability=unavailable`时`rotation_score/forecast_state/feature_contributions`必须为空且`reason_code`非空。contributions仅在model/writer重构校验整体通过时写入；上述约束须在已批准schema的未来DDL与repository校验中同时实现，禁止只靠UI解释。

v1.2最小schema设计合同冻结为：`prediction_id UUID PRIMARY KEY`；`product_bundle_id TEXT NULL`；`trade_date DATE NOT NULL`、`as_of_date DATE NOT NULL CHECK(as_of_date<trade_date)`；`sector_level TEXT NOT NULL CHECK(sector_level='L1')`；`sector_code TEXT NOT NULL`、`sector_name TEXT NOT NULL`；三个score/state/contribution字段按上段条件为空或非空；五个状态字段与`availability/validation_basis/reason_code`均为受CHECK约束的TEXT；三个OOF metric字段为`DOUBLE PRECISION NULL`；三个identity hash为`CHAR(64) NOT NULL`；`tail_accessed BOOLEAN NOT NULL`；`revision INTEGER NOT NULL CHECK(revision>=1)`；`supersedes_prediction_id UUID NULL REFERENCES ...`；`created_at TIMESTAMPTZ NOT NULL DEFAULT now()`。唯一键固定为`(model_hash,trade_date,sector_code,revision)`，另建`(trade_date,sector_code,revision DESC)`读取索引，不增加通用registry、写队列或materialized view。

额外一致性CHECK必须覆盖：

- `availability='available'`时score有限且state属于`trending|neutral|fading`，`reason_code IS NULL`；`unavailable`时score/state/contributions均NULL且reason非空；
- `advisory_status='AVAILABLE'`仅允许同时满足capability=`ADVISORY_PREDICTION_AVAILABLE`、forward=`PASSED`、`product_bundle_id IS NOT NULL`与`tail_accessed=true`；
- `research_surface_status='AVAILABLE_EXPERIMENTAL'`不得单独推导capability或advisory；tail gate未通过的OOF行固定`tail_accessed=false`；
- repository写入后以同一transaction回读数量、字段、identity与canonical row hash；任一不一致整体回滚并使用typed writer/readback reason。

两个read API只读取该repository：overview固定返回五轴顶层状态、model/as-of、31-sector coverage、development OOF metric/区间与未实现能力；rotation-l1固定返回请求日期的31个canonical sector行及上述lineage。不存在日期返回typed 404，不返回空200；存在但全部unavailable仍返回31行与逐行reason，不伪造服务失败。

持久化只允许：一个development bundle、一个独立sealed tail bundle、一个compact battery receipt、两个fresh-process child receipts、一个final acceptance/failure receipt、通过时一个model/product bundle和真实产品预测。禁止保存逐树大JSON、复制完整历史输入或迁移旧artifact。两个输入bundle只是为防止tail泄漏而分离的同一G2-A输入合同，不构成两个产品阶段。

## 10. Typed reason codes

- `hmm_risk_rotation_input_contract_invalid`
- `hmm_risk_rotation_tail_not_untouched`
- `hmm_risk_rotation_label_incomplete`
- `hmm_risk_rotation_statistical_power_insufficient`
- `hmm_risk_rotation_horizon_selection_failed`
- `hmm_risk_rotation_feature_contract_invalid`
- `hmm_risk_rotation_fit_failed`
- `hmm_risk_rotation_reproducibility_mismatch`
- `hmm_risk_rotation_score_non_finite`
- `hmm_risk_rotation_development_effect_unavailable`
- `hmm_risk_rotation_development_effect_below_mbe`
- `hmm_risk_rotation_metric_direction_divergence_observed`（diagnostic-only）
- `hmm_risk_rotation_leaf_date_coverage_insufficient`
- `hmm_risk_rotation_market_context_unavailable`
- `hmm_risk_rotation_tail_effect_unavailable`
- `hmm_risk_rotation_forward_effect_failed`
- `hmm_risk_rotation_coverage_insufficient`
- `hmm_risk_rotation_product_write_failed`
- `hmm_risk_rotation_product_readback_mismatch`

reason必须保持具体stage，不得全部压成generic unavailable；异常不得吞掉，失败不得返回空success。

### 10.1 已一次性批准的精确值

| item | 批准合同 | 状态 |
|---|---|---|
| target/as-of | §3.1 raw relative return经当日L1中位数中心化；trade date `t`只读至`t-1`，outcome固定`close(t)..close(t+h)` | `USER_APPROVED` |
| `rolling_window_open_days` | `504`；按§4.3在purge后逐fold取连续504日，feature warmup最多60日 | `USER_APPROVED` |
| purge/embargo | Ridge/GBDT按horizon `purge=h canonical open days`；target-free market context不purge；`embargo=0` | `USER_APPROVED` |
| tail end | request显式绑定immutable release/source cutoff；`last_outcome_complete_open_day(source_cutoff,h)`并冻结实际日期/count/hash | `USER_APPROVED` |
| feature profile | §5.2固定9列；market必需、其余至少7/8有限，总计至少8/9；breadth/moneyflow contributor coverage>=90%且count>=5 | `USER_APPROVED` |
| linear comparator | `Ridge(alpha=100,fit_intercept=true,solver=svd,tol=1e-4,max_iter=null,positive=false,random_state=null)` | `USER_APPROVED` |
| horizon | §5.4由Ridge comparator在5D/10D间一次选择；market context跨horizon共享且不读取target | `USER_APPROVED` |
| horizon difference | 相同metric-valid日期上，paired `mean(IC_5D-IC_10D)>0.005`且Bartlett HAC lag9 one-sided t `>=1.645`时选5D，否则仅在统计完整时保留10D；MDE不参与淘汰 | `USER_APPROVED` |
| market context | MARKET-CONTEXT-A：同release CSI300 `t-1` daily return与3日population volatility；train-only z-score；K=2 jump、lambda=4.0、seed=42；semantic=`standardized daily_return-volatility_3d`、tie epsilon `1e-8`；504日target-free fold fit；5D/10D共享 | `USER_APPROVED` |
| model | §6.1 `lightgbm==4.6.0`单一完整profile；复用仓库与Conda现有pin，不安装/升级 | `USER_APPROVED` |
| reproducibility/fit budget | 两个完整fresh processes bitwise一致；battery 15、GBDT 24、总上限39 fits | `USER_APPROVED` |
| MBE | 唯一binding Rank IC 0.02；`CONVENTIONAL_PRIOR_MAGNITUDE_NOT_VALUE_DERIVED`；spread仅展示 | `USER_APPROVED` |
| MDE | one-sided alpha 0.05、power 0.80、精确z值、Bartlett HAC lag h-1；只决定forward power状态，不阻止GBDT | `USER_APPROVED` |
| stability | 月度mean Rank IC/正向比例/最差月完整报告，diagnostic-only，不形成额外promotion gate | `USER_APPROVED` |
| availability/coverage | 禁止performance-based abstention；market必需且总输入至少8/9、metric date至少28/31、development/tail daily及per-sector coverage各`>=90%` | `USER_APPROVED` |
| development gates | research gate无效果阈值；tail access要求OOF mean Rank IC>=0.02且无额外显著性AND门 | `USER_APPROVED` |
| leaf structure | `min_child_samples=310`；每叶distinct decision dates>=20；紧凑摘要 | `USER_APPROVED` |
| forward effect failure | one-sided 95% HAC upper confidence bound<=0；按实际tail计算，不硬编码示例值 | `USER_APPROVED` |
| state projection | top/bottom 20%、`q=max(5,ceil(.20*N_available))`、tie epsilon `1e-12`且跨边界整组neutral；任一extreme<5则当日spread unavailable | `USER_APPROVED` |
| contribution integrity | pred_contrib重构误差`<=1e-12+1e-10*max(1,abs(score))`；失败阻断整批writer | `USER_APPROVED` |
| DB/API contract | §9独立最小`hmm_risk.rotation_l1_prediction`表、五轴CHECK、两个read API与404/31-row语义 | `USER_APPROVED_DESIGN_DDL_EXECUTION_NOT_AUTHORIZED` |

## 11. Implementation Plan（实施方案）与文件方向

本设计合入且用户另行授权源码实施后，同一Feature范围可连续修改：

- `backend/services/hmm_risk/rotation_l1_gbdt.py`：feature/battery/model/metric纯计算；
- `backend/services/hmm_risk/rotation_l1_product.py`：model/product writer与prediction生成；
- `backend/services/hmm_risk/repository.py`或最小专用repository：持久化/readback；
- `backend/routers/hmm_risk.py`：两个read endpoints；
- `scripts/hmm_risk/prepare_rotation_l1_g2a.py`：薄离线CLI，不查询隐式DB；
- `frontend/src/app/hmm-risk/**`：真实L1热力图纵切；
- 对应backend/frontend直接测试、nox/ownership登记及本设计状态回填。

若当前真实路径命名不同，应在实现前用ownership/graph确定并回填，不得为了匹配本文创建平行router/repository/UI。任何新增requirements、DDL和runtime target分别报告并等待授权。

## 12. Verification Plan

### 12.1 设计与静态验证

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/hmm_evolution_phase2_rotation_l1_g2a_detailed_design_20260903.md --tier F2`
- `git diff --check`
- exact changed files → `file_ownership.yaml` → `module_registry.yaml` → `test_plans.yaml`

### 12.2 必须测试的模型合同

- t-1/PIT、5D/10D target、purge与tail outcome边界；
- battery禁读tail、低线性基线不淘汰GBDT、Ridge选择horizon且GBDT不得事后重选；
- MBE/MDE分离、HAC非有限/非正、功效不足只改变confirmation状态；
- 10列feature顺序/hash、按日rank、typed NaN和无neutral/前值fallback；
- 单一GBDT参数、无early stopping/grid/第二seed、两fresh-process hash、`min_child_samples=310`与每叶至少20个distinct dates；
- research gate与tail gate互斥正反例、Rank IC binding、spread非阻断及符号分歧诊断；
- forward passed/inconclusive/failed、95% HAC上界公式及禁止硬编码样例阈值；
- failure stage/reason、不把experimental surface写成capability/advisory AVAILABLE；
- 通过后31-sector真实prediction与model/input hash回读。

### 12.3 必须测试的产品合同

- repository revision/dedupe/readback；
- overview/rotation-l1 API真实OOF数据、31分母、五轴状态、typed unavailable及非概率score；
- 前端真实API L1热力图、loading/error/empty/unavailable、OOF IC/HAC区间、forward状态和可访问文本；
- Selection/Paper/QMT/QE与现有HMM gate无写入；
- 无mock浏览器验收；runtime生效等待用户重启后的fresh-process identity与business smoke。

## 13. Risks / Failure Modes（风险与失败模式）

- **统计功效不足**：新尾部时间样本可能不足以检测产品MBE；按D5返回功效不足reason，不降MBE、不伪造模型失败。
- **非线性过拟合**：31-sector日截面与重叠标签使有效独立单位更接近日期数；以浅树、强正则、single profile、rolling和untouched tail约束。
- **horizon选择偏差**：只允许预注册5D/10D规则；tail outcome不可参与选择，其他horizon不可临时加入。
- **feature mining**：10列顺序和公式在battery前冻结；禁止全库搜索、结果后删改和PCA。
- **engineering surface冒充能力**：research gate不含效果阈值，只能形成`AVAILABLE_EXPERIMENTAL`；capability与advisory升级分别受development MBE和forward合同约束。
- **叶节点日期集中**：保留容量导向`min_child_samples=310`，训练后以20日低位门和紧凑分布摘要识别结构退化；失败不得触发调参重训。
- **依赖/数值漂移**：LightGBM wheel/build、线程和版本进入identity；未授权安装或版本不符即停止。
- **假产品进度**：market context、battery、模型文件、API壳或静态页面都不能标记能力；必须完成真实端到端读回。
- **schema错配**：旧HMM概率字段不能填造GBDT概率；最小新表或兼容扩展必须在DDL前明确二选一。

## 14. Design Acceptance Index

- **F-011 / G2A-D1**：唯一`rotation_L1`产品目标、daily-centered future relative target和连续score/state语义。
- **F-011 / G2A-D2**：已消费development、新尾部、single rolling、purge/embargo与tail功效禁读边界。
- **F-011 / G2A-D3**：≤10项feature、development battery、共享market context、Ridge-only 5D/10D选择及独立forward功效说明。
- **F-011 / G2A-D4**：唯一浅层GBDT、全部参数、确定性、fresh-process、叶节点日期低位门、research/tail双门与依赖identity。
- **F-011 / G2A-D5**：Rank IC 0.02为唯一binding MBE；spread、稳定性和coverage独立展示；forward三分支不混淆低功效与失败。
- **F-013 / G2A-D6**：真实OOF prediction/repository/API/UI可先闭合experimental surface；capability/advisory状态不得越级。
- **F-012 / G2A-ISO**：advisory-only、无Selection/Paper/QMT/QE副作用。

## 15. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-011 | 本设计D1～D5；`rotation_l1_gbdt.py`、离线CLI与development immutable input writer已在独立实现分支完成；39 fits、tail读取、正式model/product writer均未执行 | `backend/tests/hmm_risk/test_rotation_l1_gbdt.py`、`backend/tests/hmm_risk/test_rotation_l1_input_bundle.py`；真实direct-v2 development bundle Windows/WSL readback；F2 validator | SOURCE_IMPLEMENTED_LOCAL_REVIEW_COMPLETE_PENDING_PR | 正式39-fit development实验只能在源码合入后的独立validation worktree执行；F-013产品链仍未实施 |
| F-012 | 本设计§1.2、§8.3；现有isolation guard | 目标`backend/tests/hmm_risk/test_isolation.py`与写表/调用边界断言 | APPROVED_BY_USER_DESIGN_READY_PENDING_SOURCE_EVIDENCE | 用户已批准advisory-only业务语义；本次没有源码、数据库或runtime变更 |
| F-013 | 本设计D6；目标prediction repository、两个read API和真实`/hmm-risk` L1热力图 | 目标`backend/tests/hmm_risk/test_rotation_l1_prediction.py`、`backend/tests/hmm_risk/test_api.py`、`frontend/tests/hmm-risk/hmm-risk.spec.ts` | APPROVED_BY_USER_REAL_OOF_EXPERIMENTAL_SURFACE_WITHOUT_CAPABILITY_DRIFT | research gate通过后允许真实OOF闭合最终工程链；不得使用mock/in-sample，也不得把`AVAILABLE_EXPERIMENTAL`冒充rotation capability或advisory AVAILABLE |

## 16. Rollout / Rollback（发布与回滚）

- 文档批准、源码合入、依赖安装、DEV DDL、实验、产品写入、runtime activation和用户重启分别报告，互不推导。
- research gate失败时不部署research surface；research gate通过但tail gate失败时只允许真实OOF experimental surface，能力仍NOT_AVAILABLE。功效不足只限制advisory升级，不回滚已闭合的真实research工程链。
- research surface或advisory纵切在writer/API/UI任一环节失败时，对应状态不得AVAILABLE；回滚本次source/DDL时不删除历史prediction或修改旧实验。
- runtime启用后回滚只停用新route/product identity，不回退Selection/QE/Paper配置，不删除模型或预测审计行。
- 本设计不授权任何发布、回滚、清理或进程控制动作。

## 17. Production Gates

- `production_ddl_gate=pending_not_authorized`：最小prediction schema设计已批准，只有源码实施阶段先在DEV验证并获得目标明确的独立授权后才可执行；
- `production_dml_gate=noop`：本设计不修复或写生产数据；
- `production_backend_dependency_gate=noop_existing_pin`：v1.2候选复用仓库与Conda已有`lightgbm==4.6.0`，不安装/升级依赖且不得修改现有NumPy；执行时版本或build不一致则另行报告，不能自行安装；
- `production_frontend_dependency_gate=noop`：不引入新前端依赖；
- `runtime_activation_gate=pending_not_authorized`：merge不授权启停服务或启用新API；
- `backend_restart_owner=user`；
- 本文档任务的DB、runtime、fit、battery、model、product和client sync均为`noop`。

## 18. 正式设计审核清单

1. **终极目标**：只交付真实`rotation_L1`预测/API/UI；experimental surface、research capability与advisory能力分别验收，HMM、battery、model或receipt均不算能力。
2. **统计语义**：MBE与MDE分离；功效不足、模型失败和数据合同失败互不冒充；tail outcome不参与功效、horizon或参数决策。
3. **因果边界**：t-1/PIT、rolling train-only、purge、已消费窗口和untouched tail明确；无未来feature或holdout选择。
4. **唯一candidate**：线性battery仅为对照，GBDT只有一个profile；无grid、early stopping、第二seed、fallback或失败后续候选。
5. **禁止简化交付**：research gate通过也须真实31-sector OOF prediction、repository/API/UI；backend-only、静态页面、market-only均不算experimental surface，更不能算capability。
6. **禁止静默错误**：typed missing/reason、NaN、non-finite、hash、partial writer与readback全部fail closed。
7. **禁止业务逻辑迁移**：其他三能力、advisory-only、旧终态、现有QE/Paper/Selection/HMM gate均不改变。
8. **禁止未经确认的门禁和审批**：只使用§10.1已批准数值；任何后续合同变化须重新获批，不新增runtime人工确认或自动研究淘汰流程。
9. **反过度工程**：一个F2、一个candidate、一个产品纵切；不建平台、不迁移历史artifact、不拆小阶段。
10. **审核完整性**：三轮审核分别覆盖蓝图状态/矩阵、统计功效/因果隔离、模型/schema/API/UI及反过度工程；任何后续精确值变化都必须重新执行本清单与F2 validator。

当前审核状态：`THREE_PASS_REVIEW_COMPLETE_DESIGN_READY_USER_APPROVED_SOURCE_IMPLEMENTATION_REVIEWED`。

## 19. v1.2 批准与复审结论

### 19.1 只读核算依据

- development calendar直接来自已验证immutable bundle canonical `9d9658bff4c7074f962903fb0e64e8de10e041b24c96d458d2b59c8b24ac57aa`：`2020-07-30..2026-03-31`共1373个open days，五个validation block分别为126/126/126/126/116日；504日rolling在5D/10D及全部fold均可执行；
- 仓库`requirements.txt`和Conda `AIstock`当前只读回读均为`lightgbm==4.6.0`；v1.2不要求安装、升级或修改NumPy；
- 旧C-012 bundle不含v1.2精确feature schema，因此只能复用其calendar/identity与底层versioned source，不允许字段改名冒充新输入；
- 本轮未读取`2026-04-01`之后的feature、outcome、IC、spread或model score，未构建tail bundle，未运行Ridge/GBDT/jump fit。

### 19.2 三轮审核结果

1. **合同/状态审核**：research surface、tail access、forward confirmation、capability与advisory保持分离；已批准设计未写成源码、模型或产品能力；
2. **统计/因果审核**：t-1/PIT、target maturity、horizon-specific purge、target-free共享market context、Ridge complete-case与GBDT 8/9 missing路径互不偷换；MDE不淘汰模型，tail effect使用candidate实际HAC；
3. **产品/反过度工程审核**：仍为一个candidate、一个39-fit上限、一个最小表、两个read API和一个真实L1纵切；不建平台、scheduler、通用registry，不迁移历史artifact，不增加performance abstention或人工审批。

结论：`PASS_DESIGN_REVIEW_USER_APPROVED_SOURCE_IMPLEMENTATION_REVIEWED`。用户已批准全部§10.1值及MARKET-CONTEXT-A；源码实现、实验、tail、DDL、产品链和runtime仍按各自证据独立报告，任何单项PASS不得推导其他状态完成。

### 19.3 2026-09-06 direct-v2 v3消费适配状态

- 正式reader不再硬编码月度candidate目录名；每次request必须显式传入绝对candidate root，并从经校验的`direct_monthly_state.json`派生`release_id/cutoff`。不扫描`latest`，不回退旧`20260902`目录、数据库、factor `sector_data.h5`或L2数据。
- 通用direct-v2入口显式区分v2/v3且拒绝未知schema；G2-A v1.2专用入口只接受v3，并强制同release的`sw_l1_index` receipt、meta和H5全部闭合。v2只保留旧C-012兼容，不得进入G2-A。
- v3股票训练分母使用同release `stock_universe.txt`；`all.txt`中`selection_eligible=false`的`000300.SH`只作为benchmark，不再被误判为缺少股票limit/factor字段。daily-basic/moneyflow同时支持并严格区分已批准fixed与pandas table H5布局。
- Windows与WSL `rdagent-gpu` fresh process均只读验证：31 sectors、45,787 rows、1,477 open days、`2020-07-30..2026-08-31`、close非有限/非正均为0；CSI300 close与L1 close日期集均为1,477日。该证据只证明正式source adapter可执行，不证明G2-A feature、candidate、tail或产品验收完成。

### 19.4 2026-09-06 G2-A v1.2源码实现与真实development输入复核

- 新增`rotation_l1_gbdt.py`与`run_rotation_l1_g2a.py`，实现批准的504日rolling、5D/10D Ridge battery、MARKET-CONTEXT-A、唯一浅层GBDT、两次fresh-process复现、叶节点日期回读、research/tail双门及39-fit上限；没有grid、early stopping、第二seed或结果后重试。
- immutable development bundle严格保存8项连续feature、5D/10D target、逐列validity/reason与target maturity；数值NaN必须有非空typed reason，有限值不得携带失败reason。CSI300 market context只在fresh process内按批准合同拟合，不进入输入面板伪装成预计算状态。
- direct-v2 Qlib首先以header/文件长度验证完整release identity，再通过bounded memmap只物化`<=2026-03-31`数值；不会因release含2026-04-01之后数据而读取tail outcome，也不会错误要求完整release落入development窗口。
- SW L1 H5的taxonomy `sector_code`必须经同release一对一`index_code`映射为C-013 canonical published code后才能与PIT聚合连接；L2聚合改为copy-on-write，禁止原地污染供G2-A使用的L1 identity。
- 真实r4 development bundle readback：manifest SHA-256=`ad2e3b5e04e6eee76438f62e95c8bd2fcade994ea6bc7504bbc935a171a0fc13`，`2020-07-30..2026-03-31`共1,373日、31 sectors、42,563 rows；五个validation fold的Ridge 8/8与GBDT至少7/8连续feature行覆盖均为100%；Windows与WSL回读manifest hash一致。
- tail只保存不含outcome的calendar maturity identity：5D=99、10D=94；未读取`2026-04-01`之后feature/outcome/score，未运行15+12+12 fits，未选择最终horizon，未写model/product，未执行DDL、DML或runtime action。
