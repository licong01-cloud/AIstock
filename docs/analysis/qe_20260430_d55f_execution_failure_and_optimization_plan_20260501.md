# qe_20260430_010121_d55f 执行失败核查与后续优化实验计划

生成日期：2026-05-01

任务：`qe_20260430_010121_d55f`

重点 Loop：`Loop5`, `Loop10`, `Loop14`, `Loop15`，并参考 `Loop6`, `Loop9` 作为持仓接近上限的对照。

## 1. 结论

本次补充核查不再使用推测口径，而是直接检查以下证据：

- Qlib 回测产物：`indicators_normal_1day_obj.pkl` 的逐日订单 `amount/deal_amount/ffr`。
- Qlib 1min 行情：`$open`, `$close`, `$volume`, `$factor`, `$limit_up`, `$limit_down`, `$up_limit_price`, `$down_limit_price`, `$prev_close`。
- 本地 DB：`market.suspend_d`, `market.stk_limit`, `market.kline_daily_raw`, `market.kline_minute_raw`。
- 策略代码：`tail_twap_v25_strategy.py`, `tail_twap_strategy.py`, `score_weighted_strategy.py`, `qrun_limit_minute.py`。

明确结论：

1. Material 级别成交偏差主要来自停牌/无 Qlib close、涨停买入受阻、跌停卖出受阻，以及少量现金/计划切分导致的部分成交。
2. 已抽查的非停牌 material 失败不是 DB 日线或分钟线缺失；这些案例通常有完整 241 根分钟线。
3. 涨跌停价格比较口径总体统一：V25 将复权 OHLC 用 `$factor` 转回 raw price 后，与 raw 涨跌停价比较；Qlib Exchange 层使用 `$limit_up/$limit_down` 布尔字段限制交易。
4. 存在价格精度/交易单位导致的轻微偏差，但 `ffr>=0.99` 的偏差不是主要失败原因。
5. 组合计划是 50 只，但不需要机械要求每天正好 50 只；建议允许 50 附近波动。但 `MaxPos=76/81/97/101` 已超出合理范围，应设置软上限，例如 `MaxPos<=65`、`P95持仓<=60`。

## 2. 实际成交偏差分类

说明：

- `TotalDev`：订单层存在 `ffr<1` 或 `deal_amount` 与计划量不一致的记录数。
- `Round/Prec`：`0.99<=ffr<1`，主要是交易单位、复权因子、金额裁剪、价格精度造成的轻微偏差。
- `Material`：`ffr<0.99` 或无成交价/无成交量的实质偏差。
- `Suspend/NoClose`：停牌或 Qlib close 全日缺失。
- `BuyLimitAll`：买入时全天涨停，无法成交。
- `BuyLimitPart`：买入后盘中/尾盘进入涨停，只能部分成交。
- `SellLimitAll`：卖出时全天跌停，无法成交。
- `SellLimitPart`：卖出后盘中/尾盘进入跌停，只能部分成交。
- `Cash/Sched`：无停牌/涨跌停证据，主要表现为现金约束、V25 分钟计划切分或执行计划未覆盖全量。

```text
+------+----------+------------+----------+-----------------+-------------+--------------+--------------+---------------+------------+
| Loop | TotalDev | Round/Prec | Material | Suspend/NoClose | BuyLimitAll | BuyLimitPart | SellLimitAll | SellLimitPart | Cash/Sched |
+------+----------+------------+----------+-----------------+-------------+--------------+--------------+---------------+------------+
| 5    | 271      | 127        | 144      | 78              | 22          | 29           | 0            | 8             | 7          |
| 10   | 276      | 132        | 144      | 80              | 22          | 26           | 0            | 10            | 6          |
| 14   | 214      | 126        | 88       | 43              | 5           | 22           | 3            | 7             | 8          |
| 15   | 294      | 178        | 116      | 37              | 31          | 23           | 2            | 12            | 11         |
+------+----------+------------+----------+-----------------+-------------+--------------+--------------+---------------+------------+
```

解读：

- Loop5 和 Loop10 的 material 偏差都是 144 条，且停牌/无 close 分别为 78/80 条，是主要阻塞来源之一。
- Loop15 的买入全天涨停阻塞最多，为 31 条；这会促发尾盘替代买入，增加额外持仓。
- Loop14/Loop15 出现少量全天跌停卖出阻塞，虽然数量不大，但会导致旧仓无法退出。
- `Round/Prec` 数量不少，但基本是轻微偏差，不应和真实失败混为一类。

## 3. 价格口径核查

关键代码证据：

- `tail_twap_v25_strategy.py` 将复权价转换为 raw：`raw_price = adjusted_price / factor`。
- `tail_twap_v25_strategy.py` 使用 raw close 对比 raw `$up_limit_price/$down_limit_price`。
- `qrun_limit_minute.py` 订阅 `$up_limit_price`, `$down_limit_price`, `$prev_close`, `$factor`，注释说明用于复权 OHLC 转 raw 后与 raw limit/pre_close 比较。
- `conf.yaml` 的 Qlib Exchange 层使用 `limit_threshold: ["$limit_up", "$limit_down"]`，即交易限制不是用价格重新计算，而是使用 Qlib bin 中的布尔涨跌停字段。

抽查结果：

```text
+-----------+-----------+------------+---------------+-----------+-------+-------+---------+-------+--------+---------+---------+
| Case      | Stock     | Date       | Side/Reason   | QCloseRaw | QUp   | QDown | DBClose | DBUp  | DBDown | MinBars | Suspend |
+-----------+-----------+------------+---------------+-----------+-------+-------+---------+-------+--------+---------+---------+
| Suspend   | 002494.SZ | 2024-12-31 | SELL no close | NaN       | 4.39  | 3.59  | NaN     | 4.39  | 3.59   | 0       | S       |
| LimitAll  | 002628.SZ | 2024-08-02 | BUY all up    | 2.18      | 2.18  | 1.78  | 2.18    | 2.18  | 1.78   | 241     | -       |
| LimitPart | 300535.SZ | 2024-10-16 | BUY 223m up   | 16.27     | 16.27 | 10.85 | 16.27   | 16.27 | 10.85  | 241     | -       |
| LimitPart | 603839.SH | 2024-11-15 | BUY 222m up   | 5.83      | 5.83  | 4.77  | 5.83    | 5.83  | 4.77   | 241     | -       |
+-----------+-----------+------------+---------------+-----------+-------+-------+---------+-------+--------+---------+---------+
```

结论：

- `002494.SZ` 在 2024-12-31 是明确停牌：`suspend_d=S`，DB 日线/分钟线无交易数据，Qlib close 全日为空。这不是价格精度问题。
- `002628.SZ` 在 2024-08-02 是全天涨停：Qlib raw close、Qlib up limit、DB close、DB up limit 均为 2.18；买入失败是涨停阻塞，不是缺价格。
- `300535.SZ` 在 2024-10-16 有完整分钟线，尾盘涨停 223 分钟；部分买入失败是盘中/尾盘涨停导致，不是 DB 缺数据。
- `603839.SH` 在 2024-11-15 存在复权因子：Qlib adjusted close=5.75097，factor=0.986444，换算 raw close=5.83，与 DB close/up limit=5.83 对齐；说明复权/未复权口径在该案例中统一。

## 4. 持仓数量判断

策略目标是 50 只股票，但实际组合可以在 50 附近波动。合理原则不是“每日必须等于 50”，而是不能长期、显著偏离。建议用软约束判断：

- 正常：`MaxPos<=60`。
- 可观察：`61<=MaxPos<=65`。
- 需要降风险：`66<=MaxPos<=75`。
- 严重异常：`MaxPos>75` 或 `P95持仓>65`。

当前异常 Loop：

```text
+------+------------------+--------+---------+---------------------+------------------+
| Loop | Model            | MaxPos | Days>50 | MainIssue           | Judgement        |
+------+------------------+--------+---------+---------------------+------------------+
| 5    | LGB 10D HMM      | 97     | 380     | stale + substitute  | too high         |
| 10   | LGB 10D noHMM    | 101    | 389     | stale + substitute  | too high         |
| 14   | LSTM 10D noHMM   | 81     | 414     | stable rank + stale | too high         |
| 15   | TCN 10D HMM      | 76     | 281     | substitute + limit  | too high         |
| 6    | CatBoost 10D HMM | 63     | 276     | near soft cap       | acceptable watch |
| 9    | GRU64 10D HMM    | 61     | 279     | near soft cap       | acceptable watch |
+------+------------------+--------+---------+---------------------+------------------+
```

形成高持仓的实际机制：

1. 外层 `topk=50` 只决定目标排名，不是最终真实持仓硬上限。
2. `n_drop=5` 每日最多换出少量股票，旧仓可能持续滞留。
3. 停牌、涨停买入、跌停卖出会造成未成交。
4. `TAIL_SUBSTITUTE` 会把未买入资金转向备选股；如果卖出也受阻，旧仓和新备选仓会叠加。
5. 当前收益高的 Loop 恰好也是高持仓 Loop，因此后续不能直接把高收益全部归因于模型，需要做软上限回放验证。

因此建议不是强行固定 50，而是先测试 `MaxPos<=65` 的软上限。如果收益仍保持领先，说明模型 alpha 更可信；如果收益明显下降，说明原收益部分来自超额持仓暴露。

## 5. 后续实验优先级

优化顺序应先排除执行层与配置变量，再进入模型超参。建议如下：

```text
+-----+-----------------------+-------------------------------------------------+-----------------------------------------------+----------------------------------------+
| Pri | Scenario              | Config Change                                   | Purpose                                       | Pass/Fail Gate                         |
+-----+-----------------------+-------------------------------------------------+-----------------------------------------------+----------------------------------------+
| P0  | S0 audit replay       | add order/hold audit, no alpha/HMM change       | make data traceable                           | 100% failed orders classified          |
| P0  | S1 soft-cap guard     | allow 50 around, cap max holdings 65            | remove extreme 80-101 positions               | MaxPos<=65, P95<=60                    |
| P1  | S2 no Alpha158        | 10D LGB noHMM, disable Alpha158                 | test alpha baseline as independent factor set | AnnRet/MaxDD not worse vs alpha        |
| P1  | S3 no HMM default     | 10D LGB/Cat/TCN with HMM off                    | current HMM has no return gain                | noHMM >= HMM after cap                 |
| P1  | S4 5D horizon         | LGB/Cat/TCN/GRU/XGB, same factors               | check shorter horizon                         | better DD or similar AnnRet            |
| P1  | S5 20D horizon        | LGB/Cat/TCN/GRU/XGB, same factors               | check smoother horizon                        | lower turnover/DD without stale excess |
| P2  | S6 substitute policy  | TAIL_SUBSTITUTE vs no substitute/limited backup | measure extra holdings contribution           | return survives MaxPos<=65             |
| P2  | S7 factor ablation    | alpha group, custom groups, top IC groups       | select factor set before models               | higher IC/RankIC and lower corr        |
| P3  | S8 HMM retune         | stronger coeff/preset after S2-S5               | only if HMM rank effect useful                | AnnRet up and DD not worse             |
| P4  | S9 hparam tune        | TCN/LGB/Cat/GRU/XGB only                        | optimize shortlisted models                   | improve after fixed config             |
| P5  | S10 weak model repair | LMart objective, TabPFN subset                  | research only                                 | RankIC positive first                  |
+-----+-----------------------+-------------------------------------------------+-----------------------------------------------+----------------------------------------+
```

执行要点：

- S0/S1 是诊断门槛，不是为了追收益；没有这两项，后面所有收益比较都会混入执行噪声。
- S2 优先测试去掉 Alpha158，因为 Alpha158 不应默认无条件加入；应作为独立因子组做 ablation。
- S3 优先默认去掉 HMM，因为当前 Loop5 vs Loop10 的同模型对照显示 HMM 没有提升年化收益。
- S4/S5 测 5D/20D 是必要的，因为 1D 到 10D 的收益差异很大，说明 label horizon 是核心变量。
- S6 验证 `TAIL_SUBSTITUTE` 是否是收益来源或风险来源，尤其要看 MaxPos 被限制到 65 后收益是否还在。
- S9 模型超参放在后面，因为如果因子组、HMM、horizon、执行约束未定，先调超参容易过拟合当前 contaminated 回测。

## 6. 模型优化方向

```text
+------------+--------------------+------------------------------------+---------------------------+
| Priority   | Models             | Reason                             | Tune Later                |
+------------+--------------------+------------------------------------+---------------------------+
| Main       | TCN, LGB, CatBoost | highest current AnnRet/IR band     | yes, after factor/horizon |
| Main watch | GRU64, XGBoost     | good signal, lower than top tier   | yes, limited grid         |
| Secondary  | LSTM               | return good but high stale holding | only after soft-cap       |
| Low        | LambdaMART         | current RankIC/return weak         | repair objective first    |
| Low        | TabPFN             | DD good but return/RankIC weak     | use as defensive subset   |
+------------+--------------------+------------------------------------+---------------------------+
```

当前不建议把 LambdaMART 和 TabPFN 放在主线：

- LambdaMART 当前收益和 RankIC 表现弱，优先检查 ranking group/objective/label，而不是直接调参。
- TabPFN 最大回撤较好，但收益不足，更适合作防守子模型或候选池二次排序，不适合作主收益模型。

## 7. 必须新增的实验输出

为了后续优化不再依赖人工猜测，建议所有后续 QE 回测默认输出以下审计文件：

```text
+-------------------+-------------------------------------------+--------------------------------+
| Output            | Required Fields                           | Why                            |
+-------------------+-------------------------------------------+--------------------------------+
| order_audit       | date,stock,side,amount,deal,ffr,reason    | distinguish suspend/limit/cash |
| price_basis_audit | adj close,factor,raw close,DB close,limit | prove unified basis            |
| holding_audit     | date,count,in_top50,out65,stale_days      | control extreme holdings       |
| substitute_audit  | blocked buy,backup stock,backup rank,cash | measure TAIL_SUBSTITUTE        |
| hmm_rank_audit    | raw rank,adj rank,coeff,sector,enter/exit | decide HMM suitability         |
| factor_ablation   | factor group,IC,RIC,corr,SHAP,coverage    | screen Alpha158/custom groups  |
+-------------------+-------------------------------------------+--------------------------------+
```

最低要求：

1. 每个未完全成交订单必须有唯一 reason。
2. 每个 reason 必须能关联到 Qlib 字段和 DB 原始字段。
3. 每日输出持仓数量、Top50 内持仓、Top65 外持仓、旧仓滞留天数。
4. HMM 必须输出 raw rank 与 adjusted rank 的差异，否则无法判断 HMM 是有效调节还是噪声。
5. Alpha158/custom 因子必须按因子组输出 IC、RankIC、相关性和覆盖率，避免无条件全量加入。

## 8. 建议的下一轮最小实验集

为了控制实验数量，建议先跑以下最小矩阵：

```text
+------+---------------+----------+-------+----------+----------------+
| Step | Model         | Horizon  | Alpha | HMM      | ExecConstraint |
+------+---------------+----------+-------+----------+----------------+
| 1    | LGB           | 10D      | on    | off      | audit + cap65  |
| 2    | LGB           | 10D      | off   | off      | audit + cap65  |
| 3    | LGB           | 5D       | off   | off      | audit + cap65  |
| 4    | LGB           | 20D      | off   | off      | audit + cap65  |
| 5    | CatBoost      | 5/10/20D | off   | off      | audit + cap65  |
| 6    | TCN           | 5/10/20D | off   | off      | audit + cap65  |
| 7    | GRU64         | 5/10/20D | off   | off      | audit + cap65  |
| 8    | XGBoost       | 5/10/20D | off   | off      | audit + cap65  |
| 9    | Best 2 models | best     | best  | tuned/on | audit + cap65  |
| 10   | Best 2 models | best     | best  | off      | substitute off |
+------+---------------+----------+-------+----------+----------------+
```

如果 Step 2 去 Alpha158 后不劣于 Step 1，后续默认不再全量加入 Alpha158，而是只保留经过筛选的 Alpha158 子集。

如果 Step 9 的 tuned HMM 仍不优于 HMM off，HMM 暂时不作为收益主线，只作为风险控制或行业状态解释工具。
