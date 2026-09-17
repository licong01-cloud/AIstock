# Position Timing 独立现金账户收盘实验（F1）

状态：EXPERIMENT_EXECUTED_WITH_EXPLICIT_COVERAGE_LIMITATIONS。prepare/run/inspect/exact retry 已完成；未证明原 R0 有超额收益。源码未合入，不改变在线策略，不覆盖 PT-NEXT-020 旧产物。

## Execution Evidence（2026-09-17）

- 代码冻结 commit：`f2f2fda533d8840bc3cec7c7e4f3914394ab2056`；后续设计格式补齐不改变 Python 实现身份。
- 定向测试 17 passed；changed-file Ruff、compileall、diff --check 通过；F1 validator 8/8 通过（设计闭合，不表示收益验证成功）。
- request：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/pattern_close_cash_benchmark_v1/requests/745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff.json`。
- request canonical SHA：`745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff`；文件 SHA：`961bd3baca5fd2b836f1aaee29642db22359a0bd6b338f2faba3eedde75d035e`；实验合同 SHA：`f299ac66c86036855a18010a7b0f222f0e2faa98ccb467351688c63e725e8a9b`。
- source preflight：5,144 股、7,970,157 factor 行，invalid/insufficient 均 0，coverage_complete=true；factor audit `1131d05c5fa7f4c84d50eaec428cdd8359ee2cf1119ccea12afbef806ccc36bd`，restatement audit `48b78cc7237fc02d4c0d840f3ef63c7cb9ddc6ef46325330b8b7acc3b2dd32ac`。
- prepare 时 outcomes_read=false；run 在 `RUN_AFTER_FULL_SOURCE_PREFLIGHT` 后才设置 true。161 个分块全部完成；inspect=VERIFIED；exact retry=ALREADY_MATERIALIZED，request/bundle 身份不变。
- bundle：`F:/Dev/AIstock_model_artifacts/position_timing_advice_v1/research/pattern_close_cash_benchmark_v1/bundles/745c4ea3b5038532535bb86c71ac192891ac30cbac532ae6f81c543511c9f2ff`；manifest canonical SHA：`a304822c9c6bb09ca4fae151d35e6dad48c17944226f0cf5c47258b02191d444`。包含 request、report、stocks.parquet、pool_daily.parquet、receipt，receipt 绑定所有分块及每日路径/成交事件/诊断文件。
- 5,144 个诊断身份全部保留，5,138 条股票路径、8,067,196 条日账户记录；负净值/仓位超过100%/提前成交均为0。未知估值日记录3,290；4,807只股票具备完整配对期间收益，1,613只择时胜过持有（33.56%）；配对超额中位数 -26.89 个百分点。6只无完整指标历史：001221.SZ、301491.SZ、301609.SZ、301632.SZ、603262.SH、603406.SH。
- L0：`python -m nox -s l0 -- backend/services/position_timing/pattern_close_cash_benchmark.py backend/services/position_timing/pattern_close_cash_replay.py backend/services/position_timing/pattern_close_cash_report.py backend/tests/position_timing/test_pattern_close_cash_benchmark.py` 通过，0 finding / 0 blocking。代码期间没有修改。

## 结果与解释

以下均为成本后独立账户的等权日收益合成指数，**仅限当日可评价配对样本的诊断**，不是完整全人口收益，也不是共享现金组合。完整人口字段在存在未知成员日时返回 UNAVAILABLE；没有将未知收益设为0。所有指数为对应官方价格指数。

### 各自期间的诊断总收益

| 股票池 | 日收益起始日（终日均2026-08-31） | 择时 | 长持 | 对应指数 |
|---|---|---:|---:|---:|
| 全市场 | 2018-08-31 | 9.93% | 176.15% | 沪深300 38.02% |
| 沪深300成分 | 2018-08-31 | 8.09% | 67.63% | 38.02% |
| 中证500成分 | 2018-08-31 | 6.87% | 109.05% | 63.38% |
| 中证1000成分 | 2018-08-31 | 4.61% | 114.24% | 51.94% |
| 科创50成分 | 2020-08-05 | 不可计算完整期间 | 不可计算完整期间 | 7.56% |
| 科创100成分 | 2023-08-08 | 7.95% | 58.23% | 缺失，不替代 |

科创50前21个日收益日（2020-08-05～2020-09-02）没有可评价配对账户，禁止删掉这些天伪称完整期间收益。核查688001.SH：raw行情始于2019-07-22，stock PIT始于2020-08-04；现有 pattern_feature_frame 将 PIT 掩码前置到特征构造，因而需要重新预热。这不是“没有更早行情”的结论，本轮不修改既有特征语义。

### 统一期间：2023-08-08～2026-08-31（743个日收益，基准起点收盘2023-08-07）

由 bundle 的 `pool_daily.parquet` 选择 mode=dynamic、date>=2023-08-08，按 report.comparison 同一百分比复合公式读回；没有重新回放股票。以下仍是可评价配对样本诊断。

| 股票池 | 择时 | 长持 | 对应指数 | 择时−长持（百分点） |
|---|---:|---:|---:|---:|
| 全市场 | 5.64% | 46.36% | 沪深30015.91% | -40.72 |
| 沪深300成分 | 3.94% | 18.12% | 15.91% | -14.18 |
| 中证500成分 | 4.20% | 31.24% | 30.56% | -27.04 |
| 中证1000成分 | 3.02% | 27.27% | 19.47% | -24.26 |
| 科创50成分 | 5.88% | 39.81% | 73.85% | -33.93 |
| 科创100成分 | 7.95% | 58.23% | 缺失 | -50.28 |

完整期间动态池未知账户日/预期账户日分别为：全市场46,597/7,913,933；沪深300 1,110/567,801；中证500 1,432/941,297；中证1000 7,229/1,864,200；科创50 2,189/67,762；科创100 507/72,637。它们包括尚未具备指标历史的账户日与未知估值配对日，不等于3,290个原始未知估值行。跨股票对比也受不同可用起点影响；不可把配对分布解释成共同起点的正式 alpha 检验。

### 资金与交易机制诊断

- 逐股时间平均仓位再等权平均为12.41%；持仓时间占比14.58%；条件持仓比例均值83.30%。两项均值的乘积不要求严格等于总体均值。
- 入场确认62,669次：成交27,792次、价格guard拦截34,112次（54.43%）、涨停拦截753次、停牌12次。现金不足最小手数/预算失败为0，不能再把低仓位归因于未给足账户现金。
- 价格guard买入拦截中，超过最大买入价20,951次，开盘缺口超过上限13,133次，接近涨停28次；它们是冻结 guard 的实际原因，不能归因于成交量或止盈阈值。
- 成交卖出27,198次中，冻结风险退出19,797次，加速放量退出7,401次。另有卖出价格guard拦截3,814次、跌停350次、停牌38次；重试次数不是独立交易机会。
- 全市场自身期间诊断中择时最大回撤6.02%，长持32.18%；较低风险主要伴随较低暴露，不能将回撤优势当作超额收益优势。

### 下一步研究建议（未执行、不自动调参）

先拆分“原始信号机会→价格guard拦截→实际入场→风险退出”的收益损失，尤其检查突破锚点与后续回踩确认价格的兼容性；再冻结小规模退出/分步止盈对照。单独评估 PIT 交易资格与历史特征可用性的解耦，而不是本轮事后放宽掩码。科创100指数序列仅交回数据准备窗口；不要求重新引入账户级公司行动。不得宣称当前规则最优或已有稳定超额收益。

## Background / Feature Card

目标是检验同一股票择时相对长期持有的成本后收益，分离股票池选择和择时贡献。每股两账户各 1000 万元，无杠杆、追加资金或跨股票资金流动，现金不计息，自然复投。只运行冻结 R0，不搜索止盈、退出或基础筛选参数。

## Contracts

- F-001：显式绑定 r5 manifest `7b5402c38b4b279140375fa6517595f88bdfb472e617faf8b032c04f0d33d1c1`、父研究 manifest 和源码。先完整 factor/restatement/PIT source audit，再读取本次收益；公司行动账户清算、数据库、行情网络与服务控制全部禁止。
- F-002：沿用纯 Qlib adjusted OHLC/volume、factor=1 信号合同。买入按 raw 股数取合法手数，再以 `raw_quantity / source_factor` 记虚拟单位；卖出清空虚拟单位，名义成交额为虚拟单位乘复权价格。不把因子变动解释成真实股份到账。本轮不模拟券商股份账本。
- F-003：T 收盘决策、T+1 收盘模拟成交；涨停禁止买、跌停禁止卖、停牌不成交；缺失限制数据为 UNKNOWN，不当成无涨跌幅限制。保留冻结价格 guard 与风险 guard；风险退出优先并持续至可执行，普通信号不无限延期；T+1 可卖。买入按实际现金含逐腿费用重新缩量，保留 guard 减量。
- F-004：R0 breakout/pullback/acceleration 使用既有纯函数；不调模板与阈值。新账户使用当期权益而非初始资金上限；退出仍保留既有经济盈利定义与边沿状态。持有账户首次可执行买入后不主动调仓。主指标为共同终日 MTM，另报终日可清算净值，禁止为成交提前截断研究期。
- F-005：完整 2018-08-01～2026-08-31 source 历史，指标就绪后开始，不继承模型训练 756 日 burn-in。每股仅回放一次；买入使用全市场 PIT，持有不因指数进出强制清仓。停牌且有历史合法估值可注明 stale 沿用；非停牌缺失持仓估值为 UNKNOWN，不得默认为零或删除。
- F-006：六类股票池使用前一决策日 PIT 身份，等权日账户 NAV 百分比收益复合，明确是合成研究指数而非共享资金组合。另报年度首日 PIT 固定成员子群。所有聚合附预期/有效/未知计数；不完整日不得发布为完整全体业绩。个股、全市场用沪深300；csi300/csi500/csi1000/star50/star100 分别用各自官方指数。禁止缺失时替代。
- F-007：输出逐股 timing/BH/CSI300，逐池 timing/BH/对应指数、总收益/年化/回撤/超额、分布、年度与共同区间，仓位时间/条件仓位/费用/买卖原因/无法成交。指数为价格指数，不伪称总回报指数，复权股票与价格指数差异须披露。全样本已观察，仅 EXPLORATORY，不声称样本外 alpha。
- F-008：新独立 immutable request/chunk/bundle，prepare→run→inspect→exact retry。校验输入/源码/文件 hash，重试复用相同身份，禁止覆盖历史。只增加择时模块、定向测试和本设计，不修改共享实现或默认值。

## 已知数据边界

r5 index_context 有 000300.SH、000905.SH、000852.SH、000688.SH，尚未发现科创100指数价格序列。科创100择时/持有照常计算，指数收益标 UNAVAILABLE，不自行补数。各指数仅在其存在的共同日期比较，起始前不回填。所有未知估值和不足指标历史的股票仍保留诊断人口。

## Scope / Non-goals

只增加 `backend/services/position_timing/pattern_close_cash_{replay,report,benchmark}.py`、`backend/tests/position_timing/test_pattern_close_cash_benchmark.py` 和本设计。未改变在线 API、其他模块、共享默认值、数据生产或既有研究合同。使用模块现有纯计算，不新增训练、选股、多 agent 或通知系统。

## Risks

复权虚拟单位不是券商股份；官方价格指数不含现金股息，不能与复权股票口径伪称完全相同。日收盘且无冲击是研究假设，不是实盘可成交承诺。无授权无涨跌幅限制标志的缺失限制数据记 UNKNOWN。非停牌缺失持仓价格保留未知终值，不利用未来退市信息提前卖出。动态 PIT 等权指数不代表可实现共享组合。未知成员日使 full_population 指标不可用；另报的 paired_observed_only 仅为条件样本诊断，不包装成全市场完整结论。

## Verification Plan

`python -m pytest backend/tests/position_timing/test_pattern_close_cash_benchmark.py -q`：两轮修复后 17 项通过，覆盖现金费率/手数、复权缩放、自然复投、T+1、方向涨跌停、未来数据不改变既往路径、停牌/未知估值、PIT 滞后分组、指数缺失、空人口以及不可变 artifact。另做 changed-file Ruff、compileall 和 git diff --check。全市场 prepare/run/inspect/exact retry 为实验验收证据，尚未完成前不得标为实验完成。

## Production Gates

不涉及生产变更；database_read/write、行情 network、runtime/process control 均为 false。不需要用户重启。只写 timing-owned 研究产物。没有模型效果准入门槛，负结果照常交付；真实数据不足只标记相关比较不可用，不补造数据、不挡住其他比较。

## Implementation Plan

1. 实现最小离线收盘现金 replay 与分组报告，冻结合同和定向测试；不新增 API、调度或模型平台。
2. 多轮静态审核与账户/因果/哈希定向测试后，完整 source preflight 和全市场运行；逐股 checkpoint 可恢复。
3. inspect、exact retry、结果解释和真实缺口报告；未完成项如实标明，不将负收益改为调参循环。提交与 PR 遵守当前授权，未经新授权不合入。

## Design Acceptance Index

F-001～F-008 的定义见 Contracts；下表 DESIGN_VERIFIED 仅表示设计闭合，不表示全市场实验已完成或收益有效。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/position_timing/pattern_close_cash_benchmark.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-002 | backend/services/position_timing/pattern_close_cash_replay.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-003 | backend/services/position_timing/pattern_close_cash_replay.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-004 | backend/services/position_timing/pattern_close_cash_replay.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-005 | backend/services/position_timing/pattern_close_cash_replay.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-006 | backend/services/position_timing/pattern_close_cash_report.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-007 | backend/services/position_timing/pattern_close_cash_report.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
| F-008 | backend/services/position_timing/pattern_close_cash_benchmark.py | backend/tests/position_timing/test_pattern_close_cash_benchmark.py | DESIGN_VERIFIED | none |
