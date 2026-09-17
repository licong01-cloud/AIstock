# Position Timing 独立现金账户收盘实验（F1）

状态：IMPLEMENTING；用户已确认实验方向，尚无收益结论。不改变在线策略，不覆盖 PT-NEXT-020 旧产物。

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
