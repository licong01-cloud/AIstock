# 择时共享 CandidateReader 有界窗口 F1 设计

日期：2026-10-28；状态：IMPLEMENTED_SHARED_INTERFACE_AND_SEAM_VERIFIED。上位合同：[LocalSIM性能设计§2.1/§5.1/§8](localsim_replay_performance_f2_detailed_design_20261028.md)。

## Background / Scope / Non-goals

只在原CandidateReader增加物理读取优化，不创建第二Reader，不改变逐日MarketView、原始价格恢复、停牌投影、可见时间、来源SHA或交易策略。LocalSIM #6785已提供消费接缝；完整批量账户验收由LocalSIM窗口负责。本轮独立短窗口只验证已合入消费入口，不操作活动181日研究。

精确写入：`backend/services/position_timing/intraday/research_inputs.py`；`backend/tests/position_timing/test_intraday_reader_window.py`；本设计；`docs/analysis/position_timing_bounded_reader_delivery_20261028.md`。临时脚本和性能回执仅在`X:/AIstock_temp/position-timing-bounded-reader-20261028/`，WSL对应`/mnt/x`；不复制大型数据。

## Architecture / Contracts

正式入口保持`CandidateReader(manifest)`、`file/calendar/values/market/verify_pins`；新增`prepare_window(start: date,end: date,symbols)`、`release_window()`、幂等`close()`与只读资源报告`window_status`。close后读取报RESEARCH_READER_CLOSED；release_window可再次准备，替换先释放旧物理缓存。无类级/进程级共享缓存。

窗口最多20个交易日、最多1024个去重股票，日期为date、start<=end；资源参数非法显式失败，不调整股票人口。窗口只是预取域：实际读取域不受它限制，原values仍可读域外日期/股票。缓存仅持有raw物理浮点切片，不持有MarketView、PIT资格、价格偏好、信号、账户状态或未来分钟prefix。

每个bin只读映射，取窗口覆盖的只读数组后立即关闭映射；保留数组LRU总预算512MiB。日历日期偏移、报价跨度索引、停牌窗口共享物理索引的缓存总预算64MiB；超出预算只不保留该索引/切片，继续原必要读取且报告miss/eviction，不改变研究模式。最多一个本Reader拥有的bin映射和一个短生命周期I/O流；不跨调用保留文件句柄。预算是Reader保留payload，不是整个Python/Arrow/消费者MarketView的RSS保证；实际RSS/FD单独测量。文件/股票/频率/区间键绑定本Reader的candidate完整SHA和组件SHA；不使用mtime/size代替内容SHA。

预取的缺文件、缺价、历史不足和不合法字段不产生新的业务失败，不进入consumed pins；实际消费时原校验仍执行，保留原异常。cached payload在一个消费校验周期首次使用时重新核对完整文件SHA；`verify_pins()`仍完整复核所有累计已消费文件并开启下一周期，不能用缓存或stat跳过。prepare不能代替verify；消费方仍须在使用/发布当日结果前调用verify_pins。同内容重试不改身份，修订不能隐藏。

日历偏移只计算一次；报价跨度按symbol索引；停牌窗口一次过滤读取，但每次market仍按同一symbol/day调用唯一投影器，保持原receipt和错误。实际market返回仍是当日完整候选行情；核心只看到当时可见分钟前缀，此职责不迁移到物理缓存。

## Implementation Plan

同一交付包：公共接口和直接测试→多轮等价/故障审核→冻结文件WSL阶段计时与LocalSIM现有消费入口短窗口→定向回归、L0、F1、CI→PR合入。不等待后端重启，不启动完整181日研究，不改变已冻结源码、journal或旧288产物。

复杂度：日历日期索引由每字段O(全日历)降为每Reader一次O(全日历)、后续O(当日bar数)；报价解析每缓存周期一次；停牌Parquet每窗口一次；价格恢复、bar构造、完整内容SHA仍按原合同执行。不预设总体提速倍数。

## Verification Plan / Design Acceptance Index

- F-001：公共准备/替换/释放/close和明确资源预算；原入口兼容、独立Reader隔离。
- F-002：多日完整MarketView、bars、raw价格、停牌receipt/来源/时钟与原逐日读取一致。
- F-003：必要输入错误和内容修订fail closed；未消费股票/日期/字段缺价不新增失败；UNKNOWN/NONE不变。
- F-004：只读数组、未来窗口不进入当前决策；资源释放和缓存预算可观察。
- F-005：冻结文件WSL来源检查/准备/端到端计时、RSS/FD及冷热说明；LocalSIM现有入口消费命令和独立短窗口结果。

## Rollout / Rollback / Risks / Production Gates

纯离线接口交付；新消费者显式prepare_window。活动回放继续旧工作树；不热更、不恢复、不改断点。本窗口不修改LocalSIM或其设计。完整LocalSIM批量账户验收、运行态激活、经济有效性分别待owner报告。DB/网络行情/数据变更/进程控制均false；backend_restart_required=false。回滚使用未prepare的既有调用方式或旧源码新研究，不覆写旧结果。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | research_inputs.py::CandidateReader | backend/tests/position_timing/test_intraday_reader_window.py | VERIFIED | none |
| F-002 | research_inputs.py::market/values | backend/tests/position_timing/test_intraday_reader_window.py；artifact:docs/analysis/position_timing_bounded_reader_delivery_20261028.md | VERIFIED | none |
| F-003 | research_inputs.py::file/verify_pins | backend/tests/position_timing/test_intraday_reader_window.py | VERIFIED | none |
| F-004 | research_inputs.py::prepare_window/close | backend/tests/position_timing/test_intraday_reader_window.py | VERIFIED | none |
| F-005 | docs/analysis/position_timing_bounded_reader_delivery_20261028.md | artifact: X:/AIstock_temp/position-timing-bounded-reader-20261028/comparison-v1.json | VERIFIED | none |
