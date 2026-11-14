# QE 冻结权重消费入口 F1 设计

## Background / 背景

用户于2026-11-14授权执行阶段总结中的收敛方案。原任务
`qe_20261113_152355_f90c`剩余三个组合账户要求继承原逐日rank[-1,1]
及固定LSTM/FUND权重0.6966591521/0.3033408479。现有`ic_weighted`
会从评价面板重新估计权重，不能表达该合同。本设计只闭合正式消费入口。

## Scope / 范围

- `backend/routers/multi_alpha.py`：preview/run显式接收`frozen_weights`。
- `backend/services/multi_alpha/combiner.py`：新可选`frozen_weighted`精确计算。
- `backend/services/multi_alpha/combine_backtest.py`：校验、快照、零指标读取和收据。
- `backend/services/multi_alpha/durable_execution_adapter.py`：实际materializer沿同一入口消费。
- `backend/tests/multi_alpha/test_frozen_weighted_contract.py`及本设计。

## Non-goals / 边界

不改变旧IC/equal/RRF/Borda行为或默认值；不训练、重新推理、调权、修改原包、
数据集、HMM、执行策略或生产资产。三账户仍属原预算。API重启由用户执行，
源代码交付与运行加载、真实账户验收和经济发布分别记录。

## Design Acceptance Index / 设计验收索引

- F-001：新显式模式校验完整权重及来源；不重估、不归一化、不静默忽略。
- F-002：逐日平均并列rank映射[-1,1]后固定加权；全部有限预测键严格相同。
- F-003：REST、请求快照、哈希和恢复保留精确合同，旧请求身份保持。
- F-004：legacy和durable实际materializer均只加载保存预测，不读取指标估权。
- F-005：消费收据可追溯原权重/来源/尺度；源码交付与运行/经济验收分开。

## Implementation Plan / 实施方案

请求仅显式选择`weighting_schemes=["frozen_weighted"]`、`normalize_method="rank"`、
`walk_forward={"enabled":false}`。新增`frozen_weights`：

```json
{
  "schema_version": "qe_frozen_weights_v1",
  "weights": {"LSTM": 0.6966591521, "FUND": 0.3033408479},
  "source_ref": "原批准合同或冻结manifest指针",
  "source_sha256": "原合同或manifest实际SHA256"
}
```

来源字段记录调用方声明的出处，入口不把字符串校验冒称原资产已核验；正式实验
预检仍须核对该原件及实际权重。权重覆盖全部且仅有roster腿，值必须是有限非负数，
禁止bool，和须在1e-12内为1；保留传入数值，不自动归一化。
不允许与其他scheme混选、滚动估权、zscore或RRF参数混用。三腿以上开启LOO时拒绝，
避免删腿后擅改权重；两腿原默认不产生LOO。原计划请求明确关闭baseline和LOO。

以既有预测解析得到canonical键，拒绝无效值、重复键和腿间任何键差异。每腿每日期
独立升序average rank，n>1时`2*(rank-1)/(n-1)-1`，单股票为0，然后按原权重相加。
不按未来标签筛选，不向交集裁剪。原有多seed均值由既有prediction loader负责；
本轮每个seed两腿一一配对，不复制seed或声称恢复旧33+5集成。

快照仅在使用新模式时增加`frozen_weights`，避免改变旧请求哈希。
结果weights保持原字典，per-window证据记录来源及冻结消费语义；不重新读取OOS
IC/TopK指标。所有正式执行继续走原durable准入、幂等、数据绑定和全局并发机制。

## Verification Plan / 验证方案

独立算术oracle覆盖已知rank/并列/单股票/多日、权重反证、缺失/重复/不等键、
无标签及不同IC不影响冻结结果；请求通过Pydantic→parse→snapshot→reparse→实际
durable materializer，验证保存预测消费、精确输出和来源。重试复用快照，权重改变
必须改变请求身份。旧IC模式直接回归。真实实验前再核验原565日预测及共同键，
执行预算仅3个账户；源码测试不冒称已完成该经济验证。

## Risks / 风险与失败模式

权重来源失实由实验预检拒绝；schema/键/时序变化直接报错，不补默认权重或分数。
旧预测加载器可能先做seed均值，必须由原始消费收据证明该聚合合法；本入口不重建
原始seed资产。新代码未加载时不得用旧API提交新字段。已观察历史不称未消费holdout。

## Production Gates / 生产门禁

无DDL/DML或依赖变更。DEV为fresh-process真实消费验证，不创建测试数据库。
不写生产策略包，不自动晋升。运行激活等待用户重启8001；远端和WSL服务无需本功能重启。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/multi_alpha/combiner.py:validate_frozen_weights; backend/services/multi_alpha/combine_backtest.py:parse_request | backend/tests/multi_alpha/test_frozen_weighted_contract.py | pass | 无 |
| F-002 | backend/services/multi_alpha/combiner.py:normalize_frozen_prediction_frame; MultiAlphaCombiner.combine | backend/tests/multi_alpha/test_frozen_weighted_contract.py | pass | 无 |
| F-003 | backend/routers/multi_alpha.py; backend/services/multi_alpha/combine_backtest.py:request_snapshot_for | backend/tests/multi_alpha/test_frozen_weighted_contract.py | pass | 无 |
| F-004 | backend/services/multi_alpha/durable_execution_adapter.py; backend/services/multi_alpha/combine_backtest.py:build_prediction_only_legs | backend/tests/multi_alpha/test_frozen_weighted_contract.py | pass | 无 |
| F-005 | backend/services/multi_alpha/combine_backtest.py:per_window_weights_payload | backend/tests/multi_alpha/test_frozen_weighted_contract.py | pass | 无 |

fresh-process DEV读取原772身份保存的AF/FUND预测，限定三日14,751行，沿公开
`materialize_child_input`和既有float32 writer核对独立rank算术oracle；计算前后均逐值相等。
这是消费功能的真实资产夹具验证，无训练/推理/回测/传输/DB写入，不冒称本批三个LSTM
业务验收或565日组合结果。全565日原生收集及正式publish、运行加载、三账户配对仍沿
原实验合同接续。首次全窗DEV读取因可用内存低于8GiB在加载前停止，后改为有界三日；
初始float64对float32断言差异保留，验收分别验证原公式及既有精确float32序列化，未放宽容差。

DESIGN-COMPLIANCE-001作者逐项审核：完整覆盖五项消费功能，无默认回落；原rank
与固定权重语义核对；默认策略和IC估权行为不变；没有追加实验预算或生产审批门槛。
本设计不授权重启、生产资产写入或自动晋升。相关回归交CI，源码合入与运行验收分别报告。
