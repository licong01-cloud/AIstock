# 固定行业代表股逐臂仓位合同（F1）

## 背景

用户已授权承接共享revision329～330：目标SHA保持f743ce621f6727b9e4754bec999c0718b9f901ec5839cc75dc68492dbe3df154，1000万元、2024-07-02～2026-10-29完整564日及原分钟执行费用，仅T_rep/B_rep两账户，零训练、推理及参数搜索。代表缺失各自留现金导致105日目标仓位不同，最大差0.005658168574401601。两臂差异包含代表可用性与现金政策，不能称严格同敞口的纯趋势效应。

## 范围与非目标

范围为QE可选absolute-target消费者、结果收集与对应直接测试。非目标为修改研究目标、重训或推理、增加实验臂、默认策略变更、生产包发布和服务控制。

## 实施方案

保留`qe_absolute_target_replay_v1`的字段集及每日等仓位校验。新增显式`qe_absolute_target_replay_v2`，额外必填`exposure_policy=per_arm_frozen_targets_no_renormalization`。v2使用同一不可变联合8列目标，各臂直接消费自己的原始绝对NAV权重，未分配预算留现金；不得缩放、补位、归一化或更改目标文件。

逐日v2回执记录当前arm的target_exposure、两臂target_exposure_by_arm、两臂target_cash_by_arm及T减B目标仓位差；实际仓位和现金仍以Qlib账户为准，不假设等于目标。v2终端收据汇总差额天数、最大差额和逐臂平均目标仓位／现金，并保留版本化政策。v1终端收据与原等仓位语义不变。结果收集严格验证合同版本和v2政策、仓位审计，禁止v1结果冒充v2。

目标覆盖、SHA、原时点、131行业与UNKNOWN、非负权重、每臂不加杠杆、完整日历、原分钟成交算法与费用校验全部保留。只改QE消费者和直接测试，不修改研究资产、数据集、旧任务、默认模型策略或生产包。

## Design Acceptance Index

- F-001：v1默认及等仓位拒绝行为完整保留，不接受v2政策混入v1。
- F-002：v2需明确版本及固定政策，逐臂消费原权重，无额外缩放／默认安全值。
- F-003：v2逐日记录本臂及两臂目标仓位、现金与差额，不冒充实际敞口。
- F-004：终端结果保留v2版本／政策／汇总审计，收集端拒绝缺项或不一致。
- F-005：冻结真实564日资产fresh-process读取及完整Composer通过，105日差额与来源一致；零训练和推理。
- F-006：真实DEV正式任务配置验证，双节点hash/身份、查重和两轮编排审核后仅两账户；启动与运行加载分开记录。

## 验证方案

`python -m pytest -q backend/tests/quantevolver/test_qe_absolute_target_consumer.py`：36通过。`python -m pytest -q backend/tests/unified_engine/test_qe_custom_evo_status.py backend/tests/unified_engine/test_backtest_executor.py`：58通过。changed-file Ruff与diff检查通过。

真实冻结目标的Windows完整消费者及两臂Composer均通过：每臂2756086行、564日、105日目标仓位差；v1对同一原目标仍拒绝不等仓位。WSL原生Qlib逐臂完整读取、首末日真实日线／分钟报价检查通过，零训练／推理／账户。真实DEV正式任务与登记事务及独立回滚读回通过。源码、预检、任务提交和运行验收分别记录。诊断初版误假定首日有3只正权重目标，而T首日实际2只，已修正为真实抽样数并保留失败；未修改目标或交易规则。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/quantevolver/qe_absolute_targets.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py | PASS | 无 |
| F-002 | backend/services/quantevolver/qe_absolute_targets.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py | PASS | 无 |
| F-003 | backend/services/quantevolver/qe_absolute_targets.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py | PASS | 无 |
| F-004 | backend/services/quantevolver/qe_absolute_target_record.py / backend/services/quantevolver/absolute_target_consumer.py | backend/tests/quantevolver/test_qe_absolute_target_consumer.py | PASS | 无 |
| F-005 | 正式消费者和Composer | validation-receipt: tmp/handoff/qe-parent-representative-longtask-20261117/native_consumer_v1.json | PASS | 无 |
| F-006 | 正式请求入口和既有执行并行机制 | validation-receipt: tmp/handoff/qe-parent-representative-longtask-20261117/DEV_validation_v1.json | PASS | 无 |

DESIGN-COMPLIANCE-001逐项复核：F-001～006实现与直接证据齐备；未将冷读取冒充账户收益、未吞掉诊断失败、未改变冻结目标及原默认合同、未增加设计外审批或放宽数据身份。上述PASS是源码及提交前验收；真实两账户执行、用户API重启及经济结论继续分别记录，不宣称已经完成实验。

## 风险与失败模式

新合同包含代表缺失现金效应，不能把T−B直接归因为严格同仓位趋势择时。目标仓位不等于实际持仓；整手、现金、停牌和涨跌停可产生偏差，必须回看真实账户。合同版本、政策、SHA、131行业／UNKNOWN、完整时点或逐臂回执不一致均拒绝消费；不能为完成实验而修改来源或补位。旧v1仍保留每日等仓位校验。

## 生产门禁

F1逐项验收、两轮作者审核、直接测试和CI后可沿既有授权提交合入。后端由用户重启；不得通过热改运行模块规避加载验证。先核验新版本运行能力，再正式单次提交。原T/B全部复用，父包修复独立推进，不因这一接口变更重复父包训练或账户。
