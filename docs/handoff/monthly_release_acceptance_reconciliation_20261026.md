# 数据准备遗留验收对账（2026-10-26）

关联[Feature #4988](https://github.com/licong01-cloud/AIstock/issues/4988)、[30项矩阵](../architecture/monthly_unified_dataset_release_v2_acceptance_20260921.md)。本次为docs-fast证据修订，不改变发布、PIT或业务合同。

## 1. 原月更与当前修订必须分别定位

| 范围 | 身份 | 实际状态 |
|---|---|---|
| 原9月月更 | operation `dmr_63a313d97fd046a38d5a876265565014`；generation `20260930-monthly-v2-unified-correction-4ab0b48642c3`；manifest `522e8e35896e85fc9289ca2ceca1cd6127f4c7978a72766887c0429dc9b7f410` | 六阶段checkpoint、READY完整，state=ACTIVATED_VERIFIED |
| 当前统一修订 | operation `dmr_916ac005468448e2b169887505af7e6d`；generation `20260930-monthly-v2-unified-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1`；manifest `afa9158b9b1d8cc76ec1e40a6efdb2060365bb3039ea791a0b5caa2bae8c5337` | 正式adoption/CAS激活，不是又一次全量导出；旧版本保留 |

当前release=`qe_hmm_full_v2_20260930`，manifest文件SHA256=`3d416575b4dbb3cdf6d17552908c38fcc03c96f4fe4b12fa30c6ee4d1db6a36a`，profile SHA256=`276cdf7a0d0da7582af37b8ce03167017c6695bc7172bedbaf0c6dea01503dbd`，本轮前后未变。

当前候选目录名为 `20260930-qe_hmm_full_v2-direct-monthly-v2-unified-candidate-correction-4ab0b48642c3-margin1-facts1-suspend1-suspendfacts1`，三端父目录分别为 `X:/AIstock_dataset_candidates/backtest_dataset_candidates/`、`/mnt/wsl/aistock-qe-data-v1/releases/`、`/home/lc999/data/dataset_candidates/`。

## 2. 证据索引与范围

以下均为repo-external证据，不依赖本次临时工作树。只重读必要元数据/合同，不重复hash全部行情文件。

| ID | 路径及身份 | 本轮结论 |
|---|---|---|
| E1 | `F:/Dev/AIstock_runtime_state/monthly-dataset-release-v2/monthly/dmr_63a313d97fd046a38d5a876265565014/receipts/ready.json`；canonical `77687bb545c1c66738ebaf11157ba28970c862b96934e536bb146629d0427cd4` | 六阶段receipt digest与checkpoint一致；SOURCE attempt255，其余attempt294；原11项真实result PASS、unresolved=0、副作用标记false |
| E1汇总 | `X:/AIstock_temp/DATA_PREPARATION/remaining-20261025/monthly-six-stage-readback-20261026-r2.json`；文件SHA256 `8567bd899165f8619aa2f81852e4066ebbbecc40f4e2c4e954548bd4893d86ba` | 保留原manifest、逐阶段attempt/plan/input/producer、telemetry及每个consumer窗口/result_ref |
| E2 | `X:/AIstock_temp/DATA_PREPARATION/longtask-20261025/formal-activation-readback.json`、`activated-three-node-readback.json` | 当前精确profile ACTIVATED_VERIFIED；双节点fresh-process及现有API complete=true、同manifest；旧任务未改 |
| E3/E4汇总 | `X:/AIstock_temp/DATA_PREPARATION/remaining-20261025/acceptance-readback-20261026.json`；文件SHA256 `030feed6d1cc535939ff2b66c0f881ae07d914e4b6444cb3b06abdedc2734ae8` | 当前closure、派生资产、11项准备合同；原95个DEV身份及生产默认PIT独立只读核对 |

E1真实result位于 `X:/AIstock_dataset_release_control/monthly/dmr_63a313d97fd046a38d5a876265565014/consumer-validation/attempt-294/`。包括QE single/custom/multi-alpha/P10/P11、HMM file-only、factor、Selection、Advisory、择时、统一回测。P11记录1个黑名单行业、5662个物化区间、Qlib日线1行/分钟240行；HMM节点读回548日。它们证明对应probe执行，不是训练、收益或所有历史键的验收。

E3当前11项 `aistock_monthly_consumer_contract_v1` 均为 `READY_FOR_NODE_READBACK`，固定范围及组件引用，不能冒充当前输入实际smoke PASS。原manifest结果、当前节点身份、当前准备合同分别保存，禁止跨manifest替换旧回执。

当前closure文件SHA256=`019bf11de8329c6374228b8a5198ac27326a3e283e84987edd3dea249de6f096`，canonical=`eeed7a730b72a60f0e0c601258731b9dd1fb805117f9b65453158081f43788b5`。当前系数文件SHA256=`a7c04003797da347413903c4d4dd262ddc8978ed35e9f75315935d9fb1ef4e10`，内嵌manifest匹配；本轮未运行producer，不以身份匹配冒充重新生成证明。

## 3. 性能不能扩大解释

E1成功阶段合计 **4.512446小时**：SOURCE 5950.328秒、BUILD 1829.780秒、DERIVE 11.655秒、LOCAL_VALIDATE 103.859秒、DEPLOY 6382.905秒、CONSUMER_VALIDATE 1966.280秒。

合计排除早前失败attempt、人工修复、等待、暂停，SOURCE和其余阶段也不在同一attempt。不能说本月实际只花4.5小时、增量/全量已等价，或下月10小时内/1—3天必定完成。BUG-1891的10万行2.973526→0.161728秒是局部性能证据，不代替整月SLA。

## 4. BUG-1685：95个DEV profile并非尚未迁移

[#5293](https://github.com/licong01-cloud/AIstock/pull/5293) 已合入 `2b7250e3467c1419e6faca41c5df2c702724a0ec`。按原apply receipt精确95个profile/原目标版本ID，本轮只读确认：原版本95/95保留且hash未变；准备目标95/95保留且hash未变；95/95 current_version仍为当时目标，均分类为 `canonical_pointer`。本轮不写DEV/生产、不改历史Selection lease或账户。

生产另经正式resolver读回：`ACTIVE_CANONICAL`，authority=`aistock_equity_pit_canonical`，universe=`aistock_equity_pit_canonical_v2`，rule=`shsz_a_252td_st_delist_asof_v2`，generation=1，覆盖 `2018-08-01..2026-10-26`。不再报告生产仍legacy/generation0。

这不把原v15三字段执行投影变成五字段FrozenPitSnapshot，也不补写旧租约。[#5285](https://github.com/licong01-cloud/AIstock/issues/5285)保留OPEN的剩余项，是原v15业务迁移应由迁移/消费者owner按后继正式激活明确接续或替代；不是再做95次写入。范围决策明确后走正常关闭，不为清理而修改旧任务。

## 5. 三项历史运行合同

| BUG / Issue | 原source | 真实目标及剩余 |
|---|---|---|
| BUG-1683 / #5271（关闭同步#5279） | #5273 / `f031502f9dc2ba35092ea13af95fcadc8887ec62` | backend-main + worker-scheduler，旧单目标合同不足 |
| BUG-1693 / #5315（关闭同步#5356） | #5355 / `2bc5f0e0911204f5efb7f4451db355d2a31db2e2` | backend-main + worker-scheduler，旧范围/目标需正式迁移 |
| BUG-1793 / #5693 | #5695 / `7a890d648e0754cee70e17b99ed0cbbf6e2f6872` | backend-main + tdx-go-backend，launcher --check不是TDX运行态验收 |

各Issue已有原source及逐目标接续说明。未收到workflow owner的正式多目标登记/拆分路径；本轮不改公共workflow、不缩小changed_files、不伪造单目标PASS、不重启TDX。

## 6. 剩余顺序与动作边界

本轮仅闭合矩阵F-015/023/029/030，26项保留实际缺口；不是当前数据使用的新门禁，9月release继续已激活。下一步复用既有修复回执，归并变化范围/字段/消费者证据，仅对未验证变化执行只读reader；不把旧manifest PASS复制成新PASS，不再检查无关全历史。故障、等价、缓存、性能和下一月演练独立验收，不影响已冻结任务。完整Feature仍未通过，不关闭#4988。

修改仅为本文及矩阵，临时脚本/读回在X盘。production_ddl_gate=noop，database_write=false，candidate_write=false，profile_write=false，process_control=false，training/experiment=false。无需后端/API重启。

多轮审核：区分原operation/adoption、准备合同/真实result、DEV准备/生产激活、成功阶段合计/整月SLA。DESIGN-COMPLIANCE-001四项：不冒充完整交付；不隐藏未验收项；不改变PIT/数据/实验语义；不新增审批、全历史门禁或重启要求。
