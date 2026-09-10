# 因子库质量盘点：P2 操作与交接

使用[研究方法](../analysis/factor_research_methodology.md)和[P2设计](../architecture/factor_research_p2_detailed_design_20260909.md)。本入口只读既有评价结果，不读取行情或重新计算因子。研究记录复用P1两表，不另建平台。

## 执行

```powershell
python scripts/factor_research.py quality --env-file F:/Dev/AIstock/.env --target dev --as-of 2026-08-31 --history-start 2025-01-01 --recent-start 2026-07-01 --format summary
```

`--format json` 返回逐因子细节；可重复 `--factor` 指定范围，不指定时查询当前数据库全部catalog。summary不打印大规模成功明细。`--as-of`是数据/指标snapshot截止，当前库不是历史as-known目录；输出以现有源行为依据，不宣称复原过去可见目录。先用create登记研究任务，再quality获取观察，通过record保存结果与结论；show/list恢复。生产目标仅在用户授权后选择，本轮未操作。

## 如何解释

- reviewed表示有可展示的官方指标观察；不代表因子有效、可晋升或可淘汰。pending表示先补对应依据，不能删除目录项缩小分母。
- comparisons使用同批次/同评价口径。overlapping=true只作描述，不能当独立检验。月度weighted IC按有效n_days加权，但缺batch/horizon/universe关联，始终descriptive_only。
- correlation按绝对值提供线索，负相关也算信息重合；有效配对数缺失始终明确。不得把0当独立证明，或用新硬阈值自动删除。
- source_references仅历史来源引用，consumer_dependencies=unverified_not_unused不是无人使用。改进/替换关系用record decision保存source_catalog_ids、proposed_version、reason、next_action；对应owner需求用request。实际生产替换、禁用、历史清理另授权。
- 质量查询没有任何写入，无法读取/结构错误直接返回operation_failed且不打印连接秘密，不回退到另一数据库。输入错误返回invalid_request，不增加业务审批。

## 本轮 DEV 结果（2026-09-09）

研究 task `921f8531-5e64-467d-9dba-e9476c8fe75f`；result record `6783101f-f694-43a0-b4d2-90873153df4b`。既有DEV `127.0.0.1:5433/aistock_dev` 的10个目录项全部观察，50条官方指标、970条已有月度IC、45对相关性。请求范围内月份为2025-01..2026-08；完整结果和重点结论保存在DEV，不把临时工作树或本文件当研究唯一权威。

全部10项均有monthly basis和correlation sample support限制，无自动因子处置。生产库未查询，不能声称生产全库完成；研究两表生产迁移仍另授权。没有重新导出数据或运行新的因子/模型计算。

重点观察（仅DEV已有结果，不作收益承诺）：

| 对象 | 观察 | 下一步 |
|---|---|---|
| semivariance shift，catalog 12 | recent_3m h1 IC=0.009308，h20 IC=-0.122631，原方向=-1；不同期限表现不能混为统一有效性 | 先对照原用途/期限与成熟样本，再决定改进；不根据近期符号改方向 |
| return-volume coupling，catalog 18；semivariance，12 | 已有spearman_ewma相关性0.603221，仅为配对比较线索，不是淘汰依据 | FR-REQ-01补有效配对与可比增量证据，不自动替换 |
| index beta break，catalog 14 | coverage由recent_6m 0.621956到recent_1m 0.598171；h1和h20表现不同 | 先解释覆盖分母/适用范围，低相关不能单独证明增量 |

8月h20值为空不补零；其确切缺失/成熟度原因由原评价结果及owner解释。本研究任务revision=4：已保存重点decision、4项owner request和下一步，状态completed只指这轮DEV诊断结束，不是生产全库治理或P3交付完成。

## Owner交接与后续顺序

1. 用户授权P1两张研究表生产迁移及生产只读盘点后，执行精确preflight、迁移、readback；不复制DEV指标或实验到生产。生产诊断另建立任务，以实际生产目录为分母。
2. FR-REQ-01因子相关性owner提供真实可比配对支持。FR-REQ-04指标owner提供monthly basis关联或明确不可获得；本窗口不改正式writer。
3. FR-REQ-02 QE数仓、FR-REQ-03 QE趋势诊断需求已入DEV request；这里只是需求准备，未声称owner已接单/交付，不启动任何QE实验。
4. 收到对应接口后研究窗口进行只读验收，继续明确因子的保留/观察/改进/替换建议；生产处置必须再取得具体授权。

P1源码已合入PR #4448；profile同步由单一client-sync owner处理，当前业务skill未安装不能声称已部署。必要backend-main重启由用户完成，研究CLI fresh-process加载验证不代替服务运行身份验证。
