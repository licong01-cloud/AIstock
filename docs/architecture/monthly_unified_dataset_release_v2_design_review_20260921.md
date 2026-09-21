# 共享月更 v2 详细设计审核记录

审核对象：[详细设计](monthly_unified_dataset_release_v2_f2_design_20260921.md)与[实现验收矩阵](monthly_unified_dataset_release_v2_acceptance_20260921.md)。交付类型docs-fast-new / DESIGN-MAIN-001，仅设计文档。审核方式为本窗口连续多轮自审，不冒称独立专家或多代理审核。

## 当前状态

三轮自审与修订完成，设计内容审核通过。文档格式/链接/索引检查通过；后续PR的CI与merge以GitHub记录为准。设计通过不表示功能已实现；30项功能验收保持pending。

## 审核计划

1. 需求和实现事实审核：对照本月故障、当前producer/reader、现行Skill，逐项覆盖。
2. 增量与分布式故障审核：演算复权分类、snapshot/恢复、hash依赖、COW、切换竞态和HMM登记。
3. 可开发性与交付审核：检查接口字段、错误语义、consumer矩阵、迁移路径、验证证据及文档链接/范围。

每轮记录实际发现、修改位置和复核结果；最后保留未完成的实现项，不以裸PASS或文档合入冒充生产成功。

## 第一轮：需求、现有合同与身份依赖

核查了direct_monthly、incremental/COW、adj reconciler、两条daily_basic入口、shared_sector_context字段、precompute_hmm_coefficients和当前profile reader。发现与修订：

| 编号 | 发现 | 修订 / 复核 |
|---|---|---|
| R1-01 | source_dataset_manifest_sha256若指最终manifest会与component receipt循环 | §4.3加入同轮B0来源manifest；它仅作provenance，不产生第二个可选release |
| R1-02 | 数据封存与最终容器封存混用，可能诱导封存后追加文件 | §3/§7分开DATA_SEALED和final容器发布；C/E不进入B自引用 |
| R1-03 | 旧baseline PASS不足以证明新增字段检查通过 | §5.3明确只对新增规则涉及的历史范围补验，不全量重导 |
| R1-04 | 补录水位与DML非原子时可能漏掉历史变化 | §5.2明确同snapshot水位、完成readback和观测空隙处理 |
| R1-05 | 多模型需求与系数内嵌producer路径未定义 | §8.1加入derivation_key、required资产集合与来源路径非执行路径语义 |
| R1-06 | canonical算法和回执最小字段不够具体 | §4.2补digest自引用排除、8类schema领域字段和计数单位 |

第一轮修订已覆盖以上发现。F2检查器识别30条设计/矩阵，未缺章节或ID；因30条未实现且无功能测试证据返回FAIL，符合本次design-only预期，不能作为功能验收PASS。

## 第二轮：实际writer、恢复与幂等

进一步逐段读取RD-Agent `scripts/dump_bin.py::DumpDataUpdate`、AIstock authoritative exporter及现有profile激活实现，并按失败顺序推演。

| 编号 | 发现 | 修订 / 复核 |
|---|---|---|
| R2-01 | dump_update一次全载入CSV，分钟增量也可能爆内存 | §6.3明确data-owned有界writer，不能仅换CLI参数；真实Qlib读取验收 |
| R2-02 | dump_update的all.txt是单区间dict；writer错误可能只记日志 | §6.3将PIT finalizer独立，要求各子输出实际核验，不以退出0为成功 |
| R2-03 | exporter start/end既用于payload又用于sidecar/meta | §6.3拆成payload、dataset、selection、qfq basis四组范围，保护历史区间 |
| R2-04 | 按股票最后成交日直接追加可能错位；分批推进公共日历可漏写 | §6.3从header/数组长度定位、合法NaN槽位、所有feature后提交公共calendar |
| R2-05 | 激活后同idempotency_key重试若先解析active会产生新任务 | §3.1先查原幂等请求，再决定是否解析新predecessor |

本轮还复核了§9锁内CAS、replace前后崩溃与第三方指针冲突、节点先部署后中央切换以及历史retry冻结：设计保持完整old或new绑定，不依赖三端同时替换文件。以上发现已修订；未执行真实writer或生产切换。

## 第三轮：可开发性、消费边界与交付真实性

| 编号 | 发现 / 核验项 | 修订 / 结论 |
|---|---|---|
| R3-01 | API成功与后台数据阻断的错误语义需明确 | §4.1补HTTP层与operation层状态、底层cause及错误族 |
| R3-02 | 只引用staging证据可能在清理后不可读 | §4.2明确root锚及必要provenance随release保存 |
| R3-03 | 只写新closure可能遗漏原dataset-identity必需ST等字段 | §4.3列出现有reader关键字段和全部profile pins；保留complete=true门禁 |
| R3-04 | 上游不可补齐不能被“一键”承诺掩盖 | §14明确来源保留/权限限制，自动化不伪造补齐 |
| R3-05 | HMM/QE/因子/Selection/Advisory/择时边界与历史结果 | §8规定只接入共享release的新任务，旧request/模型/线上行情authority不改 |
| R3-06 | 功能验收与设计验收可能混淆 | 设计与独立矩阵均保留pending；本次F2实现检查预期FAIL，不用文档审核冒充测试 |

30个F-ID在设计和矩阵逐行一一对应，无遗漏或重复；内部章节引用、8个本地Markdown链接、代码围栏通过只读结构检查。最终变更范围仅3份普通架构文档，未修改代码/Skill/标准/数据库/数据集/profile/进程。

## DESIGN-COMPLIANCE-001 四项审核

- 完整设计交付：覆盖源数据、增量正确性、恢复、consumer、部署、切换、回滚和测试；代码实施明确尚未完成。
- 错误可见：未解释缺失、半写入、旧HMM绑定、节点漂移、激活读回失败均有明确处理。
- 业务合同：不调整模型、收益、股票池、黑名单窗口、每日adj检查或既有冻结研究。
- 审批范围：复用已有动作授权；不增加逐组件确认、资源准入或全历史source recheck门禁。

## 本次验证与后续使用

- docs-fast验证：`git diff --check`；提交暂存前后各确认实际3文件scope。
- 文档结构检查：F-ID 30/30、matrix pending 30、local links 8/8、章节引用和fences一致。
- F2实现检查：识别30/30条目，无缺章节或ID；FAIL仅来自尚未实现、计划验证不是执行证据。后续开发须填真实证据并让此检查PASS。
- 未运行模型/业务测试，因为此次没有可执行代码变更；没有DEV/生产DML、实验、训练、源抓取或服务控制。
- 后续功能实现以本设计和30项矩阵为依据；旧未提交v1草案按需参考，不作为已完成实现依据。

设计审核结论：PASS（design-only）。功能实施/三端发布/一键切换生产验收：NOT_IMPLEMENTED_BY_THIS_PR。
