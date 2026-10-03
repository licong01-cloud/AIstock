# 月更组件依赖级准备与安全采用设计

Feature ID: monthly_component_preparation_v1

Tier: F2

关联：BUG-1693 / Issue #5315；既有月更 Feature #4988。

## Background

9 月唯一 operation 的 `margin_detail/2026-09-30` 未完成来源发布，完整 SOURCE 正确阻断。然而 day/minute/index/PIT 等无融资融券依赖的准备也被整批停止。本设计补充[共享月更 v2](monthly_unified_dataset_release_v2_f2_design_20260921.md)的恢复、一致输入、私有构建、安全采用与分态验收，不替代其最终发布合同。

## Scope

同一 operation、同一 cutoff、同一最终 candidate。准备产物只能在配置的 X 盘私有 staging；不得注册成 release、部署成消费候选、封存全量 SOURCE 或切换 active。最终必须重新通过完整 SOURCE、BUILD、DERIVE、LOCAL_VALIDATE、DEPLOY、CONSUMER_VALIDATE。延期来源不视为合法 NA 或 provider absence。

不修改 HMM/QE/策略合同，不写数据库，不调用训练/实验，不安装依赖，不控制进程。仅生产源码合入可能需要用户重启后端，源码、运行态和月更完成分别报告。

## Non-goals

不发布不完整 release，不通过删字段/删股票解除融资延期，不建设新的候选分支、数据库、用户审批或训练系统。不承诺上游到齐时间。独立准备不是业务消费者可以使用的临时 release。

## Architecture

1. 小型控制读取仍在正式一致快照内核对 PIT、日历、writer ledger、schema、refresh readiness。
2. 依据代码持有的完整依赖图计算 `ELIGIBLE` / `DEFERRED`；ELIGIBLE 只表示可以继续检查，绝不表示数据完整或输出已完成。未知来源、控制面异常、未证明一致性均阻断全部准备。
3. 仅读取合格组件的依赖，在一个一致视图中流式封存、检查；按正式物化器导出私有组件。避免读取延期来源和无关数据。准备收据使用独立 schema，不冒用完整 SOURCE、artifact-ready 或 candidate receipt。
4. 每个完成单元记录输入有效内容身份、PIT、QFQ 分母、schema/producer/字段规则及输出 hash。未完成单元不得发完成收据。操作锁沿用月更产品锁，不增加 HTTP 同步长任务或第二个 worker。
5. 来源就绪后执行完整 SOURCE。以完整 SOURCE 实际输入重新计算组件依赖身份；只采用完全相同的准备产物。不同快照的输入集合不合并为一个 SOURCE；早期计算结果可以在全输入依赖等价后采用。
6. producer、规则、PIT、QFQ 或任何相关事实改变时，该单元标记 `INVALIDATED` 并重建依赖闭包。margin 到齐改变全局 source root，不应仅因此使无融资依赖的组件全部重导。
7. 已采用的输出要验证文件集合、SHA、路径安全和私有 inode，再进入最终统一校验。不存在有效准备输出时走原正式构建；不能将存在目录或 mtime 当完成证明。

### Dependencies

物理生产器的输入与逻辑组件的依赖必须取并集，不能只使用增量 action 推导表。所有组件还依赖交易日历和 canonical PIT 控制 authority。

| 组件 | 事实/authority 依赖（除共同控制） |
|---|---|
| day | raw daily、adj、limit、suspend、stock_basic、index_daily |
| minute | raw minute、raw daily、adj、limit、suspend、stock_basic |
| factor/static | raw daily、adj、daily_basic、moneyflow、bak_basic、cyq_perf、margin_detail、sector、stock_basic、SW 分类/成员 |
| index | index_daily |
| suspend | suspend_d、limit、stock_basic |
| benchmark | raw daily、adj、index_daily |
| stock_pools | index PIT、stock_basic |
| sector_context | SW 分类/成员、SW 行情、stock_basic、股票 moneyflow |

sector_context 的资金流是成员股票真实聚合，不能因融资延期清空；不能因为当前实现从 factor bundle 取得 sector_data 就将融资依赖强加给行业上下文。实际接线前必须解耦这个物理依赖。

### Contracts

`aistock_monthly_component_preparation_plan_v1`：operation/cutoff、依赖合同 digest、延期来源、全部八组件的 eligible/deferred 与明确原因、publication_allowed=false、SOURCE complete=false。

`aistock_monthly_prepared_component_v1`：operation、component、cutoff、前驱 profile 身份、输入依赖身份、producer 身份、准确输出文件列表、准备快照身份、验证回执引用、canonical digest。文件 ref 记录相对路径、size、字节 SHA。无输出或无域验证不得创建。

输入依赖身份不得包含无关全局 source root，而必须覆盖：有序实际有效源分区、PIT、QFQ（不适用时显式 null）、schema、字段规则、构建参数、producer。先验证完整 SOURCE，再进行采用比较；比较本身不是 SOURCE gate。

读取拒绝额外字段、digest 漂移、跨 operation/cutoff/前驱、空验证证据、symlink/junction、逃逸、重复路径、hash/size 漂移和部分文件集合。收据 create-exclusive；恢复不得覆盖旧完成收据或改变其 capture 时间。private staging 不是历史证券 authority。

## Failure and Recovery

- margin 未齐：factor DEFERRED，其他组件完成自己的数据检查后可准备；整次发布仍 SOURCE_BLOCKED。
- adj 未齐：day/minute/factor/benchmark DEFERRED；其他独立依赖按图推进。
- 日历/PIT/writer ledger/snapshot 不成立：全部 DEFERRED，不进行 payload 准备。
- 中途退出：无完整收据的输出不可采用；新 attempt 保留原因，以 create-exclusive 目录重做该单元。
- 准备完成后源修订：重新计算最终依赖身份，只失效相关组件；依赖等价证据不得用总行数/max_date/mtime 代替。
- 最终 SOURCE 或任一 consumer 失败：不可 READY、不可激活；完整 fail-closed 原样保持。

## Implementation Plan

I1：代码持有依赖合同、诚实准备状态、内容身份与安全收据合同；RED→GREEN。

I2：正式 worker 内的独立源/物化准备，组件域检查、私有输出和断点；禁止仅注入 fixture executor 后宣称已集成。

当前 I2 开发接线：`monthly_preparation_source.py` 复用正式 SQL/有界分区 sealer、writer ledger 和 control bracket，生成独立私有源 schema；`monthly_source_producer.py` 在同一 coordinator 的 repair-overlap seal 成功后才调用私有准备 executor，始终抛出 SOURCE_BLOCKED，不登记完整 SOURCE catalog。`monthly_preparation_artifacts.py` 用正式 provider/单位/overlay 规则生成 eligible day/minute/index 的私有输入图，完整历史域检查不通过时拒绝。`ArtifactReadyPreparationBuildSource` 使用独立 schema 和正式有效行变换，普通读取端仍拒绝部分图。`monthly_preparation_composition.py` 已加入默认 production registry 的源码接线，与完整 BUILD 共用正式 WSL 执行 scope；组合测试证明无额外 provider/数据库入口。尚未合入、用户重启或真实全市场运行，不能宣称运行态已修复。

续开发：`run_preparation_build_stage` 已复用正式 index/Qlib prepare/finalize 物化路径，私有结果独立 schema，不生成 BUILD PASS。`MonthlyPrivatePhysicalPreparationExecutor` 记录实际输出字节与域证据，按有效输入/PIT/QFQ/producer 身份恢复。正式指数 H5/Parquet/CSV 与四个逻辑组件已有真实文件级定向验证；既有 WSL Qlib 工具链的日线/240 根分钟线微型 dump→finalize→下一 attempt 恢复测试已 PASS，无假 bin writer，但源行是显式测试边界。真实全市场内容审计与运行态性能仍属 I4，不能由微型样本推定完成。

后续审核补充：完成记录由正式物化器在每个组件自己的域验证成功后立即触发，而非等待整批 finalize。指数已完成而后续 stage 失败的测试证明该指数可跨 attempt 恢复且不重算；日线私有准备可复用已钉住的同源指数 CSV。已拒绝 hardlink 输出，最终采用创建独立 inode。真实微型 WSL 用例还修复了 ResourceGate 未接线、独立 runner 误导入 DB 依赖、日线 CSV/收据日期格式不同、typed tuple 日志收据被拒绝四处生成/执行合同问题；不安装依赖、不修改读取端校验、不控制业务服务。

I3：完整 SOURCE 后的安全采用、最终构建接线、余下组件正常构建及完整终验。

I3 当前接线：正式 `_prepare` 严格加载完整 SOURCE 后，才允许采用同 operation/前驱、相同有效输入身份的 private 物理输出；逐个 create-exclusive 拷贝 pinned 文件并在拷贝流内核验 SHA/size，不使用 hardlink、不拷贝未钉住的文件或私有完成收据。REUSE/INCREMENTAL/SELECTIVE_REBUILD 保留其原有 lineage 校验，不由准备输出替换。四逻辑域已接入完整 SOURCE reader 后的恢复与采用；六池/benchmark 内容按完整源重核，sector membership/market 由正式完整 sector H5 分块重新验证，重新绑定完整源 mapping authority/manifest/receipt，不复制 private release metadata。集成测试真实复制 pinned bytes 并证明独立 inode，lookup 使用明确单元边界；严格 typed full-SOURCE/漂移/篡改恢复另有真实收据测试。正式 finalize、consumer smoke、CandidateValidator 与统一发布流程保持执行；真实整次候选终验尚未执行。

I4：真实同 operation 只读源准备 readback、性能与失败恢复；源码 CI/合入后由用户重启，验证实际运行身份。

阶段允许保留开发进度；I2/I3 未完成时不得将 BUG 标 fixed 或将本能力合入为完整交付。

## Verification Plan

直接测试覆盖融资单源延期、共享依赖传播、未知错误全阻断、实际依赖边一致、跨 attempt 身份等价、源/producer/PIT/QFQ漂移失效、跨 operation 拒绝、收据/输出篡改、链接/逃逸、create-exclusive及终验无法被准备记录替代。

使用正式有界 producer 的文件对照验证；仅合成协议测试不是实际月更成功。最终只跑一次相关小矩阵，其余 CI。真实月更分别报告 prepared/deferred/invalidated/adopted、源行/计算行/读写字节/耗时，以及六阶段终态。

## Rollout / Rollback

已有 SOURCE/BUILD 全量接口与消费者 schema 不变；准备 schema 不被其接受。默认 backend worker 正式组合完成 I2/I3 后启用，API 操作习惯不变。不新增允许绕过验证的用户参数。部署目标根据 actual changed files 推断，若有多目标则拆独立 BUG 与运行态回执，不用 metadata 降级。

未完成准备可留在私有 staging；不删除 active/旧候选。回滚源码不修改 active profile。准备目录清理需单独验证 ownership、引用、进程和明确授权。

## Risks

旧单体 SOURCE/BUILD producer 对整批封存有隐含依赖，必须用独立准备合同与正式域检查解耦，不能简单去掉融资查询。首次 direct-v2 迁移仍可能需要组件重建；源快照变化可能导致准备结果作废。仅生成准备计划而未真正导出不能视为问题修复。

## Production Gates

源码需要定向测试、lint/compile、多轮审核、完整设计矩阵及 CI 后才能合入。用户执行后端重启后独立验证运行身份与真实月更业务；单元测试不代替运行态。数据库 DDL/DML、active 切换、进程控制、依赖安装、训练及实验本轮均不执行。月更最终 READY 仍按原全套 gate。

## Design Acceptance Index

| ID | 要求 |
|---|---|
| F-101 | 完整、代码持有的事实/控制依赖；状态不假报完整 |
| F-102 | 同 operation 的正式 worker 私有准备，不创建可消费分支 |
| F-103 | 真正独立物化 day/minute/index/suspend/PIT/sector/benchmark |
| F-104 | 内容/producer/PIT/QFQ 身份及 create-exclusive 安全收据 |
| F-105 | 完整 SOURCE 后相等采用、漂移按依赖失效 |
| F-106 | 最终九 gate/六 stage/三端/消费者验证不可绕过 |
| F-107 | 恢复、取消、资源遥测与零重复计算实际验证 |
| F-108 | 源码/重启/数据准备/发布分态，受保护资产不写入 |

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | backend/services/dataset_release/monthly_component_preparation.py；monthly_postgres_source.py | backend/tests/dataset_release/test_monthly_component_preparation.py | in_progress | 依赖合同、完整 typed audit 校验及默认独立准备接线已开发；真实全市场事实分母及域计数待合入后验收 |
| F-102 | monthly_postgres_source.py / monthly_source_producer.py / monthly_preparation_source.py / monthly_preparation_composition.py / monthly_production.py | test_monthly_source_producer.py；test_monthly_preparation_source.py；test_default_registry_installs_private_executor_and_shares_formal_wsl_scope；X:/AIstock_temp/BUG-1696/check_source6_production_wiring.py | in_progress | 实际 fresh-process 正式构造器 SOURCE6 接线 PASS，无 operation/DB；两项已登记未合入 source 在隔离验证进程中组合，不是生产运行态证明 |
| F-103 | build_stage.py / monthly_preparation_executor.py / monthly_preparation_sector.py / monthly_preparation_shared.py | test_real_wsl_private_daily_minute_dump_finalize_and_recovery；test_real_private_shared_files_without_full_factor_or_release | in_progress | 七组件文件级链路已验证，真实全市场 SOURCE 和 consumer 终验未执行 |
| F-104 | monthly_component_preparation.py / monthly_preparation_artifacts.py / monthly_preparation_shared.py / artifact_ready_build_source.py | test_monthly_component_preparation.py；test_shared_recovery_uses_real_pins_and_does_not_recompute；test_shared_output_tamper_is_not_hidden_by_retry | in_progress | 私有物理/逻辑完成收据和哈希恢复已开发并定向验证；不能替代默认执行及真实全市场证据 |
| F-105 | build_stage.py / monthly_preparation_executor.py / monthly_mature_build_runner.py / monthly_shared_components.py | test_full_input_adopts_only_pinned_files_as_private_copies；test_changed_final_effective_input_uses_normal_build_not_old_preparation；test_shared_builder_seals_all_sidecars_from_one_frozen_source | in_progress | 物理/逻辑采用、重新绑定及独立复制已测试；正式月更实际终验待执行 |
| F-106 | 原正式 SOURCE/BUILD 接口不变 | test_preparation_schema_is_rejected_by_formal_artifact_ready_loader；test_private_index_reader_reuses_formal_cas_rows_and_rejects_full_ready | in_progress | 全量 reader 拒绝实际私有图，重算 digest 也不能改 publication flag；正式采用接线与文件级集成已测试，完整真实月更终验尚未执行 |
| F-107 | monthly_preparation_executor.py / monthly_preparation_shared.py / build_stage.py / monthly_supervised_build.py | test_completed_index_survives_later_stage_failure_and_is_not_recomputed；test_shared_recovery_uses_real_pins_and_does_not_recompute；test_real_wsl_private_daily_minute_dump_finalize_and_recovery | in_progress | 真实 WSL 微型 dump/恢复 PASS；最终 sector 验证已改有界扫描。全市场耗时、取消与正式重试仍待运行态验证 |
| F-108 | BUG/task card/runtime contract；BUG-1696 managed worker preflight | test_monthly_worker_runtime.py；fresh-process + production readback | in_progress | SOURCE5 真实预检及 SOURCE6 真实构造器组合预检 PASS，fail-closed 单测通过；目录已选择 managed 月更探针。未合入、未重启、未执行生产内容审计 |

## DESIGN-COMPLIANCE-001

无简化完成宣称；明确部分实现状态。无静默 fallback；未知依赖阻断。业务合同与股票池不变。无新增逐组件人工审批；沿用已授权准备与独立生产授权边界。
