# 当前版 L1/L2 D3～D6 formal executor

版本：v0.1 implementation-in-progress。Feature tier：F2。

## Background

2026-09-30 用户批准新建当前版 L1/L2 正式 executor，使用 active v15、full-v3-security-identity PIT 和既有 C-010/A5。
授权两个 family、seeds 42..49、两次 fresh process，共 5184 fits；使用现存 Conda base 固定环境。
这不是恢复 BUG-1549 的 B3/P6/D1 退役链，也不改写已交付 Rotation L2 产品链或历史实验终态。

## Scope

新增当前数值模块、输入桥接、离线 CLI 和 `backend/tests/hmm_risk` 定向测试。
训练与验收只采用既有详细设计中的 D3-03-A、D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B、D6-01-B + D6-NA-A。
对应权威：[Phase 2 详细设计](hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md)。

## Non-goals

不恢复旧模式、旧 CLI、兼容代理或历史 candidate；不安装依赖，不改 Conda AIstock。
不修改共享数据集、active profile、其他模块、数据库或服务进程。
不搜索参数、不扩 seed、不改训练/验证窗口、不使用 validation 重选 seed。
数值/语义模型验收不等于样本外轮动预测有效性，不自动生成或发布产品 READY。
源码 PR 合入需要用户另行确认。

## Architecture

共享 v15 文件与 full-v3 PIT → 完整 C-010/A5 构造 → 冻结全 7D/20D 输入及完整 D6 calendar → 两 child 独立执行各 2592 fits →
parent 验证重复结果 canonical bytes 和 entry semantic readback → 每 family/level 单一 train-only D5 selection → selected-only D6。
有 typed failure 则保留 blocked/failed，不以 fit completed 冒充 acceptance，不回到 D5 换 seed。

当前不允许用产品九维 input bundle 代替完整二十维 C-010 输入，也不允许用简化 contributor 统计代替 A5 full receipt。

## Contracts

- v15 generation：`20260928-v15-unified-moneyflow1`。
- dataset manifest：`2225e1ea28f099f4972b6a465e4aa093d3767592e2651484b79700586bf358fc`。
- PIT bundle：`051e2af357703734080ff3ea5b4311926905aa7cbd1f31d926ef5b8575261313`。
- train：2022-01-01..2024-06-30，601 日；validation：2024-07-01..2025-03-31，182 日；utility watermark：2025-04-30。
- family：legacy_covfix 7D / autocycle_all_core 20D；L1=31 / L2=131。不得按已观察到的行业数缩小目录分母。
- fit budget：每进程 `2×8×(31+131)=2592`；两进程共 5184。失败 entry 保留，不 early stop、不中途补 seed。
- D3 初始化采用 sector-local population variance 和 ν=1 covariance prior；不 clip、不 post-fit projection。
- EM 使用 pinned hmmlearn E/M step；仅 MAP numerical envelope 与 D4-02-A 同时通过才停止，最多 300 E-step。
- D4-03 保留 common gate，recurrent/persistent 按 run share 0.8 互斥；软 posterior 不补 hard evidence。
- D5 采用固定 seed schedule 的 min/median/mean tolerance-aware pool filtering，raw likelihood score；禁止 validation/future utility 输入。
- D6 使用完整 calendar transition-only 与 E=O∩U，hard tie 只在 E 验收；未来 utility 保持 daily excess 的 5/10/20 日求和及 .35/.35/.30 权重。
- 固定环境：CPython 3.13.5 / NumPy 2.3.3 / SciPy 1.16.3 / sklearn 1.8.0 / hmmlearn 0.3.3 / threadpoolctl 3.6.0，单线程。

## Design Acceptance Index

- F-001：v15/full-v3 文件身份与完整 C-010/A5 输入闭合。
- F-002：D3/D4 原数值、MAP、covariance 和结构合同。
- F-003：两次 fresh-process 全 grid、零-refit readback 与 D5 train-only selection。
- F-004：D6-NA 完整 carrier/source/masks/ledger 与 selected-only semantic acceptance。
- F-005：正式 5184 fits 和真实终态，禁止伪 READY。

## Implementation Plan

一个实现任务包完成 F-001～F-004、聚焦测试和多轮修复；不是为每个小功能另建阶段。
输入正式 preflight 和源码审核通过后执行已获批准的 5184 fits，形成 F-005 的真实结果。
只创建一个源码 PR；本文件不把未实现项标为完成，也不授权自行合入。

## Verification Plan

基于 Conda base 运行定向 pytest、Ruff、py_compile、diff/ownership 检查；广泛模块覆盖交 CI。
真实源验证必须走正式文件 reader，拒绝训练时数据库 fallback、名称猜测、密集整数下标和旧 release fallback。
2026-09-30 用户另行授权基于数据库完成准备：仅在只读 repeatable-read 事务内提取官方行业目录及成员目录，
与 full-v3 taxonomy 精确连接并一次冻结现有 L1/L2 projection authority；不写数据库、共享数据集或 active profile。
数据库目录准备不是把数据库市场行情混入 v15，也不改变原 C-013 classification interval 的权威身份。
每轮审核明确记录发现与修复。任何输入缺口不得以默认值、占位回执或新阈值替代。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | formal_state_authority.py; formal_state_domains.py; formal_state_input.py; formal_state_executor.py | test_formal_state_authority.py：9 passed；官方 DB 只读目录 freeze + adapter readback 成功；历史 stable-taxonomy-backcast 训练视图覆盖 131/131；A5 full-key partition/opportunity/eligibility 定向测试通过 | PARTIAL | 31/131 文本投影、A5 逐键构造及严格 v3 mapping manifest 读取已补齐；完整文件 source constructor 尚未接通。真实 source preflight 仍须使用已审核的 BUG-1641 reader |
| F-002 | formal_state_model.py | test_formal_state_executor.py；原数值/结构矩阵及固定 D1 projection 定向测试 | PARTIAL | 已接入批准的 801207.SI raw exact-zero、full-20 preprocess 后固定 19D likelihood；其他行业不自动降维。定向测试不代表全部合同审核完成 |
| F-003 | formal_state_executor.py; run_formal_state_model_set.py | test_formal_state_executor.py；signed-zero mismatch、CLI durable failure、child projection 重新哈希后拒绝、完整 mixed-shape serialization 与零-refit selected readback | PARTIAL | 未运行正式 fresh-process grid；真实 source-to-selected artifact 全链验证仍未完成，合成序列化测试不证明模型通过验收 |
| F-004 | formal_state_calendar.py; formal_state_input.py; formal_state_model.py; formal_state_executor.py | test_formal_state_calendar.py；完整 182 日、compact finite payload、mask/source/hash 漂移、空 sentinel、diagnostic tie、失败 ledger 和零-refit 回读 | PARTIAL | carrier/manifest、逐日 ledger、T/O/U/E 和共享语义回读已实现；须在完整文件构造接通后完成真实 source 与 selected artifact 全链验证，不以单元测试推导正式验收 |
| F-005 | run_formal_state_model_set.py | backend/tests/hmm_risk/test_formal_state_executor.py；仅合成测试，正式 fits=0/5184 | BLOCKED | 仅在 F-001～F-004 真正完成后执行，不把合成单元测试写为正式训练 |

2026-09-30 当前准备回执：官方数据库目录在只读 repeatable-read 事务中提取并冻结到
`F:/Dev/AIstock_runtime/hmm_formal_state/20260930-db-preparation/industry_authority.json`，
文件 SHA256=`1a0e5f492e8d87c7732c36303495ca7e06efb5a052651f343ff72004dc7c569f`。
目录投影与 executor 定向矩阵在批准的单线程 Conda base 中合计 34 passed；其中目录测试 9 passed。
H5 reader 修复为 [BUG-1641 PR #5153](https://github.com/licong01-cloud/AIstock/pull/5153)，
源码 merge=`03bf3f571ef288a429c026e05fe4c46a0c22772c`；close-sync PR #5163 merge=
`878f5291fa778b2f647bd7747b6920efb4574550`，Issue #5147 已关闭，BUG status=fixed。
本 feature 已安全合并上述 main，包含 reader 修复；先前独立 worktree 的真实 v15 source preflight
不冒充本 feature 的最终 source-to-request 验收。接下来完成完整 C-010/A5 文件构造与 D6 carrier/readback；
不需要数据窗口创建私有行业编号或改写 v15。

2026-10-01 增量实现仍属于上述同一个源码任务包，没有新建研究阶段或模型合同：

- D6 正式有限 compact carrier、manifest v2、逐日 source/missing ledger、T/O/U/E 与 durable semantic readback 已接入当前 executor。
- A5 builder 保存完整 P_all/P_in/P_out、O_sector 和逐股票 eligibility；复用已有严格校验器，不用 count-only 回执替代逐键依据。
- C-010 消费端显式接受并检查当前 v3 mapping manifest；未知 schema、缺失 projection/hash、错误 research basis、歧义 predicate 仍拒绝。
- 第一轮复审修复 source receipt 核对、真实文件日历逐日闭合、N_evidence<30 先行失败；第二轮复审补齐失败时已知 posterior/ledger 保留、v3 basis/backcast 身份一致性、正式 D6 状态/reason 优先级及 selected identity/model hash。
- 聚焦回归 58 passed；后续相关 fix-point 分别 13、35、11 passed（最后一组含 task-code fresh-process import/DB poison）。Ruff、py_compile、diff、14/14 ownership、module registry 8 passed 和 L0 blocking=0。上述为工作树实现验证，未伪写成正式训练或 PR 最终 HEAD 全量验收。
- 尚未完成完整 file-only source constructor、实际 source-to-request 和 selected model-set 全链审核，因此 F2 不报告 PASS、不创建实现 PR。
- 正式训练仍为 0/5184；没有 selection、正式 D6、model/READY、数据集/数据库写入或进程操作。单元测试只使用合成数据。

本轮继续同一个实现任务包的复审修复：固定 D1 v2 projection 保存 full feature/preprocess/source identity、
raw 与 processed IEEE-754 payload hash、固定 mask、effective dimension；parent 在 D5 前从 request 重算投影，
D6 先验证完整 20D O payload，再使用同一固定 19D mask，selected artifact 保留逐 entry projection 与维数直方图。
非 allowlist 常量维不自动删除，仍由各 restart 的初始化明确失败；不因投影新增检查跳过正常 grid 迭代。
合成序列化矩阵覆盖四个 family-level 和 31/131 分母，但不作为正式 D3～D6 acceptance 或模型生成证据。
完整文件 constructor 与真实 source/request/selected 链未闭合，F-001～F-004 继续保持 PARTIAL。
最终聚焦矩阵实际运行 75 passed（executor/calendar/domains/authority/stock_fact_observation）；
固定投影及 selected artifact fix-point 16 passed。Ruff check/format、py_compile、git diff --check、
module registry 8 passed 与 L0 blocking=0。两轮复审修复投影遗漏、D5 前投影 identity 闭合和
非 allowlist 常量维不得中断正常 restart schedule；不把这些代码测试写作正式 fit/D5/D6 的结果。

## Rollout / Rollback

所有变更保留在独立 task worktree。未通过 F2/源码审核前不报 ready-for-PR。
没有部署、runtime activation 或数据库迁移，因此当前无需运行态 rollback。
实验输出必须使用新路径且禁止覆盖；失败输出不得覆盖已有 accepted artifact。

## Risks

输入正式目录投影已由数据库只读冻结解决；A5 完整文件源构造及 D6 真实 source/selected artifact 全链验证仍未完成。
真实 v15 moneyflow H5 物理行乱序被旧 HMM reader 误判为无效，已登记独立 BUG-1641 / Issue #5147；
仅对 table H5 在内存中排序，保留重复键、日期、schema 的 fail-closed，不重写数据文件。
原始 full-v3 as-published classification 的 resolved identity 只观察到 126 个 L2，
其中未直接确认的 5 个为 `801011.SI`、`801015.SI`、`801018.SI`、`801194.SI`、`801731.SI`。
但当前批准的历史模式并非直接使用这一原始 resolved 子集：现有 adapter 的 stable-taxonomy-backcast
从冻结、单一且可校验的候选来源派生身份，实际 active classification 视图覆盖全部 131 个目录 L2。
因此不能将原始 identity=None 误判为历史训练缺行业，也不需要补写 full-v3 或拼接数据库成员分类。
此结论只表示行业身份覆盖闭合，不证明每个交易日的全部价格、资金流、特征或 A5 coverage 已通过。
数据库目录对应这 5 个行业的股票数为 4/9/13/7/28，仅用于目录诊断，不作为历史分类 authority。
forward 仍遵循 as-published PIT；历史 non-as-known-taxonomy 身份不能冒充 forward 确认。
正式全 grid 可能没有完整 D5 candidate，或选中后 D6 失败；均是允许的真实结果，不得改阈值、换 seed 或发布预测能力。

## Production Gates

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop。
Conda AIstock mutation=false；dataset/profile mutation=false；runtime/process control=false。
正式训练及后续提交/合入已获用户授权；不因此跳过完整实现、正式审核、F2 和 CI。
当前未完成的 feature 不创建或合入部分交付 PR。模块生产影响须以最终 changed files 的 workflow/runtime contract 为准。

## DESIGN-COMPLIANCE-001 当前复审

1. 无简化交付：当前整体明确未完成，完整 C-010/A5 和 D6 carrier 不用简化证据替代；未请求合入。
2. 无静默错误：source/seed/hash/tie/非有限值与 durable failure 采用显式拒绝，缺口继续报告。
3. 无业务漂移：模型合同与 full denominator 不变；未来 utility 纠回复用 daily-excess sum；D6 不新增 O-only tie 门禁。
4. 无未经批准门禁/审批：不增加资源、availability ratio、统计 significance 或人工 sector 特批；本次合入授权不扩展为服务控制、数据写入或删除授权。

本记录不是正式验收通过声明；F2 当前不得 PASS。
