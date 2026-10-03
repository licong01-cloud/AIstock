# 当前版 L1/L2 D3～D6 formal executor

版本：v0.4 input-rebind（冻结输入最终 v17）。Feature tier：F2。

## Background

2026-09-30 用户批准新建当前版 L1/L2 正式 executor，使用 active v15、full-v3-security-identity PIT 和既有 C-010/A5。
授权两个 family、seeds 42..49、两次 fresh process，共 5184 fits；使用现存 Conda base 固定环境。
这不是恢复 BUG-1549 的 B3/P6/D1 退役链，也不改写已交付 Rotation L2 产品链或历史实验终态。

## Scope

本任务还允许对 `backend/services/hmm_risk/rotation_l1_input_bundle.py` 的现有文件 stock-row 构造增加可选只读 callback；默认调用行为不变。用于复用已审查的 source/alias/PIT/circ_mv 构造，不复制实现，也不改产品公式、数据或其他模块。

2026-10-01 用户另行批准将本 executor 冻结输入更新为 v16 successor。允许在上述共享 HMM reader 中增加显式 frozen-release binding 参数，并复用相同 manifest/component 校验；普通产品入口仍强制 active profile，不改变其默认行为。对应身份测试纳入 `backend/tests/hmm_risk/test_rotation_l1_input_bundle.py`，不修改数据生产代码或流程文件。

2026-10-03 用户批准按复查建议执行：将冻结输入更新为最终 v17，重新构造完整 7D/20D C-010/A5 request，并执行零-fit file-only preflight 与 D6 数据可执行性检查。本轮只修改 executor/input 的冻结绑定及对应测试、本设计状态；不启动正式训练，不切换 active profile，不修改数据集。数据窗口只负责共享基础数据，特征构造、D6 carrier/readback 与可用日期复核由 HMM owner 直接执行。

新增当前数值模块、输入桥接、离线 CLI 和 `backend/tests/hmm_risk` 定向测试。
训练与验收只采用既有详细设计中的 D3-03-A、D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B、D6-01-B + D6-NA-A。
对应权威：[Phase 2 详细设计](hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md)。

2026-10-01 源码提交审核允许在同一 HMM 测试包按当前源文件职责整理已有测试：`backend/tests/hmm_risk/test_formal_state_model.py`、`test_formal_state_model_set.py`、`test_freeze_formal_industry_authority.py`，并补齐 `test_formal_state_input.py` 的直接输入测试。只移动既有测试或增加实际缺失的合同测试，不复制测试、不建立兼容代理，不修改全局 CI/nox/test plan。源码修复仍限于既有 executor 和目录冻结 CLI 的合同回读、写入安全边界。

## Non-goals

不恢复旧模式、旧 CLI、兼容代理或历史 candidate；不安装依赖，不改 Conda AIstock。
不修改共享数据集、active profile、其他模块、数据库或服务进程。
不搜索参数、不扩 seed、不改训练/验证窗口、不使用 validation 重选 seed。
数值/语义模型验收不等于样本外轮动预测有效性，不自动生成或发布产品 READY。
本轮继续源码审核、修复和提交门禁；完整源码通过后可提交并创建 PR，PR 合入仍等待用户另行确认。该授权不包含服务控制、数据写入或删除。

## Architecture

显式批准的共享最终 v17 文件与 full-v3 PIT → 完整 C-010/A5 构造 → 冻结全 7D/20D 输入及完整 D6 calendar → 两 child 独立执行各 2592 fits →
parent 验证重复结果 canonical bytes 和 entry semantic readback → 每 family/level 单一 train-only D5 selection → selected-only D6。
有 typed failure 则保留 blocked/failed，不以 fit completed 冒充 acceptance，不回到 D5 换 seed。

当前不允许用产品九维 input bundle 代替完整二十维 C-010 输入，也不允许用简化 contributor 统计代替 A5 full receipt。

## Contracts

- 当前冻结 generation：`20261002-v17-unified-basic-history1`；revision：`20261002-r8-unified-basic-history1`；release：`qe_hmm_full_v2_20260831`；cutoff：`2026-08-31`。
- dataset manifest：`97df6acdbe43dc20f577e85f8cceb2814d73fca13b0f12cda7aefd90cfb62f2c`。
- manifest 文件 SHA256：`eb193a03fedeb0aa4c825daed492f50331d4f1581b59b750203501b95694ad76`。旧 v16 与 v17 未最终闭合身份 `eef96dd6…` 均不得作为本 executor 输入。
- candidate root 必须显式传入；本 executor 不读取 active profile 来替代冻结输入，不执行 profile 切换，不自动发现 latest 或回退 v15。
- 重新通过正式 constructor 生成完整 source/request/policy/calendar 身份，不仅替换旧 request 或 manifest 哈希。共享模型、特征、窗口、seed、阈值和 PIT bundle 均不变。
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

- F-001：当前批准的最终 v17/full-v3 文件身份与完整 C-010/A5 输入闭合；v15/v16 结果仅作演进历史，不能冒充当前输入通过。
- F-002：D3/D4 原数值、MAP、covariance 和结构合同的完整源码实现与直接测试。
- F-003：两次 fresh-process 全 grid、零-refit readback 与 D5 train-only selection 的完整执行控制流和故障测试。
- F-004：D6-NA 完整 carrier/source/masks/ledger 与 selected-only semantic acceptance 的源码及共享 readback 验证。
- F-005：正式 5184 fits 和真实终态，禁止伪 READY。

## Implementation Plan

一个实现任务包完成 F-001～F-004、聚焦测试和多轮修复；不是为每个小功能另建阶段。
历史长期计划是在输入正式 preflight 和源码审核通过后执行 5184 fits，形成 F-005 的真实结果；当前 v17 输入更新授权明确不启动正式训练，本轮只到完整 file-only preflight 与 D6 数据可执行性复核，后续以允许训练动作的授权为准。
只创建一个完整源码 PR；本文件不把未实现项标为完成。源码合入与后续正式模型验收是不同结果，不以源码通过推导模型成功。

## Verification Plan

基于 Conda base 运行定向 pytest、Ruff、py_compile、diff/ownership 检查；广泛模块覆盖交 CI。
真实源验证必须走正式文件 reader，拒绝训练时数据库 fallback、名称猜测、密集整数下标和旧 release fallback。
2026-09-30 用户另行授权基于数据库完成准备：仅在只读 repeatable-read 事务内提取官方行业目录及成员目录，
与 full-v3 taxonomy 精确连接并一次冻结现有 L1/L2 projection authority；不写数据库、共享数据集或 active profile。
数据库目录准备不是把数据库市场行情混入 v15，也不改变原 C-013 classification interval 的权威身份。
每轮审核明确记录发现与修复。任何输入缺口不得以默认值、占位回执或新阈值替代。

源码提交门禁覆盖 F-001～F-004 的完整实现及实际直接验证；F-005 是源码合入之后的真实实验，不属于“代码已经可执行”的证明。用户最新明确禁止本轮正式训练，因此 F-005 以明确批准的未执行项保留。源码 F2 通过不等于 5184 fits 已执行、模型合格或产品 READY。不能为了提交门禁消耗正式训练授权，也不能删除 F-005 或用 mock 冒充其完成。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/hmm_risk/formal_state_authority.py; backend/services/hmm_risk/formal_state_domains.py; backend/services/hmm_risk/formal_state_input.py; backend/services/hmm_risk/formal_state_executor.py | backend/tests/hmm_risk/test_formal_state_input.py；最终 v17 完整 prepare + 独立 fresh-process preflight PASS；request SHA=4e2cc82614c20f6fc31a224da0df0de29621abc9e7e762b88004ca1500eaa0ec，receipt SHA=94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7 | PASS | 无 |
| F-002 | backend/services/hmm_risk/formal_state_model.py | backend/tests/hmm_risk/test_formal_state_model.py；原数值/结构矩阵、MAP 联合停止、重哈希 drift、固定 19D projection 和零-refit 数值回读 | PASS | 无 |
| F-003 | backend/services/hmm_risk/formal_state_executor.py; scripts/hmm_risk/run_formal_state_model_set.py | backend/tests/hmm_risk/test_formal_state_executor.py; backend/tests/hmm_risk/test_formal_state_model_set.py；完整 mocked grid、signed-zero mismatch、parent 环境/flags 回读、child/finalization durable failure、CLI fresh-process 拒绝和 mixed-shape serialization | PASS | 无 |
| F-004 | backend/services/hmm_risk/formal_state_calendar.py; backend/services/hmm_risk/formal_state_input.py; backend/services/hmm_risk/formal_state_model.py; backend/services/hmm_risk/formal_state_executor.py | backend/tests/hmm_risk/test_formal_state_calendar.py; backend/tests/hmm_risk/test_formal_state_executor.py::test_parent_selected_d6_sparse_evidence_real_readback_without_refit；最终 v17 全 324 组 182 日 carrier/ledger/hash 独立回读 PASS，E 最少 143 日；既有 synthetic selected D4/D5→D6→durable readback 测试保留；无正式 selection/refit | PASS | 无 |
| F-005 | scripts/hmm_risk/run_formal_state_model_set.py | backend/tests/hmm_risk/test_formal_state_executor.py；控制流测试不代表正式训练，正式 fits=0/5184 | APPROVED_BY_USER_DEFERRED_FORMAL_EXECUTION | 用户于 2026-10-03 批准最终 v17 input/preflight，本轮明确不启动正式训练；保留后续真实 5184 fits 与模型终态验收，不视为已完成，不生成 model/READY |

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
上述为 v15 阶段审核时状态，当时真实 selected 链尚未闭合、F-002～F-004 为 PARTIAL。当前 v16 source/request 终态和完整源码测试结论见最新验收矩阵及下文，不将历史状态视为当前阻断。
最终聚焦矩阵实际运行 75 passed（executor/calendar/domains/authority/stock_fact_observation）；
固定投影及 selected artifact fix-point 16 passed。Ruff check/format、py_compile、git diff --check、
module registry 8 passed 与 L0 blocking=0。两轮复审修复投影遗漏、D5 前投影 identity 闭合和
非 allowlist 常量维不得中断正常 restart schedule；不把这些代码测试写作正式 fit/D5/D6 的结果。

## Rollout / Rollback

### 当前执行授权：最终 v17 冻结输入更新与预检

最终 v17 state=`CANDIDATE_READY`，manifest 与文件 SHA 以 Contracts 为准。当前 main reader 独立 fresh process 已通过基础 source preflight，L1=31/L2=131，source inventory SHA=`c59f482664e147534e4fd893d17758129d3b832fece7051d4a14bb72c63373c9`；五个消费窗口基础覆盖 unresolved=0，7 项合法预热 NA 保留。三端交付回执的 66 个 component pins 一致，但不把数据 owner 的基础检查视为 HMM 完整 C-010/A5 或 D6 验收。

HMM owner 重新生成新 request，不覆盖 v16 资产，不修改旧 manifest 哈希，不读取源窗口之前的市值、不使用当日市值填补预热 NA。完整构造、独立 request readback 及全部 324 组 T/O/U/E 汇总的结果完成后更新 F-001。D6 carrier 合法与 |E| 足够均不代表 hard semantic/model 验收通过；本轮 fits=0、D5 selection=false、D6 model acceptance=false、model/READY=false。

### 当前终态：最终 v17 完整输入与 D6 数据可执行性 PASS

HMM owner 使用源码提交 `ea77ec5d360f19a9006c956505be3148bfaa9afe` 执行正式 `prepare`，重新读取 v17 文件并完成全部 C-010/A5 构造；随后独立 fresh process 执行正式 `preflight`，完整核对 source/request/policy/calendar/carrier，均退出 0。两个进程均禁止 network/database、active resolver 与 HMM fit；candidate state 前后读回不变。没有改共享资产、profile、行业编号、阈值或模型窗口。

- 新 request：`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-file-construction/request.json`，847,210,865 bytes。
- request 物理 SHA256：`4e2cc82614c20f6fc31a224da0df0de29621abc9e7e762b88004ca1500eaa0ec`；canonical receipt SHA256：`94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7`。
- 同目录 `preflight.json` 为正式回读结果；`data_readback.json` 为全部 324 组 T/O/U/E 的紧凑结果。训练 calendar=601，validation calendar=182；不删 calendar 日或目录行业，不补零、前填。

| family / level | 行业数 / 维数 | train rows min～max | O min～max | U min～max | E min～max |
|---|---|---|---|---|---|
| legacy_covfix / L1 | 31 / 7D | 589～601 | 182～182 | 182～182 | 182～182 |
| legacy_covfix / L2 | 131 / 7D | 565～601 | 170～182 | 153～182 | 150～182 |
| autocycle_all_core / L1 | 31 / 20D | 534～601 | 182～182 | 182～182 | 182～182 |
| autocycle_all_core / L2 | 131 / 20D | 520～601 | 163～182 | 153～182 | 143～182 |

旧 v16 的 8 组 E<30 静态阻断全部解除，新 request 的 E<30 列表为空。L2 仍有显式 feature/utility NA，由完整 carrier 记录，不能把模型输入 NA 一概归因于基础数据漏采。上述仅证明当前批准输入满足进入正式模型制备的静态条件；D3/D4、train-only D5、selected-only D6 hard semantic 仍须正式实验判断，不外推为模型或 L2 轮动预测有效。

两轮源码复审覆盖最终身份/旧输入拒绝、默认 active 消费行为不变及 producer/current-input/readback 闭合；修复测试包装器和临时目录问题后有效 RED=2 failed/1 passed，GREEN input/calendar=20 passed。classifier 绑定的 HMM slice=68 passed，registry=8 passed，L0=0 blocking，4/4 ownership、Ruff/format/diff 和 fresh-process router/health/formal import 通过。提交前再次按最终 HEAD 补齐最小门禁，不把测试中的 synthetic fit 写成正式训练。F-005 仍未执行。

### 历史执行授权：v16 冻结输入更新与预检

用户已批准显式绑定 `20261001-v16-unified-moneyflow2`，并执行完整 file-only preflight。数据窗口已交付不可变 successor，历史 v15 的输入失败事实保留在下面的时间记录；当前结论以本节实际 v16 终态为准。
当前授权不启动正式训练、服务、数据集/profile 写入；不合入数据窗口 PR #5177，也不把 CANDIDATE_READY 当作 HMM 预检通过。
完整 constructor 和独立 request readback 均实际通过后，F-001 更新为 PASS；不以局部 alias 修复推导正式模型验收。

身份更新源码提交 `9a087c299064211cdff4096fb8f55f8437588e5c`：显式 frozen binding 复用共享 manifest 校验，输入和 request 回读均拒绝旧 v15 或重哈希漂移；普通产品入口仍要求 active profile。两轮复审覆盖状态/schema/structural boolean、间接祖先路径及默认产品行为。固定 Conda base 直接矩阵 160 passed；Ruff、py_compile、16/16 ownership、module registry 8 passed、L0 blocking=0、fresh-process router/health/constructor import 通过。上述仅是代码验证，不是正式训练或全输入通过声明。
真实预检进程禁止 socket connection、active resolver 和正式 fit，使用新输出 `F:/Dev/AIstock_runtime/hmm_formal_state/20261001-v16-file-construction/request.json`；producer 为上述不可变源码提交，不覆盖 v15 失败产物。

### 历史终态：v16 完整 file-only preflight PASS

`prepare` 和独立 fresh-process `preflight` 均退出 0；正式 fits=0。预检回执为 `F:/Dev/AIstock_runtime/hmm_formal_state/20261001-v16-file-construction/preflight.json`，status=`preflight_passed`，database_write=false，runtime_action=false。request 为重新读取 v16 源文件、执行完整 C-010/A5 构造后的新资产，不是修改旧 manifest 哈希。

- source 范围 `2020-07-30..2025-04-30`；训练 calendar 601 日、validation calendar 182 日。
- 两 family 各完整包含 L1 31 个和 L2 131 个行业；legacy 为 7D，autocycle 为 20D，保留原批准的模型 projection 与数值合同。
- 实际 train rows：autocycle L1 194～558、L2 336～601；legacy L1 294～558、L2 420～601。
- validation carrier 保留完整 182 日和显式 O/U mask。实际 observed rows：autocycle L1 84～181、L2 125～182；legacy L1 112～181、L2 144～182。没有补零、前填或静默删除 calendar 日期。
- 实际 future-utility rows：两 family L1 2～182、L2 49～182。这是仍需正式 selected-only D6 判断的证据稀疏性，不能将 carrier 合法或输入预检通过当作每个行业 D6 通过，也不得为此更改样本、阈值或窗口。
- A5 schema=`hmm_risk_c010_feature_domain_policy_v2`；批准的 train exclusion 仍仅 `689009.SH`。原 `302132.SZ/2021-07-30` 资金流阻断已由真实文件构造解除，没有新增排除或伪造 provider absence。
- active profile 前后 SHA 均为 `56b4741044aa98750468b2d5b2b9ae888cf2abf9ba4df00e36019ebc3dd0cc6f`，仍为 v15；v16 仅作为本 executor 获准的显式冻结输入。没有激活或修改任何候选。

上述是输入预检时的停止节点；随后用户授权继续源码审核和提交门禁，当前源码验收矩阵为本文件最新状态。F-005 正式实验仍未执行。未执行真实 D5 selection、D6 semantic validation、model/READY、数据库写入、服务控制或依赖安装；正式训练须在允许执行该动作的后续授权下进行，不由本次审核自动启动。

### 2026-10-01 本轮源码复审与提交门禁

第一轮发现 parent 未独立验证两个相同 child 的 schema、环境和 typed flags；已加入与 parent 固定环境的 canonical 核对，环境漂移、unknown schema、ready=true、整数冒充 boolean 和 float fit-count 的 5 个 RED 用例修复后通过。该回读发生在 D5 前，不增加模型门禁或调整数值合同。

新增 synthetic 集成测试：只拟合一个合成数值 fixture，以 mocked grid 产生完整 schedule；parent 的 D4 数值回读、D5、selected-only D6 和 durable readback 均真实执行。2 条 utility evidence 明确失败并保留 182 日 ledger/posterior，禁止换 seed/refit/模型发布；逐层重哈希的 ledger 漂移仍拒绝。另有 4 个 parent child/finalization/readback 故障测试验证 durable typed failure。上述一律不是正式 5184 fits。

第二轮修复目录冻结 CLI 的输出路径未覆盖 canonical root/间接祖先；复用现有 `validate_output_location`，数据库连接之前拒绝，两项 RED→GREEN。测试按源文件职责原子整理，解决四个 direct-neighbor 路由缺口，未改变 CI/nox/test plan 或复制测试。

最终源码门禁绑定 `ef2b87b7d5f644da9e7e74678757027d5355dffe`（修复提交 `71c214af4`，随后安全合并 main）：classifier 指定 `hmm_risk_pr_slice` 实际 13 个测试文件、219 passed；module registry 8 passed、14/14 映射；L0 findings=0/blocking=0；19 个 Python 文件 Ruff check/format 与 py_compile 通过；20 个实际 changed files ownership 全部映射、无歧义；git diff --check 通过。此前未绑定 classifier 的 29 项默认 smoke 不作为完整提交 slice 证据。

同一源码 HEAD 的独立 fresh process 验证 router、health、executor/input/model 共 5 个模块全部从当前任务工作树加载；network/database、active resolver 和 formal fit 均设置 poison。复用已冻结 request 的完整正式 `preflight` 退出 0，request SHA 与 receipt SHA 均保持上述 F-001 值，fits=0。输出为 `F:/Dev/AIstock_runtime/hmm_formal_state/20261001-v16-source-review/ef2b87b7-preflight.json`。这不是重新构建输入、重跑训练或读取新实验结果。

后续仅记录文档和 PR 提交状态，不将文档-only HEAD 冒充上述代码测试的执行 HEAD。F2 源码验收通过；F-005 仍明确未执行。CI 状态以 PR 实时检查为准，不把本地通过写成 CI 全绿。

### 2026-10-02 PR #5194 数值测试修复与当前边界

已在干净任务工作树安全合并最新 main。CI run `36877367133` 的实际结果为 218 passed / 1 failed；唯一失败是合成 fixture 两次拟合的严格 bitwise 比较，差异为浮点末位。该 run 的 changed-test coverage 已通过，不将此失败归因于测试路由或全局流水线。

修复仅涉及两个 HMM 测试文件：共享 module-scoped fixture 先加载延迟导入的 hmmlearn/sklearn，再以 `threadpool_limits(1)` 和既有五个线程环境变量落实批准的单线程条件；退出模块后恢复原线程池与环境。新增前提测试在未修复的 AIstock-CI 环境实际 RED，修复后 GREEN；严格 `repeated == entry`、错误环境拒绝及生产 fail-closed 均保留，不使用容差、舍入、skip 或模型算法变更。

本轮代码验证 HEAD 为 `805bfb52116ea3695179828f26554fe715d6a290`：在未手工设置单线程环境的既有 AIstock-CI 解释器运行 classifier-bound `hmm_risk_pr_slice`，13 个测试文件、220 passed，changed-test coverage PASS；module registry 8 passed / 14 个映射；L0 findings=0/blocking=0；两份实际修改 Python 文件 Ruff check/format、py_compile 及 diff check 通过；20 个 PR 文件 ownership 全部映射。另以同一进程验证三个 fix-point 用例通过，测试结束后线程池恢复为宿主原有 24/24/32，环境变量恢复。两轮复审补齐延迟加载池的生命周期，未增加生产合同或全局 CI/nox/test plan 变更。

同一代码 HEAD 的 router/health/executor/input/model fresh-process import 通过，加载路径全部属于本任务工作树，network/database/fit/active-profile 访问均设置 poison。实际 changed-files 的 runtime 分类仍为 backend、target_ids=[backend-main]、catalog_error=null；源码合入与用户重启后的运行态验收保持独立。

先前 `ef2b87b7d5f644da9e7e74678757027d5355dffe` 的完整 file-only preflight 仍是该 HEAD 的实际结果；本轮只改测试，没有重复构造或重新读取大 request，更没有把旧 preflight 冒充本轮 HEAD 新执行的结果。正式 fits 仍为 0/5184，F-005 未执行，PR CI 以更新后实际结果为准。

数据侧尚有独立阻断：现有冻结 request 中 8 个 family×sector 的 D6 `|E|<30`，电子 `801080.SI` 两个 family 均为 E=0。定向检查发现 155 个因果 circ_mv 失败键位于 PIT entry，source window 内缺少严格早于当日的 daily_basic 事实；现有消费者已允许 source window 内的入池前事实，不得用当日市值、前填、缩小分母或修改阈值绕过。共享文件事实补齐和 successor 交付由数据窗口负责，本 HMM PR 不修改数据生产代码、冻结数据集或 active profile。未核对数据库源事实，不能把文件缺口描述为数据库或 provider 缺失。

### 2026-10-01 历史长任务增量（v15 阶段）

完整 file-only C-010/A5 constructor 与 `prepare` CLI 已实现，复用现有 shared reader、alias/PIT resolver、stock-fact 聚合和严格 receipt validator。
没有用九维产品 bundle、count-only contributor receipt 或模型私有行业编号替代正式输入。真实源构造仍在执行，完成前 F-001 不报告通过。

上述“仍在执行”为 v15 启动时记录；紧随其后是该阶段失败终态，当前结论以本节前面的 v16 PASS 为准。

### 历史输入终态：冻结 v15 历史资金流覆盖阻断

第二轮 file-only constructor 非零退出，失败点为 `302132.SZ/2021-07-30` 未找到资金流，且没有精确 provider-absence authority。未生成正式 request，fits 仍为 0/5184。
只读索引查询同时检查历史代码 `300114.SZ` 和规范代码 `302132.SZ`，不是通过猜测代码或数据库 fallback 补数据：

- 当前 moneyflow H5 的历史代码记录仅 120 行，日期 `2024-08-13..2025-02-14`；规范代码记录 377 行，日期 `2025-02-17..2026-08-31`。
- 正式训练窗口 `2022-01-04..2024-06-28` 有 601 个冻结交易日；该股票 591 行完整价格、10 行合法空 sentinel，与 10 个停牌日期一致。窗口内两种代码均没有资金流记录。
- 现有 `provider_absence_v1.json` 不包含该股票，不能将上述缺失自动解释为 provider absence，也不能直接排除该股票、填零、前填或改变 A5 eligibility。
- 这说明之前的 120/120 alias 修复只证明其当时目标日期已闭合，不证明本次完整训练与预热窗口覆盖。当前不能宣称 v15 已满足正式训练输入。
- 后续由数据 owner 对正式文件源范围 `2020-07-30..2025-04-30` 审核该 alias 的真实来源覆盖：可取得的数据通过通用不可变 successor 补齐；确属 provider absence 的日期须提供精确受审 authority。不得原地修改 v15；变更冻结 release identity 必须另行获得授权，不得只替换 manifest 哈希。
- HMM 保留现有完整 constructor 和 fail-closed；F-001/F-005 为 BLOCKED，F-002～F-004 不因单元测试通过升级为正式模型验收。当前不创建部分交付 PR，不启动训练或选择 seed。

本次只查询既有冻结文件；数据集、active profile、市场数据库、模型合同及服务进程均未修改。

- 第一轮真实构造在 0 fits 处发现 shared security schema 的 HMM consumer 缺陷：BUG-1644 / Issue #5170；独立源码 PR #5171 已合入，merge=`46f17a242852e2a6d7132157e7d2a7efb9513002`。共享 v15 文件 SHA 保持不变，不是数据缺失，也不需要修改数据集。
- BUG-1644 actual runtime 分类为 backend-main；fresh-process import/file smoke 已通过，运行态仍等待用户重启。close-sync PR #5172 保持 OPEN，不提前合入或关闭 Issue。
- 第二轮真实 file-only prepare 使用新输出 `F:/Dev/AIstock_runtime/hmm_formal_state/20261001-file-construction-v2/request.json`，明确 socket connection poison，未训练，未写数据库或数据集。输入构造通过与否以其实际终态为准。
- 本轮复审修复非法输出目录拒绝后仍可能写 failure receipt、price denominator 失败前未检查的股票被误记 complete、训练日期缺口未进入 occupancy receipt、selected artifact 仅回读 semantic 未再验证 D3/D4 数值/结构，以及初始化参数可重新哈希但未按批准公式回读等问题。
- 训练 run 继续按实际 observation rows 定义；完整日期/缺口 hash 显式保留，不私自把自然日或缺失交易日构造为 state transition。D6 仍使用完整 182 日 calendar 与 T/O/U/E。
- 固定单线程定向矩阵 75 passed；后续初始化参数漂移 fix-point 5 passed。全 grid 的 2592/5184 失败控制流使用 mock fit 检查，不是正式训练证据。此前一次未设单线程的测试浮点不一致已按固定环境复验通过，没有增加容差或更改合同。
- 正式 fits=0/5184；未选择真实 seed、未执行正式 D6，未生成 model/READY。F2 仍须实际 source/request 闭合和最终审核，不把此增量状态当作完成声明。

所有变更保留在独立 task worktree。未通过 F2/源码审核前不报 ready-for-PR。
没有部署、runtime activation 或数据库迁移，因此当前无需运行态 rollback。
实验输出必须使用新路径且禁止覆盖；失败输出不得覆盖已有 accepted artifact。

## Risks

最终 v17 完整 C-010/A5 构造、source/request 身份及全 324 组 182 日 D6 carrier 预检已闭合，E 最少为 143 日；历史 v16 的 E<30 输入阻断已解除。selected D4/D5/D6/readback 的源码链已通过合成数据合同测试，但真实模型验收仍未执行，可能在 covariance、train structure 或 hard semantic evidence 失败。禁止缩小目录、改阈值或换 seed 绕过，不能从基础数据或输入 PASS 推导模型与预测有效。
真实 v15 moneyflow H5 物理行乱序被旧 HMM reader 误判为无效，已登记独立 BUG-1641 / Issue #5147；
仅对 table H5 在内存中排序，保留重复键、日期、schema 的 fail-closed，不重写数据文件。
原始 full-v3 as-published classification 的 resolved identity 只观察到 126 个 L2，
其中未直接确认的 5 个为 `801011.SI`、`801015.SI`、`801018.SI`、`801194.SI`、`801731.SI`。
但当前批准的历史模式并非直接使用这一原始 resolved 子集：现有 adapter 的 stable-taxonomy-backcast
从冻结、单一且可校验的候选来源派生身份，实际 active classification 视图覆盖全部 131 个目录 L2。
因此不能将原始 identity=None 误判为历史训练缺行业，也不需要补写 full-v3 或拼接数据库成员分类。
该历史目录结论只表示行业身份覆盖闭合；当前最终 v17 正式文件构造的价格、资金流、特征与 A5 验证以本文件最新 preflight 为准，不推导模型通过。
数据库目录对应这 5 个行业的股票数为 4/9/13/7/28，仅用于目录诊断，不作为历史分类 authority。
forward 仍遵循 as-published PIT；历史 non-as-known-taxonomy 身份不能冒充 forward 确认。
正式全 grid 可能没有完整 D5 candidate，或选中后 D6 失败；均是允许的真实结果，不得改阈值、换 seed 或发布预测能力。

## Production Gates

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop。
Conda AIstock mutation=false；dataset/profile mutation=false；runtime/process control=false。
本轮授权为最终 v17 输入绑定、完整 file-only preflight、D6 数据可执行性复核和源码审核；不启动正式训练。源码 PR 不把明确未执行的 F-005 实验写为完成；PR 合入等待用户另行确认。
本轮实际 4 个 changed files 分类为 `runtime_impact=backend`、`target_ids=[backend-main]`、`catalog_error=null`；两个 formal source 文件为 runtime_files，不复用历史 receipt，也不因 CLI 为离线用途人工降级。fresh-process router/health/formal 依赖链加载通过，尚未合入或激活。后端进程控制仍归用户，不由输入更新、审核或提交授权推导。

## DESIGN-COMPLIANCE-001 当前复审

1. 无简化交付：F-001～F-004 完整源码已验证，完整 C-010/A5 和 D6 carrier 不用简化证据替代；F-005 正式模型实验未执行，源码就绪不等于模型或预测功能验收。
2. 无静默错误：source/seed/hash/tie/非有限值与 durable failure 采用显式拒绝，缺口继续报告。
3. 无业务漂移：模型合同与 full denominator 不变；未来 utility 纠回复用 daily-excess sum；D6 不新增 O-only tie 门禁。
4. 无未经批准门禁/审批：不增加资源、availability ratio、统计 significance 或人工 sector 特批；已有环境/路径合同回读不新增模型门禁，本次提交授权不扩展为合入、服务控制、数据写入或删除授权。

本记录不是正式模型验收通过声明。源码提交 F2 按 F-001～F-004 和明确批准的 F-005 未执行边界检查；正式 fits、D5/D6 结果与产品能力仍为独立状态。
