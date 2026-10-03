# 正式 executor：已完成L1/L2合同与后续L2-only修订

版本：v0.7 l2-contract-approved（冻结输入最终 v17）。Feature tier：F2。

当前状态：原批准L1/L2双fresh-process **5184/5184 fits已完成，原完整合同未接受**；ready=false。未来新增研究仅围绕L2。2026-10-03用户明确“批准合同，并开始后续的任务”，批准本包A/B精确合同与实施、测试和全131行业zero-refit研究读回；不重跑训练。

本轮增量：C-008-L2-D6-PERSISTENT-RC-A及C-008-L2-INDEPENDENT-A均为APPROVED_BY_USER。原模型、窗口、seed和acceptance保持不变；新源码/研究结果待实际验证，PR合入与cleanup仍需明确动作授权。

## Background

2026-09-30 用户批准新建当前版 L1/L2 正式 executor，使用 active v15、full-v3-security-identity PIT 和既有 C-010/A5。
授权两个 family、seeds 42..49、两次 fresh process，共 5184 fits；使用现存 Conda base 固定环境。
这不是恢复 BUG-1549 的 B3/P6/D1 退役链，也不改写已交付 Rotation L2 产品链或历史实验终态。

## Scope

本任务还允许对 `backend/services/hmm_risk/rotation_l1_input_bundle.py` 的现有文件 stock-row 构造增加可选只读 callback；默认调用行为不变。用于复用已审查的 source/alias/PIT/circ_mv 构造，不复制实现，也不改产品公式、数据或其他模块。

2026-10-01 用户另行批准将本 executor 冻结输入更新为 v16 successor。允许在上述共享 HMM reader 中增加显式 frozen-release binding 参数，并复用相同 manifest/component 校验；普通产品入口仍强制 active profile，不改变其默认行为。对应身份测试纳入 `backend/tests/hmm_risk/test_rotation_l1_input_bundle.py`，不修改数据生产代码或流程文件。

2026-10-03 用户批准按复查建议执行：将冻结输入更新为最终 v17，重新构造完整 7D/20D C-010/A5 request，并执行零-fit file-only preflight 与 D6 数据可执行性检查。本轮只修改 executor/input 的冻结绑定及对应测试、本设计状态；不启动正式训练，不切换 active profile，不修改数据集。数据窗口只负责共享基础数据，特征构造、D6 carrier/readback 与可用日期复核由 HMM owner 直接执行。

以上是输入更新时的授权记录。随后用户单独授权源码合入和正式训练，PR #5314于merge `dc3511458a71fff811fbcf654628992657bae929`完成，5184 fits实际执行并形成下文终态。用户最新要求未来研究全部以申万L2为目标，正常停牌/持续行情等自然因素不得本身成为失败理由；本次文档方向更新不再次授予训练、阈值变更、合入、数据库或服务控制权限。

新增当前数值模块、输入桥接、离线 CLI 和 `backend/tests/hmm_risk` 定向测试。
训练与验收只采用既有详细设计中的 D3-03-A、D4-01-MAP-A、D4-02-A、D4-03-PERSISTENT-A、D5-01-B、D6-01-B + D6-NA-A。
对应权威：[Phase 2 详细设计](hmm_evolution_phase2_risk_monitoring_detailed_design_20260722.md)。

2026-10-01 源码提交审核允许在同一 HMM 测试包按当前源文件职责整理已有测试：`backend/tests/hmm_risk/test_formal_state_model.py`、`test_formal_state_model_set.py`、`test_freeze_formal_industry_authority.py`，并补齐 `test_formal_state_input.py` 的直接输入测试。只移动既有测试或增加实际缺失的合同测试，不复制测试、不建立兼容代理，不修改全局 CI/nox/test plan。源码修复仍限于既有 executor 和目录冻结 CLI 的合同回读、写入安全边界。

## Non-goals

不恢复旧模式、旧 CLI、兼容代理或历史 candidate；不安装依赖，不改 Conda AIstock。
不修改共享数据集、active profile、其他模块、数据库或服务进程。
不搜索参数、不扩 seed、不改训练/验证窗口、不使用 validation 重选 seed。
数值/语义模型验收不等于样本外轮动预测有效性，不自动生成或发布产品 READY。
原源码交付已完成；当前批准L2精确合同与最小实现/研究读回。既有训练工作树、请求及结果保留；不包含服务控制、数据写入或删除。

## Architecture（已执行原合同）

显式批准的共享最终 v17 文件与 full-v3 PIT → 完整 C-010/A5 构造 → 冻结全 7D/20D 输入及完整 D6 calendar → 两 child 独立执行各 2592 fits →
parent 验证重复结果 canonical bytes 和 entry semantic readback → 每 family/level 单一 train-only D5 selection → selected-only D6。
有 typed failure 则保留 blocked/failed，不以 fit completed 冒充 acceptance，不回到 D5 换 seed。

当前不允许用产品九维 input bundle 代替完整二十维 C-010 输入，也不允许用简化 contributor 统计代替 A5 full receipt。

## Contracts（已完成全grid实验的精确合同，不自动成为下一轮研究范围）

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

上述L1=31/L2=131和5184预算属于已经执行的单独批准实验，结果保持原合同效力。未来不启动新的L1 fits或研究候选；市场/既有L1只能作为明确批准的辅助context，不充当L2目标或发布前置。后续L2的精确合同、预算与执行授权另在本文件同一任务包收敛，不从历史全grid直接复制。

## Design Acceptance Index

- F-001：当前批准的最终 v17/full-v3 文件身份与完整 C-010/A5 输入闭合；v15/v16 结果仅作演进历史，不能冒充当前输入通过。
- F-002：D3/D4 原数值、MAP、covariance 和结构合同的完整源码实现与直接测试。
- F-003：两次 fresh-process 全 grid、零-refit readback 与 D5 train-only selection 的完整执行控制流和故障测试。
- F-004：D6-NA 完整 carrier/source/masks/ledger 与 selected-only semantic acceptance 的源码及共享 readback 验证。
- F-005：正式 5184 fits 和真实终态，禁止伪 READY。
- F-006：L2 persistent/右截尾及独立完整性精确合同已批准；源码、测试、研究读回待实际执行，不与历史结果混同。

## Implementation Plan

一个实现任务包完成 F-001～F-004、聚焦测试和多轮修复；不是为每个小功能另建阶段。
输入正式preflight、源码交付及5184-fit实验均已完成，F-005记录真实终态而不是通过声明。下一动作是复用结果闭合后续L2自然事件/语义/完整性合同，再按批准范围实现和最小验证，不复原退役链，不另建每个小规则的独立阶段。
本轮精确合同已批准，下一动作是同一任务包的源码/测试/zero-refit读回，不再次执行F-001～F-005。
源码合入、模型结构/语义验收、样本外效果和产品交付是不同结果，不以任一项替代其他项。当前不再次创建旧源码PR，也不启动重训。

## Verification Plan

基于 Conda base 运行定向 pytest、Ruff、py_compile、diff/ownership 检查；广泛模块覆盖交 CI。
真实源验证必须走正式文件 reader，拒绝训练时数据库 fallback、名称猜测、密集整数下标和旧 release fallback。
2026-09-30 用户另行授权基于数据库完成准备：仅在只读 repeatable-read 事务内提取官方行业目录及成员目录，
与 full-v3 taxonomy 精确连接并一次冻结现有 L1/L2 projection authority；不写数据库、共享数据集或 active profile。
数据库目录准备不是把数据库市场行情混入 v15，也不改变原 C-013 classification interval 的权威身份。
每轮审核明确记录发现与修复。任何输入缺口不得以默认值、占位回执或新阈值替代。

源码提交门禁覆盖F-001～F-004的完整实现及实际直接验证；F-005已经以真实实验完成，但原模型合同未接受。源码F2通过、正式训练完成都不能推出模型合格或产品READY。以下历史审核中的fits=0/未执行只描述相应提交时点，不是当前实验状态；本次不重复历史测试或训练。

## Design Acceptance Matrix

矩阵F-005验收对象是“完整执行并如实报告终态”，不是保证模型通过；`COMPLETE_RESULT_REPORT_MODEL_NOT_ACCEPTED`只说明结果报告已闭合。模型合同是否接受始终由acceptance中的`d3_d6_accepted=false`表示，不能由F2文档PASS升级。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/hmm_risk/formal_state_authority.py; backend/services/hmm_risk/formal_state_domains.py; backend/services/hmm_risk/formal_state_input.py; backend/services/hmm_risk/formal_state_executor.py | backend/tests/hmm_risk/test_formal_state_input.py；最终 v17 完整 prepare + 独立 fresh-process preflight PASS；request SHA=4e2cc82614c20f6fc31a224da0df0de29621abc9e7e762b88004ca1500eaa0ec，receipt SHA=94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7 | PASS | 无 |
| F-002 | backend/services/hmm_risk/formal_state_model.py | backend/tests/hmm_risk/test_formal_state_model.py；原数值/结构矩阵、MAP 联合停止、重哈希 drift、固定 19D projection 和零-refit 数值回读 | PASS | 无 |
| F-003 | backend/services/hmm_risk/formal_state_executor.py; scripts/hmm_risk/run_formal_state_model_set.py | backend/tests/hmm_risk/test_formal_state_executor.py; backend/tests/hmm_risk/test_formal_state_model_set.py；完整 mocked grid、signed-zero mismatch、parent 环境/flags 回读、child/finalization durable failure、CLI fresh-process 拒绝和 mixed-shape serialization | PASS | 无 |
| F-004 | backend/services/hmm_risk/formal_state_calendar.py; backend/services/hmm_risk/formal_state_input.py; backend/services/hmm_risk/formal_state_model.py; backend/services/hmm_risk/formal_state_executor.py | backend/tests/hmm_risk/test_formal_state_calendar.py；最终v17全324组182日carrier/ledger/hash预检PASS，E最少143；正式selected-only D6保留完整calendar和typed evidence结果；语义证据未全过见F-005，carrier/readback通过不等于semantic acceptance | PASS_IMPLEMENTATION_AND_READBACK | 无 |
| F-005 | scripts/hmm_risk/run_formal_state_model_set.py；merge dc3511458a71fff811fbcf654628992657bae929 | artifact: F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-formal-5184/run/acceptance.json；正式5184 fits，fresh_process_bitwise_equal=true；receipt SHA=fa42b6982f2ff5d8b5e7c257ad727ba66eb0ee9ac9a18e045847e573704a01ac；legacy两个层级无完整D5候选；autocycle均选seed47，L1/L2 D6分别29/31、121/131；d3_d6_accepted/ready/phase2_ready/product_capability_promoted均false，无发布产物 | COMPLETE_RESULT_REPORT_MODEL_NOT_ACCEPTED | 无 |
| F-006 | 本文件L2精确合同；实施限formal model/calendar/readback、CLI与直接HMM测试 | artifact: F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-formal-5184/run/acceptance.json；只用于原冻结身份；新测试/读回未执行 | APPROVED_BY_USER_CONTRACT_IMPLEMENTATION_PENDING | 用户明确批准A/B合同及后续实施；批准当前实现待验证状态，不推导READY |

以下2026-09-30～2026-10-02源码/输入记录保留其当时状态，不能当作当前待办；最新输入与正式模型结果见Rollout / Rollback的当前终态。

2026-09-30 历史准备回执：官方数据库目录在只读 repeatable-read 事务中提取并冻结到
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

### 历史执行授权：最终 v17 冻结输入更新与预检

最终 v17 state=`CANDIDATE_READY`，manifest 与文件 SHA 以 Contracts 为准。当前 main reader 独立 fresh process 已通过基础 source preflight，L1=31/L2=131，source inventory SHA=`c59f482664e147534e4fd893d17758129d3b832fece7051d4a14bb72c63373c9`；五个消费窗口基础覆盖 unresolved=0，7 项合法预热 NA 保留。三端交付回执的 66 个 component pins 一致，但不把数据 owner 的基础检查视为 HMM 完整 C-010/A5 或 D6 验收。

HMM owner 重新生成新 request，不覆盖 v16 资产，不修改旧 manifest 哈希，不读取源窗口之前的市值、不使用当日市值填补预热 NA。完整构造、独立 request readback 及全部 324 组 T/O/U/E 汇总的结果完成后更新 F-001。D6 carrier 合法与 |E| 足够均不代表 hard semantic/model 验收通过；本轮 fits=0、D5 selection=false、D6 model acceptance=false、model/READY=false。

### 已完成输入终态：最终 v17 完整输入与 D6 数据可执行性 PASS

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

两轮源码复审覆盖最终身份/旧输入拒绝、默认 active 消费行为不变及 producer/current-input/readback 闭合；修复测试包装器和临时目录问题后有效 RED=2 failed/1 passed，GREEN input/calendar=20 passed。classifier 绑定的 HMM slice=68 passed，registry=8 passed，L0=0 blocking，4/4 ownership、Ruff/format/diff 和 fresh-process router/health/formal import 通过。上述提交前证据当时F-005尚未执行，不把synthetic fit冒充正式训练；随后真实执行结果如下。

### 当前终态：最终 v17 双fresh-process 5184-fit完成，原完整合同未接受

不可变validation worktree：`F:/Dev/AIstock_validation_worktrees/hmm-formal-v17-5184-20261003`，源码merge为`dc3511458a71fff811fbcf654628992657bae929`。正式结果：`F:/Dev/AIstock_runtime/hmm_formal_state/20261003-v17-formal-5184/run/acceptance.json`；本轮只读复核其紧凑字段，没有重新fit或重写产物。

- fit_attempts=5184；fresh_process_bitwise_equal=true。
- repeat SHA=`e23d7c36fec13aa6a678b3780475e048ad217d764ccce31000bb21b6d2bd89c4`；acceptance receipt SHA=`fa42b6982f2ff5d8b5e7c257ad727ba66eb0ee9ac9a18e045847e573704a01ac`；request canonical SHA=`94b35f9c8b5767b1a5cb3ade009fcada1fe5811c2486404a9c0e1796b03084f7`。
- d3_d6_accepted=false；ready=false；phase2_ready=false；product_capability_promoted=false；database_write=false；runtime_action=false。没有accepted model-set或READY发布，不能把执行完成包装成正式模型通过。

| family / level | D5 | selected-only D6 | 结论 |
|---|---|---|---|
| legacy_covfix / L1 | 无完整candidate，不选seed | 未进入 | 原train数值/结构合同blocked |
| legacy_covfix / L2 | 无完整candidate，不选seed | 未进入 | 原train结构/初始化合同blocked |
| autocycle_all_core / L1 | train-only选seed47 | assignment 31/31；evidence 29/31 | 历史L1结果保留，不再新增L1研究 |
| autocycle_all_core / L2 | train-only选seed47 | assignment 131/131；evidence 121/131 | 可复用的L2研究结果；非预测效果通过或全模型READY |

10个L2 D6未通过行业均有O/U/E=182、availability events=0：`801032.SI`、`801033.SI`、`801045.SI`、`801084.SI`、`801141.SI`、`801204.SI`、`801223.SI`、`801733.SI`、`801743.SI`、`801783.SI`。其中4项主要涉及1～3日稀有状态，另6项主要涉及持续run、run集中或转换覆盖；分类仅用于解释，不豁免其他已记录reason。`801084.SI`末端run延续至验证末日，提示右截尾需单独合同；不能假造窗口外退出。

既有6个后来停止官方报价的L2（801019/801117/801207/801216/801961/801983.SI）本次autocycle D6均通过；官方停发起点2026-04-29晚于本次验证终点2025-03-31，不能用停发解释此轮失败。单成员`801207.SI`在autocycle版本也通过；legacy同一行业的稀有train状态属于模型结构证据问题，不是新增最小股票数门的依据。小行业集中度要披露，不自动失败。

legacy L1的`801130.SI`达到300 E-step仍未同时满足MAP/covariance自洽停止条件，不能按“行情差”豁免；legacy L2的`801207.SI`/`801782.SI`稀有或少月状态以及个别初始化簇不足，需与自然持续状态分开分析。初始化簇的样本数指日期观测，不是行业股票只数。

本轮不再补数、不重跑grid、不用validation换seed、不从D6通过子集倒推新eligible集合。结果只表示旧精确结构/语义合同未接受，不证明L2预测器整体无价值或必然有价值。

### 后续L2-only自然事件合同收敛（原则及以下精确合同已批准）

本节展开父蓝图§4.5，不覆盖上面的原D3～D6或旧acceptance。优先复用autocycle L2既有结果；legacy保留独立版本/结论，不默认作为产品成功前置，也不静默替换QE现用版本。一个任务包完成以下边界，不建设新平台或历史证据归档：

1. **正常停牌/合法NA**：按冻结authority和现有D6-NA-A保留calendar、O/U/E及transition-only递推；合法NA不记数据损坏。不插值、不填0、不删日期；若证据不足，诚实保留不足状态，不使其他合法行业自动失败。未知缺失与真实漏采仍fail closed。
2. **行情与预测分开**：低收益、持续fading/risk_off不是模型失败原因，预测这些事实可能有用；数值不收敛、错误预测/有害效果或证据不足仍是不同真实终态。最终任务验证L2历史样本外轮动排序、独立风险目标或消费者增量，不以三态遍历率替代业务效果。
3. **persistent与右截尾**：提出D6持续路径和窗口边界下可观测entry/exit分母，区分末端尚未退出与真实转换不足。保留posterior/utility有限性、hard authority和数值gap，不能临时删除不通过的阈值、使用未来窗口或新增自动豁免。路径互斥、精确count/month/run/transition/run-share公式及typed状态须一次闭合并获批后实施。
4. **稀有语义证据**：1～3日状态仍不足以证明稳定语义，不能强填mapping或把variance未定义称为源数据坏。新的train-only语义校准与独立确认方案须明确数据角色、因果性和样本要求；不能以当前validation失败后换seed或事后挑行业解决。
5. **独立L2完整性**：131目录逐项状态、预注册资格、合格输出及coverage分开；不把L1或另family的失败自动合取为L2产品失败。数据资格在访问validation结果前冻结，模型结果如实报告；不能只评价121个D6通过行业并声称全截面有效。正式指数输入与C-010股票事实聚合版各遵其合同，无指数不造指数、不默认回落其他层级。
6. **下一执行边界**：先给出本节精确合同及必要直接测试范围，审核通过并获模型合同批准后才修改代码/执行最小L2验证；保持尚未批准修改的features/window/seed/数值合同不变。不得直接把未来L2-only范围改写进历史5184请求。当前不启动新训练、实验、tail或服务。

处理原则及下面A/B精确合同已批准，不是所有模型必须通过。语义证据不足不等于数值/数据故障，也不能冒充已有可用预测；新的运行schema和失败reason仅按批准合同实施。

### L2精确修订决策包（一次闭合；2026-10-03精确合同已批准）

本包只调整L2语义证据结构解释与版本完整性，不修改D3/D4数值、D4 train gate、D5分数/seed、features、窗口、utility、PIT或release。按批准合同复用已有autocycle L2 selected模型执行zero-refit readback，不启动新grid，不修改原child/request/acceptance或模型hash；legacy与L1终态保持。

#### 决策A：C-008-L2-D6-PERSISTENT-RC-A

推荐合同版本：`hmm_risk_l2_d6_persistent_rc_a_v1`。仅适用于新增L2语义回读版本；原`hmm_risk_c008_b3_d6_01_b_na_a_v1`原位保持。

**共同条件原数值全部保留**：T仍为182日完整calendar，E=O∩U且|E|≥30；每state hard count≥max(5,ceil(0.02×|E|))、occupancy≥0.02、calendar months≥2；posterior finite/nonnegative/row-sum与top1-top2 margin、hard argmax唯一authority、utility sample variance/SE finite、原numeric adjacent utility gap与.35/.35/.30权重保持。不新增95% separation；共同条件不通过时仍无正式semantic mapping，不用persistent补缺态、singleton或soft mass。

定义与计算顺序：

1. 只从冻结完整T与E上的hard assignments提取state连续run；两个E位置仅当完整calendar下标相邻才属于同一run。合法NA会切断可观测run，不跨NA桥接，不以transition-only posterior冒充O/utility evidence。
   内部NA造成转换证据不可辨识时，只报告该行业该状态证据不足，不当作股票漏采或数值失败，也不自动否定其他行业；正常停牌不能成为绕过共同count/month/utility保护或伪造转换的理由。
2. `n_s`是E上的state count；`R_s`是上述run数；`S_s=max(run length)/n_s`。若n_s=0，不计算比例且共同条件明确不足。`I_s`/`X_s`仍只数相邻且双方都在E上的真实异state进入/退出事件。
3. `B_left(s)=1`仅当T的第0日属于E且hard state=s；否则0。`B_right(s)=1`仅当T的最后一日属于E且hard state=s；否则0。边界标志表示左/右截尾，**不是额外转换事件**；内部NA、首个/最后一个非边界E日不得取得此标志。
4. 共同条件检查后，`R_s=1 OR S_s>0.9`进入persistent；其余进入recurrent。路径互斥，share=0.9且R_s≥2只能走recurrent；路径由冻结数据的确定公式决定，不由操作者或失败后的重试选择。

| 路径 | 推荐精确结构条件 | 明确不证明的性质 |
|---|---|---|
| recurrent | R_s≥2；S_s≤0.9；I_s≥2−B_left(s)；X_s≥2−B_right(s)。保持实际I_s/X_s值，单独记录可观测边界与比较阈值，不伪造进入/退出 | 不据此证明状态收益显著、持续时间稳定或预测有效；右截尾只影响可观测边界，不免除count/month/utility |
| persistent | 共同条件全部通过；R_s≥1；每个非截尾且两侧可观察的run边界必须与实际hard state切换一致；I_s/X_s全部如实记录，不要求重复进出或S_s≤0.9 | 单一长期run只提供该窗口的状态条件utility证据，不是独立重复regime或可靠duration/hazard估计；不扩大为未来有效性结论 |

persistent不再额外复制D4的train六个月/30样本门到D6；本包保留原D6 common gate，不新增与自然持续现象对冲的更高阈值。相应代价是接受的语义统计可能依赖一段长期run，假接受风险必须在结果中明示；如果用户要求独立regime重复证据，该状态应报告不足，而不是事后提高阈值或再调模型。

原10个L2未通过项中，4项涉及1～3日rare state，仍不能因本包直接通过；其余6项主要涉及持续/转换覆盖。本包不是针对这6个代码的allowlist，必须同一规则回读全131行业；这是预期作用范围，不是新验收结果，不承诺新增通过数量。所有已知sample/utility/identity故障保留；不能在看到新结果后改0.9、路径或共同条件。
本包在已观察原validation结果之后制定；因此该窗口已经参与规则设计，未来zero-refit即使通过也只属开发期结构/语义校准结果，不是新的独立模型验证或untouched证明。新旧规则差异必须如实列出，不能追认原合同成功。

**状态/失败语义**：原`semantic_assignment_valid`与`semantic_evidence_valid`分开回读；数值/identity失败仍typed fail closed，共同证据或对应路径不足不生成mapping。保留具体reason与自然事件解释，不能把“证据不足”描述成基础数据缺失，也不能将其改成accepted。新版本及路径/censor字段须在获批实现时纳入原receipt/hash/schema回读；不建立平行writer或兼容代理。

#### 决策B：C-008-L2-INDEPENDENT-A

建议将本轮诊断对象固定为已有`autocycle_all_core:L2`、D5所选seed47、冻结v17及原selected model hashes；**不进行新的D5选择**。L1/legacy不参与该L2研究版本的合取。本包不宣称原“两family×两level”模型集通过，不更名/覆盖旧产物，不将新研究版本自动替换QE现用版本。

- 数据人口仍由冻结131目录、PIT与原预注册输入资格确定，不从新D6结果反推eligible；每个目录项都要回读并报告numerical/assignment/evidence状态及原因。未知源缺失、hash/schema漂移不因局部发布而容忍。
- 对合法NA、无官方报价和semantic evidence不足逐行业诚实报告；缺少正式mapping时不输出trending/neutral/fading、不补1.0或把不可用当末位。当前正式指数版与C-010股票事实版不互相替代。
- zero-refit成功只形成一个新的**结构/语义研究回读结果**，不直接写accepted_model_set/READY、不升级产品capability、不写数据库或runtime receipt。若需要将部分行业接入产品，coverage/评价人口及消费者状态必须在预测效果合同中明示，不把D6通过子集表现冒充全截面表现。
- 同一行业某family的真实numerical/train不足仍保留，不豁免旧D4。将来其他已批准L2版本可独立评价，但本包不启动它们的训练或自动择优。

#### 预测及QE消费的因果边界（不得由结构修订绕过）

当前D6用2024-07-01..2025-03-31的future excess utility完成语义校准，outcome watermark为2025-04-30。其mapping是该校准完成后才可用的研究结果，不能给同一校准窗口的QE历史交易贴上“当时已知”的状态/系数。即使posterior递推本身因果，使用后来才确定的mapping仍可能泄漏。

下一预测效果验证须明确选择：使用校准完成之后的合格历史评价区间，或另行批准严格walk-forward的train-only语义校准。评价区间起点不得早于所有模型/预处理/mapping实际available-at；末日必须满足完整outcome且不跨未授权tail边界；具体日期由冻结calendar算出后提交批准。不得把已消费development恢复成untouched，不因模型名称或hash相同而跳过available-at。
历史回放的available-at按对应批准的as-of合同解释，工程重建时间与模拟信息水位分别报告，不把本日生成模型伪称为历史实际发布版本。原stable-taxonomy-backcast的non-as-known局限保持披露；其历史研究结果不能冒充as-published PIT下的forward确认。

本包不替代现有QE正式历史系数资产；QE正式实验仍由QE窗口执行，HMM侧只交付通过其场景合同的冻结可选资产。新结构回读不能据此宣布QE增益、轮动Rank IC或风险precision通过。

#### 一个任务包内的实现、直接测试与停止条件

按批准合同一次完成D6新版本比较/回读及必要CLI路由，复用formal model/calendar/receipt。只改HMM-owned源码/CLI和backend/tests/hmm_risk，不改全局CI/nox/test plan，不复制实现。本次授权实施与zero-refit读回，不授权新训练、合入、cleanup、数据写入或进程控制。

直接矩阵必须覆盖：共同gate失败的singleton、所有boundary/tie取值；完整calendar首尾与内部NA不混同；两个独立run/单一长期run/share=0.9与>0.9；真实enter/exit计数不增加；unknown missing不当停牌；persistent不补mapping、D5不重选、不refit；旧合同结果不变；全131目录/模型hash/source identity闭合；mapping available-at与旧系数版本隔离。至少两轮审核，发现问题在同scope修复，不因新reason增加矩阵膨胀。

A/B均APPROVED_BY_USER。本次终点是源码/测试多轮审核通过及全131行业zero-refit研究结果/局限，提交PR后等待合入授权，不是保证全部通过或READY。不执行训练、tail、生产写入、依赖安装、profile切换或进程控制。

### 历史执行授权：v16 冻结输入更新与预检

以下各时间段保留当时的授权、输入失败和fits=0记录；不作为当前授权或待办，当前状态只以上面的v17正式终态为准。

用户已批准显式绑定 `20261001-v16-unified-moneyflow2`，并执行完整 file-only preflight。数据窗口已交付不可变 successor，历史 v15 的输入失败事实保留在下面的时间记录；当前结论以本节实际 v16 终态为准。
当时授权不启动正式训练、服务、数据集/profile 写入；不合入数据窗口 PR #5177，也不把 CANDIDATE_READY 当作 HMM 预检通过。
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

上述“仍在执行”为v15启动时记录；紧随其后是该阶段失败终态；v16预检是历史中间结果，当前结论以上面的v17正式终态为准。

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

最终v17完整C-010/A5构造、source/request身份及全324组D6 carrier预检已闭合；正式5184 fits已完成，原合同未接受。当前autocycle L2的10项D6未通过不涉及输入日期缺数；persistent/边界规则和稀有语义证据是下一合同问题，不交回数据窗口当作补数任务。真实MAP/covariance收敛问题保留，不因自然行情豁免。不能从输入PASS或121/131结构比例推导预测有效。
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
正式全grid实际出现legacy无完整D5 candidate、autocycle选中后D6未全通过；均保留真实结果，不改阈值、换seed或发布预测能力。未来L2-only精确合同不得回写旧验收。

## Production Gates

production_ddl_gate=noop；production_dml_gate=noop；dependency_install=noop。
Conda AIstock mutation=false；dataset/profile mutation=false；runtime/process control=false。
本轮仅修改本详细设计，父蓝图v2.62已由PR #5321合入：runtime_impact=none，无新增fit、数据库/数据集/profile写入或runtime activation，无后端重启需求。
原源码4个changed files的runtime分类为backend/target_ids=[backend-main]，该历史分类不因文档-only任务降级；源码合入、用户重启与正式训练各自按对应授权和证据记录，不由本次文档重新控制或代报运行态。后端进程控制仍归用户。

## DESIGN-COMPLIANCE-001 当前复审

1. 无简化交付：F-001～F-004完整源码已验证，完整C-010/A5及D6 carrier不用简化证据替代；F-005真实5184 fits完成但原模型合同未接受，源码/计算就绪不等于预测功能通过；未来L2-only是用户业务方向，不假装旧完整合同成功。
2. 无静默错误：source/seed/hash/tie/非有限值与 durable failure 采用显式拒绝，缺口继续报告。
3. 无业务漂移：原模型合同、目录/窗口/seed和结果不变；已批准L2精确合同仅用于新研究读回，不回写旧验收。
4. 无未经批准门禁/审批：不增加资源、availability ratio、统计significance、最小股票只数或人工sector特批；正常停牌/持续行情不伪造数据或成功。新增精确模型合同需既有批准程序，不新增审批层；本次不合入、不控制服务、不写数据、不清理资产。

本记录不是正式模型验收通过声明。F-005完成而原合同未接受；A/B精确合同已批准，源码/读回/效果/产品/运行态分别验收，不用文档validator替代实现证据。

2026-10-03文档两轮自审：第一轮修复历史fits=0与当前执行完成的表述混同，补齐F-004/F-005直接结果引用，明确结果报告完成不等于模型接受；第二轮核对父蓝图L2-only、合法自然事件与证据不足、旧阈值/目录/窗口/seed保持、停止发布与真实moneyflow分离及发布授权边界。未遗留文档范围阻断；新增精确D6修订仍待批准，不以文档F2 PASS冒充模型或源码验收。

以上为v0.5审核记录，PR #5321已合入。本轮v0.6第一轮修正current/proposal状态及单文档scope，补齐F-006待批准矩阵，明确内部NA不取得完整窗口截尾标志；第二轮复核共同数值原样、互斥路径、真实转换不增加、旧全grid不改判、已看validation不能冒充独立证明，以及语义mapping的future-utility/available-at与QE隔离。未发现文档范围阻断；不以两轮自审代替用户对新增精确合同的批准。
