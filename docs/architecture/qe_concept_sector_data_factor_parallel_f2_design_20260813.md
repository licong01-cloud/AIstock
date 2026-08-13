# QE 概念板块数据与因子并行研发 F2 详细设计

- 设计层级：F2
- 设计状态：`DESIGN_READY_FOR_PARALLEL_IMPLEMENTATION`
- 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.7+
- 适用范围：QE-only、candidate-only、文件数据面
- 创建日期：2026-08-13
- 设计目的：允许 Claude Code 在不阻塞、不改写当前 P0 三联诊断的前提下，并行完成概念板块 PIT 数据本地化与概念因子研发。

## 1. Background / 背景与当前结论

当前策略演进的唯一 P0 仍是更优多 Alpha 策略包。MA-E16 完整矩阵、model-age/refit、四格 sector oracle 与 benchmark/Brinson 由主线继续推进；概念板块数据与因子是独立的 P2 并行候选生产线，不是 P0/P1 的人工前置门禁。

概念板块对 A 股题材轮动、龙头扩散、成员参与度和跨概念传播具有潜在增量价值，但当前文件数据面只有稳定的申万 L2 行业键，尚无可证明的概念成员 PIT 资产。没有 PIT 有效区间的当前成员快照不能回填历史并冒充可部署结论。

历史文档 `docs/analysis/p2_relational_model_hist_master_feasibility_20260708.md` 只保留为当时的可行性记录。以下旧语义被本设计和父蓝图明确取代：

1. 不允许把静态 `stock2concept` 当前快照作为正式 HIST-concept 或概念因子结论；
2. QE composer、worker、factor、qrun 和回测不得读取数据库中的行业或概念成员表；
3. GAT/HIST 的先后结果不构成概念数据研发的 GO/STOP 门禁；
4. MASTER、HATS、超图和联合训练不属于本阶段交付。

## 2. Scope / 目标与交付范围

### 2.1 业务目标

1. 建立可复算的概念板块成员、概念目录和概念日级聚合 candidate 数据集。
2. 以同一冻结数据身份开发低成本、可解释的概念板块因子候选。
3. 明确每个因子的 `DIRECT_ALPHA`、`CONDITIONING_STATE`、`RELATION_PRIOR` 或 `NEGATIVE_CONTROL` 主角色。
4. 形成可供后续 QE matched trial 消费的文件、代码、manifest、指标与限制说明。
5. 保持当前 P0 三联诊断、MA-E16 归因和既有多 Alpha 运行链完全独立。

### 2.2 本阶段包含

- 概念数据源只读盘点、许可/频率/历史有效区间与可用时点审计；
- 外部来源原始响应的不可变 landing、规范化、PIT 区间化与 candidate 构建；
- candidate manifest、逐文件 SHA256、coverage/quality receipt 与确定性重建；
- 六个首批概念因子候选的独立代码、单元测试、h20 快筛和相关性证据；
- 文件型因子工作区和未来 QE 接入 handoff；
- 实现完成后更新本设计的验收矩阵和 receipt 引用。

### 2.3 分阶段交付物

| 阶段 | 交付物 | 是否可与 P0 并行 |
|---|---|---|
| C0 source audit | 数据源矩阵、PIT/代理等级、许可和缺口 | 是；只读/网络元数据，不占计算节点 |
| C1 candidate builder | 规范化合同、builder、manifest、测试 | 是；Windows CPU/IO 有界执行 |
| C2 candidate build | repo 外 candidate 文件与 receipt | 是；不切换生产，不同步计算节点 |
| F1 factor batch | 六个因子源码、fixture、h20 quick screen | 是；CPU-only，默认单进程 |
| F2 evidence | 四窗口指标、增量相关性、角色说明 | 条件并行；结果面写入另行授权 |
| I1 QE integration | composer/远端分发/matched trial | 否；在当前 P0 相关改动合入后另立 feature/实验任务 |

## 3. Non-Goals / 非目标与禁止事项

1. 不修改或替代 MA-E16、P0-D1、P0-D2、P0-D3 的实验设计、任务或结果。
2. 不修改多 Alpha 编排、QE composer、远端分发、UI、Archive、schema 或历史物化。
3. 不执行正式 QE 模型训练、HIST/HATS/GAT/超图/MASTER 接入或策略包 promotion。
4. 不读取数据库构建回测数据，不从数据库补齐缺失概念关系，不在实验运行时访问数据库。
5. 不把当前成员静态快照回填历史，不把代理数据冒充完整 PIT 数据。
6. 不覆盖生产 H5/Parquet/bin/sidecar，不修改 symlink/junction，不激活 candidate。
7. 不在 source worktree、`rdagent_assets` 或生产数据目录写运行产物。
8. 不增加收益阈值、人工审批、PASS/KILL 或论文结果门禁；指标只描述当前 candidate 证据。
9. 不执行 DDL/DML、依赖安装、后端/WSL API/远端服务启停或重启。

## 4. Parallel Ownership / 并行所有权与冲突隔离

### 4.1 当前主线所有权（Codex/策略演进窗口）

当前主线继续拥有：

- `docs/analysis/sector_rotation_factors_develop_spec_20260710.md`；
- MA-E16 完整归因与 P0-D1/D2/D3 设计、实验和结果；
- `backend/services/quantevolver/**`、`backend/services/multi_alpha/**` 中与当前 P0 有关的修改；
- WSL GPU 与远端节点的正式 QE 资源安排。

Claude Code 在 C0～F2 阶段不得修改上述文件或控制上述实验。父蓝图的进度由主线后续统一更新，避免两个窗口同时编辑同一总账。

### 4.2 Claude Code 首轮允许写入范围

Claude Code 必须从合入本设计后的最新 `origin/main` 创建新的 task worktree。初始 `allowed_write_scope` 只包含下列精确文件：

- `docs/architecture/qe_concept_sector_data_factor_parallel_f2_design_20260813.md`（仅实现 receipt/矩阵更新）；
- `backend/services/dataset_release/concept_contract.py`；
- `backend/services/dataset_release/concept_source.py`；
- `backend/services/dataset_release/concept_candidate.py`；
- `backend/tests/dataset_release/test_concept_contract.py`；
- `backend/tests/dataset_release/test_concept_source.py`；
- `backend/tests/dataset_release/test_concept_candidate.py`；
- `scripts/build_qe_concept_candidate.py`；
- `scripts/factors/concept/common.py`；
- `scripts/factors/concept/m_concept_momentum_acceleration_5d_20d.py`；
- `scripts/factors/concept/m_concept_breadth_acceleration_5d_20d.py`；
- `scripts/factors/concept/m_concept_leader_diffusion_5d_20d.py`；
- `scripts/factors/concept/m_stock_concept_relative_strength_20d.py`；
- `scripts/factors/concept/m_concept_dispersion_compression_5d_20d.py`；
- `scripts/factors/concept/m_concept_crowding_exhaustion_5d_20d.py`；
- `backend/tests/quantevolver/test_concept_factor_candidates.py`。

不得借助 glob 修改已有 `dataset_release` 共享文件。若实现确实需要修改既有 registry、composer、loader、remote dispatch 或公共合同，先报告精确文件和原因，另立 I1 feature 或更新当前 Feature Card 后再继续，不在原 scope 中顺带修改。

### 4.3 旧工作树边界

现有 `F:/Dev/AIstock_worktrees/sector-rotation-concept-blueprint-20260712` 是旧文档工作树，不属于本任务。任何窗口不得进入、复用、清理或覆盖它；新工作必须从最新 `origin/main` 创建新的独立 worktree。

### 4.4 资源所有权

- C0/C1/C2/F1 默认 CPU-only、单进程、最多 4 个 BLAS/OpenMP 线程；
- 不占用 WSL GPU，不向远端节点提交训练或回测；
- 大文件按日期/来源流式或分区处理，不一次性把全部历史成员展开到内存；
- 若未来 F2 指标计算需要 WSL，只能在主线没有占用对应资源并完成显式资源协调后执行；
- I1 正式 QE 的 WSL 并行度继续服从全局 1，远端并行度服从当时主线资源合同。

## 5. Architecture / 目标架构

```text
external provider API / immutable raw files
  -> raw landing (request fingerprint + response sha256)
  -> source normalizer (provider id -> canonical concept id)
  -> PIT interval builder (effective_from/effective_to_exclusive/available_at)
  -> concept candidate root
       concept_catalog.parquet
       concept_membership.parquet
       concept_daily.parquet
       manifest.json
       quality_receipt.json
  -> scripts/factors/concept/* (file-only)
  -> result.h5 + quick-screen receipt (candidate evidence)
  -> later I1 integration PR
  -> QE matched trial / multi-alpha evolution
```

所有箭头均为显式文件或外部 provider API。数据库只允许在后续单独授权阶段保存控制面、因子 catalog 和结果指标；数据库永远不是训练、预测、因子计算或回测输入。

## 6. Contracts / 数据与接口契约

### 6.1 Candidate root

物理输出根必须由显式 `QE_CONCEPT_CANDIDATE_ROOT` 或 CLI `--output-root` 指定，并满足：

- 位于 repository/worktree、`rdagent_assets` 和生产数据根之外；
- 目标目录不存在，或与同一 functional manifest 完全一致；
- 禁止静默回退当前工作目录、临时 source worktree 或生产 snapshot；
- 禁止 symlink/junction；
- builder 先写同文件系统临时目录，逐文件回读并校验后原子 rename。

建议业务身份：

```text
qe_concept_<provider>_<start>_<cutoff>_<manifest_sha256_prefix>
```

物理路径不进入业务 hash。同一 functional request 重建必须得到相同内容 hash；`retrieved_at` 等观察字段放入 receipt，不污染 functional identity。

### 6.2 `concept_catalog.parquet`

每行描述一个带版本和有效区间的概念名称记录：

| 字段 | 合同 |
|---|---|
| `canonical_concept_id` | 稳定、非空；名称变更不得产生新 ID |
| `provider_concept_id` | 来源原始 ID，字符串保存 |
| `concept_name` | 当时可见名称 |
| `effective_from` | 含首日，交易日期或来源业务日期 |
| `effective_to_exclusive` | 不含尾日；开放区间为空 |
| `source_available_at` | 研究者可获得该记录的最早时间，UTC |
| `source_name/source_version` | 来源及发布版本 |
| `evidence_quality` | `PIT_COMPLETE/PIT_PARTIAL/SNAPSHOT_PROXY` |
| `record_sha256` | 规范化业务字段 hash |

### 6.3 `concept_membership.parquet`

每行描述一段股票—概念成员关系有效区间：

| 字段 | 合同 |
|---|---|
| `instrument` | 规范化 `SH/SZ/BJ` 股票代码；股票池策略另记录 |
| `canonical_concept_id` | 必须存在于 catalog |
| `effective_from` | 成员关系开始日，含首日 |
| `effective_to_exclusive` | 成员关系结束日，不含尾日 |
| `source_available_at` | 决策时可见时间，UTC |
| `source_name/source_version` | 来源身份 |
| `evidence_quality` | 与该关系真实可证明程度一致 |
| `record_sha256` | 规范化业务字段 hash |

同一股票同日属于多个概念是合法语义；同一 `(instrument, concept_id)` 的有效区间不得重叠。缺少开始日、结束语义或可用时点的当前快照只能标为 `SNAPSHOT_PROXY`。

原始 landing 可以保留 provider 返回的 SH/SZ/BJ 行以便覆盖审计，但 factor-ready candidate 的投资域必须逐值继承 manifest 钉住的冻结 QE universe；当前 universe 不含 BJ 时不得把 BJ 成员引入因子样本。out-of-universe 行进入明确的 exclusion receipt，不静默删除，也不因 provider 覆盖改变股票池。

### 6.4 `concept_daily.parquet`

以冻结 `daily_pv.h5` 和 PIT membership 派生，索引为 `(datetime, canonical_concept_id)`，至少包含：

- `member_count/eligible_member_count/coverage_ratio`；
- `equal_weight_return_1d/5d/20d`；
- `advancer_ratio_1d/5d/20d`；
- `amount_sum/amount_participation`；
- `member_return_dispersion_5d/20d`；
- `leader_return_5d/follower_return_5d`。

所有统计只消费 `datetime` 当日或之前已生效且 `source_available_at <= decision_as_of` 的成员。成分、价格或历史长度不足时写 `NaN` 和 coverage 字段，禁止填零。

### 6.5 `manifest.json`

manifest 至少钉住：

- schema/version、dataset id、functional request hash；
- provider、source version、请求参数 fingerprint、raw response hashes；
- decision-time policy、timezone、calendar identity；
- 股票池、基础 H5/Parquet/bin identity 与 cutoff；
- 各 artifact 的相对路径、SHA256、行数、列顺序、日期范围；
- 概念数、成员数、区间数、每日覆盖率摘要和 evidence-quality 分布；
- builder commit、Python/pandas/pyarrow 版本；
- 无秘密的凭据位置标识，不记录 token/password 内容。

### 6.6 数据质量等级

| 等级 | 含义 | 允许用途 |
|---|---|---|
| `PIT_COMPLETE` | 范围内成员增删与可用时点可证明 | 正式 matched factor/QE candidate |
| `PIT_PARTIAL` | 只有明确时间段或部分概念可证明 | 限定样本研究并报告 coverage loss |
| `SNAPSHOT_PROXY` | 只有当前或少数截面 | 敏感性/代理诊断，不得冒充历史 PIT Alpha |

等级低不自动终止研究，但结果必须按等级分层，不能混合后输出一个未标注指标。

## 7. Initial Factor Scope / 首批概念因子范围

首批因子只使用 `daily_pv.h5`、`concept_membership.parquet` 和 `concept_daily.parquet`。共同输出为单列 `result.h5`，索引严格为 `(datetime, instrument)`；无可用概念关系的股票为 `NaN`，不得填零。

| 因子名 | 主角色 | 设计含义 | 主要对照 |
|---|---|---|---|
| `m_concept_momentum_acceleration_5d_20d` | `DIRECT_ALPHA` | 股票所属概念的短期相对中期动量加速度 | 申万行业动量、股票自身动量 |
| `m_concept_breadth_acceleration_5d_20d` | `CONDITIONING_STATE` | 概念上涨成员比例的 5D 对 20D 加速 | 行业 breadth、等权概念动量 |
| `m_concept_leader_diffusion_5d_20d` | `DIRECT_ALPHA` | 以 t-1 可见领导组定义，度量领先收益向非领导成员扩散 | 领导持续性、随机组负对照 |
| `m_stock_concept_relative_strength_20d` | `DIRECT_ALPHA` | 个股相对其有效概念等权收益的强弱 | 行业相对强弱、裸股票动量 |
| `m_concept_dispersion_compression_5d_20d` | `CONDITIONING_STATE` | 概念成员收益离散度短期相对中期压缩/扩张 | 行业离散度、波动率压缩 |
| `m_concept_crowding_exhaustion_5d_20d` | `CONDITIONING_STATE` | 高收益后 breadth/amount participation 走弱的耗竭状态 | 原方向、静态阈值负对照 |

一股多概念的主聚合规则固定为：对当日所有有效概念信号做等权平均，并另存 `active_concept_count` 诊断；不得复制股票决策行。概念规模敏感性可另做等权概念与 `1/sqrt(member_count)` 归一化对照，但不能用测试期选择聚合方式。

因子公式、窗口和方向在首次 quick screen 前写入 factor card。弱结果作为当前公式证据保留，不触发方向删除；若角色从 `DIRECT_ALPHA` 改为状态或关系先验，必须同步 factor card 与本设计 receipt。

## 8. Factor Evidence Contract / 因子证据合同

1. 先通过因子库只读接口检查名称、语义和相关性重复，不直接写 catalog。
2. quick screen 使用 `scripts/quick_ic_screen.py --horizon 20 --split-manifest <manifest>`，标签保持 h20 裸前向收益。
3. 至少报告 `full/out_sample/recent_6m/recent_3m` 的 IC、RankIC、ICIR、coverage 和方向一致性。
4. 与行业、股票动量、breadth、压缩和领导类现有因子做增量相关性；`0.8` 只作拥挤诊断，不是人工淘汰门槛。
5. 概念因子和行业对照必须使用同一 observation panel；不同 evidence-quality 或 coverage 的总体不得直接比较。
6. 不把 quick-screen PASS/MARGINAL/KILL 文案当作研究许可；所有可计算结果自然记录。
7. 因子 catalog、指标、分类和相关性属于结果面；只有未来任务明确授权后才通过应用 API 写入，禁止本阶段直接 SQL/DML。
8. 进入正式 QE 前必须另立 I1 task，钉住 candidate manifest、factor code hash、label、split、策略和成本。

## 9. Source Acquisition / 数据源选择与本地化顺序

### 9.1 C0 数据源审计

Claude Code 首先只读比较候选来源：

- 是否提供稳定概念 ID 和成员增删历史；
- 是否提供公告/生效/采集可用时点；
- 是否允许历史研究与本地缓存；
- 最早日期、更新频率、退市股票与概念改名处理；
- 请求限额、分页、重复和修订语义；
- 能否直接形成 `PIT_COMPLETE`，否则属于哪种部分/代理等级。

数据源选择写入 source matrix；不得因为某个来源名称熟悉就假定其具备历史 PIT。

### 9.2 C1 原始 landing

- 每个请求保存 provider、endpoint key、非秘密参数、分页游标、请求时间、响应 hash 和 row count；
- 重试保留相同 functional request identity，错误显式落 receipt；
- 不把 HTML/UI 当前成分抓取冒充历史成员；
- 凭据只从既有秘密位置读取，日志和 manifest 不输出值。

### 9.3 C2 规范化与 candidate build

- 代码规范化、ID 映射、区间合并、修订处理和逐日 coverage 全部确定性；
- 第一版只在单一 provider 内以稳定 provider concept id 形成 canonical id；禁止按概念中文名称模糊合并跨 provider 记录；
- provider 修订不得静默改写已有 candidate，必须形成新 dataset identity；
- candidate build 可以重跑，但不得覆盖另一 identity 或生产数据。

## 10. Failure Semantics / 失败语义

| 条件 | 必须行为 |
|---|---|
| 无历史有效区间 | 标记 `SNAPSHOT_PROXY`；禁止生成 PIT_COMPLETE receipt |
| 成员区间重叠/倒置 | typed failure，列出首个 key 和来源记录 hash |
| orphan concept id | typed failure，不猜测名称或创建随机 ID |
| 跨 provider 仅名称相似 | 保持独立 ID；需要合并时另立可审核 mapping，不做模糊匹配 |
| source_available_at 晚于决策日 | 对该决策日不可见，不回填 |
| 股票代码无法规范化 | 隔离到 rejection receipt，不静默删除 |
| 成员不在冻结 QE universe | 写 exclusion receipt，不改变 universe、不加入因子样本 |
| 基础 H5/日历 hash 漂移 | typed failure，要求新 candidate identity |
| 输出路径在 repo/生产根 | 拒绝运行，不回退 cwd |
| artifact hash/readback 不一致 | 不发布 candidate 目录 |
| 因子无有效概念/历史不足 | 输出 NaN 与 coverage，不输出 0 |
| 数据库驱动/连接被调用 | 测试 fail；运行立即失败，不允许 fallback |

## 11. Implementation Plan / 实施方案

### PR-A：candidate 数据合同与构建器

1. 注册独立 feature task/card 和精确 allowed write scope。
2. 实现 `concept_contract/source/candidate` 新文件及 CLI。
3. 用小型真实结构 fixture 覆盖改名、增删、重叠、晚到、多概念和代理等级。
4. 执行 source audit，选择一个明确来源和 evidence level。
5. 在 repo 外构建一个 candidate，生成 manifest/quality receipt。
6. 只提交源码、测试、设计 receipt；candidate 大文件不进入 Git。
7. 完成审核、PR 与 CI；是否合入由用户单独确认。

### PR-B：首批概念因子候选

1. 从合入 PR-A 后的最新 `origin/main` 创建新 worktree。
2. 只读执行因子库去重和现有行业因子对照盘点。
3. 实现 `scripts/factors/concept/**` 六个候选及共享纯文件 loader。
4. 在 tmp/candidate workspace 运行格式、PIT、DB-poison、确定性和 h20 quick screen。
5. 生成因子 evidence receipt；不做 catalog DML、不启动正式 QE。
6. 完成审核、PR 与 CI；是否合入由用户单独确认。

### I1：后续 QE 接入（不属于 PR-A/PR-B）

只有数据和因子本地化证据完成后，另立 feature：

- 增加 QE workspace 文件分发和 loader 合同；
- 以 matched LGBM/低成本模型先运行概念因子独立 trial；
- 根据 P0-D2 sector oracle 结果决定两层概念模型或 HIST-concept；
- 再评估是否进入 multi-alpha blend/LOO。

I1 不得反向修改 PR-A 的历史 candidate，也不得把未授权的 composer/remote changes 塞入 PR-B。

## 12. Verification Plan / 验证方案

### 12.1 数据合同测试

- catalog/membership schema、列序与 dtype；
- 同股多概念合法、同股同概念区间不重叠；
- effective-to-exclusive 和 decision-as-of 边界；
- late availability、rename、removal/re-entry；
- 冻结 QE universe 继承、BJ/out-of-universe exclusion receipt；
- 跨 provider 名称相似时保持独立 ID；
- snapshot proxy 不能升级为 PIT；
- raw/normalized/artifact hash 确定性；
- output root 边界、无 symlink/junction、原子发布；
- DB driver poison 下完整 build 成功。

目标测试：`python -m pytest backend/tests/dataset_release/test_concept_*.py -q`。

### 12.2 因子测试

- 每个因子只读冻结文件；
- MultiIndex/单列/命名和 NaN 语义；
- t 日结果不因 t+1 成员、价格或修订变化；
- 多概念聚合不复制股票行；
- 领导组只由决策前信息定义；
- fixture 手算与向量化实现一致；
- 临时输出全部隔离到 `tmp_path`；
- DB poison、无网络、无工作树写入。

目标测试：`python -m pytest backend/tests/quantevolver/test_concept_factor_candidates.py -q`。

### 12.3 设计与源码门禁

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_concept_sector_data_factor_parallel_f2_design_20260813.md --tier F2`；
- changed-file lint/compile；
- `git diff --check`；
- scope/ownership guardrail；
- PR/CI receipt 绑定最终 HEAD。

### 12.4 结果验证

- candidate manifest 中所有文件逐一 SHA256 readback；
- 每日概念数、成员数、coverage、成员变更和拒绝记录可复算；
- factor result 的日期、股票数、coverage、分布与 h20 指标齐全；
- evidence-quality 分层结果不混合；
- 没有正式 QE task、生产 symlink、数据库 data-plane 读取或 runtime activation。

## 13. Design Acceptance Index / 设计验收索引

| ID | requirement |
|---|---|
| F-501 | 概念数据与因子作为 P2 candidate 生产线并行，不阻塞或改写当前 P0 三联诊断 |
| F-502 | Claude 与主线拥有互斥文件、worktree 和计算资源边界，scope 扩张必须先显式登记 |
| F-503 | 概念目录、成员、日级聚合和 manifest 使用稳定、版本化、可复算合同 |
| F-504 | PIT_COMPLETE/PIT_PARTIAL/SNAPSHOT_PROXY 分层，当前快照不得冒充历史 PIT |
| F-505 | 数据采集、本地化、因子与 QE 数据面均无数据库输入或 fallback |
| F-506 | candidate 输出根位于 repo/生产根之外，原子发布且不覆盖生产或其他 identity |
| F-507 | 一股多概念使用稀疏关系与唯一股票输出，不复制决策样本 |
| F-508 | 六个首批因子角色、名称、h20 标签、聚合和对照合同稳定 |
| F-509 | 因子证据包含四窗口、coverage、方向和增量相关性，指标不构成未授权研究门禁 |
| F-510 | PR-A 数据构建与 PR-B 因子研发分离，I1 QE 接入另立任务 |
| F-511 | 缺失、漂移、晚到、区间冲突、hash 错配和 DB 调用全部显式失败或分层，不静默填零 |
| F-512 | 不新增 UI、Archive、schema、历史回填平台、HIST/HATS/MASTER 或生产激活 |
| F-513 | 测试覆盖 PIT、确定性、DB poison、资源/路径隔离和文件数据面 |
| F-514 | merge、candidate build、结果面写入、数据激活、依赖和进程控制保持独立授权 |
| F-515 | DESIGN-COMPLIANCE-001 四项逐项审查且所有实现 receipt 绑定最终 HEAD |

## 14. Design Acceptance Matrix / 设计验收矩阵

本矩阵的 `ready` 只表示设计给出稳定实现引用和可执行验证路径，不表示源码、数据、指标、PR 或运行态已经完成。实现任务必须用真实代码、测试和 artifact receipt 更新对应行后，才能报告该实现项完成。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-501 | 本文 §1～§4；父蓝图 v6.7 | validation-receipt: design review confirms concept lane remains P2 parallel and P0 unchanged | ready | none |
| F-502 | 本文 §4 | validation-receipt: allowed-write-scope and ownership matrix documented | ready | none |
| F-503 | `backend/services/dataset_release/concept_contract.py`; `concept_candidate.py` | `backend/tests/dataset_release/test_concept_contract.py`; `test_concept_candidate.py` | ready | none |
| F-504 | `backend/services/dataset_release/concept_contract.py`; source matrix receipt | `backend/tests/dataset_release/test_concept_contract.py` | ready | none |
| F-505 | builder and `scripts/factors/concept/**` | `backend/tests/dataset_release/test_concept_candidate.py`; `backend/tests/quantevolver/test_concept_factor_candidates.py` | ready | none |
| F-506 | `scripts/build_qe_concept_candidate.py` | `backend/tests/dataset_release/test_concept_candidate.py` | ready | none |
| F-507 | `concept_membership.parquet`; `scripts/factors/concept/common.py` | `backend/tests/quantevolver/test_concept_factor_candidates.py` | ready | none |
| F-508 | `scripts/factors/concept/m_concept_*.py`; factor cards | `backend/tests/quantevolver/test_concept_factor_candidates.py`; artifact: future factor evidence receipt | ready | none |
| F-509 | h20 quick-screen and correlation receipt | artifact: future factor evidence receipt | ready | none |
| F-510 | 本文 §11 PR-A/PR-B/I1 | validation-receipt: separate phase and file ownership review | ready | none |
| F-511 | typed errors in candidate/factor loaders | `backend/tests/dataset_release/test_concept_candidate.py`; `backend/tests/quantevolver/test_concept_factor_candidates.py` | ready | none |
| F-512 | changed-file scope | validation-receipt: scope audit excludes runtime/UI/DB/schema/model integration | ready | none |
| F-513 | 本文 §12 | `backend/tests/dataset_release/test_concept_*.py`; `backend/tests/quantevolver/test_concept_factor_candidates.py` | ready | none |
| F-514 | 本文 §17 Production Gates | validation-receipt: action states listed separately | ready | none |
| F-515 | 本文 §15 | validation-receipt: itemized DESIGN-COMPLIANCE-001 review | ready | none |

## 15. DESIGN-COMPLIANCE-001 Review / 设计符合性审查

1. **禁止简化交付**：设计覆盖真实 PIT/代理分层、数据获取、candidate、六个因子、证据、并行所有权、失败语义和后续接入；不把静态快照、fixture、单因子或空 manifest 声称为完整数据/因子交付。
2. **禁止静默错误**：缺区间、晚到、orphan、代码失败、hash 漂移、coverage 缺失和 DB 调用都有明确 failure/quality 语义；无成员和历史不足使用 NaN，不以 0 或当前快照回填。
3. **禁止改变业务逻辑**：当前 P0、MA-E16、QE 策略、标签、成本、资源合同和多 Alpha 编排均不改变；概念结果以后只能通过独立 I1 matched trial 加入演进。
4. **禁止私增门禁审批**：PIT/identity/hash 是数据正确性合同，不是收益门禁；IC、相关性和论文方法只记录证据，不引入人工 PASS/KILL、二次 promotion 或历史平台前置。

## 16. Risks / 风险与失败模式

| 风险 | 处置 |
|---|---|
| 来源只有当前成员 | 明确 `SNAPSHOT_PROXY`，限定敏感性研究 |
| 供应商事后修订历史 | 每次 raw response/version 新建 candidate，不覆盖旧 identity |
| 概念改名或合并 | 稳定 canonical id + 有效区间；保留 provider id 和映射 receipt |
| 一股多概念导致权重膨胀 | 唯一股票行 + 规范化聚合，不复制样本 |
| 热门大概念支配信号 | 保存 member_count；等权和规模敏感性分开报告 |
| 概念数量/成员稀疏 | coverage 与 evidence level 分层，不填零 |
| 因子与行业/动量高度重复 | 增量相关性与 matched control，自然记录而非静默淘汰 |
| 数据构建占满主机 | 单进程、线程上限、分区/流式；不占 GPU/远端节点 |
| 与 P0 修改同文件 | Claude scope 禁止 quantevolver/multi_alpha/父蓝图；I1 后置 |
| 旧 feasibility 语义复活 | 本文明确取代静态快照正式结论和 DB 路径 |

## 17. Production Gates / 动作边界（不定义科研门禁）

| action | 本设计交付状态 | 独立授权/后续动作 |
|---|---|---|
| 本设计 source merge | pending user confirmation | 用户确认后才合入 |
| Claude PR-A/PR-B 实现 | not started | 本设计合入后分别立项 |
| provider 凭据/网络采集 | not executed | 使用既有秘密位置；不得输出内容 |
| candidate build | not executed | repo 外新 identity；不覆盖生产 |
| factor catalog/metrics/classification writes | noop | 后续明确授权，通过应用 API |
| formal QE experiment | noop | I1/实验任务另行安排 |
| production dataset/symlink activation | noop | 需要独立、精确 candidate 授权 |
| DDL/DML | noop | 本设计和 PR-A/PR-B 默认不需要 |
| dependency install | noop | 如确有新增依赖需独立报告和授权 |
| backend/WSL API/remote service restart | user-owned/noop | 本设计不产生进程控制授权 |
| UI/Archive/history backfill | prohibited | 不进入本路线 |

## 18. Rollout / Rollback

### 18.1 Rollout

1. 本设计与父蓝图最小引用先通过 docs PR 合入。
2. Claude Code 从新的 `origin/main` 分别执行 PR-A、PR-B。
3. 每个 PR 使用最终 HEAD 重跑最小门禁并由用户分别决定是否合入。
4. 数据和因子证据完成后，主线在父蓝图中更新进度和角色判断。
5. 只有 I1 明确立项后，才把 candidate/因子加入 QE matched trial。

### 18.2 Rollback

- docs PR 回滚只撤销本设计引用，不修改任何数据或运行态；
- candidate 未激活，回滚实现 PR 不需要切换生产数据；
- candidate 文件按明确 dataset id 保留或由未来精确清理授权处理，不通配删除；
- 因子未写 catalog/未进入 QE 时，回滚仅撤销源码；
- 若未来已激活，必须按当时 release manifest 恢复明确上一 identity，不扫描 latest。

## 19. Review Record / 审核记录

- Round 1：F2 validator 通过 15/15；人工审计发现父蓝图仍保留 MA-E16 10/12 的旧当前态，不能作为 Claude 并行依赖基线。
- Round 2：实时回读 8001 后把父蓝图更新为 MA-E16 12/12、六组 matched pair 与归因待办；保留 v6.3～v6.6 历史快照不改写。
- Round 3：把 Claude 初始 scope 从宽泛新增文件模式收紧为 16 个精确文件；再次检查数据零数据库、PIT/代理分层、资源互斥、PR-A/PR-B/I1 分离和四项 DESIGN-COMPLIANCE。
- Round 4：固定冻结 universe 继承、BJ/out-of-universe exclusion receipt 与跨 provider 禁止名称模糊合并，避免数据本地化阶段自行改变股票池或关系身份。
- Round 5：对新 F2 设计和父蓝图分别执行 feature validator、`git diff --check`、关键状态 token、secret 和 changed-file scope 复核；最终 receipt 以 PR HEAD 的校验结果为准。
