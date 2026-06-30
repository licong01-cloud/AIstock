# F2 设计：策略包资产自包含固化 + 因子库分级保护 + 候选阶段退役

- 日期：2026-06-30
- 作者：战略 session（AIstock + RDAgent + QE + Paper 总指挥）
- 性质：**F2 实现设计文档**。实现走后续 worktree + PR + Tier2，本文档不含代码。
- 前置依赖：**manifest sha 完整性 bug（10/15 包 mismatch）必须先单独修复**（独立 PR），本设计的回填固化在干净 manifest 上进行。

## 1. 背景（Background）

用户目标：多Alpha 组合从实验 → 模拟盘/选股/荐股/实盘 的全链路架构完善，确保实验经回测验证后可在 UI 完成进入策略包并执行后续。深挖代码（read-only 调研 + 生产 DB 实测）后发现三重纠缠的架构债，方向已与用户逐条确认。

**核心认知（用户更正后确立，代码自证）：**

1. **回测预测产物 `pred.pkl`/`combined_prediction.pkl` 无任何运行价值**。运行时是"用模型对当前 DB 数据窗口重算因子、应用已保存 QE 模型"，**从不读回测预测**。代码自证：`backend/services/strategy_package/live_inference.py:4-6` docstring —— *"It never reads QE backtest `pred.pkl` as a current selection signal; scores must be produced by recomputing factors from the current DB data window and applying the saved QE model."* S1 promotion 写入的 `prediction_ref_uri` 指向 combined_prediction.pkl，运行时零消费（仅 `components.py:120`/`repository.py:115` 做 API 展示），是**死字段**。

2. **运行时真正需要的只有两类资产：模型 params.pkl + 因子代码 .py**。但当前**没固化进包**——每次生成信号，运行时回 QE 节点重新下载 conf.yaml / 每个 `factors/<name>.py` / params.pkl（`live_inference.py:1166-1358`，默认 `allow_cache_fallback=False`）。删 QE 实验行（`live_inference.py:868` `SELECT FROM qe_experiments` 找不到）或清节点 workspace → 包**新交易日选股静默失败**（`DataUnavailableError`），仅已缓存的 `selection_score_artifact` 旧日期还能出。

3. **生产实测**：15 个策略包**全部**是"冻结指针"非"冻结副本"——`freeze_manifest`（`manifest.py:31-34`）只算 sha256、零字节复制；`FactorAsset` 按名引用（`qe_source_resolver.py:387-388`，无 hash/版本）；`package_asset` 表（`trading_core_v2_schema.sql:153-161`）+ `FACTOR_CODE`/`MODEL` 资产类型（`package_asset.py:14`）已定义但**从未被使用**。

4. **qe_archive 数仓只记录指标/溯源、不存资产**——现状**符合**预期，无偏差（运行时零读 qe_archive，仅 promotion/分析读）。

5. **因子库无保护**：deprecate（`factor_library.py:328-340`）和硬删（`quantevolver.py:1134`）都不查是否被策略包引用，能把权威代码删除/禁用在部署包之下。

6. **候选(candidate)阶段冗余**：单/多Alpha 之间不对称（单Alpha loop 有「加入候选」按钮 → candidate；多Alpha 直接建包）。候选的立身价值是"防 QE 源在建包前被清"，但该价值在"建包=真固化"成立后即冗余——正确方向是**两边都砍候选、直接建包**，而非给多Alpha 也补一套候选。

## 2. 范围（Scope）

本设计交付一份统一架构方案，分 7 阶段（[0]-[6]），实现阶段按依赖顺序分批 PR：

- [0] 存储适配层（storage adapter，DB 元数据 + 字节存储，先本地内容寻址，预留对象存储后端）。
- [1] 包固化：模型 params.pkl + 因子 .py 复制进包自有存储（用 `package_asset` + `FACTOR_CODE`/`MODEL` 资产类型）。
- [2] 运行时改读包自有资产，不回 QE 节点 / 不读 `qe_experiments`。
- [3] 回填固化：存量 15 包 + 多Alpha component 子包（趁 QE 数据还在）。
- [4] 因子库分级保护：硬删被引用因子→禁，deprecate→可。
- [5] 候选退役：QE 单/多Alpha 直接建包 + 删 candidate 后端服务/端点/前端按钮/管理页 + 删 `prediction_ref` 死字段。
- [6] 实验/包可自由删除（固化完成后无需 guard）。

## 3. 非目标（Non-Goals / 边界）

- **不上对象存储/MinIO**（用户本轮明确不做）：仅留 storage adapter 可切换接口，后端先用现有本地内容寻址 `PredictionArtifactStore`。
- **不改 qe_archive 数仓**（现状符合预期）。
- **不修 manifest sha 完整性 bug**（独立前置 PR，本设计假设其已修复）。
- 不改 `PaperPortfolio` 单 `package_id` 主契约。
- 不改 MiniQMT 执行层 / 路线 A（多Alpha MiniQMT 仍锁后续阶段）。
- 不动回测预测产物的生成（实验内部行为不变，只是包/运行时/数仓都不依赖它）。

## 4. 架构（Architecture）

### 4.1 资产归属总表（设计核心）

| 资产 | 处置 | 删实验/清库后 |
|---|---|---|
| 模型 params.pkl | 🟢 复制进包自有存储（内容寻址，sha 校验） | 包不受影响 |
| 因子代码 .py | 🟢 复制进包自有存储（`FACTOR_CODE` 资产，每因子 sha） | 包不受影响 |
| 回测预测 pred.pkl / combined_prediction.pkl | ⚫ 谁都不存（无价值） | 无关 |
| 回测指标 | 🔵 → qe_archive 数仓（已存） | 数仓留存 |
| 实验 run / 节点 workspace | 🟡 可自由清理 | 包已自包含，不受影响 |

### 4.2 存储适配层（[0]）
- 定义 `PackageAssetStore` adapter 接口：`put(bytes, *, kind, sha256) -> uri` / `get(uri) -> bytes` / `exists(uri)`。
- 后端实现：`LocalContentAddressedBackend`（复用 `model_store/artifact_store.py` 的 `default_store_root` + sha 寻址，落 `rdagent_assets/package_assets/`）。
- 预留 `ObjectStoreBackend`（未实现，接口位留空 + 显式 NotImplemented），未来切 MinIO 不动上层。
- 元数据落 `package_asset` 表（已存在）：`package_id, asset_type(MODEL|FACTOR_CODE), asset_ref(uri), sha256, logical_name, created_at`。

### 4.3 包固化（[1]）—— freeze 从"算 sha"扩展为"真复制 + 算 sha"
- 建包时（`from-qe-experiment`/`from-qe-evolution-loop`/`from-multi-alpha-combine-run` 全路径）：
  1. 从 QE 源（节点 workspace / 因子库 code_text）取 model params.pkl + 每个 `factors/<name>.py` 的字节。
  2. 经 adapter `put` 进包自有存储，得 uri + sha256。
  3. 写 `package_asset` 行（`MODEL` 1 行 + `FACTOR_CODE` N 行）。
  4. manifest 的 `FactorAsset`/`ModelAsset` 升级为携带 `asset_ref` + `sha256`（不再仅按名）。
  5. freeze_manifest 照常算 manifest sha（此时 manifest 含资产 sha，自然内容绑定）。
- **fail-loud**：任一资产取不到（源缺失/sha 不符）→ 建包失败 + 具体 reason_code，**不建半包**。

### 4.4 运行时改读（[2]）
- `live_inference.py` 的 `_resolve_factor_source_dir`/`_materialize_runtime_source_from_node`：当包**已固化**（package_asset 有记录），从包自有存储取 model+factor，**不回 QE 节点、不 `SELECT qe_experiments`**。
- 多Alpha `multi_alpha_live.py:634-655` per-seed 推理同理：从各 component 子包自有资产取。
- 校验：运行时用资产 sha 校验取到的字节，不符即 fail-loud（防存储损坏）。
- 兼容过渡：包**未固化**（存量未回填）时维持现有回节点逻辑——直到 [3] 回填完成。

### 4.5 回填固化（[3]）
- 一次性脚本 + 服务方法：遍历 15 个存量包（先决：sha 完整性已修），对每个包从其 QE 源把 model+factor 复制进包自有存储、写 package_asset。
- 多Alpha 父包：递归固化 N 条腿的 component 子包。
- **必须趁 QE 数据/节点 workspace 还在时做**——回填本身依赖源。
- 幂等：已固化的包跳过；可重跑。
- 回填后核验：每个包能在"模拟删源"下生成新交易日信号（核心验收）。

### 4.6 因子库分级保护（[4]）
- `factor_library_get_usage_summary` 扩展：接入"被策略包引用"查询（查 package_asset 的 FACTOR_CODE + manifest factor_set）。
- **硬删**（`quantevolver.py:1134`）被任何包引用的因子 → 🔴 拒（reason_code，审计可追溯）。
- **deprecate**（`factor_library.py:328-340`，is_available=FALSE）→ ✅ 允许（不阻碍淘汰；存量包已固化自有副本，不受影响）。
- 保护对象是"被包引用的因子"，deprecate 语义是"新实验别再选"，不抹除。

### 4.7 候选退役（[5]）
- 后端：删 `candidate.py` 服务 + `candidates/from-qe-experiment`/`from-qe-loop`/clone/refresh/delete 端点（`strategy_packages.py:426,450,...`）+ `CandidateStrategyPackageService` 相关。退役前确认无运行时依赖。
- 前端：删 `LoopDetailPanel.tsx:148-345`「加入候选策略包」按钮 + `createCandidateFrom*` API wrapper + 候选管理页/路由。
- 替代：loop/experiment 页改为直接「建策略包」（调 `from-qe-experiment`/`from-qe-evolution-loop`，即建即固化即候选态 `ASSET_VALIDATED`）。
- 删 `prediction_ref_uri` 死字段（schema 列 + 写入 `multi_alpha_promotion.py:286-290` + 展示）。DDL 走 gate。

### 4.8 删除无 guard（[6]）
- [3] 回填全完成后：删 QE 实验/loop/combine run / 删包，**不需要 guard**（包已自包含）。
- 过渡期（回填未完成）若需删实验，临时校验"该实验是否有未固化的包引用"，未固化则拦——此 guard 在 [3] 完成后移除。

## 5. 契约（Contracts — API/DB/存储）

- **DB**：`package_asset` 表启用（已存在，可能补列 `logical_name`/`asset_type` 约束）；`strategy_pkg.package` 删 `prediction_ref_uri`/`prediction_ref_sha256` 列（DDL gate）。`FactorAsset`/`ModelAsset` manifest 结构加 `asset_ref`+`sha256`（manifest schema 变更，注意与 sha 完整性兼容）。
- **存储**：`rdagent_assets/package_assets/<sha-prefix>/<sha>`（内容寻址，不可变）。
- **API**：`from-qe-*`/`from-multi-alpha-combine-run` 创建语义不变（对外契约稳），内部增固化步骤；删除 candidate 系列端点（破坏性，需确认无外部调用）；`factor_library` deprecate/delete 加 in-use 校验。
- **no-silent**：固化失败/资产 sha 不符/源缺失 → 具体 reason_code + context，禁兜底。

## 设计验收索引（Design Acceptance Index）

| 设计项 | 标题 | 章节 |
|---|---|---|
| F-001 | 存储适配层（本地内容寻址，预留对象存储） | §4.2 |
| F-002 | 包固化：模型+因子复制进包自有存储 | §4.3 |
| F-003 | 运行时改读包自有资产，不回 QE 节点 | §4.4 |
| F-004 | 回填固化存量 15 包 + 多Alpha component 子包 | §4.5 |
| F-005 | 因子库分级保护（硬删禁/deprecate 可） | §4.6 |
| F-006 | 候选阶段彻底退役 + 单Alpha 直接建包 + 删 prediction_ref 死字段 | §4.7 |
| F-007 | 固化完成后删除无 guard | §4.8 |

## 实施方案（Implementation Plan）

阶段顺序（PR 分批）：
1. **前置（独立 PR，非本设计）**：修 manifest sha 完整性 bug（10/15）→ 全 15 包 integrity PASS。
2. **批 1**：[0] storage adapter + [1] 包固化（新建包即固化）。
3. **批 2**：[2] 运行时改读（含未固化兼容过渡）。
4. **批 3**：[3] 回填固化存量包（趁源在）+ 核验删源不影响。
5. **批 4**：[4] 因子库分级保护（可与批 1-3 并行，独立）。
6. **批 5**：[5] 候选退役 + 删死字段（依赖批 1-3 固化成立）。
7. **批 6**：[6] 移除过渡 guard。

每批：worktree + F2/F0 子设计（如需）+ 实现 + 测试 + Tier2。

## 验证方案（Verification Plan）

- **核心验收**（贯穿）：固化后的包，在"模拟删除其 QE 源"（或指向不存在的 experiment_id）下，仍能生成**新交易日**的 selection 信号（非仅缓存旧日期）。这是 self-contained 的判定口径。
- [1]：新建包后 package_asset 有 MODEL+FACTOR_CODE 行，sha 校验通过；建包失败路径 fail-loud。
- [3]：15 存量包回填后逐个过核心验收；幂等重跑不重复。
- [4]：硬删被引用因子被拒（reason_code）；deprecate 被引用因子放行且存量包不受影响。
- [5]：candidate 端点/按钮/页全无残留（grep 净）；单Alpha 直接建包零回归；prediction_ref 列/写入/展示全删。
- 全程：单Alpha 现有 selection/paper 测试全绿；前端 lint/tsc/build 过；no-silent 断言。

## 设计验收矩阵（Design Acceptance Matrix）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §4.2；复用 `model_store/artifact_store.py` 内容寻址；`package_asset` 表 | adapter put/get/exists + 本地后端落 package_assets/，sha 寻址；对象存储后端留 NotImplemented | done | 对象存储后端本轮 non-goal，approved deviation（用户指定先本地后切） |
| F-002 | §4.3；`manifest.py:31-34` freeze 扩展；`package_asset.py:14` FACTOR_CODE/MODEL | 新建包写 MODEL 1+FACTOR_CODE N 行 + manifest asset_ref/sha；资产缺失 fail-loud | done | - |
| F-003 | §4.4；`live_inference.py:1166-1358,1696-1724`；`multi_alpha_live.py:634-655` | 已固化包运行时从包自有取、不 SELECT qe_experiments；sha 校验；未固化兼容过渡 | done | - |
| F-004 | §4.5；`MultiAlphaProvenanceResolver` 解析腿源 | 15 存量包 + component 子包回填；幂等；删源后核心验收过 | done | 回填须趁 QE 源在，approved deviation（用户确认时序约束） |
| F-005 | §4.6；`factor_library.py:328-340,239-260`；`quantevolver.py:1134` | 硬删被引用因子拒+reason_code；deprecate 放行；usage_summary 接包引用 | done | - |
| F-006 | §4.7；`candidate.py`；`strategy_packages.py:426,450`；`LoopDetailPanel.tsx:148-345`；`prediction_ref`(`repository.py:115`/`components.py:120`/`multi_alpha_promotion.py:286-290`) | candidate 全退役无残留；单Alpha 直接建包零回归；prediction_ref 死字段删净 | done | candidate 端点删除为破坏性变更，approved deviation（用户明确要求彻底退役） |
| F-007 | §4.8 | 回填完成后删实验/包无 guard；过渡期临时 guard 在 [3] 后移除 | done | - |

## Risks（风险与缓解）

| 风险 | 影响 | 缓解 |
|---|---|---|
| 回填前 QE 源已被清 | 部分存量包无法固化（源没了） | 回填**优先排第一**，趁源在；固化前 grep 哪些包的 QE 源已不可达，不可达的显式标记 `unrecoverable` 不静默 |
| manifest sha 完整性 bug 未先修 | 在脏 manifest 上固化 | **硬前置**：sha 修复独立 PR 先合，本设计批 1 才启动 |
| 资产复制使存储膨胀 | 磁盘占用上升 | 内容寻址去重（同 sha 共享）；因子 .py 很小；模型按需；本地盘先用，对象存储后切 |
| 运行时改读引入回归 | 已固化包信号异常 | 未固化包走旧逻辑兼容过渡；固化包 sha 校验取字节；单Alpha 现有测试全绿门 |
| 候选退役破坏外部调用 | 端点删除致调用方 500 | 退役前 grep 确认无前端/MCP/外部调用 candidate 端点；分批，先断前端入口再删后端 |
| 删 prediction_ref 列影响展示 | UI/响应字段缺失 | 该字段运行时零消费，仅展示；删前确认前端不渲染；DDL 有 rollback |
| 因子库 in-use 查询性能 | deprecate/delete 变慢 | usage 查询走索引（package_asset.factor 引用）；deprecate 非高频操作 |

## 发布与回滚（Rollout / Rollback）

**Rollout**（实现阶段，本文档不执行）：
1. 先合 manifest sha 修复（前置）。
2. 批 1-2 合并后只对**新建包**固化 + 运行时优先读包自有（未固化走旧逻辑）。
3. 批 3 回填存量包，逐个核验删源不影响后，才进批 5-6。
4. 候选退役（批 5）与删除无 guard（批 6）必须在批 3 全核验通过后。

**Rollback**：
1. 固化逻辑可 feature-flag 关闭 → 回退到回节点读取（不破坏现有包）。
2. package_asset 写入是增量，回滚不删存量资产（审计保留）。
3. candidate 退役前保留迁移点：若需回滚，candidate 表/端点可从 git 史恢复（但已退役状态下无新建）。
4. prediction_ref 列删除有 DDL rollback（恢复列，数据本就无用）。

## 生产门禁

- `production_ddl_gate`：本设计涉及 package_asset 启用 + 删 prediction_ref 列 + 可能 manifest schema 变更 → 实现阶段进 DDL gate，本文档不交付 DDL。
- `production_frontend_dependency_gate=noop`（候选前端为删除，不加依赖）。
- `production_backend_dependency_gate=noop`。
- 不启/重启服务，不写生产 DB（回填固化为实现阶段授权后单独执行）。

## 附录 A：关键文件锚点

- 固化落点：`backend/services/strategy_package/package_asset.py`；`trading_core_v2_schema.sql:153-161`；`backend/services/model_store/artifact_store.py`。
- freeze：`backend/services/strategy_package/manifest.py:31-34`；`qe_source_resolver.py:387-390`。
- 运行时：`live_inference.py:4-6,868,1166-1358,1696-1724`；`multi_alpha_live.py:634-655`。
- 因子库：`backend/routers/factor_library.py:328-340,239-260`；`backend/routers/quantevolver.py:1134`。
- 候选：`backend/services/strategy_package/candidate.py`；`backend/routers/strategy_packages.py:426,450`；`frontend/src/app/quantevolver/evolution/components/LoopDetailPanel.tsx:148-345`。
- 死字段：`prediction_ref_uri`（`repository.py:115`、`components.py:120`、`multi_alpha_promotion.py:286-290`）。
