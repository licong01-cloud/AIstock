# 详细实施设计 · P2 预测存储基础设施（中心化 MLflow + pred 固化 + 数仓指针）

> **类型**：阶段详细设计（design / impl）· 关键路径起点（P2→P3）
> **日期**：2026-06-16
> **模块**：quantevolver / qe_archive / RDAgent workspace / Qlib recorder（**与 research-assistant 完全隔离**）
> **上游**：`multi_alpha_phased_implementation_plan_20260616.md` P2 + 蓝图附录 A1/A4（数仓-MLflow 边界、复用 PG）
> ⚠️ 所有 file:line 为探查定位，**实施前复核当前代码**。

---

## 0. 修订 R1（2026-06-17）—— 架构调整 + incident 处置（取代 §3 / §4 / §5① 的相关内容）

> 经磁盘/共享/隔离讨论 + 一次 PG 误建 DDL incident，P2 架构调整如下。本节优先级高于下方 §3/§4/§5① 的旧表述。

**A. MLflow-tracking-to-PG 整体延后到 M4。** P2 核心目标（pred/params 持久化 + 可按 run_id 查 → 解锁 P3）**不需要 MLflow-PG**：Qlib recorder 仍**文件态本地**产 pred.pkl 不变，持久化由"后端内嵌 artifact 服务 + PG 指针"承担。MLflow 实验追踪 UI / Model Registry 属 **M4 治理**。**P2 不引入 PG-MLflow，不再连 MLflow client 到生产库默认 schema。**

**B. artifact 持久化 = AIstock 后端内嵌，不另起服务、不用 MinIO、不用共享挂载、不用 E: HDD（取代 §3.1/3.2/3.3 的 central artifact-root / SMB / Option 1·2 / E: 方案）：**
- 后端新增 FastAPI artifact **上传/下载路由** + **快盘 store（F: 或 SSD，绝不用 E: 机械盘）**；run-id/内容寻址。
- 节点 run 完成、workspace 清理前，经 **HTTP 直推** pred.pkl/params.pkl 到后端（网络直传，**无共享挂载、无 HDD 中转**）。
- PG 指针（`qe_archive.run_source.mlflow_artifact_uri` 或新 `prediction_index` 表）记 run_id → store 位置 + 格式/行数/股票数/日期范围。
- MLflow 原生 artifact_location 保持节点本地（**不作持久化路径**）。
- **多节点 = 统一网络推送**（替代原 Option 1 SMB / Option 2 pull）。

**C. incident 处置记录（2026-06-17 已完成）：** Task0 误触发 MlflowClient(PG) 把 **39 张 MLflow 3.8.1 模型表**建到了 PG **public**（与 AIstock + research-assistant 共用的 schema）。已**单事务 DROP 这 39 张模型表**（无 CASCADE、未动业务表；public 132→93；assistant=34/research=6/aistock=19/qe=24 等业务表数量不变）。`public.alembic_version='1bd49d398cd23'`（MLflow 的）**保留未动**，待确认"RA/任何模块是否用 alembic"后再决定是否清除（属 RA 窗口协调项，非阻塞）。**教训：MLflow-PG 验证只能连 scratch 库或预先 pin schema，绝不连生产默认 schema。** M4 重建 MLflow 用**专用 DB 用户 + 专用 mlflow schema**。

**D. 工程任务调整：** 原 §5 改造点①（MLflow URI 中心化到 PG）→ **移 M4**。P2 改造点 = 后端 artifact 服务(T1) + 节点推送 hook(T2) + PG 指针(T3) + model_store(T5) + 只读 MCP(T6) + 嵌入 UI(T7)；§5②③④、§6-§8 中与 artifact/指针/model_store/MCP/UI 相关部分仍适用（去掉其中 MLflow-PG/schema 内容）。

---

## 1. 背景与目标

**现状**：Qlib 回测产 `pred.pkl`（全截面预测分数）+ `params.pkl`（模型权重），写在每个 workspace 的本地 `mlruns/`，**workspace 完成后被 `shutil.rmtree` 清掉**（即弃）；qe_archive 的 mlflow 指针列**已存在但全 null**。

**目标**：把 MLflow tracking 中心化到**现有 PostgreSQL（独立 `mlflow` schema）**+ **共享 artifact store**；让 `pred.pkl`/`params.pkl` **持久化**（不随 workspace 清理丢失）；qe_archive 指针列**接线填充**；建 `services/model_store` 统一读写；嵌入式 UI；全程 MCP-first。**不起独立 mlflow server**（库模式 + PG backend）。

**解锁下游（关键 linkage）**：
- pred.pkl 固化 → **P3 多 Alpha 离线组合回测**（按 run_id 拉各腿预测分数融合）的唯一前提。
- 同时解锁**评估口径 Tier-2**（precision@K/NDCG@K 全截面，需 pred.pkl）。
- params.pkl 固化 + model_cache → **安全删源 workspace**（解 cleanup_plan / 磁盘危机的 Tier-2 清理）。

---

## 2. 现状（code-grounded，复核）

| 事实 | 位置 |
|------|------|
| `MLFLOW_TRACKING_URI` **单点设置**=`file://<workspace>/mlruns` | `workspace.py:85` + `qrun_limit_minute.py:202-207`（`exp_manager["kwargs"]["uri"]`，Qlib MLflowExpManager 接受任意 uri） |
| pred.pkl/label/params 写入 | `qrun_limit_minute.py:235-237/356/394/466`（`recorder.save_objects`） |
| workspace + mlruns **清理**（持久化窗口边界）| `qe_evolution_api.py:733-745`（`cleanup_task_workspace` → `shutil.rmtree`），**无 post-run hook** |
| 指针列**已存在、未填** | `init_qe_archive_schema.py:101-102`（run_source.mlflow_tracking_uri/artifact_uri + recorder_id/recorder_experiment_id）；`models.py:132-133` |
| 入仓抽取（未抽 mlflow_*）| `payload_extractor.py`（outbox_event → archive worker → run_source） |
| PG 连接**可复用** | `pg_pool.get_conn()`；qe_archive 已用同实例 → 加 `mlflow` schema 即可 |
| 模型权重 lazy copy 到 cache | `model_asset_resolver.py:29/82-92`（`rdagent_assets/model_cache/execution`，`copy_missing` 默认 True 但按需触发） |
| AIstock **无任何 MLflow UI/读路由** | grep routers 无 → P2 加首个嵌入式 |

---

## 3. 架构决策（核心）

### 3.1 决策 A：固化靠"中心 artifact-root"而非 copy hook（推荐）
把 MLflow **tracking backend = PG**、**artifact-root = 中心共享位置**后，Qlib recorder 写的 pred.pkl/params **直接落中心，不进 ephemeral workspace** → `cleanup_task_workspace` 只删瞬态计算文件，**pred 天生持久，无需易碎的 copy hook**。这是最优解，且 URI 单点可改。
- 备选 B（不推荐）：保留 file mlruns + 在清理前注入 copy hook（时序/部分失败脆弱）。仅当中心 artifact-root 无法落地时退而求其。

### 3.2 决策 B：多节点 artifact 共享（关键基础设施选择）
QE 跑在 **WSL(wsl2-5080) + 远端(rdagent-node1)** 两节点。tracking(PG 元数据)天然共享(两节点连同一 PG)；但 **artifact(大二进制)需两节点都能写的中心位置**：
- **Option 1（中心 artifact store，推荐）**：共享挂载(SMB/NFS)或对象存储(MinIO/S3，MLflow 支持 `s3://` artifact-root)。两节点都写中心 → P3 按 run_id 一处拉取，最干净。
- **Option 2（节点本地 + 指针 + 按需拉，低基建兜底）**：artifact 留产出节点的**持久目录(非被清的 workspace)**，PG 存 node+path 指针，AIstock 经 QE-node-API 按需拉 pred.pkl(复用现有 node 服模型的机制)。无需新基建，但环节多。
- **推荐 Option 1**；若共享 FS/对象存储未就绪，先 Option 2 过渡。

### 3.3 决策 C：artifact-root 位置 → 解耦磁盘危机
**pred.pkl/params 是冷产物**（写一次、P3 研究时按需读），**不在训练热路径** → artifact store **可放 E: 机械盘(15T，6.9T 空闲)**，与"WSL 95% / qe_workspace 热数据"危机**解耦**。即 P2 的 artifact 存储**不依赖暂停的 X: 迁移决策**（X: 留给热数据/workspace）。远端节点经 SMB 挂 E: 或走 Option 2。

> 与"无独立 server"一致：PG backend + 文件/对象 artifact-root 走 **MlflowClient 库模式**，**不需要 `mlflow server` 进程**。

---

## 4. 数据流（目标态）

```
QE loop(WSL/远端) → Qlib recorder.save_objects(pred.pkl/params.pkl)
   │  MLFLOW_TRACKING_URI=postgresql://…?schema=mlflow ; artifact-root=中心位置
   ▼
中心 MLflow: PG(mlflow schema, run/metric/params/tags 元数据) + artifact-root(E:/共享, pred.pkl/params)
   │  (workspace 清理只删瞬态, pred 已在中心)
   ├─ run 完成 → outbox_event → archive worker → payload_extractor 填 run_source.mlflow_*指针 + recorder_id
   ▼
qe_archive(指标 SoT + 指针)  ──按 run_id──►  services/model_store.pull(pred/params)
   │                                              ▲
   └─ MCP(prediction_store_*/mlflow_run_*) + UI(嵌入 run 对比/pred 浏览/模型版本)
                                                  │
                              P3 组合回测 / 评估 Tier-2 / live_inference 模型来源(M4)
```

---

## 5. 改造点（Codex 工程项）

1. **改造点① 中心化 URI**（单点）：`workspace.py:85` + `qrun_limit_minute.py:202-207` 改为读 `MLFLOW_BACKEND_STORE_URI`(PG, `?options=-c search_path=mlflow`) + `MLFLOW_DEFAULT_ARTIFACT_ROOT`(中心位置)；缺省回退当前 file 行为(灰度)。两节点 env 注入。
2. **改造点② 初始化 mlflow schema**：在现有 PG 加 `mlflow` schema（MLflow 首次连接自建表，或预置迁移）。**去重红线**：MLflow 的 metrics 表**不与 qe_archive 互同步**，仅诊断；qe_archive 仍是指标 SoT（蓝图 A1）。
3. **改造点③ 指针接线**：`payload_extractor.py` 抽取并填 `run_source.mlflow_tracking_uri/artifact_uri/recorder_id/recorder_experiment_id`（从 run 完成 payload / recorder 元数据）。outbox worker 入仓时落库。
4. **改造点④ 模型权重固化**：run 完成→清理前，确保 `params.pkl` 已进中心 artifact-root（决策A 下天生满足）+ 可选 copy 到 `rdagent_assets/model_cache`（让策略包可独立于源 workspace）。
5. **改造点⑤ services/model_store 模块**：封装 `write_pointer / pull_pred(run_id) / pull_params(run_id) / register_model_version`，作为 P3/评估/live_inference 的统一出口。库模式 MlflowClient + 指针 join qe_archive。
6. **改造点⑥ MCP（MCP-first）**：见 §6。
7. **改造点⑦ UI（嵌入）**：见 §7。

---

## 6. MCP 面（每能力必有 MCP 工具）

- **只读**：`prediction_store_get_pointer(run_id|experiment_id)`（返回 artifact_uri/schema 指针）、`prediction_store_pull_pred(run_id, head?)`（拉 pred.pkl 摘要/路径，全量给 P3 用）、`mlflow_run_list(filter)` / `mlflow_run_compare(run_ids)`、`model_store_health`。
- **写/确认门控**：`prediction_store_backfill_confirmed`（存量 run 若 mlruns 尚存则迁中心 + 填指针；mlruns 已清的标记不可回填）。
- 全部走 aistock-qe / 新 model_store MCP server，**禁止只能 UI 的能力**。

## 7. UI 面（观测性）

- 嵌入 AIstock 前端新增 **MLflow 视图**（新 FastAPI 路由 `routers/mlflow_ui.py` 读 MlflowClient + qe_archive 指针）：实验/run 列表 + 对比、pred 产物浏览/下载、模型版本与 stage、artifact-root 健康/占用、指针回填进度。**不引第二套 UI、不开 mlflow 自带 UI**。

---

## 8. DB / schema 变更

- PG 加 `mlflow` schema（MLflow 自建其 experiments/runs/metrics/params/tags 表）。
- qe_archive 指针列**已存在**（无需建列），只接线填充。
- 可选新增 `qe_archive.prediction_index`（run_id → artifact_uri/格式/行数/股票数/日期范围，便于 P3 快速发现，不存内容）。

## 9. 磁盘前置（解耦结论）

- artifact-root **可放 E: 机械盘**（冷产物，6.9T 空闲）→ **不阻塞于暂停的 X: 迁移**。
- 多节点：远端经 SMB 挂 E:（Option 1）或走 Option 2 节点本地+拉。
- ⚠️ 仍需用户定 artifact-root 落点 + 多节点共享方式（Option 1 vs 2）——这是 P2 唯一的基建决策。

## 10. 验收标准

- 新 run 的 pred.pkl/params **落中心 artifact-root**，workspace 清理后仍可经 MCP `prediction_store_pull_pred(run_id)` 拉到。
- qe_archive `run_source.mlflow_*` 指针被填充（抽样非 null）。
- PG `mlflow` schema 存在、metrics 不与 qe_archive 双维护。
- AIstock UI 可浏览某 run 的 pred 产物 + 模型版本。
- 灰度：env 未配时回退旧 file 行为不报错。
- **P3 联调样本**：能从中心拉某腿 pred.pkl 进内存做一次截面读取（证明接口通）。

## 11. 风险与门控

- **不起 mlflow server**（库模式 + PG）；改 env 后**提醒用户重启相关进程，不自启服务**。
- **多节点 artifact 写权限**是主风险：Option 1 需共享 FS/对象存储就绪;否则 Option 2。开工先验证两节点能否写中心位置。
- 存量回填条件：mlruns 多已随 workspace 清理→**多数存量不可回填**，以新 run 前向固化为主（与 cleanup_plan 一致）。
- 禁 silent error：pred 写中心失败必须显式报错 + run 标记，不静默退回 file。
- additive：保留 file 回退路径作灰度。
- **隔离**：全程不碰 research-assistant。

## 12. 任务拆分（me / Codex）

| 任务 | owner |
|------|-------|
| artifact-root 落点 + 多节点共享方式(Option 1/2) 决策 | **我 + 用户**（基建决策） |
| 改造点①②③④（URI 中心化 / mlflow schema / 指针接线 / 权重固化） | **Codex** |
| services/model_store 模块 + MCP 工具 | **Codex** |
| UI 嵌入(mlflow_ui 路由 + 前端视图) | **Codex** |
| 两节点写中心 artifact 的连通性验证（开工硬前置） | Codex 先验 → 我/用户定基建 |
| 验收：P3 拉 pred 联调样本 | **我** |

## 13. 测试

- 单测：URI 构造 / 指针抽取 / model_store pull。
- 集成：跑一个小 loop，验证 pred 落中心 + workspace 清理后仍可拉。
- MCP 冒烟：prediction_store_* / mlflow_run_* / 回填 confirm。
- 去重核查：MLflow metrics 不进 qe_archive 报表。
- 灰度回退：env 未配时旧 file 行为不变。

---

*落于 worktree `docs/ma1-multi-alpha-sourcing-20260615`。P2 解锁 P3 组合回测 + 评估 Tier-2；artifact 存储解耦磁盘危机(可放 E:)；唯一基建决策=artifact-root 落点+多节点共享方式。与 research-assistant 完全隔离。实施前复核 file:line。*
