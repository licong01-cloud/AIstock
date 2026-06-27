# 多 Alpha 组合回测(combine-backtest)远端节点派发能力设计

- 日期:2026-06-27
- 文档类型:功能设计方案(**F1 标准单模块功能**)+ 现状可行性分析
- 落位:`docs/architecture/`（[DOC-LOCATION-001]:架构设计 / 顶层方案 / 实施方案）
- 模块:QuantEvolver / Multi-Alpha / Compute Dispatch
- worktree:`F:\Dev\AIstock_worktrees\docs-multi-alpha-flow-decision-20260627`
- branch:`docs/multi-alpha-flow-decision-20260627`(从 origin/main)
- 状态:🔶 F1 设计待评审(代码已考证;实现需用户批准后另起开发 worktree,跨 AIstock + RDAgent 两仓库)

> 本文为只读代码考证 + 设计草案,不改任何产品代码 / DB / 运行时。按 [DESIGN-MAIN-001] 验证通过后再 push origin/main。

---

## 0. TL;DR

**用户诉求**:多 Alpha 组合回测(`macb_`)不训练模型、只跑 `qrun --pred-backtest`,远端机(`192.168.50.215` / `rdagent-node1`,内存更大)空闲时完全能并行 4 个任务。希望让 combine-backtest 像 QE CPU loop 一样**派发到远端节点并行**,提升大批量(窗口×topk×scheme×roster)扫描效率。

**核心结论(经代码考证,已含远端执行端考证)**:
1. **QE 已有成熟的远端派发架构**,且是**统一 HTTP 抽象**——本地和远端走同一条代码路径,仅 `api_base_url` 不同。
2. **combine-backtest 完全没接这套**,它用最原始的 `subprocess.run(["wsl","bash","-lc",...])` 本地直跑,绑死在后端进程所在机器的本地 WSL。
3. **远端执行端已经任务无关**(关键考证,见 §2.5):RDAgent 9000 服务的 loop 端点 `POST /api/v1/qe_workspace/tasks/{id}/loops` 本质是**通用 `wsl_command` 执行器**——`if wsl_command: final_cmd = wsl_command` 直接执行 AIstock 传入的任意 WSL 命令,对"训练 vs 纯回测"无任何假设。combine 的命令(`wsl bash -lc "... qrun_limit_minute.py --pred-backtest combined_prediction.pkl && read_exp_res.py"`)**形态完全兼容**该端点。
4. **远端执行端零改动**:不需要在 `192.168.50.215` 上新增 pred-backtest 接收端——loop 端点已通用执行 wsl_command。
5. **但远端"数据传输端"有一个真实缺口(长期方案核心,见 §3.5)**:combine 的核心输入 `combined_factors_df.parquet`(908MB)无现成通道上远端——`qe_file_sync` 限 10MB,factor-cache 流式通道语义是"单个因子文件"非"任意 workspace 文件"。长期解 = 新增**内容寻址的 Workspace Artifact Store(WAS)**:大文件按 sha256 一次上传、跨 run 复用、自动去重/失效。关键支撑事实(做 1 §6.1):这份 parquet **跨 run 不变**,故稳态下每 run 仅推 <10MB 的 `combined_prediction.pkl`,远端并行 4 路高度划算。
6. **工作量:中(本机为主 + 远端加 WAS 端点)**。执行派发是接线;真正的工程量在 WAS(大文件同步)+ 容量守卫统一 + 路径解耦。WAS 是通用能力,不绑定 combine,值得作为长期基础设施投入。

---

## 1. 背景(Background)与现状考证:两套执行路径

### 1.1 QE 自定义演进 loop —— 统一 HTTP 派发(已支持远端)

证据链(file:line):
- 执行器 `backend/services/quantevolver/executors/backtest.py:202` 把 loop 交给
  `self.client.create_and_run_loop(task_id, loop_index, rdagent_config, experiment_files, wsl_command, ...)`。
- `self.client` = `QEWorkspaceClient`(`executors/backtest.py:37-44` 注入)。
- `QEWorkspaceClient.for_node(node_id)`(`qe_workspace_client.py:47-57`):
  - `SELECT api_base_url FROM infra.compute_nodes WHERE node_id = %s`(L53)
  - 本地默认 `http://localhost:9000/api/v1/qe_workspace`(L35),远端为该节点注册的 `api_base_url`。
- `create_and_run_loop`(`qe_workspace_client.py:70-107`):**永远走 HTTP POST** `{base_url}/tasks/{task_id}/loops`,body 含 `wsl_command`、`config`、`callback_url`。
  - **本地和远端是同一条 HTTP 路径**,唯一差异是 base_url(localhost vs 远端 IP)。
- 节点注册表 = DB 表 `infra.compute_nodes`(`dispatch_service.py:248-351`:`get_node` / `update_node` / `get_node_client` / `probe_node`),字段含 `node_id, display_name, api_base_url, gpu_model, gpu_vram_mb, ...`。
- 远端节点系统指标/健康:`backend/infra/compute_node_client.py`(httpx)`get_system_metrics` / `probe_health` / `create_task` / `get_task_progress` / `get_scheduler_results`(`compute_node_client.py:36-85`)。
- 远端数据预同步:`backend/services/quantevolver/factor_cache_remote_sync_service.py`、`qe_file_sync_client.py`(factor cache / 工件同步到远端机)。

**架构小结**:本机后端是 orchestrator;每个计算节点(本地 WSL `localhost:9000` / 远端 `192.168.50.215`)各跑一个 QE workspace 服务,通过 HTTP 接 loop;数据靠 sync 服务预先铺到远端;loop 完成由远端 `callback_url` 主动回调或本机轮询。**node_id 是逻辑名,真实 host/IP 在 `infra.compute_nodes.api_base_url`。**

### 1.2 combine-backtest —— 本地 subprocess 直跑(无远端能力)

证据链(file:line):
- 执行原语 `ShellPredBacktestExecutor.execute_pred_backtest`(`multi_alpha/combine_backtest.py:211-282`):
  - command 来自 payload(Run#4 实测 = `wsl bash -lc "... conda activate ... python qrun_limit_minute.py conf.yaml --pred-backtest combined_prediction.pkl && python read_exp_res.py"`)。
  - 由 `run_command`(`combine_backtest.py:579-621`)= **`subprocess.run(...)`**(L594)起**本地子进程**,`cwd=workspace`。
- **全文件无 ssh / paramiko / 远端 host / dispatch / compute_node / QEWorkspaceClient 任何引用**(已 grep 确认 0 命中)。
- `node_id`(payload 内 `node_parallelism: {"wsl2-5080": 2}`)**仅**被容量守卫 `DatabaseQENodeCapacityChecker.ensure_slot_available`(`combine_backtest.py:154-197`)拿去查
  `SELECT COUNT(*) FROM qe_evolution_loops WHERE node_id=%s AND status IN ('running','processing')`(L164-171),作并发记账;**不改变命令落地在哪台机**。
- workspace 已隔离:`self._workspace_root / run_id / name`(`combine_backtest.py:1594`),root 来自 `AISTOCK_MULTI_ALPHA_*` 环境变量(L1052)。
- run 内部已并发:`ThreadPoolExecutor`,`max_workers = min(node_parallelism_limit, len(tasks))`(L1365),一个 run 含 1 组合 + N 个 LOO 子任务并发;`node_parallelism` 校验范围 **1–4**(L2011)。

**架构小结**:combine-backtest 的"节点"是个假概念——`node_id` 只记账,命令永远在**后端进程本机 + 本机 WSL**跑。所谓"串行约束"的根因是:同一本地节点容量有限(撞守卫)+ 多 run 抢同机 GPU/factor-cache I/O。

### 1.5 远端执行端考证(做 1 结论:远端已能接,无需改远端)

谁在远端机执行 QE loop?——**不是 AIstock 后端**,而是独立的 **RDAgent 9000 服务**(每个计算节点各跑一个:本机 `localhost:9000`、远端 `192.168.50.215:9000`)。考证链:
- AIstock 侧 `QEWorkspaceClient` 默认 `base_url = http://localhost:9000/api/v1/qe_workspace`(`qe_workspace_client.py:35`);`for_node` 换成节点的 `api_base_url`(`:47-57`)。
- 该 9000 服务在 RDAgent 仓库:`F:/Dev/RD-Agent-main/rdagent/app/results_api_server.py:78` 挂载 `qe_evolution_router`(prefix `/api/v1/qe_workspace`),另 `:52` mount `/scheduler`。
- 受理端点:`rdagent/app/api_endpoints/qe_evolution_api.py:389` `POST /tasks/{task_id}/loops`,请求体含 `wsl_command`(`:65`)。
- **执行逻辑任务无关**:`_run_qlib_backtest`(`qe_evolution_api.py:86`)在 `:170-173` —— **`if wsl_command: final_cmd = wsl_command`**,**直接执行 AIstock 传入的任意 WSL 命令**,不解析、不假设是否训练模型;仅当没传 wsl_command 时才用默认 `qrun conf.yaml` 链(`:174-180`)。
- 跨节点能力已内建:`experiment_files` 支持 b64 二进制注入(`:114-127`)、`model_source.cross_node` 从 tar 解压(`:139-148`)、`callback_url` 完成回调(`:86` 签名)。

**决定性推论**:combine-backtest 的命令本就是一条自包含 WSL 命令(`wsl bash -lc "... qrun_limit_minute.py --pred-backtest combined_prediction.pkl && read_exp_res.py"`),把它当作 `wsl_command` POST 给远端 9000 的 loop 端点,远端会**原样执行**——和它执行一个训练 loop 没有区别。**所以远端机零改动,无需新增 pred-backtest 任务类型。** 这正坐实了用户"远端机原来就能直接执行 QE 自定义演进、与 WSL 环境无区别"的判断。

---

## 2. 差距定位(为什么不能"改个 node_id 就远端并行")

| 维度 | QE loop | combine-backtest | 差距 |
|---|---|---|---|
| 派发传输 | HTTP POST 到节点 workspace 服务 | 本地 `subprocess.run` | combine 无远端传输 |
| node→host 解析 | `infra.compute_nodes.api_base_url` | 不解析(node_id 仅记账) | combine 不读注册表 |
| 远端执行端 | 远端 RDAgent 9000 loop 端点已通用执行 wsl_command | 无(本地 subprocess) | **远端零改动**;combine 命令直接当 wsl_command 投递即可 |
| 数据就绪 | factor cache / 工件 sync 服务预铺 | 假设本地 factor cache 路径 | 远端需有 pred.pkl + factor cache + conf 模板 |
| 结果回传 | callback_url / 轮询 + DB 写 | 本地读 `qlib_results_enhanced.json` | 远端需回传 enhanced metrics |
| 任务协议 | RDAgent loop(含模型训练语义) | 纯 pred-backtest(无训练) | 协议不同,不能直接套 loop 端点 |

**关键洞察**:正因为 combine 不训练模型,它**比 QE loop 更简单**(无 CoSTEER/模型代码生成/mlruns 写回),远端执行只需:① 拿到 `combined_prediction.pkl` + conf 模板 + factor cache;② 跑 `qrun --pred-backtest`;③ 回传 `qlib_results_enhanced.json`。这正是用户判断"远端完全能并行"的依据——**计算上成立,只是当前代码没接派发**。

---

## 3. 设计方案:复用 QE 派发骨架 + 新增 combine 远端执行器

### 3.1 复用(不重造)
- **节点注册**:直接用 `infra.compute_nodes`(`192.168.50.215` 已注册即可,或新增一条),`api_base_url` 指向远端 workspace 服务。
- **传输层**:复用 `QEWorkspaceClient.for_node(node_id)` 的 HTTP 模式 / 或 `ComputeNodeClient` 的 `/scheduler/tasks` 模式(二选一,见 §3.4 决策点)。
- **数据同步**:复用 `factor_cache_remote_sync_service` / `qe_file_sync_client` 把 factor cache + pred.pkl + runtime_template 铺到远端。

### 3.2 新增(combine 专属适配)
- **远端 pred-backtest 执行器**:在 `ShellPredBacktestExecutor` 旁新增 `RemotePredBacktestExecutor`,按 node 的 `api_base_url` 把 pred-backtest 任务 POST 给远端,轮询/回调取 `qlib_results_enhanced.json`。
- **远端接收端点**(若远端 workspace 服务尚不支持纯 pred-backtest 任务类型):在远端节点服务加一个 `/pred-backtest` 任务类型,执行 `qrun --pred-backtest` + 回传 enhanced metrics。
- **执行器选择**:combine run orchestrator 按 `node_id` 解析"本地 vs 远端",本地走 `ShellPredBacktestExecutor`(现状),远端走 `RemotePredBacktestExecutor`(新增)。统一接口,不破坏现有本地路径(参考 QE 的"同一 HTTP 抽象"思路,但 combine 保留本地 subprocess 兼容)。

### 3.3 容量守卫修正
- 现状守卫只查 `qe_evolution_loops`(QE loop 表),combine 自身的并发不在该表 → 多 combine run 互相不可见,是撞车根因之一。
- 远端化后需让守卫**按 (node_id) 统一计入 QE loop + combine run 双来源**,或给 combine 自己的 per-node 预约表,避免远端节点也被超订。

### 3.4 待决策点(实现前需用户/评审确认)
1. **传输复用哪条**:QE workspace HTTP(`/api/v1/qe_workspace/tasks/{id}/loops`)vs compute_node `/scheduler/tasks`。**倾向前者**——已考证它通用执行 `wsl_command`、支持 `experiment_files`/`callback_url`,combine 直接复用 `QEWorkspaceClient.for_node()` 即可,最省事。
2. **远端是否已有 pred-backtest 接收端**:✅ **已考证为是**(§1.5)——远端 loop 端点任务无关,无需在远端加任何东西。本决策点已关闭。
3. **数据同步成本**:combine 命令含本机绝对路径 `FACTOR_CACHE_DIR=/mnt/f/Dev/AIstock/rdagent_assets/factor_values` + `combined_prediction.pkl`。远端需:① 对应 factor cache 已同步(复用 `factor_cache_remote_sync_service`);② `combined_prediction.pkl` 随 `experiment_files` 推送或预同步;③ runtime_template 就绪。**这是落地的真正工作量所在**,需评估 pred.pkl 大小 + factor cache 远端常驻情况。
4. **并行度上限**:用户提 4,与守卫 `node_parallelism` 1–4 校验一致;远端内存更大可支撑。需确认远端 factor cache I/O 不成新瓶颈。
5. **路径解耦**:combine 命令把 factor cache 路径硬编码在 command string 里。远端化需把路径参数化(按节点解析),否则远端拿到本机路径会跑错/跑空 → 必须 fail-loud(F-004)。

---

## 3.5 [长期方案核心] Workspace 数据同步架构(解决 908MB parquet 上远端)

> 用户硬要求:远端运行是**长期必须实现的目标**,需给出**长期可持续方案**,不接受带外 scp / 手工搬运这类一次性 workaround。本节是本设计的关键交付。

### 3.5.1 约束(已代码考证,非推测)
- `combined_factors_df.parquet` = **908MB**,是 combine workspace 的核心输入(`conf.yaml:49-51` `StaticDataLoader`)。
- 现有两条远端上传通道**都不直接适配**:
  - `qe_file_sync` `/experiments/{id}/files`:`MAX_FILE_SIZE = 10MB`(`qe_file_sync_api.py:55`)→ 只能传 `combined_prediction.pkl`(小),**传不了 parquet**。
  - `factor-cache /sync`:`_atomic_write_request_stream` 流式无大小限制(`factor_cache_api.py:134-157`),但语义是"按 `factor_name` 上传**单个因子文件**到 factor-cache 目录",**不是任意 workspace 文件**。
- loop 端点 `experiment_files` 走 b64 注入(`qe_evolution_api.py:114-127`),对 908MB 不现实(base64 膨胀 33% + 内存)。
- **结论**:当前无现成通道能把 workspace 级大文件干净送到远端。这是远端化唯一的真实工程缺口。

### 3.5.2 数据分层(决定同步策略的关键事实)
combine workspace 的数据按"变化频率"分三层:
| 层 | 文件 | 大小 | 变化频率 | 同步策略 |
|---|---|---|---|---|
| L1 行情基座 | qlib_bin / qlib_minute_bin | 数十 GB | 几乎不变(随数据更新) | **远端常驻**,已就绪(`/home/lc999/data/...`),复用现有数据更新流程 |
| L2 组合因子矩阵 | `combined_factors_df.parquet` | ~908MB | **跨 run 不变**(做 1 §6.1 证实:与 roster/窗口/topk/scheme 无关) | **内容寻址预同步 + 远端缓存复用**(见 3.5.3) |
| L3 run 增量 | `combined_prediction.pkl` + conf override + 小模板文件 | < 10MB | 每 run 变 | 走现有 `qe_file_sync` 10MB 通道,每 run 推 |

**核心洞察**:真正的大文件(L2)**跨 run 不变**,所以不需要"每 run 推 1GB"。长期方案 = 给 L2 建一个**内容寻址的 workspace artifact 缓存**,一次上传、多 run 复用、按需失效。

### 3.5.3 长期方案:内容寻址的 Workspace Artifact Store(WAS)
在远端 RDAgent 服务新增一个**通用、内容寻址、流式**的大文件 artifact 通道(参考 factor-cache 流式实现,但语义是"workspace 任意大文件",非"因子"):

**远端新增端点(RDAgent 9000)**:
- `HEAD/GET /api/v1/qe_workspace/artifacts/{sha256}` → 查询某 artifact 是否已在远端缓存(返回 size/exists)。
- `POST /api/v1/qe_workspace/artifacts/{sha256}`(流式,无 10MB 限制,复用 `_atomic_write_request_stream` 模式)→ 上传大文件,落到远端 `artifact_store/{sha256}`,服务端校验 sha256 一致性(fail-loud)。
- artifact 以 **sha256 内容寻址**:相同 parquet → 相同 hash → 远端已存在则**跳过上传**(幂等、自动去重、自动复用)。

**本机新增(AIstock)**:
- `WorkspaceArtifactSyncClient`:派发前对 L2 大文件算 sha256 → `HEAD` 远端 → 不存在才流式 `POST`(已存在直接复用,零传输)。
- 远端 workspace 装配:用 symlink/hardlink 把 `artifact_store/{sha256}` 链接成 workspace 内的 `combined_factors_df.parquet`(零拷贝),或在 wsl_command 里指向 artifact 路径。

**为什么这是长期方案而非 workaround**:
- **幂等 + 去重**:同一 parquet 只上传一次,后续所有 run(任意窗口/topk/roster)复用,无重复传输。
- **通用**:不绑定 combine——任何需要把大 workspace 文件送远端的功能(未来其它回测/批量任务)都能用。
- **可校验**:sha256 内容寻址天然防数据损坏 / 版本错配(parquet 更新 → hash 变 → 自动重传)。
- **失效自然**:parquet 内容变(因子集更新)→ hash 变 → 自动重新上传,无需手工清缓存。
- **复用已有流式原语**:远端 `_atomic_write_request_stream` 已验证可流式写大文件,工作量集中在"通用化 + 内容寻址封装",非从零造传输。

### 3.5.4 端到端数据流(长期稳态)
```
首次 / parquet 变更时:
  AIstock 算 sha256(parquet) → HEAD 远端 artifacts/{sha}
    → 不存在 → 流式 POST 上传(一次, ~908MB)
    → 已存在 → 跳过(零传输)
每个 combine run:
  AIstock 生成 combined_prediction.pkl(小) → qe_file_sync 推送(<10MB)
  + conf override(窗口/topk/scheme,文本)
  → POST loop 端点(wsl_command 指向: 远端 artifact parquet + 推送的 pred.pkl + 远端 qlib_bin)
  → 远端执行 qrun --pred-backtest → enhanced metrics 回传(callback/轮询)
```
稳态下每 run 网络成本 ≈ pred.pkl(<10MB) + 文本,**远端并行 4 路高度划算**。

### 3.5.5 分阶段落地(渐进,每阶段可独立验证)
- **Phase 1(打通端到端,最小)**:WAS 上传/HEAD 端点 + 本机 sync client + 远端执行器,先支持**单 run 远端**。验收 = 一次真实远端 run 数值与本地一致。
- **Phase 2(并行 + 守卫)**:combine orchestrator 按 node 分流 + 容量守卫跨来源统一(§3.3),支持远端 4 路并行。
- **Phase 3(运维硬化)**:artifact GC / TTL、失败重传、sha 校验失败 fail-loud、并行度自适应。

> 注:在 WAS(Phase 1)就绪前,**无法完成真·端到端 smoke**(908MB 无通道上远端)。这把"做 1 的端到端实测"从"smoke 临时操作"正确地推入"F1 实现 Phase 1"——即长期方案的第一个可验证里程碑,而非带外搬运。

---

## 4. 设计验收索引(F-xxx,实现阶段引用)

| ID | 设计条目 | 关键约束 |
|---|---|---|
| F-001 | node_id→host 解析复用 `infra.compute_nodes` | combine orchestrator 读注册表,不再仅记账 |
| F-002 | RemotePredBacktestExecutor 新增 | 与本地 ShellPredBacktestExecutor 同接口;本地路径零回归 |
| F-003 | 复用远端 RDAgent loop 端点(执行端零改动) | combine wsl_command 投 `POST /api/v1/qe_workspace/tasks/{id}/loops`;远端**执行端**原样执行 + 回传 enhanced metrics(注:大文件传输端需新增 WAS,见 F-009) |
| F-004 | 数据同步:L1 常驻 / L2 经 WAS / L3 经 10MB 通道 | 三层就绪校验(§3.5.2),缺失 fail-loud;路径按节点解析 |
| F-005 | 容量守卫跨来源统一 | 远端节点不被 QE loop + combine 双重超订 |
| F-006 | 本地路径零回归 | node_id=本地时仍走 subprocess,行为字节级不变 |
| F-007 | 结果回传一致性 | 远端 enhanced metrics 与本地口径完全一致(数值核对) |
| F-008 | 失败显式不静默 | 远端失败上报 node/stage/exit_code/stderr_tail,禁兜底 |
| F-009 | WAS 远端端点(内容寻址流式) | `HEAD/POST /api/v1/qe_workspace/artifacts/{sha256}`;无 10MB 限制;服务端 sha256 校验 fail-loud(§3.5.3) |
| F-010 | WAS 本机 sync client | 派发前 sha256 → HEAD → 不存在才上传;幂等去重;parquet 变更自动重传 |
| F-011 | WAS 通用性 + 零拷贝装配 | 不绑定 combine;远端 symlink/hardlink artifact 入 workspace,不重复落盘 |

---

## 4A. 范围(Scope)

- **In scope**:让 multi-alpha combine-backtest 的 pred-backtest 任务可派发到远端计算节点(`rdagent-node1`/192.168.50.215)执行并行多 run;新增 Workspace Artifact Store(WAS)解决大文件(L2 parquet)远端同步;本机执行器按 node 分流(本地 subprocess / 远端 HTTP);容量守卫跨来源统一;结果回传一致性。
- **Out of scope**:见 §6(并发去重锁、Paper v2 直连、统一实验联合视图)。
- **不改**:`qe_evolution_*` 表/服务/路由、`qe_experiments`、本地 subprocess 执行路径的既有行为(零回归)、QE loop 的远端 RDAgent 执行端逻辑(仅复用,不改)。

## 4B. 实施方案(Implementation Plan)

分 3 个 Phase,每 Phase 可独立验证(禁简化交付 DESIGN-COMPLIANCE-001):

**Phase 1 — 端到端单 run 远端(打通)**
1. 【远端 RDAgent】新增 WAS 端点(F-009):`HEAD /api/v1/qe_workspace/artifacts/{sha256}`(查存在/size)、`POST .../artifacts/{sha256}`(流式上传,复用 `_atomic_write_request_stream` 模式,无 10MB 限制,服务端校验 sha256 一致 fail-loud)。落盘 `artifact_store/{sha256}`。
2. 【本机 AIstock】新增 `WorkspaceArtifactSyncClient`(F-010):对 L2 大文件算 sha256 → HEAD → 不存在才流式 POST;幂等去重。
3. 【本机】新增 `RemotePredBacktestExecutor`(F-002):按 node 解析 base_url(复用 `QEWorkspaceClient.for_node`),装配远端 workspace(WAS artifact 做 symlink + 小文件经 10MB 通道推 + conf override)→ POST loop 端点(F-003)→ 轮询/callback 取 `qlib_results_enhanced.json`。
4. 【本机】combine orchestrator 按 `node_id` 分流:本地走现有 `ShellPredBacktestExecutor`(F-006 零回归),远端走新执行器。
5. 【本机】路径参数化(F-004):factor cache / parquet / qlib_bin 路径按节点解析;三层就绪校验,缺失 fail-loud(F-008)。
6. **Phase 1 验收 = 一次真实远端单 run,enhanced metrics 与本地基线数值一致(F-007)**。

**Phase 2 — 远端并行 + 守卫**
7. 容量守卫跨来源统一(F-005):per-node 计入 QE loop + combine run 双来源,远端节点不被超订。
8. orchestrator 支持远端 `node_parallelism` 4 路并行调度。

**Phase 3 — 运维硬化**
9. WAS artifact GC/TTL、失败重传、sha 校验失败显式上报;并行度自适应;WAS 通用化复用(F-011)。

**allowed_write_scope(给 Codex 的写入边界)**:
- 【AIstock】`backend/services/multi_alpha/`(新增 RemotePredBacktestExecutor、WAS client、orchestrator 分流)、相关 `backend/tests/`。
- 【RDAgent】`rdagent/app/api_endpoints/`(新增 WAS artifacts 端点)、`results_api_server.py`(挂载)、相关 tests。
- 禁改:`qe_evolution_*` 表/服务/路由、`qe_experiments`、本地 subprocess 既有行为、QE loop RDAgent 执行逻辑、任何 DB migration(除非 WAS 需要 artifact 元数据表,需单独 DDL 门禁)。

## 4C. 验证方案(Verification Plan)/ 测试范围(L0–L5)

| 层级 | 范围 | 内容 |
|---|---|---|
| L0 静态 | 编译/类型 | `python -m compileall` 新文件;前端不涉及 |
| L1 单元 | WAS client / 执行器分流 / 路径解析 | sha256 幂等、HEAD 命中跳过上传、node 分流正确、路径按节点解析、fail-loud |
| L2 集成 | WAS 端点 + 远端执行器 | 流式上传大文件 + sha 校验 + symlink 装配;mock 远端 loop 端点的 POST/poll |
| L3 API/DB | 隔离边界 | 断言不访问 `qe_evolution_*`/`qe_experiments`;WAS 仅写 artifact_store |
| L4 端到端 | 真实远端单 run | Phase 1 验收:真投远端 → 取回 metrics → 与本地基线数值一致 |
| L5 业务 oracle | 数值正确性 | 远端 run 的 CAGR/Sharpe/LOO 与本地同配置逐值一致(F-007) |

- 长运行:combine 本体长任务,L4 用一个已完成配置复跑做数值对账;Phase 2 并行用 nightly smoke 验四态渲染。
- 覆盖率:新增 Python line ≥80% / branch ≥70%(规范 §15.6)。

## 4D. 设计验收矩阵(Design Acceptance Matrix)

> 设计交付阶段 status=`ready`(条目定义完整可进入实现)。实现阶段 Codex 必须回填 `implementation_refs` 真实路径、`status` 改 `done/verified`、附 `test_or_evidence`。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | 实现阶段填 | L1 node 解析单测 | ready | - |
| F-002 | 实现阶段填 | L2 远端执行器集成 | ready | - |
| F-003 | 实现阶段填 | L4 远端 loop 投递 | ready | - |
| F-004 | 实现阶段填 | L1 三层就绪校验 | ready | - |
| F-005 | 实现阶段填 | L1 守卫跨来源单测 | ready | - |
| F-006 | 实现阶段填 | L2 本地路径前后对照 | ready | - |
| F-007 | 实现阶段填 | L5 数值对账 | ready | - |
| F-008 | 实现阶段填 | L1 fail-loud 用例 | ready | - |
| F-009 | 实现阶段填 | L2 WAS 流式+sha 校验 | ready | - |
| F-010 | 实现阶段填 | L1 sha 幂等去重 | ready | - |
| F-011 | 实现阶段填 | L2 symlink 装配 | ready | - |

---

## 5. 工作量与风险初判(已含远端考证修正)
- **可复用(骨架已在)**:HTTP 传输(`QEWorkspaceClient`)、节点注册(`compute_nodes`)、远端**执行端**(RDAgent 9000 loop 端点通用执行 wsl_command,零改动)、小文件同步(`qe_file_sync` ≤10MB)、流式写原语(factor-cache `_atomic_write_request_stream` 可复用为 WAS 基础)。
- **需新增**:
  - 【本机】`RemotePredBacktestExecutor`:按 node 分流,远端走 `QEWorkspaceClient.for_node(node_id)` POST loop + 轮询/回调取结果。
  - 【本机】`WorkspaceArtifactSyncClient`(WAS 客户端,F-010):大文件 sha256 → HEAD → 幂等上传。
  - 【**远端**】WAS 端点(F-009):`HEAD/POST /api/v1/qe_workspace/artifacts/{sha256}` 内容寻址流式,无 10MB 限制。**这是远端唯一需要新增的东西**(执行端不动)。
  - 【本机】路径参数化(按节点解析 factor cache / parquet / qlib_bin 路径)+ 三层就绪校验(F-004)。
  - 【本机】结果回传一致性(F-007)+ 容量守卫跨来源统一(F-005)。
- **工作量判定:中**。执行派发是接线;真正工程量在 **WAS 大文件同步**(本机 client + 远端端点)+ 守卫统一 + 路径解耦。WAS 是**通用长期基础设施**(不绑定 combine),值得投入。
- **关键支撑(做 1 §6.1)**:L2 parquet 跨 run 不变 → WAS 一次上传多 run 复用 → 稳态每 run 仅推 <10MB,远端并行 4 路高度划算。
- **风险**:远端数据未就绪跑空(F-004 fail-loud);WAS sha 校验失败需 fail-loud(F-009);路径硬编码未解耦(§3.4-5)。
- **端到端实测的正确位置**:在 WAS Phase 1 就绪后做(§3.5.5)——908MB 在 WAS 前无通道上远端,故端到端验证是 Phase 1 的验收项,不是前置 smoke。

---

## 6. 非目标 / 边界(Non-Goals)
- 不在本设计内:combine 并发撞车的去重锁彻底方案(属另一 issue);combine 结果直接建 Paper v2 组合(历史明确暂不做);把 macb 纳入统一「实验历史」联合视图。
- 本文是**设计方案**;实现属运行时代码,必须按 [DESIGN-MAIN-001] / FEATURE-WORKFLOW-001 走**独立开发 worktree + 流水线 + 用户确认**,本 docs worktree 只交付设计。

---

## 7. 生产门禁(Production Gates)
- **DB / DDL**:Phase 1/2 默认零 schema 变更(WAS 用文件系统 artifact_store,不需表)。若 Phase 3 引入 artifact 元数据表,必须走 [DB-COMMENT-001] + 独立 DDL 门禁 + 用户确认,不得由业务 service 隐式建表。
- **运行时代码合入**:执行器/WAS 端点/orchestrator 均运行时代码,走独立开发分支 + 自动化流水线 + 用户确认后再合 Main(DESIGN-MAIN-001)。本设计文档本身作为 durable deliverable 可先行提交。
- **合入前最低验证**:
  - `python -m compileall <changed backend paths>`(AIstock + RDAgent 各自)
  - `python -m pytest <targeted L1/L2> -q`
  - `python scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1`
  - `python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F1` 必须 PASS
- **隔离门禁**:合入前提供 L3 证据,证明运行期零访问 `qe_evolution_*` / `qe_experiments`(F-005 守卫除外的只读计数);本地 subprocess 路径前后对照证明零回归(F-006)。
- **跨仓库门禁**:本功能跨 AIstock + RDAgent 两仓库;两侧各自走分支 + PR + 验证,WAS 端点(RDAgent)与 client(AIstock)需契约对齐(sha256/流式协议),实现时以契约测试钉死。
- **数据安全门禁**:WAS 仅在受信内网节点间传输 workspace artifact;sha256 校验失败必须 fail-loud 拒收(F-009),不得静默接受损坏/错配文件。
- **CI 边界**:CI / L0–L5 通过不等于设计验收通过;矩阵未全部 `done/verified` 且 Phase 验收点未达成前,不得请求合入 Main、不得关闭 Issue。

---

## 附:关键代码索引(file:line)
- combine 本地执行:`backend/services/multi_alpha/combine_backtest.py:211-282`(executor)、`:579-621`(run_command/subprocess)、`:154-197`(容量守卫)、`:1365/1594/2011`(并发/workspace/校验)
- QE 远端派发:`backend/services/quantevolver/executors/backtest.py:202`、`qe_workspace_client.py:35/47-57/70-107`、`backend/infra/compute_node_client.py:36-85`、`backend/services/dispatch_service.py:248-351`
- 远端数据同步:`backend/services/quantevolver/factor_cache_remote_sync_service.py`、`qe_file_sync_client.py`
- 节点注册表:DB `infra.compute_nodes`
- **远端执行端(RDAgent 9000,独立仓库 `F:/Dev/RD-Agent-main`)**:`rdagent/app/results_api_server.py:52/78`(挂载 `/scheduler` + `qe_evolution_router`)、`rdagent/app/api_endpoints/qe_evolution_api.py:389`(`POST /tasks/{id}/loops`)、`:65`(`wsl_command` 字段)、`:86/170-173`(`_run_qlib_backtest` 通用执行 wsl_command)、`:114-127`(b64 文件注入)、`:139-148`(cross_node tar)
