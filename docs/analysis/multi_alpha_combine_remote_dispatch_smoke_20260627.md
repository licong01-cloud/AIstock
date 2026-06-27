# combine-backtest 远端派发可行性 Smoke 验证报告

- 日期:2026-06-27
- 文档类型:可行性验证证据(docs/analysis)
- 关联设计:`docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`(§5 建议的"远端单 run smoke"首步)
- 性质:只读探活 + 数据就绪核查,**未在远端真正提交任务**(只 GET 探活 + 查缓存元数据 + 本地读模板),不改任何运行时/DB
- 结论:**远端派发可行;最大成本是每 run 把 ~908MB workspace 同步到远端,不是架构缺口**

---

## 1. 远端节点存活与资源(✅ 通过)

DB `infra.compute_nodes` 注册(只读 SELECT):
| node_id | api_base_url | status | GPU | VRAM |
|---|---|---|---|---|
| `rdagent-node1` | `http://192.168.50.215:9000` | online | RTX 2060 | 6 GB |
| `wsl2-5080` | `http://127.0.0.1:9000`(本机) | online | RTX 5080 | 16 GB |

远端 215 HTTP 探活(只读 GET):
- `GET /health` → 200 `{"status":"ok"}`
- `GET /system/metrics` → 200:GPU util **0%**、RAM used **4.1%**、disk free **486 GB**、`running_tasks: []` —— **完全空闲,资源充足**
- `GET /scheduler/tasks` → 200(可受理任务)
- 注:combine 不训练模型,对 GPU 要求低;215 的 RTX 2060/6GB 对纯 pred-backtest 足够。

## 2. 远端执行端任务无关(✅ 通过,呼应设计 §1.5)

- 远端 9000 = RDAgent 服务(`F:/Dev/RD-Agent-main/rdagent/app/results_api_server.py:78`)。
- loop 端点 `POST /api/v1/qe_workspace/tasks/{id}/loops` 收 `wsl_command`,`_run_qlib_backtest:170-173` **`if wsl_command: final_cmd = wsl_command`** 原样执行 —— combine 的 pred-backtest 命令可直接投递,**远端零改动**。

## 3. 数据就绪核查(⚠️ 关键发现 — 真正的工作量所在)

### 3.1 factor cache:远端已就绪,但本 conf 不依赖它
- `GET /api/v1/qe_workspace/factor-cache/meta`:
  - 远端 215:cache_dir=`/home/lc999/aistock_cache/factor_values`,**713 个因子**
  - 本机 WSL:cache_dir=`/mnt/f/Dev/AIstock/rdagent_assets/factor_values`,575 个因子
- **路径不同已坐实**(草案 §3.4-5 路径解耦风险为真):combine 命令里硬编码 `FACTOR_CACHE_DIR=/mnt/f/...` 直接发远端会指向不存在的路径。但见 §3.2 —— 本 conf 主数据走 parquet,factor cache 影响面需进一步确认。

### 3.2 真正的数据依赖 = workspace 内的大文件(核心成本)
考证 combine runtime template(`conf.yaml`):
- 数据加载 `StaticDataLoader` 读 **`combined_factors_df.parquet`(908 MB)**(conf.yaml:49-51),workspace 内相对路径,**非现算因子**。
- `combined_prediction.pkl` 在 workspace 内生成(`combine_backtest.py:1596`),是各腿 seed 预测的加权组合。
- `provider_uri` 行情数据指向 **`/home/lc999/data/qlib_bin` + `/qlib_minute_bin`**(conf.yaml:3-4)—— **已是远端 Linux 本地路径**,说明这套 conf 模板本就是为 `/home/lc999` 节点(WSL/远端)准备的;Qlib bin 行情应在远端就绪(215 disk free 486GB,容量足),实现时需最终核对该目录存在。

### 3.3 workspace 装配方式
- `prepare_pred_backtest_workspace`(`combine_backtest.py:285`)从 `runtime_template_dir` **本地拷贝**整个模板(含 908MB parquet)到 `workspace_root/run_id/name`。
- **本机执行**:本地拷贝,零网络成本。
- **远端执行**:整个 workspace(含 908MB parquet + pred.pkl)必须先送到远端机 → **每 run ~1GB 文件同步**,这是远端化的真实主成本。b64 注入(`qe_evolution_api.py:114-127`)不适合 1GB,需走 `qe_file_sync_client` / 文件同步通道。

## 4. 结论与修正

| 维度 | smoke 结论 |
|---|---|
| 远端存活/资源 | ✅ 在线、空闲、容量足 |
| 远端执行端 | ✅ 任务无关,零改动可接 combine 命令 |
| 行情数据(provider_uri) | ✅ conf 已指向远端 `/home/lc999` 路径(待实现时核对目录存在) |
| factor cache 路径 | ⚠️ 本机/远端路径不同,命令硬编码需参数化(F-005) |
| **workspace 大文件同步** | ⚠️ **每 run ~908MB parquet + pred.pkl 必须推送远端 — 远端化的主成本与主风险** |

**总判定**:远端单 run 在数据层面可行,无架构阻塞。落地的核心不是"能不能跑",而是"**每 run 1GB workspace 同步的成本与机制**":
- 若 908MB parquet 可**预同步常驻**远端(各 run 复用同一份组合因子,仅 pred.pkl 变)→ 成本可摊薄,远端并行 4 路很划算。
- 若每 run parquet 都不同 → 每 run 推 1GB,网络/磁盘成本需权衡是否仍优于本机串行。

→ **实现首要任务**:确认 `combined_factors_df.parquet` 是否跨 run 复用(同 roster 多窗口/topk 扫描时是否同一份),决定"预同步常驻"还是"每 run 推送"。这直接决定远端并行的性价比。

## 5. 下一步建议
1. **(只读)确认 parquet 跨 run 复用性** + 远端 `/home/lc999/data/qlib_bin` 目录存在性 —— 关闭最后两个数据未知。（✅ 已完成,见 §6）

## 6. 做 1 后续:两个数据未知已关闭(只读)

### 6.1 ✅ parquet 跨 run 复用 —— 可预同步常驻(性价比成立)
直接读模板 `combined_factors_df.parquet`(`pandas.read_parquet`):
- shape = **(8,240,001 行, 24 列)**;index = `(datetime, instrument)`;**日期范围 2018-08-01 → 2026-04-28**(全期全市场)。
- 列 = 全因子输入特征集(`m_turnover_*` / `bb_cp_momentum` / `book_value_price_ratio` / `roe_stability_score` / `ChipWinnerRate*` 等 24 个),即**喂各腿模型的输入特征全集**。
- 本会话所有 combine 提交 payload 的 `runtime_template_dir` **只有一个**:`...multi-alpha-combine-backtest-20260620/.../template_runtime`(三腿/两腿、所有窗口/topk 共用)。
- **结论**:这份 parquet 与 roster / 窗口 / topk / scheme **全部无关**——
  - 窗口(oos_start/end)= 回测时间切片(conf override),不改 parquet;
  - topk / n_drop = 策略参数(conf override);
  - scheme = 组合权重(只改 `combined_prediction.pkl`,该文件小);
  - roster 不同 = 各腿 seed 预测的不同加权 → 只影响 `combined_prediction.pkl`,**parquet 输入不变**。
- **性价比裁决**:**parquet 可一次预同步常驻远端,所有 run 复用**;每 run 仅需推送小体积 `combined_prediction.pkl`(+ 必要 conf override)。→ **远端并行 4 路高度划算,不是"每 run 推 1GB"。** §4 的悲观分支被排除。

### 6.2 ✅ 远端 `/home/lc999/data/qlib_bin` 存在 —— 历史运行已证实
- 无现成端点直接 stat 该目录,但有强证据:
  - 远端 215 此前**已成功跑 QE 自定义演进 loop**(用户陈述 + 记忆 `feedback_qe_parallelism_config`:远端 CPU 4 并行为既定配置);QE loop 的 conf 用**同一套** `provider_uri: /home/lc999/data/qlib_bin` + `/qlib_minute_bin`。若该目录不存在,QE loop 无法启动 → 既然历史能跑,目录必然就绪。
  - smoke 实测 `GET /scheduler/tasks` 远端有 running 的 rdagent 任务(`fin_quant`),证明远端 RDAgent 数据链工作正常。
- **结论**:行情数据(day + 1min bin)在远端就绪。实现时仅需首次部署做一次目录存在性 fail-loud 断言,非阻塞。

### 6.3 做 1 最终裁决
- 远端派发**数据层面可行**,且**性价比强**(L2 跨 run 不变,可预同步复用):
  - 一次性同步:`combined_factors_df.parquet`(908MB)→ 远端常驻复用。
  - 每 run 增量:`combined_prediction.pkl`(<10MB)+ conf override(窗口/topk/scheme)。
  - 行情 bin(L1):远端已就绪。
- **但发现一个真实工程缺口(做 1 受控验证阶段)**:908MB parquet **无现成通道上远端**——`qe_file_sync` 限 10MB(`qe_file_sync_api.py:55`),factor-cache 流式通道语义是"单因子文件"非"任意 workspace 文件"。
  - **修正此前"无需改远端"的结论**:远端**执行端**确实零改动,但远端需新增一个**大文件传输端**(WAS,内容寻址流式)。详见设计 §3.5(长期方案)+ F-009~011。
- **端到端实测的正确位置**:WAS Phase 1 就绪后做(设计 §3.5.5)。在 WAS 前,908MB 无通道上远端 → 真·端到端验证是 **F1 Phase 1 的验收项**,不是带外 scp 的临时 smoke。带外搬运被明确排除(用户要求长期可持续方案)。

---

## 附:smoke 执行的只读操作清单(可复现)
- DB:`SELECT ... FROM infra.compute_nodes`(只读)
- HTTP GET:`192.168.50.215:9000` 的 `/health`、`/system/metrics`、`/scheduler/tasks`、`/api/v1/qe_workspace/factor-cache/meta`
- 本地读:combine runtime template 的 `conf.yaml` / `.factor_env` / 文件清单
- **未执行任何远端写操作 / 未提交任何远端任务**
