# Codex 实现提示词 — combine-backtest 远端派发 Phase 1(WAS + 单 run 远端跑通)

> 用途:整段复制给 Codex dispatch。按 [feedback_codex_prompt_markers] 用显式开始/结束标识包裹。
> 设计真源:`docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md`(F1 validate PASS)。
> 范围:仅 Phase 1(端到端单 run 远端跑通,数值对齐本地)。Phase 2/3 不在本次。

---

========================= CODEX PROMPT START =========================

【任务】实现 multi-alpha combine-backtest 远端节点派发能力的 Phase 1:让一个 combine pred-backtest 单 run 能派发到远端计算节点(rdagent-node1 / 192.168.50.215)执行,并把结果回传,数值与本地基线一致。这是跨 AIstock + RDAgent 两仓库的 F1 功能开发。

【权威设计文档(必读,只引编号不复述全文)】
F:/Dev/AIstock/docs/architecture/multi_alpha_combine_backtest_remote_dispatch_design_20260627.md
- 本次只做 §4B 实施方案的 Phase 1(步骤 1–6)。Phase 2/3 不做。
- 严格遵守设计验收索引 F-001~F-011 与 §7 生产门禁。

【流程铁律(AIstock 规范,必须遵守)】
1. 走 FEATURE-WORKFLOW-001。不得在 F:/Dev/AIstock 根 main 直接开发;为本任务各仓库创建独立 worktree + task 分支(从各自 origin/main)。
   - AIstock 分支建议:feature/combine-remote-dispatch-phase1-20260628
   - RDAgent 分支建议:feature/qe-workspace-artifact-store-20260628
2. 禁 silent error:任何失败必须显式上报(哪个节点/哪个 stage/exit_code/stderr_tail/sha 不一致),禁 except:pass、禁兜底默认值、禁"insufficient"掩盖真实错误。
3. 不得交付简化版/POC/mock-only/静默 fallback 并描述为完成(DESIGN-COMPLIANCE-001)。无法完整实现立即停下报告阻塞,不得自行降级或塞进未声明的"后续"。
4. 合入前回填设计验收矩阵(§4D):每条 F-00x 填 implementation_refs 真实路径 + status=done/verified + test_or_evidence。
5. 不改:qe_evolution_* 表/服务/路由、qe_experiments、本地 subprocess 既有行为(零回归 F-006)、QE loop 的 RDAgent 执行端逻辑(仅复用,不改)。

【allowed_write_scope】
- 【AIstock】backend/services/multi_alpha/(新增远端执行器 + WAS client + orchestrator 分流)、backend/tests/
- 【RDAgent】rdagent/app/api_endpoints/(新增 WAS artifacts 端点)、rdagent/app/results_api_server.py(挂载)、对应 tests
- 不得改 DB schema(Phase 1 用文件系统 artifact_store,不建表)。

【Phase 1 实现内容(对应 F-002/003/004/006/008/009/010/011 + 部分 001)】

A. 【RDAgent 远端,F-009】新增内容寻址的 Workspace Artifact Store 端点:
   - HEAD /api/v1/qe_workspace/artifacts/{sha256} → 返回 {exists, size}(查远端是否已缓存该 artifact)。
   - POST /api/v1/qe_workspace/artifacts/{sha256} → 流式接收大文件(复用 factor_cache_api.py 的 _atomic_write_request_stream 模式,无 10MB 限制),落盘到 artifact_store 根目录下 {sha256}。
   - 服务端必须重算 sha256 并与 URL 中的 {sha256} 比对,不一致则删除临时文件 + 返回 4xx 显式错误(fail-loud,F-008/F-009),禁静默接受。
   - artifact_store 根目录用环境变量配置(参考 QE_WORKSPACE_* 既有约定),不存在则创建;路径不得硬编码本机路径。
   - 参考实现:rdagent/app/api_endpoints/factor_cache_api.py 的流式写(_atomic_write_request_stream / _atomic_write_fileobj),以及 qe_file_sync_api.py 的目录/校验风格。

B. 【AIstock 本机,F-010】新增 WorkspaceArtifactSyncClient:
   - 给定本地文件路径,流式算 sha256(分块,勿全量读入内存)。
   - 先 HEAD 远端 artifacts/{sha256}:已存在(size 一致)→ 跳过上传(幂等去重);否则流式 POST 上传。
   - 上传后可选再 HEAD 确认存在。失败显式抛错带上下文。
   - 节点 base_url 复用 QEWorkspaceClient.for_node(node_id) 的解析(从 infra.compute_nodes.api_base_url)。

C. 【AIstock 本机,F-002/003/004/011】新增 RemotePredBacktestExecutor:
   - 与现有 ShellPredBacktestExecutor(combine_backtest.py:211-282)同接口(execute_pred_backtest 等价签名),便于 orchestrator 透明切换。
   - 装配远端 workspace:
     * L2 大文件(combined_factors_df.parquet)→ 经 WAS client 确保远端已有该 sha256 artifact;在远端 workspace 用 symlink/hardlink 指向 artifact_store/{sha256}(F-011 零拷贝),或在 wsl_command 中指向 artifact 路径。
     * L3 小文件(combined_prediction.pkl <10MB + conf override + 必要模板小文件)→ 经现有 qe_file_sync 通道(experiments/{id}/files)推送。
     * 行情 L1(qlib_bin)→ 远端常驻,不传;仅做存在性校验(缺失 fail-loud,F-004)。
   - 路径解耦(F-004):wsl_command 里所有路径(factor cache / parquet / qlib_bin)按目标节点解析,不得把本机 /mnt/f 路径发给远端。远端真实路径示例(已实测):factor cache=/home/lc999/aistock_cache/factor_values,qlib_bin=/home/lc999/data/qlib_bin。具体以节点配置为准,不要写死。
   - 派发:复用 QEWorkspaceClient.for_node(node_id).create_and_run_loop(wsl_command=...)(F-003),POST 远端 loop 端点;轮询/callback 取 qlib_results_enhanced.json;ingest 成与本地相同的 metrics 结构。

D. 【AIstock 本机,F-001/F-006】orchestrator 按 node_id 分流:
   - node_id 解析为"本地"→ 走现有 ShellPredBacktestExecutor(行为字节级不变,F-006 零回归)。
   - node_id 解析为"远端"(infra.compute_nodes 中 api_base_url 非 localhost)→ 走 RemotePredBacktestExecutor。
   - 分流判定基于 infra.compute_nodes 注册,不是字符串猜测。

【验证(必须真实证据,不接受 mock 替代业务路径)】
- L0:python -m compileall 两仓库改动文件。
- L1:WAS client sha256 幂等(HEAD 命中跳过)、node 分流正确、路径按节点解析、fail-loud 各分支、服务端 sha 不一致拒收。覆盖率新增代码 line≥80%/branch≥70%。
- L2:WAS 端点流式上传大文件 + sha 校验 + symlink 装配;远端执行器对 mock loop 端点的 POST/poll。
- L4(Phase 1 验收硬指标,F-007):用一个已完成的本地 combine 配置,真实派发到远端 215 跑一次,取回 enhanced metrics,与本地基线 CAGR/Sharpe/MaxDD/Calmar/LOO 逐值一致(容差说明清楚)。提供真实 run 证据(run_id、远端日志、回传 metrics、对账表)。
- L3:断言运行期零访问 qe_evolution_*/qe_experiments(除 F-005 守卫只读计数,Phase 1 可不动守卫);WAS 仅写 artifact_store。

【交付物】
- 两仓库各自 PR(分支、改动文件、验证命令与结果)。
- 设计验收矩阵 §4D 回填(F-001/002/003/004/006/008/009/010/011 → done/verified + 真实 refs + 证据;F-005/007 中 F-007 须 L4 实测,F-005 守卫属 Phase 2 可标 ready/not-in-phase1 并说明)。
- 跨仓库契约说明:WAS 的 sha256/流式协议两端对齐,附契约测试。
- 明确列出未实现/未验证项;只要有未获批准缺项,不得请求合入 Main、不得关闭 Issue。

【关键约束复述】
- 严禁简化:不得用"把 parquet 塞进 10MB 通道分片""跳过 sha 校验""本地跑假装远端"等捷径冒充完成。
- 严禁静默:远端任何失败必须 loud 且可定位。
- 零回归:本地 subprocess 路径行为不得变(F-006,提供前后对照)。
- 端到端数值一致是 Phase 1 的硬验收(F-007),不是"跑通即可"。

========================== CODEX PROMPT END ==========================
