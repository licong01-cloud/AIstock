# Advisory 荐股工作台交付与验证

日期：2026-10-28。设计：[F2 详细设计与 14 项验收矩阵](../architecture/advisory_recommendation_workbench_f2_design_20261028.md)。设计 PR [#6776](https://github.com/licong01-cloud/AIstock/pull/6776) 已独立合入，merge `84c580d14a4d0b3d41d2efc7ae62bbcc0b3919f4`。本记录对应后续源码交付；代码 PR/最终 merge 身份由 GitHub 的不可变 PR 和合入记录提供。

## 交付内容

全局左侧“策略包与模拟交易”分组新增“荐股”，直达 `/paper-v2/advisory`。打开荐股路径自动展开对应分组并高亮。总览不自动选中最早的旧包；五个启用任务分别展示首次发布日、最新名单、输入截止、估值日、有效样本和费用前参考收益。父包资产阻断保留可见，旧名单不冒充当前推荐。

详情包含当日推荐、持续跟踪、历史记录、调整记录、效果统计、持久化模型观察、共享数据就绪与运行状态。新加入、继续推荐、移出集合依据正式前后名单；WATCH 等待进入可以在范围内，范围外 WAITING 不等于已经退出。连续天数缺链或换绑定时显示未知。历史按 episode 独立分页；同一股票重复进入不会合并。股票详情区分进入、退出信号与当前参考结算结果。

原管理页面移动到 `/paper-v2/advisory/manage`，保留配置、手工复评和历史研究入口；新日常工作台仅 GET 读取。新增 8 个只读接口，使用一致性只读事务、SQL 分页、轻量字段和快照校验，409/404/422/503 均可见。

## 三轮审核与修复

本轮由同一执行者进行不同视角的三轮自审，没有声称外部独立审核。

| 轮次 | 审核对象 | 发现与已落实修复 | 验证 |
|---|---|---|---|
| 1：业务语义 | 原始已保存名单、episode、发布日期与绩效合同 | 分离 WATCH 进入和阈值外观察；集合变化不等于 ENTER/HOLD；待结算不以 EXITED 代替；价格使用正式 1000 单位除数；前序不足保留 UNKNOWN | projection/API/service 测试、真实五任务读回 |
| 2：接口与一致性 | 过滤、分页、串包、价格版本和错误路径 | HTTP 页大小改为正确整数解析；空 episode 身份集合不能回退全量；批量读取页内价格；水位查询按固定日期裁剪以避免超时；水位纳入实际输入日；历史已发布日期不套用其它日期失败；只读就绪结果白名单投影 | 41 项直接/既存绩效测试；跨包 404、变化 409；五包源/分页读回 |
| 3：页面及逐项设计符合性 | 已确认图、实际渲染、键盘与兼容入口 | 加入全局导航；仅在有效页面显示来源/日期筛选；补模型类别、生成时间、待结算与过程成员身份；历史日期不称今日；缺前序计数显示未知；退出原因中文/颜色/图标；异步切换隔离与抽屉键盘关闭 | 工作台 9 项、旧管理页 4 项浏览器验证及三张实际截图 |

DESIGN-COMPLIANCE-001 四项核对：14 项均有实现与直接证据；无错误伪成功/旧名单静默回退；模型、发布、排名、交易和数据就绪语义未改；无新增审批门禁。设计补充了导航文件、页面容器、路由叶与精确证据文件范围。

## 真实只读核对

观察时间与源码文件摘要见 [receipt.json](evidence/advisory_workbench_20261028/receipt.json)。通过新建 Python 进程、FastAPI TestClient 和真实既有数据库执行只读查询；没有启动用户后端。接收 95 份真实响应，随后将这些响应提供给 3012 隔离前端做浏览器验收；这与在线生产激活分开。

| 任务 | 最新目标日 | 首次发布日 | 范围内 | 新加入 | 继续 | 移出 | episode 总数 |
|---|---|---|---:|---:|---:|---:|---:|
| 多 Alpha 父包（每日 Top20） | 2026-10-09 | 2026-08-14 | 当前目标无名单 | — | — | — | 164 |
| LGB 新包 | 2026-10-28 | 2026-10-26 | 25 | 6 | 19 | 9 | 20 |
| LSTM 新包 | 2026-10-28 | 2026-10-12 | 21 | 7 | 14 | 7 | 71 |
| TCN 新包 | 2026-10-28 | 2026-10-12 | 24 | 12 | 12 | 10 | 69 |
| 单 Alpha 旧包 | 2026-10-28 | 2026-08-14 | 20 | 19 | 1 | 20 | 230 |

这些数量是核对时点的源事实，不是预设 TopK 或模型成果。首次发布日来自正式 PUBLISHED 版本；旧决策/episode 可以早于正式名单发布起点。数据在随后发布或结算时可变化。

- 五任务 source 过滤结果均符合实际 origin_source；第一页与第二页 episode 身份无重叠。
- 跨任务查询另一个任务的 episode 返回 404；不存在任务返回 404；过期 snapshot 返回 409。
- 新接口不返回巨型 publication/selection payload；每日读取不调用 `model-shadow`，模型分栏仅读既存 `reranking-shadow`。
- 共享就绪合同可读，但本次五包 consumer_ready 均为 false、依赖状态 UNKNOWN。页面保留原因，不根据同步时间或接口成功推断就绪。
- 正式 manifest 的 model_type 未填写时显示“类别待核验”；不从程序名猜测，也不改写策略资产。

## 验证命令与结果

| 验证 | 结果 |
|---|---|
| `python -m pytest backend/tests/advisory_workbench backend/tests/advisory_historical_range/test_program_performance.py -q --tb=short` | 41 passed |
| 缺链/重复身份最后修订后的 `backend/tests/advisory_workbench/test_projection.py` | 12 passed |
| `frontend/tests/paper-v2/advisory-workbench.spec.ts`，chromium，60s 每例上限 | 9 passed：8 项合成边界、1 项真实响应呈现 |
| 旧管理页四项：history 分页与重置、慢请求 shell、冻结观察 | 4 passed |
| changed-file Ruff、直接 TypeScript 编译器、ESLint | PASS |
| CI 新文件归属、nox 真实执行目标与浏览器隔离配置注册 | 2 项直接合同通过，隔离配置收集到工作台 9 项；CI 中真实捕获用例需显式本地文件，保持环境限定 |
| F2 feature validator、diff/scope | PASS：14 条设计、14 条矩阵、0 warnings |

本机 RTK 的 tsc/npm 包装未正确解析 worktree 的已安装依赖；读取实际输出后改用 `node node_modules/typescript/bin/tsc --noEmit --incremental false` 和已有 ESLint/Playwright Node CLI。没有安装依赖，不把包装器的“No errors”误报当作成功。前端既有 Browserslist/旧 Paper CSS 警告不属于本次变更。

实际截图：[卡片总览](evidence/advisory_workbench_20261028/overview.png)、[当日推荐](evidence/advisory_workbench_20261028/daily.png)、[历史与股票详情](evidence/advisory_workbench_20261028/history-detail.png)。原始响应与执行脚本位于任务工作树的 `tmp/advisory-workbench/`；持久化收据记录哈希与核心事实，不提交完整数据载荷。

直接矩阵覆盖本次契约和 UI 路径。全仓库、其它 Paper/LocalSIM/QE、真实服务生命周期由现有 PR CI/nightly 范围承担或保持未执行；未启动额外研究和流水线任务。

## 发布、运行与残余状态

- `production_ddl_gate=noop`；`production_dml=not_requested`；数据库写入 0；依赖变化 noop。
- 源码合入与 root 同步按既有授权执行，工作树与分支保留，不做未授权清理。
- `backend_restart_owner=user`，目标为 `backend-main`，参见 [用户重启 runbook](backend_main_runtime_restart_runbook.md) 和 [runtime targets](../standards/aistock_runtime_targets_v1.yaml)。没有执行用户服务启停或后端启动脚本。
- 用户重启后先读 `/api/v1/runtime-identity` 核对合入身份，再读 `/api/v1/advisory/workbench/overview`、LSTM 当日名单/历史和父包阻断状态；前端激活单独核对导航与页面。重启前不声明生产页面已生效。
- 父包冻结资产阻断、数据依赖未知、模型影子观察成熟度和经济效果保留各自状态；本次 UI 不解除它们。重排序仍是影子观察，没有替换生产排名或追加训练。
