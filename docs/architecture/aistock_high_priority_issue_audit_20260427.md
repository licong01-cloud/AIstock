# AIstock 高优先级问题审计与问题 5 专项分析（2026-04-27）

> 范围：基于当前 `F:\Dev\AIstock` 工作区的静态代码检索与既有项目分析结论。本文只记录问题与整改单，不修改业务代码。
> 重点：先确认 WSL 文件系统直连仍存在，再展开“问题 5：旧评级/旧指标路径仍被调用”的专项审计。

## 0. 总体结论

- AIstock 当前仍存在直接访问 WSL/WSL 挂载路径的运行时代码，不能认为该问题已经清零。
- 因子评级与独立指标已经有权威实现，但前端与部分后端服务仍混用旧的 `qe_factor_classification.grade`、`ic_value`、`sharpe_value`、`ann_ret_value`，以及未限定 `calc_engine='qe_eval_v2'` 的 `aistock_factor_metrics` 查询。
- `qe_factor_classification` 仍可作为分类、维度、画像、数据源分组等元数据表使用，但不应继续作为生产决策的评级和指标权威来源。
- 需要优先统一到：官方独立指标 `FactorOfficialEvaluationService` + `CALC_ENGINE = 'qe_eval_v2'` + 官方评级 `FactorRatingService` / `qe_factor_official_ratings` active rule version。

## 1. 全部高优先级问题清单

| 编号 | 优先级 | 问题 | 当前证据 | 风险 | 建议处理 |
| --- | --- | --- | --- | --- | --- |
| 1 | P0 | 仍有直接访问 WSL 文件系统/WSL 本机路径的程序代码 | `backend/services/manual_factor_service.py:173` 通过 `\\wsl$\Ubuntu...` 从 Windows 侧读 WSL 文件；`scripts/generate_stock_pool.py:39`、`scripts/generate_stock_pool.py:51` 使用 `\\wsl.localhost\Ubuntu...`；大量 `/mnt/f/...`、`/home/lc999/...` 硬编码仍散落在 backend/scripts/configs | 破坏跨机器、远程节点和服务化部署；运行结果依赖本机 WSL 目录 | 运行态改为节点能力、DB、对象存储或标准 artifact API；脚本路径显式标注 diagnostic-only |
| 2 | P0 | V24/V25 模型资产路径仍硬编码到 WSL/个人机器 | `configs/execution_algos/v25_two_stage.yaml:4`、`configs/execution_algos/v25_two_stage.yaml:5`；`backend/init_catalog_db.py:1037`；`backend/services/quantevolver/config_composer.py:404`；`scripts/add_v25_to_db.py:15`、`scripts/add_v25_to_db.py:16` | StrategyPackage/Paper v2 在真实环境会依赖本机文件；模型缺失时出现不可复现实验或失败 | 模型资产必须由 StrategyPackage manifest/asset registry 管理，路径解析只读取受控 artifact，不内置个人路径 |
| 3 | P0 | Paper v2 的真实 V25 执行仍缺 `day_features` 权威提供链 | `backend/execution_algos/v25_two_stage_algo.py:355` 要求 `day_features`；`backend/services/paper_trading_v2/market_data.py:525` 返回的 `market_context` 没有 `day_features`；`backend/services/paper_trading_v2/live_session.py:462` 只透传并补 `v25_realtime_streaming` | V25 会 fail-fast，或若被错误打开默认特征会污染真实执行结论 | 建立 `day_features` provider，按 symbol/date 提供 10 维特征；缺失必须 fail-fast，禁止默认零向量用于真实运行 |
| 4 | P1 | WSL/local path hardcoding 仍散落在 backend/scripts/configs | `backend/services/manual_factor_service.py:30`、`backend/services/manual_factor_service.py:32`、`backend/services/quantevolver/config_composer.py:38`、`backend/services/quantevolver/config_composer.py:73`、`backend/services/quantevolver/stock_pool_sync.py:193` 等 | 远程 QE、多节点 dispatch、Paper v2 复盘无法保证同一套路径语义 | 路径集中到 compute node/asset registry/runtime profile；新增静态检查阻断运行态新增硬编码 |
| 5 | P0 | 前端/后端仍消费旧评级和旧指标路径 | 详见本文第 2 节 | 因子筛选、Multi-Alpha、组合架构、相关性去重、清洗/删除可能基于旧口径做决策 | 统一迁移到官方评级 + `qe_eval_v2` 独立指标；保留 legacy 字段仅展示或迁移对照 |
| 6 | P1 | Remote QE / Multi-Alpha 分布式基础设施仍需 gated remediation | 已有文档 `docs/architecture/qe_remote_execution_capability_audit_20260427.md`；当前分布式开关、callback、artifact 传输、CPU/GPU 节点规划仍需分阶段处理 | 直接开启分布式会产生状态不同步、artifact 不完整、GPU/CPU 分配错误 | 先修 callback、stock pool、artifact fail-fast，再打开分布式负载均衡 |
| 7 | P1 | 当前工作区存在大量未提交/未归档改动，追溯风险高 | `git status --short` 显示 backend/frontend/scripts/docs 多处 modified/untracked | 审计和修复容易混入其它窗口改动，难以回滚和验收 | 先把本轮审计文档独立提交或归档；修复代码前建立 issue/branch/checklist |
| 8 | P1 | 缺少静态质量护栏 | 目前没有明确阻断新增 `\\wsl$`、`\\wsl.localhost`、生产决策读取 `classification.grade` 的检查 | 同类问题会反复回流 | 增加 `rg`/pytest 静态检查：禁止运行态新增 WSL UNC；禁止生产筛选/排序读取旧 `classification.grade`/`ic_value` |

## 2. 问题 5 专项：旧评级/旧指标路径审计

### 2.1 权威口径

当前应统一到以下权威实现：

- 官方独立指标计算与读取：`backend/services/quantevolver/factor_official_evaluation_service.py:22` 定义 `CALC_ENGINE = "qe_eval_v2"`，写入/读取 `aistock_factor_metrics`。
- 官方独立指标 API：`backend/routers/quantevolver.py:3569` `/official-evaluation/compute`，`backend/routers/quantevolver.py:3584` `/official-evaluation/factors/{factor_name}`，`backend/routers/quantevolver.py:3600` `/official-evaluation/summary`，`backend/routers/quantevolver.py:3608` `/official-evaluation/factors/{factor_name}/ic-decay`。
- 官方评级服务：`backend/services/quantevolver/factor_rating_service.py:226` 默认读取 active rule version；`backend/services/quantevolver/factor_rating_service.py:570` 评级输入只读取 `calc_engine = CALC_ENGINE` 的指标。
- 官方评级表：`qe_factor_official_ratings`，字段应使用 `official_grade`、`official_score`、`grade_reason_structured`、`hard_gate_flags`、`metrics_snapshot`。
- active 评级版本来源：`qe_rating_rule_versions.status = 'active'`，不能硬编码 `v2.0.0`。

### 2.2 仍在调用旧评级/旧指标的后端路径

| 路径 | 当前旧依赖 | 影响 | 目标替换 | 优先级 |
| --- | --- | --- | --- | --- |
| `backend/services/quantevolver/factor_analyst.py:1390` | `get_classifications()` 用 `qe_factor_classification.grade` 做 `grade_filter`，选择 `grade/ic_value/sharpe_value/ann_ret_value`，并按 `grade ASC, ic_value DESC` 排序 | `/factor-analyst/classifications` 返回旧评级/旧指标，前端 compose 仍在消费 | classification endpoint 改为 join active `qe_factor_official_ratings` + `aistock_factor_metrics.calc_engine = CALC_ENGINE`；旧字段改名 `legacy_*` | P0 |
| `backend/services/quantevolver/factor_analyst.py:1477` | `recommend_factor_combination()` 过滤 `c.grade IS NOT NULL`，按旧 `c.grade` 和 `c.ic_value` 排序 | 因子组合推荐可能基于旧评级做优先级 | 用 `official_grade/official_score` 排序，用 `ind_rank_ic_best_abs` 或 `rank_ic_*` 作为强度 | P0 |
| `backend/services/quantevolver/factor_analyst.py:2018` | 仍把 `ic_value/sharpe_value/ann_ret_value` 写回 `qe_factor_classification` | 旧指标镜像继续存在，容易被误读为权威 | 保留仅作 legacy mirror 时必须命名/注释清楚；生产读取禁止使用 | P1 |
| `backend/services/quantevolver/qe_evolution_agents.py:735` | 候选因子查询选择 `c.grade/c.ic_value`，`grade_weights` 以旧 grade 加权抽样，LLM prompt 显示旧 grade/IC | 演进 Agent 可能被旧评级和旧 IC 引导 | 候选池 join 官方评级；prompt 使用 `official_grade/official_score` 和 `ind_rank_ic_*` | P0 |
| `backend/services/quantevolver/qe_evolution_agents.py:846` | 详情查询仍返回 `c.grade/c.ic_value`；虽然 `m` 已限定 `calc_engine`，但 prompt 里 fallback 到旧 `ic_value` | 缺指标时会回落到旧 IC | 缺官方指标时显示 `N/A` 并触发 official evaluation，不回落旧指标 | P0 |
| `backend/services/quantevolver/qe_evolution_service.py:1150` | 因子库摘要按 `qe_factor_classification.grade` 分组，聚合 `AVG(ic_value/sharpe_value)` | researcher/model agent 的方向判断可能基于旧口径 | 按 official grade 分组，聚合 `qe_eval_v2` 指标；category 可继续来自 classification | P1 |
| `backend/services/quantevolver/portfolio_architect.py:310` | `_analyze_factors()` 从 classification 读 `grade/ic_value/sharpe_value/ann_ret_value` | 组合架构分析使用旧评分 | join 官方评级和官方指标；旧任务指标只作为 task-level 参考 | P0 |
| `backend/services/quantevolver/portfolio_architect.py:845` | `_get_factor_metadata_summary()` 过滤 `c.grade IS NOT NULL`，按旧 grade/IC 排序 | LLM 生成组合时被旧评级排序影响 | 过滤 official grade，排序 official score + official RankIC | P0 |
| `backend/services/quantevolver/portfolio_architect.py:908` | `_prefilter_factors()` 用旧 `c.grade` 过滤，并 `COALESCE(m.ic_mean, c.ic_value, 0)` 回退旧 IC | 预筛选可能放入旧评级强但官方弱的因子 | 只用 active official grade；无官方指标时 fail/跳过，不回退 `c.ic_value` | P0 |
| `backend/services/quantevolver/multi_alpha_selector.py:205` | grade 已来自 official，但仍选择/排序 `fc.ic_value`，`best_horizon_ic` 也包含旧 `ic_value` | Multi-Alpha 自动选因子仍受旧 IC 影响 | 删除 `fc.ic_value` 参与排序；使用 `rank_ic_1d/5d/10d/20d`、`ind_rank_ic_best_abs` | P0 |
| `backend/routers/quantevolver.py:488` | `/factors` 列表的 `aistock_factor_metrics` lateral query 没有 `calc_engine = CALC_ENGINE` | 因子库 UI 可能混入历史/非官方 engine 指标 | 所有 metrics lateral query 增加 `calc_engine`，并统一返回 `ind_*` 字段 | P0 |
| `backend/routers/quantevolver.py:1733` | `/multi-alpha/classified-factors` 虽用 `fr.official_grade`，但返回 `fc.ic_value`；metrics query 无 `calc_engine`；排序 `ABS(fc.ic_value)` | Multi-Alpha 前端候选列表展示和排序旧 IC | 返回 `official_grade/official_score/ind_rank_ic_best_abs`，SQL 绑定 `CALC_ENGINE` | P0 |
| `backend/routers/quantevolver.py:1819` | `/multi-alpha/validate-config` 查询 `factor_name, grade, data_source_group FROM qe_factor_classification` | 覆盖率校验的 `has_grade` 是旧 grade 覆盖率 | `has_grade` 改为 active official rating 覆盖率；`data_source_group` 继续来自 classification | P0 |
| `backend/routers/quantevolver.py:1848` | `/multi-alpha/classification-coverage` 用 `COUNT(grade)` 与 `AVG(ABS(fc.ic_value))` | UI 显示旧评级覆盖和旧平均 IC | 按 active official rating 统计覆盖，平均 IC 用 `qe_eval_v2` `rank_ic_mean/ic_mean` | P0 |
| `backend/routers/quantevolver.py:6800` | 旧 `/factors/{factor_name}/ic-decay-trend` 查询 `aistock_factor_metrics` 没有 `calc_engine` | 与 official ic-decay endpoint 口径不一致 | 废弃该端点或内部转调 `/official-evaluation/.../ic-decay` | P1 |
| `backend/routers/quantevolver_evolution.py:2224` | correlation overview 硬编码 `calc_engine = 'qe_eval_v2'`，未复用常量 | 口径对，但维护风险高 | import `CALC_ENGINE`，避免常量漂移 | P2 |
| `backend/routers/quantevolver_evolution.py:2741` | correlation pair 详情的 independent metrics 无 `calc_engine`；classification 查询直接返回旧 `grade` | 相关性详情页评级比较使用旧 grade，指标可能混 engine | metrics 加 `calc_engine = CALC_ENGINE`；classification 返回 `official_grade`，旧 grade 改名 `legacy_grade` | P0 |
| `backend/routers/quantevolver_evolution.py:2957` | related factors 批量指标查询无 `calc_engine` | 去重面板可能比较非官方指标 | 增加 `calc_engine = CALC_ENGINE` | P0 |
| `backend/services/quantevolver/deletion_candidate_service.py:110` | latest metrics CTE 未限定 `calc_engine`；rating CTE 硬编码 `rule_version = 'v2.0.0'` | 删除候选可能不跟随 active 规则版本 | 使用 active version；metrics 加 `calc_engine = CALC_ENGINE` | P0 |
| `backend/services/quantevolver/factor_cleanup_service.py:350` | cleanup 评级 CTE 不限定 active version；metrics CTE 无 `calc_engine` | 因子清洗的 D 级/纯噪声判定可能用错版本或混指标 | active official rating + `calc_engine = CALC_ENGINE` | P0 |
| `backend/services/manual_factor_service.py:296` | 手工因子 full pipeline 仍走旧 WSL `compute_factor_metrics_unified.py` 指标计算；`_save_metrics_to_db()` 已 no-op，导致 UI 有结果但官方指标/评级不一定落表 | 手工因子新增后可能没有官方独立指标和官方评级 | 手工因子保存后调用 `FactorOfficialEvaluationService.compute`，再调用 `FactorRatingService`；前端显示官方结果 | P0 |
| `scripts/batch_develop_factors_v2.py:987` | 直接 `DELETE/INSERT aistock_factor_metrics`，没有 `calc_engine/factor_catalog_id/snapshot_date` 权威约束 | 脚本运行会污染官方指标表 | 改为调用 official evaluation 或写入隔离 diagnostic 表 | P1 |
| `scripts/optimize_timeout_factors.py:369` | 直接删除/插入 `aistock_factor_metrics` full 窗口，无 `calc_engine` | 同上 | 改为 official evaluation 或 diagnostic-only | P1 |
| `backend/services/rdagent_factor_metrics_sync.py:110`、`backend/services/manual_factor_service.py:582` | 保留 legacy UPSERT SQL 常量，但执行函数已 no-op | 低运行风险，但容易误用/复制 | 删除死 SQL 或加静态检查禁止调用 | P2 |

### 2.3 仍在消费旧路径的前端页面/组件

| 路径 | 当前旧依赖 | 影响 | 目标替换 | 优先级 |
| --- | --- | --- | --- | --- |
| `frontend/src/app/quantevolver/compose/page.tsx:250` | `classificationMap` 类型包含 `ic_value/sharpe_value/ann_ret_value/grade` | compose 页面已选因子指标汇总仍取旧字段 | 改拉 `/quantevolver/factors` 或新 enriched classifications endpoint，字段改为 `official_grade/ind_*` | P0 |
| `frontend/src/app/quantevolver/compose/page.tsx:269` | 请求 `/quantevolver/factor-analyst/classifications?limit=1000&active_only=false` | 前端主动拉旧 classification endpoint | endpoint 后端先改权威口径，或前端改用官方 enriched source | P0 |
| `frontend/src/app/quantevolver/compose/page.tsx:279` | 因子列表 enrichment 用 `cls?.grade` | 评级显示/筛选可能旧 | 用 `official_grade`，旧评级仅作 `legacy_grade` 调试 | P0 |
| `frontend/src/app/quantevolver/compose/page.tsx:541` | `selectedFactorMetrics` 用 `cls?.ic_value/sharpe_value/ann_ret_value/grade` | QE 组合前的指标摘要可能误导 | 用 `ind_ic/ind_rank_ic/ind_sharpe/ind_annual_return/official_grade` | P0 |
| `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx:42` | `ClassifiedFactor` 接口暴露 `ic_value` | UI 数据模型仍旧 | 改为 `official_grade`、`official_score`、`ind_rank_ic_best_abs`、`ind_icir_annualized` | P0 |
| `frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx:705` | 候选因子列表显示 `cf.ic_value` | Multi-Alpha 选因 UI 展示旧 IC | 后端返回并前端展示官方独立 RankIC/Best Abs RankIC | P0 |
| `frontend/src/app/quantevolver/factor-correlation/components/PairDetail.tsx:43` | `FactorClassification.grade` 语义来自 classification | 类型层继续把旧 grade 当作评级 | 改名为 `official_grade`，legacy 字段显式区分 | P0 |
| `frontend/src/app/quantevolver/factor-correlation/components/PairDetail.tsx:348` | 因子对比较显示 `classification.grade` 并用 `judgeGrade()` 判胜 | 相关因子去重/比较会按旧 grade 给优胜提示 | 用官方评级和官方 score；若无官方评级则不判胜 | P0 |
| `frontend/src/app/quantevolver/components/ManualFactorDialog.tsx:243` | 保存成功显示 `result.classification.grade` | 手工因子页面可能显示旧/未落表评级 | 保存后显示 official rating 状态；无评级时提示“待官方评级” | P1 |
| `frontend/src/app/quantevolver/components/ManualFactorDialog.tsx:251` | full pipeline 显示 `result.save.classification.grade` | 与后端手工旧计算链叠加，口径不权威 | 改为展示 official evaluation + official rating 返回 | P1 |
| `frontend/src/app/quantevolver/factor-deletion/page.tsx:30` | 类型字段 `v2_grade/v2_score` | 前端绑定硬编码 v2，不跟 active version | 改为 `official_grade/official_score/rule_version` | P0 |
| `frontend/src/app/quantevolver/factor-deletion/page.tsx:357` | 删除候选页显示 `v2_grade` | active 规则切换后显示错误版本 | 后端 deletion service 改 active 后同步字段名 | P0 |
| `frontend/src/app/quantevolver/components/FactorList.tsx:746` | `grade: f.official_grade ?? f.grade` 仍保留旧 fallback | 当前 `/factors` 基本对齐，但 fallback 可能掩盖后端回归 | 生产路径只接受 `official_grade`；`f.grade` fallback 仅允许兼容迁移并加注释/告警 | P1 |
| `frontend/src/app/quantevolver/components/FactorList.tsx:866` | official summary 失败后 fallback 到 `/factors/independent-metrics-summary` | 该 legacy endpoint 目前内部转 official summary，风险较低 | 可保留短期兼容，但 UI 文案应指向 official endpoint | P2 |

### 2.4 可以继续使用 `qe_factor_classification` 的字段

以下字段仍可作为“分类/画像元数据”，但不能作为评级和量化表现权威：

- 可保留：`category`、`factor_dimension`、`factor_profile`、`holding_period_class`、`data_source_group`、`ts_info_density`、`update_freq`、`linearity`、`direction`、`signal_mechanism`、`sector_exposure_corr`、`cross_horizon_consistency`、`cluster_id/cluster_role/cluster_size`。
- 禁止用于生产筛选、排序、推荐：`grade`、`ic_value`、`sharpe_value`、`ann_ret_value`。
- 如必须返回旧字段，命名必须是 `legacy_grade`、`legacy_ic_value`、`legacy_sharpe_value`、`legacy_ann_ret_value`，且不得参与默认排序、过滤、自动选因或 LLM prompt。

### 2.5 统一改造目标

1. 建一个后端共享查询/DTO，所有列表、推荐、Multi-Alpha、相关性、清洗、删除都使用同一套 active official rating + `qe_eval_v2` metrics 语义。
2. 所有 SQL 中读取 `aistock_factor_metrics` 的生产路径都必须带 `calc_engine = CALC_ENGINE`，不要硬编码字符串，除非迁移脚本专门声明。
3. 所有读取官方评级的生产路径都必须按 active rule version，不得硬编码 `v2.0.0`。
4. 前端统一字段名：评级用 `official_grade/official_score`；独立指标用 `ind_ic/ind_rank_ic/ind_icir/ind_sharpe/ind_annual_return/ind_rank_ic_best_abs`。
5. `/factor-analyst/classifications` 若继续存在，应变成“分类元数据 + 官方评级/指标 enrichment”的兼容端点，而不是旧评级端点。
6. 手工因子与批处理脚本不再直接写 `aistock_factor_metrics`；只允许 official evaluation writer 写官方指标。

### 2.6 建议整改单顺序

| 阶段 | 内容 | 验收标准 |
| --- | --- | --- |
| P0-1 | 先修后端权威查询：`/factor-analyst/classifications`、`/factors`、Multi-Alpha classified/validate/coverage、PortfolioArchitect、QE evolution agents | 任意生产筛选/排序不再引用旧 `classification.grade/ic_value` |
| P0-2 | 修 correlation pair/related、deletion、cleanup | 相关性去重、删除候选、清洗候选全部显示 active official rating 与 `qe_eval_v2` 指标 |
| P0-3 | 修前端 compose、MultiAlphaGroupEditor、PairDetail、factor-deletion | 前端类型和显示字段不再叫 `ic_value`/`v2_grade`，旧字段仅以 legacy 标签出现 |
| P1 | 手工因子 pipeline 改为 official evaluation + official rating | 保存手工因子后，能在 official metrics/rating endpoint 查到结果；无结果时 UI 明确提示待计算 |
| P1 | 清理直接写 `aistock_factor_metrics` 的脚本 | 运行脚本不会污染官方指标表；diagnostic 输出写独立文件或 diagnostic 表 |
| P2 | 增加静态护栏 | CI/pytest 阻断新增运行态 `\\wsl$`、`\\wsl.localhost`、旧 grade 生产决策、未带 `calc_engine` 的 metrics 查询 |

## 3. 静态检查建议

建议新增一个轻量测试或脚本，至少包含以下规则：

```powershell
# 运行态禁止新增 WSL UNC 访问
rg -n -S "\\\\wsl\$|\\\\wsl\.localhost" backend frontend scripts

# 生产路径禁止用旧评级做筛选/排序/推荐
rg -n -S "c\.grade|fc\.grade|classification\.grade|WHERE .*grade IS NOT NULL|ORDER BY .*grade" backend/services/quantevolver backend/routers frontend/src/app/quantevolver

# 生产路径读取 aistock_factor_metrics 必须能解释 calc_engine 口径
rg -n -S "FROM aistock_factor_metrics|JOIN aistock_factor_metrics" backend/services/quantevolver backend/routers
```

白名单应只允许：schema/migration、diagnostic-only 脚本、明确标记 legacy display 的字段、官方 service 自身写入/读取实现。

## 4. 本文档后续用途

- 作为下一轮“问题 5”代码整改的 checklist。
- 作为评审标准：任何因子评级/指标相关 PR 都应说明是否使用 active official rating 与 `CALC_ENGINE`。
- 作为回归测试基线：修复后用同一组 `rg` 命令确认旧路径收敛。
