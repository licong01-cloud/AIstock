# 因子独立指标单写源改造详细设计方案

日期：2026-04-27
范围：AIstock 因子库、官方独立指标计算、月度 IC、分类、评级、LLM 分析、UI 调度。

## 1. 目标

本方案将因子独立指标统一收敛到唯一权威写入源，避免 UI、调度、评级、分类、脚本补录各自写同类字段。

- `aistock_factor_metrics`：所有因子独立指标的唯一权威主表。
- `aistock_factor_monthly_ic`：所有月度 IC 序列和月度派生指标的唯一权威表。
- 因子库 UI 手动计算、后端 API、调度任务必须进入同一条官方指标计算路径。
- 分类、评级、LLM 分析只能写分类、评级、分析结果，不能再重复写独立指标。
- 历史脚本补录逻辑必须产品化到官方指标计算后端，未来不再需要补录脚本。

## 2. 表职责边界

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>表</th><th>职责</th><th>允许写入者</th><th>禁止写入者</th><th>改造要求</th></tr>
  </thead>
  <tbody>
    <tr><td><code>aistock_factor_metrics</code></td><td>多窗口 IC、Rank IC、ICIR、收益、回撤、换手、覆盖率、半衰期、方向、最佳持有期等独立指标</td><td>官方独立指标计算服务的单写入器</td><td>分类、评级、LLM、补录脚本、RD-Agent 同步、手工因子服务</td><td>所有读取端按 <code>calc_engine = qe_eval_v2</code> 读取本表</td></tr>
    <tr><td><code>aistock_factor_monthly_ic</code></td><td>月度 IC 序列、月度 EWMA、12 个月符号一致性、趋势斜率、OOS/IS 比率</td><td>官方独立指标计算服务的单写入器</td><td>分类、评级、LLM、镜像同步脚本</td><td>基础月度字段与派生字段必须同流程写入</td></tr>
    <tr><td><code>qe_factor_classification</code></td><td>语义分类、因子维度、数据源分组、信号机制、描述</td><td>FactorAnalyst 分类服务</td><td>指标计算服务和评级服务写指标字段</td><td>不再保存 IC、Sharpe、年化收益、月度派生指标镜像</td></tr>
    <tr><td><code>qe_factor_official_ratings</code></td><td>官方评级结果、评分维度、硬门禁、评级解释</td><td>FactorRatingService</td><td>指标计算服务、分类服务</td><td>评级可以读指标，但只写评级表</td></tr>
    <tr><td><code>qe_factor_rating_runs</code></td><td>评级批次审计</td><td>FactorRatingService</td><td>其他服务</td><td>记录触发来源、规则版本、范围、执行摘要</td></tr>
  </tbody>
</table>

## 3. 目标调用架构

UI 手动计算与调度任务必须进入同一条后端路径。

```text
因子库 UI / API
  -> POST /api/v1/quantevolver/official-evaluation/compute
  -> FactorOfficialEvaluationService.compute()
  -> IndependentMetricsWriter.write_all()
  -> aistock_factor_metrics
  -> aistock_factor_monthly_ic
```

```text
因子指标调度 UI / API
  -> POST /api/v1/factor-metrics/schedules/{schedule_id}/run
  -> FactorMetricsScheduler.submit_job()
  -> official_evaluation payload
  -> 同一个 FactorOfficialEvaluationService / remote official evaluation worker
  -> 同一个 IndependentMetricsWriter.write_all()
```

建议把当前 `FactorOfficialEvaluationService._save_metrics()` 与 `_save_monthly_ic()` 收敛为显式内部组件 `IndependentMetricsWriter`。如果第一阶段不新增文件，也必须保持“唯一入口、唯一 SQL 写入点、统一校验”。

## 4. 字段归属

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>字段类型</th><th>字段示例</th><th>权威来源</th><th>动作</th></tr>
  </thead>
  <tbody>
    <tr><td>核心 IC</td><td><code>ic_mean</code>、<code>rank_ic_mean</code>、<code>ic_std</code>、<code>rank_ic_std</code>、<code>icir</code>、<code>rank_icir</code></td><td><code>aistock_factor_metrics</code></td><td>继续由官方指标服务写入</td></tr>
    <tr><td>多周期 Rank IC</td><td><code>rank_ic_1d</code>、<code>rank_ic_5d</code>、<code>rank_ic_10d</code>、<code>rank_ic_20d</code></td><td><code>aistock_factor_metrics</code></td><td>最佳持有期和筛选逻辑从本表读取</td></tr>
    <tr><td>年化 ICIR</td><td><code>icir_annualized</code>、<code>rank_icir_annualized</code></td><td><code>aistock_factor_metrics</code></td><td>补齐到官方计算流程，不再脚本补录</td></tr>
    <tr><td>方向和持有期</td><td><code>direction</code>、<code>best_horizon</code>、<code>best_horizon_advantage</code></td><td><code>aistock_factor_metrics</code></td><td>从评级回写迁到指标写入器</td></tr>
    <tr><td>月度基础序列</td><td><code>month_end</code>、<code>ic_mean</code>、<code>rank_ic_mean</code>、<code>ic_std</code>、<code>ic_ewma_6m</code>、<code>n_days</code></td><td><code>aistock_factor_monthly_ic</code></td><td>继续由官方指标服务写入</td></tr>
    <tr><td>月度派生字段</td><td><code>sign_consistency_12m</code>、<code>trend_slope_12m</code>、<code>oos_is_ratio</code></td><td><code>aistock_factor_monthly_ic</code></td><td>把 <code>scripts/backfill_monthly_ic_v2.py</code> 产品化进后端</td></tr>
    <tr><td>分类语义</td><td><code>category</code>、<code>factor_dimension</code>、<code>data_source_group</code>、<code>signal_mechanism</code>、<code>description</code></td><td><code>qe_factor_classification</code></td><td>分类表只保留语义字段</td></tr>
    <tr><td>评级结果</td><td><code>official_grade</code>、<code>official_score</code>、<code>dimension_scores</code>、<code>hard_gate_flags</code></td><td><code>qe_factor_official_ratings</code></td><td>评级服务只写评级表</td></tr>
  </tbody>
</table>

## 5. 当前问题与目标改造

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>路径</th><th>当前行为</th><th>问题</th><th>目标行为</th><th>优先级</th></tr>
  </thead>
  <tbody>
    <tr><td><code>FactorOfficialEvaluationService._save_metrics()</code></td><td>写 <code>aistock_factor_metrics</code>，并更新 catalog 摘要</td><td>未写入年化 ICIR、方向、最佳持有期；按因子删除旧行后重写会清空历史补录字段</td><td>扩展为唯一独立指标写入器，完整写所有指标字段</td><td>P0</td></tr>
    <tr><td><code>FactorOfficialEvaluationService._save_monthly_ic()</code></td><td>只写月度基础 IC 序列</td><td>不写 12 个月符号一致性、趋势斜率、OOS/IS 比率</td><td>同一次指标计算写入基础字段与派生字段</td><td>P0</td></tr>
    <tr><td><code>scripts/backfill_monthly_ic_v2.py</code></td><td>脚本后补月度派生字段</td><td>形成第二写入源，UI 和调度不会自动补齐</td><td>迁入官方指标服务，脚本仅保留为历史迁移工具或删除</td><td>P0</td></tr>
    <tr><td><code>scripts/sync_classification_ic_mirror.py</code></td><td>把月度派生字段镜像到分类表</td><td>分类表保存独立指标副本，形成 stale mirror</td><td>取消镜像，读取端直接读 <code>aistock_factor_monthly_ic</code></td><td>P0</td></tr>
    <tr><td><code>FactorRatingService._writeback_classification_v2()</code></td><td>评级后回写方向、最佳持有期到分类表</td><td>评级服务重复写独立指标派生字段</td><td>评级只写评级表，方向和持有期来自指标表</td><td>P0</td></tr>
    <tr><td><code>FactorAnalyst._upsert_classification()</code></td><td>分类时写 <code>ic_value</code>、<code>sharpe_value</code>、<code>ann_ret_value</code></td><td>分类表重复保存独立指标</td><td>停止写入旧兼容字段，迁移读取端后删除字段</td><td>P0</td></tr>
    <tr><td><code>FactorMetricsScheduler.submit_job()</code></td><td>调度提交官方指标任务</td><td>原路径不能安全只指定 2 个因子，且 UI 路径存在 404 和 job_type 约束问题</td><td>支持 <code>factor_names</code>、测试 <code>schedule_id</code>、UI alias 路由、合法 job_type</td><td>P0</td></tr>
  </tbody>
</table>

## 6. 读取路径迁移

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>旧读取字段</th><th>新来源</th><th>涉及功能</th><th>要求</th></tr>
  </thead>
  <tbody>
    <tr><td><code>qe_factor_classification.ic_value</code></td><td><code>aistock_factor_metrics.ic_mean</code> 或最佳周期 Rank IC</td><td>因子库、多因子筛选、组合构建</td><td>按 <code>calc_engine = qe_eval_v2</code> JOIN 指标主表</td></tr>
    <tr><td><code>qe_factor_classification.sharpe_value</code></td><td><code>aistock_factor_metrics.top_sharpe</code> 或 <code>top_excess_sharpe</code></td><td>排序、质量展示</td><td>读取 full 窗口最新指标</td></tr>
    <tr><td><code>qe_factor_classification.ann_ret_value</code></td><td><code>aistock_factor_metrics.top_annual_return</code> 或 <code>top_excess_annual_return</code></td><td>质量展示、评级辅助</td><td>停止从分类表读取</td></tr>
    <tr><td><code>qe_factor_classification.direction</code></td><td><code>aistock_factor_metrics.direction</code></td><td>因子详情、评级、筛选</td><td>方向由独立指标计算确定</td></tr>
    <tr><td><code>qe_factor_classification.best_horizon</code></td><td><code>aistock_factor_metrics.best_horizon</code></td><td>因子详情、评级、持有期分类</td><td>最佳周期由指标引擎或写入器派生</td></tr>
    <tr><td><code>qe_factor_classification.ic_sign_consistency_12m</code></td><td><code>aistock_factor_monthly_ic.sign_consistency_12m</code> 最新月</td><td>稳定性、删除候选、评级</td><td>取消镜像读取</td></tr>
    <tr><td><code>qe_factor_classification.monthly_ic_trend_slope</code></td><td><code>aistock_factor_monthly_ic.trend_slope_12m</code> 最新月</td><td>趋势展示、删除候选、评级</td><td>取消镜像读取</td></tr>
    <tr><td><code>qe_factor_classification.ic_oos_is_ratio</code></td><td><code>aistock_factor_monthly_ic.oos_is_ratio</code> 最新月</td><td>过拟合门禁、评级、稳定性</td><td>评级直接读取月度指标表</td></tr>
  </tbody>
</table>

## 7. 产品化脚本补录指标

### 7.1 月度派生字段

将 `scripts/backfill_monthly_ic_v2.py` 迁入后端纯函数模块，例如 `backend/services/quantevolver/monthly_ic_derived_metrics.py`。

- `sign_consistency_12m`：最近 12 个月 IC 符号与 12 个月均值符号一致比例。
- `trend_slope_12m`：最近 12 个月 IC 的 Theil-Sen 趋势斜率。
- `oos_is_ratio`：最近 6 个月均值 / 前 6 个月均值。

写入要求：`_save_monthly_ic()` 在写入当前因子的完整月度序列前计算派生字段，并在同一 SQL 中写入基础字段和派生字段。

### 7.2 年化 ICIR、方向、最佳持有期

将以下字段迁入 `aistock_factor_metrics` 单写入器：

- `icir_annualized`
- `rank_icir_annualized`
- `direction`
- `best_horizon`
- `best_horizon_advantage`

评级服务只能读取这些字段，不得再回写分类表。

## 8. 调度统一路径设计

调度任务支持两种模式：

- 全量模式：`factor_names = null`，计算全部 eligible 因子。
- 指定因子模式：`factor_names = ["factor_a", "factor_b"]`，只计算给定因子。

安全原则：

- 测试调度必须使用显式测试 UUID，`enabled = false`，`one_shot = true`。
- 手动触发测试调度后删除 schedule row。
- 不停止、不重启、不管理生产端口 `8001` 和 `3000`。
- 测试端口使用 `8011` 或 `8012`；如需要前端，使用 `3011` 或 `3012`。

## 9. 实施阶段

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>阶段</th><th>目标</th><th>主要改动</th><th>验收标准</th></tr>
  </thead>
  <tbody>
    <tr><td>Phase 0</td><td>安全验证</td><td>补齐 2 因子调度参数、UI alias、job_type 兼容</td><td>2 因子 direct compute、分类、评级、调度触发可审计</td></tr>
    <tr><td>Phase 1</td><td>单写入器抽象</td><td>抽出 <code>IndependentMetricsWriter</code> 或等效模块</td><td>生产写入独立指标只有一个代码入口</td></tr>
    <tr><td>Phase 2</td><td>补齐字段</td><td>内置年化 ICIR、方向、最佳持有期、月度派生字段</td><td>UI 和调度计算后所有权威字段完整更新</td></tr>
    <tr><td>Phase 3</td><td>清理重复写</td><td>停止分类写旧指标，停止评级回写分类表指标字段</td><td>分类表只保存分类语义，评级表只保存评级结果</td></tr>
    <tr><td>Phase 4</td><td>读取端迁移</td><td>因子库、多因子选择、删除候选、评级读取改到权威表</td><td>静态扫描无旧字段读取</td></tr>
    <tr><td>Phase 5</td><td>数据库清理</td><td>备份后删除旧兼容字段或标记弃用</td><td>无代码依赖旧字段，迁移可回滚</td></tr>
  </tbody>
</table>

## 10. 验证方案

1. 选择 2 个已入库且 eligible 的因子。
2. 启动测试后端 `127.0.0.1:8011`，并禁用 ingestion、strategy、paper trading、node health、HMM、evolution scanner 等后台调度。
3. 调用 `POST /api/v1/quantevolver/official-evaluation/compute` 计算 2 个因子。
4. 调用 `POST /api/v1/quantevolver/pipeline/full-stream` 验证分类和评级。
5. 调用 `POST /api/v1/factor-metrics/schedules` 创建测试禁用调度。
6. 调用 `POST /api/v1/factor-metrics/schedules/{schedule_id}/run` 手动触发调度。
7. 轮询 `GET /api/v1/factor-metrics/jobs`。
8. 校验 `aistock_factor_metrics`、`aistock_factor_monthly_ic`、`qe_factor_classification`、`qe_factor_official_ratings`。

## 11. 风险与回滚

- 当前指标计算会删除并重写指定因子的指标行；验证必须严格限制在 2 个因子。
- 调度测试不能覆盖默认生产调度；必须使用测试 UUID 和 `enabled = false`。
- 如果 dispatch 节点不可用，不能切换到生产端口验证。
- 删除旧字段前必须完成读取端迁移和备份。
- 评级规则版本需要保持唯一 active，避免评级结果选择不一致。

## 12. 改造前验证记录

验证时间：2026-04-27 19:56-20:06（Asia/Shanghai）。

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>验证项</th><th>结果</th></tr>
  </thead>
  <tbody>
    <tr><td>测试后端</td><td><code>http://127.0.0.1:8011</code></td></tr>
    <tr><td>生产服务保护</td><td><code>8001</code> 和 <code>3000</code> 未停止、未重启、未管理</td></tr>
    <tr><td>测试因子</td><td><code>BookToPrice_Ratio</code>、<code>ChipProfitPressureFactor</code></td></tr>
    <tr><td>数据快照</td><td><code>20260410</code>，<code>snapshot_date = 2026-04-10</code></td></tr>
    <tr><td>直接指标计算</td><td>dispatch_task_id=<code>97c110ef-6a59-4c4d-86a7-e065ba47dbae</code>，remote_task_id=<code>223</code>，2/2 成功，写入 10 行</td></tr>
    <tr><td>分类 + 评级</td><td>run_id=<code>bd2c5dd5-640b-4e11-abc9-08687744c34a</code>，v2.0.0，2/2 成功；两个因子评级均为 C</td></tr>
    <tr><td>调度任务</td><td>schedule_id=<code>42c7e775-a6f5-432a-9da6-71d80c39774b</code>，job_id=<code>00639346-4786-426d-87fe-9a547eb4d587</code>，dispatch_task_id=<code>2363e83e-9ae1-4081-8b90-5b6ce8daca71</code>，remote_task_id=<code>224</code>，状态 success，写入 10 行</td></tr>
    <tr><td>测试调度清理</td><td>测试 schedule row 已删除；job 审计保留</td></tr>
  </tbody>
</table>

## 13. 改造前验证结论

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>检查项</th><th>BookToPrice_Ratio</th><th>ChipProfitPressureFactor</th><th>结论</th></tr>
  </thead>
  <tbody>
    <tr><td><code>aistock_factor_metrics</code> 行数</td><td>5</td><td>5</td><td>通过</td></tr>
    <tr><td>最后指标写入时间</td><td>2026-04-27 20:05:22+08</td><td>2026-04-27 20:05:39+08</td><td>通过</td></tr>
    <tr><td><code>aistock_factor_monthly_ic</code> 行数</td><td>92</td><td>92</td><td>通过</td></tr>
    <tr><td>最新月度派生字段</td><td><code>NULL</code></td><td><code>NULL</code></td><td>不通过：当前 UI/调度计算未写入派生字段</td></tr>
    <tr><td>年化 ICIR、最佳持有期字段</td><td>非空数 0</td><td>非空数 0</td><td>不通过：当前官方写入器未写入这些字段</td></tr>
    <tr><td><code>qe_factor_classification</code></td><td>分类存在，旧镜像字段仍有历史值</td><td>分类存在，旧镜像字段仍有历史值</td><td>功能通过，但暴露 stale mirror 风险</td></tr>
    <tr><td><code>qe_factor_official_ratings</code></td><td>C / 57.51</td><td>C / 55.76</td><td>通过</td></tr>
  </tbody>
</table>

结论：基础独立指标计算、分类、评级、UI 调度后端触发均已通过测试端口验证；但“所有字段都由同一流程完整写入”的目标尚未达成。当前最关键缺口是月度派生字段、年化 ICIR、方向、最佳持有期仍没有在官方指标写入器中统一写入，且分类表镜像字段会保留旧值并与权威月度表不一致。下一阶段必须优先实施 Phase 1 和 Phase 2。

## 14. 2026-04-27 修复实施结果

本轮修复先不做破坏性 DDL，不删除历史兼容字段；代码层面把 UI/API/调度的产品化写入路径收敛到官方独立指标服务，并补齐之前缺失的字段。

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>文件</th><th>改动</th><th>结果</th></tr>
  </thead>
  <tbody>
    <tr><td><code>backend/services/quantevolver/factor_official_evaluation_service.py</code></td><td>扩展 <code>aistock_factor_metrics</code> UPSERT，写入 <code>icir_annualized</code>、<code>rank_icir_annualized</code>、<code>direction</code>、<code>best_horizon</code>、<code>best_horizon_advantage</code>；同一因子的 5 个窗口统一使用 full 窗口派生出的方向和最佳持有期。</td><td>官方独立指标计算一次写完整指标主表字段。</td></tr>
    <tr><td><code>backend/services/quantevolver/factor_official_evaluation_service.py</code></td><td>把 <code>scripts/backfill_monthly_ic_v2.py</code> 的 12 个月派生逻辑产品化到 <code>_save_monthly_ic()</code>，用纯 Python 计算符号一致性、Theil-Sen 中位数斜率、OOS/IS 比率。</td><td><code>aistock_factor_monthly_ic</code> 基础月度序列和派生字段同流程写入，不再依赖补录脚本。</td></tr>
    <tr><td><code>backend/services/quantevolver/factor_rating_service.py</code></td><td>评级读取 <code>aistock_factor_metrics.direction</code> 和 <code>aistock_factor_monthly_ic</code> 最新月派生字段；移除评级后回写分类表方向/最佳持有期的动作。</td><td>评级服务只写评级表，不再重复写独立指标镜像。</td></tr>
    <tr><td><code>backend/services/quantevolver/factor_analyst.py</code></td><td>分类 UPSERT 不再更新 <code>ic_value</code>、<code>sharpe_value</code>、<code>ann_ret_value</code>、<code>direction</code> 旧兼容字段；新插入行这些旧字段保持 <code>NULL</code>。</td><td>分类服务不再新增独立指标副本。</td></tr>
    <tr><td><code>backend/routers/quantevolver.py</code></td><td>因子库列表改为从 <code>aistock_factor_metrics</code> 和 <code>aistock_factor_monthly_ic</code> 返回方向、最佳持有期、月度派生字段；多因子分类列表和分组统计改为读取正式指标表。</td><td>因子库 UI 后端展示路径使用权威指标来源。</td></tr>
    <tr><td><code>backend/services/quantevolver/multi_alpha_selector.py</code></td><td>多因子候选筛选中的 <code>ic_value</code> 改为来自 <code>aistock_factor_metrics.ic_mean</code>。</td><td>核心筛选不再依赖分类表旧 IC 镜像。</td></tr>
  </tbody>
</table>

## 18. 2026-04-28 UI 后台验证与补充修复

验证时间：2026-04-28 00:09-00:19（Asia/Shanghai）。验证使用测试后端 <code>127.0.0.1:8013</code> 和测试前端 <code>127.0.0.1:3011</code>；生产后端 <code>8001</code> 和生产前端 <code>3000</code> 未停止、未重启、未管理。

### 18.1 验证中发现并修复的问题

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>问题</th><th>影响</th><th>修复</th><th>状态</th></tr>
  </thead>
  <tbody>
    <tr><td>本地数据页“因子指标调度”UI 只能创建全量调度</td><td>无法在 UI 中安全指定 2 个测试因子；容易误触发全量指标计算</td><td>新增“指定因子”输入框，支持逗号、空格、换行分隔，并把 <code>factor_names</code> 传给 <code>/api/v1/factor-metrics/schedules</code></td><td>已修复</td></tr>
    <tr><td>调度 UI 未暴露 <code>workers</code> 和创建后启用状态</td><td>无法通过 UI 控制测试并行度，也不便创建 <code>enabled=false</code> 的安全测试调度</td><td>新增“并行度”和“创建后启用”控件；验证时使用 <code>workers=2</code>、<code>enabled=false</code>、<code>one_shot=true</code></td><td>已修复</td></tr>
    <tr><td>调度列表无法直观看到当前调度因子范围</td><td>难以确认 UI 创建的是 2 因子调度还是全量调度</td><td>列表新增“因子”和“并行度”列，显示 <code>options.factor_names</code> 与 <code>options.workers</code></td><td>已修复</td></tr>
    <tr><td>后台 UI 自动化缺少稳定选择器</td><td>测试端口下难以稳定验证具体 tab 和调度按钮</td><td>为本地数据 tab、调度表单、立即运行按钮增加 <code>data-testid</code></td><td>已修复</td></tr>
  </tbody>
</table>

### 18.2 UI/浏览器上下文验证记录

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>验证项</th><th>证据</th><th>结果</th></tr>
  </thead>
  <tbody>
    <tr><td>因子库 UI 触发独立指标计算</td><td>Playwright 打开 <code>/quantevolver/factors</code>，选择快照 <code>20260410</code>，勾选 <code>BookToPrice_Ratio</code> 和 <code>ChipProfitPressureFactor</code>，点击“计算指标(2)”并确认弹窗。</td><td>成功；后端返回指标获取完成，API 验证两个因子的方向、最佳周期、月度派生字段均非空。</td></tr>
    <tr><td>浏览器上下文触发分类 + 评级流水线</td><td>在前端页面上下文请求 <code>POST /api/v1/quantevolver/pipeline/full-stream</code>，<code>enable_llm_analysis=false</code>、<code>enable_llm_audit=false</code>、<code>parallelism=2</code>。</td><td>成功；run_id=<code>1f860edc-41cc-492d-bdf4-00c69f2c796f</code>，2/2 成功，评级为 B / 68.72 和 B / 68.76。</td></tr>
    <tr><td>本地数据 UI 创建 2 因子调度</td><td>Playwright 打开 <code>/local-data</code>，进入“因子指标调度”，创建 <code>one_shot=true</code>、<code>enabled=false</code>、<code>workers=2</code>、<code>factor_names=[BookToPrice_Ratio, ChipProfitPressureFactor]</code> 的调度。</td><td>成功；schedule_id=<code>db4d4b7a-9bfa-4de6-8a67-f7a4bd4a9480</code>。</td></tr>
    <tr><td>本地数据 UI 手动触发调度</td><td>点击 UI “立即运行”，弹窗返回 job_id=<code>c1a8ea89-b928-476b-86f7-782168e854c8</code>；轮询 <code>/api/v1/factor-metrics/jobs</code>。</td><td>成功；dispatch_task_id=<code>0857147f-31b2-43ff-9439-c590f3952d80</code>，remote_task_id=<code>231</code>，最终状态 <code>success</code>。</td></tr>
    <tr><td>测试调度清理</td><td>调度成功后删除 <code>schedule_id=db4d4b7a-9bfa-4de6-8a67-f7a4bd4a9480</code>。</td><td>成功；调度行已删除，job 审计保留。</td></tr>
  </tbody>
</table>

### 18.3 数据库字段复核

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>检查项</th><th>BookToPrice_Ratio</th><th>ChipProfitPressureFactor</th><th>结论</th></tr>
  </thead>
  <tbody>
    <tr><td><code>aistock_factor_metrics</code> 行数</td><td>5</td><td>5</td><td>通过</td></tr>
    <tr><td><code>icir_annualized</code> / <code>rank_icir_annualized</code> 非空数</td><td>5/5 / 5/5</td><td>5/5 / 5/5</td><td>通过</td></tr>
    <tr><td><code>best_horizon</code> / <code>best_horizon_advantage</code> / <code>direction</code> 非空数</td><td>5/5 / 5/5 / 5/5</td><td>5/5 / 5/5 / 5/5</td><td>通过</td></tr>
    <tr><td>最近指标写入时间</td><td>2026-04-28 00:19:14+08</td><td>2026-04-28 00:19:28+08</td><td>通过；来自 UI 调度触发的最新 official evaluation</td></tr>
    <tr><td><code>aistock_factor_monthly_ic</code> 行数</td><td>92</td><td>92</td><td>通过</td></tr>
    <tr><td>月度派生字段非空数</td><td><code>sign_consistency_12m</code> 81/92，<code>trend_slope_12m</code> 81/92，<code>oos_is_ratio</code> 81/92</td><td><code>sign_consistency_12m</code> 81/92，<code>trend_slope_12m</code> 81/92，<code>oos_is_ratio</code> 81/92</td><td>通过；前 11 个月按设计为 <code>NULL</code></td></tr>
    <tr><td>最新月度派生字段</td><td>2026-03：0.5833333333333334，-0.002763953001832495，0.3322045591859235</td><td>2026-03：0.75，0.0008031871622123248，1.0587418692652628</td><td>通过</td></tr>
    <tr><td>最新官方评级</td><td>B / 68.72 / v2.0.0</td><td>B / 68.76 / v2.0.0</td><td>通过</td></tr>
  </tbody>
</table>

结论：独立指标计算、分类、评级、因子库 UI 触发、调度 UI 创建与手动运行均已在测试端口闭环验证。所有本轮关注的权威字段都由 official evaluation 路径写入；分类、评级、LLM 流程不再重复写入独立指标数据。

## 15. 修复后验证记录

验证时间：2026-04-27 20:28-20:35（Asia/Shanghai）。验证只使用测试后端 <code>127.0.0.1:8011</code>；生产后端 <code>8001</code> 和前端 <code>3000</code> 未停止、未重启、未管理。

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>验证项</th><th>结果</th></tr>
  </thead>
  <tbody>
    <tr><td>编译检查</td><td><code>python -m py_compile</code> 通过：官方指标服务、评级服务、因子库路由、调度路由、分类服务、多因子选择服务。</td></tr>
    <tr><td>静态写入口扫描</td><td>产品化路径中 <code>aistock_factor_metrics</code> 和 <code>aistock_factor_monthly_ic</code> 的写入只剩 <code>FactorOfficialEvaluationService</code>；因子删除 API 只保留删除清理语句。</td></tr>
    <tr><td>旧镜像写入扫描</td><td>分类/评级路径未发现 <code>ic_value = EXCLUDED</code>、<code>sharpe_value = EXCLUDED</code>、<code>ann_ret_value = EXCLUDED</code>、<code>direction = COALESCE(EXCLUDED.direction,...)</code> 等旧镜像更新语句。</td></tr>
    <tr><td>直接官方指标计算</td><td><code>POST /api/v1/quantevolver/official-evaluation/compute</code>；dispatch_task_id=<code>66048434-bf87-4113-addc-e867f283158c</code>，remote_task_id=<code>226</code>，2/2 成功，写入 10 行。</td></tr>
    <tr><td>分类 + 评级流水线</td><td><code>POST /api/v1/quantevolver/pipeline/full-stream</code>；run_id=<code>6e5f3a41-cd5f-451f-92de-b9263c942e10</code>，v2.0.0，LLM 分析/审计关闭，2/2 成功。</td></tr>
    <tr><td>UI 调度后端路径</td><td>测试 schedule_id=<code>533f65aa-091c-4888-ba24-fd4b92e7eaed</code>，job_id=<code>8c800215-e652-4b5f-b67a-7b1f0d36772b</code>，dispatch_task_id=<code>497e3c42-5d99-4b37-8acc-bbd161bc971c</code>，remote_task_id=<code>227</code>，状态 <code>success</code>，写入 10 行。</td></tr>
    <tr><td>测试调度清理</td><td>测试 schedule row 已删除；<code>market.ingestion_jobs</code> 审计保留。</td></tr>
  </tbody>
</table>

## 16. 修复后字段验证

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>检查项</th><th>BookToPrice_Ratio</th><th>ChipProfitPressureFactor</th><th>结论</th></tr>
  </thead>
  <tbody>
    <tr><td><code>aistock_factor_metrics</code> 行数</td><td>5</td><td>5</td><td>通过</td></tr>
    <tr><td><code>icir_annualized</code> 非空数</td><td>5/5</td><td>5/5</td><td>通过</td></tr>
    <tr><td><code>rank_icir_annualized</code> 非空数</td><td>5/5</td><td>5/5</td><td>通过</td></tr>
    <tr><td><code>best_horizon</code> 非空数</td><td>5/5，值为 20</td><td>5/5，值为 20</td><td>通过</td></tr>
    <tr><td><code>best_horizon_advantage</code> 非空数</td><td>5/5，值为 0.008739510130940847</td><td>5/5，值为 0.0077255957651148</td><td>通过</td></tr>
    <tr><td><code>direction</code> 非空数</td><td>5/5，值为 1</td><td>5/5，值为 -1</td><td>通过</td></tr>
    <tr><td><code>aistock_factor_monthly_ic</code> 行数</td><td>92</td><td>92</td><td>通过</td></tr>
    <tr><td>月度派生字段非空数</td><td><code>sign_consistency_12m</code> 81/92，<code>trend_slope_12m</code> 81/92，<code>oos_is_ratio</code> 81/92</td><td><code>sign_consistency_12m</code> 81/92，<code>trend_slope_12m</code> 81/92，<code>oos_is_ratio</code> 81/92</td><td>通过；前 11 个月按设计为 <code>NULL</code></td></tr>
    <tr><td>最新月度派生字段</td><td>2026-03：0.5833333333333334，-0.002763953001832495，0.3322045591859235</td><td>2026-03：0.75，0.0008031871622123248，1.0587418692652628</td><td>通过</td></tr>
    <tr><td>修复后评级</td><td>B / 68.72</td><td>B / 68.76</td><td>通过；评级已使用正式指标/月度派生字段</td></tr>
    <tr><td>因子库 UI 后端返回</td><td>方向 1、最佳周期 20、月度派生字段非空</td><td>方向 -1、最佳周期 20、月度派生字段非空</td><td>通过</td></tr>
  </tbody>
</table>

## 17. 当前结论与剩余事项

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr><th>事项</th><th>状态</th><th>说明</th></tr>
  </thead>
  <tbody>
    <tr><td>独立指标产品化单写入口</td><td>已达成</td><td>UI/API 直接计算和 UI 调度后端路径均进入 official evaluation，并由 <code>FactorOfficialEvaluationService</code> 写 <code>aistock_factor_metrics</code> 与 <code>aistock_factor_monthly_ic</code>。</td></tr>
    <tr><td>脚本补录产品化</td><td>已达成</td><td>月度 12M 派生字段已并入官方指标写入流程；后续不需要运行 <code>scripts/backfill_monthly_ic_v2.py</code> 补齐这三个字段。</td></tr>
    <tr><td>分类/评级重复写指标</td><td>已停止</td><td>评级不再回写分类表指标镜像；分类不再更新旧 IC、Sharpe、年化收益、方向兼容字段。</td></tr>
    <tr><td>旧兼容字段</td><td>暂未删除</td><td><code>qe_factor_classification</code> 中历史兼容字段仍可能保留旧值；本轮不做破坏性 DDL。后续要删除字段前，需先完成全量读取端迁移扫描、备份和回滚方案。</td></tr>
    <tr><td>历史/诊断脚本</td><td>保留但不作为产品路径</td><td><code>scripts/backfill_monthly_ic_v2.py</code>、<code>scripts/sync_classification_ic_mirror.py</code> 等应视为历史迁移/诊断工具，不再纳入 UI 或调度流程。</td></tr>
  </tbody>
</table>
