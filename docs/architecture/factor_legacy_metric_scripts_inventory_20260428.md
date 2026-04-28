# 因子指标旧脚本停用清单（需用户确认后删除）

更新日期：2026-04-28

## 执行原则

- RD-Agent task/loop 回测阶段指标可以继续记录在 task、loop、experiment 等历史记录表中，但不得写入任何因子独立指标表或因子 catalog 指标字段。
- QE 实验中的因子表现可以作为实验历史表现记录保存，例如 `qe_factor_experiment_metrics`、`qe_loop_factor_records` 等；这些记录不得作为权威独立指标来源。
- 因子独立指标的权威来源只允许是官方独立指标流程写入的 `aistock_factor_metrics` 和 `aistock_factor_monthly_ic`。
- 分类、评级、LLM 分析只能写各自对应表；不得再镜像或补录独立指标字段。
- 本文只列出待确认停用/删除或诊断-only 脚本；本轮未删除任何脚本。

## 待确认删除或永久停用的旧补录/旧写入脚本

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>脚本</th>
      <th>原作用</th>
      <th>为什么不再需要/不得再使用</th>
      <th>建议处理</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>scripts/backfill_monthly_ic_v2.py</code></td>
      <td>补录 <code>aistock_factor_monthly_ic</code> 的 <code>sign_consistency_12m</code>、<code>trend_slope_12m</code>、<code>oos_is_ratio</code>。</td>
      <td>月度派生字段已产品化到官方独立指标计算流程；UI 和调度计算时应同流程写入，不再需要后补。</td>
      <td>确认无历史迁移需求后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/sync_classification_ic_mirror.py</code></td>
      <td>把月度独立指标镜像到 <code>qe_factor_classification</code>。</td>
      <td>分类表不得保存独立指标镜像；读取端应直接读 <code>aistock_factor_monthly_ic</code>。</td>
      <td>建议删除。</td>
    </tr>
    <tr>
      <td><code>scripts/p1c_batch_update_holding_period_class.py</code></td>
      <td>从 <code>aistock_factor_metrics.ic_decay_half_life</code> 批量补录分类表 <code>holding_period_class</code>。</td>
      <td>持有期/方向等派生口径应由官方指标和分类/评级流程统一产生，不应脚本后补。</td>
      <td>确认分类流水线覆盖后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/batch_fill_multi_alpha_dimensions.py</code></td>
      <td>批量补录 <code>data_source_group</code>、<code>update_freq</code>、<code>linearity</code> 等分类维度。</td>
      <td>这些属于分类维度补录，不应作为常规生产脚本；应由 FactorAnalyst/统一流水线写分类表。</td>
      <td>确认统一流水线覆盖后删除或迁移为一次性 migration。</td>
    </tr>
    <tr>
      <td><code>scripts/_backfill_v2_deterministic.py</code></td>
      <td>批量回填确定性分类字段，例如 <code>ts_info_density</code>、<code>cross_horizon_consistency</code>。</td>
      <td>确定性分类字段应由分类流程写入；脚本补录会形成第二写入口。</td>
      <td>确认无历史回填需求后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/migrate_factor_rating_v2.py</code></td>
      <td>评级 v2 schema 迁移，并回填 <code>aistock_factor_metrics.direction</code>。</td>
      <td>schema 已迁移后不应重复运行；<code>direction</code> 已由官方独立指标流程写入。</td>
      <td>保留为历史 migration 备份或归档，禁止常规运行。</td>
    </tr>
    <tr>
      <td><code>scripts/batch_develop_factors_v2.py</code></td>
      <td>旧批量开发流程，会直接删除/插入 <code>aistock_factor_metrics</code>。</td>
      <td>直接写权威指标表，绕过官方单写入口，可能污染 <code>qe_eval_v2</code> 权威指标。</td>
      <td>删除或改为只调用官方独立指标 API。</td>
    </tr>
    <tr>
      <td><code>scripts/optimize_timeout_factors.py</code></td>
      <td>旧超时因子优化流程，会直接删除/插入 <code>aistock_factor_metrics</code>。</td>
      <td>直接写权威指标表，违反独立指标单写入口。</td>
      <td>删除或改为只调用官方独立指标 API。</td>
    </tr>
    <tr>
      <td><code>scripts/compute_factor_metrics_unified.py</code></td>
      <td>旧 WSL 指标计算脚本，供旧批量/手工流程计算指标 JSON。</td>
      <td>不能再作为独立指标落库来源；官方独立指标计算已产品化。若仍被手工验证调用，只能作为临时诊断输出，不能写库。</td>
      <td>先确认手工因子验证链路是否仍依赖，再决定删除或改名 diagnostic-only。</td>
    </tr>
    <tr>
      <td><code>backend/scripts/batch_factor_metrics_sync.py</code></td>
      <td>旧 RD-Agent task/loop 因子指标同步脚本。</td>
      <td>当前实现已 no-op，不再写 <code>aistock_factor_metrics</code>；task/loop 指标应保存在 task/loop 相关表。</td>
      <td>确认无人手工运行后删除。</td>
    </tr>
    <tr>
      <td><code>backend/scripts/restore_task_metrics.py</code></td>
      <td>从 <code>performance_metrics</code> 把 task 指标回填到 <code>aistock_factor_catalog.ic/sharpe/annualized_return</code>。</td>
      <td>明确违反“RD-Agent task 指标不得写入因子表”的原则。</td>
      <td>建议删除。</td>
    </tr>
  </tbody>
</table>

## 旧清洗/聚类执行脚本（不再作为生产入口）

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>脚本</th>
      <th>原作用</th>
      <th>替代方式</th>
      <th>建议处理</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>scripts/_factor_cleanup_noise_negcorr.py</code></td>
      <td>按噪声/反向相关规则扫描并可执行禁用。</td>
      <td><code>FactorCleanupService</code> + 因子库 UI 清洗弹窗。</td>
      <td>确认 UI 清洗 preview/execute 覆盖后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/_factor_cleanup_execute.py</code></td>
      <td>按旧规则执行清洗禁用。</td>
      <td><code>/quantevolver/factors/cleanup/execute</code>。</td>
      <td>确认后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/_factor_cleanup_rollback.py</code></td>
      <td>回滚旧清洗批次并清理分类聚类字段。</td>
      <td><code>/quantevolver/factors/cleanup/rollback</code>，但分类聚类字段是否清理需单独确认。</td>
      <td>先保留，确认回滚语义后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/_factor_cleanup_candidates.py</code></td>
      <td>导出旧清洗候选 CSV。</td>
      <td>因子库 UI 清洗 preview 或删除候选页面。</td>
      <td>确认后删除。</td>
    </tr>
    <tr>
      <td><code>scripts/_factor_cluster_compute.py</code></td>
      <td>计算因子相关性聚类并写回 <code>qe_factor_classification</code>。</td>
      <td>该能力是否已完全产品化仍需单独确认；不要直接删除。</td>
      <td>暂保留，后续单独设计 UI/API 产品化。</td>
    </tr>
  </tbody>
</table>

## 诊断-only 脚本（可保留，但不能作为补录/生产写入入口）

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>脚本</th>
      <th>当前定位</th>
      <th>后续要求</th>
    </tr>
  </thead>
  <tbody>
    <tr><td><code>scripts/analyze_factor_library.py</code></td><td>因子库分析报告。</td><td>若继续使用，应限定 <code>calc_engine='qe_eval_v2'</code> 或标明 diagnostic-only。</td></tr>
    <tr><td><code>scripts/diagnose_factor_library.py</code></td><td>因子库诊断报告。</td><td>同上。</td></tr>
    <tr><td><code>scripts/list_all_factor_issues.py</code></td><td>列出因子库问题。</td><td>同上。</td></tr>
    <tr><td><code>scripts/optimize_factor_library_p0.py</code></td><td>旧 P0 优化/分析脚本。</td><td>不得直接作为生产决策入口；如保留需限定权威指标。</td></tr>
    <tr><td><code>scripts/p0_half_life_analysis.py</code></td><td>半衰期分析。</td><td>诊断-only。</td></tr>
    <tr><td><code>scripts/p0b_investigate_null_half_life.py</code></td><td>半衰期空值排查。</td><td>诊断-only。</td></tr>
    <tr><td><code>scripts/_dry_grade_v2_overfit.py</code></td><td>评级/过拟合 dry-run。</td><td>不得写库；若读指标需限定权威引擎。</td></tr>
    <tr><td><code>scripts/_inspect_factor_metrics_schema.py</code></td><td>指标表 schema/数据检查。</td><td>诊断-only。</td></tr>
    <tr><td><code>scripts/_inspect_monthly_ic.py</code></td><td>月度 IC schema/覆盖检查。</td><td>诊断-only。</td></tr>
    <tr><td><code>scripts/_scan_neg_corr.py</code></td><td>负相关扫描。</td><td>诊断-only；生产入口应使用 UI/API。</td></tr>
    <tr><td><code>scripts/_scan_rule_b_v2.py</code></td><td>旧噪声规则扫描。</td><td>诊断-only；生产入口应使用 UI/API。</td></tr>
    <tr><td><code>scripts/_scan_leaky_noise.py</code></td><td>疑似泄漏/噪声扫描。</td><td>诊断-only。</td></tr>
    <tr><td><code>scripts/_probe_metric_schema.py</code></td><td>指标表探针。</td><td>诊断-only。</td></tr>
  </tbody>
</table>

## 本轮已落实的代码侧保护

- 旧 IC 衰变 endpoint 读取 <code>aistock_factor_metrics</code> 时已限定 <code>calc_engine = CALC_ENGINE</code>。
- 删除候选服务读取 <code>aistock_factor_metrics</code> 时已限定 <code>calc_engine = CALC_ENGINE</code>，并从 active rule version 读取正式评级。
- 因子清洗服务读取 <code>aistock_factor_metrics</code> 时已限定 <code>calc_engine = CALC_ENGINE</code>，并从 active rule version 读取正式评级。
- 新增静态测试，阻止生产运行目录新增未限定 <code>calc_engine</code> 的 <code>aistock_factor_metrics</code> 读取。
