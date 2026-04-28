# 因子权威口径与运行路径硬编码整改方案（Phase 2）

更新日期：2026-04-29

> 状态：执行中。用户已确认其他窗口提交完成；本阶段在测试端口验证后提交代码，不触碰生产端口 `8001`。

## 1. 背景与目标

前一阶段已经修复生产运行路径中部分 `aistock_factor_metrics` 读取未限定 `calc_engine` 的问题，并增加了静态护栏。本阶段需要继续收敛以下风险：

- 当前生产决策、筛选、排序、LLM prompt 不得再使用 `qe_factor_classification.grade` 旧评级。
- Multi-Alpha、组合构建、QE 演进等路径不得继续混用旧字段名和旧强度口径。
- 前端不得再回退展示旧评级和旧独立指标字段。
- 手工因子 full pipeline 必须进入官方独立指标计算 + 官方评级闭环，不能出现 UI 显示“计算完成”但权威表未落库。
- 运行态不得硬编码本机路径、WSL 个人路径、个人用户名、个人工程目录、数据库密码 fallback。
- 所有补录脚本必须由用户逐项确认后再删除；本方案不直接删除脚本。

## 2. 不变原则

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>原则</th>
      <th>要求</th>
      <th>禁止事项</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>独立指标唯一权威来源</td>
      <td><code>aistock_factor_metrics</code> 与 <code>aistock_factor_monthly_ic</code> 只由官方独立指标流程写入。</td>
      <td>分类、评级、LLM、补录脚本、RD-Agent 同步不得写独立指标字段。</td>
    </tr>
    <tr>
      <td>当前评级唯一权威来源</td>
      <td>生产筛选、排序、prompt 使用 active <code>qe_factor_official_ratings.official_grade/official_score</code>。</td>
      <td>不得用 <code>qe_factor_classification.grade</code> 做生产决策。</td>
    </tr>
    <tr>
      <td>RD-Agent task/loop 指标边界</td>
      <td>task/loop 回测阶段指标可记录在 task、loop、experiment 相关历史表中。</td>
      <td>不得写入任何因子表，不得作为当前因子独立指标。</td>
    </tr>
    <tr>
      <td>QE 实验表现边界</td>
      <td>QE 实验中的因子表现可作为实验历史表现记录保存。</td>
      <td>不得把实验表现字段伪装成权威独立指标。</td>
    </tr>
    <tr>
      <td>路径与密钥配置</td>
      <td>运行路径来自 compute node 配置、环境变量、StrategyPackage artifact、DB/对象存储配置。</td>
      <td>不得在生产代码中硬编码 <code>F:/Dev</code>、<code>/mnt/f</code>、<code>/home/lc999</code>、个人用户名、默认数据库密码。</td>
    </tr>
  </tbody>
</table>

## 10. 2026-04-29 验证记录

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>验证层级</th>
      <th>命令/端口</th>
      <th>结果</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Python 编译</td>
      <td><code>python -m py_compile</code> 覆盖本阶段修改的 backend/router/service/test 文件。</td>
      <td>通过。</td>
    </tr>
    <tr>
      <td>静态护栏</td>
      <td><code>pytest backend/tests/test_factor_metrics_authority_static.py -q</code></td>
      <td>5 项通过，覆盖 <code>calc_engine</code>、旧评级读取、前端旧字段、运行路径和敏感 fallback。</td>
    </tr>
    <tr>
      <td>前端构建</td>
      <td><code>cd frontend; npm run build</code></td>
      <td>通过。一次 Windows/Next 临时构建产物缺块后清理 <code>.next</code> 并重跑通过。</td>
    </tr>
    <tr>
      <td>测试后端 API</td>
      <td><code>uvicorn backend.main:app --host 127.0.0.1 --port 8011</code></td>
      <td>通过。验证因子列表、分类列表、Multi-Alpha 候选、相关性详情、手工因子无快照 fail-fast 均符合 official/ind 口径。</td>
    </tr>
    <tr>
      <td>测试前端 UI</td>
      <td><code>NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1 npm run start -- -p 3011</code> + Playwright smoke。</td>
      <td>通过。<code>/quantevolver/factors</code>、<code>/quantevolver/compose</code>、<code>/quantevolver/factor-correlation</code>、<code>/quantevolver/factor-deletion</code> 均返回 200，无 page error/console error/request failed。</td>
    </tr>
  </tbody>
</table>

## 3. 已完成基线

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>项目</th>
      <th>当前状态</th>
      <th>备注</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>aistock_factor_metrics</code> 生产读取限定 <code>calc_engine</code></td>
      <td>大部分已完成；旧 IC 衰变、删除候选、因子清洗已补齐。</td>
      <td>已有静态测试阻止生产路径新增未限定 <code>calc_engine</code> 的读取。</td>
    </tr>
    <tr>
      <td>补录脚本清单</td>
      <td>已初步落地。</td>
      <td>见 <code>docs/architecture/factor_legacy_metric_scripts_inventory_20260428.md</code>。</td>
    </tr>
    <tr>
      <td>物理删除旧字段</td>
      <td>未执行。</td>
      <td>需单独 migration、备份和回滚方案。</td>
    </tr>
  </tbody>
</table>

## 4. 待修复问题清单

### 4.1 生产决策仍使用旧评级

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>当前问题</th>
      <th>风险</th>
      <th>目标口径</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backend/services/quantevolver/factor_analyst.py:1413-1447</code></td>
      <td><code>grade_filter</code>、列表返回和排序仍使用 <code>qe_factor_classification.grade</code>。</td>
      <td>分类列表/组合推荐可能按旧评级筛选排序。</td>
      <td>JOIN active <code>qe_factor_official_ratings</code>，返回 <code>official_grade/official_score</code>。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/factor_analyst.py:1486-1507</code></td>
      <td>推荐组合仍读取、过滤、排序 <code>c.grade</code>。</td>
      <td>推荐结果不跟随 active 官方评级。</td>
      <td>官方评级 + 权威独立指标。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/qe_evolution_agents.py:735/753/858</code></td>
      <td>演进 Agent 候选/详情 prompt 仍读取 <code>c.grade</code>。</td>
      <td>LLM prompt 使用旧评级，影响演进决策。</td>
      <td>prompt 只使用 <code>official_grade/official_score</code>。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/portfolio_architect.py:856/876/878/922/931/946</code></td>
      <td>组合构建路径仍筛选、排序 <code>c.grade</code>。</td>
      <td>组合候选可能使用过期评级。</td>
      <td>active 官方评级参与筛选、排序。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/qe_evolution_service.py:1244/1258</code></td>
      <td>因子库统计仍按 <code>c.grade</code> 聚合。</td>
      <td>演进系统看到的评级分布不准确。</td>
      <td>按 active 官方评级聚合。</td>
      <td>P0</td>
    </tr>
  </tbody>
</table>

### 4.2 Multi-Alpha / 组合 / 演进仍混用旧字段名和旧强度口径

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>当前问题</th>
      <th>整改要求</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backend/services/quantevolver/multi_alpha_selector.py:207-255</code></td>
      <td><code>grade</code> 虽来自 official，但字段仍命名为 <code>grade</code>；<code>ic_value</code> 来自 <code>fm.ic_mean</code>；排序使用 <code>ABS(fm.ic_mean)</code>。</td>
      <td>返回 <code>official_grade/official_score/ind_*</code>；强度统一为 <code>ind_rank_ic_best_abs</code>，旧字段不得参与排序。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/routers/quantevolver.py:1769-1795</code></td>
      <td>Multi-Alpha classified factors API 返回 <code>grade/ic_value</code>。</td>
      <td>统一返回 <code>official_grade/official_score/ind_rank_ic_best_abs</code> 等正式字段。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/routers/quantevolver.py:1856/1887</code></td>
      <td>相关统计或筛选可能继续按旧兼容字段表达。</td>
      <td>逐项改为 official 与 ind 字段；兼容字段只允许 <code>legacy_*</code> 且不参与决策。</td>
      <td>P0</td>
    </tr>
  </tbody>
</table>

### 4.3 前端仍展示/回退旧评级和旧指标字段

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>当前问题</th>
      <th>整改要求</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>frontend/src/app/quantevolver/compose/page.tsx:287/553</code></td>
      <td>仍回退 <code>grade</code>、<code>cls.grade</code>。</td>
      <td>只使用 <code>official_grade/official_score</code>；无官方评级时显示未评级，不回退旧评级。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>frontend/src/app/quantevolver/components/MultiAlphaGroupEditor.tsx:44/45/705</code></td>
      <td>类型和显示仍是 <code>grade/ic_value</code>。</td>
      <td>改为 <code>official_grade/official_score/ind_rank_ic_best_abs</code>。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>frontend/src/app/quantevolver/factor-correlation/components/PairDetail.tsx:44/342</code></td>
      <td>相关性详情仍有 <code>grade</code> 字段。</td>
      <td>如为当前评级，改 official；如为历史实验表现，明确文案。</td>
      <td>P1</td>
    </tr>
    <tr>
      <td><code>frontend/src/app/quantevolver/factor-deletion/page.tsx:32/359</code></td>
      <td>仍保留 <code>v2_grade/v2_score</code> fallback。</td>
      <td>后端完成 official 字段后清理 fallback。</td>
      <td>P1</td>
    </tr>
    <tr>
      <td><code>frontend/src/app/quantevolver/components/ManualFactorDialog.tsx:243</code></td>
      <td>手工因子 UI 展示可能仍基于旧计算结果。</td>
      <td>展示 official evaluation + official rating 的结果。</td>
      <td>P0</td>
    </tr>
  </tbody>
</table>

### 4.4 手工因子 full pipeline 未进入官方闭环

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>当前问题</th>
      <th>目标流程</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backend/services/manual_factor_service.py:30/381-389</code></td>
      <td>仍通过 WSL 调用旧 <code>compute_factor_metrics_unified.py</code>。</td>
      <td>旧脚本最多作为诊断，不作为官方指标完成依据。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/manual_factor_service.py:462-478</code></td>
      <td>调用 <code>_save_metrics_to_db()</code>，但该函数当前 no-op。</td>
      <td>保存因子后强制调用 <code>FactorOfficialEvaluationService</code>，再调用官方评级或统一 pipeline。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/manual_factor_service.py:582-577</code></td>
      <td>no-op 可能造成 UI 上“指标计算完成”的假成功。</td>
      <td>返回结果必须明确 official 落表状态；失败则 fail-fast。</td>
      <td>P0</td>
    </tr>
  </tbody>
</table>

### 4.5 运行态硬编码本机/WSL/个人路径

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>硬编码内容</th>
      <th>风险</th>
      <th>整改方向</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backend/services/quantevolver/config_composer.py:33-83</code></td>
      <td><code>F:/Dev/RD-Agent-main</code>、<code>/mnt/f/Dev/RD-Agent-main</code>、<code>/home/lc999/data/qlib_bin</code>、<code>/home/lc999/data/qlib_minute_bin</code>。</td>
      <td>破坏远程节点、多机部署和可复现实验。</td>
      <td>改为 compute node 配置、环境变量、artifact root、数据集配置；缺失 fail-fast。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/config_composer.py:405</code></td>
      <td><code>/home/lc999/data/rl_models/v24/v24_plan_net.pt</code>。</td>
      <td>模型资产路径被固定到个人机器。</td>
      <td>改为 StrategyPackage/validated policy/model asset registry 路径。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/manual_factor_service.py:28-37</code></td>
      <td><code>/mnt/f/dev/RD-Agent-main/...</code>、<code>/mnt/f/Dev/AIstock/scripts/compute_factor_metrics_unified.py</code>、<code>/home/lc999/factor_workspace</code>。</td>
      <td>手工因子验证和计算绑定本机 WSL。</td>
      <td>改为 compute node 工作目录配置；官方指标计算路径不依赖旧脚本。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/routers/rdagent_sync_admin.py:84-86</code></td>
      <td>固定 RD-Agent debug_tools 路径和 conda Python 路径。</td>
      <td>远程节点/不同安装目录无法同步。</td>
      <td>使用 compute node 的 RD-Agent root、Python executable 配置或任务资产 API。</td>
      <td>P1</td>
    </tr>
    <tr>
      <td><code>backend/routers/quantevolver.py:2101/2164/2238</code></td>
      <td><code>F:/Dev/AIstock/rdagent_assets/qe_strategies</code>。</td>
      <td>策略资产路径固定本机。</td>
      <td>使用配置化 artifact root 或 DB/对象存储资产路径。</td>
      <td>P1</td>
    </tr>
    <tr>
      <td><code>backend/routers/quantevolver.py:2991-2995</code></td>
      <td>因子缓存目录和 backfill 脚本固定到 <code>F:/Dev/AIstock</code> / <code>/mnt/f/Dev/AIstock</code>。</td>
      <td>缓存生成不可迁移，远程节点不可复现。</td>
      <td>改为 artifact root + compute node 执行，不在 router 中硬编码。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/routers/quantevolver.py:3333</code></td>
      <td>WSL 命令固定 <code>cd /mnt/f/Dev/AIstock</code>。</td>
      <td>命令只能在作者本机运行。</td>
      <td>改为 compute node command builder，工作目录来自节点配置。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/stock_pool_sync.py:25/118/178</code></td>
      <td>默认 <code>/home/lc999/data/qlib_bin</code>，默认 ssh_user <code>lc999</code>。</td>
      <td>股票池同步绑定个人用户和路径。</td>
      <td>节点配置必须显式提供 qlib 数据路径和 ssh 用户；缺失 fail-fast。</td>
      <td>P1</td>
    </tr>
  </tbody>
</table>

### 4.6 敏感配置硬编码 fallback

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>文件/位置</th>
      <th>当前问题</th>
      <th>风险</th>
      <th>整改要求</th>
      <th>优先级</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>backend/routers/quantevolver.py:3334</code></td>
      <td>WSL 因子缓存命令含 <code>TDX_DB_PASSWORD</code> 默认密码 fallback。</td>
      <td>敏感信息泄露；部署环境缺密钥时不会 fail-fast，可能误连错误数据库。</td>
      <td>删除代码内默认密码；只允许环境变量/密钥管理注入；缺失时 fail-fast。</td>
      <td>P0</td>
    </tr>
    <tr>
      <td><code>backend/services/quantevolver/config_composer.py:3523-3524</code></td>
      <td>DB user fallback 为 <code>postgres</code>，password 直接读取环境。</td>
      <td>user fallback 是否允许需统一安全策略；password 缺失应 fail-fast。</td>
      <td>统一 DB secret 读取工具；禁止密码默认值；生产环境禁止默认用户。</td>
      <td>P1</td>
    </tr>
  </tbody>
</table>

## 5. 分阶段实施方案

### Phase 0：等待、隔离与基线同步

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>步骤</th>
      <th>执行内容</th>
      <th>通过条件</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>0.1</td>
      <td>等待其他窗口完成当前修改并提交，或由用户确认可使用独立 worktree 基于最新 <code>origin/main</code> 开始。</td>
      <td>无未确认的同文件并行修改。</td>
    </tr>
    <tr>
      <td>0.2</td>
      <td>创建独立 worktree/分支，例如 <code>codex/factor-authority-hardening-phase2</code>。</td>
      <td>不污染当前主工作区。</td>
    </tr>
    <tr>
      <td>0.3</td>
      <td>读取本方案、项目记忆、现有官方指标/评级服务实现。</td>
      <td>执行者明确允许 API 和禁止模式。</td>
    </tr>
  </tbody>
</table>

### Phase 1：官方评级替换旧 <code>classification.grade</code>

- 后端生产决策路径统一 JOIN active `qe_factor_official_ratings`。
- API 返回 `official_grade/official_score`。
- 旧 `grade` 字段如保留，必须为兼容 alias，且值来自 official，不得来自 classification。
- 增加静态测试阻断 `WHERE c.grade`、`ORDER BY c.grade`、生产 prompt 读取 `c.grade`。

### Phase 2：独立指标字段与强度口径统一

- Multi-Alpha、组合、演进路径统一返回 `ind_*` 字段。
- 因子强度统一为 `ind_rank_ic_best_abs = max(abs(rank_ic_1d), abs(rank_ic_5d), abs(rank_ic_10d), abs(rank_ic_20d))`。
- 不再用 `ABS(ic_mean)` 作为候选强度排序主口径。

### Phase 3：前端字段清理

- 前端类型统一改为 `official_*` 和 `ind_*`。
- 删除对 `grade/ic_value/sharpe_value/ann_ret_value/v2_grade/v2_score` 的正常展示回退。
- 如需保留历史兼容字段，仅允许 `legacy_*` 调试显示。
- QE 实验/loop 历史表现区保留，但文案必须明确“实验历史表现”。

### Phase 4：手工因子官方闭环

- 手工因子保存后调用官方独立指标计算。
- 官方指标成功后调用官方评级或统一 pipeline。
- UI 只以 official 落表结果作为完成状态。
- 旧 WSL 指标脚本退出官方链路；如保留，标记 diagnostic-only。

### Phase 5：路径、节点、密钥配置整改

- 把本机路径替换为 compute node 配置、环境变量、artifact registry 或 DB/对象存储路径。
- 删除所有生产命令中的密码 fallback。
- 缺少必需路径/密钥时 fail-fast，并返回结构化错误。
- 不改变持久资产内容，不修改 StrategyPackage manifest、模型文件、QE artifacts。

### Phase 6：静态护栏与回归验证

- 新增/扩展静态测试：
  - 生产路径禁止新增 `/mnt/f/`、`/home/lc999/`、`F:/Dev`、`F:\Dev`。
  - 禁止生产代码出现数据库密码默认值 fallback。
  - 禁止生产决策读取 `qe_factor_classification.grade`。
  - 禁止未限定 `calc_engine` 的 `aistock_factor_metrics` 读取。
- 在测试端口验证，不触碰生产端口 `8001`。

## 6. 验证清单

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>验证项</th>
      <th>命令/方式</th>
      <th>通过标准</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Python 编译</td>
      <td><code>python -m py_compile</code> 指定修改文件</td>
      <td>无语法错误。</td>
    </tr>
    <tr>
      <td>静态护栏</td>
      <td><code>pytest backend/tests/test_factor_metrics_authority_static.py ...</code></td>
      <td>阻断旧评级、旧路径、缺 <code>calc_engine</code>。</td>
    </tr>
    <tr>
      <td>前端构建</td>
      <td><code>cd frontend && npm run build</code></td>
      <td>类型和构建通过。</td>
    </tr>
    <tr>
      <td>API 验证</td>
      <td>测试端口调用 factor analyst、Multi-Alpha、QE evolution、manual factor API。</td>
      <td>返回 official/ind 字段；无旧字段回退参与决策。</td>
    </tr>
    <tr>
      <td>UI 验证</td>
      <td>测试端口打开因子库、Compose、Multi-Alpha、相关性、删除候选、手工因子。</td>
      <td>展示口径正确；实验历史表现和当前独立指标分区清楚。</td>
    </tr>
    <tr>
      <td>安全验证</td>
      <td>grep 静态扫描敏感 fallback 与个人路径。</td>
      <td>生产路径无硬编码密码和个人路径。</td>
    </tr>
  </tbody>
</table>

## 7. 暂不执行事项

- 暂不删除脚本；所有脚本删除必须用户逐项确认。
- 暂不物理删除数据库旧字段。
- 暂不修改 StrategyPackage manifest、模型权重、QE 实验 artifacts 等持久资产。
- 暂不重启生产端口 `8001`。
- 暂不在其他窗口未提交的脏工作区上直接改代码。

## 8. 提交策略

- 每个阶段独立提交并推送。
- 每次只 stage 当前阶段文件。
- 如果其他窗口已提交，需要先基于最新 `origin/main` rebase/merge 后再开始。
- 若出现同文件冲突，先报告冲突位置和影响，不擅自覆盖他人修改。

## 9. 2026-04-29 执行落地记录

<table border="1" cellspacing="0" cellpadding="6">
  <thead>
    <tr>
      <th>整改项</th>
      <th>落地设计</th>
      <th>验证要求</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>生产评级读取</td>
      <td>生产筛选、排序、prompt、相关性详情统一读取 active <code>qe_factor_official_ratings.official_grade/official_score</code>；<code>qe_factor_classification.grade</code> 仅允许以 <code>legacy_grade</code> 命名保留调试输出，不参与决策。</td>
      <td>静态测试禁止 <code>c.grade/cl.grade/fc.grade</code> 参与生产读取；API smoke 返回 <code>official_grade</code>。</td>
    </tr>
    <tr>
      <td>独立指标展示</td>
      <td>前端因子库、Compose、Multi-Alpha 候选、相关性详情、删除候选、手工因子结果只使用 <code>official_*</code> 与 <code>ind_*</code> 字段；旧 <code>ic_value/sharpe_value/ann_ret_value/v2_grade/v2_score</code> 不再作为正常展示字段。</td>
      <td>静态测试扫描 QuantEvolver UI 关键页面，禁止旧字段和 <code>*.grade</code> 当前因子评级回退。</td>
    </tr>
    <tr>
      <td>手工因子 full pipeline</td>
      <td>手工因子全流程必须带 <code>data_date</code>，通过 <code>FactorOfficialEvaluationService</code> 写入官方独立指标，再调用 active <code>FactorRatingService</code> 写入官方评级；旧 WSL 指标脚本不再作为产品化写入路径。</td>
      <td>无快照时 UI/后端 fail-fast；成功结果展示 official metrics DB 写入数量和官方评级 run 统计。</td>
    </tr>
    <tr>
      <td>运行态路径和密钥</td>
      <td>生产运行路径改为环境变量、repo-local artifact root 或 compute node 配置；删除数据库密码 fallback，缺少必需配置时 fail-fast。</td>
      <td>静态测试禁止 <code>F:/Dev</code>、<code>F:\Dev</code>、<code>/mnt/f</code>、<code>/home/lc999</code>、个人密码和 shell 密码 fallback。</td>
    </tr>
  </tbody>
</table>
