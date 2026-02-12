# AIstock 选股推理资产保障详细分析（RD-Agent 资产包固化改造）

## 1. 文档目标

本文件用于回答以下问题：

- **目标**：是否能够严格保证 AIstock 侧“选股推理”稳定运行，并明确需要哪些策略资产。
- **现状**：RD-Agent 侧当前 `solidify_loop_assets` 的打包逻辑产物是否包含所有必需资产。
- **差异**：现状与目标之间缺失了什么、哪里存在歧义/不确定性/潜在遗漏。
- **方案**：给出可落地的改造方案（RD-Agent 侧与 AIstock 侧分工、接口约定、资产包结构约定、命名与去重方案）。
- **保障**：如何通过自动化校验/验收标准保证改造后 AIstock 选股推理正确执行。

执行操作手册（全量初始化/固化/导出/验收）：`docs/RD-Agent_Catalog数据同步操作手册.md`

### 1.1 新增强约束（AIstock Task 化与实盘截止日期）

本节为新增的硬性约束，优先级高于历史基于 loop 的方案：

- **Task 显示命名**：AIstock 侧展示与选择的 task 必须使用 `log/<日期时间目录名>`（例如 `2026-01-12_09-00-00-000000`）作为唯一可见标识；不得在 UI/接口返回中暴露 UUID/32位 hash 等“无意义字符串”作为 task 主标识.
- **因子命名与关联**：因子必须使用真实名称展示；同时需要建立“首次被加入的 task”的关联关系（可追溯：该因子第一次出现在何时、由哪个 task 引入）。
- **模型与关联**：模型需要关联到“每次加入 SOTA 时的 task”（即每次成为 SOTA 都形成一条可追溯的 task 版本记录）。
- **资产落盘（强制）**：模型与因子除了数据库记录外，必须落盘保存其所需的文件资产（至少包含推理/实盘可运行所需入口文件与权重/配置等），并能被后续实盘选股稳定加载.
- **策略与版本控制**：策略表不需要与 task 直接关联；策略来源于 RD-Agent 演进模板，但 AIstock 侧必须对策略做版本控制（同名策略内容更新 => 新版本），可回溯到具体内容/哈希.
- **同步与依赖关系**：本次新增的所有程序不再依赖“旧 loop 中写入的标识”或 RD-Agent registry sqlite 中的记录作为权威来源；以 `log` 目录与可落盘资产为权威来源.
- **初始化同步**：初始化同步脚本必须支持历史全量幂等同步；首次同步允许直接拷贝文件资产（不强制必须通过 zip 下载），但必须在 AIstock 侧形成统一的资产目录结构与 manifest（可校验）.
- **实盘选股数据截止日期**：AIstock 基于 task 的实盘选股必须支持配置 `cutoff_date`（数据截止日期）。例如选择 `2026-01-12`，则选股过程严禁使用该日期之后的任何数据（包括因子计算与模型输入数据），以保证回放/验证口径一致.

### 1.2 Task-only 同步的“Loop”口径（权威定义）

本项目存在两个容易混淆的“loop”概念：

- **source_session_dir_id（快照目录号）**：`log/<task_id>/__session__/0,1,2...` 中的数字目录名.
  - 含义：RD-Agent 在第 N 轮流程阶段保存的一份 session 快照容器.
  - 用途：用于选择“从哪个快照读取 trace”（通常选择最大 N 作为最终快照）.
  - 限制：该目录号本身不等价于某次因子/模型实验，也不能唯确定位模型训练结果的 workspace.

- **trace_loop_id（权威 loop id）**：从 session 反序列化得到的 `trace.hist` 列表下标.
  - 含义：某一次具体 Experiment（因子实验/模型实验）在全历史中的位置.
  - 用途：用于确定性定位
    - 哪一次因子实验被 `decision=True` 接纳为 SOTA
    - 哪一次模型实验被 `decision=True` 接纳为 SOTA
    - 对应的 `exp.experiment_workspace.workspace_path`

结论（强约束）：

- **定位“workspace 中的模型训练结果数据”的主键必须使用 `trace_loop_id`.**
- `source_session_dir_id` 仅用于说明“本次解析采用了哪个快照目录”（可审计），不作为定位 workspace 的主键.

同时，为满足“不得遍历/不得猜测”的约束：

- Task-only 同步必须以 `log/<task_id>/__session__/MAX/1_coding`（或同目录最新可反序列化文件）为权威来源.
- 从 session 的 `trace.hist` 与 `sub_workspace_list.file_dict` 提取：SOTA 决策、workspace_path、因子源码、模型权重.
- 不允许通过扫描 workspace 文件系统（遍历目录/按扩展名猜测）来定位权重或入口.

## 2. 目标定义（AIstock 选股推理的“硬性资产要求”）

### 2.1 AIstock 推理流程对资产的调用链

AIstock 推理入口：`backend/inference_engine.py`（`run_inference`）

关键点：

- **因子模块加载**：`self.validator.validate_and_load(..., assets["factor_file"])`
- **因子计算入口**：
  - 优先：Catalog 的 `interface_info.standard_wrapper` 指定的 wrapper
  - 其次：自动探测 `factor_*` 函数
  - 再次：传统 `Factor` 类
- **模型加载**：
  - 优先：`model_config` 重建模型实例，再加载权重（`assets["model_file"]`）
  - 回退：`pickle.load(assets["model_file"])`

因此，**AIstock 要求至少能稳定定位到两类物理文件**：

- **必需 1：因子实现文件**（可 import 并满足 wrapper/Factor 约定）
- **必需 2：模型权重文件**（可被 model.load_model 或 pickle.load）

### 2.2 AIstock 当前对资产包的定位规则（现有约束）

AIstock 从 Catalog JOIN 得到 `asset_bundle_id` 与 `loop_ws_id` 后，会调用：

- `rdagent_asset_service.download_and_extract_bundle(bundle_id)`
- `rdagent_asset_service.get_strategy_files(bundle_id, loop_ws_id)`

`get_strategy_files` 当前策略：

- 在 `bundle/{workspace_id}/` 存在时优先使用；否则如果根目录含 `factor.py` 或 `model.py` 就直接用根目录；否则尝试唯一子目录。
- **因子实现**：优先 `factor.py`，否则 `model.py`，否则 `ws_path.glob("*.py")` 找到第一个 `.py`。
- **模型权重**：优先 `weights/model.pkl`；否则 `mlruns/**/params.pkl`；否则根目录 `*.pkl`（排除一批结果文件关键词）。

> 结论：**AIstock 目前并不理解“扁平化+workspace_id前缀命名”这套规则**，它只会“随机取第一个 py”或“猜一个 pkl”。

### 2.3 “严格保证可运行”的定义

要严格保证 AIstock 选股推理可运行，至少满足：

- **确定性**：资产包内必须存在**唯一且可确定**的 `factor_file` 与 `model_file`。
- **正确性**：`factor_file` 必须包含推理需要的 wrapper/Factor；`model_file` 必须是该模型真正的权重文件。
- **一致性**：Catalog（或 manifest）提供的映射必须与资产包结构一致，且可校验。
- **完备性**：对于一个 loop 下的多个 workspace / 多因子，必须有明确策略：
  - AIstock 推理使用哪一个 workspace 的哪一份代码/模型（主 experiment_workspace？best loop？best model？）
  - 多个同名因子如何区分（表达式指纹/版本/来源）

### 2.4 AIstock 选股结果输出与行情/名称数据源约束（强契约）

本节定义 AIstock“选股 UI / 回放推理”的结果口径与数据源选择策略，作为后续实现的硬性约束：

- **默认候选数量**：`top_k = 50`（Top50）。
- **结果最小字段集合**（每行一只股票）：
  - `symbol`：股票代码（优先 ts_code，如 `000001.SZ`）
  - `name`：股票名称（若无法获取允许为空，但不得影响展示）
  - `price`：价格（交易时段优先实时价；非交易时段为最近交易日收盘价）
  - `pct_change`：涨幅%（交易时段优先实时涨幅；非交易时段为最近交易日涨幅）
  - `score`：策略推理评分
  - `rank`：排名（1..top_k）
  - `quote_source`：`miniqmt|tdx|fallback_trade_day`（用于排查与可解释性）
  - `quote_time`：行情时间戳（若无法提供可为空）

数据源优先级（必须满足）：

- **实时行情（价格/涨幅）优先级**：
  1. **miniQMT（xtquant）**：优先使用 xtquant 获取实时快照（例如 `get_full_tick` / `get_realtime_snapshot`），作为交易时段的首选。
  2. **TDX**：当 miniQMT 不可用或返回空数据时，再尝试 TDX 实时行情。
  3. **最近交易日回退**：当实时行情不可用（包括非交易时段/休市/数据源不可达）时，必须回退到最近交易日（日线）的收盘价与涨幅。

- **股票名称（name）优先级**：
  1. **TDX**：优先从 TDX 基本信息/搜索接口获取名称（例如 `get_stock_basic_info` 或等价能力）。
  2. **兜底**：若 TDX 名称不可用，允许回退到其他数据源返回的名称；若仍不可用则返回空字符串或 `None`（但不得抛错导致选股结果不可用）。

说明：

- 上述策略要求“**优先 miniQMT 行情**、**名称优先 TDX**”。
- AIstock 的推理评分与候选生成不应依赖行情可用性；行情/名称失败只能影响展示字段，不得影响 `score/rank` 的生成与返回。

补充（可诊断性与可控开关，已落地实现）：

- **为什么午间/闭市时没有使用 miniQMT 实时行情**：AIstock 在补齐展示字段时会判断当前时间是否为交易时段；当处于午间休市（例如 11:30-13:00）或闭市时，`realtime_allowed=false`，因此会直接使用 `fallback_trade_day`（最近交易日收盘价/涨幅）以保证口径稳定。
- **强制尝试实时行情（仅影响展示字段）**：设置环境变量 `AISTOCK_FORCE_REALTIME_QUOTE=1` 后，即使处于午间/闭市，也会强制尝试调用 `get_realtime_snapshot` 获取最新可用快照（若数据源不可达仍会回退最近交易日）。
- **服务端日志会输出决策原因**：会输出 `market_open/force_realtime/realtime_allowed`，用于解释为什么当次请求走了 `miniqmt|tdx|fallback_trade_day`。

## 3. 现状（RD-Agent 资产包打包逻辑与产物）

### 3.1 RD-Agent 打包入口与流程

入口：`RD-Agent-main/tools/backfill_registry_artifacts.py --mode solidify-all`

调用：`rdagent/utils/solidification.py::solidify_loop_assets(task_run_id, loop_id)`

产物：`RDagentDB/production_bundles/{asset_bundle_id}/`（扁平化目录）

### 3.2 当前打包逻辑的关键行为（会导致 AIstock 侧不确定）

#### 3.2.1 Python 文件打包规则（扁平化 + 冲突前缀）

- 遍历 workspace（排除目录：`mlruns`、`data`、`result` 等）
- `.py` 文件会被拷贝到 bundle 根目录
- 如果名字冲突，用 `{workspace_id}_xxx.py` 前缀改名
- **但当前排除了 `read_exp_res.py`**（该文件在你给的案例 workspace 中是唯一 py 文件）

#### 3.2.2 模型权重打包规则（对 mlruns 的过滤存在致命问题）

- 仅拷贝根目录下的 `model.pkl`
- 对 `mlruns/` 目录，会遍历并拷贝 `.pkl`，但 **排除了 `params.pkl`**

而你的案例中：

- workspace 根目录只有 `read_exp_res.py` 与 `ret.pkl`
- 模型权重在 `mlruns/**/artifacts/params.pkl`

因此当前打包逻辑会导致：

- **因子实现 py 被排除**（read_exp_res.py）
- **模型权重 pkl 被排除**（params.pkl）

最终 AIstock 在资产包根目录只看到一堆 `*_pred.pkl`、`*_ic.pkl` 等结果文件与 conf_*.yaml，无法推理。

#### 3.2.3 扁平化结构的固有风险

即使把 `read_exp_res.py` 与 `params.pkl` 放进资产包，扁平化依然存在风险：

- **错误选择风险（py）**：`get_strategy_files` 会拿到“第一个 py”，不保证它是因子入口文件。
- **错误选择风险（pkl）**：根目录存在大量 `.pkl` 时，fallback 策略可能拿到回测结果 pkl，而不是模型权重。
- **冲突覆盖风险**：同名文件在多个 workspace 中出现，前缀虽能避免覆盖，但 AIstock 侧并不知道应选择哪个前缀文件。

### 3.3 现状结论

- **仅实施“因子命名区分方案（指纹/显示名）”并不能保证 AIstock 选股推理可运行**。
- 即使修复 RD-Agent 打包把 `read_exp_res.py` / `params.pkl` 放进去，只要仍是扁平化且没有“明确映射”，AIstock 依旧可能选错。

## 4. 现状与目标的差异（Gap）

### 4.1 Gap 清单

- **Gap-1：缺少强约定的“入口文件”**
  - 目标：明确 `factor_entry.py`（或 `factor.py`）作为推理入口
  - 现状：py 文件可能多个且命名随机/带前缀，AIstock 可能随便取第一个

- **Gap-2：缺少强约定的“模型权重文件”**
  - 目标：明确 `model.pkl`（或 `weights/model.pkl`）为权重
  - 现状：权重可能在 `mlruns/**/params.pkl`，且当前 RD-Agent 还排除了它

- **Gap-3：缺少“资产映射清单（manifest）”**
  - 目标：资产包自描述（manifest 指出主推理 workspace、入口 py、权重 pkl、配置 yaml）
  - 现状：AIstock 只能猜

- **Gap-4：多 workspace / 多因子 的归一化规则缺失**
  - 目标：明确“loop->主推理 workspace”与“因子集合”如何选择与加载
  - 现状：一个 loop 下可能有 experiment_workspace + factor_workspace + 其他 workspace

- **Gap-5：同名因子的区分与引用机制未闭环**
  - 目标：即便同名，也能唯一引用到具体实现（表达式指纹 + 版本 + workspace_id）
  - 现状：RD-Agent 侧 registry 记录了 `factor_name/expression/workspace_id`，但 AIstock 推理并不按这个去定位文件

## 5. 详细设计方案（保证 AIstock 可运行的“最小闭环契约”）

本节给出一个能严格保证推理的闭环：**资产包结构约定 + manifest 映射 + AIstock 加载逻辑对齐 + 校验/验收**。

### 5.1 资产包结构约定（RD-Agent 输出必须满足）

建议升级为“目录化 + 入口标准化 + manifest”的结构：

```text
production_bundles/{asset_bundle_id}/
  manifest.json
  workspaces/
    {workspace_id}/
      factor_entry.py           # 推理入口（强约定）
      model.pkl                 # 权重（强约定）
      config.yaml|conf_*.yaml    # 训练/推理配置（可选但建议）
      extras/                    # 其他辅助文件（可选）
```

关键约束：

- `manifest.json` 必须存在
- `manifest.json` 必须明确一个 `primary_workspace_id`
- `manifest.json` 必须明确 `factor_entry_relpath` 与 `model_weight_relpath`

### 5.2 manifest.json 约定（两端接口契约）

建议 schema（示意）：

- `schema_version`: 固定如 `1`
- `task_run_id` / `loop_id`
- `primary_workspace_id`
- `primary_assets`:
  - `factor_entry_relpath`
  - `model_weight_relpath`
  - `config_relpath`（可选）
- `workspaces`: 列出所有 workspace 的资产（便于审计/排错）
- `factors`: 因子集合（用于 Catalog/前端展示/去重），每项包含：
  - `factor_name`
  - `expression`
  - `fingerprint_md5`
  - `source`
  - `workspace_id`

### 5.3 RD-Agent 侧改造点（分工）

RD-Agent 侧必须完成：

1. **修复打包漏文件**
   - 不得排除 `mlruns/**/params.pkl`（或明确将其复制为 `model.pkl`）
   - 不得排除关键推理入口 py（你案例中的 `read_exp_res.py`）

2. **生成标准入口文件**
   - 在 bundle 的 `workspaces/{workspace_id}/factor_entry.py` 写入可导入入口
   - 推荐方式：从 workspace 中抽取“真正的 factor wrapper”并生成稳定入口（或复制既定的 factor.py/model.py 并重命名为 factor_entry.py）

3. **生成 manifest.json**
   - 选择 primary workspace：
     - 优先 `workspace_role == experiment_workspace`
     - 若多个 experiment_workspace：选择 metrics 最佳/或 loop 指定的 best workspace
   - 将权重统一复制为 `model.pkl`

4. **因子命名区分方案落地（指纹）**
   - `fingerprint_md5 = md5(normalize(expression))`
   - 同名但不同表达式 => 不同 fingerprint
   - 同表达式重复出现 => 同 fingerprint，可在 registry 中去重/记录 occurrences

### 5.4 AIstock 侧改造点（分工）

AIstock 侧必须完成：

1. **资产加载改为“manifest 优先”**
   - `get_strategy_files`：优先读取 `manifest.json`，用 manifest 指定的路径返回 `factor_py` 与 `model_pkl`
   - 只有 manifest 不存在时才走旧的 heuristic fallback

2. **选择规则显式化**
   - 从 Catalog 得到 `loop_ws_id` 后：
     - 若 manifest 的 `primary_workspace_id` 与 `loop_ws_id` 不一致，记录告警并以 manifest 为准（或以配置开关决定）

3. **增强校验与错误信息**
   - 当缺失资产时，把 bundle 目录扫描结果、manifest 内容摘要写入日志（便于定位）

### 5.5 “扁平化 + 前缀命名 + 指纹”是否可严格保证？

结论：**不能严格保证。**原因如下：

- AIstock 现有加载逻辑不识别“workspace_id 前缀文件名”，只能猜。
- 只要 bundle 内存在多个 `.py` 或多个 `.pkl`，`get_strategy_files` 仍然可能拿到错误文件。
- 指纹解决的是“Catalog 展示/去重/关联”，不是“推理入口文件定位”。

因此：

- 若要严格保证推理稳定运行，必须引入 **manifest 映射** 或 **标准入口命名**（并且 AIstock 侧按该规则加载）。

## 6. 如何确保改造后支持 AIstock 正确执行（保障与验收）

### 6.1 RD-Agent 侧自检（打包时必做）

对每个 bundle 生成后执行校验：

- manifest.json 存在且可解析
- primary workspace 目录存在
- factor_entry.py 存在
- model.pkl 存在
- `python -c "import factor_entry; ..."`（可选）

若失败：

- 不写入 `loops.asset_bundle_id`
- 不置 `is_solidified=1`

### 6.2 AIstock 侧自检（下载解压后必做）

- 读取 manifest.json
- 校验文件存在
- `validator.validate_and_load(..., factor_entry.py)` 能成功
- `pickle.load(model.pkl)` 或 `model.load_model(model.pkl)` 能成功

失败时：

- 返回明确错误：缺哪个文件、manifest 的指向是什么、bundle 目录结构扫描摘要

### 6.3 端到端验收标准（必须达成）

- 对任意一个 solidified loop：
  - API `/api/v1/rdagent/strategies/{strategy_id}/inference` 返回 200
  - 生成的 `scores` 非空且可落库 `trading.rdagent_signal`
- 对包含多 workspace 的 loop：
  - manifest 指定的 primary workspace 能稳定被加载
  - 不因目录中其他 `.py/.pkl` 干扰推理

## 7. 分工明细与接口约定（摘要）

### 7.1 RD-Agent 侧职责

- 产出 **可推理的资产包**（含入口 py 与权重 pkl）
- 产出 **manifest.json**（强契约）
- 维护因子指纹（同名可区分）

### 7.2 AIstock 侧职责

- 按 manifest 精确定位资产文件
- 执行因子模块安全加载与模型加载
- 提供清晰可诊断的错误输出与回退策略

### 7.3 两端契约

- Bundle 必须包含：`manifest.json` + `workspaces/{primary_workspace_id}/factor_entry.py` + `workspaces/{primary_workspace_id}/model.pkl`
- manifest schema 版本化：`schema_version`
- AIstock 若发现 schema_version 不支持：直接报错并提示升级

## 8. 详细设计（唯一约束方案，路径1：扩展 aistock_loop_catalog）

本节是开发期间的**唯一约束方案（Single Source of Truth）**。RD-Agent 与 AIstock 两端必须严格按本节实现，才能保证：

- AIstock 推理在任何 bundle 内存在多个 `.py/.pkl` 的情况下，仍能**确定性**加载正确入口文件与权重文件。
- AIstock 推理/模拟盘运行时不再依赖“猜测式文件搜索”。
- 未来 AIstock 侧支持“人工组合因子+模型 -> 交给 RD-Agent 训练/回测”时，仍保留原始源码与可追溯信息。

### 8.1 资产包结构约定（最终版）

约定：每个 `asset_bundle_id` 对应一个固化后的 Loop（粒度为 `task_run_id + loop_id`）。

目录结构：

```text
production_bundles/{asset_bundle_id}/
  manifest.json
  workspaces/
    {workspace_id}/
      factor_entry.py
      model.pkl
      config.yaml | conf_*.yaml (可选)
      src/ (必须，面向未来组合)
        ... 原始源码（至少包含 factor.py 及依赖）
      extras/ (必须，保留原始权重与中间产物)
        ... 例如 params.pkl / 训练日志片段 / 其他候选权重
```

强约束：

- 推理入口文件固定为：`workspaces/{primary_workspace_id}/factor_entry.py`
- 推理权重文件固定为：`workspaces/{primary_workspace_id}/model.pkl`
- 任何其他 `.py/.pkl` 都只能作为：
  - `src/**` 原始源码（用于未来组合/重训）
  - `extras/**` 原始权重/中间产物（用于排障/追溯）
- AIstock 推理永远只使用 manifest 指向的入口与权重，不允许扫描猜测。

### 8.2 manifest.json schema v1（两端接口契约）

manifest 的定位：

- **每个 bundle 必须包含一个 `manifest.json`**，并作为该 bundle 的权威自描述。
- **AIstock 必须优先读取 manifest.json**，按其中相对路径定位入口文件。

schema_version：

- `schema_version` 必须为整数。
- 本期实现支持 `schema_version = 1`。

最小必需字段（AIstock 推理硬依赖）：

- `schema_version`
- `task_run_id`
- `loop_id`
- `asset_bundle_id`
- `primary_workspace_id`
- `primary_assets.factor_entry_relpath`
- `primary_assets.model_weight_relpath`

推荐字段（用于追溯、排障、未来组合）：

- `primary_assets.config_relpath`（可空）
- `source_workspace_path`（RD-Agent 原始 workspace 目录路径，字符串）
- `log_dir` / `log_uri`（RD-Agent 原始 log 目录路径/URI）
- `candidates.weights[]`（候选权重列表，含 relpath、kind、notes）
- `factors[]`（因子列表，含 name/expression/fingerprint_md5/source/workspace_id）

manifest 示例（schema v1，示意）：

```json
{
  "schema_version": 1,
  "task_run_id": "2025-12-29_05-17-56-204326",
  "loop_id": 1,
  "asset_bundle_id": "645f3a32-3bb9-45c6-9587-45c03a1d967d",
  "primary_workspace_id": "b3caf6168516403580ea6ad430c1e31c",
  "source_workspace_path": "F:/Dev/RD-Agent-main/git_ignore_folder/RD-Agent_workspace/b3caf6168516403580ea6ad430c1e31c",
  "log_dir": "F:/Dev/RD-Agent-main/log/2025-12-29_05-17-56-204326",
  "primary_assets": {
    "factor_entry_relpath": "workspaces/b3caf6168516403580ea6ad430c1e31c/factor_entry.py",
    "model_weight_relpath": "workspaces/b3caf6168516403580ea6ad430c1e31c/model.pkl",
    "config_relpath": "workspaces/b3caf6168516403580ea6ad430c1e31c/conf_model.yaml"
  },
  "candidates": {
    "weights": [
      {
        "relpath": "workspaces/b3caf6168516403580ea6ad430c1e31c/extras/params.pkl",
        "kind": "mlruns_params",
        "notes": "original mlruns artifact params.pkl"
      }
    ]
  },
  "factors": [
    {
      "factor_name": "MA",
      "expression": "MA(close,5)",
      "fingerprint_md5": "<md5>",
      "source": "rdagent_generated",
      "workspace_id": "b3caf6168516403580ea6ad430c1e31c"
    }
  ]
}
```

### 8.3 路径1：扩展 aistock_loop_catalog（DB 索引字段设计）

目的：

- 让 AIstock 侧可快速检索每个 loop 的入口资产与追溯信息（无需扫描文件系统）。
- 让 UI/排障能够直接展示 bundle_id、入口文件、原始 workspace/log 路径等。

约定：DB 中保存 **manifest 摘要/索引字段**，不保存 bundle 全量文件清单。

建议在 `aistock_loop_catalog` 中新增字段（字段名可按现有命名风格调整，但语义必须一致）：

- `manifest_schema_version` (int)
- `manifest_primary_workspace_id` (text)
- `manifest_factor_entry_relpath` (text)
- `manifest_model_weight_relpath` (text)
- `manifest_config_relpath` (text, nullable)
- `source_workspace_path` (text, nullable)
- `log_dir` (text, nullable)
- `log_uri` (text, nullable)

写入时机：

- 在 AIstock 侧“导入/同步 loop catalog（JSON->PG）”流程中写入。
- 数据来源优先级：
  - 优先从 `manifest.json` 解析
  - 若 manifest 缺失（仅兼容历史数据），字段可为空，但该 loop 不应被视为“强保证推理可运行”的新标准产物

### 8.4 RD-Agent 侧实施步骤（必须逐项实现）

本节为 RD-Agent 侧固化/导出开发的实施步骤清单。

#### 8.4.1 修改 solidify_loop_assets 的总体策略

现状是扁平化拷贝。新方案必须改为“结构化输出 + manifest”。

步骤：

1. **确定 primary workspace**
   - 规则：优先 `workspace_role == experiment_workspace`
   - 如果缺失 experiment_workspace：
     - 选择第一个可用 workspace，但必须在 manifest 中标明该降级选择（可在 notes 字段）

2. **构建 bundle 目录结构**
   - 创建 `workspaces/{workspace_id}/`
   - 为每个 workspace 创建：
     - `src/`（原始源码）
     - `extras/`（原始权重与中间产物）

3. **打包原始源码到 src/**
   - 将 workspace 中用于因子/策略实现的源码拷贝到 `src/**`
   - 允许保留多文件结构（不要扁平化改名），以便未来组合/重训

4. **生成推理入口 factor_entry.py（仅 primary workspace 必需）**
   - 目标：让 AIstock 侧稳定 import 且稳定找到标准入口
   - 推荐内容：
     - 在 factor_entry.py 内将原始实现包装成 `factor_xxx(df)` 或 `Factor.compute(df)`
     - 如原始实现位于 src 内多文件，factor_entry.py 负责正确 import

5. **生成推理权重 model.pkl（仅 primary workspace 必需）**
   - 目标：对外暴露一个固定路径的推理权重文件
   - 规则：
     - 若 workspace 根目录存在 `model.pkl` 且可用：复制为 `workspaces/{ws}/model.pkl`
     - 否则从 `mlruns/**/params.pkl` 选择一个“可推理权重源”，复制生成 `model.pkl`
     - 任何原始权重（包括 params.pkl）必须保留到 `extras/`，不得覆盖丢失

6. **保留原始权重/候选权重到 extras/**
   - 将 `mlruns/**/params.pkl`、其他可能的权重文件复制到 `extras/`
   - 不要求统一命名，但必须在 manifest 的 `candidates.weights[]` 列出 relpath 与 kind

7. **生成 manifest.json（schema v1）**
   - 填写最小必需字段 + 推荐字段
   - `primary_assets.*_relpath` 必须是相对于 bundle 根目录的相对路径

8. **固化自检（失败即回滚）**
   - 校验 manifest.json 可解析
   - 校验 `factor_entry.py` 存在
   - 校验 `model.pkl` 存在
   - 建议附加：尝试 import factor_entry（best-effort）
   - 任一失败：
     - 删除该 bundle 目录
     - 不更新 loops 表中的 `asset_bundle_id` 与 `is_solidified`

#### 8.4.2 RD-Agent 侧对外约定（供 AIstock 消费）

- RD-Agent Results API 下载的是 zip，解压后应包含以上结构。
- 不允许仅输出扁平化结构作为新标准产物。

### 8.5 AIstock 侧实施步骤（必须逐项实现）

本节为 AIstock 侧加载/入库/推理改造的实施步骤清单。

#### 8.5.1 资产包读取：manifest 优先（推理链路必须）

修改点：`backend/services/rdagent_asset_service.py::get_strategy_files`

步骤：

1. 在 `bundle_path` 下优先读取 `manifest.json`
2. 校验 `schema_version == 1`（不支持则报错）
3. 使用 manifest 指定的：
   - `primary_assets.factor_entry_relpath`
   - `primary_assets.model_weight_relpath`
   - 返回为 `factor_py` 与 `model_pkl`
4. 仅在 manifest 缺失时才走旧 heuristics（作为历史兼容路径）

强约束：

- 推理链路不得在“manifest 存在”的情况下仍然扫描猜测 `.py/.pkl`。

#### 8.5.2 入库：扩展 aistock_loop_catalog 存 manifest 摘要（路径1）

修改点：AIstock 的 loop catalog 导入/同步逻辑（JSON->PG）

步骤：

1. 同步 loop catalog 时，若 loop 记录存在 `asset_bundle_id`：
   - 可选：从 Results API 同步 bundle 到本地（或延迟到推理时再拉取）
2. 解压后读取 `manifest.json` 并抽取摘要字段
3. 写入 `aistock_loop_catalog` 新增字段（8.3 节字段清单）

备注：

- 如果你希望“导入时不拉取 zip”，也可以在推理首次拉取 bundle 后，再做一次异步回填这些字段，但必须保证最终一致。

#### 8.5.3 推理：严格使用 loop_catalog 的入口字段（可选增强）

在 AIstock 推理时，除了读取 manifest 文件本身，也可以（可选）对照 `aistock_loop_catalog` 中的：

- `manifest_primary_workspace_id`
- `manifest_factor_entry_relpath`
- `manifest_model_weight_relpath`

若发现与 manifest.json 不一致：

- 记录告警
- 以 manifest.json 为准（manifest 是 bundle 的权威自描述）

### 8.6 回归与验收（必须严格执行）

验收目标：确保两端开发都符合本方案设计，最终能达成“严格保证 AIstock 推理可运行”。

#### 8.6.1 最小验收用例

选择至少 1 个历史策略/loop（你的问题 case 即可），执行：

1. RD-Agent 侧生成新结构 bundle
2. AIstock 下载解压 bundle
3. AIstock 读取 manifest 并定位：
   - factor_entry.py
   - model.pkl
4. AIstock 推理运行成功，scores 非空，落库成功

#### 8.6.2 强确定性用例（关键）

在 bundle 中人为制造干扰：

- `src/` 内放多个 `.py`
- `extras/` 内放多个 `.pkl`（含 pred/ic 等结果文件）

验收：

- AIstock 仍严格按 manifest 指向加载，不受干扰

#### 8.6.3 失败可诊断用例

删除 `model.pkl` 或 `factor_entry.py` 后再推理，验收：

- 错误信息明确指出缺失文件
- 输出 manifest 摘要与 bundle 树形摘要（或关键文件列表）

### 8.7 基于事实的结论：RD-Agent 原始 Workspace/打包能力能否支撑 AIstock 实盘选股

本节结论严格基于：

- RD-Agent 侧资产固化实现：`F:/Dev/RD-Agent-main/rdagent/utils/solidification.py::solidify_loop_assets`
- 本次问题 loop：`task_run_id=legacy_fc5d0d3da76e459ca119e3636d52a747, loop_id=0, workspace_id=fc5d0d3da76e459ca119e3636d52a747`
  - 原始 workspace 路径：`F:\Dev\RD-Agent-main\git_ignore_folder\RD-Agent_workspace\fc5d0d3da76e459ca119e3636d52a747`
  - 可见文件（节选）：`combined_factors_df.parquet`, `conf_baseline.yaml`, `model.pkl`, `signals.parquet`, 多个 `conf_*.yaml`
  - 该 workspace **不存在** `factor.py`/`*_factor.py` 等“可在线计算因子”的 Python 实现文件

但以上 `workspace_id=fc5d0d...` 仅代表“AIstock 当前拿到的资产包中声明的 primary_workspace_id / source_workspace_path”，并不等价于“该 loop 在 RD-Agent 任务运行时实际涉及的全部 workspace”。

基于 RD-Agent log session 的权威解析结果（Windows 环境需处理 log pkl 中的 `PosixPath` 反序列化兼容）：

- log session：`F:/Dev/RD-Agent-main/log/2026-01-13_06-56-49-446055`
- 解析脚本：`tools/backfill_registry_artifacts.py::_collect_log_session_loops(log_path)`（通过 `FileStorage(log_path).iter_msg()` 扫描消息对象上的 `experiment_workspace.workspace_path/workspace_path`）
- 对 `loop_id=0` 解析得到的 `workspace_paths` 为：
  - `\\mnt\\f\\Dev\\RD-Agent-main\\git_ignore_folder\\RD-Agent_workspace\\829ec1b1b4774ff49d3c553991aecd89`

因此：针对本 case 的“该 loop 实际涉及哪些 workspace”必须以 log 解析结果为准，不能以 `primary_workspace_id/source_workspace_path` 或 registry 的记录为准。

补充（关键事实核验：一个 loop 可能对应多个 workspace，且 registry 并不具备“全量覆盖”的权威性）：

- RD-Agent 在运行期间对 registry 的写入是 best-effort，存在多处 `try/except: pass` 的静默抑制（例如 subprocess/异常场景会跳过 DB 写入），因此 registry 只能视为派生索引，不能作为“loop→workspace 全量枚举”的权威来源。
- RD-Agent 代码库内已存在“从任务 log session 解析 loop→workspace_paths”的工具链，这说明 RD-Agent 自身也把 logs 视为可回填/更完整的来源：
  - `F:/Dev/RD-Agent-main/tools/bootstrap_registry_from_logs.py` 会对某个 log session 目录执行：`loops_info = _collect_log_session_loops(log_path)`，并从 `info.get('workspace_paths')` 得到每个 loop 的 workspace 路径集合。

因此：本节对“该 loop 是否存在其他 workspace（例如 factor/strategy workspace）”的事实校验方法，必须改为：

- 先定位该 `task_run_id` 对应的 log session 目录（`rdagent.log.conf.LOG_SETTINGS.trace_path` 的父目录/会话目录）。
- 再基于该 log session 解析 `loop_id` 对应的 `workspace_paths` 集合，作为该 loop 的全量 workspace 列表。

补充（会直接影响资产打包是否能遵守本原则）：

- AIstock 下载到的 production bundle `manifest.json` 中当前 `log_dir/log_uri` 为空（见 `backend/data/rdagent_assets/production_bundles/<bundle_id>/manifest.json`）。
- 这会导致“打包阶段从 log 枚举 workspace”无法自动定位 log session 目录。
- 因此 RD-Agent 的 manifest/元数据必须补齐 `log_dir`（至少是 log session 的本地目录路径），否则资产打包无法满足“从 log 获取 workspace”这一强约束。

#### 8.7.1 结论 1：在“不修改 RD-Agent 任务生成文件”的前提下，原始 workspace 文件集无法直接支撑 AIstock 实盘选股

原因（事实）：

- AIstock 的实盘推理链路要求：从 miniQMT+DB 获取 `df_history`，再调用因子入口计算特征（而不是读取回测/回放产物）。
- 该 workspace 的可用资产中，能够得到的“因子”只有 `combined_factors_df.parquet`（离线预计算产物），以及 `signals.parquet`（回测/回放信号产物）。
- 在严格约束“**禁止在 AIstock 侧使用历史回测/回放选股结果**”下：
  - 任何基于 `signals.parquet` 或 `combined_factors_df.parquet` 的推理入口都必须判定为不合规并硬失败（422）。

因此：若 RD-Agent loop 的 workspace 仅包含回放产物而没有可在线计算的因子实现，则 AIstock 无法在“无人工改文件”的前提下直接实盘选股。

#### 8.7.2 结论 2：RD-Agent 侧“资产打包”可以扩展来补齐实盘所需入口（允许新增“打包阶段生成物”，但不得改写任务原始文件）

RD-Agent `solidify_loop_assets` 的事实能力（来自代码）：

- 会扫描每个 workspace：
  - 复制 `*.yaml/*.yml`
  - 复制 `factor.py/*_factor.py/*factor*.py`（若存在）
  - 复制 `model.pkl`（或 `mlruns/**/params.pkl` 映射为 `model.pkl`）
- 若发现可用的因子实现 `.py`：会在 bundle 内生成一个 `factor_entry.py`，动态加载这些因子实现并统一暴露 `compute(df_history)`。
- 若没有任何因子实现文件，但 workspace 内存在 `conf_baseline.yaml/conf.yaml`：代码里有一条“生成实时 Alpha158 的 `factor_entry.py`”的分支（根据 `open/high/low/close/volume` 在线计算 RESI/WVMA/RSQR 等列）。

基于以上事实，我们将“无需人工改文件即可实盘”转化为 RD-Agent 固化阶段的硬约束：

- 对每个 loop 的 production bundle，必须在 bundle 内生成/提供一个“实盘型因子入口”文件（见 8.7.4）。
- 该入口文件可以是：
  - 直接指向打包得到的 `factor.py`（若该文件本身满足 `compute(df_history)` 或 `Factor().compute` 约定），或
  - RD-Agent 在固化阶段生成的 `factor_entry.py`（聚合多个因子实现文件），或
  - RD-Agent 固化阶段“自动生成”的 Alpha158 在线计算入口（仅限能从事实字段推导的 Alpha158/Alpha360 等标准因子族）。

关键点：上述都属于“打包阶段生成物”，不需要改写任务生成的原始 workspace 文件。

#### 8.7.3 结论 3：为何推理入口必须统一（factor_entry.py vs factor.py）以及 AIstock 是否能不依赖 factor_entry

基于现有 AIstock 推理代码事实：

- AIstock 需要一个确定的、可校验的、可动态加载的“单一入口”，以保证：
  - 训练/推理特征对齐可诊断
  - manifest 作为权威索引，避免在 bundle 内扫描猜测入口
  - 严格禁止回放产物（parquet/signals）混入实盘

因此，“必须使用 factor_entry.py”并不是语法强制，而是“统一入口契约”的工程需要。

可选增强（如果你希望允许 `factor.py` 作为入口）：

- RD-Agent 侧在 manifest 中将 `primary_assets.factor_entry_relpath` 指向 `workspaces/<ws_id>/factor.py`（或其他因子实现文件），并保证其导出函数满足 `compute(df_history)`/`Factor().compute`。
- AIstock 侧无需理解 RD-Agent 的任务结构，只需按 manifest 指向动态加载并调用统一接口。

注意：AIstock 侧不建议增加“当没有 factor_entry 时再扫描 factor.py/alpha 文件”的复杂兼容逻辑；这会破坏“manifest 唯一权威入口”的强契约，增加歧义与排障成本。

#### 8.7.4 本次问题 loop 的事实分析：workspace 文件集是否满足“实盘入口可生成/可打包”的条件

对 `fc5d0d3da76e459ca119e3636d52a747` workspace 的事实判断：

- workspace 内无 `factor.py` 或其它因子实现 `.py`：因此无法通过“打包现有因子代码”来获得实盘入口。
- workspace 内存在 `conf_baseline.yaml`：满足 RD-Agent 固化代码中“生成实时 Alpha158 factor_entry.py”的触发条件。
- workspace 内存在 `combined_factors_df.parquet` 与 `signals.parquet`：这些均为回放产物，在 AIstock 实盘推理中必须被禁止依赖。

因此，对于类似本 loop 的“仅配置 + 回放产物、无因子代码”的 workspace，要想让 AIstock 实盘选股成立，RD-Agent 必须在固化阶段生成一个不依赖 parquet/signals 的实时因子入口（例如 Alpha158 在线计算入口），并将其写入 manifest 的 `factor_entry_relpath`。

若该 loop 的模型训练使用的并非 Alpha158 标准因子族，而是自定义 `feature_*` 因子：则必须在 RD-Agent 任务产物中存在对应的因子实现代码（`factor.py` 等），否则仅凭现有 workspace 文件无法“无猜测”推导出可在线计算的特征逻辑。

#### 8.7.5 同步流程需要更新的点（全量/增量/资产同步）

- 全量/增量同步：
  - 只要 loop 有 `asset_bundle_id`，AIstock 必须按需下载 bundle，并基于 manifest 做硬校验（缺文件/入口不合规 → 422）。
  - 如果发现入口为回放型（parquet/signals），AIstock 必须显式标记失败并提示“需要 RD-Agent 重新固化生成实盘入口”。
- 资产同步（bundle 下载/解压）新增强校验：
  - 解压后立即执行：manifest 完整性 + 入口类型检查（禁止 parquet 回放入口）。
  - 校验结果应入库/可观测（用于 UI 展示“该 loop 是否具备实盘推理资产”）。

## 9. 实施顺序建议（避免两端不同步导致返工）

推荐顺序：

1. RD-Agent 侧先完成：结构化 bundle + manifest + factor_entry/model.pkl + 自检
2. AIstock 侧完成：manifest 优先加载
3. AIstock 侧完成：扩展 `aistock_loop_catalog` 字段 + 同步入库
4. 跑完 8.6 节验收用例后，再开始做因子指纹去重与“人工组合”相关扩展

## 10. AIstock 选股功能（loop 复用）落地方案（权威开发方案）

本节用于补齐“AIstock 选股 UI / 选股结果展示 / 自选池写入 / 同步工作流”的端到端开发方案。

约束：

- 本节所有“现状覆盖/缺口/方案”均基于 AIstock 代码与已存在 API 的事实分析（见“附：关键代码位置”）。
- 本节与第 2.4 节一起构成 AIstock 选股交付的**强契约**（默认 Top50、行情优先 miniQMT、名称优先 TDX）。
- 本节不定义“最小实现/POC/精简版”，所有实现以满足业务需求为准。

### 10.1 现状覆盖情况（AIstock 现有选股代码已经具备哪些能力）

#### 10.1.1 UI 触发与结果展示（已存在，但字段不足）

前端已有：

- `frontend/src/app/rdagent/strategies-catalog/page.tsx`
  - 单策略“执行实时推理”触发。
  - 结果预览字段主要为 `rank/symbol/score`（字段不足，未包含名称/现价/涨幅）。
- `frontend/src/app/rdagent/multi-selection/page.tsx`
  - 多策略选股中心。
  - 支持：
    - 每个策略结果的复选框选择
    - 全选/取消全选
    - 弹窗“加入自选股池”，支持选择已有分类或新建分类

补充（可观测性/交互，已落地实现）：

- 多策略选股中心已接入后端 SSE：`/api/v1/rdagent/loops/{task_run_id}/{loop_id}/selection/stream`。
- 每个 loop 卡片都可以查看“实时选股日志”（包含资产加载、数据拉取、因子计算、模型加载、推理、TopK、行情/名称补齐等关键步骤）。
- **日志面板默认折叠**：触发选股不会自动展开日志；仅当用户点击按钮时才展开查看（避免多策略同时运行时 UI 过长）。

## 11. DB 性能与索引（TimescaleDB/PG）

本节用于落地“DB 耗时优化”的可执行方案。由于选股推理在严格模式下会进行大量按股票+日期窗口查询，若缺失合适索引，容易出现 `DB conn held`（连接持有时间过长）以及整体推理耗时过高。

### 11.1 推荐索引清单（与推理查询模式对齐）

- `market.daily_basic (ts_code, trade_date)`
- `market.moneyflow_ts (ts_code, trade_date)`
- `market.adj_factor (ts_code, trade_date)`
- `market.kline_daily_raw (trade_date)`
- `market.kline_daily_raw (ts_code, trade_date)`（hypertable，覆盖按标的拉窗口）
- `trading.strategy_version (strategy_id, version_tag)`
- `trading.rdagent_signal (strategy_id, trade_date, strategy_version_id, rank, score)`

说明：

- `kline_daily_raw` 为 Timescale hypertable，创建普通索引即可，Timescale 会为 chunk 建立对应索引。
- 以上索引使用 `CREATE INDEX CONCURRENTLY`，以降低对线上读写的影响（但仍会占用一定资源）。

### 11.2 一键创建脚本（从 .env 读取连接）

已新增脚本：`scripts/create_aistock_indexes.py`。

运行方式（会修改数据库结构，建议在低峰期执行）：

1. 安装依赖：`pip install psycopg2-binary`
2. 执行脚本：`python scripts/create_aistock_indexes.py`

脚本行为：

- 从项目根目录 `.env` 读取 `TDX_DB_HOST/TDX_DB_PORT/TDX_DB_NAME/TDX_DB_USER/TDX_DB_PASSWORD`。
- 对上述索引逐条执行 `CREATE INDEX CONCURRENTLY IF NOT EXISTS ...`。
- 每条 SQL 打印耗时与成功/失败原因，方便你回溯哪条索引创建失败。

补充（已在本项目环境执行验证）：

- **第一次执行失败原因**：Timescale hypertable 不支持 `CREATE INDEX CONCURRENTLY`，会报错 `hypertables do not support concurrent index creation`。
- **脚本已修正**：会自动检测 hypertable（读取 `timescaledb_information.hypertables`），对 hypertable 使用 `CREATE INDEX IF NOT EXISTS`（不带 concurrently），对普通表继续使用 `CREATE INDEX CONCURRENTLY IF NOT EXISTS`。
- **第二次执行结果**：7/7 全部成功。
  - `market.daily_basic(ts_code, trade_date)`：约 47s（hypertable）
  - `market.moneyflow_ts(ts_code, trade_date)`：约 43s（hypertable）
  - `market.kline_daily_raw(trade_date)`：约 5.6s（hypertable）
  - `market.kline_daily_raw(ts_code, trade_date)`：约 5.0s（hypertable）
  - 其余普通表索引：毫秒级（已存在或创建很快）

注意：

- hypertable 的非 concurrently 索引创建可能对写入造成影响，建议低峰执行。

### 11.3 索引是否生效的验证方式（必须做）

- 校验索引是否存在：
  - `SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes WHERE schemaname IN ('market','trading') AND tablename IN ('daily_basic','moneyflow_ts','adj_factor','kline_daily_raw','rdagent_signal','strategy_version') ORDER BY schemaname, tablename, indexname;`
- 校验查询是否命中索引（核心）：
  - 对推理常用 SQL（按 `ts_code + trade_date` 拉窗口、按 `trade_date` 求最新）执行 `EXPLAIN (ANALYZE, BUFFERS)`，确认出现 `Index Scan` 或 `Bitmap Index Scan`，而不是 `Seq Scan`。
- 性能回归观测：
  - 观察后端日志中的 `DB conn held` 出现频次与持有时长（索引完善后应显著下降）。

## 12. Loop/策略/模型可读显示名（替换 UUID 展示）的设计方案

### 12.1 问题定义

现状：

- 多处 UI/日志/选股中心使用 `task_run_id/loop_id/strategy_id/workspace_id/model_id` 等 UUID/长字符串做展示。
- 用户在选股/排障时很难通过这些 ID 快速建立“这是哪次实验/哪个目录”的直觉关联。

目标：

- **在不丢失可追溯性的前提下**，为 loop/策略/模型提供稳定、可读、可复制的展示名。
- 所有与 loop 关联的信息展示中，优先展示新的 loop 名（并保留原始 ID 作为 secondary 信息/tooltip）。

### 12.2 可用数据源与约束（基于代码事实）

- `aistock_loop_catalog` 已包含 `log_dir/log_uri`（可由 manifest 回填，也可来自 raw_payload）。
- `aistock_model_catalog` 已包含 `log_dir`（模型记录维度为 `(task_run_id, loop_id, workspace_id)`）。
- `aistock_strategy_catalog` 不保证有 `log_dir`，但通常能关联一个示例 loop（`example_task_run_id/example_loop_id`），可以间接获取该 loop 的 `log_dir`。

约束：

- 显示名必须 **确定性** 生成（同一条记录在不同页面生成结果一致）。
- 显示名不得作为主键参与业务逻辑（避免重命名导致引用断裂）；真实主键仍是原始 ID。

### 12.3 命名规则（推荐，确定性）

定义辅助函数：

- `log_run_name = basename(log_dir) 或 basename(log_uri) 或 short(task_run_id)`
  - `basename`：取路径最后一段（支持 Windows 路径与 URI）。
  - `short(x)`：对长 ID 截断显示（例如保留前 8-12 位）。

#### 12.3.1 LoopDisplayName

- `loop_display_name = {log_run_name}-loop{loop_id}`

示例：

- `F:/Dev/RD-Agent-main/log/2025-12-29_05-17-56-204326` + loop_id=0
  - `2025-12-29_05-17-56-204326-loop0`

当 `log_dir/log_uri` 缺失时：

- `loop_display_name = {short(task_run_id)}-loop{loop_id}`

#### 12.3.2 ModelDisplayName

模型是 loop 的子对象，推荐以 loop_display_name 为前缀：

- `model_display_name = {loop_display_name}-model-{short(workspace_id)}`

说明：

- 对模型来说，`workspace_id` 比 `model_id` 更稳定地体现“来源 workspace”，且数据库唯一键也包含 `workspace_id`。

#### 12.3.3 StrategyDisplayName

策略本质上是“模板/配置集合”，并不天然对应一个唯一 log_dir。推荐做法：

- 主展示：`strategy_display_name = strategy:{short(strategy_id)}`
- 若存在示例 loop，可附加示例 loop 名作为辅助：
  - `strategy_display_name = strategy:{short(strategy_id)} ({example_loop_display_name})`

这能保证：

- 即使没有 log_dir，也有稳定可读的策略名。
- 当用户从策略页跳转到 loops 页时，仍能看到与该策略相关的“那次实验”的可读名。

### 12.4 UI 展示替换范围（建议）

- `/rdagent/loops` 列表：
  - 原本展示 `task_run: short(task_run_id)` + `loop: loop_id`，替换为 `loop_display_name`。
  - 将原始 `task_run_id/loop_id` 放入 tooltip 或详情抽屉内保留。
- 多策略选股中心（loop 选股卡片）：
  - 卡片标题/日志前缀统一展示 `loop_display_name`。
  - 本地存储的 key 仍用 `{task_run_id}/{loop_id}`（保证引用稳定），展示名仅用于 UI。
- 策略目录页（/rdagent/strategies-catalog）：
  - 策略行展示 `strategy_display_name`；当需要展示 example loop 时显示 `example_loop_display_name`。
- 模型目录页（若存在）：
  - 展示 `model_display_name`，并在详情内保留 `model_id/workspace_id`。

### 12.5 落库 vs 动态生成（推荐策略）

推荐 **动态生成优先**：

- 不新增 DB 字段：展示名由后端 API 或前端 helper 在运行时生成。
- 优点：
  - 不引入迁移/回填成本。
  - log_dir 更新后展示名自动更新。
- 兼容性：
  - 对历史数据缺 `log_dir` 的情况自动回退到 `short(task_run_id)`。

可选增强（未来）：

- 若需要“可搜索/可固定显示名”，可在 catalog 表增加 `display_name` 字段并在同步时回填，但必须保留动态重算能力，避免历史显示名与真实 log_dir 脱节。

后端已有：

- `POST /api/v1/rdagent/strategies/{strategy_id}/inference`
  - 触发推理并将结果落库到 `trading.rdagent_signal`（用于 UI 结果预览/回看）。
- `POST /api/v1/rdagent/loops/{task_run_id}/{loop_id}/replay`
  - 基于 loop 的一键重放（按 `task_run_id + loop_id` 复用固化资产/历史配置）。
- `GET /api/v1/rdagent/signals/by_date ...`
  - 拉取某日信号/评分（用于前端结果预览）。

结论：

- AIstock 已具备“触发推理 -> 落库 -> 查询结果 -> UI 展示”的基本链路。
- 但 UI 展示字段与行情/名称补齐能力不足，且多策略页面尚未闭环使用 loop 推理链路。

#### 10.1.2 加入自选股票池（已存在，基本满足）

后端已有：

- `backend/routers/watchlist.py`
  - `POST /watchlist/categories`：新建分类
  - `POST /watchlist/items/bulk-add`：批量加入自选

服务层已有：

- `backend/services/watchlist_service.py`
  - 会尝试补齐股票名称（通过 `get_stock_basic_info` 等接口能力）。
  - 会尝试填入加入时价格（通过 `_fetch_quotes` 尝试获取实时行情）。

结论：

- 自选池 CRUD 与批量加入已经存在。
- 但其行情/名称来源与“选股结果展示”的强契约不完全一致（见 10.2.3）。

### 10.2 与业务要求的差距（必须改的点）

#### 10.2.1 “基于 loop 场景选股”目前没有在 UI 里真正闭环

现状：

- 选股中心主要仍按 `strategy_id` 调用 `/strategies/{id}/inference`。

业务要求：

- 必须复用某个 loop 的资产（`task_run_id + loop_id`）进行选股/模拟盘推理。

差距原因（基于代码事实）：

- 虽已存在 `/loops/{task_run_id}/{loop_id}/replay`，但前端未以“loop 为主键”构建选股结果聚合返回，导致 UI 侧需要自行拼装行情/名称，且无法保证数据源策略一致。

#### 10.2.2 选股结果字段不满足要求

业务要求的结果列表必须包含：

- 股票代码
- 股票名称
- 当前价格
- 当前涨幅（非交易日/非开盘：最近交易日涨幅；开盘：miniQMT 实时）
- 评分分数（score）
- 支持排序（升/降序）

现状：

- 前端结果仅有 `symbol/rank/score`，没有 `name/price/pct_change`。

#### 10.2.3 行情源切换逻辑目前不符合“交易时段实时/非交易日回退”的强契约

现状（基于代码事实）：

- `backend/services/watchlist_service.py` 内的行情获取使用的是 `data_source_manager.get_realtime_quotes`，其实现优先 TDX，其次 Tushare（并非强制优先 miniQMT/xtquant）。
- AIstock 实际已存在“优先 xtquant、失败再 TDX”的统一行情能力：
  - `backend/data_service/api.py::get_realtime_snapshot`（Primary: xtquant；Fallback: TDX；严格模式下两者都失败会抛错）。

业务要求：

- 交易日开盘期间必须优先使用 miniQMT（xtquant）实时行情展示。
- 非交易时段/休市/数据源不可达必须回退到最近交易日（日线 close/pct）。

差距结论：

- “选股结果展示”的行情/名称来源必须在新聚合接口中统一，实现与第 2.4 节一致的强契约。

#### 10.2.4 排序要求未实现（前端必须提供表格排序）

现状：

- `multi-selection/page.tsx` 没有对结果表格提供列头排序交互。

要求：

- 需要支持按 `code/symbol/name/price/pct_change/score/rank` 多字段升降序排序。

#### 10.2.5 默认分类名称（策略名 + 日期）未自动填充

现状：

- 弹窗中新建分类名 `newCategoryName` 为空，需要用户手工输入。

要求：

- 默认值必须为：`{策略名或strategy_id}_{YYYY-MM-DD}`，且允许编辑。

### 10.3 方案总览：目标数据流与责任边界

核心原则：

- **推理与候选生成**：只依赖资产与模型，不依赖行情可用性。
- **结果展示字段（name/price/pct_change）**：由 AIstock 后端在“选股结果聚合 API”中统一补齐，并严格按第 2.4 节的数据源策略执行。
- **UI**：只负责触发与展示，不在浏览器侧做跨源行情拼装，避免重复请求与口径不一致。

责任划分：

- RD-Agent 侧：保证 loop 资产包可被 AIstock 确定性加载（manifest + factor_entry.py + model.pkl），并提供可下载的 results/bundle。
- AIstock 侧：负责
  - 资产加载（manifest 优先）
  - loop 回放推理
  - Top50 结果聚合与行情/名称补齐
  - UI 触发、排序展示、加入自选池
  - 同步工作流（全量初始化 + UI 增量）

### 10.4 AIstock 后端改造（必须实现）

#### 10.4.1 新增“选股结果聚合 API”（核心）

新增接口（建议放在 `backend/routers/rdagent.py`）：

- `POST /api/v1/rdagent/loops/{task_run_id}/{loop_id}/selection`

行为（严格顺序）：

1. **触发 loop 回放推理**
   - 复用现有 loop 推理能力（对应 `/loops/{task_run_id}/{loop_id}/replay` 的内部实现）。
2. **从评分表读取 Top50**
   - 读取 `trading.rdagent_signal` 的 `symbol/rank/score`。
   - 默认 `top_k=50`（见第 2.4 节）。
   - `as_of_date` 必须明确（用于行情回退的基准与 UI 展示）。
3. **批量补齐名称（name，优先 TDX）**
   - 优先从 TDX 基本信息/搜索能力获取名称；失败允许为空但不得影响返回。
4. **批量补齐行情（price/pct_change，优先 miniQMT）**
   - 交易时段：优先 miniQMT（xtquant）实时快照；失败回退 TDX；仍失败再回退最近交易日。
   - 非交易时段：直接回退最近交易日（日线 close/pct）。
   - 必须返回 `quote_source` 与 `quote_time` 以便可诊断。
5. **返回 rows**
   - 每行字段至少包含（见第 2.4 节）：
     - `symbol, name, price, pct_change, score, rank, quote_source, quote_time`
   - 可选增加：`pre_close`（用于解释 pct 的计算口径）。

注意：

- `backend/data_service/api.py::get_realtime_snapshot` 为严格模式：当 xtquant 与 TDX 都失败会抛错。
- 为满足“选股结果必须可展示”的业务要求，本接口必须捕获该错误并回退最近交易日，同时将 `quote_source` 标为 `fallback_trade_day`。

#### 10.4.2 交易时段/最近交易日判定（必须可解释）

实现要求：

- 交易时段判定必须为明确规则（例如按本地时区与交易日历）。
- 最近交易日必须可解释且可复现：
  - 优先基于统一数据源的交易日历；
  - 若不可用，至少基于“向前回退 N 天查询日线数据，取第一条有效交易日”的策略。

输出要求：

- 在接口返回中包含 `as_of_date` 或 `quote_time`，使前端可明确展示“行情来源与时间”。

### 10.5 AIstock 前端改造（必须实现）

#### 10.5.1 选股中心改为“按 loop 触发”

修改文件：`frontend/src/app/rdagent/multi-selection/page.tsx`

改造点：

- 点击“执行选股”时，不再调用 strategy inference，而是调用：
  - `POST /api/v1/rdagent/loops/{task_run_id}/{loop_id}/selection`
- 表格展示列：
  - `代码(symbol)`
  - `名称(name)`
  - `现价(price)`
  - `涨幅(pct_change)`
  - `分数(score)`
  - `排名(rank)`
  - （可选）`行情来源(quote_source)`、`行情时间(quote_time)`（建议在 tooltip 或次级列展示，便于排障）
- 复选框/全选逻辑沿用现有实现（已存在）。

#### 10.5.2 结果表格排序（必须实现）

实现要求：

- 支持列头点击排序（升序/降序切换）。
- 支持排序字段：`symbol/name/price/pct_change/score/rank`。
- 排序在前端本地执行即可（Top50 数据量小），但必须保证：
  - 数值字段按数值排序，空值（None）有确定位置（例如始终排最后）。

#### 10.5.3 加入自选池：默认分类名与回溯信息

修改文件：`frontend/src/app/rdagent/multi-selection/page.tsx`

要求：

- 弹窗打开时默认设置：`newCategoryName = {策略名或strategy_id}_{YYYY-MM-DD}`。
- 批量加入自选继续使用：`POST /watchlist/items/bulk-add`。
- 建议写入回溯信息（需与后端字段能力匹配）：
  - `task_run_id/loop_id/as_of_date/score`（用于后续审计与回看）。

### 10.6 同步功能：全量初始化 + UI 增量（现状与缺口）

业务要求：

- 首次全量初始化同步：允许通过脚本/文件拷贝完成。
- 后续增量同步：必须支持 UI 触发（调用 RD-Agent API 并写入 AIstock Catalog/Bundle）。

新增强约束（资产同步介质）：

- **首次手工初始化全同步期间（Full Init）**：文件资产**不打包成压缩包**，改为**直接目录拷贝**（例如从 RD-Agent 侧的 `production_bundles/{asset_bundle_id}/` 直接拷贝到 AIstock 本地缓存目录）。
- **后续增量同步期间（Incremental）**：继续使用“打包（zip）-> 下载 -> 解压”的方式执行资产同步，以降低增量传输成本并便于按 loop 粒度更新。

后端现状（基于代码事实）：

- 已存在同步管理接口：`backend/routers/rdagent_sync_admin.py`
  - `POST /api/v1/rdagent/sync/run` 支持 `mode=incremental`、`materialize_and_sync` 等
  - `GET /api/v1/rdagent/sync/status` 查询状态

缺口：

- 前端尚无“同步控制台/按钮/进度展示”入口页面。
- 文档需要把“全量初始化步骤”与“增量同步 UI 操作步骤”补齐为可执行手册（见后续文档交付清单）。

#### 10.6.1 全量初始化同步（手工模式，目录拷贝）

目标：

- 在第一次部署或第一次接入 RD-Agent Phase2 数据时，通过“文件系统拷贝”的方式一次性把历史 bundles 与 catalogs 导入 AIstock。

流程约定：

1. **Catalog（结构化数据）初始化**
   - 由脚本/一次性任务完成（JSON/DB 导入），将 `aistock_factor_catalog / aistock_strategy_catalog / aistock_loop_catalog` 初始化到目标 PG。
2. **资产目录拷贝（不打包）**
   - 将 RD-Agent 侧产物目录 `production_bundles/{asset_bundle_id}/` 直接拷贝到 AIstock 侧本地缓存目录（保持目录结构不变，包含 `manifest.json` 与 `workspaces/**`）。
3. **一致性校验**
   - 对每个 loop 记录（含 `asset_bundle_id`）校验：
     - 本地缓存目录存在
     - `manifest.json` 可解析且 `schema_version` 支持
     - `primary_assets.factor_entry_relpath` 与 `primary_assets.model_weight_relpath` 指向的文件存在

验收要点：

- 全量初始化后，对任意一个 solidified loop 调用选股推理（或回放推理）无需再依赖 Results API 下载 zip，即可直接从本地缓存加载资产。

#### 10.6.1.1 全量初始化同步（脚本化执行，推荐）

本小节将“10.6.1 全量初始化同步”固化为可重复执行的脚本流程。

要求（强约束）：

- 必须明确哪些命令在 **RD-Agent 侧**执行，哪些命令在 **AIstock 侧**执行。
- 全量初始化阶段资产同步介质必须为“目录拷贝”，不走 zip。
- 脚本文件位置必须固定：
  - RD-Agent 侧：`RD-Agent-main/tools/`
  - AIstock 侧：`AIstock/tools/`

##### A. 前置条件检查（RD-Agent 侧执行）

本步骤的目的：确认 RD-Agent 侧已有 `production_bundles` 目录，且其中 bundle 基本结构完整（至少包含 `manifest.json`）。

1. 打开 PowerShell，执行：

```powershell
powershell -ExecutionPolicy Bypass -File F:\Dev\AIstock\RD-Agent-main\tools\rdagent_full_init_prepare.ps1 `
  -RDAgentBundlesDir "F:\Dev\RD-Agent-main\RDagentDB\production_bundles"
```

1. 脚本会输出：

- 实际解析到的 `production_bundles` 路径
- bundle 数量
- 是否存在缺失 `manifest.json` 的 bundle（仅提示，不会中断）
- 下一步在 AIstock 侧执行的推荐命令

> 说明：如果你本机 RD-Agent 的 `production_bundles` 不在示例路径，请把 `-RDAgentBundlesDir` 改为你的真实路径。

##### B. 执行 Catalog 全量初始化 + bundles 目录拷贝（AIstock 侧执行）

本步骤的目的：

- 通过 AIstock 后端接口触发 RD-Agent Catalog 的全量刷新（只同步结构化数据，不下载 zip）。
- 通过文件系统把 RD-Agent 侧历史 `production_bundles` 直接拷贝到 AIstock 本地缓存目录。

1. 确保 AIstock 后端已启动并可访问（默认）：

- `http://127.0.0.1:8001/api/v1`

1. 在 PowerShell 中执行（AIstock 侧 tools 脚本）：

```powershell
powershell -ExecutionPolicy Bypass -File F:\Dev\AIstock\tools\rdagent_full_init_sync.ps1 `
  -RDAgentBundlesDir "F:\Dev\RD-Agent-main\RDagentDB\production_bundles"
```

可选参数：

- `-ApiBase "http://127.0.0.1:8001/api/v1"`：覆盖默认 API Base。
- `-AIstockBundlesDir "F:\Dev\AIstock\backend\data\rdagent_assets\production_bundles"`：覆盖默认 bundles 目标目录。
- `-SkipHttpCatalogSync`：仅拷贝 bundles，不触发 Catalog 全量刷新（一般不建议）。

1. 脚本执行内容说明（按顺序）：

- Step1（HTTP）：调用 `POST /api/v1/rdagent/sync/run`，参数：
  - `mode=full_refresh`
  - `clean=true`
  - `sync_metadata_only=true`
  - `sync_assets_only=false`
  - 目的：只初始化/刷新 catalog 数据（PG 表），不触发 zip 下载。
- Step2（文件）：把 RD-Agent `production_bundles/*` 递归拷贝到 AIstock 本地缓存 `backend/data/rdagent_assets/production_bundles/`。
- Step3（校验）：对每个 bundle 目录检查 `manifest.json` 是否存在（仅提示，不中断）。

1. 执行后建议在浏览器打开（AIstock 侧）：

- `http://127.0.0.1:3000/rdagent/sync`

确认同步状态为 `success`（或查看错误原因）。

##### C. 验收（AIstock 侧执行）

本步骤的目的：确认“全量初始化同步”后，AIstock 推理/选股不再依赖 Results API zip 下载。

建议验收动作：

1. 在 AIstock 前端：

- 打开 `RD-Agent 实验 / loop 目录` 页面，确认 loop 记录存在且 `asset_bundle_id` 有值（若你的 RD-Agent 侧已固化）。

1. 在 AIstock 前端：

- 打开“多策略选股中心”，对任意一个已固化 loop 执行选股，确保能返回 Top50 结果。

1. 观察本地目录：

- `backend/data/rdagent_assets/production_bundles/{asset_bundle_id}/manifest.json` 存在。

#### 10.6.2 增量同步（UI 触发模式，zip 包）

目标：

- 日常使用中，按需拉取 RD-Agent 新产出的 loop/catalog 与对应资产包，更新 AIstock 侧数据库与本地缓存。

流程约定：

1. UI 触发增量同步任务
2. 后端调用 RD-Agent API 获取增量 catalog（或 materialize_and_sync）
3. 对新增/变更的 `asset_bundle_id`：
   - 通过 Results API 下载 zip
   - 解压到本地缓存目录（覆盖同 bundle_id 或按版本策略管理）
4. 同步完成后回填 `aistock_loop_catalog` 的 manifest 摘要字段（见 8.3 节）

验收要点：

- 增量同步后，新 loop 的资产可被 AIstock 推理链路按 manifest 精确加载。

#### 10.6.2.1 增量同步（UI 操作步骤，推荐）

本小节将“10.6.2 增量同步”固化为可重复执行的 UI 操作流程。

目标：

- **UI 侧一键触发**增量同步任务。
- **后端严格走 zip**：对新增/变更的 `asset_bundle_id`，执行“下载 zip -> 解压到本地缓存目录”。
- **可观测**：同步过程中可看到 `phase/progress`（阶段与进度）。

##### A. 前置条件（AIstock 侧执行）

本步骤的目的：确保 AIstock 能访问 RD-Agent Results API（用于拉取增量与下载 zip）。

1. 确保 AIstock 后端已启动。
1. 确保环境变量（或默认值）指向正确的 Results API：
   - `RDAGENT_RESULTS_API_BASE_URL`（默认 `http://127.0.0.1:9000`）
1. 确保 Results API 可访问（能响应 `/catalog/incremental` 与 `/artifacts/bundle/{asset_bundle_id}`）。

##### B. UI 触发增量同步（AIstock 侧执行）

1. 打开页面：`/rdagent/sync`
1. 点击按钮：`一键增量同步（zip）`
   - 该按钮会强制使用 `mode=incremental`
   - 并忽略 `clean / 仅同步结构化数据 / 仅下载资产包` 等选项，避免误操作
1. 观察页面状态区：
   - `当前阶段(phase)`：例如 `fetching_incremental / importing_meta / processing_loop_i/n` 等
   - `进度(progress)`：0.0 ~ 1.0
1. 同步完成后：
   - `状态(state)` 变为 `success` 或 `failed`
   - “最近一次同步结果汇总”展示本次导入统计

##### C. 增量同步后的资产落盘验证（AIstock 侧执行）

1. 找到某个新增/变更 loop 的 `asset_bundle_id`
1. 验证本地目录存在：
   - `backend/data/rdagent_assets/production_bundles/{asset_bundle_id}/`
1. 验证关键文件存在：
   - `manifest.json`
   - `workspaces/**` 以及 `primary_assets.*` 指向的目标文件

### 10.7 预计改动文件清单（用于评审与控范围）

后端（预计）：

- `backend/routers/rdagent.py`
  - 新增：`POST /api/v1/rdagent/loops/{task_run_id}/{loop_id}/selection`
- `backend/services/...`
  - 新增：选股结果聚合服务（封装 Top50 读取、名称补齐、行情补齐、交易时段判断、最近交易日回退）
- （复用）`backend/data_service/api.py`
  - 复用：`get_realtime_snapshot`（优先 xtquant，失败 TDX）

前端（预计）：

- `frontend/src/app/rdagent/multi-selection/page.tsx`
  - 接入 loop selection API
  - 表格列扩展（name/price/pct_change）
  - 列头排序
  - 默认分类名填充

文档（预计）：

- 本文档（本节）作为“选股功能落地方案”的权威来源。
- 后续将新增/扩展操作手册：全量初始化同步、增量同步、选股 UI 操作手册、验收方案、文件变更清单。

### 10.8 严格验收标准（必须基于实际数据/实际代码路径验证）

#### 10.8.1 选股结果 API 验收

- 调用 `POST /api/v1/rdagent/loops/{task_run_id}/{loop_id}/selection`：
  - 必须返回 Top50。
  - 每行必须包含 `symbol/score/rank`，且与 `trading.rdagent_signal` 中对应记录一致。
  - 行情字段必须符合第 2.4 节策略：
    - 交易时段：优先 `quote_source=miniqmt`；miniQMT 不可用时允许 `tdx`；仍失败必须 `fallback_trade_day`。
    - 非交易时段：必须允许 `fallback_trade_day`，并且 `price/pct_change` 与最近交易日数据一致。

#### 10.8.2 前端 UI 验收

- 选股中心可按 loop 触发选股，显示列：代码/名称/现价/涨幅/分数/排名。
- 支持列头排序（数值排序正确，空值处理一致）。
- 复选框/全选可用。
- 加入自选池：默认分类名为“策略名或strategy_id + 日期”，可编辑；批量加入成功且可在自选池页面查到。

#### 10.8.3 同步流程验收

- 后端同步接口可查询状态并触发 `mode=incremental`。
- UI 增量同步入口上线后：点击可触发同步并展示进度/结果（失败可诊断）。

---

## 附：本次分析引用的关键代码位置

- AIstock 资产加载：`backend/inference_engine.py::_load_strategy_assets`
- AIstock bundle 文件定位：`backend/services/rdagent_asset_service.py::get_strategy_files`
- RD-Agent 固化打包：`RD-Agent-main/rdagent/utils/solidification.py::solidify_loop_assets`

---

## 11. 交付物进度更新（以本文档为准）

本节用于将“设计方案要求的交付物”与“当前实际落地情况”统一对齐，便于验收。

### 11.1 当前进度（里程碑）

- 全量初始化同步（脚本化）已完成：
  - RD-Agent 侧 tools 脚本 + AIstock 侧 tools 脚本
  - 文档已补齐可执行步骤（明确 RD-Agent vs AIstock 执行环境）
- 增量同步（UI 触发，zip）已完成：
  - 同步页支持展示 `phase/progress`
  - 提供 `一键增量同步（zip）` 按钮与操作步骤

### 11.2 修改文件清单（修改目标 / 修改内容 / 功能描述）

#### 11.2.1 AIstock 侧

1. `backend/routers/rdagent_sync_admin.py`
   - 修改目标：对齐前端/脚本参数，保证“全量重装(clean)”语义可用。
   - 修改内容：
     - `POST /api/v1/rdagent/sync/run` 新增接收 `clean` 字段
     - 当 `clean=true` 时，强制等价为 `mode=full_refresh`
   - 功能描述：避免 UI 传参导致后端忽略 clean，引发“用户以为全量重装但实际没有”这种严重一致性问题。

1. `frontend/src/app/rdagent/sync/page.tsx`
   - 修改目标：复用现有同步界面实现“增量同步 UI 触发（zip）”与可观测性。
   - 修改内容：
     - 同步状态展示新增：`phase/progress` + 进度条
     - 新增按钮：`一键增量同步（zip）`，强制 `mode=incremental` 并忽略 clean/互斥选项
     - 增强可访问性：同步模式下拉框补齐 `aria-label/title`
   - 功能描述：让增量同步从 UI 可一键触发、且用户能看到“当前阶段/进度”，并明确走 zip 增量链路。

1. `tools/rdagent_full_init_sync.ps1`
   - 修改目标：将“全量初始化同步”变成可重复执行的脚本闭环。
   - 修改内容：
     - 触发 Catalog 全量刷新（HTTP：`mode=full_refresh + sync_metadata_only=true`）
     - 目录拷贝 bundles 到 `backend/data/rdagent_assets/production_bundles`
     - 基础校验 `manifest.json` 是否存在
   - 功能描述：首次接入时不走 zip，直接目录拷贝，降低初始化复杂度与失败概率。

1. `docs/AIstock选股推理资产保障详细分析.md`
   - 修改目标：把“同步功能（全量初始化 + UI 增量）”补齐为可执行操作手册，并补齐交付物清单。
   - 修改内容：
     - 新增：`10.6.1.1 全量初始化同步（脚本化执行，推荐）`
     - 新增：`10.6.2.1 增量同步（UI 操作步骤，推荐）`
     - 新增：第 11 章交付物进度更新与修改文件清单
   - 功能描述：提供端到端可执行步骤、明确两端执行环境、并沉淀验收口径。

#### 11.2.2 RD-Agent 侧

1. `RD-Agent-main/tools/rdagent_full_init_prepare.ps1`
   - 修改目标：在 RD-Agent 侧对“全量初始化”的拷贝源目录做准备检查。
   - 修改内容：
     - 校验 `production_bundles` 源目录存在
     - 统计 bundle 数量与 `manifest.json` 存在性
     - 输出 AIstock 侧下一步执行命令
   - 功能描述：减少路径配置错误与拷贝源不完整导致的初始化失败。

### 11.3 文档交付物完备性核对清单

以下清单以“本文档中的设计方案要求”为准，逐项核对：

- 全量同步初始化（脚本化）操作步骤：已补齐（10.6.1.1）。
- 增量同步（UI 触发，zip）操作步骤：已补齐（10.6.2.1）。
- 修改文件清单（目标/内容/功能）：已补齐（11.2）。
- 进度更新（已完成/待办）：已补齐（11.1）。

---

## 12. 本轮进展更新（2026-01-13）（以本文档为准）

本节用于记录“从 RD-Agent log 反推并回填 registry.workspace 信息”相关工作的**实际落地进展**，包括代码修改汇总与下一步计划。

### 12.1 已完成：代码修改汇总（按仓库）

#### 12.1.1 AIstock 侧

1. `backend/routers/rdagent_catalog_admin.py`
   - 修改目标：为诊断/回填/资产缺失排查提供“按 asset_bundle_id 下载并解压资产包”的管理接口。
   - 修改内容：新增下载并解压接口（用于快速定位某个失败 loop/资产包的缺失文件）。
   - 当前状态：已合入。

1. `backend/data/rdagent_assets/production_bundles/.../workspaces/<workspace_id>/factor_entry.py`
   - 修改目标：将回放型 parquet 重放入口替换为实盘计算入口，且缺失字段必须硬失败。
   - 修改内容：实现基于 `df_history` 的因子计算入口，并对必需特征字段缺失做严格校验，避免 silent fallback 生成全 NaN。
   - 当前状态：已用于问题 case 的诊断与修复验证。

1. `docs/AIstock选股推理资产保障详细分析.md`
   - 修改目标：补齐“logs 为权威来源”与“资产合同/同步流程”的事实依据。
   - 修改内容：新增/修订 8.7 节相关内容（以 logs 枚举 workspace 为权威的原则、以及 manifest/log_dir 对排障的重要性）。
   - 当前状态：已合入，本节为追加更新。

#### 12.1.2 RD-Agent 侧

1. `rdagent/log/storage.py`
   - 修改目标：解决 Windows 下反序列化包含 `pathlib.PosixPath` 的 pkl 日志失败问题。
   - 修改内容：为 `FileStorage.iter_msg()` 引入兼容 Unpickler，将 `PosixPath/PurePosixPath` 映射为本机 `Path/PurePath`。
   - 直接效果：Windows 可稳定遍历 `log/<session>/**/*.pkl`，从而支持从 log 解析 loop/workspace。

1. `tools/backfill_registry_workspaces_from_logs.py`
   - 修改目标：扫描 RD-Agent log 目录，对 `RDagentDB/registry.sqlite` 中记录的所有 loop（可选仅 has_result=1），从 log 中解析其全量 `workspace_paths`，并在**备份 DB 后**回填到 `workspaces/loops.log_dir/task_runs.log_trace_path`，作为未来打包的唯一依据。
   - 修改内容（关键事实）：
     - 以 registry 中 `loops` 为主驱动，定位 log session（优先使用 registry 的 log_dir/log_trace_path，不足则扫描 `--logs-root` 建索引）。
     - 对每个 `(task_run_id, loop_id)` 调用 `tools/backfill_registry_artifacts.py::_collect_log_session_loops(session_dir)` 提取 `workspace_paths`。
     - 写库前对 `workspace_path` 做规范化（去掉日志字符串中潜在的换行/空白），避免同一路径重复入库。
     - 写库时补齐 `task_runs.log_trace_path` 与 `loops.log_dir`（使用 `resolve()` 的绝对路径）。
   - 当前状态：`--dry-run` 可在本机跑通；真实写库待用户触发。

1. `rdagent/utils/solidification.py`
   - 修改目标：资产固化打包阶段仅依赖 registry.workspaces 作为 workspace 枚举来源（避免运行期从 log 再解析造成不一致）。
   - 修改内容：已将 workspace 枚举来源恢复为 registry.workspaces（配套要求：打包前必须先 backfill）。

### 12.2 当前现状（阻塞点与已验证事实）

1. **Windows 直接 `pickle.load(__session__/0/1_coding)` 会失败**：错误为 `pathlib._abc.UnsupportedOperation: cannot instantiate 'PosixPath'`。
   - 已用 `rdagent.log.storage._CompatUnpickler` 验证可在 Windows 成功加载 session。

1. **log session 内确实存在 `__session__/<n>/1_coding`**，且不同 `<n>` 表示同一任务的不同快照；需要选择包含完整 trace.hist 的快照进行解析。

### 12.3 下一步计划（必须落地的任务）

1. 执行回填真实写库（由你触发）：
   - 命令：`python tools/backfill_registry_workspaces_from_logs.py --db RDagentDB/registry.sqlite --logs-root log --only-has-result --backup`
   - 验收：
     - `workspaces` 表中每个 loop 的 workspace_path 数量与 log 解析一致
     - `task_runs.log_trace_path` 与 `loops.log_dir` 不再为空

1. 将“全量/增量资产同步流程”补充为硬性步骤：固化/同步前必须先 backfill。

1. 基于回填后的 workspace 清单，核验本 case 与抽样 loop 的资产包是否包含实盘推理所需文件（入口/模型/配置/依赖），输出缺失项清单。

---

## 13. 方案评估：是否可改为“以 task 为单位（SOTA 因子 + 最终模型）”执行 AIstock 选股（严格基于事实）

本节基于以下两类事实来源给出结论：

1. 文档：`F:\Dev\RD-Agent-main\SOTA因子完整分析文档_v2.md`
2. 代码与实测：
   - Windows 下使用 `rdagent.log.storage._CompatUnpickler` 成功加载 `log/<task_session>/__session__/<n>/1_coding` 并检查 `trace.hist`。
   - 抽样 session：`log/2026-01-10_15-50-13-970961/__session__/9/1_coding`。

### 13.1 文档声称的“task 级复用”数据来源（来自 SOTA 文档）

SOTA 文档明确给出：

- SOTA 因子与模型的权威位置：`log/<task>/__session__/*/1_coding`（从 `session.trace.hist` 中筛选 `feedback.decision=True` 的实验）。
- SOTA 因子代码可从 `exp.sub_workspace_list[i].file_dict['factor.py']` 获取。
- SOTA 模型的选择逻辑为“倒序取第一个 `decision=True` 的模型实验”。
- 每个实验的回测指标记录在 `exp.result`。

### 13.2 抽样 log 的事实验证结果（不做推测）

对 `log/2026-01-10_15-50-13-970961/__session__/9/1_coding` 进行实际解包检查，得到事实：

1. `trace.hist` 确实存在，且长度为 6。
1. `decision=True` 的实验存在（共 2 个）：
   - 一个 `QlibModelExperiment`
   - 一个 `QlibFactorExperiment`
1. 对这两个 `decision=True` 实验，`sub_workspace_list[0].file_dict` 的实际 keys 为：
   - `QlibModelExperiment`：仅包含 `model.py`（字符串）
   - `QlibFactorExperiment`：仅包含 `factor.py`（字符串）
1. 在该 session 的 `file_dict` 中**未发现任何模型权重文件**（例如 `model.pkl/params.pkl/*.pth/*.ckpt` 等）。
1. 但对应的 `experiment_workspace.workspace_path` 在本机存在，且该 workspace 下存在 `mlruns/` 目录（权重位于 mlruns 的可能性需要从文件系统进一步确认）。

上述结论意味着：

- **回测表现/指标维度**：可仅通过 `log/__session__/.../1_coding` 中的 `exp.result` 获取（无需读取 workspace 文件）。
- **源码维度**：可仅通过 `file_dict` 获取 `factor.py/model.py`（无需读取 workspace 文件）。
- **模型权重维度**：在该抽样 task 的 session 事实中，`file_dict` 不包含权重二进制，因此**仅靠 log session 不能完成 AIstock 侧的模型推理**。

### 13.3 “以 task 为单位选股、弱化 loop”的可行性结论（基于事实）

在当前事实约束下：

1. **可以弱化 loop 的部分**（可行）：
   - **不需要逐 loop 读取回测结果文件**来获取指标：`exp.result` 已在 session 中给出。
   - **不需要逐 loop 采集策略演进信息**：若 RD-Agent 侧本身不做策略演进（策略来自模板），则 AIstock 侧可以只在“策略模板变更”时同步策略；这点不依赖 loop 细节。

1. **无法只依赖 log 完成 AIstock 侧“实盘推理/选股”的部分**（不可行，事实阻塞）：
   - 抽样 task 的 session 中，`file_dict` **不包含模型权重文件**。
   - 因此“仅从 log 获取 SOTA 因子 + 最终模型”这一口径，**不能构成 AIstock 侧可执行的完整推理资产闭环**（至少缺少权重）。

### 13.4 若要落地 task 级选股，必须满足的硬性前置条件（基于事实推导出的缺口清单）

要使 AIstock 能以 task 成果作为选股/实盘依据，必须在数据来源上满足：

1. **模型权重必须可被稳定获取**（二选一满足即可）：
   - A. session `file_dict` 中包含权重二进制（当前抽样不满足）；
   - B. log/manifest 能给出“权重文件在 workspace 文件系统中的稳定定位规则”，并且 AIstock 有权限访问该路径；
   - C. 由 RD-Agent 固化/导出时将“最终 SOTA 模型权重”复制进资产包并在 manifest 显式指向。

1. **因子入口必须可用于实盘计算**：即便 session 提供 `factor.py` 源码字符串，AIstock 侧仍需一个可 import 的入口文件（例如约定 `factor_entry.py` + `compute(df_history)`）。

---

本节结论不基于推测；仅基于已读取的 SOTA 文档内容与抽样 log session 的实际解包结果。

---

## 14. 新方案（提案）：以 task 为单位的数据获取与选股运行（基于 log/workspace，不依赖 loop/sqlite）

本章在第 13 章“事实验证”的基础上，给出一套可落地的新方案：

1. **以 task 为第一公民**：AIstock 的结构化数据与资产采集，以 task 为单位组织与展示。
1. **logs/workspace 为权威来源**：以 `RD-Agent log/<task>/__session__/*/1_coding` 反推出 SOTA 实验列表；必要资产（训练配置/权重/数据快照）从对应 workspace 的 `mlruns/**/artifacts/` 获取。
1. **不再依赖原方案的“运行期 loop 写标识 + 本地 sqlite”**：本章方案不以 `registry.sqlite` 为必需输入（loop 表结构暂保留但不作为 task 关联来源）。
1. **因子库与模型库持续沉淀资产**：因子与模型的源码、配置、关键指标均需入库；未来允许“基于某个 SOTA 模型类型 + 人工挑选因子集合”重新训练并用于实盘选股。

本章的目标不是否定原方案，而是：在不损失可追溯性的前提下，将“推理可运行闭环”从 `loop` 迁移到 `task`。

### 14.1 新方案的数据来源与权威性定义

新方案的数据来源分为两层：

1. **决策层（task 的 SOTA 列表与指标）**：
   - 来源：`log/<task>/__session__/*/1_coding` 反序列化后的 `session.trace.hist`。
   - 用途：
     - 解析 `decision=True` 的实验集合（SOTA 因子 / SOTA 模型）。
     - 获取每个实验的 `exp.result` 指标（无需读取任何回测结果文件）。

1. **资产层（训练配置/权重/源码/必要文件）**：
   - 来源：`exp.experiment_workspace.workspace_path` 指向的 workspace（本机文件系统）以及其 `mlruns/**/artifacts/`。
   - 用途：
     - 采集模型的训练配置 `artifacts/task`、参数/权重 `artifacts/params.pkl`、以及相关 config。
     - 采集因子/模型的源码（优先 `file_dict['factor.py'/'model.py']`，缺失时再从 workspace 文件读取）。

关键约束（必须遵守）：

1. **严禁将“回测预测产物”作为训练资产入库**：必须排除 `pred.pkl`、`signals.*`、`portfolio_analysis/`、`sig_analysis/`、`ret.pkl` 等。
1. **训练资产采用白名单采集**：仅采集被定义为“训练/配置/权重”的资产，避免命名变化导致漏排。

#### 14.1.1 补充：SOTA 因子源码的权威来源（runner result）

本次确认的事实更新如下：

1. **SOTA 因子“名单”与源码优先从 log 的 `runner result` 获取**：
   - 在 `runner result` 的 `QlibFactorExperiment` 中读取 `prop_dev_feedback.feedback_list`，筛选 `final_decision=True`（兼容 `decision=True`）。
   - 对应的因子源码位于 `sub_workspace_list[idx].file_dict['factor.py'/'factor_entry.py']`。
1. **对同一 task 的所有 SOTA 因子源码必须完整落盘**：
   - AIstock 同步时将其统一存入 `task_dir/sota_factors/`，并在 manifest 的 `extra_assets.sota_factor_relpaths` 记录。
   - 入口推理仍以“最后一个 SOTA 因子”为主（用于生成主 `factor_entry.py`），但**所有 SOTA 因子源码必须可追溯**，以支撑后续因子库沉淀与复用。
1. **回退规则**：若 session `trace.hist` 对应因子实验的 `file_dict` 缺失，则使用上述 runner result 的源码作为同步回退来源（只做源码回退，不改变 SOTA 判定）。

### 14.2 因子库：保留 alpha 因子 + 所有进入过 SOTA 的因子（去重 + 最佳指标 + 源码沉淀）

#### 14.2.1 保留范围

因子库必须长期保留两大类：

1. **所有 alpha 因子**：例如 `alpha158/alpha360` 全量。
1. **所有进入过 SOTA 列表的因子**（非 alpha，自研/演进因子）：只要在任何 task 的 `decision=True` 因子实验中出现过，就必须被保留。

#### 14.2.2 去重规则（完全相同因子去重）

同一“逻辑等价”的因子可能在多个 task 反复进入 SOTA。必须去重以避免因子库膨胀与 UI 误导。

建议的去重键（按优先级）：

1. `factor_key = (source, factor_name, expression)`：最稳定、可解释。
1. 若 `expression` 不可用：`factor_key = (source, factor_name, impl_module, impl_func, impl_version)`。
1. 必要时补充指纹：对 `expression` 或 `raw_payload` 计算 hash（例如 `sha1(normalized_expression)`），作为 `expression_fingerprint` 字段。

去重策略：

1. 因子“元信息”以去重键为唯一记录。
1. 因子“在不同 task 的绩效”应独立记录（见 14.3 的 SOTA 因子明细表）。

#### 14.2.3 因子源码沉淀（AIstock 侧必须存储）

每个因子必须在 AIstock 侧保存可追溯源码，用于未来：

1. 人工挑选因子做选股/回测；
1. 与某个 SOTA 模型类型组合重新训练；
1. 资产审计与复现。

建议字段：

1. `source_code_py`（TEXT）：`factor.py`/`factor_entry.py` 源码字符串。
1. `source_code_origin`（TEXT）：`file_dict|workspace_file|bundle_file`。
1. `source_code_sha1`（TEXT）：源码指纹（用于去重与变更追踪）。

### 14.3 SOTA 因子明细表：记录“因子在 task 中的最佳回测指标 + 来源 task”

因子库是“全局去重后的因子字典”，但你要求还需要：

1. 每个因子在某个 task 中进入 SOTA 时的最佳回测数据；
1. 能追溯“这个 SOTA 记录来自哪个 task”。

因此建议新增表（示意）：`aistock_sota_factor_task_best`。

建议字段（全部用独立字段，便于排序）：

1. `task_id`（TEXT，FK -> task 表）
1. `factor_id`（TEXT 或复合键映射到因子库主键）
1. `best_annualized_return`（DOUBLE）
1. `best_max_drawdown`（DOUBLE）
1. `best_sharpe`（DOUBLE）
1. `best_ic`（DOUBLE）
1. `best_icir`（DOUBLE）
1. `best_rank_ic`（DOUBLE）
1. `best_rank_icir`（DOUBLE）
1. `entered_sota_at_utc`（TIMESTAMPTZ，可从 log 解析或落库时间）
1. `source_session_id`（INTEGER，可选：来自 log 的 session 编号）
1. `source_workspace_id`（TEXT：来自哪个 workspace 的 SOTA 因子实验）

说明：

1. “最佳指标”来自 `exp.result`（决策层），不依赖读取 `ret.pkl/ic.pkl` 等回测文件。
1. 同一 task 内若同一因子多次进入 SOTA，仅保留该 task 内“最佳”一条记录。

### 14.4 模型库：记录模型详细信息 + 进入 SOTA 时的最佳性能指标（与 task 关联）

模型库需满足两个目标：

1. **可重建训练配置**（未来重新训练/替换因子）
1. **可排序/可筛选**（基于指标字段进行 UI 排序）

建议拆分两层：

1. `aistock_model_catalog`：模型字典（去重后的模型“定义”）
1. `aistock_sota_model_task_best`：模型在某个 task 进入 SOTA 时的最佳指标（与 task 关联）

模型去重键建议：`(model_class, model_module_path, model_hyperparams_fingerprint)`。

模型必须保存：

1. `model_source_code_py`：模型源码（优先 `file_dict['model.py']`）
1. `model_config_task_blob`：`mlruns/**/artifacts/task`（pickle）或其 JSON 化版本
1. `model_config_blob`：`mlruns/**/artifacts/config`（若可读取）
1. `model_params_path`：权重文件路径（本方案阶段可以“可定位但不必立即用于推理”）

模型在 task 中的最佳指标字段（单列，便于排序）：

1. `best_annualized_return`
1. `best_max_drawdown`
1. `best_sharpe`
1. `best_ic`
1. `best_icir`
1. `best_rank_ic`
1. `best_rank_icir`

### 14.5 新增 task 表：以 task 为单位记录“全部成果信息”并支持 UI 展示

本章方案的核心是 task 表（示意：`aistock_task_catalog`），用于承载“一个任务的全部成果”。

建议字段：

1. `task_id`（TEXT，主键）：可用 log 目录名（例如 `2026-01-13_06-56-49-446055`）或 RD-Agent 的 task_run_id。
1. `log_dir`（TEXT）：log 绝对路径。
1. `created_at_utc`（TIMESTAMPTZ）：任务首次发现时间。
1. `sessions_count`（INTEGER）：`__session__` 数量。
1. `loops_count`（INTEGER）：本 task 运行了多少轮 loop（从 session/hist 或外部元数据统计）。
1. `sota_factors_count`（INTEGER）：本 task 产生了多少个 SOTA 因子（去重后数量）。
1. `sota_models_count`（INTEGER）：本 task 产生了多少个 SOTA 模型（去重后数量）。
1. `last_sota_factor_workspace_id`（TEXT）：最后一个进入 SOTA 的因子实验 workspace。
1. `last_sota_factor_model_workspace_id`（TEXT）：用于“包含全量 SOTA 因子训练”的模型 workspace（见 14.6）。
1. `last_sota_factor_model_run_id`（TEXT，可选）：对应 mlruns run_id。

UI 交互要求：

1. task 列表页：可按 `created_at_utc/loops_count/sota_factors_count/sota_models_count` 排序。
1. task 详情页：展示该 task 关联的 SOTA 因子列表与 SOTA 模型列表（分别来自 14.3/14.4 的 task-best 表）。

### 14.6 “最后一个进入 SOTA 的因子训练时的模型数据”如何确定（本轮已验证的规则）

由于 RD-Agent 的 SOTA 因子与 SOTA 模型列表独立，`QlibModelExperiment` 不保证使用了全量 SOTA 因子训练。

因此建议在 task 汇总时采用以下规则（本轮已在 log/ workspace 实测可行）：

1. 遍历 task 的所有 session，提取 `decision=True` 的 FactorExperiment。
1. 将这些因子实验对应 workspace 的 `combined_factors_df.parquet` 列集合做 union，得到“该 task 的 SOTA 动态因子全集”。
1. 找到“最后一个新增 SOTA 因子事件”对应的 workspace（按 session_id + hist_idx 排序的最后一条）。
1. 校验该 workspace 的 `combined_factors_df.parquet` 是否覆盖全集：
   - 若覆盖：将该 workspace 的 `mlruns/**/artifacts/task + params.pkl` 作为“全量 SOTA 因子训练的模型数据”。
   - 若不覆盖：继续向前回溯（按时间逆序）寻找第一个覆盖全集的 workspace，直到找到或判定缺失。

### 14.7 策略表继续保留（与 task 关联，记录策略源代码与配置）

本章方案中策略表继续保留，用于记录每个 task 使用的策略：

1. 策略模板 ID / 策略名称
1. 策略源代码（TEXT）
1. 策略配置（JSONB/TEXT）
1. 与 task 的关联（`task_id`）

说明：策略往往来自模板库且变化频率低，但 task 级展示必须能还原“当次任务使用的策略版本与配置”。

### 14.8 原有 loop 表的处置

1. **loop 表结构暂时保留**（便于未来决策是否继续使用 loop 维度做分析/回放）。
1. **新方案阶段不强制 loop 与 task 关联**：task 级选股与资产闭环不依赖 loop 表。
1. 原方案中依赖 `registry.sqlite` 的回填/固化流程，在新方案中不再作为必需前置。

### 14.9 对原方案的改动评估（影响面与兼容策略）

#### 14.9.1 主要变化

1. 数据主线从 `loop` 切换为 `task`：UI/接口/同步脚本以 task 为入口。
1. 因子/模型的“全局去重字典”与“task-best 明细”分离，避免重复与便于排序。
1. 同步不再依赖运行期 sqlite：以 log + workspace 为权威来源。

#### 14.9.2 与现有 Catalog/资产包方案的兼容

1. 资产包（production_bundles）与 manifest 方案仍可保留，用于“推理可运行”的闭环。
1. 新方案主要面向“训练资产与成果沉淀/复现/再训练”，并不强制立即替换推理资产包逻辑。

### 14.10 详细设计与实现步骤（脚本初始化 + 增量同步 + API/UI）

#### 14.10.1 初始化同步（脚本执行，首次全量）

目标：将历史任务的成果一次性入库。

步骤：

1. 输入：logs 根目录（例如 `F:/Dev/RD-Agent-main/log/`）。
1. 对每个 task 目录：
   - 枚举 `__session__/*/1_coding` 并反序列化。
   - 提取 `decision=True` 的 FactorExperiment / ModelExperiment 列表。
   - 写入 task 表：`sessions_count/loops_count/sota_factors_count/sota_models_count` 等。
   - 对每个 SOTA 因子：
     - 获取源码（优先 file_dict），写入因子库（去重 upsert）。
     - 写入 `sota_factor_task_best`（取该 task 内最佳指标）。
   - 对每个 SOTA 模型：
     - 获取源码（优先 file_dict），写入模型库（去重 upsert）。
     - 从 workspace 的 `mlruns/**/artifacts/task` 读取模型训练配置（只采集训练资产白名单）。
     - 写入 `sota_model_task_best`（取该 task 内最佳指标）。
   - 计算“全量 SOTA 因子训练的模型数据定位”（14.6），写入 task 表。

#### 14.10.2 增量同步（每次新 task 运行后的补充）

目标：新增任务出现后，AIstock 侧可通过 API/UI 触发增量入库。

增量原则：

1. 以 `task_id` 为幂等键：已存在则跳过或更新统计字段。
1. 因子/模型字典表 upsert（去重键不变）。
1. task-best 表按 `(task_id, factor_id)` / `(task_id, model_id)` upsert。

建议提供接口：

1. `POST /api/rdagent/tasks/sync`：传入 `log_dir` 或 `task_id`，触发解析与入库。
1. `GET /api/rdagent/tasks`：分页列表（支持排序字段）。
1. `GET /api/rdagent/tasks/{task_id}`：任务详情（包含 SOTA 因子列表、SOTA 模型列表、策略信息）。
1. `GET /api/rdagent/factors`：因子检索/排序。
1. `GET /api/rdagent/models`：模型检索/排序。
1. `GET /api/rdagent/assets/{id}`：按需下载源码/配置等文件资产（避免一次性传大字段）。

#### 14.10.3 选股运行方式（task 级）

本方案阶段的运行目标：

1. 以 task 为入口，AIstock 展示“任务成果”（SOTA 因子/模型/策略/指标）。
1. **暂不强依赖该 task 的权重用于推理**：模型权重可以只做“可定位与可下载”，未来允许用户选择因子集合并重新训练。

#### 14.10.4 关键验收标准

1. 同一个 task 多次同步不产生重复数据（幂等）。
1. 因子库包含：

   - 全量 alpha 因子；
   - 所有进入过 SOTA 的因子；
   - 每个因子保存源码与指纹；
   - 完全相同因子去重。

1. SOTA 因子 task-best 表：每个因子在 task 中的最佳指标字段可排序。
1. 模型库与模型 task-best 表：模型的类型/源码/训练配置可追溯，指标字段可排序。
1. task 详情页可展示：该 task 的 SOTA 因子列表与 SOTA 模型列表。
1. 训练资产采集严格排除 `pred/signals/portfolio_analysis` 等回测预测产物。

---

## 15. Task 选股详细设计方案（实现版，满足“Task 资产同步 + Task 目录 + manifest + 加入选股 + 新选股页面”）

本章是在第 14 章“新方案（提案）”基础上，结合你新增的实现要求，给出 **AIstock 与 RD-Agent 两端必须落地的详细设计**。

本章的强约束：

1. **因子/模型/策略三张 Catalog 表继续使用现有表结构**，仅允许“补充字段”，不做拆表/重命名。
1. **新增 Task 相关数据结构与 UI 页面**，且 **暂不改动原有同步页面/原有多策略选股页面**。
1. **每个 Task 在 AIstock 侧必须有一个独立 Task 目录**，命名方式参考 RD-Agent 侧 log 目录命名方式。
1. **Task 必须具备 manifest（任务级资产清单）**，并明确“存 DB 还是存文件资产”的决策与实现。
1. **Task 支持加入选股**：加入后在 Task 表中有标识字段，列表可直接显示状态，并可跳转到新建的 RD-Agent 选股页面。
1. **新建 RD-Agent 选股页面**：复用底层接口与数据服务层，不影响旧页面。
1. **RD-Agent 侧必须补齐增量同步 API**：按本章约定输出 Task 增量、manifest 与资产定位信息，AIstock 侧可幂等落库/落盘。

### 15.1 概念与边界定义

#### 15.1.1 Task 的定义

Task（任务）对应 RD-Agent `log/<task_name>/` 的一次“端到端实验会话”，其下包含 `__session__/*/1_coding` 快照。

AIstock 侧 Task 的主键建议直接采用 **log 目录名**（例如 `2026-01-13_06-56-49-446055`），记为 `task_id`。

说明：

1. 该命名与 RD-Agent log 目录一致，天然稳定，便于从 UI 反查原始 logs。
1. 若未来 RD-Agent 同时提供 `task_run_id`，可作为 `task_run_id` 字段额外落库，但不替代 `task_id`。

#### 15.1.2 Task 资产的定义（严格排除回测预测产物）

Task 资产仅包含“**可用于复现/推理/重训**”的必要输入，严禁把回测预测产物当成可入库资产。

硬排除（不得入 Task manifest 的 training_assets 或 inference_assets）：

1. `pred.pkl`
1. `signals.*`
1. `portfolio_analysis/**`
1. `sig_analysis/**`
1. `ret.pkl`
1. 任何“回测输出表/图/中间结果”目录（以白名单采集为准，避免漏排）

### 15.2 结构化存储：Task 目录规范（AIstock 侧）

#### 15.2.1 目录根路径

建议统一落在：`F:/Dev/AIstock/rdagent_assets/rdagent_tasks/`（不放到 backend 服务目录；若你希望使用相对路径，则以项目根目录下 `rdagent_assets/rdagent_tasks/` 为准）。

#### 15.2.2 每个 Task 的目录结构（必须）

每个 task 对应一个目录：

```text
rdagent_assets/rdagent_tasks/
  {task_id}/
    manifest.json
    sync_state.json
    source/
      session_index.json
      sessions/
        ...（可选：用于诊断的 session 摘要或必要字段）
    assets/
      primary/
        factor_entry.py        # 必须：由 AIstock 在“Task 同步”时生成，作为选股推理稳定入口
        model.py               # 可选：如果需要将 log 的 model.py 落为文件
        weights/
          model.pkl | params.pkl  # 仅训练/推理权重（不含 pred/signal）
        configs/
          conf_*.yaml
          task.pkl|task.json
      extras/
        ...（只允许“训练相关候选资产”，用于追溯；禁止放回测预测产物）
```

说明：

1. `manifest.json` 是 **Task 目录的权威清单**。
1. `sync_state.json` 记录同步状态、版本、最后一次同步时间、错误信息与诊断摘要。
1. `assets/primary` 只放“Task 级选股运行需要的最小闭环资产”（入口/权重/配置）。
1. `assets/extras` 允许放“训练相关候选资产”（例如原始 `params.pkl`、mlruns 片段），但仍必须遵守白名单采集与排除规则。

### 15.3 Task manifest：存 DB 还是存文件？（决策与落地方案）

本节给出结论与实现细节。

#### 15.3.1 结论：采用“文件为权威 + DB 存索引摘要 + 可选存 JSONB 镜像”的混合方案

原因（必须满足业务逻辑，不做简化）：

1. **manifest 体积与结构可能演进**：全量 JSON 直接放 DB text 会带来迁移/查询成本；同时 UI 需要快速列表字段，不需要每次读全量。
1. **文件系统是资产天然载体**：manifest 与资产文件同目录，便于拷贝/打包/审计/导出。
1. **DB 必须能检索与排序**：Task 列表页需要按状态/时间/计数排序，必须在 DB 有索引字段。
1. **强一致性要求**：manifest 文件必须有 hash 校验；DB 存 hash 与路径可用于诊断“DB 与文件不一致”。

因此方案为：

1. **manifest.json 全量存储在 Task 目录**（文件为权威，必须存在）。
1. **DB 存 manifest 摘要字段**（可索引/可排序/可过滤）。
1. （可选增强）DB 额外存一份 `manifest_json`（JSONB）镜像，用于 API 直接返回详情时减少读盘；但仍以文件为准，发现不一致必须报错并进入诊断流程。

#### 15.3.2 manifest.json 的内容要求（Task 级别）

必须字段（用于 UI/同步/推理闭环）：

1. `schema_version`
1. `task_id`
1. `log_dir`（或 `log_uri`）
1. `sessions[]`（至少包含被采信的 session id 列表/解析版本）
1. `sota_summary`：
   - `sota_factors_count`
   - `sota_models_count`
   - `last_sota_factor_workspace_id`
   - `candidate_model_workspace_id`（用于覆盖全集 SOTA 因子的训练 run 定位）
1. `primary_assets`：
   - `factor_entry_relpath`（必须：由 AIstock 同步时生成并落盘，指向 task 目录内的稳定入口文件）
   - `model_weight_relpath`
   - `config_relpaths[]`（可为空数组）
   - `model_task_relpath`（训练配置 task）
1. `asset_inventory`：
   - `training_assets[]`（白名单类型）
   - `inference_assets[]`（白名单类型）
   - `excluded_assets[]`（用于诊断：记录被排除的关键项及原因）

说明：

1. `asset_inventory` 的设计是为了满足“诊断透明”：同步后你能回答“为什么没同步某个文件”。
1. `excluded_assets[]` 必须包含 `reason`（例如 `backtest_artifact_excluded` / `not_in_whitelist` / `missing_in_workspace`）。

### 15.4 数据库设计：沿用现有表 + 新增 Task 表 + 字段补充

#### 15.4.1 现有三张表的处理原则

1. `aistock_factor_catalog`：继续使用，仅补充必要字段（例如已实现的 `first_sota_task_id`）。
1. `aistock_strategy_catalog`：继续使用，仅补充与 task 的关联字段（见下）。
1. `aistock_model_catalog`：如果项目中已存在则沿用（本章不强制新建“模型字典表”）；若尚未落地模型库，可先把 task 级模型信息落在 Task 表与 manifest 中，但不得丢失业务字段。

#### 15.4.2 新增表：aistock_task_catalog（必须新增）

表目标：

1. 提供 Task 列表/详情的快速查询。
1. 管理 Task 同步状态与“加入选股”状态。
1. 提供 Task 目录与 manifest 的索引。

建议字段（示意，字段名可按现有命名风格微调但语义必须一致）：

1. `task_id`（TEXT，PK）：log 目录名。
1. `log_dir`（TEXT）：RD-Agent log 目录。
1. `task_run_id`（TEXT, nullable）：RD-Agent 任务 ID（若可获得）。
1. `created_at_utc`（TIMESTAMPTZ）：首次同步时间。
1. `updated_at_utc`（TIMESTAMPTZ）：最近同步/更新。
1. `task_dir`（TEXT）：AIstock 侧 Task 目录绝对/相对路径。
1. `manifest_path`（TEXT）：一般为 `{task_dir}/manifest.json`。
1. `manifest_sha1`（TEXT）：用于一致性校验。
1. `manifest_schema_version`（INT）：用于升级策略。
1. `sync_status`（TEXT）：`pending|syncing|success|failed|partial`。
1. `sync_error`（TEXT, nullable）：最近一次失败摘要。
1. `sync_diagnostics`（TEXT/JSONB, nullable）：诊断摘要（文件缺失、白名单排除统计等）。
1. `sessions_count`（INT）
1. `loops_count`（INT, nullable）
1. `sota_factors_count`（INT）
1. `sota_models_count`（INT）
1. `last_sota_factor_workspace_id`（TEXT, nullable）
1. `candidate_model_workspace_id`（TEXT, nullable）
1. `candidate_model_run_id`（TEXT, nullable）
1. **`is_enabled_for_selection`（BOOL，默认 false）**：是否加入选股。
1. `enabled_for_selection_at_utc`（TIMESTAMPTZ, nullable）
1. `enabled_for_selection_by`（TEXT, nullable）：操作人/来源（UI/脚本）。

#### 15.4.3 对现有策略表的字段补充（用于 Task 详情展示）

在 `aistock_strategy_catalog` 增加：

1. `task_id`（TEXT, nullable）：该策略记录来自哪个 Task（如果策略在多个 Task 复用，可不强制唯一）。
1. `task_dir`（TEXT, nullable）：便于定位 Task 目录中的策略快照/配置。

说明：

1. 策略往往稳定，但 Task 详情页要求能还原“当次任务使用的策略版本与配置”，因此需要 task 维度关联。

### 15.5 同步链路：RD-Agent 增量同步 API 与 AIstock Task 资产同步

本节覆盖“RD-Agent 侧补齐增量同步工作”与“AIstock 新增 Task 资产同步页面”的闭环。

同步分为两类（本方案的硬约束）：

1. **初始化同步（全量）**：仅通过脚本执行，一次性同步历史 logs 下的全部 task（可重复执行且幂等）。
1. **增量同步（按需）**：仅通过 UI 执行。UI 先通过 API 获取 logs 下最新 task 列表与每个 task 的概要信息，再由用户选择是否同步该 task。

#### 15.5.1 RD-Agent 侧：新增/补齐 API（增量同步）

必须提供能力（接口形式可按现有 Results API/管理 API 风格实现）：

1. `GET /api/v1/rdagent/tasks`：返回任务列表（至少包含 `task_id/log_dir/updated_at`）。
1. `GET /api/v1/rdagent/tasks/latest?limit=...`：返回 logs 下“最新若干个 task”的列表（用于 UI 增量同步候选列表）。
1. `GET /api/v1/rdagent/tasks/{task_id}/summary`：返回单个 task 的概要信息（用于 UI 先看概要再决定是否同步），至少包含：
   - `task_id/log_dir/updated_at`
   - `sessions_count`（可估算）
   - `sota_factors_count/sota_models_count`
   - `candidate_model_workspace_id`（若可得）
   - `has_required_assets`（布尔，指“按白名单是否能拿到关键资产”；不能则为 false）
   - `missing_or_excluded_reasons[]`（原因列表，必须可诊断）
1. `GET /api/v1/rdagent/tasks/{task_id}`：返回单个 task 的 **task-manifest payload**（包含：SOTA 因子/模型概览、workspace 定位、训练资产白名单列表、排除清单与原因）。
1. `GET /api/v1/rdagent/tasks/{task_id}/assets`：按需下载资产（至少支持：`factor.py/model.py/task/params.pkl/conf_*.yaml`），并支持“只下载训练资产白名单”。
1. `GET /api/v1/rdagent/tasks/changes?since=...`：增量变更集（新 task、更新 task、删除/失效 task 的状态）。

注意：

1. RD-Agent 输出 payload 必须“可诊断”：对于缺失/排除项必须给出 reason。
1. RD-Agent 必须保证“不会把回测预测产物作为可同步资产输出”。

#### 15.5.2 AIstock 侧：新增 Task 资产同步 API（不影响旧同步）

AIstock 新增独立后端接口（示意）：

1. `POST /api/v1/rdagent/tasks/sync`：触发同步。
   - 入参：`task_id` 或 `log_dir`（二选一）。
   - 行为：
     - 创建/更新 `aistock_task_catalog`
     - 创建 Task 目录
     - 拉取 RD-Agent task-manifest payload
     - 写入 `{task_dir}/manifest.json` 与 `manifest_sha1`
     - 生成 `{task_dir}/assets/primary/factor_entry.py`（AIstock 生成稳定入口，不依赖 RD-Agent bundle 是否已生成入口文件）
     - 按白名单下载必要资产到 `{task_dir}/assets/**`
     - 更新 `sync_status/sync_diagnostics`
1. `GET /api/v1/rdagent/tasks`：分页列表（含 `is_enabled_for_selection` 字段）。
1. `GET /api/v1/rdagent/tasks/sync-candidates?limit=...`：用于 UI 增量同步候选列表。
   - 行为：调用 RD-Agent 的 `tasks/latest` 与 `tasks/{id}/summary` 聚合得到列表。
   - 返回每个 task 的概要信息 + AIstock 本地是否已同步/同步状态（便于 UI 决策）。
1. `GET /api/v1/rdagent/tasks/{task_id}`：详情（聚合：Task 表字段 + manifest + 关联因子/模型/策略摘要）。
1. `POST /api/v1/rdagent/tasks/{task_id}/enable_for_selection`：加入选股（置 `is_enabled_for_selection=true`）。
1. `POST /api/v1/rdagent/tasks/{task_id}/disable_for_selection`：移出选股（可选，但建议提供）。

幂等与一致性要求：

1. 同一个 `task_id` 多次 `sync`：必须幂等；若 manifest_sha1 未变化，可跳过资产下载（但需校验文件存在性）。
1. 若 DB 的 `manifest_sha1` 与文件 sha1 不一致：必须标记 `sync_status=partial` 并要求重新同步（不得静默忽略）。

### 15.6 前端 UI 设计：Task 资产同步页 + Task 列表/详情 + 加入选股 + 跳转

本节只描述新增页面，不改现有同步页与旧选股页。

#### 15.6.1 新增页面 1：Task 资产同步页

页面目标：

1. 触发“按 task 增量同步”。
1. 显示同步状态、失败原因、可诊断摘要。

交互：

1. 打开页面时，先调用 `GET /api/v1/rdagent/tasks/sync-candidates?limit=...` 拉取“最新 task + 概要信息”列表。
1. 列表必须展示概要字段：`task_id/updated_at/sota_factors_count/sota_models_count/has_required_assets/missing_or_excluded_reasons`，并展示 AIstock 本地 `sync_status`。
1. 用户可勾选/选择某个 task 后，点击“同步/更新”触发 `POST /api/v1/rdagent/tasks/sync`。
1. 显示：`sync_status/updated_at/manifest_sha1/sessions_count/sota_*_count`。
1. 对 `failed|partial` 展示：缺失资产/排除资产统计与 reason。

#### 15.6.2 新增页面 2：Task 列表页（已同步 Task）

展示字段（必须包含）：

1. `task_id`
1. `updated_at_utc`
1. `sync_status`
1. `sota_factors_count` / `sota_models_count`
1. **`is_enabled_for_selection`**（列表上直接显示）

操作：

1. 点击行进入 Task 详情页。
1. 对未加入选股的 task 提供“加入选股”按钮。
1. 对已加入选股的 task 显示“已加入选股”，并提供“去选股”跳转按钮。

#### 15.6.3 新增页面 3：Task 详情页

分区展示：

1. **Task 基本信息**：log_dir、task_dir、sync_status、diagnostics。
1. **SOTA 因子列表**：名称/表达式/指标/来源（来自现有因子表 + task-best 数据；若尚未落库 task-best，则至少从 manifest 展示）。
1. **SOTA 模型信息**：模型类型/关键指标/训练配置摘要（task/artifacts）。
1. **资产清单**：manifest 的 training_assets / inference_assets / excluded_assets。

操作：

1. “加入选股”（置标识并跳转到新 RD-Agent 选股页）。
1. “重新同步”（回到 Task 资产同步页或直接触发）。

### 15.7 新增 RD-Agent 选股页面（不影响旧多策略选股）

#### 15.7.1 路由与入口

新增页面：`/rdagent/task-selection`（命名可调整，但必须与旧页面分离）。

入口：

1. Task 列表/详情页点击“去选股”。
1. 仅允许从 `is_enabled_for_selection=true` 的 task 进入（否则 UI 必须提示先加入选股）。

#### 15.7.2 复用底层服务层的原则

1. 推理/选股执行尽量复用现有 `rdagent_selection_service` / `inference_engine` / 行情补齐等底层能力。
1. 新页面只改变“选择入口实体”：从“多策略/loop”改为“task”。

#### 15.7.3 Task->推理资产定位（关键业务逻辑，必须可解释）

执行选股前，必须明确从 Task 目录取得：

1. `factor_entry.py`（或等价入口）
1. 模型权重（`model.pkl/params.pkl`）
1. 推理配置（FilterCol、handler、特征列表等）

说明（入口生成策略）：

1. `factor_entry.py` 由 AIstock 在同步时生成，并以 manifest 的 `primary_assets.factor_entry_relpath` 作为唯一权威指向。
1. AIstock 推理不得扫描 task 目录猜测入口文件；必须严格按 manifest 指向加载。

若 Task 目录缺失上述任何一项：

1. 必须硬失败并提示“缺失哪一项、manifest 指向哪里、同步时排除原因是什么”。

#### 15.7.4 执行结果

选股结果落库与展示字段口径沿用现有规范（第 2.4 节），并在结果中额外附带：

1. `task_id`
1. `task_manifest_sha1`
1. `asset_paths`（本次推理实际使用的入口与权重相对路径，便于排障）

### 15.8 实施步骤（不做功能简化，可直接按步骤开发/验收）

#### 15.8.1 数据库迁移

1. 新增 `aistock_task_catalog` 表。
1. 为 `aistock_strategy_catalog` 增加 `task_id/task_dir` 字段（若已有等价字段则跳过）。
1. 若决定启用 DB 镜像：为 `aistock_task_catalog` 增加 `manifest_json`（JSONB）字段（可选）。

#### 15.8.2 后端：Task 同步与资产落盘

1. 实现 Task 目录创建与命名规范。
1. 实现 manifest 写入与 sha1 计算、DB 摘要字段更新。
1. 实现“白名单资产下载”与“排除清单 reason 记录”。
1. 实现 `sync_state.json` 与 `sync_diagnostics`（DB + 文件）双落地。

补充：初始化同步脚本（必须落地）：

1. 新增脚本 `scripts/init_rdagent_task_assets.py`（或同等命名），输入 logs 根目录，一次性全量同步所有历史 task。
1. 脚本必须幂等：已存在 task 仅更新统计/manifest/缺失诊断（按 sha1 判断是否需要重新下载）。
1. 脚本执行完成后，UI 只负责“增量候选 + 人工选择同步”，不承担全量历史同步。

#### 15.8.3 后端：Task 加入选股与推理入口

1. 实现 `enable_for_selection/disable_for_selection` API。
1. 新增 `TaskSelectionService`（或在现有 selection service 上扩展）支持 `task_id` 入口，并复用现有推理引擎。
1. 将推理资产定位严格绑定到 Task 目录 + manifest（不得扫描猜测）。

#### 15.8.4 前端：新增 3 个页面

1. Task 资产同步页（独立入口，不动旧同步页）。
1. Task 列表页（展示已同步任务与加入选股状态）。
1. Task 详情页（展示任务全部信息、资产清单、SOTA 因子/模型/策略摘要）。
1. 新增 RD-Agent 选股页面（不动旧页面）。

#### 15.8.5 RD-Agent：补齐增量同步 API

1. 实现 tasks 列表、task 详情(manifest payload)、task assets 下载、task changes 增量接口。
1. 输出必须包含排除项原因（excluded_assets）。

### 15.9 验收标准（必须逐项满足）

1. **不影响原有功能**：旧同步页、旧多策略选股页不做任何行为变化。
1. **Task 目录必存在**：每个已同步 Task 在 AIstock 侧都有独立目录，且目录名与 `task_id` 一致。
1. **manifest 权威**：
   - 文件存在且可解析
   - `manifest_sha1` 与 DB 一致
   - 推理严格按 manifest 指向加载，不做扫描猜测
1. **可诊断性**：同步失败/部分成功时，UI 能展示缺失资产/排除资产及 reason。
1. **加入选股闭环**：
   - 点击加入选股后，Task 表 `is_enabled_for_selection` 更新
   - 列表立即显示已加入
   - 可跳转新 RD-Agent 选股页并执行选股
1. **选股结果可追溯**：结果记录必须包含 `task_id` 与 `task_manifest_sha1`（以及实际使用资产路径摘要）。
