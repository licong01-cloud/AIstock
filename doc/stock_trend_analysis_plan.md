# 股票趋势分析模块设计与实施方案

> 版本：v1.0（初始方案）  
> 目的：为后续分阶段落地提供长期可复用的设计文档与执行步骤清单。

---

## 1. 背景与目标

### 1.1 背景

现有系统已经实现了“多智能体股票综合分析”功能，包括：

- 技术分析师
- 基本面分析师
- 资金面分析师
- 风险管理师
- （可选）情绪、新闻、研报、公告、筹码分析师

现有分析功能以“当前价值与投资决策建议”为核心输出，而**缺少一个专门面向“未来股价趋势与概率分布”的模块**，无法清晰回答：

- 未来 1 天 / 1 周 / 1 个月 / 长线，涨跌方向和大致幅度如何？
- 不同分析师的观点是如何**一步步修正预测**的？
- 这些预测结论能否以结构化 JSON 形式存储，用于后续 AI 再分析 / 回测？


### 1.2 目标

新建一个 **“股票趋势分析”** 模块，要求：

- **与现有股票分析完全隔离，不干扰原有逻辑**。
- **仅复用统一数据访问和 DeepSeekClient 基础设施**。
- 支持：
  - 实时趋势分析（基于当日数据）。
  - 指定历史时点趋势分析（基于历史数据回放）。
- 分析师链路：
  - 第 1 步：技术资金分析师（技术 + 资金 + 筹码，给出初始多周期预测及概率）。
  - 第 2~6 步：
    - 基本面分析师
    - 研报分析师
    - 公告分析师
    - 情绪分析师
    - 新闻分析师
    
    依次对预测进行修正。
  - 风险分析师：
    - 不直接修改预测数值，
    - 输出风险分析与风险提示，补充在趋势分析结果中。
- 输出：
  - 多个时间跨度（1 天 / 1 周 / 1 月 / 长线）的**涨跌情景 + 概率分布 + 期望涨跌幅**。
  - 每位分析师的详细自然语言报告 + 结构化 JSON 结论。
  - 完整的“预测演化时间轴”（从初始预测到每一轮修正）。
- 存储：
  - 建独立的趋势分析数据表，
  - 结构化结论与分析过程均以 JSON 形式落库，供后续 AI 再利用。
- 前端：
  - 类似现有多分析师报告的分页展示。
  - 新增 **Apache ECharts 图表**，直观展示：
    - 各时间跨度的最终预测结果。
    - 初始预测及每轮修正的概率分布和期望收益变化。

---

## 2. 架构与边界

### 2.1 与现有模块的关系

- 保持不变：
  - `analyze_stock` 及其调用链（综合分析）。
  - 现有 `StockAnalysisAgents` 和相关提示词。
  - 现有 `analysis_records` 表及前端 UI 展示逻辑。

- 新增：
  - 独立的服务函数：`analyze_stock_trend`。
  - 独立的 orchestrator：`StockTrendAnalysisAgents`（新文件 `trend_analysis.py`）。
  - 独立的 Pydantic 模型 / schema。
  - 独立的数据库表与 Repo：`trend_analysis_records` / `trend_analyst_results` / `trend_analysis_repo`。
  - 前端 `/analysis` 页面下的新 section：**“股票趋势分析”** 及其图表与报告组件。

### 2.2 复用的基础设施

- 统一数据访问：`NextUnifiedDataAccess`
  - 复用以下获取方法：
    - `get_stock_info`
    - `get_stock_data` + 技术指标计算
    - `get_financial_data`
    - `get_quarterly_reports`
    - `get_fund_flow_data`
    - `get_risk_data`
    - （按需启用）`get_market_sentiment_data`, `get_news_data`, `get_research_reports_data`, `get_announcement_data`, `get_chip_distribution_data`。

- 模型调用：`DeepSeekClient`
  - 为新分析师提供 `trend_*` 系列方法（可在实现时增加）：
    - `trend_tech_capital_analysis`
    - `trend_fundamental_adjustment`
    - `trend_research_adjustment`
    - `trend_announcement_adjustment`
    - `trend_sentiment_adjustment`
    - `trend_news_adjustment`
    - `trend_risk_analysis`

> 约束：新模块**不直接依赖旧根目录下的 `ai_agents.py`**，所有新逻辑均在 `next_app/backend` 下自洽实现。

---

## 3. 数据模型设计

### 3.1 趋势预测基础结构

#### 3.1.1 情景

```python
class TrendPredictionScenario(BaseModel):
    direction: Literal["up", "down", "flat"]
    magnitude_min_pct: float      # 例如 -2.0
    magnitude_max_pct: float      # 例如 5.0
    probability: float            # 0~1
    label: str                    # 统一场景标签，如 "上涨0~5%"，供图表使用
    narrative: str                # 对该情景的一句话解释
```

#### 3.1.2 时间跨度（horizon）

```python
class TrendPredictionHorizon(BaseModel):
    horizon: Literal["1d", "1w", "1m", "long"]
    scenarios: list[TrendPredictionScenario]
    base_expectation_pct: float | None = None  # 期望涨跌幅（E[return]）
```

> 说明：
> - `label` 由后端统一生成（方向 + 区间），确保 ECharts series 对齐。
> - `base_expectation_pct` 用于折线图展示“期望收益随分析师修正的变化”。

---

### 3.2 分析师结果结构

```python
class TrendAnalystResult(BaseModel):
    name: str                # "技术资金分析师"
    role: str
    raw_text: str            # 完整自然语言报告
    conclusion_json: dict    # 内部约定结构：{"horizons": [...]} 等
    created_at: datetime
```

> `conclusion_json` 中至少包含一个 `horizons: list[TrendPredictionHorizon]` 字段，供前端和后续 AI 使用。

---

### 3.3 预测演化时间轴（给图表使用）

```python
class PredictionStep(BaseModel):
    step: int                        # 0,1,2,... 修正顺序
    analyst_key: str                 # "tech_capital" / "fundamental" / ...
    analyst_name: str                # 用于图表 X 轴显示，如 "技术资金分析师"
    description: str                 # 本轮修正摘要（给 tooltip / 标注用）
    horizons: list[TrendPredictionHorizon]  # 此时的完整预测快照
    created_at: datetime
```

> 每一轮分析/修正后，Orchestrator 都会把当前预测状态存成一个 `PredictionStep`。

---

### 3.4 请求 / 响应模型

#### 3.4.1 请求

```python
class StockTrendAnalysisRequest(BaseModel):
    ts_code: str
    start_date: str | None = None     # YYYY-MM-DD，可选区间
    end_date: str | None = None
    analysis_date: str | None = None  # 精确分析时点，优先级最高
    enabled_analysts: dict[str, bool] | None = None
    mode: Literal["realtime", "backtest"] = "realtime"
```

#### 3.4.2 响应

```python
class StockTrendAnalysisResponse(BaseModel):
    ts_code: str
    analysis_date: str
    mode: Literal["realtime", "backtest"]
    horizons: list[TrendPredictionHorizon]         # 最终预测（新闻分析师修正后）
    analysts: list[TrendAnalystResult]             # 各分析师报告
    risk_report: TrendAnalystResult | None         # 风险分析师结果
    prediction_evolution: list[PredictionStep]     # 预测演化时间轴
    record_id: int | None                          # 落库 ID
```

---

## 4. Orchestrator 流程设计

### 4.1 分析师顺序

1. **技术资金分析师（初始预测）**
   - 输入：
     - `stock_info`
     - `stock_data_with_indicators`
     - `indicators`
     - `fund_flow_data`
     - `chip_data`
   - 输出：
     - `pred0: list[TrendPredictionHorizon]`
     - `result0: TrendAnalystResult`
   - 记录：
     - `PredictionStep(step=0, analyst_key="tech_capital", analyst_name="技术资金分析师", horizons=pred0, ...)`

2. **基本面分析师（趋势修正）**
   - 输入：`pred0` + `financial_data` + `quarterly_data`。
   - 输出：`pred1`, `result1`。
   - 记录：`PredictionStep(step=1, analyst_key="fundamental", ...)`。

3. **研报分析师（趋势修正）**
   - 输入：`pred1` + `research_data`。
   - 输出：`pred2`, `result2`。

4. **公告分析师（趋势修正）**
   - 输入：`pred2` + `announcement_data`。
   - 输出：`pred3`, `result3`。

5. **情绪分析师（偏短期修正）**
   - 输入：`pred3` + `sentiment_data`。
   - 输出：`pred4`, `result4`。

6. **新闻分析师（偏短期修正）**
   - 输入：`pred4` + `news_data`。
   - 输出：`pred5`, `result5`（记为 `pred_final`）。

7. **风险分析师（风险补充，不改预测）**
   - 输入：`pred_final` + `risk_data` + `fund_flow_data` + `indicators` + `stock_info`。
   - 输出：`risk_result`。

最终：

- `horizons = pred_final`
- `prediction_evolution = [step0, step1, ..., stepN]`
- `analysts = [result0..result5]`
- `risk_report = risk_result`

> 各分析师的 Prompt 明确要求：
> 
> - 阅读上一轮 `predX`；
> - 对每个时间跨度 horizon 的各个情景 scenario：
>   - 给出保持/上调/下调/新增/删除的理由；
>   - 更新 `probability` 与 `base_expectation_pct`；
> - 输出完整的新 `predX+1`（不只是 diff）。

---

## 5. 数据获取与服务函数

### 5.1 数据获取逻辑（与 `analyze_stock` 对齐）

在 `analysis_service.py` 中新增 `analyze_stock_trend`，重用现有数据获取模式：

- 使用 `NextUnifiedDataAccess` 获取：
  - `stock_info`
  - `stock_data` + 指标计算
  - `financial_data`, `quarterly_data`
  - `fund_flow_data`
  - `risk_data`
  - 若对应趋势分析师启用，则获取：
    - `sentiment_data`
    - `news_data`
    - `research_data`
    - `announcement_data`
    - `chip_data`

- `analysis_date` 规则：
  - 若请求包含 `analysis_date` → 直接采用；
  - 否则使用 `end_date`（转 `YYYYMMDD`）；
  - 若都为空，则视为实时（当天）。

### 5.2 服务函数流程

伪代码示意：

```python
def analyze_stock_trend(req: StockTrendAnalysisRequest) -> StockTrendAnalysisResponse:
    # 1. 数据获取（复用 NextUnifiedDataAccess，与 analyze_stock 保持一致风格）
    uda = NextUnifiedDataAccess()
    stock_info, stock_data, indicators, ... = ...

    # 2. 调用新 Orchestrator
    agents = StockTrendAnalysisAgents(model="deepseek-chat")
    horizons, analysts, risk_report, prediction_evolution = agents.run_trend_analysis(
        stock_info=stock_info,
        stock_data_with_indicators=stock_data_with_indicators,
        indicators=indicators,
        financial_data=financial_data,
        fund_flow_data=fund_flow_data,
        quarterly_data=quarterly_data,
        risk_data=risk_data,
        sentiment_data=sentiment_data,
        news_data=news_data,
        research_data=research_data,
        announcement_data=announcement_data,
        chip_data=chip_data,
        enabled_analysts=req.enabled_analysts,
    )

    # 3. 落库
    record_id = trend_analysis_repo.save_trend_analysis(
        symbol=req.ts_code,
        analysis_date=analysis_date,
        mode=req.mode,
        stock_info=stock_info,
        horizons=horizons,
        prediction_evolution=prediction_evolution,
        analysts=analysts,
        risk_report=risk_report,
    )

    # 4. 返回响应
    return StockTrendAnalysisResponse(
        ts_code=req.ts_code,
        analysis_date=analysis_date_str,
        mode=req.mode,
        horizons=horizons,
        analysts=analysts,
        risk_report=risk_report,
        prediction_evolution=prediction_evolution,
        record_id=record_id,
    )
```

---

## 6. 数据库存储设计

### 6.1 表：trend_analysis_records

字段建议：

- `id` (PK)
- `symbol` (text)
- `analysis_date` (date / text)
- `mode` (text) — `"realtime"` / `"backtest"`
- `stock_info` (JSONB)
- `final_predictions` (JSONB) — 存 `horizons`
- `prediction_evolution` (JSONB) — 存 `prediction_evolution`
- `created_at` (timestamp)

### 6.2 表：trend_analyst_results

- `id` (PK)
- `record_id` (FK → trend_analysis_records.id)
- `analyst_key` (text) — `"tech_capital"` / `"fundamental"` / ...
- `analyst_name` (text)
- `role` (text)
- `raw_text` (text)
- `conclusion_json` (JSONB)
- `created_at` (timestamp)

### 6.3 Repo：trend_analysis_repo

接口示例：

- `save_trend_analysis(symbol, analysis_date, mode, stock_info, horizons, prediction_evolution, analysts, risk_report) -> int`
- `get_trend_analysis(record_id)` — 供未来趋势历史详情页使用。

---

## 7. 前端 UI 与 Apache ECharts 设计

### 7.1 模块位置与布局

在 `/analysis` 页面中，在现有“股票分析”结果卡片下新增：

- 标题：`📈 股票趋势分析`
- 内容分为三块：

1. **预测概览卡片区**
   - 四个卡片分别展示：1 天 / 1 周 / 1 月 / 长线。
   - 内容包括：
     - 最可能情景（例如：上涨 0~5%）；
     - 对应概率；
     - 期望收益 `base_expectation_pct`。

2. **图表区（ECharts）**
   - 水平 Tab：`1天 / 1周 / 1月 / 长线`。
   - 每个 Tab 下包含两张图：

   **图 A：概率分布演化（堆叠条形图）**

   - X 轴：分析师步骤（来自 `prediction_evolution.step` / `analyst_name`）：
     - 技术资金分析师
     - 基本面分析师
     - 研报分析师
     - 公告分析师
     - 情绪分析师
     - 新闻分析师
   - Y 轴：概率（0~100%）。
   - Series：不同情景 `label`（如“上涨>5%”、“上涨0~5%”、“平盘±1%”、“下跌>3%”）形成不同 series。
   - 每个 series 在每个 step 上的值为该情景的 `probability * 100`。
   - Tooltip：显示分析师名称、情景 label、概率、简要 narrative。

   **图 B：期望收益演化（折线图）**

   - X 轴：同上。
   - Y 轴：期望收益 `base_expectation_pct`（单位 %）。
   - 单条折线：显示从技术资金初判到新闻修正后的期望值变化。
   - Tooltip：显示当前分析师下该 horizon 的期望涨跌幅与一句话总结。

3. **分析师报告分页**

- 使用 Tab 切换：
  - 技术资金分析师
  - 基本面分析师
  - 研报分析师
  - 公告分析师
  - 情绪分析师
  - 新闻分析师
  - 风险分析师
- 每个 Tab 内展示：
  - 上方：从 `conclusion_json` 解析出来的各 horizon 结构化结论（表格）。
  - 下方：完整 `raw_text`，支持折叠/展开。

### 7.2 ECharts 与数据映射

组件接收：

- `prediction_evolution: PredictionStep[]`
- `selectedHorizon: "1d" | "1w" | "1m" | "long"`

数据准备逻辑（概念）：

1. 遍历 `prediction_evolution`：
   - 对每个 step：
     - 找到 `step.horizons` 中 `horizon == selectedHorizon`；
     - 采集 `scenarios`（各自的 `label`, `probability`）与 `base_expectation_pct`。

2. 构造堆叠条形图数据：
   - 收集所有 step 中出现过的 `label` 作为 series 集合。
   - 每个 series（label）对应一个数组，长度 = step 数，缺失时补 0。

3. 构造折线图数据：
   - 建立一个数组 recordings step 顺序的 `base_expectation_pct`。

> 实现时推荐使用 `echarts-for-react`，并在 Next 中通过 `dynamic` + `ssr:false` 使用。

---

## 8. 分阶段实施计划（含状态占位）

> 说明：后续每完成一个阶段，可在本节更新状态（例如 `[x]` / `[ ]` + 日期 + 说明）。

### 阶段 1：后端模型与服务骨架

- [x] 定义 Pydantic 模型：
  - `TrendPredictionScenario`, `TrendPredictionHorizon`
  - `TrendAnalystResult`, `PredictionStep`
  - `StockTrendAnalysisRequest`, `StockTrendAnalysisResponse`
- [x] 在 `analysis_service.py` 中添加 `analyze_stock_trend` 函数签名（先返回 mock 数据）。

> 阶段 1 已完成：新增趋势分析相关的请求/响应与内部数据结构模型，并在 `analysis_service.py` 中提供了返回空白结果的 `analyze_stock_trend` 骨架函数，未改动现有股票分析逻辑。

### 阶段 2：数据库与 Repo

- [x] 设计并创建表：
  - `trend_analysis_records`
  - `trend_analyst_results`
- [x] 新建 `trend_analysis_repo`：
  - `save_trend_analysis(...) -> record_id`
  - `get_trend_analysis(record_id)`。

> 阶段 2 已完成：在 `app` schema 下新增 `trend_analysis_records` 和 `trend_analyst_results` 两张表，使用 JSONB 存储预测结果、TEXT 存储分析报告，并实现 `TrendAnalysisRepoPG`（`trend_analysis_repo_impl.py`）提供基础的保存与读取能力。

### 阶段 3：趋势分析 Orchestrator 与分析师实现

- [ ] 新建 `trend_analysis.py`，实现 `StockTrendAnalysisAgents`：
  - [ ] 技术资金分析师（完整实现输出 pred0 + TrendAnalystResult）。
  - [ ] 基本面修正分析师。
  - [ ] 研报修正分析师。
  - [ ] 公告修正分析师。
  - [ ] 情绪修正分析师。
  - [ ] 新闻修正分析师。
  - [ ] 风险分析师。
- [ ] 在 orchestrator 中填充 `prediction_evolution`（每轮一个 `PredictionStep`）。

### 阶段 4：服务函数整合与落库

- [ ] 在 `analyze_stock_trend` 中：
  - [ ] 复用 `NextUnifiedDataAccess` 完成数据获取。
  - [ ] 调用 `StockTrendAnalysisAgents.run_trend_analysis`。
  - [ ] 调用 `trend_analysis_repo.save_trend_analysis` 落库。
  - [ ] 返回完整的 `StockTrendAnalysisResponse`。

### 阶段 5：前端基础集成（无图表或简单双图）

- [ ] 在 `/analysis` 页面中增加“📈 股票趋势分析” section。
- [ ] 调用新 API，展示：
  - [ ] 四个 horizon 的 summary 卡片。
  - [ ] 各分析师报告 Tab（使用 `analysts` 和 `risk_report`）。

### 阶段 6：接入 Apache ECharts 图表

- [ ] 安装依赖：`echarts`, `echarts-for-react`。
- [ ] 新建 `TrendPredictionCharts` 组件：
  - [ ] 支持 horizon Tab 切换。
  - [ ] 图 A：预测概率堆叠条形图（使用 `prediction_evolution`）。
  - [ ] 图 B：期望收益折线图。

### 阶段 7：优化与文档持续更新

- [ ] 优化场景 label 规则与颜色映射。
- [ ] 对 backtest 模式在 UI 中增加明显标识。
- [ ] 在每次完成阶段后，在本文件对应阶段打勾并补充：
  - 完成日期
  - 关键实现说明
  - 发现的问题与改进建议

---

## 9. 未来扩展方向（可选）

- 基于已存储的趋势预测历史数据，训练/微调一个“元分析师”，学习：
  - 哪些组合条件（技术 + 资金 + 筹码 + 基本面 + 研报 + 新闻）更容易带来高预测准确率。
- 针对趋势预测结果做：
  - 回测统计：预测-现实对比，统计各 horizon 的命中率、偏差分布。
  - 策略建议：例如结合风险水平给出“是否应采取分批建仓/减仓”等更细策略。

---

> 本文档是“股票趋势分析”模块唯一权威设计描述，后续如有设计变动，应在此同步更新。
