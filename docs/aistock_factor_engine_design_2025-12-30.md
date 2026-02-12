# AIstock 因子计算与评估（Alphalens 集成）设计方案（2025-12-30）

> 目标：在 AIstock 内部落地一套因子计算与评估框架，利用现有统一数据服务层，从行情/财务数据出发计算时序因子值，并结合 Alphalens 对因子进行批量分析与评分，为后续策略/模型选择提供统一的“因子质量”参考。当前仅为设计备忘录，计划在 Phase 3 以后实施。

---

## 1. 设计背景与总体思路

### 1.1 背景

- RD-Agent 负责研究与产出实验结果，目前已经实现：
  - RD-Agent Catalog（三张/四张目录表：factor/strategy/loop/model）
  - AIstock 策略导入 / signals 入库 / Data Service 统一行情层。
- 为了对大量因子进行系统性评估（IC、收益分布、稳定性等），希望在 **AIstock 侧** 完成：
  - 因子时序值的统一计算和存储；
  - 基于 Alphalens 的批量因子分析与打分；
  - 前端浏览和对策略/模型的辅助决策能力。

### 1.2 总体思路

1. **因子定义**：
   - 继续使用 `aistock_factor_catalog` 作为“有哪些因子需要被支持与评估”的唯一来源。
2. **数据获取**：
   - 通过现有 Data Service（xtquant/tdx/timescaledb 适配器）统一拉取行情和财务数据。
3. **因子计算**：
   - 在 AIstock 内实现一个“因子计算引擎”，根据因子定义 + Data Service 输入，计算 `(trade_date, symbol, factor_name, factor_value)`。
4. **因子值存储**：
   - 将结果写入 `trading.factor_value` 表，支持幂等更新与增量刷新。
5. **因子评估（Alphalens）**：
   - 使用离线任务（脚本或 Job），读取因子值 + 价格数据，调用 Alphalens 生成 IC/收益/稳定性指标，并写入 `trading.factor_score` 表。
6. **前端展示**：
   - 在 `/rdagent/factors` 或新的“因子评估”页面展示因子评分和核心指标；
   - 为策略/模型选择提供统一的“因子质量”视图。

---

## 2. 数据表设计（trading schema）

### 2.1 `trading.factor_value` —— 因子时序值表

**用途**：存储因子在时间 × 标的维度上的数值，用于后续回测、Alphalens 分析等。

**字段建议：**

- `trade_date` date
- `symbol` text
- `factor_name` text  —— 对应 `aistock_factor_catalog.factor_name`
- `factor_value` double precision
- `source` text  —— 因子实现来源（如 `qlib_alpha158` / `rdagent` / `aistock_manual`）
- `meta` jsonb null  —— 预留（如说明标准化方式、去极值方法等）
- `created_at` timestamptz default now()

**约束与索引：**

- `UNIQUE (trade_date, symbol, factor_name)`
- 索引：
  - `(factor_name, trade_date)`
  - `(factor_name, symbol)`

### 2.2 `trading.factor_score` —— 因子评估与评分表

**用途**：保存 Alphalens 等评估工具输出的“因子质量”指标，供前端和策略模块使用。

**字段建议：**

- 维度：
  - `factor_name` text
  - `universe` text  —— 如 `CSI300` / `CSI500` / `全A` 等
  - `start_date` date
  - `end_date` date
- 核心评估指标（示例）：
  - `ic_mean` double precision
  - `ic_ir` double precision
  - `long_short_ann_return` double precision  —— 多空组合年化收益
  - `turnover` double precision  —— 因子组合换手率
  - `decile_spread` double precision null  —— 高低分位组合收益差
  - `drawdown` double precision null  —— 因子多空组合最大回撤
  - `score` double precision  —— 统一综合评分（按自定义公式计算）
- 附加信息：
  - `meta` jsonb  —— 存放 Alphalens 返回的摘要统计（如各分位收益、IC 分布）或生成参数
  - `created_at` timestamptz default now()

**约束与索引：**

- `UNIQUE (factor_name, universe, start_date, end_date)`
- 索引：
  - `(factor_name)`
  - `(universe, end_date)`

---

## 3. 因子定义与管理

### 3.1 因子定义来源：`aistock_factor_catalog`

使用已存在的 RD-Agent Catalog 表作为因子定义的唯一来源：

- 字段示意：
  - `factor_name`
  - `expression`
  - `source`
  - `region`
  - `tags`（如 `alpha158`）

### 3.2 补充字段（后续可选）

为了更好地驱动 AIstock 侧因子引擎，可在因子 catalog 或新的配置表中补充：

- `calc_backend`：
  - 如 `qlib` / `pandas` / `rdagent_pipeline` / `custom_py`。
- `universe_default`：
  - 默认使用的股票池/指数，如 `CSI300`。
- `lookback_window`：
  - 计算该因子需要的历史回看窗口长度（如 60 天）。
- `update_frequency`：
  - 因子刷新频率（如 `daily` / `weekly`）。

这些可以作为 **Phase 3 以后** 的增强，目前先在设计中预留，实施时再具体落地。

---

## 4. 因子计算引擎设计

### 4.1 位置与职责

- 后端 Service 建议：`backend/services/factor_engine_service.py`（待创建）。
- 职责：
  - 解析因子定义（从 `aistock_factor_catalog` 及可能的增强配置表中读取）；
  - 调用 Data Service 获取所需行情/财务数据；
  - 执行动子计算（支持多种实现后端）；
  - 将结果批量写入 `trading.factor_value`（支持幂等 upsert）。

### 4.2 对外 API（暂定，后续再精化）

**内部 Python 接口**（Service 层）：

```python
class FactorEngineService:
    def compute_factors(
        self,
        factor_names: list[str] | None = None,
        tag: str | None = None,
        universe: str = "CSI300",
        start_date: str | datetime.date | None = None,
        end_date: str | datetime.date | None = None,
        backfill: bool = False,
    ) -> dict:
        """计算一批因子，并写入 trading.factor_value。

        返回一个简要统计：每个因子的插入/更新行数等。
        """
        ...
```

**预留 FastAPI 管理接口**（未来 Phase 3 可选实现）：

- `POST /api/v1/factors/compute`：
  - 请求体：因子列表 / tag / 时间区间 / universe 等；
  - 行为：在后台异步触发一次因子计算任务，返回任务 ID。

（当前阶段仅在设计层面预留，不要求立刻实现。）

### 4.3 数据获取：统一 Data Service

因子计算所需的行情/财务数据统一通过 Data Service 层获取：

- 日线行情：
  - close / open / high / low / volume / amount 等；
- 基本面/财务数据（如后续需要）：
  - EPS、ROE、PB 等。

具体调用方式复用现有 Data Service API，例如：

- Python Service 内直接调用 `data_service` 适配器；
- 或通过内部 HTTP 调用 `backend/data_service/api.py` 中的接口。

### 4.4 因子计算后端（Backend）策略

考虑到未来可能会有不同来源/实现方式的因子，因子引擎内部可以采用 **策略模式**：

- 对每个因子，根据 `calc_backend` 字段选择不同的计算器：
  - `QlibFactorCalculator`：调用 qlib 或类似框架；
  - `PandasExpressionCalculator`：根据因子表达式在 pandas DataFrame 上直接运算；
  - `RDAgentPipelineCalculator`：将计算委托给某些 RD-Agent 脚本或 pipeline；
  - `CustomPythonCalculator`：为复杂因子注册专门的 Python 函数。

当前设计阶段 **不要求实现具体计算逻辑**，仅在架构上为将来扩展预留抽象接口。

### 4.5 写入因子值表

- 单因子或多因子批量计算后，统一构造 DataFrame：
  - 列：`trade_date`, `symbol`, `factor_name`, `factor_value`, `source`, `meta`
- 使用已有 PostgreSQL 写入模式：
  - 优先考虑 `execute_values` 批量插入
  - SQL 模式：
    ```sql
    INSERT INTO trading.factor_value (
        trade_date,
        symbol,
        factor_name,
        factor_value,
        source,
        meta
    )
    VALUES %s
    ON CONFLICT (trade_date, symbol, factor_name) DO UPDATE SET
        factor_value = EXCLUDED.factor_value,
        source = EXCLUDED.source,
        meta = EXCLUDED.meta;
    ```

---

## 5. 因子评估与 Alphalens 集成

### 5.1 数据准备

Alphalens 典型使用需要两个核心对象：

1. **factor**：
   - MultiIndex `(date, asset)` 的因子值 Series；
2. **prices**：
   - 资产价格时间序列 DataFrame（通常是收盘价）。

在 AIstock 中的映射关系：

- 从 `trading.factor_value` 获取：
  - 指定 `factor_name`、`universe`、`date` 区间；
- 从 Data Service 获取价格：
  - 与上述 universe/date 对齐的 `close`（或 open/close）。

### 5.2 评估流程（离线任务）

设计一个独立的离线脚本/Job，例如 `scripts/evaluate_factors_with_alphalens.py`（未来创建）：

1. 选取待评估因子集合：
   - 可以按 `tag=alpha158`、`source=qlib_alpha158` 或指定因子名列表；
2. 对每个因子循环：
   1. 从 `trading.factor_value` 取出该因子在区间内的 `(date, symbol, factor_value)`；
   2. 从 Data Service 取同一 symbol/universe 的收盘价；
   3. 对齐并构造 Alphalens 所需的 `factor` 与 `prices`；
   4. 调用 Alphalens（伪代码示例）：
      ```python
      factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
          factor,
          prices,
          periods=[1, 5, 10],   # 可配置
          quantiles=5,          # 分位数
      )
      ic = alphalens.performance.factor_information_coefficient(factor_data)
      tears = alphalens.tears.create_full_tear_sheet(
          factor_data,
          long_short=True,
          group_neutral=False,
          by_group=False,
          # 实际使用时可选择只生成数据，不展示图形
      )
      ```
   5. 提取核心指标：
      - 平均 IC / IC_IR
      - 多空组合年化收益 / 回撤
      - 分位组合收益分布
      - Turnover / 自相关
   6. 计算统一的 `score`（如：IC_mean + 0.5 * long_short_ann_return - λ * drawdown）。
   7. 将结果写入 `trading.factor_score`。

### 5.3 运行与调度

- 初始阶段：
  - 手动运行脚本（如每天/每周一次），评估所有候选因子或新增因子；
- 后续阶段（Phase 3 之后）：
  - 将脚本注册到调度系统中，定期更新 `factor_score`；
  - 或提供 API 触发单因子重新评估。

### 5.4 前端展示（预留）

- 在 `/rdagent/factors` 中：
  - 为每个因子增加显示：`IC`, `IC_IR`, `long_short_ann_return`, `score` 等摘要；
  - 根据 `score` 增加简单的“质量标记”（如高/中/低）。
- 可选新增页面 `/rdagent/factors/evaluation`：
  - 展示某一因子的详细评估结果摘要（文字 + 少量统计表），
  - 若需要展示图形，可考虑通过静态图导出或前端轻量重绘关键图表。

---

## 6. 与现有 Phase 设计的关系与实施阶段

### 6.1 与现有设计的关系

- 利用已有：
  - RD-Agent 因子 catalog（`aistock_factor_catalog`）
  - 统一 Data Service 行情层
  - PostgreSQL 作为统一存储
- 在现有 Phase 1/2 上迭代：
  - 不影响已经完成的 RD-Agent Catalog / 策略导入 / signals 可视化；
  - 作为 Phase 3 之后的“因子质量评估”增强模块。

### 6.2 实施阶段建议

当前仅作为**设计备忘录**，不立即编码，建议拆分为以下阶段：

1. **阶段 A（数据基础）**：
   - 建表：`trading.factor_value` / `trading.factor_score`；
   - 写一个最小脚本，将 1~2 个简单因子（如市值、PE）计算并写入 `factor_value`。
2. **阶段 B（Alphalens 通路打通）**：
   - 为上述 1~2 个因子跑通 Alphalens，并写入 `factor_score`；
   - 在 notebook 或简单 CLI 输出关键信息，验证指标合理性。
3. **阶段 C（批量因子 + 前端展示）**：
   - 针对 `tag=alpha158` 等成体系因子批量计算与评估；
   - 在 `/rdagent/factors` 上加“评分/IC”列，做简单排序与筛选。
4. **阶段 D（与策略/模型联动）**：
   - 在策略/模型页面中引入因子质量信息，例如：
     - 显示该策略使用因子的平均/最低因子评分；
     - 在模型选择时参考因子质量。

---

## 7. 结论

- 在 AIstock 侧实现因子计算与评估是可行且与现有架构高度契合的：
  - 依托统一 Data Service 获取行情/财务数据；
  - 使用已有因子 catalog 作为统一定义来源；
  - 利用 PostgreSQL 存储因子值与评估结果；
  - 使用 Alphalens 做系统性评估。
- 本文件仅作为 **2025-12-30 的设计备忘录**，为 Phase 3 之后的实现提供清晰的蓝图，当前不要求立即落地具体代码。
