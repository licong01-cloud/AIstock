# AIstock × Qlib 日频前复权数据集成设计与实施方案

> 约束：  
> - 仅使用 **现有数据库中的前复权日线表** 作为日线行情基础数据源。  
> - 不做“试玩版 / 精简版”，直接按正式方案设计与实现。  
> - 实施过程中，**设计文档与进度需要同步更新**，确保严格按计划执行。

---

## 1. 范围与目标

### 1.1 范围

- 数据频率：**日频**（前复权价格，含 OHLCV + amount + 多因子列）。
- 数据源：现有 PostgreSQL/TimescaleDB 中的“前复权日线表”，例如：
  - `market.kline_daily_qfq` 或其他当前使用的前复权表（在配置中精确指定）。
- 输出形态：Qlib/RD-Agent 可直接使用的日频特征视图：
  - 首选形式：`daily_pv.h5` 宽表（MultiIndex `(datetime, instrument)`）。
  - 同时可生成 `instruments/all.txt` 与 `calendars/day.txt`，以备 Qlib 自带工具使用。

### 1.2 目标

- **阶段性目标**：
  - 从前复权日线表直接导出完整的 **Qlib Snapshot（日频）**，供 RD-Agent/Qlib 回测使用。
  - 支持 **增量导出**：每日收盘后，仅导出新增日期的数据到同一 Snapshot 中。
  - 建立**统一字段中间层**，为后续实盘和多因子扩展提供稳定的 schema。
  - 在 Web 前端提供完整 UI：配置、触发导出、监控进度、查看 Snapshot 列表。
- **长期目标**：
  - Snapshot 数据直接作为 RD-Agent 多因子研究与回测基准；
  - 实盘端通过相同字段定义从 DB 获取特征，保证训练/回测/实盘三者一致。

---

## 2. 系统角色与职责

### 2.1 AIstock（本项目）

- 唯一权威数据源（Single Source of Truth）：
  - 行情（分/日线，含复权价）  
  - 成交量/金额  
  - 资金流、筹码、板块、指数  
  - 因子表、标签表
- 职责：
  - 数据清洗、复权、对齐。
  - 多源因子计算与统一存储（如 `factor_daily`）。
  - 从 DB 导出 **Qlib Snapshot**（供 RD-Agent/Qlib 用）。
  - 实盘调度与执行（通过 QMT 或其他交易通道）。
  - Web UI：本地数据管理、Qlib 导出管理、策略上线管理。

### 2.2 Qlib

- 角色：**研究与回测视图 + 回测引擎**
  - 消费 AIstock 导出的 Snapshot（本地目录 / HDF5）。
  - 提供 DataHandler、因子/模型 Pipeline、回测与评估工具。
- 不直接连 DB、不负责增量补数。

### 2.3 RD-Agent

- 角色：**研究引擎（LLM 驱动的策略/因子/模型生成工具）**
  - 假设数据已在 Qlib/HDF5 视图中准备完毕。
  - 生成因子、模型、策略代码，并调用 Qlib 做回测。
- 与 AIstock 通过：
  - Snapshot 路径（`QLIB_DATA_PATH`）；
  - 实验配置（`prompts.yaml`, `.env`）解耦。

---

## 3. 数据模型与字段规范（仅日频前复权）

### 3.1 前复权日线表假设

以 `market.kline_daily_qfq` 为例（实际表名在配置中指定）：

- 关键字段（示例）：
  - `trade_date`：日期（`date` 或 `timestamp`）。
  - `ts_code`：标的代码（如 `000001.SZ`）。
  - `open_li` / `high_li` / `low_li` / `close_li`：前复权 OHLC。
  - `volume_hand`：成交量（手）。
  - `amount_li`：成交金额。
- 未来可 join 的因子表：
  - `factor_daily`：以 `(ts_code, trade_date)` 为主键的多因子宽表（资金、筹码、板块、指数等）。

### 3.2 逻辑字段名（策略/因子统一视角）

策略与特征工程统一使用以下逻辑字段名：

- 行情基础：
  - `open`, `high`, `low`, `close`, `volume`, `amount`
- 因子与标签（示例）：
  - 资金因子：`fund_main_net`, `fund_main_ratio`, ...
  - 筹码因子：`chip_avg_cost`, `chip_concentration`, ...
  - 板块/指数：`sector_mom_20d`, `index_sh000001_ret_1d`, ...
  - 标签：`label_ret_1d`, `label_rank_5d`, ...

### 3.3 字段映射中间层

在 `backend/qlib_exporter/config.py` 中定义字段映射（**必须存在，不可省略**）：

```python
# 从 DB 列名 → 逻辑字段名
FIELD_MAPPING_DB_DAILY = {
    "datetime": "trade_date",   # DB 中作为日期时间索引的字段
    "open": "open_li",
    "high": "high_li",
    "low": "low_li",
    "close": "close_li",
    "volume": "volume_hand",
    "amount": "amount_li",
    # 因子列（示意）
    "fund_main_net": "fund_main_net",
    "fund_main_ratio": "fund_main_ratio",
    "chip_avg_cost": "chip_avg_cost",
    "chip_concentration": "chip_concentration",
    # ...
}
```

- 逻辑字段名在：
  - Snapshot（`daily_pv.h5`）中直接使用；
  - 实盘 `LiveDataAdapter` 输出中使用；
  - RD-Agent/Qlib 因子/策略代码中使用。

---

## 4. Qlib Snapshot 导出设计（仅日频）

### 4.1 Snapshot 目录结构

在 `QLIB_SNAPSHOT_ROOT` 下，为每次需要的样本区间和股票池创建一个目录，例如：

```text
QLIB_SNAPSHOT_ROOT/
  quant_2015_2020_allA/
    daily_pv.h5        # MultiIndex (datetime, instrument)，列为逻辑字段 + 因子 + label
    meta.json          # Snapshot 元信息
    instruments/
      all.txt          # ts_code start end
    calendars/
      day.txt          # 交易日历（从 trade_date 导出）
```

### 4.2 HDF5 宽表规范

`daily_pv.h5`：

- Index：`MultiIndex [datetime, instrument]`
  - `datetime`: pandas Timestamp（由 `trade_date` 转换）。
  - `instrument`: `ts_code`。
- Columns：
  - 至少包括：`open`, `high`, `low`, `close`, `volume`, `amount`
  - 再加上所有因子和标签列（以后扩展）。

---

## 5. 模块结构与接口（仅日频）

### 5.1 文件结构

```text
backend/
  qlib_exporter/
    __init__.py
    config.py          # 路径、市场、字段映射等
    db_reader.py       # 从前复权日线表 + 因子表读取数据
    meta_repo.py       # 记录每个 ts_code 的 last_datetime（增量导出用）
    snapshot_writer.py # 写入 daily_pv.h5 + meta.json + instruments/all.txt + calendars/day.txt
    exporter.py        # 提供 full / incremental 导出接口
    router.py          # FastAPI 路由（/api/v1/qlib/...）
```

### 5.2 `config.py`

- 内容包括：

  - `QLIB_SNAPSHOT_ROOT`：Snapshot 根路径。
  - `QLIB_MARKET`: 如 `"aistock"`。
  - `DAILY_QFQ_TABLE`: 前复权日线表名，如 `"market.kline_daily_qfq"`。
  - `FIELD_MAPPING_DB_DAILY`：如第 3.3 节所述。
  - 默认股票池 / 时间范围（可选）。

### 5.3 `db_reader.py`

- 核心接口：

```python
class DBReader:
    def __init__(self, get_conn):
        self._get_conn = get_conn

    def get_all_ts_codes(self) -> list[str]:
        """从前复权日线表中查询所有 ts_code（去重）。"""

    def load_daily(
        self,
        ts_codes: list[str],
        start: date | None,
        end: date | None,
    ) -> pd.DataFrame:
        """从前复权日线表（以及未来的 factor_daily 等表）读取日频数据，
        返回 MultiIndex (datetime, instrument) + 逻辑列的宽表。
        """
```

- 实现要点：
  - SQL 从 `DAILY_QFQ_TABLE` 联因子表（后续阶段）。
  - 应用 `FIELD_MAPPING_DB_DAILY` 做列重命名。
  - 索引为 `(trade_date, ts_code)`，排序后再重命名 index level 为 `(datetime, instrument)`。

### 5.4 `meta_repo.py`（增量导出元数据）

- DB 表：

```sql
CREATE TABLE IF NOT EXISTS market.qlib_export_meta (
  ts_code       text        NOT NULL,
  freq          text        NOT NULL, -- '1d'
  last_datetime timestamptz NOT NULL,
  PRIMARY KEY (ts_code, freq)
);
```

- 接口：

```python
class MetaRepo:
    def get_last_datetime(self, ts_code: str, freq: str = "1d") -> datetime | None: ...
    def upsert_last_datetime(self, ts_code: str, freq: str, dt: datetime) -> None: ...
```

### 5.5 `snapshot_writer.py`

- 核心 API：

```python
class SnapshotWriter:
    def __init__(self, snapshot_root: Path, market: str):
        ...

    def write_daily_full(self, snapshot_id: str, df: pd.DataFrame) -> None:
        """全量写 daily_pv.h5 + meta.json + instruments/all.txt + calendars/day.txt"""

    def write_daily_incremental(
        self,
        snapshot_id: str,
        df_new: pd.DataFrame,
    ) -> None:
        """基于已有 daily_pv.h5 进行增量更新（注意去重和时间对齐）"""
```

- 要求：
  - 全量导出时，若已有同名 Snapshot，可配置为覆盖或报错（由配置控制）。
  - 增量导出时，严格基于 `MetaRepo.last_datetime` 和现有 HDF5 内容进行合并，并保持索引唯一性与有序性。

### 5.6 `exporter.py`

封装对外导出逻辑：

```python
class QlibDailyExporter:
    def __init__(self, db: DBReader, writer: SnapshotWriter, meta: MetaRepo):
        ...

    def export_full(
        self,
        snapshot_id: str,
        start: date,
        end: date,
        ts_codes: list[str] | None = None,
    ) -> ExportResult:
        ...

    def export_incremental(
        self,
        snapshot_id: str,
        end: date,
        ts_codes: list[str] | None = None,
    ) -> ExportResult:
        ...
```

`ExportResult` 记录：

- 导出的 ts_code 数量；
- 每个 ts_code 的起止时间、记录数；
- 耗时统计；
- 错误明细。

---

## 6. FastAPI API 与前端 UI（仅日频阶段）

### 6.1 后端路由 `/api/v1/qlib`

- `GET /config`
  - 返回当前配置：
    - `snapshot_root`, `market`, `daily_qfq_table`, 可用字段列表等。

- `POST /config`
  - 更新上述配置（写入配置文件或数据库）。

- `POST /snapshots/daily`
  - 请求体：

    ```json
    {
      "snapshot_id": "quant_2015_2020_allA",
      "mode": "full" | "incremental",
      "codes": ["000001.SZ", "000002.SZ"],
      "start": "2015-01-01",
      "end": "2020-12-31"
    }
    ```

  - 行为：
    - 提交一个导出任务，返回 `job_id` + `snapshot_id`。

- `GET /snapshots/daily`
  - 罗列现有日频 Snapshots（id、时间范围、股票池大小、字段数等）。

- `GET /jobs/{job_id}`
  - 返回导出任务进度与摘要。

### 6.2 前端 `/qlib` 页面

- 在 UI 上明确：**目前只支持“日频前复权”导出**。
- 表单：
  - Snapshot ID
  - 模式（全量/增量）
  - 股票池：全市场 / 自选 ts_code 列表
  - 时间范围（全量：start+end；增量：至少 end）
- 按钮：
  - “开始导出” → 调用 `/snapshots/daily`，并展示 job 状态。
- 列表：
  - Snapshot 列表 & 导出任务列表。

---

## 7. 实盘数据中间层与策略复用（展望）

虽然本阶段只实现日频 Snapshot 导出，但需要提前规划实盘中间层，以保证未来训练/回测/实盘一致性。

### 7.1 LiveDataAdapter 设计（与 Snapshot 对齐）

在 AIstock 中实现：

```python
class LiveDataAdapter:
    def __init__(self, db_reader: DBReader, tdx_client: Optional[TDXClient]):
        ...

    def get_feature_df(
        self,
        symbols: list[str],
        now: datetime,
        window: int,
        freq: str = "1d",
    ) -> pd.DataFrame:
        """
        返回结构与 Snapshot 训练/回测时相同的 MultiIndex (datetime, instrument) DataFrame。
        """
```

- 内部：
  - 从 DB 读取最近 `window` 个交易日的前复权日线 + 因子表；
  - 可选：从 TDX 实时数据补上最新一根 bar；
  - 应用与导出模块相同的 `FIELD_MAPPING_DB_DAILY`；
  - 做与 Snapshot 相同的复权、缺失值处理与排序。

### 7.2 策略/因子代码复用路径

- 回测阶段：
  - RD-Agent + Qlib 基于 Snapshot（`daily_pv.h5`）进行因子与策略研发。
  - 信号逻辑封装为函数/类，如 `generate_signals(df_features)`。

- 实盘阶段：
  - AIstock 通过 `LiveDataAdapter` 拉取最新窗口特征（与 Snapshot 列结构一致）；
  - 使用相同的 `generate_signals` 生成买卖信号；
  - 由交易模块（QMT 等）执行并回写结果到 DB。

---

## 8. 实施计划与进度管理（仅“日频前复权”范围）

> 要求：实施过程中**实时更新设计和进度**，不做任何功能简化。

### 阶段 1：架构落地与骨架搭建

**目标：** 创建 `qlib_exporter` 模块骨架与配置，打通 DBReader→SnapshotWriter→Exporter 的最小链路（仅全量导出逻辑，日频前复权）。

**任务列表：**

1. 创建目录 `backend/qlib_exporter/` 与基础文件（`__init__.py`, `config.py`, `db_reader.py`, `snapshot_writer.py`, `exporter.py`, `meta_repo.py`）。
2. 在 `config.py` 写入：
   - `QLIB_SNAPSHOT_ROOT`
   - `QLIB_MARKET`
   - `DAILY_QFQ_TABLE`
   - `FIELD_MAPPING_DB_DAILY`（根据实际表结构补全）。
3. 实现 `DBReader.get_all_ts_codes()` 与 `DBReader.load_daily()`（首阶段可仅包含前复权行情列，因子 join 在后续阶段实现）。
4. 实现 `SnapshotWriter.write_daily_full()`（写 `daily_pv.h5` + `meta.json` + instruments/ + calendars/）。
5. 实现 `QlibDailyExporter.export_full()`，支持对指定时间区间与股票池的全量导出。
6. 编写简单的集成测试脚本或临时 CLI，在本地验证导出结果的正确性（索引、列名、行数）。

**进度管理：**

- 在项目内维护 `docs/qlib_daily_export_progress.md`：
  - 每完成一项任务立即打勾并记录日期；
  - 若设计需微调，更新本设计文档对应段落，并在进度文档中注明变更原因。

### 阶段 2：后端 API + 前端 UI（日频全量）

**目标：** 用 Web UI 发起日频全量 Snapshot 导出，并可查看任务状态。

**任务列表：**

1. 在 FastAPI 中挂载 `/api/v1/qlib` 路由，创建 `router.py`。 
2. 实现：
   - `POST /api/v1/qlib/snapshots/daily`：调用 `QlibDailyExporter.export_full()`，同步或通过后台任务执行，返回 `job_id` + `snapshot_id`。
   - `GET /api/v1/qlib/snapshots/daily`：扫描 `QLIB_SNAPSHOT_ROOT` 下目录并读取 `meta.json`，罗列 Snapshot。
   - `GET /api/v1/qlib/jobs/{job_id}`：基于现有任务表或新增表记录导出任务状态与结果摘要。
3. 前端 `/qlib` 页面 V1：
   - 完成只支持日频全量导出的表单与按钮；
   - 显示最近若干导出任务的状态。
4. 在 `docs/qlib_daily_export_progress.md` 中更新 API 和 UI 完成情况，并记录首次从前端成功触发 Snapshot 导出的时间点。

### 阶段 3：增量导出 + 元数据表（日频前复权）

**目标：** 支持日频前复权的增量导出，避免全表重刷。

**任务列表：**

1. 在 DB 中创建 `market.qlib_export_meta` 表（如 5.4 所述）。
2. 实现 `MetaRepo` 的 `get_last_datetime` / `upsert_last_datetime`。
3. 在 `QlibDailyExporter` 中实现 `export_incremental()`：
   - 对每个 ts_code：
     - 查 `last_datetime`；
     - 从前复权表（及因子表）中只拉新增日期记录；
     - 合并到现有 `daily_pv.h5`，确保索引无重复且按时间排序；
     - 更新 `qlib_export_meta` 中的 `last_datetime`。
4. 扩展 `POST /api/v1/qlib/snapshots/daily` 支持 `mode=incremental`；
   - 后端根据 mode 调用 `export_full` 或 `export_incremental`。
5. 在 `/qlib` 页面中加入增量导出选项，并展示最近一次增量日期。
6. 在进度文档中记录：
   - 多次真实增量导出测试结果；
   - 每次增量导出影响的行数与耗时统计。

### 阶段 4：多因子接入（仍仅日频前复权）

**目标：** 将 DB 中的日频因子表 join 进 Snapshot，使 Qlib/RD-Agent 直接看到多因子宽表。

**任务列表：**

1. 确定日频因子表结构（如 `market.factor_daily`）：
   - 主键 `(ts_code, trade_date)`；
   - 多个因子列（统一使用 DB 内部命名）。
2. 在 `DBReader.load_daily()` 中：
   - 将行情表与因子表在 `(ts_code, trade_date)` 维度 join；
   - 对所有因子列通过 `FIELD_MAPPING_DB_DAILY` 映射到逻辑列名；
   - 确保输出宽表不会出现列名冲突。
3. 更新 `SnapshotWriter` 逻辑，确保因子列被正确写入 `daily_pv.h5`，并在 `meta.json` 中记录字段列表和说明（可选）。
4. 在 `/qlib` 页面中：
   - 显示每个 Snapshot 中字段数量和部分示例列名；
   - 可选：支持按勾选的因子列集进行导出（未来扩展）。
5. 在进度文档中记录：
   - 多因子 Snapshot 被 RD-Agent/Qlib 成功加载与使用的实验情况；
   - 对策略表现的初步观察（可选）。

---

## 9. 文档与进度更新要求

- 本 `qlib_daily_qfq_design.md` 作为**主设计说明文档**：
  - 后续如有架构或字段设计调整，必须更新相应章节；
  - 在文末增加“变更记录”说明原因与时间。
- 每个阶段在 `docs/qlib_daily_export_progress.md` 中维护细粒度进度：
  - 每完成一个子任务，立即标记完成日期；
  - 如遇阻塞，需要记录问题描述、影响和临时解决方案。

---

## 10. 变更记录

- 2025-12-04：创建初始版本，定义日频前复权 × Qlib 集成的总体架构与实施计划。
- 2025-12-05：阶段 1 完成，包括日频、分钟线、板块日线的全量导出功能；修复 HDF5 MultiIndex 扩展 dtype 兼容性问题。
