# 字段映射与 Schema 生成设计方案

> 版本：v1.0  
> 日期：2025-02-09  
> 状态：待实施

## 一、问题分析

### 1.1 当前状态问题

1. **dtype_hint 字段为空**：`aistock_field_map.csv` 中的 `dtype_hint` 列始终为空，未填充数据类型信息
2. **字段映射不会自动生成**：导出 H5 文件后需手动调用 API 生成字段映射
3. **无统一 Schema 文件**：未生成 RD-Agent 可用的 `static_factors_schema.csv`

### 1.2 数据来源对比

| 数据来源 | 类型示例 | 适用场景 |
|---------|---------|---------|
| PostgreSQL 原始类型 | `numeric(12,4)`, `double precision` | 数据库存储精度描述 |
| Pandas DataFrame 类型 | `float64`, `int64` | 运行时数据操作 |

**结论**：RD-Agent 需要** Pandas 数据类型**（`float64`），而非数据库原始类型。

### 1.3 RD-Agent 需求分析

根据 `prompts_dataset_info.yaml:7`：
```yaml
列为一个或多个 float32 类型的因子列
```

RD-Agent 在 `utils.py:141` 中直接使用 dtype 信息：
```python
line = f"- {col} ({dtype})"  # 期望：- db_close (float64)
```

**关键发现**：
- RD-Agent 期望的是**运行时数据类型**（pandas dtype）
- 数据库类型 `numeric(12,4)` 对 RD-Agent 无实际意义
- 数据最终会被加载为 pandas DataFrame，类型由 H5 文件决定

---

## 二、设计方案（方案 B：统一 Schema 文件）

### 2.1 核心决策

| 决策项 | 选择 | 理由 |
|-------|------|------|
| Schema 文件方案 | **方案 B**（统一文件） | RD-Agent 已内置支持 `static_factors_schema.csv`，单文件读取效率更高 |
| 数据类型来源 | **Pandas H5 文件** | RD-Agent 实际需要的是运行时数据类型，不是数据库原始类型 |
| 自动生成触发 | **导出后自动触发** | 用户无感知，导出完成立即可用 |

### 2.2 数据流设计

```
导出 H5 文件 → 读取 H5 获取列名和 pandas dtype → 查询 PG 获取注释 → 
生成 FieldMapRow → 写入 aistock_field_map.csv → 
生成 static_factors_schema.csv（统一文件） → 附加列注释到 H5 属性
```

### 2.3 文件结构

```
qlib_export_YYYYMMDD/
├── bak_basic.h5                    # 包含列注释属性
├── cyq_perf.h5                     # 包含列注释属性
├── daily_basic.h5                  # 包含列注释属性
├── moneyflow.h5                    # 包含列注释属性
├── ...
└── metadata/
    ├── aistock_field_map.csv       # AIstock 内部使用（完整信息）
    │   └── 列：name, meaning_cn, unit, source_table, comment, dtype_hint
    └── schemas/
        └── static_factors_schema.csv   # RD-Agent 专用（统一 Schema）
            └── 列：name, meaning_cn, unit, source_table, comment, dtype
```

### 2.4 CSV 文件格式

#### 2.4.1 aistock_field_map.csv（AIstock 内部）

```csv
name,meaning_cn,unit,source_table,comment,dtype_hint
bb_pe_dyn,动态市盈率,,bak_basic,动态市盈率,float64
bb_eps,每股收益,,bak_basic,每股收益,float64
cp_winner_rate,胜率,,cyq_perf,筹码胜率,float64
cp_profit_ratio,获利比例(%),,cyq_perf,获利比例,float64
db_close,收盘价,,daily_basic,日线收盘价,float64
db_turnover_rate,换手率(%),,daily_basic,换手率(%),float64
db_circ_mv,流通市值(万元),,daily_basic,流通市值,float64
mf_net_mf_amount,主力净流入额(万元),,moneyflow,主力净流入额,float64
mf_buy_sm_amount,小单买入额(万元),,moneyflow,小单买入额,float64
```

#### 2.4.2 static_factors_schema.csv（RD-Agent 专用）

```csv
name,meaning_cn,unit,source_table,comment,dtype
bb_pe_dyn,动态市盈率,,bak_basic,动态市盈率,float64
bb_eps,每股收益,,bak_basic,每股收益,float64
cp_winner_rate,胜率,,cyq_perf,筹码胜率,float64
cp_profit_ratio,获利比例(%),,cyq_perf,获利比例,float64
db_close,收盘价,,daily_basic,日线收盘价,float64
db_turnover_rate,换手率(%),,daily_basic,换手率(%),float64
db_circ_mv,流通市值(万元),,daily_basic,流通市值,float64
db_pe,市盈率(总市值/净利润),,daily_basic,市盈率,float64
mf_net_mf_amount,主力净流入额(万元),,moneyflow,主力净流入额,float64
mf_buy_sm_amount,小单买入额(万元),,moneyflow,小单买入额,float64
```

### 2.5 数据类型映射

从 H5 文件读取的实际 pandas dtype 直接写入：

| H5 中的 pandas dtype | CSV dtype 值 | 说明 |
|---------------------|-------------|------|
| `float64` | `float64` | 双精度浮点数 |
| `float32` | `float32` | 单精度浮点数 |
| `int64` | `int64` | 64位整数 |
| `int32` | `int32` | 32位整数 |
| `bool` | `bool` | 布尔值 |
| `object` | `object` | 字符串或其他对象 |
| `datetime64[ns]` | `datetime64[ns]` | 日期时间 |

**RD-Agent 输出示例**：
```
- db_close (float64): 收盘价
- bb_pe_dyn (float64): 动态市盈率
- mf_net_mf_amount (float64): 主力净流入额
```

---

## 三、实施细节

### 3.1 修改点清单

#### 修改 1：填充 dtype_hint（field_map.py）

```python
def build_field_map_rows_for_snapshot(...) -> List[FieldMapRow]:
    # 读取 H5 文件并获取 dtype
    df = pd.read_hdf(h5_path, key="data", start=0, stop=5)  # 只读少量数据
    dtype_hints = _infer_dtype_hints(df)  # 返回 {col: dtype_str}
    
    # 为每个字段创建 FieldMapRow
    for col in columns:
        row = FieldMapRow(
            name=exported_col_name,
            meaning_cn=comment or fallback_meaning,
            unit=infer_unit_from_comment(comment),
            source_table=table_name,
            comment=full_comment,
            dtype_hint=dtype_hints.get(original_col, "float64"),  # ← 填充数据类型
        )
```

#### 修改 2：导出后自动触发（exporter.py / 各 exporter）

在每个 Exporter 的 `export_full` 方法末尾添加：

```python
def export_full(self, snapshot_id: str, **kwargs):
    # ... 现有导出逻辑 ...
    
    # 导出完成后自动触发字段映射生成
    try:
        from ..field_map_service import export_field_map_for_snapshot
        result = export_field_map_for_snapshot(
            snapshot_id=snapshot_id,
            write_to_h5=True
        )
        logger.info(f"Auto-generated field map: {result.get('rows', 0)} rows")
    except Exception as e:
        logger.warning(f"Failed to auto-generate field map: {e}")
        # 不影响主导出流程
```

**涉及的 Exporter 文件**：
- `bak_basic_exporter.py`
- `cyq_perf_exporter.py`
- `daily_basic_exporter.py`
- `moneyflow_exporter.py`
- 其他数据集 exporter

#### 修改 3：生成统一 Schema 文件（field_map_service.py）

```python
def export_field_map_for_snapshot(snapshot_id: str, write_to_h5: bool = True):
    # ... 生成 aistock_field_map.csv ...
    
    # 生成 RD-Agent 专用的统一 Schema 文件
    schema_dir = get_metadata_path(snapshot_id) / "schemas"
    schema_dir.mkdir(parents=True, exist_ok=True)
    
    # 生成 static_factors_schema.csv
    schema_csv_path = schema_dir / "static_factors_schema.csv"
    generate_static_factors_schema(all_rows, schema_csv_path)
    
    return {
        "csv_path": str(csv_path),
        "schema_path": str(schema_csv_path),
        "rows": len(all_rows),
    }

def generate_static_factors_schema(rows: List[FieldMapRow], output_path: Path):
    """生成 RD-Agent 兼容的统一 Schema 文件"""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'meaning_cn', 'unit', 'source_table', 'comment', 'dtype'])
        for row in rows:
            writer.writerow([
                row.name,
                row.meaning_cn,
                row.unit,
                row.source_table,
                row.comment,
                row.dtype_hint  # 使用 pandas dtype
            ])
```

### 3.2 增量更新机制

支持多次导出后合并字段映射：

```python
def export_field_map_for_snapshot(snapshot_id: str, write_to_h5: bool = True):
    csv_path = get_metadata_path(snapshot_id) / "aistock_field_map.csv"
    
    # 读取已存在的字段映射（如果有）
    existing_rows = []
    if csv_path.exists():
        existing_rows = read_existing_field_map(csv_path)
    
    # 只处理当前存在的 H5 文件对应的数据集
    current_datasets = detect_available_datasets(snapshot_id)
    
    # 为每个数据集生成字段映射行
    new_rows = []
    for dataset in current_datasets:
        rows = build_field_map_rows_for_snapshot(snapshot_id, dataset)
        new_rows.extend(rows)
    
    # 合并：保留未变化的数据集字段，更新新增/变化的
    merged_rows = merge_field_maps(existing_rows, new_rows)
    
    # 写入 CSV
    write_field_map_csv(merged_rows, csv_path)
    
    # 重新生成统一 Schema 文件
    schema_csv_path = get_metadata_path(snapshot_id) / "schemas" / "static_factors_schema.csv"
    generate_static_factors_schema(merged_rows, schema_csv_path)
```

### 3.3 命名规范

| 数据集 | 前缀 | 示例字段 |
|--------|------|---------|
| bak_basic | `bb_` | `bb_pe_dyn`, `bb_eps` |
| cyq_perf | `cp_` | `cp_winner_rate`, `cp_profit_ratio` |
| daily_basic | `db_` | `db_close`, `db_turnover_rate` |
| moneyflow | `mf_` | `mf_net_mf_amount`, `mf_buy_sm_amount` |

---

## 四、RD-Agent 兼容性验证

### 4.1 文件搜索路径（已支持）

`utils.py:46-90` 中已配置的搜索路径：

```python
def _candidate_schema_paths_for_file(p: Path) -> list[Path]:
    cands = [
        p.with_name(f"{stem}_schema.csv"),  # 同级目录
        p.with_name(f"{stem}_schema.json"),
        cur / "metadata" / "schemas" / f"{stem}_schema.csv",  # metadata/schemas
        gov_dir / "schemas" / f"{stem}_schema.csv",  # data_governance
    ]
    # ...
```

### 4.2 Schema 读取逻辑（已支持）

`utils.py:114-180` 中已实现的读取逻辑：

```python
def _load_schema_preview(schema_path: Path, data_path: Optional[Path] = None, max_rows: int = 60):
    if schema_path.suffix.lower() == ".csv":
        df = pd.read_csv(schema_path)
        for _, row in df.head(max_rows).iterrows():
            col = str(row.get("name", ""))
            dtype = row.get("dtype")  # ← 读取 dtype 列
            # ...
            if not _is_nan(dtype):
                line = f"- {col} ({dtype})"  # ← 输出格式
```

### 4.3 验证标准

1. **CSV 验证**：`dtype` 列非空，值为有效的 pandas dtype（如 `float64`, `int64`）
2. **RD-Agent 验证**：`_load_schema_preview` 输出包含数据类型，如 `- db_close (float64): 收盘价`
3. **自动化验证**：导出 H5 后无需手动调用 API，字段映射自动生成

---

## 五、实施计划

| 阶段 | 任务 | 优先级 | 预计工时 |
|------|------|--------|---------|
| P0 | 修复 `dtype_hint` 填充（从 H5 读取 pandas dtype） | 高 | 2h |
| P1 | 导出后自动触发字段映射生成 | 高 | 3h |
| P2 | 生成统一 `static_factors_schema.csv` | 高 | 2h |
| P3 | 增量更新机制 | 中 | 2h |
| P4 | 集成测试与 RD-Agent 兼容性验证 | 高 | 3h |

---

## 六、附录

### 6.1 数据库类型 vs Pandas 类型对比

| 场景 | 推荐类型 | 理由 |
|------|---------|------|
| RD-Agent 使用 | **Pandas dtype** (`float64`) | RD-Agent 直接操作 pandas DataFrame |
| 数据库存储 | PostgreSQL 类型 (`numeric(12,4)`) | 保证存储精度 |
| H5 文件存储 | N/A（由 pandas 决定） | pandas to_hdf 自动处理 |

### 6.2 关键代码引用

1. **RD-Agent 提示词要求**（`prompts_dataset_info.yaml:7`）：
   ```yaml
   列为一个或多个 float32 类型的因子列
   ```

2. **RD-Agent Schema 读取**（`utils.py:141`）：
   ```python
   line = f"- {col} ({dtype})"
   ```

3. **RD-Agent 已支持的搜索路径**（`utils.py:46-90`）：
   - 包含 `metadata/schemas/` 目录

### 6.3 相关文件

| 文件路径 | 作用 |
|---------|------|
| `backend/qlib_exporter/field_map.py` | 字段映射生成逻辑 |
| `backend/qlib_exporter/field_map_service.py` | 导出服务 |
| `backend/qlib_exporter/exporter.py` | 基础导出器 |
| `backend/qlib_exporter/bak_basic_exporter.py` | bak_basic 导出器 |
| `backend/qlib_exporter/cyq_perf_exporter.py` | cyq_perf 导出器 |
| `RD-Agent/utils.py` | RD-Agent Schema 读取 |
| `RD-Agent/prompts_dataset_info.yaml` | RD-Agent 数据类型要求 |

---

**结论**：采用 **方案 B（统一 Schema 文件）**，数据类型从 **H5 文件（pandas dtype）** 获取，更符合 RD-Agent 的实际需求。
