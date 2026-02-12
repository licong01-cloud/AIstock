# Qlib CSV 转 Bin 文件数据转换失败分析

## 1. 概述

本文档分析 Qlib Snapshot 导出程序中 CSV 数据转换为 bin（HDF5）文件过程中，可能导致数据转换失败并产生 NaN、空值或 0 值的原因。

## 2. 数据转换流程

### 2.1 完整流程

```
PostgreSQL 数据库 → DBReader → 数据转换 → SnapshotWriter → HDF5 文件
```

### 2.2 关键转换步骤

#### 步骤1：从数据库读取原始数据（DBReader）

**文件位置**: `backend/qlib_exporter/db_reader.py`

**关键代码**（第766-771行）：
```python
# 1. 数值缩放 (厘 -> 元, 手 -> 股)
df["open"] = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
df["high"] = pd.to_numeric(df["high_li"], errors="coerce") / PRICE_UNIT_DIVISOR
df["low"] = pd.to_numeric(df["low_li"], errors="coerce") / PRICE_UNIT_DIVISOR
df["close"] = pd.to_numeric(df["close_li"], errors="coerce") / PRICE_UNIT_DIVISOR
df["amount"] = pd.to_numeric(df["amount_li"], errors="coerce") / PRICE_UNIT_DIVISOR
df["volume"] = pd.to_numeric(df["volume_hand"], errors="coerce") * 100.0
```

#### 步骤2：数据标准化（SnapshotWriter）

**文件位置**: `backend/qlib_exporter/snapshot_writer.py`

**关键代码**（第77-82行）：
```python
# 强制数值列为 float，避免 HDF5 写入时出现 int64（例如 amount 变成全 0）
if "amount" not in tmp.columns:
    tmp["amount"] = float("nan")
for col in ["open", "high", "low", "close", "volume", "amount", "factor"]:
    if col in tmp.columns:
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
```

**关键代码**（第148-150行，分钟数据）：
```python
# 强制将所有数值列转为 float64，避免扩展 dtype（如 Int64）导致 HDF5 写入失败
for col in ["open", "high", "low", "close", "volume", "amount"]:
    if col in tmp.columns:
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
```

## 3. 可能导致数据转换失败的原因

### 3.1 使用 `pd.to_numeric(..., errors="coerce")` 的风险

**核心问题**：`errors="coerce"` 参数会将无法转换为数字的值设为 `NaN`

#### 3.1.1 可能产生 NaN 的情况

1. **原始数据为 NULL 或空字符串**
   - 数据库中的字段为 NULL
   - CSV 文件中字段为空字符串 ""
   - 字段值为 None

2. **原始数据为非数字字符串**
   - 字段值为 "N/A"、"NULL"、"-" 等特殊字符串
   - 字段值为文本描述（如 "停牌"、"无数据"）
   - 字段值为科学计数法格式错误（如 "1.23e"）

3. **原始数据为异常数值**
   - 超出浮点数表示范围（如 1e308）
   - 包含非数字字符（如 "123,456" 中的逗号）
   - 包含货币符号（如 "¥123.45"）

4. **字段不存在或类型错误**
   - 查询结果中缺少必需的字段
   - 字段类型为 TEXT 但包含非数字内容
   - 字段类型为 JSON 或其他复杂类型

#### 3.1.2 可能产生 0 值的情况

1. **数值缩放计算错误**
   - 原始值为 0，除以 PRICE_UNIT_DIVISOR（1000）后仍为 0
   - 原始值为 0，乘以 100.0 后仍为 0

2. **强制类型转换**
   - `astype("float64")` 可能将某些异常值转换为 0
   - HDF5 写入时的数值截断

### 3.2 特殊场景分析

#### 场景1：amount 字段缺失

**代码位置**: `snapshot_writer.py` 第78-79行

```python
if "amount" not in tmp.columns:
    tmp["amount"] = float("nan")
```

**影响**：
- 如果 DataFrame 中没有 amount 列，会直接设置为 `NaN`
- 这会导致所有缺失 amount 的数据点都变成 NaN

#### 场景2：除法运算产生 NaN

**代码位置**: `db_reader.py` 第766-771行

```python
df["open"] = pd.to_numeric(df["open_li"], errors="coerce") / PRICE_UNIT_DIVISOR
```

**影响**：
- 如果 `open_li` 为 NaN，除以 PRICE_UNIT_DIVISOR 后仍为 NaN
- 如果 PRICE_UNIT_DIVISOR 为 0，会产生 `inf` 或 `-inf`

#### 场景3：HDF5 写入时的类型转换

**代码位置**: `snapshot_writer.py` 第82行

```python
tmp[col] = pd.to_numeric(tmp[col], errors="coerce").astype("float64")
```

**影响**：
- `astype("float64")` 会将所有值转换为 64 位浮点数
- 可能丢失精度或产生舍入误差
- 某些特殊值（如 `inf`、`-inf`）会被保留

## 4. 数据转换失败的常见原因

### 4.1 数据库层面

1. **数据质量问题**
   - NULL 值
   - 空字符串
   - 非法字符
   - 数据类型不匹配

2. **数据同步问题**
   - 数据未及时同步
   - 数据重复或缺失
   - 数据格式不一致

### 4.2 数据转换层面

1. **类型转换失败**
   - 字符串无法转换为数字
   - 数值超出范围
   - 精度丢失

2. **计算错误**
   - 除零错误
   - 溢出错误
   - 舍入误差

### 4.3 数据写入层面

1. **HDF5 格式限制**
   - 不支持某些数据类型
   - 数组维度限制
   - 数据大小限制

2. **编码问题**
   - 字符编码不一致
   - 特殊字符处理错误

## 5. 验证和测试建议

### 5.1 数据质量检查

1. **检查原始数据**
   - 统计 NULL 值数量
   - 检查数据类型分布
   - 验证数据范围

2. **检查转换过程**
   - 记录转换失败的记录
   - 分析失败原因
   - 生成转换报告

### 5.2 转换过程监控

1. **添加日志记录**
   - 记录每个转换步骤
   - 记录转换失败的详细信息
   - 记录转换前后的数据对比

2. **添加异常处理**
   - 捕获转换异常
   - 提供错误恢复机制
   - 生成错误报告

### 5.3 数据验证

1. **验证转换结果**
   - 检查 NaN 值数量
   - 检查 0 值数量
   - 检查数据完整性

2. **对比原始数据**
   - 对比转换前后的数据
   - 验证数据一致性
   - 验证数据准确性

## 6. 结论

### 6.1 数据转换失败的可能性

**结论**：是的，数据转换过程中有可能失败并产生 NaN、空值或 0 值。

**主要原因**：
1. 使用 `pd.to_numeric(..., errors="coerce")` 会将无法转换的值设为 NaN
2. amount 字段缺失时会直接设置为 NaN
3. 数值缩放计算可能产生 NaN 或 0 值
4. HDF5 写入时的类型转换可能产生数据丢失

### 6.2 风险评估

**高风险场景**：
1. 原始数据质量差（NULL 值、空字符串、非数字字符串）
2. 数据类型不匹配
3. 数据同步问题

**中风险场景**：
1. 数值缩放计算
2. HDF5 写入时的类型转换
3. 数据格式不一致

**低风险场景**：
1. 正常数据转换
2. 数据质量良好
3. 数据格式统一

### 6.3 改进建议

1. **数据预处理**
   - 在转换前检查数据质量
   - 清理异常数据
   - 标准化数据格式

2. **转换过程优化**
   - 添加详细的日志记录
   - 实现错误恢复机制
   - 提供数据验证功能

3. **数据质量监控**
   - 定期检查数据质量
   - 生成数据质量报告
   - 及时发现和处理问题

## 7. 附录

### 7.1 关键代码位置

| 文件 | 行号 | 代码 | 说明 |
|------|------|------|------|
| `db_reader.py` | 766-771 | `pd.to_numeric(..., errors="coerce")` | 数值缩放转换 |
| `snapshot_writer.py` | 78-82 | `pd.to_numeric(..., errors="coerce")` | 日线数据标准化 |
| `snapshot_writer.py` | 148-150 | `pd.to_numeric(..., errors="coerce")` | 分钟数据标准化 |

### 7.2 相关文档

- Qlib Snapshot 导出流程文档
- 数据质量检查脚本
- 数据转换日志分析
