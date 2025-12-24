# Qlib Snapshot 字段说明增强（HDF5 attrs + CSV）更新说明

更新时间：2025-12-13

## 背景与目标

为 Qlib Snapshot 导出增加“字段中文含义说明”，实现：

- 从 PostgreSQL 表字段的 `COMMENT ON COLUMN` 自动读取中文说明
- 按 Snapshot ID 生成 `aistock_field_map.csv`（字段名为 **HDF5 文件中的列名**，而不是数据库字段名）
- 将“列名 → 中文含义”写入对应 Snapshot 的 `daily_basic.h5` / `moneyflow.h5` 的 HDF5 attrs（不改变数据本身，仅追加元信息）
- 在前端“现有 Snapshot”列表中提供一键生成 CSV、导出 zip 等操作

## 主要功能变更

### 1. daily_basic 导出支持（补齐 DBReader）

- **文件**：`backend/qlib_exporter/db_reader.py`
- **变更**：新增 `DBReader.load_daily_basic_panel(...)`
- **作用**：从 `market.daily_basic` 读取 daily_basic 指标数据并转换为面板格式：
  - Index: `(datetime, instrument)`
  - Columns: `db_*`（float32）

### 2. 字段说明生成与写入（核心能力）

#### 2.1 新增字段映射模块

- **文件**：`backend/qlib_exporter/field_map.py`
- **能力**：
  - 从 `pg_catalog.pg_description` 读取列 comment
  - daily_basic：在 DB comment 缺失时提供 fallback 中文含义（用于兼容老库）
  - moneyflow：识别导出时单位换算（手→股、万元→元），并在 `meaning_cn` 中输出换算后的单位
  - 输出 `FieldMapRow(name, meaning_cn, unit, source_table, comment, dtype_hint)`
  - 写出 CSV：`write_field_map_csv(...)`
  - 写入 HDF5 attrs：
    - `storer.attrs.column_comments`
    - `storer.attrs.column_comments_json`

#### 2.2 新增字段映射服务

- **文件**：`backend/qlib_exporter/field_map_service.py`
- **接口**：`export_field_map_for_snapshot(snapshot_id, write_to_h5=True)`
- **关键行为**：
  - 读取指定 snapshot 的 `daily_basic.h5` / `moneyflow.h5` 的实际列名
  - 生成字段说明 CSV（默认输出到 snapshot 目录内）
  - 可选写入 HDF5 attrs（修改的是元信息，不改变数据表结构）

#### 2.3 CSV 输出位置调整（按 Snapshot ID 输出）

- **旧默认输出**：`AIstock/metadata/aistock_field_map.csv`
- **新默认输出**：`qlib_snapshots/<snapshot_id>/metadata/aistock_field_map.csv`

这样可以针对“已有 Snapshot ID”生成对应字段说明，不依赖导出表单。

### 3. 后端 API 变更

- **文件**：`backend/qlib_exporter/router.py`

#### 3.1 新增字段说明导出 API

- `POST /api/v1/qlib/field_map/export`
- Body:
  - `snapshot_id`: string
  - `write_to_h5`: bool（默认 true）
- 返回：
  - `csv_path`, `rows`, `written_h5` 等信息

#### 3.2 新增 Snapshot 导出 ZIP API

- `GET /api/v1/qlib/snapshots/{snapshot_id}/export`
- 作用：将指定 snapshot 目录打包为 zip 下载
- 安全性：对路径进行 resolve 校验，避免 path traversal

### 4. Snapshot 根目录路径修复（重启后列表为空问题）

- **文件**：`backend/qlib_exporter/config.py`
- **问题**：旧实现使用相对路径 `Path("qlib_snapshots")`，后端重启工作目录变化会导致 Snapshot 列表为空。
- **修复**：改为稳定绝对路径：
  - 默认：`<项目根>/qlib_snapshots`
  - 支持环境变量覆盖：`QLIB_SNAPSHOT_ROOT`

### 5. 前端 UI 变更（/qlib 页面）

- **文件**：`frontend/src/app/qlib/page.tsx`

新增/调整：

- 在“现有 Snapshot”列表每行新增按钮：
  - **生成CSV**：对该行 snapshot_id 调用 `POST /api/v1/qlib/field_map/export`
  - **导出**：下载 zip（`GET /api/v1/qlib/snapshots/{id}/export`）
- `SnapshotInfo` 类型补齐 `has_daily_basic` 字段，以匹配后端返回
- 修复 `buildBackendUrl` 定义位置

### 6. 脚本与运维辅助

#### 6.1 新增：检查列 comment 是否存在

- **脚本**：`scripts/check_pg_column_comments.py`
- 用途：检查 `market.daily_basic` / `market.moneyflow_ts` 是否存在真实 `COMMENT ON COLUMN`

示例：

```powershell
python scripts\check_pg_column_comments.py --schema market --tables daily_basic,moneyflow_ts
```

#### 6.2 更新：daily_basic 表补充 COMMENT ON COLUMN

- **脚本**：`scripts/create_daily_basic_table.py`
- 变更：在建表后追加 `COMMENT ON TABLE` / `COMMENT ON COLUMN`（来源为脚本内原有行内注释的中文含义）

说明：
- `CREATE TABLE IF NOT EXISTS` 不会删除/修改现有数据
- `COMMENT ON COLUMN` 只写元信息，不影响行数据

## 使用流程

### A. 先写入 daily_basic 的真实字段 comment（建议）

```powershell
python scripts\create_daily_basic_table.py
```

然后验证：

```powershell
python scripts\check_pg_column_comments.py --schema market --tables daily_basic,moneyflow_ts
```

预期：两个表 `missing_comment` 都为 0。

### B. 在 UI 中对已有 Snapshot 生成 CSV + 写 attrs

- 打开 `/qlib`
- 在“现有 Snapshot”表格中找到目标 snapshot
- 点击 **生成CSV**

生成文件：

- `qlib_snapshots/<snapshot_id>/metadata/aistock_field_map.csv`

并会写入 attrs：

- `qlib_snapshots/<snapshot_id>/daily_basic.h5`
- `qlib_snapshots/<snapshot_id>/moneyflow.h5`

### C. 导出 Snapshot ZIP

- 在“现有 Snapshot”表格中点击 **导出**
- 下载 `<snapshot_id>.zip`

## 注意事项

- 写入 HDF5 attrs 会修改 `.h5` 文件（仅元信息），不会改变 DataFrame 数据/列/索引。
- Qlib 正常读取 `.h5` 时通常不会关心 attrs，因此不会影响 Qlib 访问。
- 若担心并发文件锁，建议在写 attrs 时避免同时有训练/回测任务读取同一个 snapshot。

## 已知限制

- 若未来 `daily_basic.h5` / `moneyflow.h5` 新增列，需要同步更新 `field_map.py` 中的映射表，否则 CSV 可能不会覆盖新增字段。

## 文件清单（本次相关变更）

- `backend/qlib_exporter/db_reader.py`
- `backend/qlib_exporter/field_map.py`
- `backend/qlib_exporter/field_map_service.py`
- `backend/qlib_exporter/router.py`
- `backend/qlib_exporter/config.py`
- `frontend/src/app/qlib/page.tsx`
- `scripts/create_daily_basic_table.py`
- `scripts/check_pg_column_comments.py`
