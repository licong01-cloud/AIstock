# Qlib 日频前复权导出 - 实施进度

> 对应设计文档：`docs/qlib_daily_qfq_design.md`

---

## 阶段 1：架构落地与骨架搭建 ✅ 已完成

| 任务 | 状态 | 完成日期 | 备注 |
|------|------|----------|------|
| 创建 `backend/qlib_exporter/` 目录与基础文件 | ✅ | 2025-12-04 | `__init__.py`, `config.py`, `db_reader.py`, `snapshot_writer.py`, `exporter.py`, `router.py` |
| `config.py` 配置 | ✅ | 2025-12-04 | `QLIB_SNAPSHOT_ROOT`, `QLIB_MARKET`, `DAILY_QFQ_TABLE`, `FIELD_MAPPING_DB_DAILY`, `FIELD_MAPPING_DB_MINUTE` |
| `DBReader.get_all_ts_codes()` | ✅ | 2025-12-04 | 从日线表查询所有 ts_code |
| `DBReader.load_daily()` | ✅ | 2025-12-04 | 加载日频前复权数据，返回 MultiIndex DataFrame |
| `DBReader.load_minute()` | ✅ | 2025-12-05 | 加载分钟线数据，修复了 SQL 列名和 dtype 问题 |
| `DBReader.load_board_daily()` | ✅ | 2025-12-04 | 加载板块日线数据 |
| `SnapshotWriter.write_daily_full()` | ✅ | 2025-12-04 | 写入 `daily_pv.h5` + `meta.json` + `instruments/` + `calendars/` |
| `SnapshotWriter.write_minute_full()` | ✅ | 2025-12-05 | 写入 `minute_1min.h5`，修复了 MultiIndex 扩展 dtype 问题 |
| `SnapshotWriter.write_board_daily_full()` | ✅ | 2025-12-04 | 写入 `boards/board_daily.h5` |
| `QlibDailyExporter.export_full()` | ✅ | 2025-12-04 | 日频全量导出 |
| `QlibMinuteExporter.export_full()` | ✅ | 2025-12-05 | 分钟线全量导出 |
| `QlibBoardExporter.export_full()` | ✅ | 2025-12-04 | 板块日线全量导出 |
| 集成测试脚本 | ✅ | 2025-12-05 | `tests/test_qlib_export_and_inspect.py` 验证通过 |

### 阶段 1 测试结果（2025-12-05）

```
=== Step 1: export daily ===
[HTTP] status: 200, rows: 2

=== Step 2: export minute (1m) ===
[HTTP] status: 200, rows: 480

=== Step 3: export board daily ===
[HTTP] status: 200, rows: 613

=== Step 4: inspect generated snapshot files ===
- daily_pv.h5: shape (2, 6), index ['datetime', 'instrument'] ✅
- minute_1min.h5: shape (480, 6), index ['datetime', 'instrument'] ✅
- boards/board_daily.h5: shape (613, 7), index ['datetime', 'board'] ✅
```

### 阶段 1 问题修复记录

1. **分钟线 SQL 列名错误**（2025-12-05）
   - 问题：`load_minute` 查询 `open, high, ...` 但实际列名是 `open_li, high_li, ...`
   - 修复：更新 `FIELD_MAPPING_DB_MINUTE` 和 SQL 查询

2. **HDF5 MultiIndex 扩展 dtype 错误**（2025-12-05）
   - 问题：Pandas `to_hdf` 不支持带 `StringDtype` 的 MultiIndex
   - 修复：在 `write_minute_full` 中强制转换为 `object` dtype，使用 `format="fixed"`

---

## 阶段 2：后端 API + 前端 UI（日频全量）✅ 已完成

| 任务 | 状态 | 完成日期 | 备注 |
|------|------|----------|------|
| FastAPI 路由 `router.py` | ✅ | 2025-12-04 | 已挂载到 `/api/v1/qlib` |
| `POST /api/v1/qlib/snapshots/daily` | ✅ | 2025-12-04 | 日频全量导出 API |
| `POST /api/v1/qlib/snapshots/minute` | ✅ | 2025-12-05 | 分钟线全量导出 API |
| `POST /api/v1/qlib/boards/daily` | ✅ | 2025-12-04 | 板块日线导出 API |
| `GET /api/v1/qlib/snapshots` | ✅ | 2025-12-05 | 罗列现有 Snapshot |
| `GET /api/v1/qlib/config` | ✅ | 2025-12-05 | 返回当前配置 |
| `DELETE /api/v1/qlib/snapshots/{id}` | ✅ | 2025-12-05 | 删除指定 Snapshot |
| 前端 `/qlib` 页面 V1 | ✅ | 2025-12-05 | 导出表单 + Snapshot 列表 + 删除功能 |

---

## 阶段 3：增量导出 + 元数据表 ⏳ 待开始

| 任务 | 状态 | 完成日期 | 备注 |
|------|------|----------|------|
| 创建 `market.qlib_export_meta` 表 | ⏳ | - | |
| 实现 `MetaRepo` | ⏳ | - | |
| `QlibDailyExporter.export_incremental()` | ⏳ | - | |
| `QlibMinuteExporter.export_incremental()` | ⏳ | - | |
| API 支持 `mode=incremental` | ⏳ | - | |
| 前端增量导出选项 | ⏳ | - | |

---

## 阶段 4：多因子接入 ⏳ 待开始

| 任务 | 状态 | 完成日期 | 备注 |
|------|------|----------|------|
| 确定日频因子表结构 | ⏳ | - | |
| `DBReader.load_daily()` join 因子表 | ⏳ | - | |
| 更新 `SnapshotWriter` 支持因子列 | ⏳ | - | |
| 前端显示因子字段 | ⏳ | - | |

---

## 变更记录

| 日期 | 变更内容 |
|------|----------|
| 2025-12-05 | 创建进度文档，记录阶段 1 完成情况 |
| 2025-12-05 | 分钟线导出功能完成，修复 dtype 兼容性问题 |
