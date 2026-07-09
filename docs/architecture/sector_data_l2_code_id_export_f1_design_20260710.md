# sector_data L2 Code Id Export F1 Design

## Background

现有因子执行侧不直接读取数据库 H5 基表，而是由静态因子 bundle 路径把 `sector_data.h5` 等数据合并进 `static_factors.parquet`，因子再按列名显式取列。`sector_data.h5` 目前只有 22 个连续数值 `sw2_*` 字段，缺少离散申万 L2 行业码，导致板块间 RS 排名、板块广度等轮动因子无法按板块 group/rank。

`market.sw_index_member` 已保存个股到申万 L1/L2/L3 的 PIT 归属，包含 `in_date/out_date` 和历史行业迁移。本功能只在导出时补 `l2_code_id`，不修改、不重建 `market.sector_data` 基表。

## Scope

- 在 `backend/qlib_exporter/db_reader.py::load_sector_data_panel` 导出的每行 `(datetime, instrument)` 增加 `l2_code_id`。
- `l2_code_id` 是申万 L2 `index_code` 的稳定整数编码，未知或未匹配为 `-1`。
- 映射唯一真源是 `backend.services.industry_code_map.load_sw_l2_code_map`：从 `market.sw_index_classify` 的 L2 节点按 `index_code ASC` 生成，回测导出与未来线上运行时复用同一 helper。
- `market.sector_data` 结构不变，不新增 DDL，不写生产 DB。
- 现有 22 个 `sw2_*` 字段行数、列名、逐值语义不变。

## Non-Goals

- 不重建 `market.sector_data` 基表。
- 不在本 PR 接入模拟盘、选股或 Paper v2 运行时消费 `l2_code_id`。
- 不修改 RD-Agent 仓库的 `tools/generate_static_factors_bundle.py`。
- 不引入字符串行业列，避免 bundle float32 合并路径崩溃或产生静默 NaN。

## Design Acceptance Index

| item | requirement |
| --- | --- |
| F-001 | L2 映射由唯一 helper 从 `market.sw_index_classify` L2 `index_code ASC` 生成，禁止 `pd.factorize` 或数据行顺序依赖。 |
| F-002 | sector_data 导出 SQL 对 `market.sw_index_member` 做 PIT `LEFT JOIN LATERAL`，只取当日有效 L2，禁止未来行业归属泄露。 |
| F-003 | 导出 DataFrame 在保留 22 个 `sw2_*` float32 数值列不变的基础上追加整数 `l2_code_id`，未知为 `-1`。 |
| F-004 | 每日 `l2_code_id != -1` 覆盖率低于 90% 时 loud warning，包含稳定 `reason_code`、缺失数和总数。 |
| F-005 | field map 能暴露 `l2_code_id`，并声明其整数编码语义和唯一真源。 |
| F-006 | 验证覆盖 bundle float32 安全、PIT 边界、映射稳定、缺失优雅、`sw2_*` 回归和 feature workflow gate。 |

## Implementation Plan

- 新增 `backend/services/industry_code_map.py`，提供 `build_sw_l2_code_map`、`load_sw_l2_code_map` 和 `encode_l2_codes`，作为 train/serve parity 的单一代码真源。
- 修改 `load_sector_data_panel`：
  - SQL 增加 `LEFT JOIN LATERAL`，按 `m.in_date <= sd.trade_date AND (m.out_date IS NULL OR m.out_date >= sd.trade_date)` 取当日有效 `l2_code`。
  - 使用唯一 helper 把 `l2_code` 编码为 `l2_code_id`。
  - 只对 `sw2_cols` 执行 `astype("float32")`，再追加 `np.int16` 的 `l2_code_id`。
  - 按交易日检查覆盖率，低于阈值 warning。
- 修改 `backend/qlib_exporter/field_map.py`，把 `l2_code_id` 纳入 sector_data field map。

## Verification Plan

- 单元测试模拟真实 DBReader 导出路径，断言 SQL 含 PIT lateral join，行业迁移日前后取不同编码，未匹配为 `-1`。
- 单元测试验证 `build_sw_l2_code_map` 对输入顺序不敏感，禁止行顺序依赖。
- 单元测试验证低覆盖率输出 loud warning。
- 单元测试验证含 `l2_code_id` 的 DataFrame 走整体 `astype(np.float32)` 不崩，`0..130/-1` 精确保持整数语义，且现有 `sw2_*` 值不变。
- 单元测试验证 field map 包含 `l2_code_id`。
- 本地 gate：目标 pytest、`py_compile`、`git diff --check`、`scripts/aistock_feature_workflow.py validate --tier F1`。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `backend/services/industry_code_map.py` | `test_build_sw_l2_code_map_is_order_stable` | pass | n/a |
| F-002 | `backend/qlib_exporter/db_reader.py` | `test_load_sector_data_panel_adds_pit_l2_code_id_and_preserves_sw2_values` | pass | n/a |
| F-003 | `backend/qlib_exporter/db_reader.py` | `test_load_sector_data_panel_adds_pit_l2_code_id_and_preserves_sw2_values`, `test_static_bundle_float32_cast_keeps_l2_code_id_integer_semantics` | pass | n/a |
| F-004 | `backend/qlib_exporter/db_reader.py` | `test_load_sector_data_panel_warns_when_l2_code_coverage_is_low` | pass | n/a |
| F-005 | `backend/qlib_exporter/field_map.py` | `test_sector_field_map_includes_l2_code_id` | pass | n/a |
| F-006 | tests and feature workflow command | Targeted pytest, py_compile, diff check, feature validate | pass | n/a |

## Risks

- 如果 `market.sw_index_classify` 缺少 L2 master 节点，所有行会编码为 `-1` 并触发低覆盖率 warning；不会丢行或静默成功。
- 如果未来申万 L2 master 版本发生结构性变化，需要先明确编码版本策略；本 PR 的唯一真源 helper 是未来加版本表或常量表的接线点。
- 生产 H5/parquet 重导不在本 PR 中执行，需合并后由用户按生产数据流程触发。

## Production Gates

- `production_ddl_gate`: noop
- `production_frontend_dependency_gate`: noop
- `production_backend_dependency_gate`: noop
- Runtime/DB touch: 不重启生产服务；不写生产 DB；不应用 DDL；不在项目目录生成 H5/parquet。
