# RD-Agent × AIstock Phase2（HTTP results-api 同步）开发进度（2025-12-29）

## 背景与标准

- 顶层入口：`RD-Agent-main/docs/2025-12-26_RD-Agent_AIstock_TopLevel_Architecture_Design_v2.md`
- Phase2 最终版：`RD-Agent-main/docs/2025-12-29_Phase2_Detail_Design_RD-Agent_AIstock_Final.md`
- 结论：Phase2 同步的标准方式为 **AIstock 通过 HTTP 调用 RD-Agent 只读成果 API（rdagent-results-api）完成全量 + 增量同步**。

## P0（Phase2 收口必须完成）

### P0-1 results-api HTTP Client
- 状态：TODO
- 目标：支持 `RDAGENT_RESULTS_API_BASE_URL`（例如 `http://127.0.0.1:9000`）
- 必须能力：
  - 超时/错误处理（快速失败，不阻塞主线程）
  - 基础 endpoints：
    - `GET /catalog/factors`
    - `GET /catalog/strategies`
    - `GET /catalog/loops`
    - `GET /catalog/models`
    - （后续）`GET /alpha158/meta`
    - （后续）`GET /loops/{task_run_id}/{loop_id}/artifacts`

### P0-2 Catalog 同步服务（全量 + 增量）
- 状态：TODO
- 目标：将 results-api 拉取到的数据 upsert 到本地 PG：
  - `aistock_factor_catalog`
  - `aistock_strategy_catalog`
  - `aistock_loop_catalog`
  - `aistock_model_catalog`（需新增）
- 增量策略（优先实现）：
  - 以 `generated_at_utc` 或 server-side 的 `updated_since`（若 API 支持）为边界
  - 若 API 暂不支持：先全量同步，后续补增量

### P0-3 同步触发与状态 API（AIstock 后端）
- 状态：TODO
- 目标：提供管理接口：
  - 手动触发全量/增量同步
  - 查询最近一次同步状态/耗时/错误

### P0-4 Loop artifacts 详情闭环
- 状态：TODO
- 目标：AIstock UI 能查看 loop 的：
  - `factor_meta.json`
  - `factor_perf.json`
  - `feedback.json`
  - `ret_curve.png` / `dd_curve.png`

### P0-5 Model Catalog 全链路
- 状态：TODO
- 目标：补齐 Phase2 四大 Catalog 的第 4 类（model）：
  - DB 表/索引/初始化脚本
  - 同步/查询 API
  - UI 列表页（最小可用）

## P1（DataServiceLayer 对齐）

- 离线文件视图导出：h5/parquet/qlib_bin/calendars/instruments
- 统一 source 选择策略固化（xtquant/tdx/timescaledb/tushare）

## P2（Phase3 准备项）

- FactorEngine/StrategyEngine 接口落点与对齐测试工具
- Strategy Preview（preview_* 表 + 定时刷新）
