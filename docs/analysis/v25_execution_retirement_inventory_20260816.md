# V25 execution-algorithm 退役与历史保留清单

日期：2026-08-16
上位权威：`docs/architecture/simulation_platform_unified_authoritative_blueprint_20260715.md` §0.4、§9 P0-I、`F-122..F-125`
范围：仅指 execution-algorithm 语义中的 `V25_TWO_STAGE`、`V25_1_SMALL_CAP`。

## 1. 当前退役 authority

- 唯一 code-owned authority：`backend/services/trading_core/execution_algo_retirement.py`。
- 稳定 reason/error code：`V25_EXECUTION_ALGO_RETIRED`。
- 活动 registry 和 capability factory 不再注册或构造两个 V25 algorithm。
- QE config/runtime contract、StrategyPackage 新建/晋级/activation、Paper v2 replay/live、Simulation bridge、execution catalog/API/MCP 和启动型 CLI 在模型、day-feature、workspace、数据库写入或 broker side effect 前调用同一 authority。
- 不允许自动改写为 TWAP、CLOSE_PRICE 或其他算法。LocalSIM 的 `localsim_twap_only_v1` 是独立 runtime-mode authority；它对既有 V25 source policy 的只读解析不构成 fallback。

## 2. 保留为历史只读的仓库资产

| 类别 | 保留路径/对象 | 保留用途 | 活动限制 |
| --- | --- | --- | --- |
| 算法 core/adapter | `backend/execution_algos/v25_core.py`、`v25_two_stage_algo.py`、`v25_1_small_cap_algo.py` | 历史 contract、审计、结果解释 | 可直接 import；不得由 `ALGO_REGISTRY/get_algo` 构造 |
| QE 历史模板 | `scripts/tail_twap_v25_strategy.py`、`scripts/tail_twap_v25_1_strategy.py` | 旧 workspace/hash/实验复现解释 | 不再进入活动 QE helper copy/依赖 allowlist |
| 历史 audit CLI | `scripts/qe_v25_existing_artifact_audit.py` | 只读读取旧 artifact | 不调用活动执行 factory，不创建 task/workspace |
| 旧 backtest/validation/catalog CLI | `scripts/add_v25_to_db.py`、`add_v25_to_catalog.py`、`compare_v25_vs_v25_1_1y.py`、`qlib_v25_limit_state_smoke.py`、`v25_1_smoke_backtest.py`、`v24_v25_test.py`、`v24_v25_real_test.py`、`v25_verify_final.py`、`v25_minute_test.py`、`v25_minute_test_final.py`、`v25_mini_backtest.py`、`verify_v25_integration.py`、`verify_v25_minute_execution.py`、`paper_v2_live_validation.py` | 历史源码与审计证据 | 启动入口首先返回 `V25_EXECUTION_ALGO_RETIRED`，不访问 DB/模型/行情/workspace |
| 历史配置/迁移 | `configs/execution_algos/v25_two_stage.yaml`、`v25_1_small_cap.yaml`、`backend/db/migrations/add_v25_execution_algo.sql`、`add_v25_1_small_cap_execution_algo.sql` | 历史 identity、配置和 schema provenance | catalog/API 只读投影 `retired=true/selectable=false/activatable=false` |
| 历史测试 | V25 core、market-state、wrapper/hash contract tests | 证明历史 artifact 仍可解释 | 不作为新 V25 admission 证据 |
| 外部模型/checkpoint/workspace | 既有模型缓存、QE workspace、checkpoint、metrics/report | 历史查询、对账、实验元数据复现 | 本 slice 不加载、改写、复制或删除 |
| 数据库历史事实 | `execution_algorithm_catalog` identity/config、旧 execution policy、run/order/fill/report | 历史查询和审计 | retirement DML 只把两个 catalog row 设为 `is_enabled=false`；不删除或覆盖历史配置 |

## 3. 语义字段边界

退休 matcher 只检查已经由调用方识别出的 execution-algorithm 字段：

- `policy_json.algo_code`；
- `manifest.minute_execution_policy.algo_code`；
- `manifest.backtest_context.execution.execution_algo`；
- QE request/runtime contract 的 `execution_algo`；
- Simulation plan execution-policy identity 中的 explicit `algo_code` 或 legacy colon-separated exact segment。

以下内容即使包含相同文本也不得触发退休：

- `stock_pool`、universe 名称；
- package/report/model/display label；
- 文件名、日志文本、说明文档；
- 历史 artifact 内的 class/algorithm metadata，只要工具保持只读且不创建新 executable work。

## 4. Catalog migration 状态

- 版本化 artifacts：
  - `backend/migrations/v25_execution_algorithm_retirement_20260816.preflight.sql`
  - `backend/migrations/v25_execution_algorithm_retirement_20260816.sql`
  - `backend/migrations/v25_execution_algorithm_retirement_20260816.rollback.sql`
- DEV：已在现有 DEV PostgreSQL 的临时 schema 验证 exact two-row、幂等、TWAP 不受影响、description/default_config 不变。
- Production：未授权、未执行；必须单独批准 production target/DML，执行后再 exact readback。
- Rollback artifact 是安全只读 no-op；重新启用 V25 会与源码 authority 冲突，必须由新的获批设计和 successor migration 处理。

## 5. 后续物理清理边界

本 slice 不删除任何 V25 源码、配置、migration、模型、workspace、checkpoint、report 或数据库历史行。物理删除必须另行完成引用 inventory、retention 期限、准确路径/对象清单和独立授权，不能作为当前禁用生效的前置条件。
