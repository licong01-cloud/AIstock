# HMM 每日系数生成设计方案（Paper v2 / Selection Center）

日期：2026-04-28
状态：Implemented in current development branch; async job update added 2026-04-28

## 1. 背景与问题

当前 HMM 运行时只消费两类已存在产物：

1. `model_train_snapshots.model_path` 指向的 HMM 模型快照；
2. 与 `models.json` 同目录下的 `coefficients_<preset>_<start>_<end>.json` 系数文件。

这对历史回测窗口可用，但不能支撑真实模拟盘/实盘式选股：交易日 D 不能使用 D 之后数据，也不能预知 D 收盘后的 HMM 状态。正确做法是：在 D 开盘前，使用最近已完成交易日 D-1（或更早的最新完整数据日）的行业/指数/资金流数据，前向滤波得到 as-of 日状态，再把该状态生成的行业系数写成对 D 生效的系数产物。

因此缺口不是“每天重新训练 HMM”，而是“每天基于最新已收盘数据生成下一交易日 HMM 系数”。

## 2. 目标

- 支持对某个完成的 `model_snapshot_id` 和 `signal_preset` 生成每日系数产物。
- 默认使用最新公共完整数据日作为 `as_of_trade_date`。
- 默认使用 `as_of_trade_date` 后的下一个交易日作为 `effective_trade_date`。
- 产物写入 HMM 快照同目录，供 Selection Center 与 Paper Trading v2 继续通过现有 HMM runtime 读取。
- 不修改已有模型权重、训练快照、历史系数产物或 StrategyPackage manifest。
- 不做中性系数、空系数、默认路径、日频 fallback 等静默兜底。

## 3. 核心概念

| 字段 | 含义 |
| --- | --- |
| `model_snapshot_id` | 已完成 HMM 模型快照，必须存在且状态完成。 |
| `signal_preset` | HMM 系数预设，例如 `preset_A` / `preset_B`。 |
| `as_of_trade_date` | 生成系数时可用的最新已完成交易日。 |
| `effective_trade_date` | 系数实际用于选股/模拟盘的交易日，必须晚于 `as_of_trade_date`。 |
| `generation_mode` | 固定为 `daily_asof_prediction_v1`。 |
| `input_data_max_dates` | 生成时检查到的 HMM 所需数据集最大日期。 |
| `artifact_sha256` | 生成文件的 SHA256，用于追踪与审计。 |

## 4. 数据与 PIT 规则

1. 对交易日 D 的 HMM 系数只能来自 D 之前已经完成的数据。
2. `effective_trade_date <= as_of_trade_date` 一律失败。
3. `as_of_trade_date` 与 `effective_trade_date` 必须都是交易日。
4. HMM 所需数据源（当前为 `market.sector_data`、`market.sw_daily`、`market.index_daily/000300.SH`）必须都覆盖 `as_of_trade_date`。
5. 生成脚本只把 `as_of_trade_date` 的前向滤波状态映射到 `effective_trade_date`，不会把未来日期数据读入推理。
6. 生成的每日系数是新增产物；如目标文件已存在，只有元数据完全匹配时才返回 `EXISTS`，否则拒绝覆盖。

## 5. API 设计

新增路由：

```text
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/preview
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/generate
POST /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/jobs
GET  /api/v1/hmm-training/daily-coefficients/jobs/{job_id}
GET  /api/v1/hmm-training/snapshots/{snapshot_id}/daily-coefficients/jobs
```

请求体：

```json
{
  "signal_preset": "preset_A",
  "as_of_date": "2026-04-27",
  "effective_trade_date": "2026-04-28",
  "confirm_text": "<snapshot_id>"
}
```

说明：

- `as_of_date` 可省略，后端自动选择最新公共完整数据日。
- `effective_trade_date` 可省略，后端自动选择 as-of 后的下一个交易日。
- `generate` 必须提供 `confirm_text == snapshot_id`。
- `/jobs` 是 UI 和长耗时生成的权威入口：请求只做参数、PIT 数据与目标产物校验，随后创建持久化任务并立即返回 `job_id`；WSL 生成在 FastAPI background task 中执行，UI 轮询任务状态。
- 保留同步 `/generate` 仅用于直接 API/脚本诊断，不作为前端长耗时交互路径。

## 5.1 异步任务与代理超时控制

Next.js dev rewrite proxy 对长时间 HTTP 连接不应作为业务可靠性边界。每日系数生成可能触发 WSL/conda/Python 推理，耗时从数秒到数分钟不等，因此 UI 不再等待同步 `/generate` 请求完成。

新增持久化表：

```text
model_train_daily_coefficient_jobs
```

关键字段：

| 字段 | 含义 |
| --- | --- |
| `job_id` | 每次人工生成请求的审计 ID。 |
| `snapshot_id` / `config_id` | HMM 快照与配置来源。 |
| `signal_preset` | 本次生成使用的 HMM 信号预设。 |
| `as_of_trade_date` / `effective_trade_date` | PIT 数据截至日与生效交易日。 |
| `status` | `PENDING` / `RUNNING` / `COMPLETED` / `FAILED`。 |
| `result_status` | 产物结果：`CREATED` 或 `EXISTS`。 |
| `plan_json` | 创建任务时冻结的校验计划。 |
| `result_json` | 完成后记录的产物结果。 |
| `error_message` / `error_context` | 失败时的明确错误与堆栈尾部。 |

失败语义：

- WSL 命令失败、超时、目标文件未生成、已有文件元数据不匹配、缺数据、非交易日等全部进入 `FAILED`，并在 UI 原样展示。
- 不允许把失败任务转成空系数、默认系数或成功状态。
- 不覆盖已有不匹配产物；元数据一致的已有产物只记录为 `EXISTS`。

## 6. 产物格式

每日产物沿用现有 runtime 兼容格式：

```text
coefficients_<preset>_<effective_trade_date>_<effective_trade_date>.json
```

文件内容保留现有字段：

```json
{
  "model_path": ".../models.json",
  "preset_key": "preset_A",
  "preset_coeffs": {"trending": 1.05, "neutral": 1.0, "fading": 0.96},
  "daily_coefficients": {"2026-04-28": {"801010.SI": 1.05}},
  "stock_sector_map": {"600000.SH": "801780.SI"}
}
```

并新增审计字段：

```json
{
  "generation_mode": "daily_asof_prediction_v1",
  "as_of_trade_date": "2026-04-27",
  "effective_trade_date": "2026-04-28",
  "generated_at": "2026-04-28T...Z",
  "snapshot_id": "...",
  "config_id": "...",
  "input_data_max_dates": {"sector_data": "2026-04-27"}
}
```

## 7. UI 设计

在 `/paper-v2/model-hmm` 增加“HMM 每日系数生成”卡片：

- 选择已完成 HMM 快照；
- 选择信号预设；
- 可选填写数据截至日；
- 可选填写生效交易日；
- 先预览再确认生成；
- 生成后刷新快照产物列表；
- 所有错误直接展示后端 fail-fast 详情。

## 8. 不做事项

- 不自动重训 HMM。
- 不修改 HMM 模型权重或已有训练快照。
- 不把缺失系数替换为 1.0 中性系数。
- 不绕过 Selection Center/Paper v2 现有 HMM runtime。
- 不从 Windows 侧拼接或访问 WSL UNC 路径；WSL 子进程通过 `/mnt/<drive>` 写回 Windows 工作区。

## 9. 验证要求

- 后端单元测试覆盖日期规划、preset 解析、已存在产物幂等、生成命令参数。
- `scripts/precompute_hmm_coefficients.py` 保持历史批量预计算兼容，并新增每日输出日期映射测试。
- 前端类型检查通过。
- 使用 8012/3012 开发端口执行 Paper v2 UI E2E，至少覆盖模型/HMM 页面每日系数预览与生成控件可用性。
