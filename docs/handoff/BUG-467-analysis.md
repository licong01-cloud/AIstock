# BUG-467 LocalSim day_features turnover_rate_f 根因分析

## 结论

BUG-467 与 GitHub issue #1432 的描述一致：`DbV25DayFeatureProvider._free_float_turnover_rate()` 在
`market.daily_basic.turnover_rate_f` 为 `NULL`、`NaN`、`Decimal("NaN")` 或非有限值时，调用
`_substitute_free_float_turnover_rate()`，把同一行 `turnover_rate` 除以 100 后写入
`free_float_turnover_rate`。这会让 V25 day feature 第 5 列 `turnover_rate` 与第 6 列
`free_float_turnover_rate` 在缺失场景下相同，属于用总股本换手率伪造自由流通换手率。

## 当前代码证据

- `backend/services/paper_trading_v2/day_features.py`：`load_day_features()` 同时读取 `turnover_rate` 与
  `turnover_rate_f`，随后调用 `_free_float_turnover_rate()` 生成第 6 个特征。
- `_free_float_turnover_rate()`：当源值缺失或非有限时没有抛出 `DataUnavailableError`，而是进入
  `_substitute_free_float_turnover_rate()`。
- `_substitute_free_float_turnover_rate()`：仅向 audit 追加 `field_repair/substituted`，最终返回
  `turnover_rate_raw / 100.0`。这个 audit 不会阻止模型消费该向量，也不是 loud/operator-surfaced degradation。
- `backend/tests/paper_trading_v2/test_v25_day_features.py` 旧用例显式期望缺失/NaN 时第 5、6 列都等于
  `0.05`，证明现有回归保护了错误行为。

## 根因

缺失 `turnover_rate_f` 的处理策略把“保持向量有限”放在“特征语义真实性”之前，且没有真实自由流通股本输入可用于重算；
在当前写入范围内不能引入新的自由流通股本数据源。因此最小正确修复是 fail-closed：源字段缺失、NaN 或非有限时直接抛出
带 `reason_code` 的 `DataUnavailableError`，由 LocalSim/market data 调用链按已有 fail-fast/排除逻辑处理该标的当日。

## 修复方案

1. 删除 `turnover_rate` 替代 `turnover_rate_f` 的路径；不得再生成两列相同的伪造特征。
2. `turnover_rate_f` 缺失、NaN、非有限或非法格式时，抛出具体 `DataUnavailableError`：包含调用点、表、字段、symbol、
   trade_date、reason_code、source_value、fail_closed 策略说明。
3. 在抛错前写 error log，日志中包含 `reason_code`、symbol、trade_date、source_value 与 fail-closed policy，满足
   no-silent-error 审计要求。
4. 保留正常真实 `turnover_rate_f` 路径：例如 `6.0` 继续输出 `0.06`，不影响已有真实自由流通换手特征。
5. 仅修改 LocalSim day feature provider 与对应单测；不触碰 MiniQMT 专属路径。

## 与 issue 的分歧

无实质分歧。Issue 允许两类方案：fail-closed 排除该标的当日，或使用真实自由流通股本重算。本 lane 选择 fail-closed，
因为当前 scope 中没有可审计的自由流通股本来源，使用 `turnover_rate` 替代会继续违反 BUG-399/408/409 lineage 的
no-silent-fallback 约束。

## 生产门禁

- production_ddl_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
- 不需要、也未授权启动/重启/停止任何服务；合并后如运行中服务需使用新代码，需要用户自行重启。
