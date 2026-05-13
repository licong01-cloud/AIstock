# 因子缓存 ST PIT Universe 强一致设计方案

日期：2026-05-13
分支：`codex/paper-v2-qe-candidate-platform-20260512`

## 背景

Paper v2 / QE 已将 `shsz_st_pit_active_v1` 作为 ST PIT 买入资格股票池的权威平台能力。因子缓存、QE 回测、独立指标、Selection/Paper/未来实盘必须使用同一套 PIT 股票池口径，否则会出现以下问题：

- 旧缓存使用 legacy/static universe，但被新 QE/Paper 流程复用。
- 因子独立指标 denominator 与回测/模拟盘实际 universe 不一致。
- PIT 源数据更新后，缓存仍命中过期股票池。
- 策略包误把 ST PIT 数据范围当作自身限制，而不是平台能力。

## 设计边界

- ST PIT 股票池数据只保存在平台权威源：`market.stock_universe_pit_spans` 和 `market.stock_universe_pit_state`。
- 因子缓存不重复保存完整 PIT spans、逐日 mask 或股票池大数据。
- 因子缓存只保存并强校验最小审计元数据：
  - `universe_key`
  - `universe_rule_version`
  - `universe_fingerprint_sha256`
  - `index_policy`
  - `coverage_semantics`
  - 缓存窗口与因子代码 hash
- ST PIT 默认 universe 是平台默认行为，不进入策略包的锁定资产范围。
- legacy/all-stock/ST-including universe 仅允许显式 research-only 使用，不得默认进入模拟盘或未来实盘。

## 一致性规则

### 1. 缓存写入

所有官方因子缓存写入必须同时写入 universe 元数据。缺失元数据的缓存视为旧缓存。

### 2. 缓存命中

缓存命中必须同时满足：

1. 因子代码 hash 匹配。
2. 请求窗口被覆盖，允许已记录的 warm-up 缺口。
3. `universe_key` 匹配。
4. `index_policy` 匹配。
5. `universe_fingerprint_sha256` 匹配。

任一项不满足则不得复用缓存，必须重算或增量补算。缺少 universe 指纹时，官方 QE 生成脚本默认禁用缓存命中，避免静默复用旧缓存。

### 3. 独立指标

`qe_eval_v2` 独立指标必须使用 ST PIT eligible mask 作为 coverage denominator 的组成部分：

`eligible_mask & market_valid & ~suspended & non_warmup`

指标入库必须写入 universe 元数据和 ST PIT 排除样本计数。

### 4. schema 初始化

现有迁移 `backend/migrations/factor_metrics_st_pit_universe_metadata_20260506.sql` 已为现有库补充元数据列。新库初始化 DDL 也必须直接包含这些列，避免 DEV/CI fresh DB 漏列。

## 当前实现任务

### P0：backfill 缓存跳过逻辑

`scripts/backfill_factor_cache.py` 在 `plan_factor_action()` 中必须把当前 ST PIT universe 元数据传入 `factor_cache_covers_window()`。如果旧缓存 universe 不匹配或缺失，不能 `skip` 或 `extend_forward`，必须 `full_rebuild`。

### P0：QE 生成 prepare_factors.py

`ConfigComposer._compose_prepare_factors()` 生成的脚本必须：

- 嵌入生成时的官方 ST PIT universe 元数据。
- 缓存命中前校验 universe key / fingerprint / index policy。
- 缓存写入时把 universe 元数据写到单因子 entry 和 `_meta.json` 顶层。
- 如果无法取得 universe fingerprint，则默认禁用缓存命中，防止旧缓存被误用。

### P1：fresh DB 初始化

将 ST PIT 元数据列补进：

- `backend/init_catalog_db.py`
- `backend/db/init_quant_schema.py`

迁移仍保留，用于已有数据库。

## 验证

最小验证集：

```powershell
C:\Users\lc999\miniconda3\envs\aistock\python.exe -m pytest `
  backend/tests/test_factor_st_pit_metrics_cache.py `
  backend/tests/test_factor_cache_wsl_env.py `
  backend/tests/unified_engine/test_multi_alpha_command_generation.py `
  -q -p no:cacheprovider
```

通过标准：

- PIT mask / coverage denominator / universe metadata 单测通过。
- backfill 计划在 universe mismatch 时触发 full rebuild。
- `prepare_factors.py` 生成脚本可编译，并包含 universe 校验与写入逻辑。
- fresh DB DDL 直接包含 ST PIT metadata columns。

