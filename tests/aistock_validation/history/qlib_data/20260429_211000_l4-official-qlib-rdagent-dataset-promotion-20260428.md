# Qlib/RD-Agent 正式数据集覆盖与默认日期更新验证

- 时间：2026-04-29 21:10 Asia/Shanghai
- 范围：WSL Qlib day/minute 正式数据集、WSL static parquet、RD-Agent prod/debug factor data、QE 默认 data_split、RD-Agent 主仓与 app_tpl 各版本配置日期

## 覆盖/备份

- `/home/lc999/data/qlib_bin` 已由 `/home/lc999/data/qlib_bin_20260428_shsz_candidate` 覆盖；备份：`/home/lc999/data/qlib_bin_backup_20260429_204145`
- `/home/lc999/data/aistock_factors/combined_static_factors.parquet` 已由 2026-04-28 candidate static_factors 覆盖；备份：`/home/lc999/data/aistock_factors/combined_static_factors.parquet.backup_20260429_204145`
- `F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data` 已覆盖；备份：`F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data_backup_20260429_204226`
- `F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data_debug` 已覆盖；备份：`F:\Dev\RD-Agent-main\git_ignore_folder\factor_implementation_source_data_debug_backup_20260429_204226`
- `/home/lc999/data/qlib_minute_bin` 已在原 2024-01-02~2026-03-19 全市场分钟 bin 基础上增量追加到 2026-04-28；备份：`/home/lc999/data/qlib_minute_bin_backup_20260429_205315`

## 数据校验

- WSL day Qlib：`calendar_last=2026-04-28`，`calendar_count=1876`，`all.txt=4616`，`D.list_instruments(..., 2026-04-28)=4615`。
- WSL day sample：`000001.SZ` 在 2026-04-24/27/28 的 `$close/$factor/$up_limit_price/$down_limit_price/$prev_close` 可读；`000300.SH` 指数 `$close` 可读，limit/prev_close 为空符合指数字段预期。
- RD-Agent prod static parquet：`7,313,383` index rows，`2018-08-01 ~ 2026-04-28`，`4696` instruments。
- RD-Agent debug static parquet：`33,944` index rows，`2018-08-01 ~ 2019-12-31`，`99` instruments，符合 debug 裁剪定位。
- WSL combined static parquet：`7,313,383` index rows，`2018-08-01 ~ 2026-04-28`，`4696` instruments。
- WSL minute Qlib：增量追加 `35,491,201` DB minute rows，新增 `6506` 个 1min calendar entries，`2026-03-20 09:31:00 ~ 2026-04-28 15:00:00`，最终 `calendar_last=2026-04-28 15:00:00`，`calendar_count=134807`。
- WSL minute sample：`000001.SZ/000333.SZ/300750.SZ/600519.SH` 在 2026-04-28 均为 `241` 行；13:00 在旧数据和新数据中均为空，是既有 240/241 bar 对齐特征，不是本次增量新增异常。
- RD-Agent Data Doctor：`reports/rdagent_data_doctor_after_promote_20260428.json`，9 项 PASS。

## 配置校验

- QE 默认 split：`test_end=2026-04-28`，自动派生安全组合回测 `backtest_end=2026-04-27`，避免 Qlib 在最后日访问 `calendar[index+1]` 越界。
- RD-Agent 主仓及 app_tpl 版本：`rdagent/scenarios/qlib`、`app_tpl/qlib/v0`、`app_tpl/all/v1/v2/V3/v4/v5/v6/v4-5d/v4-10d` 活跃配置中的 `2026-03-10` 已替换为 `2026-04-28`，`2026-03-09` 已替换为 `2026-04-27`；保留 `backup`/`.bak` 历史文件不改。
- 活跃路径旧日期扫描：无 `2026-03-10/2026-03-09/2026-03-19/2026-03-03/2025-12-01` 残留（排除 backup/.bak）。

## 执行验证

- `python -m py_compile backend/services/quantevolver/config_composer.py backend/routers/quantevolver.py backend/services/quantevolver/factor_transformation_service.py backend/services/hmm_training_service.py scripts/qrun_limit.py scripts/qrun_limit_minute.py scripts/export_minute_prod.py scripts/register_strict_dynamic_hmm_candidates.py`：PASS。
- RD-Agent qrun helper `py_compile`：PASS。
- RD-Agent YAML 直接 `safe_load`：84 个配置中部分 Jinja 模板因 `{% ... %}` 不能被裸 YAML 解析，这是模板既有结构；本次另用旧日期扫描和 py_compile 验证替换结果。
- 官方 day 多数据集 smoke：`reports/qlib_multi_dataset_smoke_official_20260428/report.json`，PASS，20 股票，signal 35 dates，portfolio report 34 rows。
- 官方 day+minute NestedExecutor smoke：`reports/qlib_official_minute_chain_smoke_20260428.json`，PASS，正式 `/home/lc999/data/qlib_bin` + `/home/lc999/data/qlib_minute_bin`，2026-04-20~2026-04-27，4 股票，NestedExecutor(day->1min)。

## 结论

- 可以使用新的正式 WSL day/minute Qlib 数据与 RD-Agent factor 数据运行到最新完成交易日数据范围：数据/信号覆盖 `2026-04-28`。
- 因 Qlib 组合回测需要下一根 day calendar，默认组合回测截止为 `2026-04-27`；`2026-04-28` 作为数据和信号最后日保留。
