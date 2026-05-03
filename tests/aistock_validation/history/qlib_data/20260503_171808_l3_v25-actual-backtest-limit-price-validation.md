# V25 实盘执行策略涨跌停价格读取回测验证

- 日期：2026-05-03
- 级别：L3 data / execution smoke
- 数据集：`/home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428`
- 日频 provider：`/home/lc999/data/qlib_bin`
- 验证窗口：`2025-07-01` ~ `2025-07-08`
- 股票样本：`000153.SZ,001236.SZ,002080.SZ,600110.SH`
- 核心目标：验证 Qlib 1min bin 数据完整、DB 与 bin 价格口径一致、实际 V25 Qlib inner strategy 能读取正确涨跌停价格并按 raw basis 处理涨跌停状态。

## 执行命令

```bash
cd /mnt/f/Dev/AIstock
source /home/lc999/miniconda3/etc/profile.d/conda.sh
conda activate rdagent-gpu

PYTHONPATH=/mnt/f/Dev/AIstock PYTHONWARNINGS=ignore python scripts/qlib_authoritative_bin_export.py \
  --dataset stock_minute \
  --stage validate \
  --snapshot-id qlib_minute_authoritative_shsz_nonst_full_20260428 \
  --start 2025-07-01 \
  --end 2025-07-08 \
  --basis-start 2024-01-02 \
  --basis-end 2026-04-28 \
  --exchanges sh,sz \
  --codes 000153.SZ,001236.SZ,002080.SZ,600110.SH \
  --csv-root /home/lc999/data/qlib_csv_authoritative \
  --bin-root /home/lc999/data \
  --reports-dir reports/qlib_authoritative_export/backtest_v25_20260503 \
  --validate-max-errors 20

PYTHONPATH=/mnt/f/Dev/AIstock python scripts/qlib_authoritative_smoke_backtest.py \
  --minute-provider-uri /home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428 \
  --day-provider-uri /home/lc999/data/qlib_bin \
  --start 2025-07-01 \
  --end 2025-07-08 \
  --codes 000153.SZ,001236.SZ,002080.SZ,600110.SH \
  --topk 2 \
  --drop 1 \
  --output reports/qlib_authoritative_export/backtest_v25_20260503/qlib_minute_authoritative_shsz_nonst_v25_backtest_smoke_20260503.json

PYTHONPATH=/mnt/f/Dev/AIstock python scripts/qlib_v25_limit_state_smoke.py \
  --provider-uri /home/lc999/data/qlib_minute_authoritative_shsz_nonst_full_20260428 \
  --day-provider-uri /home/lc999/data/qlib_bin \
  --start 2025-07-01 \
  --end 2025-07-08 \
  --codes 000153.SZ,001236.SZ,002080.SZ,600110.SH \
  --output reports/qlib_authoritative_export/backtest_v25_20260503/qlib_minute_authoritative_shsz_nonst_v25_limit_state_20260503.json
```

实际 V25 Qlib inner strategy 回测使用临时脚本运行 `TailTWAPWithV25TwoStageStrategy`，未修改仓库代码或 Qlib bin 数据，输出：

- `reports/qlib_authoritative_export/backtest_v25_20260503/qlib_minute_authoritative_shsz_nonst_v25_actual_backtest_smoke_20260503.json`
- `reports/qlib_authoritative_export/backtest_v25_20260503/qlib_minute_authoritative_shsz_nonst_v25_actual_backtest_smoke_20260503.log`

## 验证结果

```text
检查项                         结果        关键数据
----------------------------  ----------  ------------------------------------------------------------
DB vs Qlib bin 字段一致性       PASS        5,780 rows / 69,360 field values; max_abs_diff <= 0.0
普通 Qlib 1min 回测             PASS        portfolio rows=6; last_account=1,028,601.004812
实际 V25 inner strategy 回测    PASS        TailTWAPWithV25TwoStageStrategy; last_account=1,012,891.006811
分钟字段缺失                   PASS        open/high/low/close/volume/amount/factor/limit 字段 NaN 全部为 0
V25 涨跌停状态分类              PASS        rows_checked=5,780; data_error_count=0; flag_mismatch_count=0
DB vs Qlib 涨跌停样本           PASS        sample_count=10; max_abs_diff <= 1e-6
V25 日志错误扫描                PASS        Traceback/RuntimeError/missing_data_error/limit_price_missing 均为 0
```

## 数据口径核验

```text
字段/逻辑                         口径                         验证结果
-------------------------------  ---------------------------  ----------------------------------------
Qlib open/high/low/close          前复权 adjusted price         使用 factor 可还原 DB raw price
Qlib factor                       adj_factor / 单股票窗口最大值  与 DB market.adj_factor 计算结果一致
Qlib prev_close                   未复权 raw price              与 DB market.stk_limit.pre_close 一致
Qlib up_limit_price               未复权 raw price              与 DB market.stk_limit.up_limit 一致
Qlib down_limit_price             未复权 raw price              与 DB market.stk_limit.down_limit 一致
Qlib limit_up/limit_down          raw close 与 raw limit 比较    与 V25 独立重算结果一致
V25 价格比较                      raw basis                    adjusted close / factor 后与 raw limit 比较
```

## V25 实际回测证据

实际 V25 strategy 已在 Qlib NestedExecutor 中执行，使用 `TailTWAPWithV25TwoStageStrategy` 和真实 V25 模型文件：

```text
指标                       值
------------------------  ------------------------------------------------
execution_strategy        TailTWAPWithV25TwoStageStrategy
minute_rows               5,780
minute_dates              2025-07-01, 2025-07-02, 2025-07-03, 2025-07-04, 2025-07-07, 2025-07-08
portfolio_rows            6
first_account             1,000,000.000000
last_account              1,012,891.006811
return_sum                0.014285346335
return_nan                0
last_cash                 23,199.007032
last_value                989,691.999779
indicator_keys            1day, 1min
```

V25 日志中出现 raw basis 计划生成与涨跌停状态处理：

```text
stock       time                 V25 reason                  raw price / limit evidence
----------  -------------------  --------------------------  ----------------------------------------
000153.SZ   plan generation      generated plan              open_raw=6.900000 prev_close_raw=6.790000 factor=1.00000000
001236.SZ   plan generation      generated plan              open_raw=13.630000 prev_close_raw=14.380000 factor=0.99920225
001236.SZ   2025-07-02 13:49:00  p0_limit_buy_at_down_limit  close_raw=12.940000 down_limit_raw=12.940000 factor=0.99920225
```

## 独立涨跌停状态扫描

```text
状态                         次数
--------------------------  ------
limit_up_buy_blocked          209
p0_limit_sell_at_up_limit     209
limit_down_sell_blocked       162
p0_limit_buy_at_down_limit    162
intraday_halt_or_no_bar        50
tradable                   10,768
data_error_count                0
flag_mismatch_count             0
```

## DB 与 Qlib 样本核验

```text
样本类型     股票        时间                 Qlib raw close  DB raw close  up_limit  down_limit  V25 buy reason              V25 sell reason
----------  ----------  -------------------  --------------  ------------  --------  ----------  --------------------------  --------------------------
limit_up    000153.SZ   2025-07-01 11:11:00        6.790000      6.790000  6.790000    5.550000  limit_up_buy_blocked        p0_limit_sell_at_up_limit
limit_up    000153.SZ   2025-07-01 11:12:00        6.790000      6.790000  6.790000    5.550000  limit_up_buy_blocked        p0_limit_sell_at_up_limit
limit_up    000153.SZ   2025-07-01 11:18:00        6.790000      6.790000  6.790000    5.550000  limit_up_buy_blocked        p0_limit_sell_at_up_limit
limit_down  001236.SZ   2025-07-01 10:50:00       14.380000     14.380000 17.580000   14.380000  p0_limit_buy_at_down_limit  limit_down_sell_blocked
limit_down  001236.SZ   2025-07-01 10:51:00       14.380000     14.380000 17.580000   14.380000  p0_limit_buy_at_down_limit  limit_down_sell_blocked
limit_down  001236.SZ   2025-07-01 10:52:00       14.380000     14.380000 17.580000   14.380000  p0_limit_buy_at_down_limit  limit_down_sell_blocked
close       002080.SZ   2025-07-01 15:00:00       21.450001     21.450000 21.450001   17.549999  limit_up_buy_blocked        p0_limit_sell_at_up_limit
close       600110.SH   2025-07-01 15:00:00        6.290000      6.290000  7.690000    6.290000  p0_limit_buy_at_down_limit  limit_down_sell_blocked
```

样本最大误差：

```text
比较项                               max_abs_diff
----------------------------------  ------------
qlib_raw_close_vs_db_close              0.000001
qlib_factor_vs_db_qfq_factor            0.000000
qlib_prev_close_vs_db_pre_close         0.000000
qlib_up_limit_vs_db_up_limit            0.000001
qlib_down_limit_vs_db_down_limit        0.000001
```

## 结论

- 本次验证范围内未发现 Qlib 1min bin 字段缺失、DB/bin 价格不一致、涨跌停字段口径不一致、V25 无法读取涨跌停价格、或 V25 静默降级的问题。
- V25 实际回测已经证明 `TailTWAPWithV25TwoStageStrategy` 可以在 Qlib NestedExecutor 中读取 `$factor/$prev_close/$up_limit_price/$down_limit_price`，并以 raw basis 完成涨跌停判断。
- Qlib 日志中的 Gym 维护提示、future calendar fallback、`common_infra` warning 为本次 smoke 环境的非阻断警告；错误扫描未发现 Traceback、RuntimeError 或关键 missing-data 错误。
- 本记录只验证小样本真实数据窗口，不替代全市场全时间段审计；后续如需生产级结论，应继续执行全量字段覆盖率、全量涨跌停事件、停牌日、ST/退市/BJ 排除、IPO all.txt 生效范围的批量验证。
