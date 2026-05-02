# P0 AIstock Qlib Bin 导出程序准确性审计

- 日期：2026-05-03
- 范围：AIstock 侧 Qlib bin 导出程序，重点检查 1min 分钟 bin 导出链路。
- 结论级别：P0，当前 AIstock 侧分钟 bin 导出程序不能视为“完全准确/可直接用于生产替换”。
- 本次动作：只读代码与现有产物审计；未修改 Qlib bin；未重跑 QE；未修改导出代码。

## 总结结论

当前 AIstock 侧存在多条导出相关链路，但只有 10 股候选验证脚本证明了公式链路可行；backend API 的分钟 bin 导出和旧的 `scripts/export_minute_prod.py` 都不能直接作为权威生产导出程序使用。

```text
Exporter / Tool                                               Verdict        Main Reason
------------------------------------------------------------  -------------  ------------------------------------------------------------
backend/qlib_exporter/router.py::_export_minute_to_csv...     FAIL/P0        单 CSV 输入 dump_all、freq=1m、UTC 时间、缺 prev/limit 价格字段
backend/qlib_exporter/db_reader.py::load_qlib_minute_data     FAIL/P0        timestamptz 未转上海时区；factor 分母随导出窗口变化；limit 语义不一致
scripts/export_minute_prod.py                                 WARN/P0        公式接近但有 UTC 时间、qfq 缺失填 1.0、缺 prev/limit 价格、只打印 dump 命令
scripts/qlib_full_factor_minute_chain_validate.py             PASS/Sample    10 股样本验证通过；使用 AT TIME ZONE、完整字段、close-only limit 标志
scripts/dump_limit_price_minute_bins.py                       Partial        只补 prev_close/up_limit_price/down_limit_price，不补 OHLCV/factor
```

## P0 问题 1：backend 分钟导出把所有股票写成一个 CSV，dump_bin.py 会生成错误 instrument

证据：

- 日频导出已经知道 `dump_all` 需要每只股票一个 CSV：`backend/qlib_exporter/router.py:817` 到 `backend/qlib_exporter/router.py:823` 按 `symbol` 拆分。
- 分钟导出却写成单个文件：`backend/qlib_exporter/router.py:951` 到 `backend/qlib_exporter/router.py:952` 写 `minute_1m_all.csv`。
- RD-Agent `dump_bin.py` 的 `DumpDataAll` 对文件路径模式不按 `symbol` 分组，而是用文件名作为 instrument：`F:/Dev/RD-Agent-main/scripts/dump_bin.py:320` 取 `self.get_symbol_from_file(file_path)`，`F:/Dev/RD-Agent-main/scripts/dump_bin.py:281` 也是按文件名作为 code。

影响：

```text
Expected                         Current backend minute export
-------------------------------  ------------------------------------------------------------
000001.SZ.csv -> 000001.sz       minute_1m_all.csv -> minute_1m_all / 错误 instrument
每只股票独立 close/factor bin     多股票混在一个 CSV，dump_all 不会按 symbol 拆分
instruments/all.txt 为真实股票     instruments/all.txt 可能出现 fake symbol
```

这一个问题单独就足以判定 backend 分钟 bin 导出不准确。

## P0 问题 2：backend 分钟导出使用 `freq=1m`，而生产 / QE / Qlib 验证使用 `1min`

证据：

- API 请求模型允许 `freq="1m"`：`backend/qlib_exporter/router.py:656`。
- 分钟分支设置 `dump_freq = "1m"`：`backend/qlib_exporter/router.py:1057`。
- `dump_bin.py` 会按传入 freq 原样写文件名和 calendar：`F:/Dev/RD-Agent-main/scripts/dump_bin.py:210` 写 `{freq}.txt`，`F:/Dev/RD-Agent-main/scripts/dump_bin.py:260` 写 `{field}.{freq}.bin`。
- 正式生产与 QE 验证使用 `1min`：`tests/aistock_validation/history/qlib_data/20260429_211000_l4-official-qlib-rdagent-dataset-promotion-20260428.md` 记录 `/home/lc999/data/qlib_minute_bin` 是 1min；`scripts/qlib_full_factor_minute_chain_validate.py:407` 使用 `--freq 1min`。

影响：

```text
Expected Production Name        Backend Minute Export Name
------------------------------  --------------------------
calendars/1min.txt              calendars/1m.txt
features/<stock>/close.1min.bin features/<stock>/close.1m.bin
Qlib D.features(..., freq=1min)  不能直接读取 1m 命名产物
```

## P0 问题 3：backend 分钟导出没有把 DB timestamptz 转成上海交易时间

证据：

- `backend/qlib_exporter/db_reader.py:1715` 直接 SELECT `k.trade_time`。
- `backend/qlib_exporter/router.py:872` 直接 `.dt.strftime("%Y-%m-%d %H:%M:%S")`。
- 10 股候选验证脚本曾明确修复过这个问题：`tests/aistock_validation/history/qlib_data/20260429_192821_l4_full-factor-minute-execution-chain-validation-20260428.md` 中记录 “Local time exported as UTC clock”，修复方式是 SQL 使用 `trade_time AT TIME ZONE 'Asia/Shanghai'`。
- 本次直接 DB 抽样验证：

```text
DB Field                         Sample Value
-------------------------------  -------------------------
trade_time                       2025-07-08 01:31:00+00:00
trade_time AT TIME ZONE Shanghai 2025-07-08 09:31:00
```

影响：如果 backend 当前导出 CSV，交易时间会从 `09:31` 变成 `01:31`，生成的 1min calendar 与真实 A 股交易时间不一致。

## P0 问题 4：backend 分钟导出缺少 V25 / QE 已依赖的原始涨跌停价格和昨收字段

证据：

- backend 分钟 CSV 只包含 `open/high/low/close/volume/amount/factor/limit_up/limit_down`：`backend/qlib_exporter/router.py:925` 到 `backend/qlib_exporter/router.py:937`。
- 10 股候选验证脚本包含 `up_limit_price/down_limit_price/prev_close`：`scripts/qlib_full_factor_minute_chain_validate.py:291` 到 `scripts/qlib_full_factor_minute_chain_validate.py:296`，CSV 列在 `scripts/qlib_full_factor_minute_chain_validate.py:300` 到 `scripts/qlib_full_factor_minute_chain_validate.py:315`。
- V25 标准契约明确需要 `prev_close`、`up_limit_price`、`down_limit_price`，并要求原始价格口径一致：`docs/architecture/minute_execution_algo_standard_contract.md:201` 到 `docs/architecture/minute_execution_algo_standard_contract.md:210`。

影响：backend minute bin 即使 dump 成功，也不能满足当前 V25/QE 分钟执行所需字段。

## P0 问题 5：factor 分母随导出窗口变化，局部导出不会等价于全量导出

证据：

- backend 计算 qfq：`AdjFactorProvider.calculate_qfq_factor()` 默认使用传入 DataFrame 内每只股票的最大 adj_factor：`backend/qlib_exporter/adj_factor_provider.py:199` 到 `backend/qlib_exporter/adj_factor_provider.py:203`。
- `load_qlib_minute_data()` 只按请求 `start/end` 取 adj_factor：`backend/qlib_exporter/db_reader.py:1745`，随后直接计算 qfq：`backend/qlib_exporter/db_reader.py:1755`。
- 因此如果只导出缺口日期 2025-07-08~2025-07-16，factor 分母会变成这 7 天内的最大 adj_factor，而不是全量 2024-01-02~2026-04-28 的最大 adj_factor。

本次 5 股 DB 抽样验证：

```text
Code       max_full   max_gap    adj_20250708  qfq_full  qfq_gap  gap/full
---------  ---------  ---------  ------------  --------  -------  --------
000001.SZ  134.5794   131.7878   131.7878      0.979257  1.000000 1.021183
000063.SZ   17.1055    17.1055    17.1055      1.000000  1.000000 1.000000
000651.SZ  230.3931   215.2873   215.2873      0.934435  1.000000 1.070166
300750.SZ    1.9495     1.9125     1.9125      0.981021  1.000000 1.019346
600519.SH    8.4464     8.3050     8.3050      0.983259  1.000000 1.017026
```

影响：针对缺失股票/缺失日期做局部重导，如果仍使用当前 backend qfq 逻辑，价格、volume、factor 会与全量导出不一致。

## P0 问题 6：limit_up / limit_down 语义与生产邻近 bin、候选验证脚本不一致

证据：

- backend 当前用“一字涨跌停”判断：`backend/qlib_exporter/db_reader.py:1799` 到 `backend/qlib_exporter/db_reader.py:1812`，要求 open/low/close 或 open/high/close 同时达到涨跌停。
- 旧生产脚本和候选验证脚本使用 close-only 判断：`scripts/export_minute_prod.py:135` 到 `scripts/export_minute_prod.py:138`；`scripts/qlib_full_factor_minute_chain_validate.py:295` 到 `scripts/qlib_full_factor_minute_chain_validate.py:296`。
- 之前现有正式 bin 邻近样本也已验证：生产 `limit_up/limit_down` 匹配 raw close vs raw limit，不是复权价，也不是一字板逻辑。

基于保留 10 股候选 CSV 的本地验证：

```text
Symbol     Rows      CloseUp  OneWordUp  UpDiff  CloseDown  OneWordDown  DownDiff
---------  --------  -------  ---------  ------  ---------  -----------  --------
000001.SZ  134764    153      152        1       0          0            0
000063.SZ  134764    675      636        39      176        159          17
000333.SZ  134764    11       11         0       2          0            2
000651.SZ  134764    2        2          0       0          0            0
000858.SZ  134764    89       70         19      0          0            0
600000.SH  134764    0        0          0       0          0            0
600036.SH  134764    5        5          0       0          0            0
600519.SH  134764    0        0          0       0          0            0
601318.SH  134764    116      100        16      0          0            0
601688.SH  134764    484      472        12      34         31           3
TOTAL      1347640   1535     1448       87      212        190          22
```

影响：同一批分钟 bar 中，backend 一字板语义会少标记 87 个涨停 close bar 和 22 个跌停 close bar；如果 Qlib 交易所 `limit_threshold` 或模型 warmup 特征按 close-only 语义设计，这会改变交易约束或特征。

## P0 问题 7：旧 `scripts/export_minute_prod.py` 也不是权威准确导出程序

主要问题：

```text
Issue                                  Evidence / Line
-------------------------------------  ------------------------------------------------------------
直接 SELECT trade_time                  scripts/export_minute_prod.py:100-107，存在 UTC clock 风险
缺失 qfq_factor 静默填 1.0              scripts/export_minute_prod.py:117-120
缺失 limit 数据时填 0.0                 scripts/export_minute_prod.py:139-141
不导出 prev_close / up_limit_price      scripts/export_minute_prod.py:147-148 只有 limit_up/limit_down
只打印 dump_bin 命令，不实际执行         scripts/export_minute_prod.py:177-187
无 stock filter / 无 targeted export     scripts/export_minute_prod.py:37-40 直接取全部 minute raw 股票
```

它的 OHLCV/factor 主公式接近当前候选验证逻辑，但上述问题使它不能作为“完全准确”的生产导出程序。

## 历史导出记录与现有证据边界

```text
Record / Artifact                                                                                  Evidence
--------------------------------------------------------------------------------------------------  ------------------------------------------------------------
tests/aistock_validation/history/qlib_data/20260429_211000_l4-official-qlib-rdagent-dataset...      正式 minute bin = 旧 2024-01-02~2026-03-19 基线 + 2026-03-20~2026-04-28 增量
reports/qlib_official_minute_chain_smoke_20260428.json                                              official smoke 只覆盖 2026-04-20~2026-04-27
reports/qlib_full_factor_minute_chain_20260428/minute_export_summary.json                           10 股候选 CSV：1,347,640 行，缺失 stock-date=0
reports/qlib_full_factor_minute_chain_20260428/report.json                                          10 股候选 1min required fields NaN=0
qlib_minute_prod/csv                                                                                本机缺失，无法复盘全市场生产 CSV
qlib_minute_full/csv                                                                                本机缺失，无法复盘早期 full CSV
```

因此，能确定的是：当前 DB + 正确公式可以生成有效 10 股候选 minute bin；但不能证明历史正式全市场生产 CSV 是完整的，也不能证明 AIstock backend 当前 minute export 产物可用于生产。

## 修复建议

```text
Priority  Action                                                                  Purpose
--------  ----------------------------------------------------------------------  ------------------------------------------------------------
P0        backend minute CSV 改为每只股票一个 CSV                                 与 dump_bin.py dump_all 行为一致
P0        freq 统一为 `1min`，或在 API 层 `1m -> 1min` 显式映射                  与正式 Qlib/RD-Agent/QE provider_uri 一致
P0        SQL 导出 `trade_time AT TIME ZONE 'Asia/Shanghai' AS trade_time`        防止 09:31 变 01:31
P0        minute CSV 增加 `prev_close/up_limit_price/down_limit_price`            满足 V25/QE minute execution contract
P0        qfq denominator 对定向导出使用完整基准窗口                              保证局部重导与全量重导一致
P0        limit_up/down 统一为 raw close vs raw limit close-only 语义             与现有正式 bin 和候选验证脚本一致
P0        禁止 qfq 缺失填 1.0、limit 缺失填 0.0                                   数据缺失必须 fail-fast 或保留 NaN 并报告
P1        增加全市场 DB-vs-Qlib coverage gate 与随机历史日期 smoke                防止再次漏掉历史局部缺口
P1        导出保留 CSV/log/checksum/参数 manifest                                  后续可复盘生产导出输入
```

## 对当前分钟缺口修复的直接结论

不建议用当前 AIstock backend minute export 或 `scripts/export_minute_prod.py` 直接修复 `/home/lc999/data/qlib_minute_bin`。

正确方向应是新增或修复一个“权威定向重导”工具：

```text
Requirement                         Required Behavior
----------------------------------  ------------------------------------------------------------
股票范围                            支持指定 2696 只缺口股票，不必全市场重导
时间范围                            可只替换指定股票文件，但 qfq 分母必须来自完整导出基准窗口
字段                                open/high/low/close/volume/amount/factor/limit_up/limit_down/prev_close/up_limit_price/down_limit_price
时间                                使用上海交易时间 09:30/09:31 到 15:00
freq                                固定输出 1min
输出                                先 staging dump_bin，再逐字段校验，再替换正式 bin
验证                                DB rows、Qlib non-null、factor、raw close、limit、calendar offset、checksum 全部校验
```

## 本次验证命令摘要

```text
Command / Check                                                                    Result
---------------------------------------------------------------------------------  ------------------------------------------------------------
python -m py_compile scripts/export_minute_prod.py backend/qlib_exporter/*.py       PASS
读取 RD-Agent dump_bin.py 行为                                                     dump_all 按文件名建 instrument，不按 CSV symbol 分组
读取 20260429 official promotion 文档                                              正式 minute bin 是旧基线 + 增量追加
读取 10 股候选验证报告                                                             1,347,640 minute rows，required NaN=0
DB 抽样 trade_time vs Shanghai time                                                 01:31 UTC 对应 09:31 Shanghai
DB 5 股 qfq 分母抽样                                                                局部窗口可导致 factor 放大 1.7%~7.0%
10 股候选 CSV limit 语义对比                                                        close-only 与一字板存在 109 个 bar 差异
```
