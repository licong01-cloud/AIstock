# AIstock 回测数据集导出与签收指引（2026-07-12）

## 1. 适用范围

本指引覆盖：

- Qlib 日线 bin、分钟 bin；
- `daily_pv.h5`、`moneyflow.h5` 等7类 H5；
- `static_factors.parquet`；
- PIT 股票池、`l2_code_id`、debug 子集；
- WSL/node1 版本化部署；
- UI、REST、MCP 和脚本导出的一致性签收。

此前临时签收文件 `docs/handoff/_scratch/dc_signoff_candidate_20260630.md` 不在当前仓库中。本文件作为可追踪的长期导出指引，后续更新不得只写临时 handoff。

## 2. 数据层与生产路径

| 组件 | WSL runtime | 候选/版本目录 |
|---|---|---|
| 日线 bin | `/home/lc999/data/qlib_bin` | `/home/lc999/data/qlib_bin_*_candidate_<cutoff>` |
| 分钟 bin | `/home/lc999/data/qlib_minute_bin` | `/home/lc999/data/qlib_minute_*_candidate_<cutoff>` |
| 因子源 | `/home/lc999/data/factor_data` symlink | `/home/lc999/data/factor_data_versions/<bundle>` |
| Windows 候选 | 不作为 QE runtime | `F:/Dev/AIstock/qlib_snapshots/<snapshot_id>` |

QE、因子库、荐股、选股和模拟盘统一读取 canonical 因子单位，不直接读取 Tushare 原始单位。

## 3. 资金流单位与公式契约

### 3.1 源数据

Tushare `moneyflow` 和 `market.moneyflow_ts`：

- `*_vol`、`net_mf_vol`：手；
- `*_amount`、`net_mf_amount`：万元。

源表不可为迁就下游而改写历史单位。

### 3.2 DB 外 canonical 单位

| 字段 | 转换 | canonical 单位 |
|---|---:|---|
| 9个量字段 | `source × 100` | 股 |
| 9个金额字段 | `source × 10000` | 元 |

契约版本：`tushare_moneyflow_shares_yuan_v1`。

统一实现：`backend/data_service/moneyflow_contract.py`。REST、脚本、QE、因子库和实时推理不得复制另一套倍率或根据数值大小猜单位。

### 3.3 保留字段名并修正数值

为兼容已有因子，不重命名 `mf_total_net_*`：

- `mf_total_net_amt = mf_net_amt`；
- `mf_total_net_vol = mf_net_vol`；
- 5日/20日字段从上述 canonical 值滚动计算；
- 不得用四档买卖金额或成交量合计相减替代 Tushare Level-2 `net_mf_*`；
- 金额比例分母为成交额（元）；
- 量比例分母为未复权成交股数：`daily_pv.volume × daily_pv.factor`；
- `mf_elg_share_in_main_*` 统一为超大单净流入/主力净流入。

## 4. 候选导出流程

### 4.1 Preflight

1. 检查 WSL 数据盘剩余空间；分钟导出建议保留120GB以上。
2. 检查后端8001；未运行时由用户决定是否重启。
3. 确认 DB 数据截止日和逐日覆盖。
4. 确认 WSL/node1路径来自当前 `infra.compute_nodes` 配置。
5. 输出必须包含 `candidate`；禁止覆盖 production。

### 4.2 日线和分钟 bin

使用：

```bash
python scripts/qlib_authoritative_bin_export.py --dataset stock_daily --stage all \
  --snapshot-id <daily_candidate> --start 2018-08-01 --end <cutoff> \
  --stock-universe-mode pit_spans --universe-key shsz_st_pit_active_v1 \
  --exchanges sh,sz --bin-root /home/lc999/data
```

分钟线使用同一脚本的 `stock_minute`、chunked 参数。源表只允许读取或 UPSERT，禁止 truncate。

### 4.3 H5 与 static

UI 和 REST 依次执行：

1. `daily`
2. `daily_basic`
3. `moneyflow`
4. `bak_basic`
5. `margin_detail`
6. `cyq_perf`
7. `sector_data`
8. `POST /snapshots/{sid}/static_factors`
9. `POST /field_map/export`

脚本入口：

```bash
python scripts/export_qe_qlib_candidate.py \
  --start 2018-08-01 --end <cutoff> \
  --snapshot-id <h5_candidate> --bin-id <bin_candidate> \
  --static-schema-source \
  F:/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260630/static_factors.parquet
```

两条路径必须共用 canonical moneyflow contract。不得使用 RD-Agent 旧 `generate_static_factors_bundle.py` 重新派生资金流字段。

`--static-schema-source` 必须指向包含 121 个数据列的权威 static 基线；Parquet
连同 `datetime/instrument` 共 123 列，并包含 `l2_code_id int16`。导出器会在读取
DB 前 fail-fast 拒绝缺少 `l2_code_id` 的旧 120 列 `qlib_test` schema。股票按去重
代码全局分批读取；不得再按每个 `list_date` 拆成上千批。逐股票上市日起点在复权
合并前过滤，上市满 365 天仍由 official universe mask 处理。

### 4.4 PIT bundle

canonical index 必须由 `instruments/all.txt` 多区间 spans 与 `calendars/day.txt` 展开得到。将7个 H5 和 static 全部裁剪到 `index ⊆ canonical`：

- 越界行必须为0；
- 缺失保持 NaN，禁止补0；
- `sector_data.h5` 包含 `l2_code_id int16`；
- static 同样包含 `l2_code_id`；
- 尚未进入申万二级行业的新股显式编码为 `UNKNOWN_L2_CODE_ID=-1`，不得填 0；
- debug 子集从完整候选切片，保持100股和既定 debug 日期窗。

## 5. 强制签收

执行：

```bash
python scripts/validate_qe_qlib_candidate.py \
  --start 2018-08-01 --end <cutoff> \
  --snapshot-dir <candidate_snapshot> \
  --bin-dir <candidate_bin> \
  --static-schema-source \
  F:/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260630/static_factors.parquet
```

仅重建 H5/static 时显式加 `--skip-bin`；这会生成 WARN 回执而不是伪造 bin
验证通过。需要完整发布时仍必须单独完成 bin smoke。

至少通过：

1. calendar、末日真实行情、文件集、行列数；
2. PIT 越界=0、股票池多 span 正确；
3. H5/static schema 与 `l2_code_id`；
4. `meta.json.moneyflow_unit_contract.version` 正确；
5. H5/static 18个原始资金流字段逐值一致；
6. 相对 DB 的量倍率=100、金额倍率=10000；
7. `mf_total_net_*` 与 `mf_net_*` 一致；
8. 最小日频/分钟 backtest smoke；
9. WSL/node1 文件哈希一致。

单位门禁失败时，不得启动因子独立指标、缓存、分类评级或相关性计算。

## 6. UI、后端和实时环境

- UI 的资金流导出明确显示量=股、额=元，并显示 backend 返回的 contract version。
- REST full/incremental moneyflow 和 static 导出把 contract receipt 写入 `meta.json`。
- `qe_data_service`、`timescaledb_adapter`、因子预处理和实时 inference 共用同一转换和派生函数。
- 荐股、选股、模拟盘读取的原始 `mf_*` 与派生字段必须和离线 static 完全一致。
- 旧缓存不得跨 contract version 复用。

## 7. 历史实验影响

历史数据分为三类：

1. 只使用价格、基本面、板块等非资金流字段：不受本问题影响。
2. 使用资金流原始值或固定倍率错误的比例，但先做逐日截面 rank/z-score：正倍率通常不改变排序，IC可能基本不变；仍需重算确认。
3. 使用 `mf_total_net_*`、绝对阈值、跨字段运算、线性/神经网络未标准化输入或旧实时推理：会受到实质影响。

其中 `mf_total_net_*` 旧值由四档买卖合计相减，常接近0，和 Tushare `net_mf_*` 不是同一信号。所有引用这些字段的历史实验必须标记 `moneyflow_contract=legacy_invalid`，不得作为新因子或生产策略的最终证据；需要在新 bundle 上重跑 QE 对照。

2026-07-12 对生产因子目录和 `qe_experiments` 的只读审计结果：

- 247个因子代码或表达式引用资金流字段，其中177个当前可用；
- 16个引用 `mf_total_net_*`，其中3个当前可用；
- 6个引用 `mf_elg_share_in_main_*`，且6个当前可用；
- 989条历史实验记录引用105个受影响因子；
- 其中276条实验记录引用10个发生公式语义变化的因子。

这些数量只说明需要复核的集合，不表示所有989条实验的排序或收益必然变化。是否实质变化取决于因子是否做截面排序/标准化、是否含阈值和跨字段运算、以及模型对特征尺度是否敏感。

## 8. 部署与回滚

1. 完成全部签收并输出 dry-run 回执。
2. 用户明确确认后，原子切换 `/home/lc999/data/factor_data` symlink。
3. node1 先同步版本目录并校验哈希，再切 symlink。
4. 清理或版本隔离旧资金流缓存。
5. 保留上一个版本目录和回滚命令：
   `ln -sfn <previous_version> /home/lc999/data/factor_data`。
6. 数据集部署、代码合入、服务重启分别报告，不得混称“已生产生效”。
