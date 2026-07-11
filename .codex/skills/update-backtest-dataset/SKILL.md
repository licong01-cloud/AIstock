---
name: update-backtest-dataset
description: Rebuild, validate, and candidate-deploy AIstock Qlib daily/minute bins and H5/static factor bundles to a new cutoff. Use for 更新回测数据集, 补齐回测数据, rebuild qlib bin/h5, refresh factor_data, dataset signoff, or versioned WSL deployment. Enforces PIT universe and canonical moneyflow share/CNY parity; never overwrites production without explicit confirmation.
---

# 更新 AIstock 回测数据集

先读 `F:/Dev/AIstock/docs/analysis/qlib_backtest_dataset_export_guide_20260712.md`。该文档是命令、路径、校验和回滚的权威指引；本 skill 只保留执行护栏。

## 硬性规则

1. 仅写 `*_candidate_*` 或版本化目录；验证通过且用户明确确认前不得切换生产 symlink。
2. 源表只读或 UPSERT；禁止 truncate、静默补零、静默丢行。
3. 不自行启动或重启后端、前端、QE、RDAgent 服务。
4. 股票池使用 `pit_spans + shsz_st_pit_active_v1 + sh,sz`，不得把实验黑名单写入 `all.txt`。
5. `market.moneyflow_ts` 保持 Tushare 原始单位：量=手、额=万元；DB 外所有 H5、static、QE、因子库和实时环境统一：量=股、额=元。
6. 保留 `mf_total_net_*` 字段名；其值必须来自 Tushare `net_mf_vol/net_mf_amount`，禁止用四档买卖合计相减替代。
7. 任何 moneyflow contract、H5/static parity 或 PIT 边界检查失败都停止部署。

## 执行入口

- 日线/分钟 bin：`python scripts/qlib_authoritative_bin_export.py ...`
- H5 REST：`/api/v1/qlib/snapshots/{daily,daily_basic,moneyflow,bak_basic,margin_detail,cyq_perf,sector_data}`
- static：`POST /api/v1/qlib/snapshots/{snapshot_id}/static_factors`
- 脚本候选：`python scripts/export_qe_qlib_candidate.py ... --static-schema-source <121列static基线>`
- 完整校验：`python scripts/validate_qe_qlib_candidate.py --snapshot-dir <candidate> --static-schema-source <同一基线> ...`

优先 REST 生成完整 H5；脚本路径和 REST 路径必须产生相同单位契约。不要使用 RD-Agent 旧版 `generate_static_factors_bundle.py` 重新派生资金流字段。

当前 2026-06-30 权威 schema 基线为
`F:/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_20260630/static_factors.parquet`。
它包含 121 个数据列（Parquet 连同 `datetime/instrument` 共 123 列）和
`l2_code_id int16`。不得使用缺少 `l2_code_id` 的 `qlib_test` 旧 120 列基线。

## 资金流签收门禁

候选必须同时满足：

- `meta.json.moneyflow_unit_contract.version == tushare_moneyflow_shares_yuan_v1`
- `moneyflow.h5` 与 `static_factors.parquet` 的18个原始 `mf_*` 字段逐值一致
- 相对源 DB：9个量字段倍率=100，9个额字段倍率=10000
- `mf_total_net_amt == mf_net_amt`、`mf_total_net_vol == mf_net_vol`
- 金额比例分母为未复权成交额（元）
- 成交量比例分母为原始成交股数，即 `daily_pv.volume * factor`
- 5日/20日派生值由相同 canonical 字段重新计算
- `sector_data.h5` 和 static 都包含 PIT `l2_code_id int16`
- 尚未进入申万二级行业的新股使用显式 `UNKNOWN_L2_CODE_ID=-1`；不得补 0

不得只凭列存在、日期截止或文件大小判定合格。

## 版本化部署

1. 完成候选、PIT mask、schema/meta、Data Doctor、最小因子读取和 QE smoke。
2. 输出当前 symlink、候选目标、回滚命令，先 dry-run。
3. 用户确认后才原子切换 `/home/lc999/data/factor_data`。
4. WSL 与 node1 分别同步版本目录并校验哈希。
5. 单位契约升级后清理或版本隔离旧资金流因子缓存，再触发独立指标、分类评级与相关性计算。
6. 保留旧版本目录，确保单命令回滚。

## 交付回执

报告候选路径、日期范围、股票/PIT范围、全部文件行列、moneyflow contract、H5/static parity、bin smoke、WSL/node1哈希、生产是否切换及回滚命令。
