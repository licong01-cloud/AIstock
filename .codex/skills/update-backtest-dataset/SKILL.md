---
name: update-backtest-dataset
description: "Rebuild the qlib backtest dataset (daily bin + minute bin + h5 factor source + optional stock pool) to a new cutoff date, validate it against production口径, and deploy after user confirm. Use for '更新回测数据集', '补齐回测数据到某日期', 'rebuild qlib bin/h5', 'refresh backtest data', or extending daily/minute/h5 coverage. Candidate-only until validated + user confirm."
---

# Skill: 更新回测数据集 (qlib daily bin + minute bin + h5 + pool → 新截止日)

用户说"更新回测数据到 YYYY-MM-DD / 补齐回测数据 / rebuild qlib bin"时按此流程。**工具无关**:Claude Code 与 Codex 均可执行(脚本为纯 bash/python)。

## 铁律 (违反即停)
1. **候选优先**:全程导出到 `*_candidate_*` 路径,**绝不覆盖生产**,直到完整性验证通过 + 用户明确确认。
2. **禁 truncate**:`kline_minute_raw` 等源表只 UPSERT,绝不 `--truncate`;删任何数据前先核实非生产 + 用户批准。
3. **禁启动服务**:后端 8001 若未运行,提醒用户重启,不自行启动。
4. **回测读 bin/h5 文件,不读 DB**。

## 数据集三件 + 生产路径
| 组件 | 生产路径 (WSL) | node1 路径 (DB `infra.compute_nodes`) |
|---|---|---|
| 日线 bin | `/home/lc999/data/qlib_bin` | 同 (`qlib_data_path`) |
| 分钟 bin | `/home/lc999/data/qlib_minute_bin` | 同 (`qlib_minute_path`) |
| h5 因子源 | `/mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data` | `/home/lc999/data/factor_data` (`factor_data_dir`) |

候选输出到 `/home/lc999/data/qlib_bin_st_pit_active_daily_candidate_20180801_<CUTOFF>` 等;h5 候选到 `/mnt/f/Dev/AIstock/qlib_snapshots/qlib_st_pit_active_h5_daily_candidate_20180801_<CUTOFF>`。

## Preflight (先全过再动手)
```bash
# 1. 磁盘: 分钟 bin ~76G+, 需 WSL 剩余 >120G。清旧候选 CSV 先核实非生产
df -h /home/lc999/data
# 2. 后端 8001 运行(h5 导出走 REST)。未运行→提醒用户重启, 不自启
curl -s http://127.0.0.1:8001/api/v1/health >/dev/null && echo backend-up || echo "提醒用户重启后端"
# 3. DB 分钟线已补到 CUTOFF (逐日无缺口)。用 UPSERT 补, 禁 truncate
# 4. node1 SSH 通: ssh -i ~/.ssh/id_ed25519 lc999@192.168.50.215 hostname
```

## 执行顺序 (CUTOFF=目标日, 如 2026-06-30)
canonical 脚本在 `F:/Dev/AIstock/scripts/`。分钟+h5 可并行(并行度 ≤2)。

### A. 日线 bin (export→dump→validate)
```bash
python scripts/qlib_authoritative_bin_export.py --dataset stock_daily --stage all \
  --snapshot-id qlib_bin_st_pit_active_daily_candidate_20180801_<CUTOFF> \
  --start 2018-08-01 --end <CUTOFF> --basis-start 2018-08-01 --basis-end <CUTOFF> \
  --stock-universe-mode pit_spans --universe-key shsz_st_pit_active_v1 \
  --exchanges sh,sz --bin-root /home/lc999/data
```
### B. 分钟 bin (chunked, 后台)
```bash
python scripts/qlib_authoritative_bin_export.py --dataset stock_minute --stage all \
  --snapshot-id qlib_minute_authoritative_full_candidate_20240102_<CUTOFF> \
  --start 2024-01-02 --end <CUTOFF> \
  --stock-universe-mode pit_spans --universe-key shsz_st_pit_active_v1 --exchanges sh,sz \
  --minute-chunked-export --minute-code-batch-size 300 --minute-chunk-months 3 \
  --csv-root /home/lc999/data/qlib_csv_authoritative --bin-root /home/lc999/data
```
### C. h5 因子源 (REST, 候选 snapshot)
`POST /api/v1/qlib/snapshots/{daily,daily_basic,moneyflow,bak_basic,margin_detail,cyq_perf,sector_data}` 各带:
`{snapshot_id, start, end:<CUTOFF>, exchanges:["sh","sz"], stock_universe_mode:"pit_spans", universe_key:"shsz_st_pit_active_v1"}`
再 `POST /snapshots/{sid}/static_factors` + `POST /field_map/export`。
> **DB 竞争**:静态因子导出前把分钟导出进程组 SIGSTOP (`kill -STOP -<PGID>`),完再 SIGCONT,避免 statement_timeout。

### D. 股票池 (可选, 非母池!)
`filtered_pool_<CUTOFF>` **仅**在某实验要开黑名单时才生成 (`python scripts/generate_stock_pool.py --date <CUTOFF>`),写入本地缓存 `F:/Dev/AIstock/stock_pools/`。母池 = `all.txt`(全量),**绝不把单实验黑名单烧进 all.txt**。数据集更新本身不需要生成 pool。

### E. Smoke 验证
```bash
python scripts/qlib_authoritative_smoke_backtest.py \
  --minute-provider-uri <候选分钟bin> --day-provider-uri <候选日线bin> \
  --start <CUTOFF-6周> --end <CUTOFF-4日> --codes 600519.SH,000001.SZ,600036.SH \
  --topk 2 --drop 1 --output /home/lc999/data/smoke_validate_candidate_<CUTOFF>.json
```

### F. D-C 完整性签收 (见 scripts/dc_signoff 模板)
核验:①末日有真实数据(`$close` NaN=0)②calendar 到 CUTOFF ③features/bin 计数 ④h5 文件集含 sector_data ⑤与生产口径规则等价(pool 规则、前复权、ST-PIT 多span)。

### G. 部署 (仅用户确认后)
备份现生产三件 → 候选覆盖生产 → rsync 到 node1 → md5 对齐 → 冒烟。

## 已知坑 (踩过, 必避)
- **.env 是 CRLF+GBK**:解析用 `grep -E '^TDX_DB_' .env | tr -d '\r'`,勿直接 source。
- **`wsl -d Ubuntu -- bash -lc "...$VAR..."` 变量不展开**(常返空致误删/误判)→ 一律写脚本文件 `bash /mnt/c/.../x.sh`,勿内联带变量。
- **`--exchanges sh,sz` 只两市**;传 `bj` 被拒(normalize_stock_export_exchanges rejects BJ)。
- **必须 `--stock-universe-mode pit_spans`**;否则缺 stk_limit 的个股(如 001237.SZ)导出报 RuntimeError。
- **边界 bar NaN (09:30/13:00) 是良性**:源于 TDX 分钟源 bar 约定变化,**生产同样存在**,QE 回测自动处理(`custom_strategy.py` dropna + NaN 买价跳过)。smoke `ok=False` 若仅因 `bad_minute_nan` 可接受,**非损坏,不改数据**(保持与历史一致)。
- **conda 脚本勿 `set -u`**(破坏 cuda_env.sh);wrapper 用 `export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"` + `conda activate rdagent-gpu`。
- **磁盘**:分钟 CSV 中间产物大(数十G),导出前清旧候选 CSV(核实非生产 + 用户批准)。
- **node1 因子源在 `/home/lc999/data/factor_data`**(查 `infra.compute_nodes.factor_data_dir`),**不是** `qlib_snapshots`(那是陈旧遗留)。部署必同步此路径。
- **前复权格式**:`out.close=raw_close*qfq`,`qfq=adj_factor/max(adj_factor in basis window)`,basis_end 归一=1。分红日会改 basis→非 append-safe,整段重导。

## 遗留增强 (独立方法论项, 同时影响生产)
- 分钟边界 bar 归一化;退市 PIT(delist_pit,当前退市股整体排除=幸存者偏差)。

## Codex 使用
本 skill 同时置于 `F:/Dev/AIstock/.codex/skills/update-backtest-dataset/SKILL.md`;Codex 经 `AGENTS.md` task-router 或直接读此文件执行,脚本为纯 bash/python 无 Claude Code 依赖。
