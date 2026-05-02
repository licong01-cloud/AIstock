# Qlib 1min Bin 直接修复实施计划

- 日期：2026-05-02
- 目标数据集：`/home/lc999/data/qlib_minute_bin`
- 关联实验：`qe_20260501_011054_c90a` Loop19-28 数据准确性审计
- 计划性质：实施计划；本文件不执行数据修复，不修改 Qlib bin，不重跑 QE

## 先回答：上次是否只发现缺失但没有补齐所有字段

是。需要区分两件事：

```text
Event / Work                                  What Happened                                 Field Coverage
--------------------------------------------  --------------------------------------------  ------------------------------------------------------------
本轮之前的 Codex 审计                         只读诊断 + 文档 + CSV 缺口清单                没有修改任何 Qlib bin 字段
历史 `dump_limit_price_minute_bins.py` 操作    后补 `prev_close/up_limit_price/down_limit_price`  只补 3 个价格字段，不补 OHLCV/factor
当前正式 bin 的缺口状态                        DB 有分钟线，但 Qlib OHLCV/factor 全 NaN     `open/high/low/close/volume/amount/factor` 未补齐
后续正确修复                                  必须全字段一致修复                           至少 OHLCV + amount + factor + limit flags 校验/修复
```

现有证据表明：正式 bin 中问题 offset 的 `prev_close/up_limit_price/down_limit_price` 有值，是因为历史 overlay 工具只写了这三类字段；但 `open/high/low/close/volume/amount/factor` 仍然全空。因此如果现在只补 `close.1min.bin`，会留下字段口径不一致风险，后续回测/执行/审计仍可能出错。

## Phase 0：文档发现与允许使用的接口

```text
Source                                                                                         Evidence / Allowed Usage
---------------------------------------------------------------------------------------------  ------------------------------------------------------------
docs/codex_project_memory.md                                                                   QE/Qlib 分钟缺口已证明为正式 OHLCV/factor bin 快照不完整
C:/Users/lc999/.codex/skills/qe-evolution-diagnostics/SKILL.md                                 已有只读诊断工具与解释规则；修复前后继续复用诊断工具
scripts/qe_qlib_minute_gap_diagnosis.py                                                        可直接读取 Qlib `.1min.bin`，按 calendar offset 统计缺口
scripts/export_minute_prod.py                                                                  生产计划导出公式参考：OHLCV 使用 `*_li / 1000 * qfq_factor`
scripts/qlib_full_factor_minute_chain_validate.py                                              候选导出公式参考；已证明当前 DB 可导出问题日期有效 CSV/bin
scripts/dump_limit_price_minute_bins.py                                                        证明历史 overlay 只写 3 个 limit/pre_close 字段，不修复 OHLCV/factor
docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_export_lineage_root_cause_20260502.md     已记录缺口来源、保留 CSV、直接对比证据
```

禁止假设不存在的 API。直接修复 bin 只能使用文件格式事实：每个 `.1min.bin` 第一个 float32 是 `start_index`，后续值按 `calendar_offset - start_index + 1` 定位。任何 offset、长度、字段、DB 行数、复权因子无法验证时必须 fail-fast。

## 修复目标与非目标

```text
Type       Item                                                          Decision
---------  ------------------------------------------------------------  ------------------------------------------------------------
Goal       修复 DB-present 但正式 Qlib 1min OHLCV/factor 全空的缺口       是
Goal       覆盖全市场缺口，不只覆盖 QE 命中的 486 个 stock-date           是
Goal       修复全字段一致性，不只修 close                                是
Goal       保留备份、checksum、patch manifest、验证报告                   是
Goal       支持 WSL 本机和远端节点一致修复                               是
Non-goal   重跑 QE                                                       否，本计划不重跑
Non-goal   修改策略、Qlib 源码、QE 运行期日志                             否
Non-goal   自动静默创建缺失文件或补 0                                    否，缺字段直接失败
Non-goal   用猜测修复停牌/DB 不完整日期                                  否，只修复 DB 和口径均可验证的交易日
```

## 必须处理的字段

```text
Field                         Action                  Formula / Rule
----------------------------  ----------------------  ------------------------------------------------------------
open.1min.bin                 Patch required           `open_li / 1000 * qfq_factor`
high.1min.bin                 Patch required           `high_li / 1000 * qfq_factor`
low.1min.bin                  Patch required           `low_li / 1000 * qfq_factor`
close.1min.bin                Patch required           `close_li / 1000 * qfq_factor`
volume.1min.bin               Patch required           `volume_hand * 100 / qfq_factor`
amount.1min.bin               Patch required           `amount_li / 1000`
factor.1min.bin               Patch required           `qfq_factor`
limit_up.1min.bin             Patch or verify required  用正式 bin 相邻日反推现有口径后修复；禁止盲目补 0
limit_down.1min.bin           Patch or verify required  用正式 bin 相邻日反推现有口径后修复；禁止盲目补 0
prev_close.1min.bin           Verify first             当前已有值；如与 DB 明显不一致，单独列入修复计划
up_limit_price.1min.bin       Verify first             当前已有值；如与 DB 明显不一致，单独列入修复计划
down_limit_price.1min.bin     Verify first             当前已有值；如与 DB 明显不一致，单独列入修复计划
```

## 总体策略

采用“三段式”：先全量扫描和 dry-run patch plan，再备份和本机 apply，最后远端节点同步/验证。正式写 bin 前必须产出 patch plan 并人工确认。

```text
Stage      Mode       Writes Qlib Bin  Required Output
---------  ---------  ---------------  ------------------------------------------------------------
Stage 1    scan       No               全市场 DB-vs-Qlib 缺口清单、字段缺口矩阵
Stage 2    dry-run    No               patch plan、factor 口径验证、before checksum manifest
Stage 3    apply      Yes              backup、after checksum、direct-bin verify、Qlib API verify
Stage 4    remote     Yes, remote only  remote before/after checksum、节点一致性报告
Stage 5    report     No               docs/analysis 修复报告、skill/脚本说明、Git commit
```

## Phase 1：实现只读全历史 coverage scan

新增手工 CLI，建议路径：`scripts/qe_qlib_minute_bin_repair.py`，先实现只读子命令。

```text
Subcommand      Purpose
--------------  ------------------------------------------------------------
scan            扫描 official calendar 全范围，找 DB-present 但 Qlib OHLCV/factor 全空或部分缺失的 stock-date
build-plan      对 scan 输出生成 patch plan，但不写 bin
verify-plan     校验 patch plan 的 DB 行数、calendar offset、factor 口径、before hash
```

关键校验：

```text
Check                                      Fail Condition
-----------------------------------------  ------------------------------------------------------------
DB minute rows vs Qlib calendar bars        不等于对应日期 calendar bar 数，直接失败
DB `close_li/open_li/...`                   任一必需字段为空，直接失败
DB `adj_factor`                             缺失或无法计算 `qfq_factor`，直接失败
Qlib bin file exists                        任一必修字段文件缺失，直接失败
Qlib bin start_index / array length          任一 offset 越界，直接失败
Current official adjacent-day factor match   与 DB 计算口径不一致，直接失败
Limit flag formula inference                无法与正式 bin 相邻有效日口径匹配，直接失败
```

输出位置：

```text
Artifact                                                                Purpose
----------------------------------------------------------------------  ------------------------------------------------------------
docs/analysis/P0_qlib_minute_bin_gap_scan_YYYYMMDD.csv                  全市场 stock-date 缺口清单
docs/analysis/P0_qlib_minute_bin_gap_field_matrix_YYYYMMDD.csv          每个字段的 before non-null 统计
docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_YYYYMMDD.json       可执行 patch plan；不含大体量分钟值
docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_YYYYMMDD.md         人读审计报告
```

## Phase 2：复权和 limit flag 口径验证

这是直接写 bin 前最关键的风险控制。不能只按脚本公式盲目生成。

```text
Validation                    Method
----------------------------  ------------------------------------------------------------
qfq factor                    对每只待修股票抽取缺口前后正常交易日，比较 DB 计算 factor 与 official `factor.1min.bin`
qfq OHLC close                比较 DB `close_li/1000*qfq_factor` 与 official `close.1min.bin` 相邻正常日
volume                        比较 DB `volume_hand*100/qfq_factor` 与 official `volume.1min.bin` 相邻正常日
amount                        比较 DB `amount_li/1000` 与 official `amount.1min.bin` 相邻正常日
limit_up / limit_down         对相邻正常日同时测试候选公式，选择能匹配 official 的公式；无法唯一匹配则失败
prev/up/down limit price      对问题日验证当前 official 三个价格字段与 DB `stk_limit` 是否一致
```

建议阈值：

```text
Metric                         Threshold
-----------------------------  ------------------------------------------------------------
price/factor absolute diff      <= 1e-5 或 float32 round-trip 等价
volume absolute / relative diff <= max(1e-3, 1e-6 * abs(value))
amount absolute / relative diff <= max(1e-3, 1e-6 * abs(value))
limit price diff                <= 1e-4
```

## Phase 3：备份与可回滚设计

正式 apply 前必须先备份所有即将修改的文件。备份不进 Git，大文件保留在 WSL 数据目录；Git 只提交 manifest 和报告。

```text
Backup Item                              Location Pattern
---------------------------------------  ------------------------------------------------------------
Original bin files                       `/home/lc999/data/qlib_minute_bin_patch_backup_YYYYMMDD_HHMMSS/features/...`
Backup manifest                          `docs/analysis/P0_qlib_minute_bin_patch_backup_manifest_YYYYMMDD.json`
Before checksum                           manifest 内记录 sha256 / size / mtime
Patch plan hash                           manifest 内记录 patch plan sha256
Restore command preview                   报告中列出，不自动执行
```

备份规则：

```text
Rule                                      Requirement
----------------------------------------  ------------------------------------------------------------
Only changed files                        只备份 patch plan 涉及字段文件
Path safety                               备份目标必须在 `/home/lc999/data/qlib_minute_bin_patch_backup_*`
No destructive cleanup                    不自动删除旧备份
Restore                                  需要单独 `restore --confirm-restore`，不得静默回滚
```

## Phase 4：本机 WSL apply

正式写入必须显式确认，例如：

```bash
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && cd /mnt/f/Dev/AIstock && python scripts/qe_qlib_minute_bin_repair.py apply --plan-json docs/analysis/P0_qlib_minute_bin_patch_plan_dry_run_YYYYMMDD.json --backup-root /home/lc999/data/qlib_minute_bin_patch_backup_YYYYMMDD_HHMMSS --confirm-write PATCH_QLIB_MINUTE_BIN"
```

写入策略：

```text
Step  Description
----  ------------------------------------------------------------
1     按 stock + field 分组读取 `.1min.bin`
2     校验文件当前 sha256 与 patch plan before sha256 完全一致
3     对每个 stock-date 找到 calendar offsets
4     写入全部分钟 bar 的全部必修字段
5     写入 temp 文件到同目录
6     校验 temp 文件 size、start_index、目标 offset 非空值
7     `os.replace(temp, target)` 原子替换
8     生成 after sha256 manifest
```

禁止行为：

```text
Forbidden                                Reason
---------------------------------------  ------------------------------------------------------------
直接原地 mmap 写且无备份                  出错难回滚
只 patch close                           字段口径不一致
缺 DB 行时跳过                           静默漏修
offset 越界时扩展 bin                    可能破坏 Qlib 文件结构
缺 adj_factor 时填 1.0                   会破坏复权口径
limit flag 缺失时填 0                    会掩盖涨跌停状态
```

## Phase 5：本机修复后验证

```text
Verification                              Expected Result
----------------------------------------  ------------------------------------------------------------
Direct bin field matrix                   patched stock-date 的 OHLCV/factor 非空数等于 calendar bar 数
Qlib `D.features`                         patched stock-date `$close/$factor` 不再全空
DB value comparison                       patched 值与 DB 计算值在阈值内一致
Existing gap diagnosis rerun              DB-present all-null gap 在修复范围内为 0
QE warning close-none reclassification     原 `QLIB_MINUTE_CLOSE_MISSING` 应消失或显著减少
Non-QE minute smoke                       不启动 QE，只做 Qlib API / 小样本数据读取 smoke
```

报告输出：

```text
Artifact                                                               Purpose
---------------------------------------------------------------------  ------------------------------------------------------------
docs/analysis/P0_qlib_minute_bin_patch_apply_report_YYYYMMDD.md        本机修复结果
docs/analysis/P0_qlib_minute_bin_patch_after_manifest_YYYYMMDD.json     after checksum 与字段统计
docs/analysis/P0_qlib_minute_bin_patch_verify_YYYYMMDD.csv              每个 stock-date 修复后验证行
```

## Phase 6：远端节点同步 / 修复

远端节点必须和 WSL 本机一致，否则分布式 QE 仍可能命中旧缺口。

推荐方式：生成 patch payload，在远端节点执行同一 apply 逻辑；不要直接假设远端文件与本机一致。

```text
Remote Step                              Fail Condition
---------------------------------------  ------------------------------------------------------------
Node inventory                           找不到 QE 可运行节点配置，停止并报告
Remote qlib path check                    `/home/lc999/data/qlib_minute_bin` 不存在，停止
Remote before checksum                    与本机 before checksum 不一致，停止，不强行覆盖
Patch payload transfer                    scp/rsync 失败，停止
Remote apply                              任一字段/offset/sha256 校验失败，停止
Remote after checksum                     与本机 after checksum 不一致，停止
Remote Qlib API smoke                     patched sample 仍全空，停止
```

如果远端 before checksum 不一致，有两种后续路线：

```text
Option  When To Use                                  Action
------  -------------------------------------------  ------------------------------------------------------------
A       远端只是旧但可识别                           远端单独 scan + build-plan + apply
B       远端数据目录应与本机完全一致                 打包/rsync patched files，但必须先备份远端并记录 checksum
```

## Phase 7：Skill 和长期门禁补充

修复工具实现后，应补充到 QE 诊断 skill，避免之后再次只靠抽样 smoke。

```text
Item                                      Update
----------------------------------------  ------------------------------------------------------------
qe-evolution-diagnostics skill            增加 “Qlib minute bin repair workflow”
Project memory                            记录修复脚本、报告、backup path、checksum
Validation gate                           production promotion 前必须跑 DB-vs-Qlib minute coverage
Future data export                         保留 full-market CSV/log/checksum 或至少保留缺口审计结果
```

## 最终验收标准

```text
Gate                                      Pass Criteria
----------------------------------------  ------------------------------------------------------------
No silent fallback                        所有异常均 fail-fast，报告中没有 skipped-but-success
Field completeness                        OHLCV/amount/factor/limit flags 在 patched offset 均完整
Value correctness                         patched 值与 DB 计算值在阈值内一致
Dataset consistency                       WSL 与远端节点 after checksum 一致
Qlib API readability                      `D.features` 对 patched sample 返回非空 `$close/$factor`
Gap diagnosis                             修复范围内 DB-present all-null close gap = 0
Git hygiene                               只提交脚本、文档、manifest 摘要；不提交大型 bin/backup
Runtime safety                            不重启后端、不重跑 QE、不修改 QE 策略行为
```

## 建议执行顺序

```text
Priority  Step
--------  ------------------------------------------------------------
P0        实现 `scan/build-plan/verify-plan`，先只读生成 dry-run patch plan
P0        审查 dry-run 报告，确认缺口范围和字段列表
P0        实现 `apply`，但默认禁写，必须 `--confirm-write PATCH_QLIB_MINUTE_BIN`
P0        本机 WSL 备份 + apply + verify
P0        远端节点 checksum 校验 + apply/同步 + verify
P1        更新 skill、项目记忆、修复报告并提交 GitHub
P1        修复后再运行原数据准确性审计，确认 close-none coverage warning 消失
```

## 当前建议

下一步不要直接写 bin。应先实现只读 scan/build-plan，生成 dry-run patch plan 和字段矩阵，确认需要修复的字段、股票、日期、offset、复权口径全部可验证后，再执行正式 apply。
