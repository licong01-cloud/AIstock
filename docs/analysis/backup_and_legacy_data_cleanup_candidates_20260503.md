# 备份与旧数据清理候选确认清单（2026-05-03）

## 目的

本文档记录当前已扫描到的备份、旧版本、候选导出和同步备份目录，供后续人工确认后删除。当前仅记录，不执行删除。


## 2026-05-03 补充确认

本次按“当前先不执行删除”的要求，只补充记录和验证，不再删除任何文件或目录。下方 `DELETED` 区块为此前已经执行过的低风险清理追溯记录，不代表本次继续删除；后续若要删除候选数据，必须逐项确认后再执行。

## 此前已删除的低风险产物（追溯记录）

```text
Status   Size Before  Path
-------  -----------  --------------------------------------------------------------------------
DELETED  0.002 MB     F:\Dev\AIstock\qlib_bin\qlib_daily_auth_smoke_20260503
DELETED  0.264 MB     F:\Dev\AIstock\qlib_bin\qlib_minute_auth_smoke_20260503
DELETED  36 KB        /home/lc999/data/qlib_bin_20260428_shsz_candidate_validation_calendar
DELETED  73 GB        /home/lc999/data/qlib_csv_authoritative/qlib_minute_authoritative_full_20260428
```

## 继续保留的 active provider

```text
Status   Path
-------  ---------------------------------
KEPT     /home/lc999/data/qlib_minute_bin
KEPT     /home/lc999/data/qlib_bin
```

## Qlib / 数据导出相关候选

```text
位置      大小       路径                                                                      初步判断
--------  ---------  ------------------------------------------------------------------------  --------------------------------------------
Windows   254 MB     F:\Dev\AIstock\qlib_bin\backup_20260123_212437                            明确备份目录，非当前 active provider
Windows   330 MB     F:\Dev\AIstock\qlib_bin\qlib_bin_20260311                                  旧日线 bin，确认无回滚需求后可删
Windows   315 MB     F:\Dev\AIstock\qlib_bin\qlib_bin_20260428_shsz_candidate                   候选日线导出，等新权威版本完成后可删
Windows   0.15 MB    F:\Dev\AIstock\qlib_bin\qlib_bin_20260311\instruments\all.txt.bak          旧 all.txt 备份
WSL       399 MB     /home/lc999/data/qlib_bin_20260428_shsz_candidate                          候选日线导出，非 active provider
WSL       32 GB      /home/lc999/data/qlib_minute_authoritative_full_20260428                   旧分钟线导出，非 active provider，但体积较大
WSL       622 MB     /home/lc999/data/qlib_snapshots/daily_pv.h5.bak                            旧 H5 快照备份
Remote    39 GB      /home/lc999/data/_sync_backups                                             远端同步备份，体积最大
```

## 远端 _sync_backups 明细

```text
大小       路径
---------  ------------------------------------------------------------------------
424 MB     /home/lc999/data/_sync_backups/20260430_142304/qlib_bin
30 GB      /home/lc999/data/_sync_backups/20260430_142304/qlib_minute_bin
3.1 GB     /home/lc999/data/_sync_backups/20260430_142304/factor_data_project
5.5 GB     /home/lc999/data/_sync_backups/20260430_142304/factor_data_node
15 MB      /home/lc999/data/_sync_backups/20260430_142304/factor_data_debug_project
4 KB       /home/lc999/data/_sync_backups/20260430_143337_continue
```

## 因子 / 模型相关候选

```text
位置      大小       路径                                                                      初步判断
--------  ---------  ------------------------------------------------------------------------  --------------------------------------------
Windows   2.9 GB     F:\Dev\AIstock\factors\combined_static_factors.parquet.backup              静态因子备份，确认无需回滚后可删
Windows   1.1 GB     F:\Dev\AIstock\factors\combined_static_factors.parquet.old                 静态因子旧版本，确认无需回滚后可删
WSL       3.0 GB     /home/lc999/data/aistock_factors/combined_static_factors.parquet.backup    静态因子备份，确认无需回滚后可删
WSL       1.1 GB     /home/lc999/data/aistock_factors/combined_static_factors.parquet.old       静态因子旧版本，确认无需回滚后可删
WSL       495 MB     /home/lc999/data/rl_models/v25/v25_predictions.pkl.old                     V25 预测旧文件，确认无需复现旧实验后可删
WSL       495 MB     /home/lc999/data/rl_models/v25/v25_final_predictions.pkl.old               V25 预测旧文件，确认无需复现旧实验后可删
WSL       0.7 MB     /home/lc999/data/rl_models/v24/v24_plan_net_apr24_backup.pt                V24 模型备份，谨慎删除
WSL       0.7 MB     /home/lc999/data/rl_models/v24/v24_plan_net_v1_backup_20260425_231633.pt   V24 模型备份，谨慎删除
```

## 代码 / 模板备份候选

```text
大小       路径                                                                      初步判断
---------  ------------------------------------------------------------------------  --------------------------------------------
39.6 MB    F:\Dev\AIstock\tdx-api-main\.git.bak                                      Git 目录备份，删除前需确认 tdx-api-main 当前仓库完整
2.2 MB     F:\Dev\AIstock\qlib_src_backup                                            Qlib 源码备份，确认无需对比旧源码后可删
0.55 MB    F:\Dev\AIstock\scripts\backups                                            脚本修复备份，确认无需回滚后可删
0.10 MB    F:\Dev\AIstock\template_backups                                           模板备份，确认无需回滚后可删
0.06 MB    F:\Dev\AIstock\frontend\src\app\quantevolver\compose\page.tsx.bak          前端页面备份，确认当前页面可用后可删
0.02 MB    F:\Dev\AIstock\scripts\qrun_limit_minute.py.backup                        脚本备份，确认当前脚本已提交后可删
0.02 MB    F:\Dev\AIstock\rdagent_assets\qe_experiments\qe_20260426_142629\qrun_limit_minute.py.old  实验脚本旧版本，谨慎删除
0.01 MB    F:\Dev\AIstock\rl_execution\executor\v25_two_stage_executor.py.backup      V25 执行器备份，确认当前执行器已提交后可删
```

## 推荐后续删除顺序

```text
优先级  可释放空间  路径                                                     删除前置条件
------  ----------  -------------------------------------------------------  --------------------------------------------
P0      39 GB       远端 /home/lc999/data/_sync_backups                      远端当前 qlib_bin/qlib_minute_bin 已验证且无需回滚
P0      32 GB       WSL /home/lc999/data/qlib_minute_authoritative_full_20260428  新 SH/SZ non-ST 权威分钟线 bin 完成并验证通过
P1      4.1 GB      Windows factors .backup/.old                              当前静态因子文件已验证且无需历史回滚
P1      4.1 GB      WSL aistock_factors .backup/.old                          当前 WSL 静态因子文件已验证且无需历史回滚
P1      1.0 GB      WSL rl_models/v25/*.old                                   无需复现旧 V25 预测文件
P2      1.4 GB      Windows/WSL qlib_bin 旧日线候选和备份                      新权威日线 bin 完成并验证通过
```

## 注意事项

- `bak_basic` 是 Tushare 数据集名称，不是备份文件，不能按 `bak` 关键字误删。
- 当前删除动作必须逐项确认；不要删除 active provider：`/home/lc999/data/qlib_minute_bin` 与 `/home/lc999/data/qlib_bin`。
- 大型数据删除前应确认对应新权威数据已完成 DB-vs-bin、CSV-vs-bin、Qlib smoke 回测和 UI/API 路径验证。
