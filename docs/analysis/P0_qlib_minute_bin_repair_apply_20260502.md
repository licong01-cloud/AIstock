# P0 Qlib Minute Bin Repair Apply

This command wrote verified Qlib 1min bin offsets after pre-apply checksum validation and backup creation.

## Summary

```text
Metric                 Value
---------------------  ---------------------------------------------------------------
records                9655
stocks                 2696
patched_files          24264
patched_values         20854800
readback_max_abs_diff  0.0
backup_root            /home/lc999/data/qlib_minute_bin_backup_direct_repair_20260502_
backup_file_count      24264
backup_total_bytes     12962997064
```

## Patched Values By Field

```text
Field       Values
----------  -------
amount      2317200
close       2317200
factor      2317200
high        2317200
limit_down  2317200
limit_up    2317200
low         2317200
open        2317200
volume      2317200
```

Apply JSON: `docs/analysis/P0_qlib_minute_bin_repair_apply_20260502.json`
Backup manifest: `/home/lc999/data/qlib_minute_bin_backup_direct_repair_20260502_/backup_manifest.json`
