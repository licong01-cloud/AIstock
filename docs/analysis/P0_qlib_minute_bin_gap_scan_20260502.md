# P0 Qlib Minute Bin Gap Scan

This is a read-only scan. It did not modify Qlib bin files.

## Summary

```text
Metric                Value
--------------------  ----------------------------
pairs                 9655
stocks                2696
dates                 7
date_range            ['2025-07-08', '2025-07-16']
patchable_candidates  9655
```

## By Date

```text
Date        Pairs  Stocks  PatchableCandidates
----------  -----  ------  -------------------
2025-07-08  2691   2691    2691
2025-07-09  2320   2320    2320
2025-07-10  1559   1559    1559
2025-07-11  1270   1270    1270
2025-07-14  923    923     923
2025-07-15  590    590     590
2025-07-16  302    302     302
```

## Missing Patch Fields

```text
Field       StockDatePairs
----------  --------------
open        9655
high        9655
low         9655
close       9655
volume      9655
amount      9655
factor      9655
limit_up    9655
limit_down  9655
```

Scan CSV: `docs/analysis/P0_qlib_minute_bin_gap_scan_20260502.csv`
Field matrix CSV: `docs/analysis/P0_qlib_minute_bin_gap_field_matrix_20260502.csv`
