# QE Qlib Minute Gap Diagnosis: qe_20260501_011054_c90a

Scope: existing close-none/price audit JSON plus direct Qlib 1min bin inspection. No QE rerun, no Qlib mutation, no strategy logging changes.

## Direct Answer

- Yes, the exact affected stock-date pairs can be listed. This audit found 486 QE-warning stock-date pairs, 157 warning stocks, and 7 affected trading dates.
- Across the current DB minute universe on those 7 dates, 9655 DB-present stock-date pairs have Qlib 1min close all-null.
- The concrete cause is not DB minute absence or suspension for these pairs. The Qlib 1min OHLCV/factor binary files contain NaN at those date offsets, while current DB minute rows and Qlib limit/prev_close binaries are present.
- Therefore the failure is in the Qlib minute OHLCV/factor export/bin snapshot, not in current DB minute storage, Qlib calendar, instrument membership, or limit-price precision.

## Root Cause Classes

```text
Class                                                       Pairs
----------------------------------------------------------  -----
QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT  486
```

## Date-Level Coverage

```text
Date        Pairs  Stocks  DBStocks  DBRows   DBCloseRows  QlibInstRows  QlibInstClose  QlibCloseSlot%  DBQlibGap
----------  -----  ------  --------  -------  -----------  ------------  -------------  --------------  ---------
2025-07-08  140    140     5202      1248480  1248480      5217          2511           48.13%          2691
2025-07-09  127    127     5226      1254240  1254240      5238          2906           55.48%          2320
2025-07-10  73     73      5242      1258080  1258080      5255          3683           70.09%          1559
2025-07-11  59     59      5259      1262160  1262160      5268          3989           75.72%          1270
2025-07-14  42     42      5270      1264800  1264800      5280          4347           82.33%          923
2025-07-15  29     29      5291      1269840  1269840      5300          4701           88.70%          590
2025-07-16  16     16      5310      1274400  1274400      5320          5008           94.14%          302
```

## Top QE-Warning Affected Stocks

```text
Stock      AffectedDates
---------  -------------
688607.SH  7
603396.SH  7
603520.SH  7
688063.SH  7
603937.SH  7
688217.SH  7
300519.SZ  7
301098.SZ  7
300394.SZ  7
688022.SH  7
301257.SZ  7
001360.SZ  7
300700.SZ  7
002578.SZ  7
002486.SZ  6
603909.SH  6
300966.SZ  6
688670.SH  6
688681.SH  6
002883.SZ  6
688189.SH  6
688733.SH  6
688293.SH  6
002015.SZ  6
300854.SZ  6
301309.SZ  6
603185.SH  5
688098.SH  5
688162.SH  5
002622.SZ  5
603223.SH  5
300110.SZ  5
300813.SZ  5
002905.SZ  5
603803.SH  5
300449.SZ  5
605189.SH  5
301359.SZ  5
688238.SH  5
600959.SH  5
```

## Full DB-Present Qlib-Close Gap By Date

```text
Date        DBStocks  DBRows   QlibInstClose  GapPairs  Gap/DBStocks
----------  --------  -------  -------------  --------  ------------
2025-07-08  5202      1248480  2511           2691      51.73%
2025-07-09  5226      1254240  2906           2320      44.39%
2025-07-10  5242      1258080  3683           1559      29.74%
2025-07-11  5259      1262160  3989           1270      24.15%
2025-07-14  5270      1264800  4347           923       17.51%
2025-07-15  5291      1269840  4701           590       11.15%
2025-07-16  5310      1274400  5008           302       5.69%
```

## Sample Bin File MTime Evidence

```text
Stock      Date        CloseBinMTimeUTC                  PrevCloseBinMTimeUTC
---------  ----------  --------------------------------  --------------------------------
603185.SH  2025-07-08  2026-04-29T12:59:12.663200+00:00  2026-04-30T01:01:03.762413+00:00
603421.SH  2025-07-08  2026-04-29T12:59:19.542430+00:00  2026-04-30T01:01:05.125242+00:00
688607.SH  2025-07-08  2026-04-29T12:59:49.443297+00:00  2026-04-30T01:01:12.069971+00:00
688051.SH  2025-07-08  2026-04-29T12:59:34.739253+00:00  2026-04-30T01:01:08.118393+00:00
603396.SH  2025-07-08  2026-04-29T12:59:19.431386+00:00  2026-04-30T01:01:05.065054+00:00
603898.SH  2025-07-08  2026-04-29T12:59:27.346110+00:00  2026-04-30T01:01:05.818350+00:00
605177.SH  2025-07-08  2026-04-29T12:59:31.069590+00:00  2026-04-30T01:01:07.353153+00:00
002486.SZ  2025-07-08  2026-04-29T12:57:24.034784+00:00  2026-04-30T01:00:43.696170+00:00
603385.SH  2025-07-08  2026-04-29T12:59:16.826968+00:00  2026-04-30T01:01:05.000567+00:00
002817.SZ  2025-07-08  2026-04-29T12:57:34.936617+00:00  2026-04-30T01:00:45.413314+00:00
300440.SZ  2025-07-08  2026-04-29T12:57:59.722407+00:00  2026-04-30T01:00:48.367495+00:00
000751.SZ  2025-07-08  2026-04-29T12:56:58.605233+00:00  2026-04-30T01:00:40.215476+00:00
```

## Sample Stock-Date Detail

```text
Stock      Date        Loops                    DBMin  DBLastClose  CalRows  QClose  QFactor  QPrevClose  QUpLimit  Class
---------  ----------  -----------------------  -----  -----------  -------  ------  -------  ----------  --------  ----------------------------------------------------------
603185.SH  2025-07-08  19,25,26                 240    16.39        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603421.SH  2025-07-08  19,22                    240    6.93         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688607.SH  2025-07-08  19,20,21,22,23           240    18.82        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688051.SH  2025-07-08  19,23,24,25              240    24.44        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603396.SH  2025-07-08  19,25                    240    27.25        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603898.SH  2025-07-08  19,20,21,28              240    10.93        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
605177.SH  2025-07-08  19,20,21,22,23           240    19.44        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002486.SZ  2025-07-08  19,21,22,23,25,27,28     240    2.74         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603385.SH  2025-07-08  19,20,21,22,23,28        240    6.83         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002817.SZ  2025-07-08  19,20,21,23,25,28        240    7.59         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300440.SZ  2025-07-08  19,22,26                 240    10.17        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000751.SZ  2025-07-08  19,20,21,22,23,28        240    3.21         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300787.SZ  2025-07-08  19                       240    13.42        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688680.SH  2025-07-08  19,24,26                 240    46.99        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300006.SZ  2025-07-08  19,20,21,22,23,24,26,28  240    3.86         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000050.SZ  2025-07-08  19                       240    8.61         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688098.SH  2025-07-08  19,20,21,23,27,28        240    6.16         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002373.SZ  2025-07-08  19                       240    9.33         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002524.SZ  2025-07-08  19,22,23                 240    4.54         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000931.SZ  2025-07-08  19,22,25                 240    5.49         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603808.SH  2025-07-08  19,21,22,23,24,25,28     240    8.24         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688718.SH  2025-07-08  19,20,21,23,28           240    12.4         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
600626.SH  2025-07-08  19,20,28                 240    3.67         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002160.SZ  2025-07-08  19,22                    240    4.34         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603989.SH  2025-07-08  19,20,23,28              240    15.7         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603520.SH  2025-07-08  19,25                    240    10.06        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603122.SH  2025-07-08  19                       240    6.97         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
600661.SH  2025-07-08  19,20,21,22,23,24        240    10.88        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688296.SH  2025-07-08  19,20,21,22,23,25        240    13.23        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000586.SZ  2025-07-08  19,20,21,23,24           240    11.14        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603458.SH  2025-07-08  19,22,24,25,26           240    8.71         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688162.SH  2025-07-08  19,22,26                 240    26.97        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002795.SZ  2025-07-08  19                       240    5.29         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603909.SH  2025-07-08  19,20,21,23,28           240    9.96         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603177.SH  2025-07-08  19,20,21,23,28           240    9.25         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603185.SH  2025-07-09  19,25,26                 240    17.55        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603421.SH  2025-07-09  19,22                    240    7.01         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688607.SH  2025-07-09  19,20,21,22,23           240    18.68        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688051.SH  2025-07-09  19,23,24,25              240    24.45        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603396.SH  2025-07-09  19,22,25                 240    26.92        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603898.SH  2025-07-09  19,20,21,28              240    10.92        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
605177.SH  2025-07-09  19,20,21,22,23           240    19.17        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002486.SZ  2025-07-09  19,21,22,23,24,25,27,28  240    2.73         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603385.SH  2025-07-09  19,20,21,22,23,28        240    6.89         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002817.SZ  2025-07-09  19,20,21,23,25,28        240    7.56         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300440.SZ  2025-07-09  19,22,26                 240    10.2         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000751.SZ  2025-07-09  19,20,21,22,23,28        240    3.18         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300787.SZ  2025-07-09  19                       240    13.3         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300006.SZ  2025-07-09  19,20,21,22,23,24,26,28  240    3.88         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000050.SZ  2025-07-09  19                       240    8.6          240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688098.SH  2025-07-09  19,20,21,23,27,28        240    6.19         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002373.SZ  2025-07-09  19                       240    9.32         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002524.SZ  2025-07-09  19,22,23                 240    4.56         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000931.SZ  2025-07-09  19,22,25                 240    5.44         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603808.SH  2025-07-09  19,21,22,23,24,25,28     240    8.43         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688718.SH  2025-07-09  19,20,21,23,28           240    12.83        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002160.SZ  2025-07-09  19,22                    240    4.3          240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603989.SH  2025-07-09  19,20,23,28              240    15.64        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603520.SH  2025-07-09  19,25                    240    9.97         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603122.SH  2025-07-09  19,20,28                 240    7.0          240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
600661.SH  2025-07-09  19,20,21,22,23,24        240    10.97        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688296.SH  2025-07-09  19,20,21,22,23,25        240    13.41        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603458.SH  2025-07-09  19,22,24,25,26           240    9.31         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688162.SH  2025-07-09  19,22,26                 240    26.46        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002795.SZ  2025-07-09  19                       240    5.31         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603909.SH  2025-07-09  19,20,21,23,28           240    9.93         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
301060.SZ  2025-07-09  19,20,23,24,28           240    10.11        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002821.SZ  2025-07-09  19                       240    88.27        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603185.SH  2025-07-10  19,25,26                 240    19.31        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002524.SZ  2025-07-10  19,22,23                 240    4.59         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688607.SH  2025-07-10  19,20,21,22,23           240    18.7         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688718.SH  2025-07-10  19,20,21,23,28           240    13.09        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
688051.SH  2025-07-10  19,23,24,25              240    24.64        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603396.SH  2025-07-10  19,22,25                 240    26.87        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
002486.SZ  2025-07-10  19,21,22,23,24,25,27,28  240    2.74         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
605177.SH  2025-07-10  19,20,21,22,23           240    19.21        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603520.SH  2025-07-10  19,25                    240    10.18        240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
300787.SZ  2025-07-10  19                       240    13.2         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
603122.SH  2025-07-10  19                       240    7.01         240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
000050.SZ  2025-07-10  19                       240    8.7          240      0       0        240         240       QLIB_OHLCV_FACTOR_BIN_ALL_NAN_DB_PRESENT_LIMIT_BIN_PRESENT
```

Full QE-warning stock-date list: `docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_stock_dates_20260502.csv`
Full DB-present Qlib-close gap list: `docs/analysis/P0_qe_20260501_011054_c90a_qlib_minute_gap_all_db_present_stock_dates_20260502.csv`

## Why This Happened

Evidence chain:

- Current DB minute rows exist for the affected dates and have non-null `close_li`.
- Qlib 1min calendar rows exist for the same dates.
- Qlib instrument rows exist; otherwise `D.features` would not return 240 rows per affected stock-date.
- Direct bin inspection shows `open/high/low/close/volume/amount/factor` are all NaN for the affected stock-date offsets.
- Direct bin inspection also shows `prev_close/up_limit_price/down_limit_price` are present for the same offsets, proving this is not a total instrument/calendar gap.

The precise local root cause is therefore: the current `/home/lc999/data/qlib_minute_bin` OHLCV/factor files were built from an incomplete minute OHLCV/factor export snapshot for 2025-07-08 through 2025-07-16. Current DB data is more complete than that Qlib minute snapshot. To identify the historical operational cause beyond this file-level proof, the missing export job log or preserved CSV snapshot would be required; those inputs are not present in the existing QE artifacts.
