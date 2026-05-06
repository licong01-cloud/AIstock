# Latest HMM 1Y ??????????? - qe_20260502_131502_9b54

## ??
- HMM ???L3/L4 latest dynamic PUP?snapshot `d11dc38e-84f0-4e5c-80e7-42cb5d978d40`?
- ?????`2025-04-28` ~ `2026-04-27`?? `242` ???????
- ????????? Qlib `pred.pkl` ????????HMM ???????? score overlay???????????????????????
- ?????? HMM ?? `coeff(t)` ???????? `sw_daily.pct_change(t+1)`????? RankIC ? top/bottom ???

## ???? 1Y ??
```text
Rule                           AvgN   Mean1D   Comp1D   Ann1D    Win1D   Mean5D   Mean10D  Mean20D  ExRet10DvsRaw
-----------------------------  -----  -------  -------  -------  ------  -------  -------  -------  -------------
raw_top50                       50.0    0.27%   89.26%   94.31%  59.09%    1.31%    2.40%    4.53%          0.00%
hmm_adjusted_top50              50.0    0.27%   88.15%   93.13%  59.92%    1.29%    2.37%    4.49%         -0.03%
hmm_coeff_gt_1_raw_top50        50.0    0.27%   87.72%   92.67%  61.98%    1.25%    2.29%    4.39%         -0.12%
hmm_top10pct_sector_raw_top50   50.0    0.23%   70.45%   74.24%  59.50%    1.20%    2.15%    4.16%         -0.26%
hmm_top20pct_sector_raw_top50   50.0    0.24%   74.52%   78.59%  58.68%    1.20%    2.07%    3.91%         -0.33%
```

## ???? next-day ??
```text
Days                    242
Mean RankIC             -0.0009
RankIC t-stat           -0.09
RankIC positive ratio   50.83%
Mean Pearson            0.0046
Top20 next 1D mean      0.1202%
Bottom20 next 1D mean   0.1066%
Top-Bottom mean 1D      0.0136%
Top-Bottom win ratio    53.31%
Top-Bottom t-stat       0.40
Top20 compound 1D       32.55%
Bottom20 compound 1D    27.63%
```

### ????????/??
```text
Worst months by top-bottom sum
2025-08 days=21 ls_sum=-2.59% ls_mean=-0.1235% win=52.38%
2025-04 days= 3 ls_sum=-1.67% ls_mean=-0.5573% win=0.00%
2026-01 days=20 ls_sum=-1.51% ls_mean=-0.0756% win=55.00%
2025-09 days=22 ls_sum=-1.02% ls_mean=-0.0462% win=40.91%
2025-12 days=23 ls_sum=-0.92% ls_mean=-0.0402% win=47.83%
2025-11 days=20 ls_sum=-0.78% ls_mean=-0.0389% win=50.00%
Best months by top-bottom sum
2025-07 days=23 ls_sum=0.69% ls_mean=0.0298% win=60.87%
2025-06 days=20 ls_sum=0.84% ls_mean=0.0421% win=45.00%
2025-05 days=19 ls_sum=1.53% ls_mean=0.0805% win=57.89%
2025-10 days=17 ls_sum=2.60% ls_mean=0.1530% win=64.71%
2026-02 days=14 ls_sum=2.83% ls_mean=0.2022% win=57.14%
2026-03 days=22 ls_sum=3.52% ls_mean=0.1598% win=72.73%
```

## ????
- latest HMM adjusted Top50 ? 1Y ???? raw Top50 ? 10D ???????????????????????
- ?? next-day ????????? RankIC/top-bottom??? latest HMM ??????????????????
- ??????????????????? Qlib/QE ????????????????????????????????????

## ??
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\stock_subset_forward_returns.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\stock_subset_daily_returns.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\stock_subset_summary.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\sector_rotation_next_day_rows.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\sector_rotation_daily_ic.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\sector_rotation_bucket_returns.csv`
- `F:\Dev\AIstock\.codex_tmp\hmm_offline_diag\qe_20260502_131502_9b54\rough_one_year_check\sector_rotation_monthly_bucket_returns.csv`
