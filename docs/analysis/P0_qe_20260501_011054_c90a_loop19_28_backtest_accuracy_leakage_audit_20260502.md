# P0 QE Loop Audit: qe_20260501_011054_c90a Loop19+ Backtest Accuracy and Leakage Checks

Scope: read-only audit of existing completed loop artifacts. Capital-size, capacity, and impact-cost assumptions are explicitly out of scope for this report.

## 1. Loop Summary

```text
Loop  H   Feat  IC       RankIC   CAGR      Sharpe   MDD       Cash    
----  --  ----  -------  -------  --------  -------  --------  --------
19    20  77     0.0679   0.1311    73.78%    2.096   -17.60%    11.82%
20    10  77     0.0570   0.1066    68.72%    1.978   -18.16%     6.87%
21    10  77     0.0575   0.1027    66.84%    1.942   -18.99%     6.37%
22    20  77     0.0739   0.1367    75.61%    2.074   -18.57%    12.82%
23    5   77     0.0506   0.0905    71.96%    2.059   -17.55%     5.45%
24    10  77     0.0659   0.1002    81.14%    2.028   -19.37%    11.12%
25    10  77     0.0822   0.1201    75.80%    1.937   -20.09%     9.00%
26    10  77     0.0638   0.1002    93.41%    2.334   -17.72%    10.56%
27    10  77     0.0800   0.1124    73.69%    1.858   -21.32%     4.48%
28    5   77     0.0487   0.0864    65.51%    1.979   -17.12%     4.11%
```

## 2. Backtest and Signal Statistic Accuracy

Tolerance target: IC/RankIC recomputation should be near machine precision versus Qlib artifacts; report return should match account pct_change.

```text
Loop  ICMaxDiff     RICMaxDiff    ICEnhanced    RICEnhanced   RetAcctDiff   FinalValueDiff
----  ------------  ------------  ------------  ------------  ------------  --------------
19        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      1.88e-03  
20        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      9.83e-04  
21        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      1.44e-03  
22        0.00e+00      0.00e+00      1.39e-17      2.78e-17      1.10e-16      2.87e-03  
23        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      1.55e-03  
24        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      1.15e-03  
25        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      4.12e-03  
26        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      4.06e-03  
27        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      2.82e-03  
28        0.00e+00      0.00e+00      0.00e+00      0.00e+00      1.11e-16      4.24e-03  
```

## 2b. Label Horizon Date Alignment

Signal IC dates should be shorter than report dates by label horizon because the last H days do not have future-H labels.

```text
Loop  H   SignalDates  ReportRows  Gap  Expected  Status
----  --  -----------  ----------  ---  --------  ------
19    20  422          442         20   20        OK    
20    10  432          442         10   10        OK    
21    10  432          442         10   10        OK    
22    20  422          442         20   20        OK    
23    5   437          442         5    5         OK    
24    10  432          442         10   10        OK    
25    10  432          442         10   10        OK    
26    10  432          442         10   10        OK    
27    10  432          442         10   10        OK    
28    5   437          442         5    5         OK    
```

## 3. Signal-to-Return Top Bucket Conversion

```text
Loop  Top50      Bottom50   T50-B50    D1         D10        D1-D10     LSWin    
----  ---------  ---------  ---------  ---------  ---------  ---------  ---------
19        5.32%      0.27%      5.05%      4.75%      2.27%      2.48%     69.67%
20        2.57%     -0.70%      3.26%      2.28%      0.70%      1.58%     71.53%
21        2.46%     -0.76%      3.22%      2.28%      0.65%      1.63%     72.22%
22        5.55%      0.60%      4.95%      4.90%      2.09%      2.81%     68.25%
23        1.26%     -0.93%      2.18%      1.18%      0.19%      0.99%     70.48%
24        3.11%     -0.72%      3.83%      2.61%      0.66%      1.94%     75.00%
25        2.89%     -1.62%      4.51%      2.66%      0.52%      2.14%     76.62%
26        3.02%     -0.71%      3.73%      2.53%      0.75%      1.78%     72.22%
27        2.74%     -1.47%      4.21%      2.62%      0.48%      2.14%     78.01%
28        1.24%     -0.95%      2.19%      1.14%      0.20%      0.94%     67.73%
```

## 4. Static Leakage Scan

```text
Loop  Files  Hits  ByPattern
----  -----  ----  ---------
19    59     0     {}       
20    59     0     {}       
21    59     0     {}       
22    59     0     {}       
23    59     0     {}       
24    59     0     {}       
25    58     0     {}       
26    59     0     {}       
27    58     0     {}       
28    59     0     {}       
```

Important: static scan findings are risk flags, not final proof. Dynamic truncation recompute is still required for any flagged factor before final acceptance.

## 5. Year Segment Snapshot

```text
Loop  Year  Days  IC       RankIC   Return     Sharpe   MDD      
----  ----  ----  -------  -------  ---------  -------  ---------
19    2024  125    0.0840   0.1282     44.02%    2.068    -12.85%
19    2025  243    0.0675   0.1347     61.97%    2.222    -14.01%
19    2026  54     0.0319   0.1212      9.84%    1.866    -10.97%
20    2024  125    0.0797   0.1122     41.76%    2.013    -13.22%
20    2025  243    0.0563   0.1134     63.29%    2.210    -13.94%
20    2026  64     0.0152   0.0699      6.18%    0.994    -12.86%
21    2024  125    0.0757   0.1048     39.12%    1.927    -14.12%
21    2025  243    0.0578   0.1084     62.05%    2.213    -14.07%
21    2026  64     0.0205   0.0765      7.07%    1.063    -13.62%
22    2024  125    0.0928   0.1395     39.97%    1.864    -13.02%
22    2025  243    0.0729   0.1385     71.75%    2.408    -13.17%
22    2026  54     0.0344   0.1224      8.53%    1.620    -11.28%
23    2024  125    0.0726   0.0986     53.71%    2.492    -12.36%
23    2025  243    0.0492   0.0958     54.44%    1.973    -14.78%
23    2026  69     0.0156   0.0575      8.65%    1.247    -12.71%
24    2024  125    0.0875   0.1210     43.78%    1.763    -15.16%
24    2025  243    0.0654   0.0986     78.57%    2.580    -14.02%
24    2026  64     0.0259   0.0656      8.26%    1.362    -12.32%
25    2024  125    0.1126   0.1381     45.04%    1.932    -14.31%
25    2025  243    0.0804   0.1231     68.71%    2.121    -17.31%
25    2026  64     0.0300   0.0732      8.42%    1.284    -12.04%
26    2024  125    0.0919   0.1293     45.11%    1.881    -13.49%
26    2025  243    0.0594   0.0922     87.28%    2.874    -11.72%
26    2026  64     0.0257   0.0734     16.52%    2.666     -9.65%
27    2024  125    0.1075   0.1298     49.07%    1.984    -16.18%
27    2025  243    0.0786   0.1147     64.42%    2.064    -18.13%
27    2026  64     0.0314   0.0696      4.98%    0.776    -14.60%
28    2024  125    0.0740   0.1015     49.52%    2.397    -12.23%
28    2025  243    0.0463   0.0898     50.31%    1.915    -14.33%
28    2026  69     0.0114   0.0471      7.54%    1.147    -11.65%
```

## 6. Key Findings

- IC/RankIC were recomputed from pred.pkl and label.pkl and compared with Qlib sig_analysis artifacts.
- Portfolio daily return was recomputed from account pct_change and compared with report_normal_1day return.
- Top bucket statistics test whether high RankIC is converted into Top50 returns, rather than only full-universe ranking quality.
- Static leakage scan currently checks code-level high-risk patterns; dynamic truncation recompute remains the required next step for final leakage clearance.
- Current artifact-level data statistics are internally consistent: IC/RankIC recomputation, enhanced summary values, account returns, and final account values match their source artifacts.
- Label-horizon date gaps match expected horizons for Loop19-28, which supports that 5D/10D/20D labels are being applied in these alpha-enabled loops.
- No high-risk future-function pattern was found in factor/model/prepare code by static scan; this is not a final leakage clearance until dynamic truncation recompute is run.
- This report intentionally excludes the future capital-size scenario; capacity/impact will be audited only after a dedicated capital experiment exists.
