# QE Dynamic Factor Truncation Audit: qe_20260501_011054_c90a

Scope: targeted dynamic no-future-leakage check. Factor scripts are run in a temp workspace; source workspaces are not modified.

## P0 Dynamic Truncation Summary

```text
Loop  Factor                      Cutoff      Compared  Mismatch  MaxAbsDiff  Status
----  --------------------------  ----------  --------  --------  ----------  ------
19    m_conditional_momentum_20d  2025-12-31  13946     0         0.000e+00   PASS  
19    m_turnover_zscore_60d       2025-12-31  13930     0         0.000e+00   PASS  
```

## P0 Date-Level Details

```text
Loop  Factor                      Date        Common  MaxAbsDiff  Mismatch
----  --------------------------  ----------  ------  ----------  --------
19    m_conditional_momentum_20d  2025-12-29  4650    0.000e+00   0       
19    m_conditional_momentum_20d  2025-12-30  4647    0.000e+00   0       
19    m_conditional_momentum_20d  2025-12-31  4649    0.000e+00   0       
19    m_turnover_zscore_60d       2025-12-29  4644    0.000e+00   0       
19    m_turnover_zscore_60d       2025-12-30  4642    0.000e+00   0       
19    m_turnover_zscore_60d       2025-12-31  4644    0.000e+00   0       
```

## Evidence Notes

- PASS means the selected factor values on audited dates are identical after input data is truncated to the cutoff.
- This targeted dynamic audit complements the static leakage scan; it is not a full recompute of every factor unless all factors are explicitly requested.
