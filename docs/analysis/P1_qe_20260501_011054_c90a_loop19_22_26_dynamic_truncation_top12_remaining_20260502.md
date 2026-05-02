# QE Dynamic Factor Truncation Audit: qe_20260501_011054_c90a

Scope: targeted dynamic no-future-leakage check. Factor scripts are run in a temp workspace; source workspaces are not modified.

## P0 Dynamic Truncation Summary

```text
Loop  Factor                              Cutoff      Compared  Mismatch  MaxAbsDiff  Status
----  ----------------------------------  ----------  --------  --------  ----------  ------
19    Price_ChipNormalized_Position       2025-12-31  13985     0         0.000e+00   PASS
19    small_order_flow_intensity          2025-12-31  13985     0         0.000e+00   PASS
19    Fundamental_Liquidity_Cross_Factor  2025-12-31  13719     0         0.000e+00   PASS
19    m_idio_vol_60d                      2025-12-31  13954     0         0.000e+00   PASS
19    m_turnover_mf_divergence            2025-12-31  13977     0         0.000e+00   PASS
19    m_mom_weighted_strength_20d         2025-12-31  13936     0         0.000e+00   PASS
22    Price_ChipNormalized_Position       2025-12-31  13985     0         0.000e+00   PASS
22    small_order_flow_intensity          2025-12-31  13985     0         0.000e+00   PASS
22    Fundamental_Liquidity_Cross_Factor  2025-12-31  13719     0         0.000e+00   PASS
22    m_idio_vol_60d                      2025-12-31  13954     0         0.000e+00   PASS
22    m_turnover_mf_divergence            2025-12-31  13977     0         0.000e+00   PASS
22    m_mom_weighted_strength_20d         2025-12-31  13936     0         0.000e+00   PASS
26    Price_ChipNormalized_Position       2025-12-31  13985     0         0.000e+00   PASS
26    small_order_flow_intensity          2025-12-31  13985     0         0.000e+00   PASS
26    Fundamental_Liquidity_Cross_Factor  2025-12-31  13719     0         0.000e+00   PASS
26    m_idio_vol_60d                      2025-12-31  13954     0         0.000e+00   PASS
26    m_turnover_mf_divergence            2025-12-31  13977     0         0.000e+00   PASS
26    m_mom_weighted_strength_20d         2025-12-31  13936     0         0.000e+00   PASS
```

## P0 Date-Level Details

```text
Loop  Factor                              Date        Common  MaxAbsDiff  Mismatch
----  ----------------------------------  ----------  ------  ----------  --------
19    Price_ChipNormalized_Position       2025-12-29  4661    0.000e+00   0
19    Price_ChipNormalized_Position       2025-12-30  4661    0.000e+00   0
19    Price_ChipNormalized_Position       2025-12-31  4663    0.000e+00   0
19    small_order_flow_intensity          2025-12-29  4661    0.000e+00   0
19    small_order_flow_intensity          2025-12-30  4661    0.000e+00   0
19    small_order_flow_intensity          2025-12-31  4663    0.000e+00   0
19    Fundamental_Liquidity_Cross_Factor  2025-12-29  4574    0.000e+00   0
19    Fundamental_Liquidity_Cross_Factor  2025-12-30  4572    0.000e+00   0
19    Fundamental_Liquidity_Cross_Factor  2025-12-31  4573    0.000e+00   0
19    m_idio_vol_60d                      2025-12-29  4651    0.000e+00   0
19    m_idio_vol_60d                      2025-12-30  4651    0.000e+00   0
19    m_idio_vol_60d                      2025-12-31  4652    0.000e+00   0
19    m_turnover_mf_divergence            2025-12-29  4660    0.000e+00   0
19    m_turnover_mf_divergence            2025-12-30  4658    0.000e+00   0
19    m_turnover_mf_divergence            2025-12-31  4659    0.000e+00   0
19    m_mom_weighted_strength_20d         2025-12-29  4645    0.000e+00   0
19    m_mom_weighted_strength_20d         2025-12-30  4645    0.000e+00   0
19    m_mom_weighted_strength_20d         2025-12-31  4646    0.000e+00   0
22    Price_ChipNormalized_Position       2025-12-29  4661    0.000e+00   0
22    Price_ChipNormalized_Position       2025-12-30  4661    0.000e+00   0
22    Price_ChipNormalized_Position       2025-12-31  4663    0.000e+00   0
22    small_order_flow_intensity          2025-12-29  4661    0.000e+00   0
22    small_order_flow_intensity          2025-12-30  4661    0.000e+00   0
22    small_order_flow_intensity          2025-12-31  4663    0.000e+00   0
22    Fundamental_Liquidity_Cross_Factor  2025-12-29  4574    0.000e+00   0
22    Fundamental_Liquidity_Cross_Factor  2025-12-30  4572    0.000e+00   0
22    Fundamental_Liquidity_Cross_Factor  2025-12-31  4573    0.000e+00   0
22    m_idio_vol_60d                      2025-12-29  4651    0.000e+00   0
22    m_idio_vol_60d                      2025-12-30  4651    0.000e+00   0
22    m_idio_vol_60d                      2025-12-31  4652    0.000e+00   0
22    m_turnover_mf_divergence            2025-12-29  4660    0.000e+00   0
22    m_turnover_mf_divergence            2025-12-30  4658    0.000e+00   0
22    m_turnover_mf_divergence            2025-12-31  4659    0.000e+00   0
22    m_mom_weighted_strength_20d         2025-12-29  4645    0.000e+00   0
22    m_mom_weighted_strength_20d         2025-12-30  4645    0.000e+00   0
22    m_mom_weighted_strength_20d         2025-12-31  4646    0.000e+00   0
26    Price_ChipNormalized_Position       2025-12-29  4661    0.000e+00   0
26    Price_ChipNormalized_Position       2025-12-30  4661    0.000e+00   0
26    Price_ChipNormalized_Position       2025-12-31  4663    0.000e+00   0
26    small_order_flow_intensity          2025-12-29  4661    0.000e+00   0
26    small_order_flow_intensity          2025-12-30  4661    0.000e+00   0
26    small_order_flow_intensity          2025-12-31  4663    0.000e+00   0
26    Fundamental_Liquidity_Cross_Factor  2025-12-29  4574    0.000e+00   0
26    Fundamental_Liquidity_Cross_Factor  2025-12-30  4572    0.000e+00   0
26    Fundamental_Liquidity_Cross_Factor  2025-12-31  4573    0.000e+00   0
26    m_idio_vol_60d                      2025-12-29  4651    0.000e+00   0
26    m_idio_vol_60d                      2025-12-30  4651    0.000e+00   0
26    m_idio_vol_60d                      2025-12-31  4652    0.000e+00   0
26    m_turnover_mf_divergence            2025-12-29  4660    0.000e+00   0
26    m_turnover_mf_divergence            2025-12-30  4658    0.000e+00   0
26    m_turnover_mf_divergence            2025-12-31  4659    0.000e+00   0
26    m_mom_weighted_strength_20d         2025-12-29  4645    0.000e+00   0
26    m_mom_weighted_strength_20d         2025-12-30  4645    0.000e+00   0
26    m_mom_weighted_strength_20d         2025-12-31  4646    0.000e+00   0
```

## Evidence Notes

- PASS means the selected factor values on audited dates are identical after input data is truncated to the cutoff.
- This targeted dynamic audit complements the static leakage scan; it is not a full recompute of every factor unless all factors are explicitly requested.
