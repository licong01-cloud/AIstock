# P0 Qlib Minute Bin Patch Plan Verification

This is a read-only verification. It did not write Qlib bin files.

```text
Metric                               Value
-----------------------------------  ----------------------------------------------------------------------
ok                                   True
checked_records                      9655
checked_unique_stocks                2696
failures                             0
warnings                             0
adjacent_checked_stocks              2696
factor_basis_method                  infer_official_denominator_from_adjacent_qlib_factor_and_db_adj_factor
factor_basis_rows                    2696
factor_basis_samples                 94283
db_max_differs_from_inferred_stocks  33
planned_factor_min                   0.6711399555206299
planned_factor_max                   1.0
```

## Factor Basis Evidence

The verifier infers each stock's official Qlib factor denominator from adjacent non-null Qlib `$factor` and current DB `adj_factor`; current DB max-adj is reported only as evidence and is not used as a silent fallback.

```text
Stock      InferredDen  DbMaxAdj  DbMax-Den     Samples  DenSpread
---------  -----------  --------  ------------  -------  ---------
000408.SZ  6.8433999    6.9622    0.11880012    35       0
000563.SZ  18.898101    19.2502   0.35209944    35       0
001309.SZ  2.5612       2.5639    0.0027        35       4.86e-08
002852.SZ  3.2543001    3.2948    0.040499934   35       0
002879.SZ  1.5559       1.59      0.0341        35       0
002920.SZ  1.0537       1.0663    0.0126        35       0
003033.SZ  1.0776       1.0865    0.0089        35       0
300100.SZ  5.0837       5.1008    0.0171        35       0
300456.SZ  5.7786       5.8253    0.046700001   35       0
300476.SZ  4.9349       4.9668    0.0319        35       0
300491.SZ  1.8458       1.8514    0.0056        35       1.62e-09
300501.SZ  3.2043       4.7394    1.5351        35       1.18e-08
300708.SZ  2.676        2.6842    0.0082000432  35       0
300850.SZ  2.9346       2.9554    0.0208        35       0
300859.SZ  1.0207       1.0299    0.0092        35       0
300956.SZ  1.4746       1.4849    0.0103        35       0
300995.SZ  1.0405       1.044     0.0034999972  35       0
301082.SZ  1.464        1.9119    0.4479        35       6.23e-09
301087.SZ  1.5165       1.5481    0.031600011   35       0
301203.SZ  1.1434       1.1655    0.0221        35       1.83e-08
```
